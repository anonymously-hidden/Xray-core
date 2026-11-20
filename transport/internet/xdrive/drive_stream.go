package xdrive

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

const driveStreamBlockBytes = slotStreamBlockBytes

// driveRangeStream shares immutable frame/emitter state across ranges and their
// retries/hedges. Before starting reads, the caller must validate the complete
// block table against frame.crc with decodeStreamChecksums (normally via slot
// decoding). Neither read nor readFrameStream repeats that table-root check.
// emit may be called concurrently, out of order, and more than once per block.
// Each emitted buffer belongs to the caller and is never reused by the reader.
type driveRangeStream struct {
	frame slotFrame
	emit  func(int, []byte) error
	mu    sync.Mutex
	wg    sync.WaitGroup
	done  bool
}

// closeAndWait fences late do workers as well as joining readers already inside
// read. readMediaRange cancels its losing attempts but need not join them before
// returning, so merely draining the outer range jobs would permit late emits.
func (s *driveRangeStream) closeAndWait() {
	s.mu.Lock()
	s.done = true
	s.mu.Unlock()
	s.wg.Wait()
}

// read validates response headers before releasing any block, then verifies each
// block independently. A successful return additionally requires exact EOF;
// receiving all blocks alone does not make a frame safe to acknowledge.
func (s *driveRangeStream) read(request driveMediaRequest, status int, contentRange string, contentLength int64, body io.Reader) ([]byte, error) {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return nil, context.Canceled
	}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()
	frame := s.frame
	if frame.size <= 0 || frame.size > maxSegmentBytes || request.size != frame.size ||
		request.start < 0 || request.start >= request.end || request.end > frame.size ||
		request.start%driveStreamBlockBytes != 0 ||
		(request.end != frame.size && request.end%driveStreamBlockBytes != 0) || s.emit == nil {
		return nil, errors.New("invalid Drive media stream range")
	}
	start, end := request.start, request.end
	switch status {
	case http.StatusPartialContent:
		if contentRange != fmt.Sprintf("bytes %d-%d/%d", start, end-1, frame.size) {
			return nil, errors.New("Drive media stream has an unexpected Content-Range")
		}
	case http.StatusOK:
		// An ignored Range must still contain the exact full frame. Verify even
		// the blocks belonging to other ranges, but emit only this range's blocks.
		start, end = 0, frame.size
	default:
		return nil, errors.New("Drive media stream answered ", status)
	}
	// net/http uses -1 for an unknown length; zero is a known empty response.
	if contentLength >= 0 && contentLength != int64(end-start) {
		return nil, errors.New("Drive media stream has an unexpected Content-Length")
	}
	for offset := start; offset < end; {
		index := offset / driveStreamBlockBytes
		size := streamBlockSize(frame.size, index)
		if size <= 0 || size > end-offset {
			return nil, errors.New("invalid Drive media stream block boundary")
		}
		block := make([]byte, size)
		if _, err := io.ReadFull(body, block); err != nil {
			return nil, errors.New("failed to read a complete Drive media stream block").Base(err)
		}
		if !validStreamBlock(frame, index, block) {
			return nil, errors.New("Drive media stream block does not match its checksum")
		}
		if offset >= request.start && offset < request.end {
			if err := s.emit(index, block); err != nil {
				return nil, err
			}
		}
		offset += size
	}
	var extra [1]byte
	n, err := io.ReadFull(body, extra[:])
	if n != 0 {
		return nil, errors.New("Drive media stream exceeds the expected response length")
	}
	if err != io.EOF {
		return nil, errors.New("failed to verify Drive media stream EOF").Base(err)
	}
	return nil, nil
}

// readFrameStream keeps the existing primary media-GET count and lane rotation,
// but places range boundaries between verified 32 KiB blocks. The emitter owns
// deduplication across retries/hedges. Every return joins the range jobs and
// fences their stream readers; success requires every range's EOF validation.
// frame.blocks must already have passed decodeStreamChecksums against frame.crc.
func (s *driveStorage) readFrameStream(ctx context.Context, id string, frame slotFrame, emit func(int, []byte) error) error {
	if frame.size <= 0 || frame.size > maxSegmentBytes || frame.mode != slotStream ||
		len(frame.blocks) != (frame.size+driveStreamBlockBytes-1)/driveStreamBlockBytes || emit == nil {
		return errors.New("invalid Drive media stream frame")
	}
	ctx, cancel := context.WithCancel(ctx)
	ctx = context.WithValue(ctx, bodyKey{}, true)
	ctx = context.WithValue(ctx, maxBodyKey{}, int64(max(frame.size, 64<<10)))
	target := s.filesURL + "/" + url.PathEscape(id) + "?alt=media&supportsAllDrives=true"
	lanes := min(driveMediaLanes, max(1, len(s.mediaClients)))
	partSize := int(defaultSegmentBytes)
	if ctx.Value(mediaHeadKey{}) != nil {
		partSize = driveMediaPart
	}
	parts := max(1, min((frame.size+partSize-1)/partSize, lanes))
	if ctx.Value(mediaHeadKey{}) != nil {
		parts = min(parts, s.streamHeadRangeLimit())
	}
	if s.cooling() || time.Since(s.lastRateLimitAt()) <= 5*time.Second {
		parts = 1
	}
	if parts > 1 {
		errors.LogDebug(ctx, "xdrive stream plan slot=", slotTag(id), " seq=", frame.seq,
			" bytes=", frame.size, " parts=", parts, " head=", ctx.Value(mediaHeadKey{}) != nil,
			" head-limit=", s.streamHeadRangeLimit())
	}
	first := s.mediaNext.Add(uint32(parts)) - uint32(parts)
	stream := &driveRangeStream{frame: frame, emit: emit}
	defer func() {
		cancel()
		stream.closeAndWait()
	}()
	if parts == 1 {
		r := driveMediaRequest{end: frame.size, size: frame.size, seq: frame.seq, stream: stream}
		if len(s.mediaClients) > 0 {
			r.client = s.mediaClients[int(first%uint32(len(s.mediaClients)))]
		}
		_, err := s.readMediaRange(ctx, target, r)
		return err
	}
	blocks := len(frame.blocks)
	results := make(chan error, parts)
	for i := range parts {
		r := driveMediaRequest{
			start:  (i * blocks / parts) * driveStreamBlockBytes,
			end:    min(((i+1)*blocks/parts)*driveStreamBlockBytes, frame.size),
			size:   frame.size,
			seq:    frame.seq,
			stream: stream,
		}
		if len(s.mediaClients) > 0 {
			r.client = s.mediaClients[int((first+uint32(i))%uint32(len(s.mediaClients)))]
		}
		// do registers each request with beginRequest, as for readFrameBody.
		go func() {
			_, err := s.readMediaRange(ctx, target, r)
			results <- err
		}()
	}
	var firstErr error
	canceled := ctx.Done()
	for pending := parts; pending > 0; {
		select {
		case err := <-results:
			pending--
			if err != nil && firstErr == nil {
				firstErr = err
				cancel()
				canceled = nil
			}
		case <-canceled:
			firstErr = ctx.Err()
			cancel()
			canceled = nil
		}
	}
	return firstErr
}
