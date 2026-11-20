package xdrive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type driveStreamTestTransport func(*http.Request) (*http.Response, error)

func (f driveStreamTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func driveStreamTestStorage(t *testing.T, transport driveStreamTestTransport, lanes int) *driveStorage {
	t.Helper()
	client := &http.Client{Transport: transport}
	s := &driveStorage{
		driveShared: &driveShared{
			filesURL:    "https://drive-stream.invalid/files",
			token:       "fake-token",
			tokenExpiry: time.Now().Add(time.Hour),
		},
		client:      client,
		inflight:    make(chan struct{}, driveMaxInflight),
		reserved:    make(chan struct{}, driveReservedInflight),
		mediaRanges: make(chan struct{}, driveMediaInflight),
	}
	for range lanes {
		s.mediaClients = append(s.mediaClients, &http.Client{Transport: transport})
	}
	t.Cleanup(s.driveShared.closeWork)
	return s
}

func driveStreamTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*17 + i/driveStreamBlockBytes)
	}
	return data
}

func driveStreamTestFrame(data []byte) slotFrame {
	frame := slotFrame{seq: 7, mode: slotStream, size: len(data), crc: crc32.ChecksumIEEE(data)}
	for start := 0; start < len(data); start += driveStreamBlockBytes {
		frame.blocks = append(frame.blocks, crc32.ChecksumIEEE(data[start:min(start+driveStreamBlockBytes, len(data))]))
	}
	return frame
}

func driveStreamTestResponse(request *http.Request, status int, contentRange string, length int64, body io.ReadCloser) *http.Response {
	header := make(http.Header)
	if contentRange != "" {
		header.Set("Content-Range", contentRange)
	}
	return &http.Response{StatusCode: status, Header: header, ContentLength: length, Body: body, Request: request}
}

type driveStreamTestBlock struct {
	index int
	data  []byte
}

// This fake body prevents reading beyond pauseAt until the test releases it.
// Cancellation also releases a blocked Read, like a real HTTP response body.
type driveStreamTestGatedBody struct {
	ctx       context.Context
	data      []byte
	position  int
	pauseAt   int
	release   <-chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func (b *driveStreamTestGatedBody) Read(p []byte) (int, error) {
	if b.release != nil && b.position >= b.pauseAt {
		select {
		case <-b.release:
			b.release = nil
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		}
	}
	end := len(b.data)
	if b.release != nil {
		end = min(end, b.pauseAt)
	}
	if b.position == end {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.position:end])
	b.position += n
	return n, nil
}

func (b *driveStreamTestGatedBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func TestDriveFrameStreamEmitsVerifiedBlockBeforeEOF(t *testing.T) {
	data := driveStreamTestData(2*driveStreamBlockBytes + 23)
	frame := driveStreamTestFrame(data)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	closed := make(chan struct{})
	s := driveStreamTestStorage(t, func(request *http.Request) (*http.Response, error) {
		body := &driveStreamTestGatedBody{
			ctx: request.Context(), data: data, pauseAt: driveStreamBlockBytes, release: release, closed: closed,
		}
		return driveStreamTestResponse(request, http.StatusPartialContent,
			fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)), int64(len(data)), body), nil
	}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	emitted := make(chan driveStreamTestBlock, len(frame.blocks))
	done := make(chan error, 1)
	go func() {
		done <- s.readFrameStream(ctx, "slot", frame, func(index int, block []byte) error {
			emitted <- driveStreamTestBlock{index, block}
			return nil
		})
	}()
	var first driveStreamTestBlock
	select {
	case first = <-emitted:
	case err := <-done:
		t.Fatalf("stream ended before its first block: %v", err)
	case <-ctx.Done():
		t.Fatal("first verified block was buffered until the rest of the body")
	}
	if first.index != 0 || !bytes.Equal(first.data, data[:driveStreamBlockBytes]) {
		t.Fatal("incorrect first verified block")
	}
	select {
	case err := <-done:
		t.Fatalf("stream completed while its body was blocked: %v", err)
	default:
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("stream did not finish after releasing its body")
	}
	// Keep all emitted slices through subsequent reads to detect buffer reuse.
	blocks := []driveStreamTestBlock{first}
	for range len(frame.blocks) - 1 {
		select {
		case block := <-emitted:
			blocks = append(blocks, block)
		default:
			t.Fatal("successful stream omitted a block")
		}
	}
	for _, block := range blocks {
		start := block.index * driveStreamBlockBytes
		if !bytes.Equal(block.data, data[start:min(start+driveStreamBlockBytes, len(data))]) {
			t.Fatalf("emitted block %d was changed or reused", block.index)
		}
	}
	select {
	case <-closed:
	default:
		t.Fatal("response body was not closed")
	}
}

func TestDriveRangeStreamResponseValidation(t *testing.T) {
	data := driveStreamTestData(3*driveStreamBlockBytes + 19)
	frame := driveStreamTestFrame(data)
	part := data[driveStreamBlockBytes : 2*driveStreamBlockBytes]
	contentRange := fmt.Sprintf("bytes %d-%d/%d", driveStreamBlockBytes, 2*driveStreamBlockBytes-1, len(data))
	corruptPart := append([]byte(nil), part...)
	corruptPart[0] ^= 1
	corruptBefore := append([]byte(nil), data...)
	corruptBefore[0] ^= 1
	corruptAfter := append([]byte(nil), data...)
	corruptAfter[2*driveStreamBlockBytes] ^= 1
	cases := []struct {
		name         string
		status       int
		contentRange string
		length       int64
		body         []byte
		wantError    bool
		wantBlocks   []int
	}{
		{"partial known length", http.StatusPartialContent, contentRange, int64(len(part)), part, false, []int{1}},
		{"partial unknown length", http.StatusPartialContent, contentRange, -1, part, false, []int{1}},
		{"partial known zero length", http.StatusPartialContent, contentRange, 0, part, true, nil},
		{"ignored range known length", http.StatusOK, "", int64(len(data)), data, false, []int{1}},
		{"ignored range unknown length", http.StatusOK, "", -1, data, false, []int{1}},
		{"ignored range known zero length", http.StatusOK, "", 0, data, true, nil},
		{"wrong range", http.StatusPartialContent, fmt.Sprintf("bytes 0-%d/%d", len(part)-1, len(data)), -1, part, true, nil},
		{"missing range", http.StatusPartialContent, "", -1, part, true, nil},
		{"noncanonical range", http.StatusPartialContent, contentRange + " ", -1, part, true, nil},
		{"unknown total", http.StatusPartialContent, fmt.Sprintf("bytes %d-%d/*", driveStreamBlockBytes, 2*driveStreamBlockBytes-1), -1, part, true, nil},
		{"wrong partial length", http.StatusPartialContent, contentRange, int64(len(part) - 1), part, true, nil},
		{"truncated partial", http.StatusPartialContent, contentRange, -1, part[:len(part)-1], true, nil},
		{"oversized partial", http.StatusPartialContent, contentRange, -1, append(append([]byte(nil), part...), 0), true, []int{1}},
		{"corrupt assigned block", http.StatusPartialContent, contentRange, int64(len(part)), corruptPart, true, nil},
		{"ignored range wrong announced length", http.StatusOK, "", int64(len(part)), data, true, nil},
		{"ignored range truncated", http.StatusOK, "", -1, data[:2*driveStreamBlockBytes], true, []int{1}},
		{"ignored range oversized", http.StatusOK, "", -1, append(append([]byte(nil), data...), 0), true, []int{1}},
		{"ignored range corrupt unassigned prefix", http.StatusOK, "", int64(len(data)), corruptBefore, true, nil},
		{"ignored range corrupt unassigned suffix", http.StatusOK, "", int64(len(data)), corruptAfter, true, []int{1}},
		{"unexpected status", http.StatusBadGateway, "", -1, part, true, nil},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{Transport: driveStreamTestTransport(func(request *http.Request) (*http.Response, error) {
				return driveStreamTestResponse(request, test.status, test.contentRange, test.length,
					io.NopCloser(bytes.NewReader(test.body))), nil
			})}
			response, err := client.Get("https://drive-stream.invalid/media")
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			var emitted []driveStreamTestBlock
			stream := &driveRangeStream{frame: frame, emit: func(index int, block []byte) error {
				emitted = append(emitted, driveStreamTestBlock{index, block})
				return nil
			}}
			request := driveMediaRequest{start: driveStreamBlockBytes, end: 2 * driveStreamBlockBytes, size: len(data), seq: frame.seq}
			payload, err := stream.read(request, response.StatusCode, response.Header.Get("Content-Range"), response.ContentLength, response.Body)
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v, want error %v", err, test.wantError)
			}
			if payload != nil {
				t.Fatal("stream returned a second buffered payload")
			}
			if len(emitted) != len(test.wantBlocks) {
				t.Fatalf("emitted %d blocks, want %v", len(emitted), test.wantBlocks)
			}
			for i, block := range emitted {
				if block.index != test.wantBlocks[i] || !bytes.Equal(block.data, part) {
					t.Fatalf("unexpected or unverified emitted block %d", block.index)
				}
			}
		})
	}
}

func TestDriveRangeStreamEmitterError(t *testing.T) {
	data := driveStreamTestData(2 * driveStreamBlockBytes)
	frame := driveStreamTestFrame(data)
	want := errors.New("collector stopped")
	calls := 0
	stream := &driveRangeStream{frame: frame, emit: func(int, []byte) error {
		calls++
		return want
	}}
	request := driveMediaRequest{end: len(data), size: len(data), seq: frame.seq}
	payload, err := stream.read(request, http.StatusPartialContent,
		fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)), int64(len(data)), bytes.NewReader(data))
	if !errors.Is(err, want) || calls != 1 || payload != nil {
		t.Fatalf("emitter error was not returned immediately: calls=%d, payload=%d, err=%v", calls, len(payload), err)
	}
}

func TestDriveRangeStreamCloseJoinsAndFencesReaders(t *testing.T) {
	data := driveStreamTestData(driveStreamBlockBytes)
	frame := driveStreamTestFrame(data)
	emitting := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var calls atomic.Int32
	stream := &driveRangeStream{frame: frame, emit: func(int, []byte) error {
		calls.Add(1)
		close(emitting)
		<-release
		return nil
	}}
	request := driveMediaRequest{end: len(data), size: len(data), seq: frame.seq}
	contentRange := fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data))
	readDone := make(chan error, 1)
	go func() {
		_, err := stream.read(request, http.StatusPartialContent, contentRange, int64(len(data)), bytes.NewReader(data))
		readDone <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	select {
	case <-emitting:
	case <-ctx.Done():
		t.Fatal("reader did not enter its emitter")
	}
	joined := make(chan struct{})
	go func() {
		stream.closeAndWait()
		close(joined)
	}()
	select {
	case <-joined:
		t.Fatal("stream shutdown did not join its active emitter")
	case <-time.After(10 * time.Millisecond):
	}
	unblock()
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal("stream shutdown did not join its reader")
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("reader did not complete")
	}
	payload, err := stream.read(request, http.StatusPartialContent, contentRange, int64(len(data)), bytes.NewReader(data))
	if !errors.Is(err, context.Canceled) || payload != nil || calls.Load() != 1 {
		t.Fatalf("a late worker entered the closed stream: calls=%d, err=%v", calls.Load(), err)
	}
}

func TestDriveFrameStreamRangePolicyAndOwnership(t *testing.T) {
	cases := []struct {
		name        string
		size        int
		lanes       int
		head        bool
		promoted    bool
		rateLimited bool
		ignored     bool
		parts       int
	}{
		{"default segment", defaultSegmentBytes, 4, false, false, false, false, 1},
		{"default boundary", defaultSegmentBytes + 1, 4, false, false, false, false, 2},
		{"default large", 2 * defaultSegmentBytes, 4, false, false, false, false, 2},
		{"default maximum lanes", 4 * defaultSegmentBytes, 4, false, false, false, false, 4},
		{"head part", driveMediaPart, 4, true, false, false, false, 1},
		{"head boundary", driveMediaPart + 1, 4, true, false, false, false, 2},
		{"head four parts", defaultSegmentBytes, 4, true, false, false, false, 4},
		{"head three lanes", defaultSegmentBytes + 17, 3, true, false, false, false, 3},
		{"four part ceiling", defaultSegmentBytes + 17, 8, true, false, false, false, 4},
		{"no media lanes", defaultSegmentBytes + 17, 0, true, false, false, false, 1},
		{"recent quota response", defaultSegmentBytes + 17, 4, true, false, true, false, 1},
		{"promotion keeps primary shape", defaultSegmentBytes, 4, false, true, false, false, 1},
		{"ignored range ownership", defaultSegmentBytes + 17, 4, true, false, false, true, 4},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			data := driveStreamTestData(test.size)
			frame := driveStreamTestFrame(data)
			type interval struct{ start, end int }
			var mu sync.Mutex
			var ranges []interval
			blocks := make(map[int][]byte)
			counts := make(map[int]int)
			s := driveStreamTestStorage(t, func(request *http.Request) (*http.Response, error) {
				var start, last int
				if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &last); err != nil {
					return nil, err
				}
				if request.Method != http.MethodGet || request.URL.Query().Get("alt") != "media" ||
					request.URL.Query().Get("supportsAllDrives") != "true" || request.Header.Get("Accept-Encoding") != "identity" {
					return nil, fmt.Errorf("unexpected media request: %s %s", request.Method, request.URL)
				}
				if start < 0 || last < start || last >= len(data) {
					return nil, fmt.Errorf("invalid range %d-%d", start, last)
				}
				mu.Lock()
				ranges = append(ranges, interval{start, last + 1})
				mu.Unlock()
				if test.ignored {
					return driveStreamTestResponse(request, http.StatusOK, "", int64(len(data)), io.NopCloser(bytes.NewReader(data))), nil
				}
				part := data[start : last+1]
				return driveStreamTestResponse(request, http.StatusPartialContent,
					fmt.Sprintf("bytes %d-%d/%d", start, last, len(data)), int64(len(part)), io.NopCloser(bytes.NewReader(part))), nil
			}, test.lanes)
			s.mediaNext.Store(7)
			if test.rateLimited {
				s.lastRateLimit = time.Now()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if test.head {
				ctx = context.WithValue(ctx, mediaHeadKey{}, true)
			}
			if test.promoted {
				promotion := make(chan struct{})
				close(promotion)
				ctx = context.WithValue(ctx, mediaPromotionKey{}, (<-chan struct{})(promotion))
			}
			if err := s.readFrameStream(ctx, "slot", frame, func(index int, block []byte) error {
				mu.Lock()
				defer mu.Unlock()
				counts[index]++
				blocks[index] = block
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(ranges) != test.parts || s.mediaNext.Load() != uint32(7+test.parts) {
				t.Fatalf("primary ranges=%d, rotation=%d; want %d ranges", len(ranges), s.mediaNext.Load(), test.parts)
			}
			sort.Slice(ranges, func(i, j int) bool { return ranges[i].start < ranges[j].start })
			end := 0
			for _, part := range ranges {
				if part.start != end || part.start%driveStreamBlockBytes != 0 ||
					(part.end != len(data) && part.end%driveStreamBlockBytes != 0) {
					t.Fatalf("ranges are not contiguous block-aligned partitions: %v", ranges)
				}
				end = part.end
			}
			if end != len(data) || len(blocks) != len(frame.blocks) {
				t.Fatal("successful ranges did not cover the whole frame")
			}
			for index := range frame.blocks {
				start := index * driveStreamBlockBytes
				if counts[index] != 1 || !bytes.Equal(blocks[index], data[start:min(start+driveStreamBlockBytes, len(data))]) {
					t.Fatalf("block %d was emitted %d times or has incorrect data", index, counts[index])
				}
			}
		})
	}
}

func TestDriveFrameStreamRetriesVerifiedPrefix(t *testing.T) {
	data := driveStreamTestData(2*driveStreamBlockBytes + 19)
	frame := driveStreamTestFrame(data)
	var attempts atomic.Int32
	s := driveStreamTestStorage(t, func(request *http.Request) (*http.Response, error) {
		body := data
		if attempts.Add(1) == 1 {
			body = data[:driveStreamBlockBytes+13]
		}
		return driveStreamTestResponse(request, http.StatusPartialContent,
			fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)), -1, io.NopCloser(bytes.NewReader(body))), nil
	}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var mu sync.Mutex
	seen := make(map[int][]byte)
	counts := make(map[int]int)
	if err := s.readFrameStream(ctx, "slot", frame, func(index int, block []byte) error {
		mu.Lock()
		defer mu.Unlock()
		counts[index]++
		if _, ok := seen[index]; !ok {
			seen[index] = block
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts.Load() != 2 || counts[0] != 2 || len(seen) != len(frame.blocks) {
		t.Fatalf("retry did not preserve/deduplicate its verified prefix: attempts=%d, counts=%v", attempts.Load(), counts)
	}
	for index, block := range seen {
		start := index * driveStreamBlockBytes
		if !bytes.Equal(block, data[start:min(start+driveStreamBlockBytes, len(data))]) {
			t.Fatalf("retry supplied an unverified block %d", index)
		}
	}
}

func TestDriveFrameStreamWaitsForEveryRangeEOF(t *testing.T) {
	data := driveStreamTestData(driveMediaPart + 1)
	frame := driveStreamTestFrame(data)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	s := driveStreamTestStorage(t, func(request *http.Request) (*http.Response, error) {
		var start, last int
		if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &last); err != nil {
			return nil, err
		}
		part := data[start : last+1]
		var body io.ReadCloser = io.NopCloser(bytes.NewReader(part))
		if last == len(data)-1 {
			body = &driveStreamTestGatedBody{
				ctx: request.Context(), data: part, pauseAt: len(part), release: release, closed: make(chan struct{}),
			}
		}
		return driveStreamTestResponse(request, http.StatusPartialContent,
			fmt.Sprintf("bytes %d-%d/%d", start, last, len(data)), int64(len(part)), body), nil
	}, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, mediaHeadKey{}, true)
	emitted := make(chan struct{}, len(frame.blocks))
	done := make(chan error, 1)
	go func() {
		done <- s.readFrameStream(ctx, "slot", frame, func(int, []byte) error {
			emitted <- struct{}{}
			return nil
		})
	}()
	for range frame.blocks {
		select {
		case <-emitted:
		case err := <-done:
			t.Fatalf("stream ended before all blocks: %v", err)
		case <-ctx.Done():
			t.Fatal("stream did not emit all blocks before its EOF check")
		}
	}
	select {
	case err := <-done:
		t.Fatalf("all blocks incorrectly counted as validated EOF: %v", err)
	default:
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("stream did not join all successful ranges")
	}
}

func TestDriveFrameStreamErrorCancelsOtherRanges(t *testing.T) {
	data := driveStreamTestData(driveMediaPart + 1)
	frame := driveStreamTestFrame(data)
	started := make(chan struct{})
	closed := make(chan struct{})
	var startedOnce sync.Once
	s := driveStreamTestStorage(t, func(request *http.Request) (*http.Response, error) {
		var start, last int
		if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &last); err != nil {
			return nil, err
		}
		if start == 0 {
			select {
			case <-started:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
			return driveStreamTestResponse(request, http.StatusPartialContent, "wrong range", -1,
				io.NopCloser(bytes.NewReader(data[start:last+1]))), nil
		}
		startedOnce.Do(func() { close(started) })
		body := &driveStreamTestGatedBody{
			ctx: request.Context(), data: data[start : last+1], pauseAt: 0, release: make(chan struct{}), closed: closed,
		}
		return driveStreamTestResponse(request, http.StatusPartialContent,
			fmt.Sprintf("bytes %d-%d/%d", start, last, len(data)), -1, body), nil
	}, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, mediaHeadKey{}, true)
	if err := s.readFrameStream(ctx, "slot", frame, func(int, []byte) error {
		t.Error("invalid or blocked ranges must not emit blocks")
		return nil
	}); err == nil {
		t.Fatal("invalid range succeeded")
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("failed frame did not cancel and close its other range")
	}
}

func TestDriveFrameStreamLeavesLegacyMediaUnchanged(t *testing.T) {
	data := driveStreamTestData(defaultSegmentBytes + 17)
	frame := newSlotFrame(7, slotMedia, data)
	var requests atomic.Int32
	s := driveStreamTestStorage(t, func(request *http.Request) (*http.Response, error) {
		requests.Add(1)
		var start, last int
		if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &last); err != nil {
			return nil, err
		}
		part := data[start : last+1]
		return driveStreamTestResponse(request, http.StatusPartialContent,
			fmt.Sprintf("bytes %d-%d/%d", start, last, len(data)), int64(len(part)), io.NopCloser(bytes.NewReader(part))), nil
	}, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	body, err := s.readFrameBody(ctx, "slot", frame)
	if err != nil || !bytes.Equal(body, data) || requests.Load() != 2 {
		t.Fatalf("legacy media changed: requests=%d, bytes=%d, err=%v", requests.Load(), len(body), err)
	}
	frame.crc ^= 1
	if _, err := s.readFrameBody(ctx, "slot", frame); err == nil {
		t.Fatal("legacy media no longer checks its whole-frame CRC")
	}
}
