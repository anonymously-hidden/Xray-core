package xdrive

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

const (
	// Every alt=media GET costs the same flat 200 Drive quota units whatever
	// its size, and body GETs were 90% of the units behind the per-user rate
	// limit. A frame that is not blocking delivery therefore takes one GET per
	// default segment (a 512 KiB slot is one range, a 2 MiB slot four). The head
	// frame, which every later frame waits for, takes one 128 KiB GET per lane.
	// Separate pools prevent HTTP/2 streams from sharing the same stalled TCP
	// connection, and frames are spread over them in turn.
	driveMediaLanes = 4
	driveMediaPart  = 128 << 10
	// Range GETs have a permit pool of their own so a sustained download
	// (slotBodyAhead frames of driveMediaLanes ranges) cannot take the permits
	// of slot writes and tail reads. It still covers every range of the
	// body-admission window in two waves, as the shared pool did.
	driveMediaInflight = driveMaxInflight - 2*driveReservedInflight
)

type driveMediaKey struct{}

// mediaHeadKey marks a body read for the frame the reader is delivering next.
type mediaHeadKey struct{}
type mediaPromotionKey struct{}

// Promotion observes head eligibility without reshaping or restarting a primary.
func mediaHead(ctx context.Context) bool {
	if ctx.Value(mediaHeadKey{}) != nil {
		return true
	}
	promoted, _ := ctx.Value(mediaPromotionKey{}).(<-chan struct{})
	select {
	case <-promoted:
		return true
	default:
		return false
	}
}

type driveMediaRequest struct {
	start, end, size int // end is exclusive
	seq              int64
	client           *http.Client
	rescue           bool
}

func (r *driveMediaRequest) extract(status int, contentRange string, body []byte) ([]byte, error) {
	if status == http.StatusOK {
		// An endpoint may ignore Range. Accept only an exact full-sized body.
		if len(body) == r.size {
			return body[r.start:r.end], nil
		}
	} else if contentRange == fmt.Sprintf("bytes %d-%d/%d", r.start, r.end-1, r.size) && len(body) == r.end-r.start {
		return body, nil
	}
	return nil, errors.New("Drive media range has an unexpected offset or length")
}

// readFrameBody uses the ordinary Drive Range API; peers still write exactly
// the same slot frames. The whole-frame CRC is checked before any byte is
// released or acknowledged, including when a slot changes during the GETs.
func (s *driveStorage) readFrameBody(ctx context.Context, id string, frame slotFrame) ([]byte, error) {
	if frame.size <= 0 || frame.size > maxSegmentBytes {
		return nil, errors.New("invalid Drive media frame size")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx = context.WithValue(ctx, bodyKey{}, true)
	ctx = context.WithValue(ctx, maxBodyKey{}, int64(max(frame.size, 64<<10)))
	target := s.filesURL + "/" + url.PathEscape(id) + "?alt=media&supportsAllDrives=true"
	lanes := max(1, len(s.mediaClients))
	// The head frame, which every later frame waits for, is split into
	// driveMediaPart ranges; other frames take one range per default segment.
	// Both are capped by the number of lanes.
	partSize := int(defaultSegmentBytes)
	if ctx.Value(mediaHeadKey{}) != nil {
		partSize = driveMediaPart
	}
	parts := max(1, min((frame.size+partSize-1)/partSize, lanes))
	if s.cooling() || time.Since(s.lastRateLimitAt()) <= 5*time.Second {
		parts = 1
	}
	first := s.mediaNext.Add(uint32(parts)) - uint32(parts)
	if parts == 1 {
		r := driveMediaRequest{end: frame.size, size: frame.size, seq: frame.seq}
		if len(s.mediaClients) > 0 {
			r.client = s.mediaClients[int(first%uint32(len(s.mediaClients)))]
		}
		body, err := s.readMediaRange(ctx, target, r)
		if err != nil {
			return nil, err
		}
		if !frame.holds(body) {
			return nil, errors.New("Drive media ranges do not match the segment checksum")
		}
		return body, nil
	}
	width := (frame.size + parts - 1) / parts
	type result struct {
		start int
		body  []byte
		err   error
	}
	results := make(chan result, parts)
	for i := range parts {
		r := driveMediaRequest{start: i * width, end: min((i+1)*width, frame.size), size: frame.size, seq: frame.seq}
		if len(s.mediaClients) > 0 {
			r.client = s.mediaClients[int((first+uint32(i))%uint32(len(s.mediaClients)))]
		}
		go func() {
			body, err := s.readMediaRange(ctx, target, r)
			results <- result{r.start, body, err}
		}()
	}
	body := make([]byte, frame.size)
	for range parts {
		select {
		case res := <-results:
			if res.err != nil {
				return nil, res.err
			}
			copy(body[res.start:], res.body)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if !frame.holds(body) {
		return nil, errors.New("Drive media ranges do not match the segment checksum")
	}
	return body, nil
}

// A hedge races only the missing range on the separate bulk connection, never
// redownloads completed ranges, and never cancels a primary that is progressing.
// Its per-range stall clock starts at dispatch, follows headers/body progress,
// and stops when the primary starts its own retry recovery.
func (s *driveStorage) readMediaRange(ctx context.Context, target string, r driveMediaRequest) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	primary := &driveReadProgress{dispatched: make(chan struct{})}
	type result struct {
		body  []byte
		err   error
		hedge bool
	}
	results := make(chan result, 2)
	launch := func(hedge bool) {
		request := r
		requestCtx := ctx
		if hedge {
			request.client = s.bulkClient
			request.rescue = true
			requestCtx = context.WithValue(requestCtx, readHedgeGateKey{}, readHedgeGate{primary: primary, delay: slotBodyHedge})
		} else {
			requestCtx = context.WithValue(requestCtx, readProgressKey{}, primary)
			requestCtx = context.WithValue(requestCtx, bodyProgressKey{}, primary.progress)
		}
		go func() {
			status, body, err := s.do(context.WithValue(requestCtx, driveMediaKey{}, &request), http.MethodGet, target, "", nil)
			if err == nil {
				switch status {
				case http.StatusOK, http.StatusPartialContent:
				case http.StatusNotFound:
					err = errNotFound
				default:
					err = errors.New("Drive media range answered ", status)
				}
			}
			results <- result{body, err, hedge}
		}()
	}
	launch(false)
	pending := 1
	// The hedge delay is that of slot body reads; the budget is the media one.
	var timer *time.Timer
	var hedgeAt <-chan time.Time
	var dispatched <-chan struct{}
	var promoted <-chan struct{}
	promotion, _ := ctx.Value(mediaPromotionKey{}).(<-chan struct{})
	if slotBodyHedge > 0 {
		dispatched = primary.dispatched
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		select {
		case res := <-results:
			pending--
			if res.err == nil {
				s.noteMediaRange()
				if res.hedge && s.timing != nil {
					s.timing.noteHedgeWin(kindBodyRead)
				}
				return res.body, nil
			}
			if pending == 0 || isAuthError(res.err) {
				return nil, res.err
			}
		case <-dispatched:
			dispatched = nil
			remaining, _ := primary.remaining(slotBodyHedge)
			timer = time.NewTimer(max(0, remaining))
			hedgeAt = timer.C
		case <-promoted:
			promoted = nil
			remaining, active := primary.remaining(slotBodyHedge)
			if active {
				// Use the existing stall age, not another two-second delay.
				timer.Reset(max(0, remaining))
				hedgeAt = timer.C
			}
		case <-hedgeAt:
			helper := r
			helper.rescue = true
			// A spent timer does not chase future quota/budget or race retries.
			hedgeAt = nil
			if s.bulkClient == nil || r.client == nil || r.client == s.bulkClient {
				continue
			}
			remaining, active := primary.remaining(slotBodyHedge)
			if !active {
				continue
			}
			if remaining > 0 {
				timer.Reset(remaining)
				hedgeAt = timer.C
				continue
			}
			if promotion != nil && !mediaHead(ctx) {
				// Keep the primary running, but reserve duplicate work for a
				// frame that blocks delivery. Missing/nil tracking stays legacy.
				promoted = promotion
				if s.timing != nil {
					s.timing.noteDeferredHedge(kindBodyRead)
				}
				continue
			}
			if !s.readHedgeAllowed(context.WithValue(ctx, driveMediaKey{}, &helper)) || !s.takeMediaHedgeToken() {
				continue
			}
			pending++
			launch(true)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
