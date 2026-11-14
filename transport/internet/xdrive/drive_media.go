package xdrive

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
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
	driveMediaSplit = defaultSegmentBytes
	// Range GETs have a permit pool of their own so a sustained download
	// (slotBodyAhead frames of driveMediaLanes ranges) cannot take the permits
	// of slot writes and tail reads. It still covers every range of the
	// body-admission window in two waves, as the shared pool did.
	driveMediaInflight = driveMaxInflight - 2*driveReservedInflight
)

type driveMediaKey struct{}

// mediaHeadKey marks a body read for the frame the reader is delivering next.
type mediaHeadKey struct{}

type driveMediaRequest struct {
	start, end, size int // end is exclusive
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
	unit := driveMediaSplit
	if ctx.Value(mediaHeadKey{}) != nil {
		unit = driveMediaPart
	}
	parts := min((frame.size+unit-1)/unit, lanes)
	if s.cooling() || time.Since(s.lastRateLimitAt()) <= 5*time.Second {
		parts = 1
	}
	first := s.mediaNext.Add(uint32(parts)) - uint32(parts)
	width := (frame.size + parts - 1) / parts
	type result struct {
		start int
		body  []byte
		err   error
	}
	results := make(chan result, parts)
	for i := range parts {
		r := driveMediaRequest{start: i * width, end: min((i+1)*width, frame.size), size: frame.size}
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
// Its stall clock is per range, so an earlier slow full-body download cannot
// inflate recovery delays for every later segment.
func (s *driveStorage) readMediaRange(ctx context.Context, target string, r driveMediaRequest) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var progress struct {
		sync.Mutex
		at time.Time
	}
	progress.at = time.Now()
	ctx = context.WithValue(ctx, bodyProgressKey{}, func() {
		progress.Lock()
		progress.at = time.Now()
		progress.Unlock()
	})
	type result struct {
		body  []byte
		err   error
		hedge bool
	}
	results := make(chan result, 2)
	launch := func(hedge bool) {
		request := r
		if hedge {
			request.client = s.bulkClient
			request.rescue = true
		}
		go func() {
			status, body, err := s.do(context.WithValue(ctx, driveMediaKey{}, &request), http.MethodGet, target, "", nil)
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
	if slotBodyHedge > 0 {
		timer = time.NewTimer(slotBodyHedge)
		defer timer.Stop()
		hedgeAt = timer.C
	}
	hedged, won := false, false
	defer func() {
		if hedged && s.timing != nil {
			s.timing.noteHedge(kindBodyRead, won)
		}
	}()
	for {
		select {
		case res := <-results:
			pending--
			if res.err == nil {
				s.noteMediaRange()
				won = res.hedge
				return res.body, nil
			}
			if pending == 0 || isAuthError(res.err) {
				return nil, res.err
			}
		case <-hedgeAt:
			if s.bulkClient == nil || r.client == nil || r.client == s.bulkClient {
				continue
			}
			progress.Lock()
			remaining := slotBodyHedge - time.Since(progress.at)
			progress.Unlock()
			if remaining > 0 {
				timer.Reset(remaining)
				continue
			}
			if s.cooling() || time.Since(s.lastRateLimitAt()) <= 5*time.Second || !s.takeMediaHedgeToken() {
				// The timer is spent; re-arm it so the hedge can still start
				// once the cooldown ends or the budget has refilled.
				timer.Reset(slotBodyHedge)
				continue
			}
			hedged = true
			pending++
			launch(true)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
