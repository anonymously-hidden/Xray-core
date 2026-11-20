package xdrive

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

type mediaTestResult struct {
	body []byte
	err  error
}

type mediaTestGatedBody struct {
	io.Reader
	ctx        context.Context
	release    <-chan struct{}
	started    chan struct{}
	canceled   chan struct{}
	closed     chan struct{}
	startOnce  sync.Once
	cancelOnce sync.Once
	closeOnce  sync.Once
	closes     atomic.Int32
}

func newMediaTestGatedBody(ctx context.Context, data []byte, release <-chan struct{}) *mediaTestGatedBody {
	return &mediaTestGatedBody{
		Reader: bytes.NewReader(data), ctx: ctx, release: release,
		started: make(chan struct{}), canceled: make(chan struct{}), closed: make(chan struct{}),
	}
}

func (b *mediaTestGatedBody) Read(p []byte) (int, error) {
	b.startOnce.Do(func() { close(b.started) })
	select {
	case <-b.release:
		return b.Reader.Read(p)
	case <-b.ctx.Done():
		b.cancelOnce.Do(func() { close(b.canceled) })
		return 0, b.ctx.Err()
	}
}

func (b *mediaTestGatedBody) Close() error {
	b.closes.Add(1)
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func mediaTestPayload(size int) []byte {
	body := make([]byte, size)
	for i := range body {
		body[i] = byte((i * 31) ^ (i >> 8) ^ (i >> 16))
	}
	return body
}

func mediaTestCheckRequest(t *testing.T, r *http.Request, start, end int) {
	t.Helper()
	if r.Method != http.MethodGet || r.URL.Path != "/files/item" || r.URL.Query().Get("alt") != "media" || r.URL.Query().Get("supportsAllDrives") != "true" {
		t.Errorf("unexpected media request: %s %s", r.Method, r.URL)
	}
	if got, want := r.Header.Get("Range"), fmt.Sprintf("bytes=%d-%d", start, end-1); got != want {
		t.Errorf("Range = %q, want %q", got, want)
	}
	if got := r.Header.Get("Accept-Encoding"); got != "identity" {
		t.Errorf("Accept-Encoding = %q, want identity", got)
	}
}

func mediaTestClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func mediaTestAssertBody(t *testing.T, body *mediaTestGatedBody, canceled bool) {
	t.Helper()
	if !mediaTestClosed(body.closed) || body.closes.Load() != 1 {
		t.Errorf("response body was not closed exactly once: closes=%d", body.closes.Load())
	}
	if got := mediaTestClosed(body.canceled); got != canceled {
		t.Errorf("response body canceled = %v, want %v", got, canceled)
	}
}

func mediaTestAssertNoPermits(t *testing.T, s *driveStorage) {
	t.Helper()
	if len(s.inflight) != 0 || len(s.reserved) != 0 || len(s.mediaRanges) != 0 || len(s.mediaRescue) != 0 {
		t.Errorf("leaked permits: inflight=%d reserved=%d media=%d rescue=%d", len(s.inflight), len(s.reserved), len(s.mediaRanges), len(s.mediaRescue))
	}
}

func mediaTestAssertPending(t *testing.T, done <-chan mediaTestResult) {
	t.Helper()
	select {
	case res := <-done:
		t.Fatalf("read returned before the remaining requests completed: bytes=%d err=%v", len(res.body), res.err)
	default:
	}
}

type mediaTestSplitFlight struct {
	storage  *driveStorage
	cancel   context.CancelFunc
	done     chan mediaTestResult
	releases []chan struct{}
	bodies   []chan *mediaTestGatedBody
	calls    []atomic.Int32
}

func startMediaTestSplit(t *testing.T, frame slotFrame, sources [][]byte, fullBody bool, invalidRange int) *mediaTestSplitFlight {
	t.Helper()
	parts := len(sources)
	width := (frame.size + parts - 1) / parts
	flight := &mediaTestSplitFlight{
		done: make(chan mediaTestResult, 1), releases: make([]chan struct{}, parts),
		bodies: make([]chan *mediaTestGatedBody, parts), calls: make([]atomic.Int32, parts),
	}
	clients := make([]*http.Client, parts)
	for i := range parts {
		flight.releases[i] = make(chan struct{})
		flight.bodies[i] = make(chan *mediaTestGatedBody, driveMaxAttempts)
		clients[i] = &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
			flight.calls[i].Add(1)
			start, end := i*width, min((i+1)*width, frame.size)
			mediaTestCheckRequest(t, r, start, end)
			status, data := http.StatusPartialContent, sources[i][start:end]
			if fullBody && i%2 == 1 {
				status, data = http.StatusOK, sources[i]
			}
			body := newMediaTestGatedBody(r.Context(), data, flight.releases[i])
			flight.bodies[i] <- body
			resp := memoryResponse(status, "")
			resp.Body = body
			if status == http.StatusPartialContent {
				offset := start
				if i == invalidRange {
					offset++
				}
				resp.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, end-1, frame.size))
			}
			return resp, nil
		})}
	}
	flight.storage = memoryDrive(clients[0].Transport)
	flight.storage.mediaClients = clients
	flight.storage.mediaRanges = make(chan struct{}, parts)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), mediaHeadKey{}, true))
	flight.cancel = cancel
	go func() {
		body, err := flight.storage.readFrameBody(ctx, "item", frame)
		flight.done <- mediaTestResult{body, err}
	}()
	return flight
}

func (f *mediaTestSplitFlight) awaitBodies(t *testing.T) []*mediaTestGatedBody {
	t.Helper()
	bodies := make([]*mediaTestGatedBody, len(f.bodies))
	for i := range bodies {
		bodies[i] = <-f.bodies[i]
		<-bodies[i].started
	}
	synctest.Wait()
	if len(f.storage.mediaRanges) != len(bodies) || len(f.storage.inflight) != 0 || len(f.storage.reserved) != 0 {
		t.Fatalf("split ranges did not acquire only their own permits: media=%d inflight=%d reserved=%d", len(f.storage.mediaRanges), len(f.storage.inflight), len(f.storage.reserved))
	}
	mediaTestAssertPending(t, f.done)
	return bodies
}

func TestDriveMediaRegressionSplitAssembly(t *testing.T) {
	for _, tc := range []struct {
		name       string
		fullBody   bool
		alteration string
	}{
		{name: "exact_206"},
		{name: "mixed_200_and_206", fullBody: true},
		{name: "crc_corruption", fullBody: true, alteration: "corruption"},
		{name: "mixed_versions", fullBody: true, alteration: "versions"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				payload := mediaTestPayload(3*driveMediaPart + 3)
				frame := newSlotFrame(1, slotMedia, payload)
				width := (len(payload) + driveMediaLanes - 1) / driveMediaLanes
				if len(payload)%driveMediaLanes == 0 {
					t.Fatal("fixture must have an uneven final range")
				}
				sources := make([][]byte, driveMediaLanes)
				for i := range sources {
					sources[i] = payload
				}
				switch tc.alteration {
				case "corruption":
					corrupt := bytes.Clone(payload)
					corrupt[2*width+11] ^= 0x80
					sources[2] = corrupt
				case "versions":
					other := bytes.Clone(payload)
					for i := range other {
						other[i] ^= 0x80
					}
					sources[1], sources[3] = other, other
				}
				assembled := make([]byte, len(payload))
				for i, source := range sources {
					start, end := i*width, min((i+1)*width, len(payload))
					copy(assembled[start:end], source[start:end])
				}
				if got, want := frame.holds(assembled), tc.alteration == ""; got != want {
					t.Fatalf("checksum fixture matches = %v, want %v", got, want)
				}

				flight := startMediaTestSplit(t, frame, sources, tc.fullBody, -1)
				defer flight.cancel()
				bodies := flight.awaitBodies(t)
				for i := len(bodies) - 1; i >= 0; i-- {
					close(flight.releases[i])
					synctest.Wait()
					mediaTestAssertBody(t, bodies[i], false)
					flight.storage.readHedgeMu.Lock()
					completed := flight.storage.mediaHedgeCount
					flight.storage.readHedgeMu.Unlock()
					if want := len(bodies) - i; completed != want {
						t.Fatalf("completed ranges = %d, want %d after releasing range %d", completed, want, i)
					}
					if i > 0 {
						mediaTestAssertPending(t, flight.done)
					}
				}
				res := <-flight.done
				if tc.alteration != "" {
					if res.body != nil || res.err == nil || !strings.Contains(res.err.Error(), "checksum") {
						t.Fatalf("invalid assembled data was not rejected: bytes=%d err=%v", len(res.body), res.err)
					}
				} else if res.err != nil || !bytes.Equal(res.body, payload) {
					t.Fatalf("out-of-order ranges changed the payload: bytes=%d err=%v", len(res.body), res.err)
				}
				synctest.Wait()
				mediaTestAssertNoPermits(t, flight.storage)
				for i := range flight.calls {
					if got := flight.calls[i].Load(); got != 1 {
						t.Errorf("range %d fetched %d times, want 1", i, got)
					}
				}
			})
		})
	}
}

func TestDriveMediaRegressionSplitCleanup(t *testing.T) {
	for _, invalidRange := range []int{-1, 2} {
		name := "caller_cancellation"
		if invalidRange >= 0 {
			name = "invalid_range_cancels_siblings"
		}
		t.Run(name, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				payload := mediaTestPayload(3*driveMediaPart + 3)
				sources := make([][]byte, driveMediaLanes)
				for i := range sources {
					sources[i] = payload
				}
				flight := startMediaTestSplit(t, newSlotFrame(1, slotMedia, payload), sources, false, invalidRange)
				defer flight.cancel()
				bodies := flight.awaitBodies(t)
				if invalidRange >= 0 {
					close(flight.releases[invalidRange])
				} else {
					flight.cancel()
				}
				res := <-flight.done
				if res.body != nil || res.err == nil {
					t.Fatalf("failed split returned data or no error: bytes=%d err=%v", len(res.body), res.err)
				}
				if invalidRange < 0 && res.err != context.Canceled {
					t.Errorf("cancellation returned %v, want context.Canceled", res.err)
				}
				if invalidRange >= 0 && !strings.Contains(res.err.Error(), "unexpected offset or length") {
					t.Errorf("invalid range returned an unrelated error: %v", res.err)
				}
				synctest.Wait()
				for i, body := range bodies {
					mediaTestAssertBody(t, body, i != invalidRange)
					if got := flight.calls[i].Load(); got != 1 {
						t.Errorf("range %d fetched %d times, want 1", i, got)
					}
				}
				mediaTestAssertNoPermits(t, flight.storage)
			})
		})
	}
}

func TestDriveMediaRegressionRangeValidation(t *testing.T) {
	payload := mediaTestPayload(29)
	start, end := 7, 19
	exact := fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(payload))
	for _, tc := range []struct {
		name         string
		status       int
		contentRange string
		body         []byte
		valid        bool
	}{
		{"exact_206", http.StatusPartialContent, exact, payload[start:end], true},
		{"full_200", http.StatusOK, "", payload, true},
		{"wrong_start", http.StatusPartialContent, "bytes 8-18/29", payload[start:end], false},
		{"wrong_end", http.StatusPartialContent, "bytes 7-19/29", payload[start:end], false},
		{"wrong_total", http.StatusPartialContent, "bytes 7-18/30", payload[start:end], false},
		{"missing_content_range", http.StatusPartialContent, "", payload[start:end], false},
		{"short_206", http.StatusPartialContent, exact, payload[start : end-1], false},
		{"long_206", http.StatusPartialContent, exact, payload[start : end+1], false},
		{"short_200", http.StatusOK, "", payload[:len(payload)-1], false},
		{"long_200", http.StatusOK, "", append(bytes.Clone(payload), 0), false},
		{"404_with_matching_range", http.StatusNotFound, exact, payload[start:end], false},
		{"416_with_matching_range", http.StatusRequestedRangeNotSatisfiable, exact, payload[start:end], false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				release := make(chan struct{})
				close(release)
				var body *mediaTestGatedBody
				var calls atomic.Int32
				s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
					calls.Add(1)
					mediaTestCheckRequest(t, r, start, end)
					body = newMediaTestGatedBody(r.Context(), tc.body, release)
					resp := memoryResponse(tc.status, "")
					resp.Body = body
					resp.Header.Set("Content-Range", tc.contentRange)
					return resp, nil
				}))
				s.mediaRanges = make(chan struct{}, 1)
				r := driveMediaRequest{start: start, end: end, size: len(payload), client: s.client}
				got, err := s.readMediaRange(context.Background(), s.filesURL+"/item?alt=media&supportsAllDrives=true", r)
				if tc.valid {
					if err != nil || !bytes.Equal(got, payload[start:end]) {
						t.Errorf("valid range rejected or changed: bytes=%d err=%v", len(got), err)
					}
				} else if err == nil || got != nil {
					t.Errorf("invalid response returned data or no error: bytes=%d err=%v", len(got), err)
				}
				synctest.Wait()
				mediaTestAssertBody(t, body, false)
				mediaTestAssertNoPermits(t, s)
				if got := calls.Load(); got != 1 {
					t.Errorf("range fetched %d times, want 1", got)
				}
			})
		})
	}
}

type mediaTestHedgeFlight struct {
	storage  *driveStorage
	payload  []byte
	cancel   context.CancelFunc
	done     chan mediaTestResult
	releases [2]chan struct{}
	bodies   [2]chan *mediaTestGatedBody
	calls    [2]atomic.Int32
}

func startMediaTestHedge(t *testing.T, statuses [2]int) *mediaTestHedgeFlight {
	t.Helper()
	flight := &mediaTestHedgeFlight{payload: mediaTestPayload(257), done: make(chan mediaTestResult, 1)}
	var clients [2]*http.Client
	for i := range clients {
		flight.releases[i] = make(chan struct{})
		flight.bodies[i] = make(chan *mediaTestGatedBody, driveMaxAttempts)
		clients[i] = &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
			flight.calls[i].Add(1)
			mediaTestCheckRequest(t, r, 0, len(flight.payload))
			body := newMediaTestGatedBody(r.Context(), flight.payload, flight.releases[i])
			flight.bodies[i] <- body
			resp := memoryResponse(statuses[i], "")
			resp.Body = body
			resp.Header.Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(flight.payload)-1, len(flight.payload)))
			return resp, nil
		})}
	}
	flight.storage = memoryDrive(clients[0].Transport)
	flight.storage.mediaClients = []*http.Client{clients[0]}
	flight.storage.bulkClient = clients[1]
	flight.storage.mediaRanges = make(chan struct{}, 1)
	flight.storage.mediaRescue = make(chan struct{}, 1)
	flight.storage.mediaHedgeTokens = 1
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), mediaHeadKey{}, true))
	flight.cancel = cancel
	go func() {
		body, err := flight.storage.readFrameBody(ctx, "item", newSlotFrame(1, slotMedia, flight.payload))
		flight.done <- mediaTestResult{body, err}
	}()
	return flight
}

func (f *mediaTestHedgeFlight) awaitBody(t *testing.T, lane int) *mediaTestGatedBody {
	t.Helper()
	body := <-f.bodies[lane]
	<-body.started
	synctest.Wait()
	if len(f.storage.mediaRanges) != 1 || len(f.storage.mediaRescue) != lane || len(f.storage.inflight) != 0 || len(f.storage.reserved) != 0 {
		t.Fatalf("unexpected active hedge permits: media=%d rescue=%d inflight=%d reserved=%d", len(f.storage.mediaRanges), len(f.storage.mediaRescue), len(f.storage.inflight), len(f.storage.reserved))
	}
	mediaTestAssertPending(t, f.done)
	return body
}

func (f *mediaTestHedgeFlight) assertAccounting(t *testing.T, hedged bool, completed int) {
	t.Helper()
	wantHedges := int32(0)
	wantTokens := 1
	if hedged {
		wantHedges, wantTokens = 1, 0
	}
	if primary, hedge := f.calls[0].Load(), f.calls[1].Load(); primary != 1 || hedge != wantHedges {
		t.Errorf("request counts: primary=%d hedge=%d, want 1/%d", primary, hedge, wantHedges)
	}
	f.storage.readHedgeMu.Lock()
	tokens, count := f.storage.mediaHedgeTokens, f.storage.mediaHedgeCount
	f.storage.readHedgeMu.Unlock()
	if tokens != wantTokens || count != completed {
		t.Errorf("hedge accounting: tokens=%d completed=%d, want %d/%d", tokens, count, wantTokens, completed)
	}
	mediaTestAssertNoPermits(t, f.storage)
}

func TestDriveMediaRegressionHedgeWinnerCleanup(t *testing.T) {
	for _, tc := range []struct {
		name   string
		hedged bool
		winner int
	}{
		{"primary_before_hedge", false, 0},
		{"primary_after_hedge", true, 0},
		{"hedge_wins", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				flight := startMediaTestHedge(t, [2]int{http.StatusPartialContent, http.StatusPartialContent})
				defer flight.cancel()
				bodies := []*mediaTestGatedBody{flight.awaitBody(t, 0)}
				if tc.hedged {
					bodies = append(bodies, flight.awaitBody(t, 1))
					if mediaTestClosed(bodies[0].canceled) {
						t.Fatal("launching a hedge canceled the primary")
					}
				}
				close(flight.releases[tc.winner])
				res := <-flight.done
				if res.err != nil || !bytes.Equal(res.body, flight.payload) {
					t.Fatalf("winner returned invalid data: bytes=%d err=%v", len(res.body), res.err)
				}
				synctest.Wait()
				for i, body := range bodies {
					mediaTestAssertBody(t, body, i != tc.winner)
				}
				flight.assertAccounting(t, tc.hedged, 1)
			})
		})
	}
}

func TestDriveMediaRegressionHedgeCancellationCleanup(t *testing.T) {
	for _, hedged := range []bool{false, true} {
		name := "before_hedge"
		if hedged {
			name = "after_hedge"
		}
		t.Run(name, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				flight := startMediaTestHedge(t, [2]int{http.StatusPartialContent, http.StatusPartialContent})
				defer flight.cancel()
				bodies := []*mediaTestGatedBody{flight.awaitBody(t, 0)}
				if hedged {
					bodies = append(bodies, flight.awaitBody(t, 1))
				}
				flight.cancel()
				res := <-flight.done
				if res.body != nil || res.err != context.Canceled {
					t.Fatalf("canceled read returned bytes=%d err=%v, want nil/context.Canceled", len(res.body), res.err)
				}
				synctest.Wait()
				for _, body := range bodies {
					mediaTestAssertBody(t, body, true)
				}
				flight.assertAccounting(t, hedged, 0)
			})
		})
	}
}

func TestDriveMediaRegressionHedgeBothErrorsCleanup(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		flight := startMediaTestHedge(t, [2]int{http.StatusNotFound, http.StatusNotFound})
		defer flight.cancel()
		primary, hedge := flight.awaitBody(t, 0), flight.awaitBody(t, 1)
		close(flight.releases[0])
		synctest.Wait()
		mediaTestAssertBody(t, primary, false)
		mediaTestAssertPending(t, flight.done)
		if mediaTestClosed(hedge.canceled) || len(flight.storage.mediaRanges) != 0 || len(flight.storage.mediaRescue) != 1 {
			t.Fatal("primary failure canceled the pending hedge or leaked its permit")
		}
		close(flight.releases[1])
		res := <-flight.done
		if res.body != nil || res.err != errNotFound {
			t.Fatalf("failed requests returned bytes=%d err=%v, want nil/errNotFound", len(res.body), res.err)
		}
		synctest.Wait()
		mediaTestAssertBody(t, hedge, false)
		flight.assertAccounting(t, true, 0)
	})
}
