package xdrive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestMediaHedgeDelayUsesSizeClassMedian(t *testing.T) {
	s := &driveStorage{driveShared: &driveShared{}}
	small := driveMediaRequest{end: driveMediaPart}
	large := driveMediaRequest{end: driveMediaPart + 1}
	for i := 0; i < rangeLatencyMin-1; i++ {
		s.mediaLatency[0].note(time.Second)
	}
	if got := s.mediaHedgeDelay(small); got != slotBodyHedge {
		t.Fatalf("cold threshold = %v, want %v", got, slotBodyHedge)
	}
	s.mediaLatency[0].note(time.Second)
	if got := s.mediaHedgeDelay(small); got != 1300*time.Millisecond {
		t.Fatalf("threshold = %v, want 1.3 times p50", got)
	}
	if got := s.mediaHedgeDelay(large); got != slotBodyHedge {
		t.Fatalf("small samples changed large-range threshold: %v", got)
	}
	for i := 0; i < rangeLatencyRing; i++ {
		s.mediaLatency[0].note(time.Millisecond)
		s.mediaLatency[1].note(10 * time.Second)
	}
	if got := s.mediaHedgeDelay(small); got != mediaHedgeFloor {
		t.Fatalf("floor = %v, want %v", got, mediaHedgeFloor)
	}
	if got := s.mediaHedgeDelay(large); got != slotBodyHedge {
		t.Fatalf("cap = %v, want %v", got, slotBodyHedge)
	}
	peer := &driveStorage{driveShared: s.driveShared}
	if got := peer.mediaHedgeDelay(small); got != mediaHedgeFloor {
		t.Fatalf("sessions did not share the rolling estimate: %v", got)
	}
}

func TestMediaLatencyAdmitsOnlyValidatedFirstPrimaryAttempts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		attempt  int
		rescue   bool
		replayed bool
		known    bool
		err      error
		want     int
	}{
		{name: "partial", status: http.StatusPartialContent, known: true, want: 1},
		{name: "full", status: http.StatusOK, known: true, want: 1},
		{name: "rescue", status: http.StatusPartialContent, known: true, rescue: true},
		{name: "retry", status: http.StatusPartialContent, known: true, attempt: 1},
		{name: "replay", status: http.StatusPartialContent, known: true, replayed: true},
		{name: "failure", status: http.StatusPartialContent, known: true, err: io.ErrUnexpectedEOF},
		{name: "rate_limit", status: http.StatusTooManyRequests, known: true},
		{name: "not_found", status: http.StatusNotFound, known: true},
		{name: "undispatched", status: http.StatusPartialContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &driveStorage{driveShared: &driveShared{}}
			r := &driveMediaRequest{end: driveMediaPart, rescue: tc.rescue}
			sample := timingSample{total: 20 * time.Second, dispatch: time.Second, hasDispatch: tc.known}
			s.observeMediaLatency(r, sample, tc.status, tc.attempt, tc.replayed, tc.err)
			if got := s.mediaLatency[0].n; got != tc.want {
				t.Fatalf("samples = %d, want %d", got, tc.want)
			}
			if tc.want == 1 && s.mediaLatency[0].ring[0] != time.Second {
				t.Fatal("local admission time contaminated the sample")
			}
		})
	}
}

func TestMediaLatencyRingConcurrentAndBounded(t *testing.T) {
	var latency rangeLatency
	latency.note(0)
	latency.note(-time.Second)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				latency.note(time.Second)
				if median, ready := latency.median(); ready && median != time.Second {
					t.Errorf("concurrent median = %v", median)
				}
			}
		})
	}
	wg.Wait()
	if latency.n != rangeLatencyRing || latency.pos < 0 || latency.pos >= rangeLatencyRing {
		t.Fatalf("ring grew or has an invalid cursor: n=%d pos=%d", latency.n, latency.pos)
	}
}

func TestMediaLatencyMeasuresActualDispatchAndRejectsInvalidRange(t *testing.T) {
	for _, valid := range []bool{false, true} {
		t.Run(fmt.Sprint(valid), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan struct{}, 1)
				s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
					entered <- struct{}{}
					time.Sleep(time.Second)
					response := memoryResponse(http.StatusPartialContent, "data")
					if valid {
						response.Header.Set("Content-Range", "bytes 0-3/4")
					}
					return response, nil
				}))
				s.mediaRanges = make(chan struct{}, 1)
				s.mediaRanges <- struct{}{}
				r := &driveMediaRequest{end: 4, size: 4, client: s.client}
				ctx := context.WithValue(context.Background(), driveMediaKey{}, r)
				done := make(chan error, 1)
				go func() { _, _, err := s.do(ctx, http.MethodGet, s.filesURL+"/item?alt=media", "", nil); done <- err }()
				synctest.Wait()
				time.Sleep(5 * time.Second)
				if len(entered) != 0 {
					t.Fatal("primary dispatched before its permit was available")
				}
				<-s.mediaRanges
				<-entered
				if err := <-done; (err == nil) != valid {
					t.Fatalf("range validation error = %v, valid = %v", err, valid)
				}
				synctest.Wait()
				want := 0
				if valid {
					want = 1
				}
				if got := s.mediaLatency[0].n; got != want {
					t.Fatalf("samples = %d, want %d", got, want)
				}
				if valid && s.mediaLatency[0].ring[0] != time.Second {
					t.Fatalf("dispatch latency = %v, want 1s, excluding 5s admission", s.mediaLatency[0].ring[0])
				}
			})
		})
	}
}

type adaptiveDelivery struct {
	mu      sync.Mutex
	next    int
	waitAt  time.Time
	changed chan struct{}
}

func (d *adaptiveDelivery) snapshot() (int, <-chan struct{}, time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.next, d.changed, d.waitAt
}

func (d *adaptiveDelivery) set(next int, waitAt time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.next, d.waitAt = next, waitAt
	close(d.changed)
	d.changed = make(chan struct{})
}

type adaptiveRangeFlight struct {
	s           *driveStorage
	delivery    *adaptiveDelivery
	promoted    chan struct{}
	primary     chan *http.Request
	rescue      chan *http.Request
	release     chan struct{}
	done        chan mediaTestResult
	cancel      context.CancelFunc
	primaryGets atomic.Int32
	rescueGets  atomic.Int32
}

func startAdaptiveRange(t *testing.T, tracked, head bool, start int, tokens int, queuedRescue, failRescue bool) *adaptiveRangeFlight {
	t.Helper()
	f := &adaptiveRangeFlight{
		delivery: &adaptiveDelivery{waitAt: time.Now(), changed: make(chan struct{})},
		promoted: make(chan struct{}), primary: make(chan *http.Request, 1), rescue: make(chan *http.Request, 1),
		release: make(chan struct{}), done: make(chan mediaTestResult, 1),
	}
	data := driveStreamTestData(2 * driveStreamBlockBytes)
	stream := &driveRangeStream{frame: driveStreamTestFrame(data), emit: func(int, []byte) error { return nil }}
	f.s = memoryDrive(roundTrip(func(request *http.Request) (*http.Response, error) {
		media := request.Context().Value(driveMediaKey{}).(*driveMediaRequest)
		body := io.NopCloser(strings.NewReader(string(data[media.start:media.end])))
		if media.rescue {
			f.rescueGets.Add(1)
			f.rescue <- request
			if failRescue {
				return nil, io.ErrUnexpectedEOF
			}
		} else {
			f.primaryGets.Add(1)
			f.primary <- request
			body = &driveStreamTestGatedBody{
				ctx: request.Context(), data: data[media.start:media.end],
				release: f.release, closed: make(chan struct{}),
			}
		}
		return driveStreamTestResponse(request, http.StatusPartialContent,
			fmt.Sprintf("bytes %d-%d/%d", media.start, media.end-1, len(data)), int64(media.end-media.start), body), nil
	}))
	f.s.bulkClient = &http.Client{Transport: f.s.client.Transport}
	f.s.mediaRanges = make(chan struct{}, 1)
	f.s.mediaRescue = make(chan struct{}, 1)
	f.s.mediaHedgeTokens = tokens
	for range rangeLatencyMin {
		f.s.mediaLatency[0].note(time.Second)
	}
	if queuedRescue {
		f.s.mediaRescue <- struct{}{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	ctx = context.WithValue(ctx, mediaPromotionKey{}, (<-chan struct{})(f.promoted))
	if head {
		close(f.promoted)
	}
	r := driveMediaRequest{start: start, end: start + driveStreamBlockBytes, size: len(data), client: f.s.client}
	if tracked {
		r.stream = stream
		ctx = context.WithValue(ctx, mediaDeliveryKey{}, f.delivery.snapshot)
	}
	go func() {
		body, err := f.s.readMediaRange(ctx, f.s.filesURL+"/item?alt=media", r)
		f.done <- mediaTestResult{body, err}
	}()
	t.Cleanup(func() { cancel(); stream.closeAndWait(); f.s.driveShared.closeWork() })
	return f
}

func TestAdaptiveHeadRangeHedgesAtDispatchDeadlineDespiteWireProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := startAdaptiveRange(t, true, true, 0, 1, false, false)
		request := <-f.primary
		synctest.Wait()
		time.Sleep(1300*time.Millisecond - time.Nanosecond)
		request.Context().Value(readProgressKey{}).(*driveReadProgress).progress()
		synctest.Wait()
		if f.rescueGets.Load() != 0 {
			t.Fatal("hedge fired before 1.3 times the live median")
		}
		time.Sleep(time.Nanosecond)
		<-f.rescue
		if result := <-f.done; result.err != nil {
			t.Fatal(result.err)
		}
		synctest.Wait()
		if f.primaryGets.Load() != 1 || f.rescueGets.Load() != 1 || f.s.mediaHedgeTokens != 0 {
			t.Fatal("adaptive recovery exceeded one primary, one rescue, or one budget token")
		}
	})
}

func TestAdaptiveRangeHedgeOnlyAfterHeadPromotion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := startAdaptiveRange(t, true, false, 0, 1, false, false)
		<-f.primary
		synctest.Wait()
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if f.rescueGets.Load() != 0 || f.s.mediaHedgeTokens != 1 {
			t.Fatal("speculative tail armed or spent head recovery")
		}
		at := time.Now()
		close(f.promoted)
		<-f.rescue
		if time.Now() != at {
			t.Fatal("promotion restarted the dispatch-age deadline")
		}
		if result := <-f.done; result.err != nil {
			t.Fatal(result.err)
		}
	})
}

func TestAdaptiveRangeHedgeWaitsForRequiredMissingBlock(t *testing.T) {
	for _, available := range []bool{false, true} {
		t.Run(fmt.Sprint(available), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := driveStreamBlockBytes
				if available {
					start = 0
				}
				f := startAdaptiveRange(t, true, true, start, 1, false, false)
				if available {
					f.delivery.set(0, time.Time{})
				}
				<-f.primary
				synctest.Wait()
				time.Sleep(3 * time.Second)
				synctest.Wait()
				if f.rescueGets.Load() != 0 || f.s.mediaHedgeTokens != 1 {
					t.Fatal("an available block or later range spent recovery credit")
				}
				at := time.Now()
				f.delivery.set(start, at)
				<-f.rescue
				if time.Now() != at {
					t.Fatal("the new missing-block wait replaced the original dispatch age")
				}
				if result := <-f.done; result.err != nil {
					t.Fatal(result.err)
				}
			})
		})
	}
}

func TestAdaptiveQueuedRescueRechecksDeliveryFrontier(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := startAdaptiveRange(t, true, true, 0, 1, true, false)
		<-f.primary
		synctest.Wait()
		time.Sleep(1300 * time.Millisecond)
		synctest.Wait()
		if f.s.mediaHedgeTokens != 0 || f.rescueGets.Load() != 0 {
			t.Fatal("rescue did not queue behind its existing permit")
		}
		f.delivery.set(0, time.Time{})
		<-f.s.mediaRescue
		synctest.Wait()
		if f.rescueGets.Load() != 0 {
			t.Fatal("queued rescue dispatched after the blocking block became available")
		}
		f.cancel()
		if result := <-f.done; !errors.Is(result.err, context.Canceled) {
			t.Fatalf("cancellation = %v", result.err)
		}
	})
}

func TestAdaptiveRangeUsesExistingBudgetReplenishment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := startAdaptiveRange(t, true, true, 0, 0, false, false)
		<-f.primary
		synctest.Wait()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if f.rescueGets.Load() != 0 {
			t.Fatal("recovery ignored an empty media budget")
		}
		f.s.readHedgeMu.Lock()
		f.s.mediaHedgeCount = 19
		f.s.readHedgeMu.Unlock()
		f.s.noteMediaRange()
		<-f.rescue
		if result := <-f.done; result.err != nil {
			t.Fatal(result.err)
		}
		synctest.Wait()
		if f.rescueGets.Load() != 1 || f.s.mediaHedgeTokens != 0 {
			t.Fatal("replenishment expanded the existing token budget")
		}
	})
}

func TestAdaptiveRangeRescueHasOneAttemptAndNoRepeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := startAdaptiveRange(t, true, true, 0, 3, false, true)
		<-f.primary
		<-f.rescue
		synctest.Wait()
		for range 5 {
			f.delivery.set(0, time.Now())
			time.Sleep(time.Second)
			synctest.Wait()
		}
		if f.rescueGets.Load() != 1 || f.s.mediaHedgeTokens != 2 {
			t.Fatal("failed rescue retried or repeated a 200-unit GET")
		}
		close(f.release)
		if result := <-f.done; result.err != nil {
			t.Fatal(result.err)
		}
	})
}

func TestAdaptiveRangeDoesNotRacePrimaryReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := startAdaptiveRange(t, true, true, 0, 1, false, false)
		request := <-f.primary
		request.Context().Value(readProgressKey{}).(*driveReadProgress).replay()
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if f.rescueGets.Load() != 0 || f.s.mediaHedgeTokens != 1 {
			t.Fatal("adaptive hedge duplicated primary transport recovery")
		}
		close(f.release)
		if result := <-f.done; result.err != nil {
			t.Fatal(result.err)
		}
	})
}

func TestUnmarkedMediaRangeNeverArmsHeadHedge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := startAdaptiveRange(t, false, false, 0, 1, false, false)
		<-f.primary
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if f.rescueGets.Load() != 0 || f.s.mediaHedgeTokens != 1 {
			t.Fatal("unmarked standalone body GET armed a head-only hedge")
		}
		close(f.release)
		if result := <-f.done; result.err != nil {
			t.Fatal(result.err)
		}
	})
}
