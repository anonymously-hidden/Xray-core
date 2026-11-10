package xdrive

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func hedgeStorage(t *testing.T, h http.HandlerFunc, hedge time.Duration) (*driveStorage, *timingAggregator) {
	t.Helper()
	srv := httptest.NewTLSServer(h)
	t.Cleanup(srv.Close)
	mk := func() *http.Client {
		return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	}
	agg := newTimingAggregator()
	t.Cleanup(agg.stop)
	s := &driveStorage{
		driveShared:     &driveShared{token: "t", tokenExpiry: time.Now().Add(time.Hour), filesURL: srv.URL, timing: agg, backoff: driveInitialBackoff},
		client:          mk(),
		bulkClient:      mk(),
		inflight:        make(chan struct{}, 8),
		reserved:        make(chan struct{}, 8),
		readHedge:       hedge,
		readHedgeTokens: 3,
	}
	return s, agg
}

func hedgeCounts(agg *timingAggregator) (int, int) {
	agg.mu.Lock()
	defer agg.mu.Unlock()
	return agg.hedges[kindOtherRead], agg.won[kindOtherRead]
}

func TestReadHedgeWinsOnSlowGet(t *testing.T) {
	var calls atomic.Int64
	s, agg := hedgeStorage(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		w.Write([]byte(`{"description":"ok"}`))
	}, 50*time.Millisecond)
	desc, err := s.readDescription(context.Background(), "id")
	if err != nil || desc != "ok" {
		t.Fatalf("hedged read gave %q, %v", desc, err)
	}
	if h, won := hedgeCounts(agg); h != 1 || won != 1 {
		t.Fatalf("hedges/won = %d/%d, want 1/1", h, won)
	}
}

func TestReadHedge404IsAnAnswer(t *testing.T) {
	var calls atomic.Int64
	s, agg := hedgeStorage(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}, 50*time.Millisecond)
	if _, err := s.readDescription(context.Background(), "id"); err != errNotFound {
		t.Fatalf("hedge 404 gave %v, want errNotFound", err)
	}
	if _, won := hedgeCounts(agg); won != 1 {
		t.Fatalf("404 hedge win = %d, want 1", won)
	}
}

func TestReadHedgeUsesTwoConnections(t *testing.T) {
	var mu sync.Mutex
	var addrs []string
	var calls atomic.Int64
	s, _ := hedgeStorage(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		addrs = append(addrs, r.RemoteAddr)
		mu.Unlock()
		if calls.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		w.Write([]byte(`{"description":"ok"}`))
	}, 50*time.Millisecond)
	if _, err := s.readDescription(context.Background(), "id"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(addrs) < 2 || addrs[0] == addrs[1] {
		t.Fatalf("primary and hedge used the same connection: %v", addrs)
	}
}

func TestReadNoHedgeWhenFast(t *testing.T) {
	s, agg := hedgeStorage(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"description":"ok"}`))
	}, 50*time.Millisecond)
	if _, err := s.readDescription(context.Background(), "id"); err != nil {
		t.Fatal(err)
	}
	if h, _ := hedgeCounts(agg); h != 0 {
		t.Fatalf("a fast read hedged %d times", h)
	}
}

func TestReadNoHedgeWhenDisabledOrCooldownOrRateLimit(t *testing.T) {
	for _, mode := range []string{"zero", "cooldown", "ratelimit"} {
		var calls atomic.Int64
		s, agg := hedgeStorage(t, func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusBadRequest)
		}, 50*time.Millisecond)
		switch mode {
		case "zero":
			s.readHedge = 0
		case "cooldown":
			s.coolMu.Lock()
			s.coolUntil = time.Now().Add(100 * time.Millisecond)
			s.coolMu.Unlock()
		case "ratelimit":
			s.coolMu.Lock()
			s.lastRateLimit = time.Now()
			s.coolMu.Unlock()
		}
		if _, err := s.readDescription(context.Background(), "id"); err == nil {
			t.Fatalf("%s: expected the 400 error", mode)
		}
		if h, _ := hedgeCounts(agg); h != 0 || calls.Load() != 1 {
			t.Fatalf("%s: hedged %d times with %d calls, want 0/1", mode, h, calls.Load())
		}
	}
}

func TestReadHedgeAfterTimeoutStillHedges(t *testing.T) {
	var calls atomic.Int64
	s, agg := hedgeStorage(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		w.Write([]byte(`{"description":"ok"}`))
	}, 50*time.Millisecond)
	// A prior attempt timeout bumps lastThrottle but must not disable hedges.
	s.coolMu.Lock()
	s.lastThrottle = time.Now()
	s.coolMu.Unlock()
	if _, err := s.readDescription(context.Background(), "id"); err != nil {
		t.Fatal(err)
	}
	if h, won := hedgeCounts(agg); h != 1 || won != 1 {
		t.Fatalf("after a timeout hedges/won = %d/%d, want 1/1", h, won)
	}
}

func TestReadHedgeTokenBucket(t *testing.T) {
	s, _ := hedgeStorage(t, func(w http.ResponseWriter, r *http.Request) {}, time.Millisecond)
	for i := 0; i < 3; i++ {
		if !s.takeHedgeToken() {
			t.Fatalf("token %d missing, want 3 at start", i+1)
		}
	}
	if s.takeHedgeToken() {
		t.Fatal("empty bucket granted a token")
	}
	for i := 0; i < 20; i++ {
		s.noteDescriptionRead()
	}
	if !s.takeHedgeToken() {
		t.Fatal("20 reads did not refill a token")
	}
	for i := 0; i < 20*10; i++ {
		s.noteDescriptionRead()
	}
	s.readHedgeMu.Lock()
	tok := s.readHedgeTokens
	s.readHedgeMu.Unlock()
	if tok > 6 {
		t.Fatalf("bucket over capacity: %d", tok)
	}
}

func TestReadHedgeStalledAmongFast(t *testing.T) {
	var calls atomic.Int64
	s, agg := hedgeStorage(t, func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n == 20 {
			<-r.Context().Done()
			return
		}
		if n == 21 {
			w.Write([]byte(`{"description":"ok"}`))
			return
		}
		w.Write([]byte(`{"description":"ok"}`))
	}, 50*time.Millisecond)
	for i := 0; i < 19; i++ {
		if _, err := s.readDescription(context.Background(), "id"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.readDescription(context.Background(), "id"); err != nil {
		t.Fatal(err)
	}
	if h, _ := hedgeCounts(agg); h != 1 {
		t.Fatalf("the stalled read among fast ones hedged %d times, want 1", h)
	}
}

func TestReadHedgeThreeStalledAtOnce(t *testing.T) {
	var calls atomic.Int64
	s, agg := hedgeStorage(t, func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n <= 3 {
			<-r.Context().Done()
			return
		}
		w.Write([]byte(`{"description":"ok"}`))
	}, 50*time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.readDescription(context.Background(), "id"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if h, _ := hedgeCounts(agg); h != 3 {
		t.Fatalf("three concurrent stalled reads hedged %d times, want 3", h)
	}
}

func TestReadHedgeEmptyBucketWaitsForRefill(t *testing.T) {
	var calls atomic.Int64
	s, agg := hedgeStorage(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusBadRequest)
	}, 50*time.Millisecond)
	s.readHedgeMu.Lock()
	s.readHedgeTokens = 0
	s.readHedgeMu.Unlock()
	if _, err := s.readDescription(context.Background(), "id"); err == nil {
		t.Fatal("expected the 400 error")
	}
	if h, _ := hedgeCounts(agg); h != 0 || calls.Load() != 1 {
		t.Fatalf("empty bucket hedged %d times with %d calls, want 0/1", h, calls.Load())
	}
}

func TestReadRetriesWithBackoff(t *testing.T) {
	var mu sync.Mutex
	var times []time.Time
	var calls atomic.Int64
	s, _ := hedgeStorage(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte(`{"description":"ok"}`))
	}, 0) // hedge disabled; exercise do() retries
	start := time.Now()
	if _, err := s.readDescription(context.Background(), "id"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("made %d calls, want 3 (503,503,200)", calls.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(times) != 3 {
		t.Fatalf("recorded %d attempts", len(times))
	}
	if g := times[1].Sub(times[0]); g < 90*time.Millisecond {
		t.Fatalf("first retry gap %v too tight (no backoff)", g)
	}
	if g := times[2].Sub(times[1]); g < 180*time.Millisecond {
		t.Fatalf("second retry gap %v too tight (backoff must grow)", g)
	}
	if time.Since(start) < 300*time.Millisecond {
		t.Fatal("retries ran in a tight loop")
	}
}
