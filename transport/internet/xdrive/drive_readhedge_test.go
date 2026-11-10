package xdrive

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
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
		driveShared: &driveShared{token: "t", tokenExpiry: time.Now().Add(time.Hour), filesURL: srv.URL, timing: agg},
		client:      mk(),
		bulkClient:  mk(),
		inflight:    make(chan struct{}, 8),
		reserved:    make(chan struct{}, 8),
		readHedge:   hedge,
	}
	return s, agg
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
	s.readSinceHedge = 19
	desc, err := s.readDescription(context.Background(), "id")
	if err != nil || desc != "ok" {
		t.Fatalf("hedged read gave %q, %v", desc, err)
	}
	agg.mu.Lock()
	h, won := agg.hedges[kindOtherRead], agg.won[kindOtherRead]
	agg.mu.Unlock()
	if h != 1 || won != 1 {
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
	s.readSinceHedge = 19
	_, err := s.readDescription(context.Background(), "id")
	if err != errNotFound {
		t.Fatalf("hedge 404 gave %v, want errNotFound", err)
	}
	agg.mu.Lock()
	won := agg.won[kindOtherRead]
	agg.mu.Unlock()
	if won != 1 {
		t.Fatalf("404 hedge win = %d, want 1", won)
	}
}

func TestReadNoHedgeWhenFast(t *testing.T) {
	s, agg := hedgeStorage(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"description":"ok"}`))
	}, 50*time.Millisecond)
	if _, err := s.readDescription(context.Background(), "id"); err != nil {
		t.Fatal(err)
	}
	agg.mu.Lock()
	h := agg.hedges[kindOtherRead]
	agg.mu.Unlock()
	if h != 0 {
		t.Fatalf("a fast read hedged %d times", h)
	}
}

func TestReadNoHedgeWhenDisabledOrThrottled(t *testing.T) {
	for _, mode := range []string{"zero", "cooldown", "throttled"} {
		var calls atomic.Int64
		s, agg := hedgeStorage(t, func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}, 50*time.Millisecond)
		switch mode {
		case "zero":
			s.readHedge = 0
		case "cooldown":
			s.coolMu.Lock()
			s.coolUntil = time.Now().Add(100 * time.Millisecond)
			s.coolMu.Unlock()
		case "throttled":
			s.coolMu.Lock()
			s.lastThrottle = time.Now()
			s.coolMu.Unlock()
		}
		if _, err := s.readDescription(context.Background(), "id"); err == nil {
			t.Fatalf("%s: expected the 503 error", mode)
		}
		agg.mu.Lock()
		h := agg.hedges[kindOtherRead]
		agg.mu.Unlock()
		if h != 0 || calls.Load() != 1 {
			t.Fatalf("%s: hedged %d times with %d calls, want 0/1", mode, h, calls.Load())
		}
	}
}

func TestReadHedgeBudget(t *testing.T) {
	s, _ := hedgeStorage(t, func(w http.ResponseWriter, r *http.Request) {}, time.Millisecond)
	for i := 0; i < 19; i++ {
		if s.readHedgeBudget() {
			t.Fatalf("hedge allowed at read %d, want only every 20th", i+1)
		}
	}
	if !s.readHedgeBudget() {
		t.Fatal("20th read did not get a hedge token")
	}
}
