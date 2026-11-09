package xdrive

import (
	"context"
	"testing"
	"time"
)

func TestTimingPercentiles(t *testing.T) {
	v := []time.Duration{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if got := percentile(v, 0.5); got != 6 {
		t.Fatalf("p50=%v want 6", got)
	}
	if got := percentile(v, 0.95); got != 10 {
		t.Fatalf("p95=%v want 10", got)
	}
	if got := percentile(v, 1); got != 10 {
		t.Fatalf("max=%v want 10", got)
	}
	if got := percentile(nil, 0.5); got != -1 {
		t.Fatalf("empty percentile=%v want -1", got)
	}
}

func TestTimingAggregatorKindsAndReset(t *testing.T) {
	a := newTimingAggregator()
	defer a.stop()
	for i := 0; i < 5; i++ {
		a.record(kindSlotInline, timingSample{total: time.Duration(i+1) * time.Millisecond, newConn: i == 0})
	}
	for i := 0; i < 3; i++ {
		a.record(kindBodyRead, timingSample{total: time.Duration(i+1) * time.Second})
	}
	a.mu.Lock()
	inline, body := a.n[kindSlotInline], a.n[kindBodyRead]
	conns := a.conns[kindSlotInline]
	a.mu.Unlock()
	if inline != 5 || body != 3 {
		t.Fatalf("kinds not separated: inline=%d body=%d", inline, body)
	}
	if conns != 1 {
		t.Fatalf("newconns=%d want 1", conns)
	}
	a.flush()
	a.mu.Lock()
	defer a.mu.Unlock()
	for k := 0; k < kindCount; k++ {
		if a.n[k] != 0 || a.conns[k] != 0 {
			t.Fatalf("kind %d not reset after flush", k)
		}
	}
}

func TestTimingAggregatorRingBounds(t *testing.T) {
	a := newTimingAggregator()
	defer a.stop()
	for i := 0; i < timingRing*2; i++ {
		a.record(kindDelete, timingSample{total: time.Millisecond})
	}
	a.mu.Lock()
	n := a.n[kindDelete]
	a.mu.Unlock()
	if n != timingRing {
		t.Fatalf("ring grew to %d, want bounded at %d", n, timingRing)
	}
}

func TestClassifyDriveRequest(t *testing.T) {
	ctx := context.Background()
	lane := context.WithValue(ctx, laneKey{}, true)
	cases := []struct {
		method, target, ctype string
		body                  []byte
		ctx                   context.Context
		want                  int
	}{
		{"POST", "https://x/files?fields=id", "application/json", nil, ctx, kindCreate},
		{"PATCH", "https://x/files/id", "application/json", []byte(`{"description":"WERSSVZ"}`), ctx, kindSlotInline},
		{"PATCH", "https://x/files/id", "multipart/related; boundary=" + driveBoundary, nil, ctx, kindSlotMedia},
		{"PATCH", "https://x/files/id", "application/json", []byte(`{"description":"12"}`), ctx, kindWatermark},
		{"GET", "https://x/files/id?fields=description", "", nil, lane, kindHeadRead},
		{"GET", "https://x/files/id?fields=description", "", nil, ctx, kindOtherRead},
		{"GET", "https://x/files/id?alt=media", "", nil, ctx, kindBodyRead},
		{"GET", "https://x/files?q=name", "", nil, ctx, kindList},
		{"DELETE", "https://x/files/id", "", nil, ctx, kindDelete},
	}
	for _, c := range cases {
		if got := classifyDriveRequest(c.ctx, c.method, c.target, c.ctype, c.body); got != c.want {
			t.Errorf("%s %s -> %d want %d", c.method, c.target, got, c.want)
		}
	}
}
