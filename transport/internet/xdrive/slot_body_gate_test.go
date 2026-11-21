package xdrive

import (
	"context"
	"io"
	"testing"
	"testing/synctest"
	"time"
)

func TestBodyGateZeroValueBoundsPrefetchAndNeverHoldsNearFrames(t *testing.T) {
	var g bodyGate
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	far := func() int64 { return 10 }
	near := func() int64 { return 0 }
	for i := 0; i < slotBodyAhead; i++ {
		if !g.acquire(ctx, far, nil) {
			t.Fatalf("acquire %d failed", i)
		}
	}
	// A full window never delays the head or its immediate successors.
	done := make(chan bool, 1)
	go func() { done <- g.acquire(ctx, near, nil) }()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("near acquire failed")
		}
	case <-time.After(time.Second):
		t.Fatal("near frame was held back by a full window")
	}
	// A distant frame waits until a read finishes below the window.
	go func() { done <- g.acquire(ctx, far, nil) }()
	select {
	case <-done:
		t.Fatal("distant frame started beyond the window")
	case <-time.After(100 * time.Millisecond):
	}
	g.release(0, false) // finish the near read, leaving a full prefetch window
	g.release(0, false) // finish a prefetched read, making room for the waiter
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("distant acquire failed after release")
		}
	case <-time.After(time.Second):
		t.Fatal("distant frame did not start after a release")
	}
}

func TestBodyGateStopsWaitingWhenContextEnds(t *testing.T) {
	var g bodyGate
	ctx, cancel := context.WithCancel(context.Background())
	far := func() int64 { return 10 }
	for i := 0; i < slotBodyAhead; i++ {
		g.acquire(ctx, far, nil)
	}
	done := make(chan bool, 1)
	go func() { done <- g.acquire(ctx, far, nil) }()
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("acquire succeeded after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("acquire ignored cancellation")
	}
}

func TestBodyGateWindowFollowsBodyLatency(t *testing.T) {
	var g bodyGate
	ctx := context.Background()
	near := func() int64 { return 0 }
	feed := func(d time.Duration, n int) {
		for i := 0; i < n; i++ {
			g.acquire(ctx, near, nil)
			g.release(d, true)
		}
	}
	feed(time.Second, 40)
	if got := g.window(); got != slotBodyAhead {
		t.Fatalf("window at the floor = %d, want %d", got, slotBodyAhead)
	}
	// Queueing: every read now takes 3x the floor. The window shrinks, but
	// not below the minimum, and not faster than once per base latency.
	for i := 0; i < 5; i++ {
		feed(3*time.Second, 1)
	}
	if got := g.window(); got >= slotBodyAhead || got < bodyGateMin {
		t.Fatalf("window under queueing = %d, want in [%d,%d)", got, bodyGateMin, slotBodyAhead)
	}
	g.mu.Lock()
	g.win.cutAt = time.Time{}
	g.win.limit = bodyGateMin
	g.mu.Unlock()
	feed(time.Second, 3*slotBodyAhead)
	if got := g.window(); got != slotBodyAhead {
		t.Fatalf("window after recovery = %d, want %d", got, slotBodyAhead)
	}
}

func TestLatencyWindowClampsToSmallMaximum(t *testing.T) {
	var l latencyWindow
	for i := 0; i < 2*latencyWindowWarm; i++ {
		l.observe(time.Second, bodyGateMin, 4)
	}
	if got := l.current(4); got != 4 {
		t.Fatalf("window = %d, want the maximum 4 when it is below the minimum", got)
	}
	l.observe(5*time.Second, bodyGateMin, 4)
	if got := l.current(4); got != 4 {
		t.Fatalf("window after a slow read = %d, want 4", got)
	}
}

func TestBodyGateRejectsCanceledAdmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	promoted := make(chan struct{})
	close(promoted)
	for _, tc := range []struct {
		name     string
		distance int64
		promoted <-chan struct{}
	}{
		{name: "free_window", distance: 10},
		{name: "near", distance: bodyGateNear},
		{name: "promoted", distance: 10, promoted: promoted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var g bodyGate
			if g.acquire(ctx, func() int64 { return tc.distance }, tc.promoted) || g.inflight != 0 {
				t.Fatal("canceled read acquired a body permit")
			}
		})
	}
}

func TestBodyGatePromotionInterruptsFullWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var g bodyGate
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		far := func() int64 { return 10 }
		for range slotBodyAhead {
			g.acquire(ctx, far, nil)
		}
		promoted := make(chan struct{})
		done := make(chan bool, 1)
		go func() { done <- g.acquire(ctx, far, promoted) }()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("distant read escaped a full window")
		default:
		}
		at := time.Now()
		close(promoted)
		if !<-done || time.Now() != at {
			t.Fatal("head promotion did not immediately bypass the full window")
		}
		for range slotBodyAhead + 1 {
			g.release(time.Second, false)
		}
		if g.inflight != 0 || g.win.n != 0 {
			t.Fatal("permits leaked or failed reads trained the window")
		}
	})
}

func TestLatencyWindowCutRateAndBounds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l latencyWindow
		for range latencyWindowWarm {
			l.observe(time.Second, bodyGateMin, slotBodyAhead)
		}
		l.observe(3*time.Second, bodyGateMin, slotBodyAhead)
		cut := slotBodyAhead * 3 / 4
		if l.current(slotBodyAhead) != cut {
			t.Fatal("slow sample did not cut the window by a quarter")
		}
		time.Sleep(time.Second - time.Nanosecond)
		l.observe(3*time.Second, bodyGateMin, slotBodyAhead)
		if l.current(slotBodyAhead) != cut {
			t.Fatal("window shrank again within one base latency")
		}
		time.Sleep(time.Nanosecond)
		l.observe(3*time.Second, bodyGateMin, slotBodyAhead)
		if l.current(slotBodyAhead) != cut*3/4 {
			t.Fatal("window did not shrink after one base latency")
		}
		for range 4 {
			time.Sleep(time.Second)
			l.observe(3*time.Second, bodyGateMin, slotBodyAhead)
		}
		if l.current(slotBodyAhead) != bodyGateMin {
			t.Fatal("window did not stop at the minimum")
		}
		l.observe(time.Second, bodyGateMin, slotBodyAhead)
		if l.current(slotBodyAhead) != bodyGateMin+1 {
			t.Fatal("healthy sample did not grow the window by one")
		}
	})
}

func TestSlotWriterLatencyWindowKeepsNormalAndRecoveryLimits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &bulkPermitSlots{memSlots: newMemSlots()}
		w := &slotWriter{
			store: store, table: slotTable{Slots: make([]string, slotBulkDepth)},
			slots: make(chan struct{}, slotBulkInflight), bulkSegments: 2 * slotBulkDepth,
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.writeWindow() != slotBulkInflight {
			t.Fatal("cold window changed existing bulk admission")
		}
		for range latencyWindowWarm {
			w.uplink.observe(time.Second, bodyGateMin, cap(w.slots))
		}
		w.uplink.observe(3*time.Second, bodyGateMin, cap(w.slots))
		if got := w.writeWindow(); got != slotBulkInflight*3/4 {
			t.Fatalf("bulk window = %d, want a quarter reduction", got)
		}
		for range 5 {
			time.Sleep(time.Second)
			w.uplink.observe(3*time.Second, bodyGateMin, cap(w.slots))
		}
		if w.writeWindow() != slotWriteInflight {
			t.Fatal("upload window shrank below normal admission")
		}
		w.uplink.limit = slotBulkInflight
		store.cool.Store(true)
		if w.writeWindow() != slotWriteInflight {
			t.Fatal("adaptive window bypassed cooldown admission")
		}
		store.cool.Store(false)
		store.throttle.Store(1)
		if w.writeWindow() != slotWriteInflight {
			t.Fatal("adaptive window bypassed throttle admission")
		}
	})
}

type incompleteBodyGateStreamSlots struct {
	*streamReaderFixture
}

func (s *incompleteBodyGateStreamSlots) readFrameStream(context.Context, string, slotFrame, func(int, []byte) error) error {
	time.Sleep(time.Second)
	return nil // Store success without delivering the required verified blocks.
}

func TestBodyGateIncompleteStreamDoesNotTrainWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &incompleteBodyGateStreamSlots{streamReaderFixture: newStreamReaderFixture(make([]byte, slotStreamBlockBytes))}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reader := newSlotReader(ctx, store, slotTable{Slots: []string{"in0", "in1"}, Watermark: "wm"}, streamReaderParams(), nil)
		reader.wait()
		if reader.Err() == io.EOF {
			t.Fatal("incomplete stream was accepted")
		}
		if reader.bodies.inflight != 0 || reader.bodies.win.n != 0 {
			t.Fatal("incomplete stream leaked a permit or trained the latency window")
		}
	})
}
