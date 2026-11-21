package xdrive

import (
	"context"
	"io"
	"testing"
	"testing/synctest"
	"time"
)

func TestBodyGateZeroValueBoundsPrefetchAndNeverHoldsNearFrames(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var g bodyGate
		defer g.close()
		ctx := context.Background()
		bulk, ok := g.acquire(ctx, 3, slotBodyAhead*defaultSegmentBytes)
		if !ok {
			t.Fatal("cold prefetch failed")
		}
		bulk.received(bulk.size)
		bulk.finish(time.Second, true)
		done := make(chan *bodyPermit, 1)
		go func() { p, _ := g.acquire(ctx, 4, defaultSegmentBytes); done <- p }()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("completed but undelivered bytes returned prefetch credit")
		default:
		}
		head, ok := g.acquire(ctx, 0, defaultSegmentBytes)
		if !ok {
			t.Fatal("full prefetch held the head")
		}
		head.received(head.size)
		head.finish(time.Second, true)
		head.deliver(head.size)
		bulk.deliver(defaultSegmentBytes)
		if p := <-done; p == nil {
			t.Fatal("ordered handoff did not return byte credit")
		}
		_, active, outstanding, buffered, budget := g.snapshot()
		if active != 1 || outstanding != budget || buffered != budget-defaultSegmentBytes {
			t.Fatalf("active=%d outstanding=%d buffered=%d budget=%d", active, outstanding, buffered, budget)
		}
	})
}

func TestBodyGateStopsWaitingWhenContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var g bodyGate
		defer g.close()
		_, _ = g.acquire(context.Background(), 3, slotBodyAhead*defaultSegmentBytes)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan bool, 1)
		go func() { _, ok := g.acquire(ctx, 4, defaultSegmentBytes); done <- ok }()
		synctest.Wait()
		cancel()
		if <-done {
			t.Fatal("canceled queued request was admitted")
		}
		g.mu.Lock()
		defer g.mu.Unlock()
		if len(g.queue) != 0 || len(g.live) != 1 || g.inflight != 1 {
			t.Fatal("cancellation leaked queued credit")
		}
	})
}

func TestBodyGateBudgetUsesDeliveredCapacityNotSlowFetchCuts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var g bodyGate
		g.initLocked()
		for range latencyWindowWarm {
			g.observeLatencyLocked(time.Second)
		}
		if g.budget != g.maximum {
			t.Fatal("cold delivery capacity changed the existing maximum")
		}
		g.observeDeliveryLocked(defaultSegmentBytes)
		for range 30 {
			time.Sleep(100 * time.Millisecond)
			g.observeDeliveryLocked(defaultSegmentBytes)
		}
		want := 11 * defaultSegmentBytes
		if g.budget != want {
			t.Fatalf("budget=%d, want %d bytes for 5 MiB/s and 1s fetch latency", g.budget, want)
		}
		for range 5 {
			g.observeLatencyLocked(4 * time.Second)
		}
		if g.budget != want {
			t.Fatal("slow response tails cut demonstrated throughput capacity")
		}
		time.Sleep(20 * time.Second)
		g.observeDeliveryLocked(defaultSegmentBytes)
		if g.budget != want {
			t.Fatal("delivery outage erased healthy capacity")
		}
		for range 100 {
			time.Sleep(25 * time.Millisecond)
			g.observeDeliveryLocked(defaultSegmentBytes)
		}
		if g.budget != g.maximum {
			t.Fatal("faster useful delivery did not recover the maximum")
		}
	})
}

func TestBodyGateQueuedMediaUsesSequenceOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var g bodyGate
		defer g.close()
		ctx := context.Background()
		bulk, _ := g.acquire(ctx, 3, slotBodyAhead*defaultSegmentBytes)
		bulk.received(bulk.size)
		bulk.finish(0, true)
		later := make(chan *bodyPermit, 1)
		earlier := make(chan *bodyPermit, 1)
		go func() { p, _ := g.acquire(ctx, 6, defaultSegmentBytes); later <- p }()
		synctest.Wait()
		go func() { p, _ := g.acquire(ctx, 4, defaultSegmentBytes); earlier <- p }()
		synctest.Wait()
		bulk.deliver(defaultSegmentBytes)
		if <-earlier == nil {
			t.Fatal("earliest queued frame was not admitted")
		}
		synctest.Wait()
		select {
		case <-later:
			t.Fatal("later frame won byte credit ahead of the earlier frame")
		default:
		}
		bulk.deliver(defaultSegmentBytes)
		if <-later == nil {
			t.Fatal("later frame was starved after ordered credit returned")
		}
	})
}

func TestBodyGateOversizedAndFutureFramesWaitForNearAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var g bodyGate
		g.configure(0, slotStreamBlockBytes, 4)
		defer g.close()
		done := make(chan *bodyPermit, 1)
		go func() { p, _ := g.acquire(context.Background(), 3, 5*slotStreamBlockBytes); done <- p }()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("oversized speculative frame exceeded the byte ceiling")
		default:
		}
		g.advance(1)
		p := <-done
		if p == nil {
			t.Fatal("oversized near frame deadlocked")
		}
		p.received(p.size)
		p.finish(time.Second, true)
		p.deliver(p.size)
		go func() { p, _ := g.acquire(context.Background(), 1+slotBodyAhead, slotStreamBlockBytes); done <- p }()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("future frame escaped the lookahead bound")
		default:
		}
		g.close()
		if <-done != nil {
			t.Fatal("shutdown admitted a queued future frame")
		}
		if g.inflight != 0 || g.resident != 0 || g.buffered != 0 || len(g.live) != 0 {
			t.Fatal("shutdown leaked byte or request credit")
		}
	})
}

func TestBodyReadTimingRejectsRecoverySamples(t *testing.T) {
	var timing bodyReadTiming
	timing.observe(time.Second)
	timing.observe(2 * time.Second)
	if timing.result(10*time.Second) != 2*time.Second {
		t.Fatal("range timing included local admission instead of dispatch latency")
	}
	timing.reject()
	timing.observe(time.Second)
	if timing.result(10*time.Second) != 0 {
		t.Fatal("a successful retry or rescue restored a contaminated sample")
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
	for _, tc := range []struct {
		name string
		seq  int64
	}{
		{name: "free_window", seq: 10},
		{name: "head", seq: 0},
		{name: "near", seq: bodyGateNear},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var g bodyGate
			if _, ok := g.acquire(ctx, tc.seq, defaultSegmentBytes); ok || g.inflight != 0 {
				t.Fatal("canceled read acquired a body permit")
			}
		})
	}
}

func TestBodyGatePromotionInterruptsFullWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var g bodyGate
		defer g.close()
		ctx := context.Background()
		_, _ = g.acquire(ctx, 3, slotBodyAhead*defaultSegmentBytes)
		done := make(chan bool, 1)
		go func() { _, ok := g.acquire(ctx, 7, defaultSegmentBytes); done <- ok }()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("distant read escaped a full window")
		default:
		}
		at := time.Now()
		g.advance(7)
		if !<-done || time.Now() != at {
			t.Fatal("head promotion did not immediately bypass the full window")
		}
		g.close()
		if g.inflight != 0 || g.n != 0 || g.resident != 0 || len(g.live) != 0 {
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
		if reader.bodies.inflight != 0 || reader.bodies.n != 0 || reader.bodies.resident != 0 {
			t.Fatal("incomplete stream leaked a permit or trained the latency window")
		}
	})
}
