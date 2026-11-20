package xdrive

import (
	"testing"
	"time"
)

func TestStreamDeliveryTimingSeparatesMissingAndEnqueue(t *testing.T) {
	start := time.Unix(123, 0)
	var timing streamDeliveryTiming
	timing.beginMissing(start)
	if !timing.waitSince.IsZero() {
		t.Fatal("initial-block wait was counted as an inter-block gap")
	}
	timing.queued(start, start.Add(10*time.Millisecond))
	timing.beginMissing(start.Add(time.Second))
	timing.beginMissing(start.Add(2 * time.Second))
	timing.blockReady(start.Add(3 * time.Second))
	if timing.missingCount != 1 || timing.missingTotal != 2*time.Second || timing.missingMax != 2*time.Second {
		t.Fatalf("gap lost across yield: %+v", timing)
	}
	timing.queued(start.Add(3*time.Second), start.Add(5*time.Second))
	if timing.enqueueTotal != 2*time.Second+10*time.Millisecond || timing.enqueueMax != 2*time.Second || timing.blocks != 2 {
		t.Fatalf("enqueue wait mixed with missing wait: %+v", timing)
	}
	if timing.completionWait(start.Add(6*time.Second)) != time.Second {
		t.Fatal("completion fence wait differs")
	}
	if timing.completionWait(start.Add(4*time.Second)) != 0 {
		t.Fatal("prefetched completion produced negative wait")
	}
}

func TestStreamDeliveryTimingReadyBlocksDoNotInventGaps(t *testing.T) {
	start := time.Unix(456, 0)
	var timing streamDeliveryTiming
	for index := 0; index < 16; index++ {
		now := start.Add(time.Duration(index) * time.Millisecond)
		timing.blockReady(now)
		timing.queued(now, now)
	}
	if timing.missingCount != 0 || timing.missingTotal != 0 || timing.enqueueTotal != 0 || timing.blocks != 16 {
		t.Fatalf("ready/prefetched data invented wait: %+v", timing)
	}
}
