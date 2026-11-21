package xdrive

import (
	"slices"
	"time"
)

// The writer locks this controller. Inline plans reserve a complete buffered burst.
type slotBatchWindow struct {
	unit, window, budget int
	planned              int
	latencies            [latencyWindowSamples]time.Duration
	n, pos               int
	base                 time.Duration
	rates                [8]float64
	ratePos              int
	epoch, last          time.Time
	published            int
}

func newSlotBatchWindow(size, depth int) *slotBatchWindow {
	unit, window := min(size, slotInlineLimit), min(depth, slotWriteInflight)
	return &slotBatchWindow{unit: unit, window: window, budget: unit * window}
}

func (b *slotBatchWindow) frameSize(buffered, outstanding, active, maximum int) int {
	if buffered <= b.unit {
		b.planned = max(0, b.planned-buffered)
		return buffered
	}
	if b.planned == 0 {
		budget := b.budget
		if outstanding == 0 && active == 0 {
			budget = (b.window + b.window/2) * b.unit
		}
		requests := (buffered + b.unit - 1) / b.unit
		if buffered <= budget-outstanding && requests <= b.window+b.window/2-active {
			b.planned = buffered
		}
	}
	if b.planned > 0 {
		n := min(buffered, b.planned, b.unit)
		b.planned -= n
		return n
	}
	if buffered <= 4*b.unit {
		return b.unit
	}
	return min(buffered, maximum)
}

func (b *slotBatchWindow) observe(bytes int, latency time.Duration) {
	if latency <= 0 {
		return
	}
	b.latencies[b.pos] = latency
	b.pos = (b.pos + 1) % len(b.latencies)
	b.n = min(b.n+1, len(b.latencies))
	if b.n >= latencyWindowWarm {
		v := slices.Clone(b.latencies[:b.n])
		slices.Sort(v)
		b.base = v[b.n/20]
	}
	now := time.Now()
	if b.epoch.IsZero() || b.base > 0 && now.Sub(b.last) > b.base {
		b.epoch, b.last, b.published = now, now, 0
		return
	}
	b.last = now
	b.published += bytes
	elapsed := now.Sub(b.epoch)
	if b.base == 0 || elapsed < b.base {
		return
	}
	b.rates[b.ratePos] = float64(b.published) / elapsed.Seconds()
	b.ratePos = (b.ratePos + 1) % len(b.rates)
	b.epoch, b.published = now, 0
	capacity := min(float64(b.unit*b.window), slices.Max(b.rates[:])*b.base.Seconds()+float64(b.unit))
	b.budget = max(min(b.window, bodyGateMin)*b.unit, int(capacity))
}
