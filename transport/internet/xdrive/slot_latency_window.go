package xdrive

import (
	"sort"
	"time"
)

// latencyWindow is a delay-based limit on how much bulk work may be in flight.
// Every body in flight shares the same link (and Drive's per-user throughput),
// so beyond the bandwidth-delay product more concurrency adds no throughput:
// it only queues, and the ring is a FIFO byte stream, so that queue is paid by
// every small frame written behind it. In the November 21 client log body
// latency rose from about 1.1 s to 2.7 s (max 3.4 s) at 465 frames/min.
//
// The window shrinks by a quarter, at most once per base latency, when an
// operation takes clearly longer than the unloaded floor (the 5th percentile
// of the last 256), and grows by one when operations are back near the floor.
// It starts at its maximum, so an uncongested path behaves as before.
// The zero value is ready to use; the owner provides the locking.
const (
	latencyWindowSamples = 256
	latencyWindowWarm    = 24
)

type latencyWindow struct {
	limit  int // 0 means the maximum
	ring   [latencyWindowSamples]time.Duration
	n, pos int
	cutAt  time.Time
}

func (l *latencyWindow) current(hi int) int {
	if l.limit <= 0 || l.limit > hi {
		return hi
	}
	return l.limit
}

func (l *latencyWindow) observe(d time.Duration, lo, hi int) {
	if d <= 0 {
		return
	}
	l.ring[l.pos] = d
	l.pos = (l.pos + 1) % latencyWindowSamples
	l.n = min(l.n+1, latencyWindowSamples)
	if l.n < latencyWindowWarm {
		return
	}
	v := make([]time.Duration, l.n)
	copy(v, l.ring[:l.n])
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	base := v[l.n/20]
	cur := l.current(hi)
	cut := base + base/2 + 100*time.Millisecond
	grow := base + base/3 + 50*time.Millisecond
	now := time.Now()
	switch {
	case d > cut:
		if now.Sub(l.cutAt) >= base {
			l.limit = min(hi, max(lo, cur*3/4))
			l.cutAt = now
		}
	case d <= grow:
		l.limit = min(hi, cur+1)
	}
}
