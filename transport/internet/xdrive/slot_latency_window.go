package xdrive

import (
	"sort"
	"time"
)

// latencyWindow retains the delay-based bulk upload limit; the owner locks it.
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
