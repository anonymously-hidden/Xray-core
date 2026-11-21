package xdrive

import (
	"context"
	"slices"
	"sort"
	"sync"
	"time"
)

const (
	bodyGateMin  = 6
	bodyGateNear = 2
)

// Retrieval completion returns request credit; ordered handoff returns byte credit.
type bodyGate struct {
	mu        sync.Mutex
	head      int64
	quantum   int
	maximum   int
	minimum   int
	budget    int
	inflight  int
	resident  int
	buffered  int
	queue     []*bodyPermit
	live      map[*bodyPermit]struct{}
	closed    bool
	latencies [latencyWindowSamples]time.Duration
	n, pos    int
	base      time.Duration
	rates     [8]float64
	ratePos   int
	rate      float64
	epoch     time.Time
	last      time.Time
	delivered int
}

type bodyPermit struct {
	gate     *bodyGate
	ctx      context.Context
	seq      int64
	size     int
	left     int
	buffered int
	ready    chan struct{}
	started  bool
	active   bool
	closed   bool
}

func (g *bodyGate) configure(head int64, size, depth int) {
	g.head = head
	g.quantum = size
	g.maximum = min(depth, slotBodyAhead) * size
	g.minimum = min(depth, bodyGateMin) * size
	g.budget = g.maximum
}

func (g *bodyGate) initLocked() {
	if g.quantum == 0 {
		g.configure(0, defaultSegmentBytes, slotBodyAhead)
	}
	if g.live == nil {
		g.live = make(map[*bodyPermit]struct{})
	}
}

func (g *bodyGate) snapshot() (window, active, outstanding, buffered, budget int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.initLocked()
	return (g.budget + g.quantum - 1) / g.quantum, g.inflight, g.resident, g.buffered, g.budget
}

func (g *bodyGate) advance(head int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.initLocked()
	g.head = head
	g.admitLocked()
}

func (g *bodyGate) acquire(ctx context.Context, seq int64, size int) (*bodyPermit, bool) {
	if ctx.Err() != nil {
		return nil, false
	}
	p := &bodyPermit{gate: g, ctx: ctx, seq: seq, size: size, left: size, ready: make(chan struct{})}
	g.mu.Lock()
	g.initLocked()
	if g.closed {
		g.mu.Unlock()
		return nil, false
	}
	i := sort.Search(len(g.queue), func(i int) bool { return g.queue[i].seq > seq })
	g.queue = slices.Insert(g.queue, i, p)
	g.live[p] = struct{}{}
	g.admitLocked()
	g.mu.Unlock()
	select {
	case <-p.ready:
		g.mu.Lock()
		admitted := p.started && !p.closed && ctx.Err() == nil
		g.mu.Unlock()
		if admitted {
			return p, true
		}
	case <-ctx.Done():
	}
	p.abort()
	return nil, false
}

func (g *bodyGate) admitLocked() {
	if g.closed {
		return
	}
	for len(g.queue) > 0 {
		p := g.queue[0]
		if p.ctx.Err() != nil {
			g.queue = slices.Delete(g.queue, 0, 1)
			p.discardLocked()
			continue
		}
		// Only the head and its two successors may borrow beyond the byte budget.
		near := p.seq <= g.head+bodyGateNear
		if !near && (p.seq >= g.head+slotBodyAhead || g.inflight >= slotBodyAhead || p.size > g.budget-g.resident) {
			return
		}
		g.queue = slices.Delete(g.queue, 0, 1)
		p.started, p.active = true, true
		g.inflight++
		g.resident += p.size
		close(p.ready)
	}
}

func (p *bodyPermit) received(n int) {
	if p == nil {
		return
	}
	g := p.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	if !p.closed {
		p.buffered += n
		g.buffered += n
	}
}

func (p *bodyPermit) deliver(n int) {
	if p == nil {
		return
	}
	g := p.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	if p.closed {
		return
	}
	p.left -= n
	p.buffered -= n
	g.resident -= n
	g.buffered -= n
	g.observeDeliveryLocked(n)
	if p.left == 0 && !p.active {
		p.closed = true
		delete(g.live, p)
	}
	g.admitLocked()
}

func (p *bodyPermit) finish(d time.Duration, ok bool) {
	g := p.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	if p.closed {
		return
	}
	p.active = false
	g.inflight--
	if !ok {
		p.discardLocked()
	} else {
		g.observeLatencyLocked(d)
		if p.left == 0 {
			p.closed = true
			delete(g.live, p)
		}
	}
	g.admitLocked()
}

func (p *bodyPermit) discardLocked() {
	if p.closed {
		return
	}
	p.closed = true
	if p.started {
		p.gate.resident -= p.left
		p.gate.buffered -= p.buffered
		if p.active {
			p.gate.inflight--
			p.active = false
		}
	} else {
		close(p.ready)
	}
	p.left, p.buffered = 0, 0
	delete(p.gate.live, p)
}

func (p *bodyPermit) abort() {
	g := p.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	if !p.started {
		if i := slices.Index(g.queue, p); i >= 0 {
			g.queue = slices.Delete(g.queue, i, i+1)
		}
	}
	p.discardLocked()
	g.admitLocked()
}

func (g *bodyGate) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	for p := range g.live {
		p.discardLocked()
	}
	g.queue = nil
}

func (g *bodyGate) observeLatencyLocked(d time.Duration) {
	if d <= 0 {
		return
	}
	g.latencies[g.pos] = d
	g.pos = (g.pos + 1) % len(g.latencies)
	g.n = min(g.n+1, len(g.latencies))
	if g.n < latencyWindowWarm {
		return
	}
	v := slices.Clone(g.latencies[:g.n])
	slices.Sort(v)
	g.base = v[g.n/20]
	g.updateBudgetLocked()
}

func (g *bodyGate) observeDeliveryLocked(n int) {
	now := time.Now()
	// Idle periods and delivery outages do not erase recently demonstrated capacity.
	if g.epoch.IsZero() || g.base > 0 && now.Sub(g.last) > g.base {
		g.epoch, g.last, g.delivered = now, now, 0
		return
	}
	g.last = now
	g.delivered += n
	elapsed := now.Sub(g.epoch)
	if g.base == 0 || elapsed < g.base {
		return
	}
	g.rates[g.ratePos] = float64(g.delivered) / elapsed.Seconds()
	g.ratePos = (g.ratePos + 1) % len(g.rates)
	g.rate = slices.Max(g.rates[:])
	g.epoch, g.delivered = now, 0
	g.updateBudgetLocked()
}

func (g *bodyGate) updateBudgetLocked() {
	if g.base > 0 && g.rate > 0 {
		// One healthy fetch interval of useful bytes, plus one frame of handoff slack.
		bytes := min(float64(g.maximum), g.rate*g.base.Seconds()+float64(g.quantum))
		g.budget = max(g.minimum, int(bytes))
	}
}

type bodyReadTimingKey struct{}

// Drive supplies validated dispatch timings; retries/replays/rescues invalidate a sample.
type bodyReadTiming struct {
	mu      sync.Mutex
	latency time.Duration
	dirty   bool
}

func (t *bodyReadTiming) reject() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.dirty = true
	t.mu.Unlock()
}

func (t *bodyReadTiming) observe(d time.Duration) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.latency = max(t.latency, d)
	t.mu.Unlock()
}

func (t *bodyReadTiming) result(fallback time.Duration) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dirty {
		return 0
	}
	if t.latency > 0 {
		return t.latency
	}
	return fallback
}
