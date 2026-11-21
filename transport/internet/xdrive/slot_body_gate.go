package xdrive

import (
	"context"
	"sync"
	"time"
)

// bodyGate bounds prefetched media body reads with a latencyWindow. The head
// frame and the frames right behind it are never held back, so the window only
// decides how far ahead of the head bodies may start.
const (
	bodyGateMin  = 6 // never fewer prefetched bodies than this
	bodyGateNear = 2 // frames this close to the head always start at once
)

// bodyGate is usable as a zero value; its maximum is slotBodyAhead.
type bodyGate struct {
	mu       sync.Mutex
	inflight int
	wake     chan struct{}
	win      latencyWindow
}

func (g *bodyGate) window() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.win.current(slotBodyAhead)
}

// acquire admits a body read. distance reports how many frames the slot is
// ahead of the head; promoted is closed when it becomes the head. It returns
// false only when ctx ends first.
func (g *bodyGate) acquire(ctx context.Context, distance func() int64, promoted <-chan struct{}) bool {
	for {
		if ctx.Err() != nil {
			return false
		}
		near := distance() <= bodyGateNear
		select {
		case <-promoted:
			near = true
		default:
		}
		g.mu.Lock()
		if near || g.inflight < g.win.current(slotBodyAhead) {
			g.inflight++
			g.mu.Unlock()
			return true
		}
		if g.wake == nil {
			g.wake = make(chan struct{})
		}
		wake := g.wake
		g.mu.Unlock()
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-wake:
		case <-promoted:
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return false
		}
		timer.Stop()
	}
}

// release ends a body read. A successful read's latency steers the window.
func (g *bodyGate) release(d time.Duration, ok bool) {
	g.mu.Lock()
	if g.inflight > 0 {
		g.inflight--
	}
	if ok {
		g.win.observe(d, bodyGateMin, slotBodyAhead)
	}
	if g.wake != nil {
		close(g.wake)
		g.wake = nil
	}
	g.mu.Unlock()
}
