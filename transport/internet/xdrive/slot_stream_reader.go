package xdrive

import (
	"context"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// slotStreamBuffer owns at most one validated copy of each block. A delivered
// block is released, but its seen bit remains so range retries cannot replay it.
// Storage credit is returned only after every range has completed successfully.
type slotStreamBuffer struct {
	frame    slotFrame
	mu       sync.Mutex
	data     [][]byte
	seen     []bool
	next     int
	received int
	updates  chan struct{}

	// Recovery observes the ordered reader, independently of the single-reader
	// updates signal and run-owned delivery timing. Both fields are guarded by mu.
	deliveryChanged chan struct{}
	requiredAt      time.Time
}

// These observations belong to run, not the HTTP workers. Missing-block waits
// exclude downstream channel backpressure and persist across frontier yields.
type streamDeliveryTiming struct {
	waitSince    time.Time
	lastQueued   time.Time
	blocks       int
	missingCount int
	missingTotal time.Duration
	missingMax   time.Duration
	enqueueTotal time.Duration
	enqueueMax   time.Duration
}

func (d *streamDeliveryTiming) beginMissing(now time.Time) {
	if d.blocks > 0 && d.waitSince.IsZero() {
		d.waitSince = now
	}
}

func (d *streamDeliveryTiming) blockReady(now time.Time) {
	if !d.waitSince.IsZero() {
		wait := max(0, now.Sub(d.waitSince))
		d.missingCount++
		d.missingTotal += wait
		d.missingMax = max(d.missingMax, wait)
		d.waitSince = time.Time{}
	}
}

func (d *streamDeliveryTiming) queued(start, now time.Time) {
	wait := max(0, now.Sub(start))
	d.enqueueTotal += wait
	d.enqueueMax = max(d.enqueueMax, wait)
	d.lastQueued = now
	d.blocks++
}

func (d *streamDeliveryTiming) completionWait(readyAt time.Time) time.Duration {
	if d.lastQueued.IsZero() {
		return 0
	}
	return max(0, readyAt.Sub(d.lastQueued))
}

func newSlotStreamBuffer(frame slotFrame) *slotStreamBuffer {
	return &slotStreamBuffer{
		frame:           frame,
		data:            make([][]byte, len(frame.blocks)),
		seen:            make([]bool, len(frame.blocks)),
		updates:         make(chan struct{}, 1),
		deliveryChanged: make(chan struct{}),
	}
}

// offer takes ownership of data on success; callers must never mutate it later.
func (s *slotStreamBuffer) offer(index int, data []byte) error {
	if !validStreamBlock(s.frame, index, data) {
		return errors.New("streamed slot block does not match its checksum")
	}
	s.mu.Lock()
	if !s.seen[index] {
		s.seen[index] = true
		s.data[index] = data
		s.received++
	}
	s.mu.Unlock()
	select {
	case s.updates <- struct{}{}:
	default:
	}
	return nil
}

func (s *slotStreamBuffer) take() ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next == len(s.data) {
		return nil, true
	}
	data := s.data[s.next]
	if data != nil {
		s.data[s.next] = nil
		s.next++
		s.requiredAt = time.Time{}
		close(s.deliveryChanged)
		s.deliveryChanged = make(chan struct{})
	} else if s.requiredAt.IsZero() {
		// Start only when the ordered reader actually needs this missing block;
		// prefetch age and downstream enqueue waits do not start this clock.
		s.requiredAt = time.Now()
		close(s.deliveryChanged)
		s.deliveryChanged = make(chan struct{})
	}
	return data, false
}

// deliveryState atomically snapshots the next block to take, its current reader
// wait, and a broadcast generation. The terminal offset means every block was
// taken; enqueue and the successful range/EOF fence still govern delivery/ACK.
func (s *slotStreamBuffer) deliveryState() (int, <-chan struct{}, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	waitingAt := s.requiredAt
	if s.next == len(s.data) || s.data[s.next] != nil {
		// A verified block may arrive before the ordered reader is scheduled.
		// Do not rescue already-buffered bytes during that scheduling gap.
		waitingAt = time.Time{}
	}
	return min(s.next*slotStreamBlockBytes, s.frame.size), s.deliveryChanged, waitingAt
}

func (s *slotStreamBuffer) complete() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.received == len(s.seen)
}

// deliverStream can yield to frontier growth without restarting the frame or
// replaying blocks. The ready signal, not the last available block, commits ACK.
func (r *slotReader) deliverStream(ctx context.Context, f *slotFetch) (bool, error) {
	for {
		select {
		case <-f.ready:
			if f.err != nil {
				return false, f.err
			}
		default:
		}
		data, all := f.stream.take()
		if data != nil {
			start := time.Now()
			f.delivery.blockReady(start)
			select {
			case r.ch <- data:
				now := time.Now()
				if f.firstBlockAt.IsZero() {
					f.firstBlockAt = now
				}
				f.delivery.queued(start, now)
			case <-ctx.Done():
				return false, ctx.Err()
			}
			continue
		}
		if all {
			select {
			case <-f.ready:
				return f.err == nil, f.err
			case <-r.grow:
				return false, nil
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
		f.delivery.beginMissing(time.Now())
		select {
		case <-f.stream.updates:
		case <-f.ready:
			if f.err != nil {
				return false, f.err
			}
			if !f.stream.complete() {
				return false, errors.New("streamed slot ended without every block")
			}
		case <-r.grow:
			return false, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}
