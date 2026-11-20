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
}

func newSlotStreamBuffer(frame slotFrame) *slotStreamBuffer {
	return &slotStreamBuffer{
		frame:   frame,
		data:    make([][]byte, len(frame.blocks)),
		seen:    make([]bool, len(frame.blocks)),
		updates: make(chan struct{}, 1),
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
	}
	return data, false
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
			select {
			case r.ch <- data:
				if f.firstBlockAt.IsZero() {
					f.firstBlockAt = time.Now()
				}
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
