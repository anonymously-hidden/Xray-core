package xdrive

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

// A later segment must not push the watermark write back: it is due ackDelay
// after the oldest unacknowledged segment.
func TestRegressionAckTimerNotResetByLaterSegment(t *testing.T) {
	old := ackDelay
	ackDelay = 400 * time.Millisecond
	t.Cleanup(func() { ackDelay = old })
	regressionClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer func() { cancel(); synctest.Wait() }()
		p := regressionParams()
		p.eagerWindow = time.Hour
		table := regressionTable(64, true)
		store := regressionStore(table)
		w := newSlotWriter(ctx, store, table, p)
		r := newSlotReader(ctx, store, table, p, nil)
		go func() {
			for range r.chunks() {
			}
		}()

		start := time.Now()
		// the second segment arrives just before the first one's ack is due
		if _, err := w.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(350 * time.Millisecond)
		if _, err := w.Write([]byte{2}); err != nil {
			t.Fatal(err)
		}
		var firstAck time.Duration
		for firstAck == 0 && time.Since(start) < 2*time.Second {
			time.Sleep(10 * time.Millisecond)
			store.mu.Lock()
			acked := store.acks
			store.mu.Unlock()
			if acked > 0 {
				firstAck = time.Since(start)
			}
		}
		if firstAck == 0 || firstAck > ackDelay+100*time.Millisecond {
			t.Errorf("watermark written after %s, want about %s after the oldest unacked segment", firstAck, ackDelay)
		}
	})
}
