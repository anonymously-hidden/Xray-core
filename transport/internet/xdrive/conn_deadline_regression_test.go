package xdrive

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type deadlinePair struct {
	store      *regressionSlots
	writer     *Conn
	reader     *Conn
	hold       chan struct{}
	holdWrites atomic.Bool
}

// deadlinePair has a writer whose uploads wait for hold while holdWrites, and
// a reader on the same tables
func newDeadlinePair(t *testing.T, ctx context.Context) *deadlinePair {
	t.Helper()
	table := regressionTable(64, true)
	d := &deadlinePair{store: regressionStore(table), hold: make(chan struct{})}
	d.holdWrites.Store(true)
	d.store.before = func(ctx context.Context, _ slotFrame) error {
		if !d.holdWrites.Load() {
			return nil
		}
		select {
		case <-d.hold:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p := regressionParams()
	d.writer = newSlotConn(ctx, d.store, table, regressionTable(64, true), p, nil, nil)
	d.reader = newSlotConn(ctx, d.store, regressionTable(64, true), table, p, nil, nil)
	return d
}

func TestConnDeadlineReadWakeAndClear(t *testing.T) {
	regressionBubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer func() { cancel(); synctest.Wait() }()
		d := newDeadlinePair(t, ctx)
		c := d.reader

		type result struct {
			n   int
			err error
			at  time.Duration
		}
		start := time.Now()
		read := func() chan result {
			ch := make(chan result, 1)
			go func() {
				n, err := c.Read(make([]byte, 8))
				ch <- result{n, err, time.Since(start)}
			}()
			return ch
		}

		// a pending Read without a deadline is woken by a deadline set later
		ch := read()
		synctest.Wait()
		c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		if r := <-ch; !errors.Is(r.err, os.ErrDeadlineExceeded) || r.at != 50*time.Millisecond {
			t.Fatalf("set after block: %v at %v, want a timeout at 50ms", r.err, r.at)
		}

		// moving a pending deadline earlier and later takes effect
		start = time.Now()
		c.SetReadDeadline(time.Now().Add(time.Hour))
		ch = read()
		synctest.Wait()
		c.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
		if r := <-ch; !errors.Is(r.err, os.ErrDeadlineExceeded) || r.at != 20*time.Millisecond {
			t.Fatalf("earlier deadline: %v at %v, want a timeout at 20ms", r.err, r.at)
		}
		start = time.Now()
		c.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
		ch = read()
		synctest.Wait()
		c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		if r := <-ch; !errors.Is(r.err, os.ErrDeadlineExceeded) || r.at != 200*time.Millisecond {
			t.Fatalf("later deadline: %v at %v, want a timeout at 200ms", r.err, r.at)
		}

		// a deadline in the past wakes a pending Read at once and fails the next
		c.SetReadDeadline(time.Time{})
		start = time.Now()
		ch = read()
		synctest.Wait()
		c.SetReadDeadline(time.Now().Add(-time.Second))
		if r := <-ch; !errors.Is(r.err, os.ErrDeadlineExceeded) || r.at != 0 {
			t.Fatalf("past deadline: %v at %v, want a timeout at once", r.err, r.at)
		}
		if _, err := c.Read(make([]byte, 8)); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Read after a past deadline: %v", err)
		}

		// zero clears the deadline, a pending Read then stays blocked
		c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		c.SetReadDeadline(time.Time{})
		ch = read()
		time.Sleep(time.Hour)
		synctest.Wait()
		select {
		case r := <-ch:
			t.Fatalf("Read returned %v after the deadline was cleared", r.err)
		default:
		}
		d.holdWrites.Store(false)
		close(d.hold)
		if _, err := d.writer.Write([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		if r := <-ch; r.err != nil || r.n != 5 {
			t.Fatalf("Read after clearing = %d %v", r.n, r.err)
		}
	})
}

func TestConnDeadlineWritePartialAndRetry(t *testing.T) {
	regressionBubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer func() { cancel(); synctest.Wait() }()
		d := newDeadlinePair(t, ctx)
		payload := randomPayload(32 << 20)

		start := time.Now()
		d.writer.SetWriteDeadline(start.Add(100 * time.Millisecond))
		n, err := d.writer.Write(payload)
		elapsed := time.Since(start)
		t.Logf("blocked uploads: Write accepted %d of %d bytes in %v: %v", n, len(payload), elapsed, err)
		if !errors.Is(err, os.ErrDeadlineExceeded) || elapsed != 100*time.Millisecond {
			t.Fatalf("Write = %d, %v after %v, want a timeout at 100ms", n, err, elapsed)
		}
		if n <= 0 || n >= len(payload) {
			t.Fatalf("Write accepted %d of %d bytes, want some but not all", n, len(payload))
		}

		// a past deadline fails at once with nothing accepted
		d.writer.SetWriteDeadline(time.Now().Add(-time.Second))
		if n2, err := d.writer.Write([]byte("x")); n2 != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Write after a past deadline = %d, %v", n2, err)
		}

		// retrying the rest sends no byte twice and none is lost
		d.holdWrites.Store(false)
		close(d.hold)
		d.writer.SetWriteDeadline(time.Time{})
		d.reader.SetReadDeadline(time.Time{})
		var got bytes.Buffer
		done := make(chan struct{})
		go func() {
			defer close(done)
			buf := make([]byte, 64<<10)
			for got.Len() < len(payload) {
				m, err := d.reader.Read(buf)
				got.Write(buf[:m])
				if err != nil {
					return
				}
			}
		}()
		for rest := payload[n:]; len(rest) > 0; {
			m, err := d.writer.Write(rest)
			if err != nil {
				t.Fatal(err)
			}
			rest = rest[m:]
		}
		<-done
		if !bytes.Equal(got.Bytes(), payload) {
			t.Fatalf("reader got %d bytes that differ from the %d written", got.Len(), len(payload))
		}
	})
}

func TestConnDeadlineWriteWokenByMove(t *testing.T) {
	regressionBubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer func() { cancel(); synctest.Wait() }()
		d := newDeadlinePair(t, ctx)
		type result struct {
			n   int
			err error
		}
		ch := make(chan result, 1)
		go func() {
			n, err := d.writer.Write(make([]byte, 32<<20))
			ch <- result{n, err}
		}()
		synctest.Wait()
		d.writer.SetWriteDeadline(time.Now().Add(-time.Second))
		r := <-ch
		if !errors.Is(r.err, os.ErrDeadlineExceeded) || r.n <= 0 {
			t.Fatalf("pending Write after a past deadline = %d, %v", r.n, r.err)
		}
	})
}

func TestConnCloseWakesBlockedReadAndWrite(t *testing.T) {
	regressionBubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer func() { cancel(); synctest.Wait() }()
		d := newDeadlinePair(t, ctx)
		var wg sync.WaitGroup
		var readErr, writeErr error
		wg.Go(func() { _, readErr = d.reader.Read(make([]byte, 8)) })
		wg.Go(func() { _, writeErr = d.writer.Write(make([]byte, 32<<20)) })
		synctest.Wait()
		// Close itself waits on the held uploads, so it runs aside; the blocked
		// calls must return without it finishing
		go d.writer.Close()
		go d.reader.Close()
		wg.Wait()
		d.writer.cancel()
		d.reader.cancel()
		if readErr == nil || !errors.Is(readErr, io.ErrClosedPipe) {
			t.Errorf("blocked Read after Close = %v, want io.ErrClosedPipe", readErr)
		}
		if writeErr == nil {
			t.Errorf("blocked Write after Close returned no error")
		}
	})
}
