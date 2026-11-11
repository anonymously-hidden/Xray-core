package xdrive

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type gatedStorage struct {
	Storage
	mu      sync.Mutex
	gate    chan struct{}
	started int
	stored  map[string]int
}

func (g *gatedStorage) Put(ctx context.Context, name string, data []byte) error {
	g.mu.Lock()
	g.started++
	g.mu.Unlock()
	select {
	case <-g.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	g.mu.Lock()
	g.stored[name] = len(data)
	g.mu.Unlock()
	return nil
}

func TestRegressionWALWriterBackpressure(t *testing.T) {
	regressionBubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer func() { cancel(); synctest.Wait() }()
		p := regressionParams()
		p.segmentBytes, p.concurrency = 1<<10, 4
		store := &gatedStorage{gate: make(chan struct{}), stored: map[string]int{}}
		w := newWALWriter(ctx, store, "pfx", p)

		const total = 100 << 10
		var n int
		var err error
		done := make(chan struct{})
		go func() { n, err = w.Write(make([]byte, total)); close(done) }()
		synctest.Wait()

		select {
		case <-done:
			t.Fatalf("Write of %d bytes returned with the store stalled; pending uploads are unbounded", total)
		default:
		}
		store.mu.Lock()
		started := store.started
		store.mu.Unlock()
		if started != p.concurrency {
			t.Errorf("uploads started = %d, want %d", started, p.concurrency)
		}
		w.mu.Lock()
		seq, buffered := w.seq, len(w.buf)
		w.mu.Unlock()
		if seq > int64(p.concurrency) {
			t.Errorf("segments cut while stalled = %d, want at most %d", seq, p.concurrency)
		}
		if buffered > 2*p.segmentBytes {
			t.Errorf("buffered while stalled = %d, want at most %d", buffered, 2*p.segmentBytes)
		}

		close(store.gate)
		<-done
		if err != nil || n != total {
			t.Fatalf("Write = %d, %v, want %d, nil", n, err, total)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		store.mu.Lock()
		defer store.mu.Unlock()
		if len(store.stored) != total/p.segmentBytes+1 {
			t.Errorf("stored %d objects, want %d segments and the end marker", len(store.stored), total/p.segmentBytes)
		}
	})
}

func TestRegressionWALWriterStopsOnDeadline(t *testing.T) {
	regressionBubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer func() { cancel(); synctest.Wait() }()
		p := regressionParams()
		p.segmentBytes, p.concurrency = 1<<10, 2
		store := &gatedStorage{gate: make(chan struct{}), stored: map[string]int{}}
		w := newWALWriter(ctx, store, "pfx", p)

		stop := make(chan struct{})
		time.AfterFunc(50*time.Millisecond, func() { close(stop) })
		n, err := w.writeUntil(make([]byte, 64<<10), stop, nil)
		if err != errWriteStopped {
			t.Fatalf("writeUntil err = %v, want errWriteStopped", err)
		}
		if n <= 0 || n >= 64<<10 {
			t.Fatalf("writeUntil accepted %d, want a partial count", n)
		}
		w.mu.Lock()
		held := int64(len(w.buf)) + w.seq*int64(p.segmentBytes)
		w.mu.Unlock()
		if held != int64(n) {
			t.Errorf("accepted %d but the writer holds %d", n, held)
		}
	})
}
