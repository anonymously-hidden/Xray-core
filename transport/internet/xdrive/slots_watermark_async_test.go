package xdrive

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

// slowAckStore delays watermark reads and tracks concurrent watermark reads.
type slowAckStore struct {
	*memSlots
	wm    string
	delay time.Duration
	mu    sync.Mutex
	conc  int
	max   int
	reads int
}

func (s *slowAckStore) readDescription(ctx context.Context, id string) (string, error) {
	s.mu.Lock()
	wm := s.wm
	s.mu.Unlock()
	if id != wm {
		return s.memSlots.readDescription(ctx, id)
	}
	s.mu.Lock()
	s.conc++
	if s.conc > s.max {
		s.max = s.conc
	}
	s.reads++
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		s.mu.Lock()
		s.conc--
		s.mu.Unlock()
		return "", ctx.Err()
	case <-time.After(s.delay):
	}
	s.mu.Lock()
	s.conc--
	s.mu.Unlock()
	return s.memSlots.readDescription(ctx, id)
}

func TestWatermarkSingleFlightOnlyNeederWaits(t *testing.T) {
	store := &slowAckStore{memSlots: newMemSlots(), delay: 300 * time.Millisecond}
	table := slotTable{Slots: []string{"a", "b"}, Watermark: "wm"}
	store.wm = "wm"
	store.descs["wm"] = "10"
	w := newSlotWriter(context.Background(), store, table, params{concurrency: 2, flushInterval: time.Millisecond})
	defer w.Close()

	const n = 8
	durs := make(chan time.Duration, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			w.drained(2)
			durs <- time.Since(start)
		}()
	}
	wg.Wait()
	close(durs)
	slow := 0
	for d := range durs {
		if d >= 250*time.Millisecond {
			slow++
		}
	}
	if slow != 1 {
		t.Fatalf("%d uploads waited on the watermark GET, want only 1", slow)
	}
	store.mu.Lock()
	max := store.max
	store.mu.Unlock()
	if max != 1 {
		t.Fatalf("concurrent watermark reads = %d, want 1", max)
	}
}

func TestWatermarkPrefetchSteadyStream(t *testing.T) {
	// With the old lock-held drained, every upload at a lap boundary waited a
	// full watermark GET (~0.35s). With prefetch + lock-free single-flight the
	// ack is cached ahead of need, so awaits are ~0 for almost all writes.
	store := &slowAckStore{memSlots: newMemSlots(), delay: 300 * time.Millisecond}
	client, server := slotPair(t, store, 64, 512)
	store.mu.Lock()
	store.wm = client.writer.(*slotWriter).table.Watermark
	store.mu.Unlock()
	payload := randomPayload(64 * 3 * 512)
	errs := make(chan error, 1)
	go func() {
		for i := 0; i < 64*3; i++ {
			if _, err := client.Write(payload[i*512 : (i+1)*512]); err != nil {
				errs <- err
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		errs <- client.Close()
	}()
	if err := server.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(server)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("steady stream not byte-exact")
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	max := store.max
	store.mu.Unlock()
	if max != 1 {
		t.Fatalf("concurrent watermark reads = %d, want 1", max)
	}
}

func TestWatermarkSlowAcksNoStall(t *testing.T) {
	for _, depth := range []uint32{2, 8, 64} {
		store := &slowAckStore{memSlots: newMemSlots(), delay: 300 * time.Millisecond}
		client, server := slotPair(t, store, depth, 512)
		store.mu.Lock()
		store.wm = client.writer.(*slotWriter).table.Watermark
		store.mu.Unlock()
		payload := randomPayload(int(depth) * 3 * 512)
		errs := make(chan error, 1)
		go func() {
			_, err := client.Write(payload)
			if err == nil {
				err = client.Close()
			}
			errs <- err
		}()
		if err := server.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(server)
		if err != nil {
			t.Fatalf("depth %d: %v", depth, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("depth %d: not byte-exact", depth)
		}
		if err := <-errs; err != nil {
			t.Fatalf("depth %d: writer stalled: %v", depth, err)
		}
	}
}
