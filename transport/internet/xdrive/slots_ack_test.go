package xdrive

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// ackCountStore counts watermark writes separately from slot writes.
type ackCountStore struct {
	*memSlots
	wm   string
	acks atomic.Int64
}

func (s *ackCountStore) setSlot(ctx context.Context, id, description string, body []byte) error {
	if id == s.wm {
		s.acks.Add(1)
	}
	return s.memSlots.setSlot(ctx, id, description, body)
}

func (s *ackCountStore) createSlot(ctx context.Context, id, name, description string, body []byte) error {
	if id == s.wm {
		s.acks.Add(1)
	}
	return s.memSlots.createSlot(ctx, id, name, description, body)
}

func TestAckBatchingAndTimer(t *testing.T) {
	old := ackDelay
	ackDelay = 100 * time.Millisecond
	t.Cleanup(func() { ackDelay = old })

	store := &ackCountStore{memSlots: newMemSlots()}
	client, server := slotPair(t, store, 16, 1024)
	// The server reads the client's uplink; its watermark is the client's up w.
	// slotPair names watermarks via makeRing; discover it from the writer table.
	store.wm = client.writer.(*slotWriter).table.Watermark

	// A single segment must not ack immediately (batch=4); the timer flushes it.
	if _, err := client.Write(randomPayload(1024)); err != nil {
		t.Fatal(err)
	}
	if err := server.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(server, make([]byte, 1024)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	if store.acks.Load() == 0 {
		t.Fatal("the ack timer never flushed a lone segment")
	}

	// A trickle of segments acks far fewer times than segments.
	before := store.acks.Load()
	for i := 0; i < 8; i++ {
		if _, err := client.Write(randomPayload(1024)); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(server, make([]byte, 1024)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(250 * time.Millisecond)
	added := store.acks.Load() - before
	if added >= 8 {
		t.Fatalf("8 trickle segments caused %d acks, want batching", added)
	}
	client.Close()
	server.Close()
}

func TestNoWriterStallAtWrap(t *testing.T) {
	old := ackDelay
	ackDelay = 50 * time.Millisecond
	t.Cleanup(func() { ackDelay = old })
	for _, depth := range []uint32{2, 8, 64} {
		store := newMemSlots()
		client, server := slotPair(t, store, depth, 512)
		payload := randomPayload(int(depth) * 3 * 512)
		errs := make(chan error, 1)
		go func() {
			_, err := client.Write(payload)
			if err == nil {
				err = client.Close()
			}
			errs <- err
		}()
		if err := server.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(server)
		if err != nil {
			t.Fatalf("depth %d: %v", depth, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("depth %d: stream not byte-exact", depth)
		}
		if err := <-errs; err != nil {
			t.Fatalf("depth %d: writer stalled: %v", depth, err)
		}
	}
}
