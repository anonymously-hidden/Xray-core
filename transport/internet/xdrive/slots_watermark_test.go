package xdrive

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

func TestEagerMissingWatermarkFailsPromptly(t *testing.T) {
	store := newMemSlots()
	client, server := slotPair(t, store, 2, 512)
	wm := client.writer.(*slotWriter).table.Watermark
	store.mu.Lock()
	delete(store.descs, wm)
	store.mu.Unlock()

	start := time.Now()
	errs := make(chan error, 1)
	go func() {
		_, err := client.Write(randomPayload(3 * 512))
		if err == nil {
			err = client.Close()
		}
		errs <- err
	}()
	select {
	case err := <-errs:
		if err == nil || !strings.Contains(err.Error(), "gone") {
			t.Fatalf("writer returned %v, want a prompt gone-watermark error", err)
		}
		if time.Since(start) >= slotStallTimeout {
			t.Fatal("writer waited out the stall timeout instead of failing at once")
		}
	case <-time.After(time.Second):
		t.Fatal("writer did not fail promptly on a missing eager watermark")
	}
	client.Close()
	server.Close()
}

func TestLazyNoWatermarkYetStillWrites(t *testing.T) {
	store := newMemSlots()
	ids, err := store.generateIDs(context.Background(), 6)
	if err != nil {
		t.Fatal(err)
	}
	r := lazyRing("lazy-wm", 2, ids)
	p := params{concurrency: 2, segmentBytes: 512, flushInterval: time.Millisecond,
		minPollInterval: time.Millisecond, maxPollInterval: 5 * time.Millisecond, eagerWindow: 50 * time.Millisecond}
	client := newSlotConn(context.Background(), store, r.Up, r.Down, p, nil, nil)
	server := newSlotConn(context.Background(), store, r.Down, r.Up, p, nil, nil)
	defer client.Close()
	defer server.Close()

	payload := randomPayload(3 * 512)
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
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("lazy stream without an initial watermark not byte-exact")
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}
