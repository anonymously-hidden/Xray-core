package xdrive

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func TestSlotBatchCommittedBurstSurvivesChangingPressure(t *testing.T) {
	b := newSlotBatchWindow(defaultSegmentBytes, 64)
	buffered := (slotWriteInflight + slotWriteInflight/2) * slotInlineLimit
	for i := 0; buffered > 0; i++ {
		if got := b.frameSize(buffered, i*slotInlineLimit, min(i, slotWriteInflight-1), defaultSegmentBytes); got != slotInlineLimit {
			t.Fatalf("committed frame %d became %d-byte media", i, got)
		}
		buffered -= slotInlineLimit
	}
	if b.planned != 0 {
		t.Fatal("completed inline plan retained bytes")
	}
	if got := b.frameSize(5*slotInlineLimit, 12*slotInlineLimit, 11, defaultSegmentBytes); got != 5*slotInlineLimit {
		t.Fatal("new input was admitted as another fresh inline wave")
	}
	if got := b.frameSize(4*slotInlineLimit, 0, 0, defaultSegmentBytes); got != slotInlineLimit {
		t.Fatal("a drained publisher did not restore fresh-burst admission")
	}
}

func TestSlotBatchAccountsBytesAndRequestsAcrossInputChunks(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		buffered, outstanding int
		active                int
	}{
		{"full_inline_backlog", 64 << 10, 10 * slotInlineLimit, 10},
		{"bulk_backlog", 64 << 10, 2 * defaultSegmentBytes, 3},
		{"small_request_backlog", 8 * slotInlineLimit, 1024, 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newSlotBatchWindow(defaultSegmentBytes, 64)
			if got := b.frameSize(tc.buffered, tc.outstanding, tc.active, defaultSegmentBytes); got != tc.buffered {
				t.Fatalf("backlogged %d bytes split into %d-byte inline frames", tc.buffered, got)
			}
			if got := b.frameSize(123, tc.outstanding, tc.active, defaultSegmentBytes); got != 123 {
				t.Fatal("small suffix was forced into media")
			}
			if got := b.frameSize(32<<10, tc.outstanding, tc.active, defaultSegmentBytes); got != slotInlineLimit {
				t.Fatal("short bursts spent a media GET despite fitting in a few inline frames")
			}
		})
	}
}

func TestSlotBatchBudgetLearnsPublicationCapacityAndPreservesOutages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newSlotBatchWindow(1024, 64)
		for range latencyWindowWarm {
			b.observe(0, time.Second)
		}
		if b.budget != b.window*b.unit {
			t.Fatal("latency without publication capacity changed the cold budget")
		}
		for range 20 {
			time.Sleep(100 * time.Millisecond)
			b.observe(b.unit, time.Second)
		}
		if b.budget != 11*b.unit {
			t.Fatalf("healthy 10 KiB/s budget=%d, want %d", b.budget, 11*b.unit)
		}
		time.Sleep(20 * time.Second)
		b.observe(b.unit, 4*time.Second)
		if b.budget != 11*b.unit {
			t.Fatal("a publication outage erased demonstrated capacity")
		}
		for range 40 {
			time.Sleep(25 * time.Millisecond)
			b.observe(b.unit, time.Second)
		}
		if b.budget != b.window*b.unit {
			t.Fatal("higher useful capacity did not restore the ordinary ceiling")
		}
		for range 20 {
			time.Sleep(500 * time.Millisecond)
			b.observe(b.unit, time.Second)
		}
		if b.budget != bodyGateMin*b.unit {
			t.Fatal("sustained lower capacity did not reach the bounded minimum")
		}
	})
}

func TestSlotBatchSmallSegmentsAndRingsStayBounded(t *testing.T) {
	b := newSlotBatchWindow(1024, 2)
	if b.unit != 1024 || b.budget != 2048 || b.window != 2 {
		t.Fatal("configured segment/ring bounds were lost")
	}
	if got := b.frameSize(1024, 8192, 2, 1024); got != 1024 {
		t.Fatal("an inline-only configured segment was held behind debt")
	}
	if got := b.frameSize(4096, 8192, 2, 1024); got != 1024 {
		t.Fatal("media exceeded the configured segment ceiling")
	}
}

type batchTestSlots struct {
	*memSlots
	release   chan struct{}
	streaming bool
}

func (s *batchTestSlots) throttled() int64       { return 0 }
func (s *batchTestSlots) streamingEnabled() bool { return s.streaming }

func (s *batchTestSlots) setSlot(ctx context.Context, id, description string, body []byte) error {
	frame, ok := decodeSlot(description)
	if ok && (frame.mode == slotInline || frame.mode == slotMedia || frame.mode == slotStream) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.release:
		}
	}
	return s.memSlots.setSlot(ctx, id, description, body)
}

func TestSlotWriterBacklogBatchingPreservesBytesAndSmallSuffix(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		name := "legacy"
		if streaming {
			name = "streaming"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				store := &batchTestSlots{memSlots: newMemSlots(), release: make(chan struct{}), streaming: streaming}
				p := paramsFromConfig(&Config{Concurrency: 64, FlushIntervalMs: 10})
				ring, err := makeRing(ctx, store, "batch", p)
				if err != nil {
					t.Fatal(err)
				}
				w := newSlotWriter(ctx, store, ring.Up, p)
				defer func() { cancel(); w.wait() }()
				payload := randomPayload(3 * (64 << 10))
				for off := 0; off < len(payload); off += 64 << 10 {
					if _, err := w.Write(payload[off : off+(64<<10)]); err != nil {
						t.Fatal(err)
					}
				}
				time.Sleep(2 * p.flushInterval)
				synctest.Wait()
				w.mu.Lock()
				assigned, pending, target := w.seq, w.pendingUploadBytes, w.size
				w.mu.Unlock()
				if assigned != 11 || pending != len(payload) || target != p.segmentBytes {
					t.Fatalf("assigned=%d pending=%d target=%d", assigned, pending, target)
				}
				suffix := []byte("small sparse control suffix")
				if _, err := w.Write(suffix); err != nil {
					t.Fatal(err)
				}
				time.Sleep(3 * p.flushInterval)
				synctest.Wait()
				w.mu.Lock()
				assigned = w.seq
				w.mu.Unlock()
				if assigned != 12 {
					t.Fatal("small suffix lost its existing stable-flush admission")
				}
				close(store.release)
				w.wg.Wait()
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				store.mu.Lock()
				defer store.mu.Unlock()
				var got []byte
				for seq := int64(0); seq < assigned; seq++ {
					id := w.slotID(seq)
					frame, ok := decodeSlot(store.descs[id])
					if !ok || frame.seq != seq {
						t.Fatalf("invalid published frame %d", seq)
					}
					data := frame.data
					if seq == 10 {
						wantMode := byte(slotMedia)
						if streaming {
							wantMode = slotStream
						}
						if frame.mode != wantMode {
							t.Fatal("backlog did not select the negotiated media format")
						}
						data = store.bodies[id]
						if !frame.holds(data) {
							t.Fatal("batched media checksum or length changed")
						}
					} else if frame.mode != slotInline {
						t.Fatal("initial burst or small suffix unexpectedly became media")
					}
					got = append(got, data...)
				}
				if !bytes.Equal(got, append(payload, suffix...)) {
					t.Fatal("backlog batching changed ordered bytes")
				}
				end, ok := decodeSlot(store.descs[w.slotID(assigned)])
				if !ok || end.seq != assigned || end.mode != slotEnd {
					t.Fatal("Close lost its ordered terminal frame")
				}
			})
		})
	}
}
