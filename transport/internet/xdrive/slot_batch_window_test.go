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

func TestSlotBatchModestBurstSharesCreditsWithSmallRequests(t *testing.T) {
	const buffered = 167230
	b := newSlotBatchWindow(defaultSegmentBytes, 64)
	if got := b.frameSize(buffered, 4096, 2, defaultSegmentBytes); got != slotInlineLimit || b.planned != buffered-slotInlineLimit {
		t.Fatal("a modest burst with available byte/request credit was forced into media")
	}
	b = newSlotBatchWindow(defaultSegmentBytes, 64)
	if got := b.frameSize(buffered, b.budget, 11, defaultSegmentBytes); got != buffered {
		t.Fatal("actual byte backlog was ignored to force a modest burst inline")
	}
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
	warm      int64
	latency   time.Duration
}

func (s *batchTestSlots) throttled() int64       { return 0 }
func (s *batchTestSlots) streamingEnabled() bool { return s.streaming }

func (s *batchTestSlots) setSlot(ctx context.Context, id, description string, body []byte) error {
	frame, ok := decodeSlot(description)
	if ok && (frame.mode == slotInline || frame.mode == slotMedia || frame.mode == slotStream) {
		var release <-chan struct{}
		var tick <-chan time.Time
		if frame.seq < s.warm {
			timer := time.NewTimer(s.latency)
			defer timer.Stop()
			tick = timer.C
		} else {
			release = s.release
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
		case <-tick:
		}
	}
	return s.memSlots.setSlot(ctx, id, description, body)
}

func TestSlotWriterSparseTrafficDoesNotForceModestBurstIntoMedia(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		store := &batchTestSlots{memSlots: newMemSlots(), release: make(chan struct{}), streaming: true, warm: 32, latency: 700 * time.Millisecond}
		p := paramsFromConfig(&Config{Concurrency: 64, FlushIntervalMs: 10})
		ring, err := makeRing(ctx, store, "sparse", p)
		if err != nil {
			t.Fatal(err)
		}
		w := newSlotWriter(ctx, store, ring.Up, p)
		defer func() { cancel(); w.wait() }()
		control := bytes.Repeat([]byte{1}, 89)
		for range store.warm {
			if _, err := w.Write(control); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Second)
			w.wg.Wait()
		}
		for range 2 {
			if _, err := w.Write(control); err != nil {
				t.Fatal(err)
			}
			time.Sleep(3 * p.flushInterval)
		}
		synctest.Wait()
		w.mu.Lock()
		first, pending, budget := w.seq, w.pendingUploadBytes, w.batch.budget
		w.mu.Unlock()
		if first != store.warm+2 || pending != 2*len(control) || budget != slotWriteInflight*slotInlineLimit {
			t.Fatalf("sparse traffic consumed capacity: seq=%d pending=%d budget=%d", first, pending, budget)
		}
		payload := randomPayload(167230)
		if _, err := w.Write(payload); err != nil {
			t.Fatal(err)
		}
		w.mu.Lock()
		result := w.gates[first%int64(len(w.gates))]
		inline := result != nil && result.accountedBytes == slotInlineLimit && result.inlinePlannedBytes == len(payload)-slotInlineLimit
		w.mu.Unlock()
		if !inline {
			t.Fatal("sparse publication demand converted a fitting burst into media")
		}
		close(store.release)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		store.mu.Lock()
		defer store.mu.Unlock()
		if len(store.bodies) != 0 {
			t.Fatal("a fitting burst still required media retrieval")
		}
		var got []byte
		for seq := first; seq < w.seq; seq++ {
			frame, ok := decodeSlot(store.descs[w.slotID(seq)])
			if !ok || frame.seq != seq || frame.mode != slotInline {
				t.Fatalf("invalid ordered inline frame %d", seq)
			}
			got = append(got, frame.data...)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("inline planning lost, duplicated, or reordered burst bytes")
		}
	})
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
