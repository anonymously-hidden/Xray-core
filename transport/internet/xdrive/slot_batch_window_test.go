package xdrive

import (
	"bytes"
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestSlotBatchCommittedBurstSurvivesChangingPressure(t *testing.T) {
	b := newSlotBatchWindow(defaultSegmentBytes, 64)
	buffered := 448 << 10
	for i := 0; buffered > 0; i++ {
		if got := b.frameSize(buffered, i*slotInlineLimit, min(i, slotInlineWriteInflight-1), defaultSegmentBytes); got != slotInlineLimit {
			t.Fatalf("committed frame %d became %d-byte media", i, got)
		}
		buffered -= slotInlineLimit
	}
	if b.planned != 0 {
		t.Fatal("completed inline plan retained bytes")
	}
	if got := b.frameSize(5*slotInlineLimit, b.budget, b.window-1, defaultSegmentBytes); got != 5*slotInlineLimit {
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
		{"full_inline_backlog", 64 << 10, (slotInlineWriteInflight - 2) * slotInlineLimit, slotInlineWriteInflight - 2},
		{"bulk_backlog", 64 << 10, 2 * defaultSegmentBytes, 3},
		{"small_request_backlog", (slotInlineWriteInflight/2 + 2) * slotInlineLimit, 1024, slotInlineWriteInflight - 1},
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
	for _, buffered := range []int{167230, 192207, 272702, 420 << 10} {
		b := newSlotBatchWindow(defaultSegmentBytes, 64)
		if got := b.frameSize(buffered, 4096, 2, defaultSegmentBytes); got != slotInlineLimit || b.planned != buffered-slotInlineLimit {
			t.Fatalf("a fitting %d-byte burst with spare credits was forced into media", buffered)
		}
		b = newSlotBatchWindow(defaultSegmentBytes, 64)
		if got := b.frameSize(buffered, b.budget, 11, defaultSegmentBytes); got != buffered {
			t.Fatal("actual byte backlog was ignored to force a modest burst inline")
		}
	}
}

func TestSlotBatchTwoWaveAllowanceRequiresSpareCredits(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		depth, active         int
		outstanding, buffered int
	}{
		{"request_pressure", 64, slotInlineWriteInflight/2 + 1, 1024, 420 << 10},
		{"byte_pressure", 64, 2, slotInlineWriteInflight*slotInlineLimit/2 + 1, 400 << 10},
		{"two_wave_ceiling", 64, 0, 0, 448<<10 + 1},
		{"ring_ceiling", 8, 0, 0, 8*slotInlineLimit + 1},
		{"reserved_request_credit", 64, 2, 1024, 31 * slotInlineLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newSlotBatchWindow(defaultSegmentBytes, tc.depth)
			if got := b.frameSize(tc.buffered, tc.outstanding, tc.active, defaultSegmentBytes); got != min(tc.buffered, defaultSegmentBytes) || b.planned != 0 {
				t.Fatal("a two-wave plan escaped its pressure or resource ceiling")
			}
		})
	}
}

func TestSlotBatchSmallSegmentsAndRingsStayBounded(t *testing.T) {
	b := newSlotBatchWindow(1024, 2)
	if b.unit != 1024 || b.budget != 2048 || b.burstBudget != 2048 || b.window != 2 {
		t.Fatal("configured segment/ring bounds were lost")
	}
	if got := b.frameSize(1024, 8192, 2, 1024); got != 1024 {
		t.Fatal("an inline-only configured segment was held behind debt")
	}
	if got := b.frameSize(4096, 8192, 2, 1024); got != 1024 {
		t.Fatal("media exceeded the configured segment ceiling")
	}
}

func TestSlotWriterInlineAdmissionDoesNotWidenBulkWindow(t *testing.T) {
	w := &slotWriter{
		table: slotTable{Slots: make([]string, slotBulkDepth)},
		slots: make(chan struct{}, slotBulkInflight),
		batch: newSlotBatchWindow(defaultSegmentBytes, slotBulkDepth),
	}
	w.buf = make([]byte, 272702)
	if w.writeWindow() != slotInlineWriteInflight || w.batch.planned != 0 {
		t.Fatal("inline admission did not use its own limit or prematurely committed a plan")
	}
	w.pendingUploadBytes = defaultSegmentBytes
	if w.writeWindow() != slotWriteInflight {
		t.Fatal("backlogged media inherited the wider inline window")
	}
	w.pendingUploadBytes = 0
	w.buf = make([]byte, 2*defaultSegmentBytes)
	if w.writeWindow() != slotWriteInflight {
		t.Fatal("large unplanned media inherited inline admission")
	}
	w.bulkSegments = 2 * slotBulkDepth
	if w.writeWindow() != slotBulkInflight {
		t.Fatal("the existing warmed bulk ceiling changed")
	}
	w.batch.planned = slotInlineLimit
	if w.writeWindow() != slotInlineWriteInflight {
		t.Fatal("a committed inline prefix lost its own admission limit")
	}
}

type burstStragglerSlots struct{ *batchTestSlots }

func (s burstStragglerSlots) setSlot(ctx context.Context, id, description string, body []byte) error {
	if frame, ok := decodeSlot(description); ok && frame.mode == slotInline && frame.seq == 1 && len(frame.data) == slotInlineLimit {
		if err := batchTestWait(ctx, 4*time.Second); err != nil {
			return err
		}
	}
	return s.batchTestSlots.setSlot(ctx, id, description, body)
}

func TestSlotWriterFullBurstAvoidsInlineStraggler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		store := burstStragglerSlots{&batchTestSlots{
			memSlots: newMemSlots(), warm: 1 << 20,
			latency: 600 * time.Millisecond, mediaLatency: 1100 * time.Millisecond,
			descriptionLatency: 300 * time.Millisecond, bodyLatency: 900 * time.Millisecond,
		}}
		p := paramsFromConfig(&Config{Concurrency: 64, FlushIntervalMs: 10})
		ring, err := makeRing(ctx, store, "inline-straggler", p)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		w := newSlotWriter(ctx, store, ring.Up, p)
		// This covers the media path a full burst takes without the live
		// split, XRAY_XDRIVE_LIVE_HYBRID=0; the split has its own tests.
		w.mu.Lock()
		w.liveHybrid = false
		w.mu.Unlock()
		r := newSlotReader(ctx, store, ring.Up, p, nil)
		defer func() { cancel(); w.wait(); r.wait() }()
		payload := randomPayload(defaultSegmentBytes)
		want := append(payload, []byte("following control bytes")...)
		began := time.Now()
		written := make(chan error, 1)
		go func() { _, err := w.Write(want); written <- err }()
		var got []byte
		for len(got) < len(want) {
			select {
			case chunk, ok := <-r.chunks():
				if !ok {
					t.Fatal("reader ended before control bytes")
				}
				got = append(got, chunk...)
			case <-time.After(8 * time.Second):
				t.Fatal("ordered delivery stalled")
			}
		}
		if err := <-written; err != nil || !bytes.Equal(got, want) {
			t.Fatalf("ordered delivery changed bytes: %v", err)
		}
		elapsed := time.Since(began)
		t.Logf("burst plus control delivered in %v", elapsed)
		if elapsed > 3*time.Second {
			t.Fatalf("oversized burst held the control suffix for %v", elapsed)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		store.mu.Lock()
		defer store.mu.Unlock()
		if w.seq != 2 || len(store.bodies) != 1 {
			t.Fatalf("want one media upload and one inline suffix, got %d payload frames and %d media uploads", w.seq, len(store.bodies))
		}
	})
}

type batchTestSlots struct {
	*memSlots
	release                                       chan struct{}
	streaming                                     bool
	warm                                          int64
	latency                                       time.Duration
	mediaLatency, descriptionLatency, bodyLatency time.Duration
	active                                        atomic.Int32
	peak                                          atomic.Int32
}

func (s *batchTestSlots) throttled() int64       { return 0 }
func (s *batchTestSlots) streamingEnabled() bool { return s.streaming }

func (s *batchTestSlots) setSlot(ctx context.Context, id, description string, body []byte) error {
	frame, ok := decodeSlot(description)
	if ok && (frame.mode == slotInline || frame.mode == slotMedia || frame.mode == slotStream) {
		n := s.active.Add(1)
		defer s.active.Add(-1)
		for peak := s.peak.Load(); n > peak; peak = s.peak.Load() {
			if s.peak.CompareAndSwap(peak, n) {
				break
			}
		}
		var release <-chan struct{}
		var tick <-chan time.Time
		if frame.seq < s.warm {
			latency := s.latency
			if frame.mode != slotInline && s.mediaLatency > 0 {
				latency = s.mediaLatency
			}
			timer := time.NewTimer(latency)
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

func batchTestWait(ctx context.Context, d time.Duration) error {
	if d == 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *batchTestSlots) readDescription(ctx context.Context, id string) (string, error) {
	if err := batchTestWait(ctx, s.descriptionLatency); err != nil {
		return "", err
	}
	return s.memSlots.readDescription(ctx, id)
}

func (s *batchTestSlots) readBody(ctx context.Context, id string) ([]byte, error) {
	if err := batchTestWait(ctx, s.bodyLatency); err != nil {
		return nil, err
	}
	return s.memSlots.readBody(ctx, id)
}

func TestSlotLiveSizedBurstDelivery(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window int
		ahead  int64
	}{{"resume57", 16, 3}, {"upload_only", slotInlineWriteInflight, 3}, {"resume58", slotInlineWriteInflight, slotReadAhead}} {
		t.Run(tc.name, func(t *testing.T) {
			previous, previousBurst := slotReadAhead, slotBurstReadAhead
			slotReadAhead = tc.ahead
			if tc.ahead < previous {
				// resume57/upload_only model the older, narrower reader.
				slotBurstReadAhead = tc.ahead
			}
			defer func() { slotReadAhead, slotBurstReadAhead = previous, previousBurst }()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				store := &batchTestSlots{
					memSlots: newMemSlots(), warm: 1 << 20,
					latency: 600 * time.Millisecond, mediaLatency: 1100 * time.Millisecond,
					descriptionLatency: 300 * time.Millisecond, bodyLatency: 900 * time.Millisecond,
				}
				p := paramsFromConfig(&Config{Concurrency: 64, FlushIntervalMs: 10})
				ring, err := makeRing(ctx, store, "live-sized", p)
				if err != nil {
					t.Fatal(err)
				}
				w := newSlotWriter(ctx, store, ring.Up, p)
				w.batch.window = tc.window
				w.batch.budget = tc.window * w.batch.unit
				w.batch.burstBudget = slotInlineBurstWaves * w.batch.budget
				r := newSlotReader(ctx, store, ring.Up, p, nil)
				defer func() { cancel(); w.wait(); r.wait() }()
				payload := randomPayload(defaultSegmentBytes)
				began := time.Now()
				written := make(chan error, 1)
				go func() { _, err := w.Write(payload); written <- err }()
				var got []byte
				var first time.Duration
				for len(got) < len(payload) {
					select {
					case chunk, ok := <-r.chunks():
						if !ok {
							t.Fatal("reader ended before the burst")
						}
						if len(got) == 0 {
							first = time.Since(began)
						}
						got = append(got, chunk...)
					case <-time.After(10 * time.Second):
						t.Fatal("live-sized burst did not drain")
					}
				}
				if err := <-written; err != nil || !bytes.Equal(got, payload) {
					t.Fatal("live-sized burst changed bytes", err)
				}
				store.mu.Lock()
				media := len(store.bodies)
				store.mu.Unlock()
				if store.peak.Load() > int32(inlineUploadCeiling(w, tc.window)) || tc.name == "resume57" && media != 1 || tc.name != "resume57" && media != 0 {
					t.Fatal("publication class or upload ceiling changed")
				}
				if tc.name == "resume58" && (first > time.Second || time.Since(began) > 1700*time.Millisecond) {
					t.Fatal("a healthy 512 KiB burst still stalled behind media or distant metadata admission")
				}
				t.Logf("window=%d ahead=%d first=%v complete=%v media=%d peak=%d", tc.window, tc.ahead, first, time.Since(began), media, store.peak.Load())
			})
		})
	}
}

func TestSlotWriterBacklogDoesNotRenewInlinePlanAfterPermitWait(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		name := "legacy"
		if streaming {
			name = "streaming"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				store := &batchTestSlots{
					memSlots: newMemSlots(), streaming: streaming, warm: 1 << 20,
					latency: 600 * time.Millisecond, mediaLatency: 1100 * time.Millisecond,
					descriptionLatency: 300 * time.Millisecond, bodyLatency: 900 * time.Millisecond,
				}
				p := paramsFromConfig(&Config{Concurrency: 64, FlushIntervalMs: 10, SegmentBytes: 448 << 10})
				ring, err := makeRing(ctx, store, "sustained", p)
				if err != nil {
					t.Fatal(err)
				}
				w := newSlotWriter(ctx, store, ring.Up, p)
				r := newSlotReader(ctx, store, ring.Up, p, nil)
				defer func() { cancel(); w.wait(); r.wait() }()
				payload := randomPayload(4 * p.segmentBytes)
				suffix := []byte("browsing control after sustained input")
				want := append(payload, suffix...)
				began := time.Now()
				written := make(chan error, 1)
				go func() {
					_, err := w.Write(want)
					written <- err
				}()
				var got []byte
				for len(got) < len(want) {
					select {
					case chunk, ok := <-r.chunks():
						if !ok {
							t.Fatal("reader ended before sustained input and suffix")
						}
						got = append(got, chunk...)
					case <-time.After(20 * time.Second):
						t.Fatal("sustained input held back the browsing suffix")
					}
				}
				if err := <-written; err != nil || !bytes.Equal(got, want) {
					t.Fatal("sustained batching changed ordered bytes", err)
				}
				store.mu.Lock()
				media := len(store.bodies)
				store.mu.Unlock()
				burstFrames := (p.segmentBytes + slotInlineLimit - 1) / slotInlineLimit
				if media != 3 || w.seq != int64(burstFrames+4) {
					t.Fatalf("backlog renewed inline plans: media=%d frames=%d", media, w.seq)
				}
				if store.peak.Load() > int32(liveUploadCeiling(w, slotInlineWriteInflight)) {
					t.Fatal("sustained batching exceeded upload admission")
				}
				if elapsed := time.Since(began); elapsed > 4*time.Second {
					t.Fatalf("browsing suffix waited %v behind sustained input", elapsed)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				store.mu.Lock()
				defer store.mu.Unlock()
				for seq := int64(0); seq < w.seq; seq++ {
					frame, ok := decodeSlot(store.descs[w.slotID(seq)])
					mode := byte(slotInline)
					if seq >= int64(burstFrames) && seq < int64(burstFrames+3) {
						mode = slotMedia
						if streaming {
							mode = slotStream
						}
					}
					if !ok || frame.seq != seq || frame.mode != mode {
						t.Fatalf("publication class changed at sequence %d", seq)
					}
				}
				end, ok := decodeSlot(store.descs[w.slotID(w.seq)])
				if !ok || end.seq != w.seq || end.mode != slotEnd {
					t.Fatal("sustained batching lost the ordered terminal frame")
				}
				t.Logf("four segments plus browsing suffix: frames=%d media=%d complete=%v", w.seq, media, time.Since(began))
			})
		})
	}
}

func TestSlotWriterSparseTrafficDoesNotForceModestBurstIntoMedia(t *testing.T) {
	for _, size := range []int{167230, 192207, 272702, 420 << 10} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				store := &batchTestSlots{memSlots: newMemSlots(), release: make(chan struct{}), streaming: true, warm: 24, latency: 700 * time.Millisecond}
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
				if first != store.warm+2 || pending != 2*len(control) || budget != slotInlineWriteInflight*slotInlineLimit {
					t.Fatalf("sparse traffic consumed capacity: seq=%d pending=%d budget=%d", first, pending, budget)
				}
				payload := randomPayload(size)
				written := make(chan error, 1)
				go func() { _, err := w.Write(payload); written <- err }()
				synctest.Wait()
				w.mu.Lock()
				result := w.gates[first%int64(len(w.gates))]
				inline := result != nil && result.accountedBytes == slotInlineLimit && result.inlinePlannedBytes == len(payload)-slotInlineLimit
				w.mu.Unlock()
				if !inline {
					t.Fatal("sparse publication demand converted a fitting burst into media")
				}
				close(store.release)
				if err := <-written; err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				if store.peak.Load() > int32(inlineUploadCeiling(w, slotInlineWriteInflight)) {
					t.Fatal("a two-wave inline plan exceeded inline upload concurrency")
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
		})
	}
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
				// A small ring keeps the withheld uploads outside two-wave inline credit.
				p := paramsFromConfig(&Config{Concurrency: 16, FlushIntervalMs: 10})
				ring, err := makeRing(ctx, store, "batch", p)
				if err != nil {
					t.Fatal(err)
				}
				w := newSlotWriter(ctx, store, ring.Up, p)
				defer func() { cancel(); w.wait() }()
				payload := randomPayload(2*(64<<10) + 2*defaultSegmentBytes)
				for off := 0; off < 2*(64<<10); off += 64 << 10 {
					if _, err := w.Write(payload[off : off+(64<<10)]); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := w.Write(payload[2*(64<<10):]); err != nil {
					t.Fatal(err)
				}
				time.Sleep(2 * p.flushInterval)
				synctest.Wait()
				w.mu.Lock()
				assigned, pending, target := w.seq, w.pendingUploadBytes, w.size
				w.mu.Unlock()
				if assigned != 12 || pending != len(payload) || target != p.segmentBytes {
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
				if assigned != 13 {
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
					if seq == 10 || seq == 11 {
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

func TestSlotWriterCommittedBurstPublishesInOneWave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		store := &batchTestSlots{memSlots: newMemSlots(), warm: 1 << 20, latency: 700 * time.Millisecond}
		p := paramsFromConfig(&Config{Concurrency: 64, FlushIntervalMs: 10})
		ring, err := makeRing(ctx, store, "wave", p)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		w := newSlotWriter(ctx, store, ring.Up, p)
		defer func() { cancel(); w.wait() }()
		// An established writer has ramped to the bulk aggregation size.
		w.mu.Lock()
		w.size = p.segmentBytes
		w.mu.Unlock()
		payload := randomPayload(178036)
		if _, err := w.Write(payload); err != nil {
			t.Fatal(err)
		}
		began := time.Now()
		last := w.slotID(int64((len(payload) - 1) / slotInlineLimit))
		for time.Since(began) < 2*time.Second {
			time.Sleep(p.flushInterval)
			synctest.Wait()
			store.mu.Lock()
			f, ok := decodeSlot(store.descs[last])
			store.mu.Unlock()
			if ok && f.mode == slotInline {
				break
			}
		}
		elapsed := time.Since(began)
		t.Logf("last frame published at %v", elapsed)
		if elapsed > store.latency+3*p.flushInterval {
			t.Fatalf("committed burst exceeded one upload wave: %v", elapsed)
		}
		store.mu.Lock()
		defer store.mu.Unlock()
		var got []byte
		for seq := int64(0); len(got) < len(payload); seq++ {
			f, ok := decodeSlot(store.descs[w.slotID(seq)])
			if !ok || f.seq != seq || f.mode != slotInline {
				t.Fatalf("only %d/%d bytes published within one upload wave", len(got), len(payload))
			}
			got = append(got, f.data...)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("burst corrupted")
		}
	})
}

type overlappingPrefixSlots struct {
	*batchTestSlots
	cool bool
}

func (s overlappingPrefixSlots) cooling() bool { return s.cool }

func (s overlappingPrefixSlots) readFrameStream(ctx context.Context, id string, frame slotFrame, offer func(int, []byte) error) error {
	body, err := s.readBody(ctx, id)
	if err != nil {
		return err
	}
	for i := range frame.blocks {
		start := i * slotStreamBlockBytes
		if err := offer(i, body[start:start+streamBlockSize(frame.size, i)]); err != nil {
			return err
		}
	}
	return nil
}

func TestSlotProgressivePrefixDuringOverlap(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		progressive, cooldown, wantPrefix bool
	}{
		{"legacy", false, false, false},
		{"progressive", true, false, true},
		{"cooldown", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				store := overlappingPrefixSlots{&batchTestSlots{
					memSlots: newMemSlots(), streaming: true, warm: 1 << 20,
					latency: 600 * time.Millisecond, mediaLatency: 1100 * time.Millisecond,
					descriptionLatency: 300 * time.Millisecond, bodyLatency: 900 * time.Millisecond,
				}, tc.cooldown}
				p := paramsFromConfig(&Config{Concurrency: 64})
				ring, err := makeRing(ctx, store, "progressive-overlap", p)
				if err != nil {
					t.Fatal(err)
				}
				ring.Down.streamPrefix, ring.Down.progressiveMedia = true, tc.progressive
				r := newSlotReader(ctx, store, ring.Down, p, nil)
				w := newSlotWriter(ctx, store, ring.Down, p)
				// The prefix belongs to a full media frame, which the live
				// split would turn into an inline wave and a shorter frame.
				w.mu.Lock()
				w.liveHybrid = false
				w.mu.Unlock()
				defer func() { cancel(); w.wait(); r.wait() }()
				control := bytes.Repeat([]byte("c"), slotInlineLimit)
				payload := randomPayload(defaultSegmentBytes)
				start := time.Now()
				if _, err := w.Write(control); err != nil {
					t.Fatal(err)
				}
				if _, err := w.Write(payload); err != nil {
					t.Fatal(err)
				}
				if got := receiveStreamChunk(t, r); !bytes.Equal(got, control) {
					t.Fatal("media overtook earlier control bytes")
				}
				got := receiveStreamChunk(t, r)
				first := time.Since(start)
				if (len(got) == slotStreamPrefixBytes) != tc.wantPrefix {
					t.Fatalf("first media chunk was %d bytes at %v", len(got), first)
				}
				for len(got) < len(payload) {
					got = append(got, receiveStreamChunk(t, r)...)
				}
				last := time.Since(start)
				t.Logf("first media=%v complete=%v frames=%d", first, last, w.seq)
				store.mu.Lock()
				media := len(store.bodies)
				store.mu.Unlock()
				if !bytes.Equal(got, payload) || w.seq != 2 || media != 1 || last-first < 900*time.Millisecond && tc.wantPrefix {
					t.Fatal("unexpected burst bytes, timing or request count")
				}
			})
		})
	}
}
