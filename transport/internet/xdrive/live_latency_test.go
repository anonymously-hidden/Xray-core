package xdrive

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// liveCadenceFixture publishes the successor of a long streamed head part-way
// through that head's media download and records when the reader first sees it.
type liveCadenceFixture struct {
	*streamReaderFixture
	publishAt time.Time
	seen      chan time.Time
}

func (s *liveCadenceFixture) readDescription(ctx context.Context, id string) (string, error) {
	if id != "in1" {
		return s.streamReaderFixture.readDescription(ctx, id)
	}
	if time.Now().Before(s.publishAt) {
		return "", nil
	}
	select {
	case s.seen <- time.Now():
	default:
	}
	return newSlotFrame(1, slotEnd, nil).encode(), nil
}

func (s *liveCadenceFixture) readFrameStream(ctx context.Context, id string, frame slotFrame, emit func(int, []byte) error) error {
	for i := range frame.blocks {
		if err := batchTestWait(ctx, 550*time.Millisecond); err != nil {
			return err
		}
		start := i * slotStreamBlockBytes
		if err := emit(i, bytes.Clone(s.data[start:start+streamBlockSize(frame.size, i)])); err != nil {
			return err
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.eof:
		return nil
	}
}

// A live segment split across frames needs the next frame found while the
// current one is still downloading. Before the fix the near successor fell
// back to the ordinary tail pace (up to tailMax) once the post-hit burst
// window expired, even though verified blocks kept arriving.
func TestSuccessorPollsQuicklyWhileHeadMediaDownloads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Model a live pace that keeps the ordinary near-tail wait at its ceiling.
		oldTailMin := slotTailMin
		slotTailMin = slotTailMax
		defer func() { slotTailMin = oldTailMin }()

		data := bytes.Repeat([]byte("l"), 8*slotStreamBlockBytes)
		store := &liveCadenceFixture{
			streamReaderFixture: newStreamReaderFixture(data),
			publishAt:           time.Now().Add(4250 * time.Millisecond),
			seen:                make(chan time.Time, 1),
		}
		ctx, cancel := context.WithCancel(context.Background())
		reader := newSlotReader(ctx, store, slotTable{Slots: []string{"in0", "in1"}, Watermark: "wm", progressiveMedia: true}, streamReaderParams(), nil)
		defer func() { cancel(); reader.wait() }()

		for range store.frame.blocks {
			receiveStreamChunk(t, reader)
		}
		var seenAt time.Time
		select {
		case seenAt = <-store.seen:
		case <-time.After(2 * time.Second):
			t.Fatal("the successor was never discovered")
		}
		if late := seenAt.Sub(store.publishAt); late > 150*time.Millisecond {
			t.Fatalf("successor discovered %v after publication while head media was downloading", late)
		}
		close(store.eof)
		select {
		case _, ok := <-reader.chunks():
			if ok || reader.Err() != io.EOF {
				t.Fatalf("expected ordered completion, got %v", reader.Err())
			}
		case <-time.After(2 * time.Second):
			t.Fatal("discovered successor was not delivered")
		}
	})
}

func TestBodyGateActiveTracksRetrieval(t *testing.T) {
	var g bodyGate
	g.configure(0, defaultSegmentBytes, slotBulkDepth)
	if g.active() {
		t.Fatal("an empty gate reported active retrieval")
	}
	p, ok := g.acquire(context.Background(), 0, 1024)
	if !ok {
		t.Fatal("the head was not admitted")
	}
	if !g.active() {
		t.Fatal("an admitted retrieval was not reported active")
	}
	p.finish(time.Millisecond, true)
	if g.active() {
		t.Fatal("a finished retrieval was still reported active")
	}
}

// inlineUploadCeiling is the most concurrent inline uploads a writer may run
// for a committed plan: the ordinary window, or the single-wave plan width.
func inlineUploadCeiling(w *slotWriter, window int) int {
	if w.singleWave {
		return max(window, min(cap(w.slots), slotInlineBurstFrames))
	}
	return window
}

func TestSlotWriterUsesSingleWaveDefault(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := paramsFromConfig(&Config{Concurrency: 64})
	store := &batchTestSlots{memSlots: newMemSlots()}
	ring, err := makeRing(ctx, store, "default", p)
	if err != nil {
		t.Fatal(err)
	}
	w := newSlotWriter(ctx, store, ring.Up, p)
	defer func() { cancel(); w.wait() }()
	if w.singleWave != slotInlineSingleWave {
		t.Fatal("the writer did not capture the process setting")
	}
	if w.singleWave && cap(w.slots) < slotInlineBurstFrames {
		t.Fatalf("permit channel %d cannot cover a single-wave plan", cap(w.slots))
	}
}

type recoveringSlots struct{ *memSlots }

func (recoveringSlots) cooling() bool              { return true }
func (recoveringSlots) lastRateLimitAt() time.Time { return time.Now() }

func TestSlotWriterSingleWaveInlineOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		w := &slotWriter{
			table:      slotTable{Slots: make([]string, slotBulkDepth)},
			slots:      make(chan struct{}, min(slotBulkDepth, slotPermitCap(enabled))),
			batch:      newSlotBatchWindow(defaultSegmentBytes, slotBulkDepth),
			singleWave: enabled,
		}
		want := slotInlineWriteInflight
		if enabled {
			want = slotInlineBurstFrames
		}
		// The flusher sends on this channel while holding mu.
		if cap(w.slots) < want {
			t.Fatalf("enabled=%v: permit channel %d cannot cover inline window %d", enabled, cap(w.slots), want)
		}
		w.buf = make([]byte, 400<<10)
		if got := w.writeWindow(); got != slotInlineWriteInflight {
			t.Fatalf("enabled=%v: fresh plan admission changed to %d", enabled, got)
		}
		w.batch.planned = 300 << 10
		if got := w.writeWindow(); got != want {
			t.Fatalf("enabled=%v: committed plan window %d, want %d", enabled, got, want)
		}
		w.store = recoveringSlots{}
		if got := w.writeWindow(); got != slotInlineWriteInflight {
			t.Fatalf("enabled=%v: quota recovery kept window %d", enabled, got)
		}
		w.store = nil
		w.batch.planned = 0
		w.buf = make([]byte, 2*defaultSegmentBytes)
		w.bulkSegments = 2 * slotBulkDepth
		if got := w.writeWindow(); got != slotBulkInflight {
			t.Fatalf("enabled=%v: warmed bulk ceiling changed to %d", enabled, got)
		}
	}
}

// P1. liveBurstSlots is a Drive-like batch store whose cooldown can be forced.
type liveBurstSlots struct {
	*batchTestSlots
	cool bool
}

func (s liveBurstSlots) cooling() bool            { return s.cool }
func (liveBurstSlots) lastRateLimitAt() time.Time { return time.Time{} }

// A ~200 KB live segment written while unrelated media is still uploading must
// stay inline. In the aparat logs such segments became single media frames
// (3.8-4 s end to end) beside page loads. Sustained transfers, quota recovery
// and the escape hatch keep the old media decision.
func TestSlotWriterLiveBurstStaysInlineBesideBacklog(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		sustained, cooldown, disabled bool
		inline                        bool
	}{
		{"busy", false, false, false, true},
		{"sustained", true, false, false, false},
		{"cooldown", false, true, false, false},
		{"disabled", false, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				store := liveBurstSlots{&batchTestSlots{memSlots: newMemSlots(), release: make(chan struct{})}, tc.cooldown}
				p := paramsFromConfig(&Config{Concurrency: 64, FlushIntervalMs: 10})
				ring, err := makeRing(ctx, store, "live-backlog", p)
				if err != nil {
					cancel()
					t.Fatal(err)
				}
				w := newSlotWriter(ctx, store, ring.Up, p)
				defer func() { cancel(); w.wait() }()
				w.mu.Lock()
				w.liveInline = !tc.disabled
				w.mu.Unlock()
				// Unrelated page traffic: two full media frames still uploading.
				if _, err := w.Write(randomPayload(2 * p.segmentBytes)); err != nil {
					t.Fatal(err)
				}
				w.mu.Lock()
				first, pending := w.seq, w.pendingUploadBytes
				if tc.sustained {
					w.bulkSegments = 2 * len(w.table.Slots)
				}
				w.mu.Unlock()
				if first != 2 || pending != 2*p.segmentBytes {
					t.Fatalf("background traffic: frames=%d pending=%d", first, pending)
				}
				segment := randomPayload(200 << 10)
				if _, err := w.Write(segment); err != nil {
					t.Fatal(err)
				}
				time.Sleep(20 * p.flushInterval)
				synctest.Wait()
				w.mu.Lock()
				result := w.gates[first%int64(len(w.gates))]
				inline := result != nil && result.accountedBytes == slotInlineLimit && result.inlinePlannedBytes == len(segment)-slotInlineLimit
				media := result != nil && result.accountedBytes == len(segment)
				w.mu.Unlock()
				if tc.inline && !inline || !tc.inline && !media {
					t.Fatalf("segment admission: inline=%v media=%v, want inline=%v", inline, media, tc.inline)
				}
				close(store.release)
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				store.mu.Lock()
				defer store.mu.Unlock()
				var got []byte
				for seq := first; seq < w.seq; seq++ {
					id := w.slotID(seq)
					frame, ok := decodeSlot(store.descs[id])
					if !ok || frame.seq != seq || (frame.mode == slotInline) != tc.inline {
						t.Fatalf("frame %d has the wrong publication class", seq)
					}
					got = append(got, frame.data...)
					got = append(got, store.bodies[id]...)
				}
				if !bytes.Equal(got, segment) {
					t.Fatal("the live segment changed")
				}
			})
		})
	}
}

// P2. A straggling metadata-only update gets one identical copy after
// slotUploadHedge; the copy's success returns at once, and the next write to
// the same slot waits until the straggler has also finished.
func TestDriveSlowInlineUploadIsHedgedAndReuseWaitsForBoth(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls atomic.Int32
		var mu sync.Mutex
		var bodies []string
		rt := roundTrip(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodPatch {
				return memoryResponse(http.StatusNotFound, ""), nil
			}
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			bodies = append(bodies, string(body))
			mu.Unlock()
			if calls.Add(1) == 1 {
				select {
				case <-release:
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
			} else if err := batchTestWait(r.Context(), 100*time.Millisecond); err != nil {
				return nil, err
			}
			return memoryResponse(http.StatusOK, `{"id":"slot"}`), nil
		})
		s := memoryDrive(rt)
		s.uploadHedges = make(chan struct{}, slotUploadHedgeMax)
		s.timing = newTimingAggregator()
		s.timing.started = true
		ctx := context.WithValue(context.Background(), slotUploadHedgeKey{}, true)

		began := time.Now()
		if err := s.setSlot(ctx, "slot", "first", nil); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(began); calls.Load() != 2 || elapsed > slotUploadHedge+200*time.Millisecond {
			t.Fatalf("straggling update returned after %v with %d requests", elapsed, calls.Load())
		}
		mu.Lock()
		identical := len(bodies) == 2 && bodies[0] == bodies[1]
		mu.Unlock()
		if !identical {
			t.Fatal("the copy was not identical to the primary update")
		}
		if batch := s.timing.snapshot()[kindSlotInline]; batch.hedges != 1 || batch.won != 1 {
			t.Fatalf("hedges=%d won=%d", batch.hedges, batch.won)
		}

		next := make(chan error, 1)
		go func() { next <- s.setSlot(ctx, "slot", "second", nil) }()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if n := calls.Load(); n != 2 {
			t.Fatalf("the next lap was sent while the straggler could still land (%d requests)", n)
		}
		close(release)
		if err := <-next; err != nil || calls.Load() != 3 {
			t.Fatalf("next lap after the straggler: err=%v requests=%d", err, calls.Load())
		}
	})
}

func TestDriveUploadHedgeOnlyForSlowMarkedInlineWrites(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		latency                  time.Duration
		marked, media, throttled bool
		disabled                 bool
		want                     int32
	}{
		{"slow", 2 * time.Second, true, false, false, false, 2},
		{"fast", 690 * time.Millisecond, true, false, false, false, 1},
		{"unmarked", 2 * time.Second, false, false, false, false, 1},
		{"media", 2 * time.Second, true, true, false, false, 1},
		{"recent_rate_limit", 2 * time.Second, true, false, true, false, 1},
		{"no_hedge_permits", 2 * time.Second, true, false, false, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls atomic.Int32
				s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
					if r.Method == http.MethodPatch {
						calls.Add(1)
					}
					if err := batchTestWait(r.Context(), tc.latency); err != nil {
						return nil, err
					}
					return memoryResponse(http.StatusOK, `{"id":"slot"}`), nil
				}))
				if !tc.disabled {
					s.uploadHedges = make(chan struct{}, slotUploadHedgeMax)
				}
				if tc.throttled {
					s.coolMu.Lock()
					s.lastRateLimit = time.Now()
					s.coolMu.Unlock()
				}
				ctx := context.Background()
				if tc.marked {
					ctx = context.WithValue(ctx, slotUploadHedgeKey{}, true)
				}
				var body []byte
				if tc.media {
					body = []byte("media body")
				}
				if err := s.setSlot(ctx, "slot", "d", body); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if n := calls.Load(); n != tc.want {
					t.Fatalf("%d update requests, want %d", n, tc.want)
				}
				if tc.want > 1 {
					// The copy that setSlot deliberately did not wait for is still in
					// flight; let it finish so the bubble has no blocked goroutines.
					if err := s.awaitSlotTail(ctx, "slot"); err != nil {
						t.Fatal(err)
					}
				}
			})
		})
	}
}

type hedgeMarkSlots struct {
	*memSlots
	mu     sync.Mutex
	marked map[byte][]bool
}

func (s *hedgeMarkSlots) setSlot(ctx context.Context, id, description string, body []byte) error {
	if f, ok := decodeSlot(description); ok {
		s.mu.Lock()
		s.marked[f.mode] = append(s.marked[f.mode], ctx.Value(slotUploadHedgeKey{}) != nil)
		s.mu.Unlock()
	}
	return s.memSlots.setSlot(ctx, id, description, body)
}

// Only inline frames may be hedged: never media bodies, terminal frames or
// watermarks (which are not slot frames at all).
func TestSlotWriterMarksOnlyInlineFramesForUploadHedge(t *testing.T) {
	store := &hedgeMarkSlots{memSlots: newMemSlots(), marked: make(map[byte][]bool)}
	ctx, cancel := context.WithCancel(context.Background())
	p := paramsFromConfig(&Config{Concurrency: 16})
	ring, err := makeRing(ctx, store, "hedge-marks", p)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	w := newSlotWriter(ctx, store, ring.Up, p)
	defer func() { cancel(); w.wait() }()
	if _, err := w.Write(randomPayload(600 << 10)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.marked[slotInline]) == 0 || len(store.marked[slotMedia]) == 0 || len(store.marked[slotEnd]) != 1 {
		t.Fatalf("unexpected frames: %v", store.marked)
	}
	for mode, marks := range store.marked {
		for _, marked := range marks {
			if marked != (mode == slotInline) {
				t.Fatalf("mode %d frame marked=%v", mode, marked)
			}
		}
	}
}

// P3. burstVisibilitySlots answers each description read after 300 ms and
// hides a frame until its visibility time, like Drive's update-to-read lag.
type burstVisibilitySlots struct {
	*memSlots
	visible map[string]time.Time // written before the reader starts
}

func (*burstVisibilitySlots) throttled() int64 { return 0 }

func (s *burstVisibilitySlots) readDescription(ctx context.Context, id string) (string, error) {
	if err := batchTestWait(ctx, 300*time.Millisecond); err != nil {
		return "", err
	}
	if at, ok := s.visible[id]; ok && time.Now().Before(at) {
		return "", nil
	}
	return s.memSlots.readDescription(ctx, id)
}

// A straggling head upload holds back a 19-frame live segment. Frames past
// head+slotReadAhead must already be found when the head lands; before, they
// kept the tail pace and arrived one description read (~0.3 s) late.
func TestSlotBurstTailFoundBeforeStragglingHead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		base := newMemSlots()
		p := paramsFromConfig(&Config{Concurrency: 64})
		ring, err := makeRing(ctx, base, "burst-tail", p)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		const last = 19
		start := time.Now()
		store := &burstVisibilitySlots{memSlots: base, visible: make(map[string]time.Time)}
		frames := make([][]byte, last+1)
		for seq := 1; seq <= last; seq++ {
			frames[seq] = randomPayload(slotInlineLimit)
			id := ring.Up.Slots[seq]
			if err := base.setSlot(ctx, id, newSlotFrame(int64(seq), slotInline, frames[seq]).encode(), nil); err != nil {
				cancel()
				t.Fatal(err)
			}
			switch {
			case seq == 1:
				store.visible[id] = start.Add(time.Second) // the straggling head
			case int64(seq) > 1+slotReadAhead:
				store.visible[id] = start.Add(850 * time.Millisecond)
			}
		}
		r := newSlotReader(ctx, store, ring.Up, p, []byte{0})
		defer func() { cancel(); r.wait() }()
		<-r.chunks()
		var headAt time.Time
		for seq := 1; seq <= last; seq++ {
			select {
			case got := <-r.chunks():
				if !bytes.Equal(got, frames[seq]) {
					t.Fatalf("frame %d changed: %v", seq, r.Err())
				}
				if seq == 1 {
					headAt = time.Now()
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("frame %d was not delivered", seq)
			}
		}
		tail := time.Since(headAt)
		t.Logf("head delivered at %v, last frame %v later", headAt.Sub(start), tail)
		if tail > 100*time.Millisecond {
			t.Fatalf("burst tail arrived %v after the head", tail)
		}
	})
}
