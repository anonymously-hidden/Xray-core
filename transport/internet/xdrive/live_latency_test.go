package xdrive

import (
	"bytes"
	"context"
	"io"
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
