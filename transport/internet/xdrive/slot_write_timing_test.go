package xdrive

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type slotWriteTimingRecord struct {
	frame slotFrame
	body  []byte
	data  []byte
}

// This fixture owns all its state and does not start a reader or a flush loop.
type slotWriteTimingStore struct {
	mu        sync.Mutex
	calls     []slotWriteTimingRecord
	completed []slotWriteTimingRecord
	reads     int
	started   chan slotWriteTimingRecord
	release   <-chan struct{}
	failure   error
}

func (s *slotWriteTimingStore) makeSlot(ctx context.Context, name string) (string, error) {
	return name, ctx.Err()
}

func (s *slotWriteTimingStore) createSlot(ctx context.Context, id, name, description string, body []byte) error {
	return s.setSlot(ctx, id, description, body)
}

func (s *slotWriteTimingStore) setSlot(ctx context.Context, id, description string, body []byte) error {
	frame, ok := decodeSlot(description)
	if !ok {
		return fmt.Errorf("invalid timing-fixture frame for %s", id)
	}
	data := frame.data
	if frame.mode == slotMedia || frame.mode == slotStream {
		data = body
	}
	if !frame.holds(data) {
		return fmt.Errorf("invalid timing-fixture payload for sequence %d", frame.seq)
	}
	record := slotWriteTimingRecord{frame: frame, body: bytes.Clone(body), data: bytes.Clone(data)}
	s.mu.Lock()
	s.calls = append(s.calls, record)
	s.mu.Unlock()
	if s.started != nil {
		select {
		case s.started <- record:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.failure != nil && frame.mode != slotErr {
		return s.failure
	}
	s.mu.Lock()
	s.completed = append(s.completed, record)
	s.mu.Unlock()
	return nil
}

func (s *slotWriteTimingStore) readDescription(ctx context.Context, id string) (string, error) {
	s.mu.Lock()
	s.reads++
	s.mu.Unlock()
	return "-1", ctx.Err()
}

func (s *slotWriteTimingStore) readBody(ctx context.Context, id string) ([]byte, error) {
	return nil, fmt.Errorf("unexpected timing-fixture body read for %s", id)
}

func (s *slotWriteTimingStore) snapshot() ([]slotWriteTimingRecord, []slotWriteTimingRecord, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]slotWriteTimingRecord(nil), s.calls...), append([]slotWriteTimingRecord(nil), s.completed...), s.reads
}

func newSlotWriteTimingFixture(t *testing.T, store *slotWriteTimingStore, depth int) *slotWriter {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ids := make([]string, depth)
	for i := range ids {
		ids[i] = fmt.Sprintf("timing-slot-%d", i)
	}
	w := &slotWriter{
		ctx: ctx, cancel: cancel, store: store,
		table:     slotTable{Slots: ids, Watermark: "timing-watermark"},
		params:    params{segmentBytes: defaultSegmentBytes},
		slots:     make(chan struct{}, min(depth, max(slotWriteInflight, slotBulkInflight))),
		available: make(chan struct{}), failed: make(chan struct{}),
		flushStop: make(chan struct{}), gates: make([]*slotWriteResult, depth),
		size: slotInlineLimit,
		// Cached credit isolates upload instrumentation from watermark traffic.
		acked: 1024, ackedAt: time.Now(), progress: time.Now(),
		ackRetry: slotAckRetry, watermarkTTL: slotWatermarkTTL,
	}
	t.Cleanup(func() {
		cancel()
		waitSlotWriteTimingUploads(t, w)
	})
	return w
}

func waitSlotWriteTimingUploads(t *testing.T, w *slotWriter) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timing-fixture uploads did not finish")
	}
}

func waitSlotWriteTimingStarted(t *testing.T, store *slotWriteTimingStore) slotWriteTimingRecord {
	t.Helper()
	select {
	case record := <-store.started:
		return record
	case <-time.After(5 * time.Second):
		t.Fatal("timing-fixture upload did not reach storage")
		return slotWriteTimingRecord{}
	}
}

func flushSlotWriteTiming(t *testing.T, w *slotWriter) *slotWriteResult {
	t.Helper()
	w.mu.Lock()
	seq := w.seq
	err := w.flushLocked(nil, nil)
	result := w.gates[seq%int64(len(w.gates))]
	w.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("flush did not assign a result")
	}
	return result
}

func TestSlotWriteTimingDurations(t *testing.T) {
	base := time.Unix(100, 0)
	result := &slotWriteResult{
		bufferEpisodeAt: base,
		selectedAt:      base.Add(2 * time.Second),
		assignedAt:      base.Add(2*time.Second + time.Millisecond),
		permitWait:      17 * time.Millisecond,
	}
	before := *result
	workerAt := result.assignedAt.Add(3 * time.Millisecond)
	prefetchAt := workerAt.Add(5 * time.Millisecond)
	awaitAt := prefetchAt.Add(7 * time.Millisecond)
	age, permit, queue, prefetch := result.durations(workerAt, prefetchAt, awaitAt)
	if age != 2*time.Second || permit != 17*time.Millisecond || queue != 3*time.Millisecond || prefetch != 7*time.Millisecond {
		t.Fatalf("unexpected durations: age=%v permit=%v queue=%v prefetch=%v", age, permit, queue, prefetch)
	}
	if *result != before {
		t.Fatal("duration calculation mutated immutable observations")
	}
	unknown := &slotWriteResult{}
	age, permit, queue, prefetch = unknown.durations(time.Time{}, time.Time{}, time.Time{})
	if age != -1 || permit != -1 || queue != -1 || prefetch != -1 {
		t.Fatalf("unset timestamps must be unavailable: %v %v %v %v", age, permit, queue, prefetch)
	}
	if got := slotWriteElapsed(base, time.Time{}); got != -1 {
		t.Fatalf("unset end timestamp: %v", got)
	}
	if got := slotWriteElapsed(base.Add(time.Second), base); got != 0 {
		t.Fatalf("negative elapsed time was not clamped: %v", got)
	}
}

func TestSlotWriteTimingEpisodesAndByteIdentity(t *testing.T) {
	release := make(chan struct{})
	store := &slotWriteTimingStore{release: release}
	w := newSlotWriteTimingFixture(t, store, slotBulkDepth)
	w.mu.Lock()
	w.size = w.segmentBytes // Existing ramp state must not suppress inline fan-out.
	w.appendBufferLocked(nil)
	unset := w.bufEpisodeAt.IsZero()
	w.mu.Unlock()
	if !unset {
		t.Fatal("empty append started a buffer episode")
	}
	payload := make([]byte, 3*slotInlineLimit+17)
	for i := range payload {
		payload[i] = byte(i*17 + 3)
	}
	before := time.Now()
	if n, err := w.Write(payload[:2*slotInlineLimit]); n != 2*slotInlineLimit || err != nil {
		t.Fatalf("initial Write: n=%d err=%v", n, err)
	}
	after := time.Now()
	w.mu.Lock()
	episode := w.bufEpisodeAt
	w.mu.Unlock()
	if episode.IsZero() || episode.Before(before) || episode.After(after) {
		t.Fatal("buffer episode timestamp is outside the append interval")
	}
	results := []*slotWriteResult{flushSlotWriteTiming(t, w)}
	if n, err := w.Write(payload[2*slotInlineLimit:]); n != len(payload)-2*slotInlineLimit || err != nil {
		t.Fatalf("suffix Write: n=%d err=%v", n, err)
	}
	w.mu.Lock()
	retained := w.bufEpisodeAt == episode
	w.mu.Unlock()
	if !retained {
		t.Fatal("appending to a retained suffix reset the buffer episode")
	}
	for {
		w.mu.Lock()
		empty := len(w.buf) == 0
		w.mu.Unlock()
		if empty {
			break
		}
		results = append(results, flushSlotWriteTiming(t, w))
	}
	if len(results) != 4 {
		t.Fatalf("inline fan-out changed: %d chunks", len(results))
	}
	bufferSizes := []int{2 * slotInlineLimit, 2*slotInlineLimit + 17, slotInlineLimit + 17, 17}
	for i, result := range results {
		wantOutstanding := len(payload)
		if i == 0 {
			wantOutstanding = 2 * slotInlineLimit
		}
		if result.bufferEpisodeAt != episode || result.selectedAt.Before(episode) || result.assignedAt.Before(result.selectedAt) {
			t.Fatalf("chunk %d has invalid episode/selection/assignment timestamps", i)
		}
		if !result.accounted || result.accountedBytes != min(bufferSizes[i], slotInlineLimit) || result.bufferBytes != bufferSizes[i] || result.localOutstandingBytes != wantOutstanding || result.permitWait != 0 {
			t.Fatalf("chunk %d has incorrect immutable accounting: %+v", i, result)
		}
	}
	next := []byte("next-episode")
	// Windows may return the same time.Now value for two separate episodes.
	// Seed a distinguishable prior value to verify replacement without sleeps.
	previousEpisode := episode.Add(-time.Hour)
	w.mu.Lock()
	w.bufEpisodeAt = previousEpisode
	w.mu.Unlock()
	before = time.Now()
	if n, err := w.Write(next); n != len(next) || err != nil {
		t.Fatalf("new-episode Write: n=%d err=%v", n, err)
	}
	after = time.Now()
	w.mu.Lock()
	newEpisode := w.bufEpisodeAt
	w.mu.Unlock()
	if newEpisode == previousEpisode || newEpisode.Before(before) || newEpisode.After(after) {
		t.Fatal("empty-to-nonempty append did not start a fresh episode")
	}
	last := flushSlotWriteTiming(t, w)
	if last.bufferEpisodeAt != newEpisode || last.localOutstandingBytes != len(payload)+len(next) {
		t.Fatal("new episode lost pending upload bytes or its own timestamp")
	}
	w.mu.Lock()
	pending, occupied := w.pendingUploadBytes, len(w.slots)
	size, full, bulk := w.size, w.full, w.bulkSegments
	w.mu.Unlock()
	if pending != len(payload)+len(next) || occupied != 5 || size != w.segmentBytes || full != 0 || bulk != 0 {
		t.Fatalf("unexpected queued state: bytes=%d slots=%d size=%d full=%d bulk=%d", pending, occupied, size, full, bulk)
	}
	close(release)
	waitSlotWriteTimingUploads(t, w)
	w.mu.Lock()
	pending, occupied = w.pendingUploadBytes, len(w.slots)
	w.mu.Unlock()
	if pending != 0 || occupied != 0 {
		t.Fatalf("completed uploads leaked accounting: bytes=%d slots=%d", pending, occupied)
	}
	calls, completed, reads := store.snapshot()
	if len(calls) != 5 || len(completed) != 5 || reads != 0 {
		t.Fatalf("request behavior changed: calls=%d completed=%d reads=%d", len(calls), len(completed), reads)
	}
	bySequence := make(map[int64]slotWriteTimingRecord)
	for _, record := range completed {
		if _, duplicate := bySequence[record.frame.seq]; duplicate || record.frame.mode != slotInline || record.body != nil {
			t.Fatal("duplicate sequence or altered inline publication")
		}
		bySequence[record.frame.seq] = record
	}
	var delivered []byte
	for seq := int64(0); seq < 5; seq++ {
		record, ok := bySequence[seq]
		if !ok {
			t.Fatalf("missing completed sequence %d", seq)
		}
		delivered = append(delivered, record.data...)
	}
	if !bytes.Equal(delivered, append(bytes.Clone(payload), next...)) {
		t.Fatal("completed publication bytes differ from accepted bytes")
	}
}

func TestSlotWriteTimingPermitWaitIncludesMutexReacquisition(t *testing.T) {
	release := make(chan struct{})
	store := &slotWriteTimingStore{release: release}
	w := newSlotWriteTimingFixture(t, store, 2)
	w.mu.Lock()
	w.slots <- struct{}{}
	w.slots <- struct{}{}
	w.appendBufferLocked([]byte("admission"))
	w.mu.Unlock()
	entered := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		w.mu.Lock()
		close(entered)
		err := w.flushLocked(nil, nil)
		w.mu.Unlock()
		finished <- err
	}()
	<-entered
	w.mu.Lock() // The full-window wait must release this mutex.
	flushing := w.flushing
	holdAt := time.Now()
	w.releaseSlotLocked()
	<-time.After(5 * time.Millisecond) // Capacity is available, but reacquisition remains blocked.
	releasedAt := time.Now()
	w.mu.Unlock()
	if !flushing {
		t.Fatal("flush did not enter its existing full-window admission wait")
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("admission did not resume")
	}
	w.mu.Lock()
	result := w.gates[0]
	w.releaseSlotLocked() // Return the remaining synthetic occupancy.
	w.mu.Unlock()
	if result == nil || result.permitWait < releasedAt.Sub(holdAt) {
		t.Fatal("permit timing omitted mutex reacquisition")
	}
	if age := slotWriteElapsed(result.bufferEpisodeAt, result.selectedAt); age < result.permitWait {
		t.Fatalf("episode age %v should overlap permit wait %v", age, result.permitWait)
	}
	close(release)
	waitSlotWriteTimingUploads(t, w)
	w.mu.Lock()
	pending, occupied := w.pendingUploadBytes, len(w.slots)
	w.mu.Unlock()
	if pending != 0 || occupied != 0 {
		t.Fatalf("admission cleanup leaked: bytes=%d slots=%d", pending, occupied)
	}
}

func TestSlotWriteTimingCleanupPaths(t *testing.T) {
	for _, outcome := range []string{"success", "write-error", "canceled", "previous-error"} {
		t.Run(outcome, func(t *testing.T) {
			release := make(chan struct{})
			store := &slotWriteTimingStore{release: release, started: make(chan slotWriteTimingRecord, 4)}
			if outcome == "write-error" {
				store.failure = stderrors.New("timing-fixture write failure")
			}
			w := newSlotWriteTimingFixture(t, store, slotBulkDepth)
			payload := []byte("accounted exactly once")
			if n, err := w.Write(payload); n != len(payload) || err != nil {
				t.Fatalf("Write: n=%d err=%v", n, err)
			}
			if outcome == "previous-error" {
				previous := &slotWriteResult{done: make(chan struct{}), err: stderrors.New("timing-fixture predecessor failure")}
				close(previous.done)
				w.mu.Lock()
				w.gates[0] = previous
				w.mu.Unlock()
			}
			result := flushSlotWriteTiming(t, w)
			if !result.accounted || result.accountedBytes != len(payload) || result.localOutstandingBytes != len(payload) {
				t.Fatal("assignment did not account for its payload")
			}
			if outcome != "previous-error" {
				record := waitSlotWriteTimingStarted(t, store)
				if record.frame.seq != 0 || record.frame.mode != slotInline || !bytes.Equal(record.data, payload) {
					t.Fatal("upload request changed the assigned payload")
				}
				w.mu.Lock()
				pending := w.pendingUploadBytes
				w.mu.Unlock()
				if pending != len(payload) {
					t.Fatal("upload bytes disappeared before completion")
				}
			}
			if outcome == "canceled" {
				w.cancel()
			} else {
				close(release)
			}
			waitSlotWriteTimingUploads(t, w)
			w.mu.Lock()
			pending, occupied, buffered := w.pendingUploadBytes, len(w.slots), len(w.buf)
			w.mu.Unlock()
			if pending != 0 || occupied != 0 || buffered != 0 {
				t.Fatalf("cleanup leaked: bytes=%d slots=%d buffer=%d", pending, occupied, buffered)
			}
			if (result.err == nil) != (outcome == "success") || (w.Err() == nil) != (outcome == "success") {
				t.Fatal("upload result/failure behavior changed")
			}
			select {
			case <-result.done:
			default:
				t.Fatal("result completion was not published")
			}
			calls, completed, reads := store.snapshot()
			wantCalls, wantCompleted := 1, 1
			switch outcome {
			case "write-error":
				wantCalls = 2 // The unchanged best-effort error frame.
			case "canceled":
				wantCompleted = 0
			case "previous-error":
				wantCalls, wantCompleted = 0, 0
			}
			if len(calls) != wantCalls || len(completed) != wantCompleted || reads != 0 {
				t.Fatalf("request behavior changed: calls=%d completed=%d reads=%d", len(calls), len(completed), reads)
			}
			if outcome == "write-error" && completed[0].frame.mode != slotErr {
				t.Fatal("best-effort error publication changed")
			}
		})
	}
}

func TestSlotWriteTimingDirectUploadWithoutAccounting(t *testing.T) {
	release := make(chan struct{})
	store := &slotWriteTimingStore{release: release, started: make(chan slotWriteTimingRecord, 1)}
	w := newSlotWriteTimingFixture(t, store, slotBulkDepth)
	result := &slotWriteResult{done: make(chan struct{})}
	payload := []byte("legacy direct upload")
	w.mu.Lock()
	w.slots <- struct{}{}
	w.wg.Add(1)
	w.mu.Unlock()
	go w.upload(0, payload, nil, result)
	waitSlotWriteTimingStarted(t, store)
	close(release)
	waitSlotWriteTimingUploads(t, w)
	w.mu.Lock()
	pending, occupied := w.pendingUploadBytes, len(w.slots)
	w.mu.Unlock()
	if pending != 0 || occupied != 0 || result.err != nil || result.accounted || result.accountedBytes != 0 {
		t.Fatalf("unaccounted direct upload changed accounting: bytes=%d slots=%d result=%+v", pending, occupied, result)
	}
	if !result.assignedAt.IsZero() || !result.selectedAt.IsZero() || !result.bufferEpisodeAt.IsZero() {
		t.Fatal("upload worker mutated assignment observations")
	}
	calls, completed, reads := store.snapshot()
	if len(calls) != 1 || len(completed) != 1 || reads != 0 || !bytes.Equal(completed[0].data, payload) {
		t.Fatal("direct-upload byte identity or request behavior changed")
	}
}
