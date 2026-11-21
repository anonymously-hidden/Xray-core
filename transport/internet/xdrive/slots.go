package xdrive

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// A session on a slotStore is carried in a ring of slots for each direction,
// objects that both peers know by id: segment n is written to slot n % depth
// and read from it, with nothing to list and no index to wait for. The reader
// of a direction keeps how far it has read in a watermark, and the writer
// reuses a slot only once that says what was in it before was taken

const (
	slotMagic     = "XDRVSL01"
	slotHeaderLen = len(slotMagic) + 8 + 1 + 4 + 4
	// slotInlineLimit is how large a segment is at most to go in the description
	// of its slot, which takes one request less than a body
	slotInlineLimit = 14 << 10
	// inlineRampSegments segments of one size in a row double it, from
	// slotInlineLimit up to segmentBytes
	inlineRampSegments = 8
	// Stable browsing suffixes retain two batching ticks before an early flush.
	slotInlineStableTicks   = 2
	slotInlineWriteInflight = 24

	// Bulk uploads use a wider window once a deep ring is warm. Media reads
	// have bounded lookahead for mixed traffic too. Small rings retain their
	// existing per-file write rate.
	slotBulkInflight = 24
	slotBodyAhead    = 20
	slotBulkDepth    = 64

	slotInline = 1
	slotMedia  = 2
	slotEnd    = 3
	slotErr    = 4

	// maxSlotIDLen bounds any slot or watermark id accepted from a peer.
	maxSlotIDLen = 128
)

var (
	// the head is asked for between slotHuntFloor and slotHuntCeiling, a segment
	// behind it at about the pace that segments come
	slotHuntFloor    = 10 * time.Millisecond
	slotHuntCeiling  = 25 * time.Millisecond
	slotTailMin      = 150 * time.Millisecond
	slotTailMax      = 800 * time.Millisecond
	slotTailQuietMax = 2 * time.Second
	slotFetchAhead   = 12

	slotAckRetry     = 30 * time.Millisecond
	slotWatermarkTTL = 100 * time.Millisecond
	ackDelay         = 1500 * time.Millisecond
	slotStallTimeout = 60 * time.Second
	slotCloseGrace   = 10 * time.Second
	slotGoneTimeout  = 60 * time.Second
	slotBodyHedge    = 2 * time.Second

	slotReadAhead      int64 = 11
	slotReadAheadPoll        = 40 * time.Millisecond
	slotReadAheadBurst       = 500 * time.Millisecond
)

var slotWriteInflight = 12

// slotStore is a storage of objects that are written and read again by id,
// each with a description and a body
type slotStore interface {
	makeSlot(ctx context.Context, name string) (string, error)
	// createSlot creates a slot with a pre-generated id; on a duplicate-id 4xx
	// it may fall back to updating the existing object.
	createSlot(ctx context.Context, id, name, description string, body []byte) error
	// setSlot sets the description of a slot and, unless body is nil, its body
	setSlot(ctx context.Context, id, description string, body []byte) error
	readDescription(ctx context.Context, id string) (string, error)
	readBody(ctx context.Context, id string) ([]byte, error)
}

// throttleStore is the optional part of a slotStore that reports how often
// Drive throttled or timed out its requests; only driveStorage implements it
type throttleStore interface {
	throttled() int64
}

// slotFrame is what the description of a slot says: which segment it holds,
// and either the segment itself or the size and checksum of the body that does
type slotFrame struct {
	seq    int64
	mode   byte
	size   int
	crc    uint32
	data   []byte
	blocks []uint32 // per-block CRCs; rooted against crc before streamed delivery
	prefix []byte   // verified bytes before the streamed suffix; excluded from size and crc
}

func newSlotFrame(seq int64, mode byte, data []byte) slotFrame {
	f := slotFrame{seq: seq, mode: mode, size: len(data), crc: crc32.ChecksumIEEE(data)}
	if mode == slotInline {
		f.data = data
	} else if mode == slotStream {
		f.blocks = streamBlockChecksums(data)
	}
	return f
}

func (f slotFrame) encode() string {
	b := make([]byte, slotHeaderLen, slotHeaderLen+len(f.data))
	copy(b, slotMagic)
	binary.BigEndian.PutUint64(b[8:], uint64(f.seq))
	b[16] = f.mode
	binary.BigEndian.PutUint32(b[17:], uint32(f.size))
	binary.BigEndian.PutUint32(b[21:], f.crc)
	if f.mode == slotStream {
		b = appendStreamChecksums(b, f.blocks)
		if len(f.prefix) != 0 {
			b[16] = slotStreamPrefix
			b = binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(f.prefix))
			b = append(b, f.prefix...)
		}
	} else {
		b = append(b, f.data...)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func decodeSlot(description string) (slotFrame, bool) {
	b, err := base64.StdEncoding.DecodeString(description)
	if err != nil || len(b) < slotHeaderLen || string(b[:len(slotMagic)]) != slotMagic {
		return slotFrame{}, false
	}
	f := slotFrame{
		seq:  int64(binary.BigEndian.Uint64(b[8:])),
		mode: b[16],
		size: int(binary.BigEndian.Uint32(b[17:])),
		crc:  binary.BigEndian.Uint32(b[21:]),
	}
	if f.seq < 0 || f.size < 0 || f.size > maxSegmentBytes {
		return slotFrame{}, false
	}
	switch f.mode {
	case slotInline:
		f.data = b[slotHeaderLen:]
		if f.size == 0 || !f.holds(f.data) {
			return slotFrame{}, false
		}
	case slotMedia:
		if f.size == 0 || len(b) != slotHeaderLen {
			return slotFrame{}, false
		}
	case slotStream, slotStreamPrefix:
		trailer := b[slotHeaderLen:]
		if f.mode == slotStreamPrefix {
			end := len(trailer) - slotStreamPrefixBytes - 4
			if end < 0 || f.size > maxSegmentBytes-slotStreamPrefixBytes {
				return slotFrame{}, false
			}
			f.prefix = trailer[end+4:]
			if crc32.ChecksumIEEE(f.prefix) != binary.BigEndian.Uint32(trailer[end:]) {
				return slotFrame{}, false
			}
			trailer, f.mode = trailer[:end], slotStream
		}
		var ok bool
		f.blocks, ok = decodeStreamChecksums(f.size, f.crc, trailer)
		if !ok {
			return slotFrame{}, false
		}
	case slotEnd, slotErr:
		if f.size != 0 || f.crc != 0 || len(b) != slotHeaderLen {
			return slotFrame{}, false
		}
	default:
		return slotFrame{}, false
	}
	return f, true
}

// holds reports whether body is the one that f is about, which a body read
// right after its description need not be yet
func (f slotFrame) holds(body []byte) bool {
	return len(body) == f.size && crc32.ChecksumIEEE(body) == f.crc
}

// slotTable is the ring of one direction: the ids of its slots and of the
// watermark of its reader. name, lazy and streamPrefix are local, not serialized.
type slotTable struct {
	Slots        []string `json:"s"`
	Watermark    string   `json:"w"`
	name         string
	lazy         bool
	streamPrefix bool
}

// ring is what a client announces a session with, the tables are named from
// its side. Lazy rings create their slots on first write instead of up front.
type ring struct {
	Up            slotTable `json:"u"`
	Down          slotTable `json:"d"`
	First         []byte    `json:"first,omitempty"`
	Lazy          bool      `json:"lazy,omitempty"`
	StreamPrefix  bool      `json:"streamPrefix,omitempty"` // downstream reader also accepts a verified metadata prefix
	StreamVersion uint32    `json:"stream,omitempty"`       // receiver capability, not a v1 watermark change
}

func decodeRing(b []byte) (ring, bool) {
	var r ring
	if json.Unmarshal(b, &r) != nil || len(r.Up.Slots) == 0 || len(r.Down.Slots) == 0 ||
		r.Up.Watermark == "" || r.Down.Watermark == "" {
		return ring{}, false
	}
	if len(r.Up.Slots) > maxConcurrency || len(r.Down.Slots) > maxConcurrency {
		return ring{}, false
	}
	seen := make(map[string]bool, len(r.Up.Slots)+len(r.Down.Slots)+2)
	for _, t := range []slotTable{r.Up, r.Down} {
		if len(t.Watermark) > maxSlotIDLen || seen[t.Watermark] {
			return ring{}, false
		}
		seen[t.Watermark] = true
		for _, id := range t.Slots {
			if len(id) == 0 || len(id) > maxSlotIDLen || seen[id] {
				return ring{}, false
			}
			seen[id] = true
		}
	}
	return r, true
}

// makeRing makes the slots and watermarks of a session, all at once
func makeRing(ctx context.Context, store slotStore, session string, p params) (ring, error) {
	depth := max(p.concurrency, 2)
	r := ring{
		Up:   slotTable{Slots: make([]string, depth)},
		Down: slotTable{Slots: make([]string, depth)},
	}
	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		err     error
	)
	create := func(id *string, name string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var e error
			if *id, e = store.makeSlot(ctx, name); e != nil {
				errOnce.Do(func() { err = e })
			}
		}()
	}
	for _, t := range []struct {
		table  *slotTable
		prefix string
	}{{&r.Up, uplinkPrefix(session)}, {&r.Down, downlinkPrefix(session)}} {
		t.table.name = t.prefix
		for i := range t.table.Slots {
			create(&t.table.Slots[i], t.prefix+"/s"+strconv.Itoa(i))
		}
		create(&t.table.Watermark, t.prefix+"/w")
	}
	wg.Wait()
	return r, err
}

// lazyRing assembles a ring from pre-generated ids without any create call.
// ids must hold 2*(depth+1) entries: up slots, up watermark, down slots, down
// watermark.
func lazyRing(session string, depth int, ids []string) ring {
	r := ring{
		Lazy: true,
		Up:   slotTable{Slots: append([]string(nil), ids[:depth]...), Watermark: ids[depth], name: uplinkPrefix(session), lazy: true},
		Down: slotTable{Slots: append([]string(nil), ids[depth+1:2*depth+1]...), Watermark: ids[2*depth+1], name: downlinkPrefix(session), lazy: true},
	}
	return r
}

func readWatermark(ctx context.Context, store slotStore, id string, lazy bool) (int64, error) {
	d, err := store.readDescription(context.WithValue(ctx, laneKey{}, true), id)
	if err == errNotFound && lazy {
		// A lazy ring creates its watermark on the first ack; absent means no
		// progress yet, not a cleaned-up session.
		return -1, nil
	}
	if err != nil || d == "" {
		return -1, err
	}
	return strconv.ParseInt(d, 10, 64)
}

var errWriteStopped = errors.New("write stopped")

type slotWriteResult struct {
	done chan struct{}
	err  error // published before done closes

	// These observations are immutable once the upload worker is assigned.
	// The buffer episode can outlive extracted prefixes, so its age bounds
	// the selected bytes' residence time rather than measuring each byte.
	bufferEpisodeAt        time.Time
	selectedAt             time.Time
	assignedAt             time.Time
	permitWait             time.Duration // cumulative full-window waits, including mu reacquisition
	bufferBytes            int           // input buffer before extracting this chunk
	localOutstandingBytes  int           // buffered plus assigned/not-yet-completed upload bytes
	inlineBudgetBytes      int
	inlineBurstBudgetBytes int
	inlinePlannedBytes     int
	accountedBytes         int
	accounted              bool // direct upload callers may not have enqueued/accounted this result
	inlinePrefix           bool // start of a media burst on a capable downstream ring
}

func slotWriteElapsed(start, end time.Time) time.Duration {
	if start.IsZero() || end.IsZero() {
		return -1
	}
	return max(0, end.Sub(start))
}

// Buffer episode age overlaps admission wait; these observations are not an
// additive latency breakdown. Unknown assignment timestamps remain unavailable.
func (r *slotWriteResult) durations(workerAt, prefetchAt, awaitAt time.Time) (bufferEpisodeAge, permitWait, queueWait, prefetchSetup time.Duration) {
	bufferEpisodeAge = slotWriteElapsed(r.bufferEpisodeAt, r.selectedAt)
	permitWait = -1
	if !r.assignedAt.IsZero() {
		permitWait = r.permitWait
	}
	queueWait = slotWriteElapsed(r.assignedAt, workerAt)
	prefetchSetup = slotWriteElapsed(prefetchAt, awaitAt)
	return
}

type slotWriter struct {
	ctx    context.Context
	cancel context.CancelFunc
	store  slotStore
	table  slotTable
	params
	// slots has a token for each segment on its way, at most one for each slot
	slots     chan struct{}
	wg        sync.WaitGroup
	flushDone chan struct{}
	flushStop chan struct{}
	closeOnce sync.Once
	closeErr  error
	closeDone chan struct{}

	mu                 sync.Mutex
	buf                []byte
	bufBase            []byte
	bufEpisodeAt       time.Time // reset only when nonempty input is appended to an empty buffer
	pendingUploadBytes int       // assigned upload bytes; guarded by mu, not remote unacknowledged bytes
	seq                int64
	size               int
	full               int
	lastSize           int
	held               int
	flushing           bool          // a flush is waiting for a token with mu released
	available          chan struct{} // closed when an upload permit is returned; guarded by mu
	failed             chan struct{}
	bulkSegments       int
	bulkLimited        bool
	throttles          int64
	closed             bool
	err                error
	first              chan []byte
	lazy               bool
	created            []bool
	gates              []*slotWriteResult // completion and result of the last assigned physical write
	uplink             latencyWindow      // bulk upload window steered by media put latency; guarded by mu
	batch              *slotBatchWindow

	ackRetry     time.Duration
	watermarkTTL time.Duration

	// ackMu guards acked/ackedAt/progress/reading only; the watermark GET runs
	// without the lock, single-flight, so other uploads never block on it.
	ackMu    sync.Mutex
	acked    int64
	ackedAt  time.Time
	progress time.Time
	reading  bool
}

func newSlotWriter(ctx context.Context, store slotStore, table slotTable, p params) *slotWriter {
	ctx, cancel := context.WithCancel(ctx)
	w := &slotWriter{
		ctx:       ctx,
		cancel:    cancel,
		store:     store,
		table:     table,
		params:    p,
		slots:     make(chan struct{}, min(len(table.Slots), max(slotWriteInflight, slotBulkInflight))),
		flushDone: make(chan struct{}),
		flushStop: make(chan struct{}),
		available: make(chan struct{}),
		failed:    make(chan struct{}),
		size:      min(slotInlineLimit, p.segmentBytes),
		acked:     -1,
		progress:  time.Now(),
		lazy:      table.lazy,
		gates:     make([]*slotWriteResult, len(table.Slots)),

		ackRetry:     slotAckRetry,
		watermarkTTL: slotWatermarkTTL,
	}
	if w.lazy {
		w.created = make([]bool, len(table.Slots))
	}
	if s, ok := store.(throttleStore); ok {
		w.throttles = s.throttled()
		w.batch = newSlotBatchWindow(p.segmentBytes, len(table.Slots))
	}
	go w.flushLoop()
	return w
}

func (w *slotWriter) applyGrowLocked() {
	for w.full >= inlineRampSegments && w.size < w.segmentBytes {
		w.full -= inlineRampSegments
		w.size = min(2*w.size, w.segmentBytes)
	}
	if w.size >= w.segmentBytes {
		w.full = 0
	}
}

func (w *slotWriter) Write(p []byte) (int, error) {
	return w.writeUntil(p, nil, nil)
}

// writeUntil is Write that gives up with errWriteStopped, having returned the
// bytes it took into buf, once either channel is closed
func (w *slotWriter) writeUntil(p []byte, deadline, closing <-chan struct{}) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.slots) == 0 {
		w.bulkSegments, w.bulkLimited = 0, false
	}
	w.applyGrowLocked()

	if err := w.admissionErrorLocked(deadline, closing, false); err != nil {
		return 0, err
	}

	accepted := 0
	for len(p) > 0 {
		if err := w.admissionErrorLocked(deadline, closing, false); err != nil {
			return accepted, err
		}
		n := min(len(p), w.segmentBytes-len(w.buf))
		w.appendBufferLocked(p[:n])
		accepted += n
		p = p[n:]
		for len(w.buf) >= w.size {
			if err := w.flushLocked(deadline, closing); err != nil {
				return accepted, err
			}
		}
	}
	return accepted, nil
}

func (w *slotWriter) appendBufferLocked(p []byte) {
	if len(w.buf) == 0 && len(p) > 0 {
		w.bufEpisodeAt = time.Now()
	}
	if w.bufBase == nil {
		w.bufBase = w.buf[:0]
	}
	if len(w.buf)+len(p) > cap(w.buf) {
		if len(w.buf)+len(p) <= cap(w.bufBase) {
			w.buf = append(w.bufBase, w.buf...)
		} else {
			w.buf = append(w.buf, p...)
			w.bufBase = w.buf[:0]
			return
		}
	}
	w.buf = append(w.buf, p...)
}

func (w *slotWriter) admissionErrorLocked(deadline, closing <-chan struct{}, allowClosed bool) error {
	if w.err != nil {
		return w.err
	}
	if err := w.ctx.Err(); err != nil {
		if allowClosed {
			return err
		}
		return io.ErrClosedPipe
	}
	if !allowClosed && w.closed {
		return io.ErrClosedPipe
	}
	select {
	case <-deadline:
		return errWriteStopped
	case <-closing:
		return errWriteStopped
	default:
		return nil
	}
}

func (w *slotWriter) flushLoop() {
	defer close(w.flushDone)
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.flushStop:
			return
		case <-ticker.C:
			w.mu.Lock()
			if !w.flushing {
				w.applyGrowLocked()
			}
			if !w.closed && w.err == nil && !w.flushing && len(w.buf) > 0 && w.readyToFlush() {
				// The timer coalesces new bytes. Once an inline burst has
				// been selected its frame boundaries are fixed: spacing them
				// one tick apart only delays the last byte. Dispatch the
				// committed remainder while permits are immediately free.
				for {
					if err := w.flushLocked(nil, nil); err != nil || w.batch == nil ||
						w.batch.planned == 0 || len(w.slots) >= w.batch.window {
						break
					}
				}
			}
			done := w.closed || w.err != nil
			w.mu.Unlock()
			if done {
				return
			}
		}
	}
}

func (w *slotWriter) readyToFlush() bool {
	grew := len(w.buf) > w.lastSize
	w.lastSize = len(w.buf)

	// Drive keeps two coalescing ticks for small stable inline suffixes;
	// growing buffers still use the full budget below. Flushing a suffix
	// sooner can split a later burst into another PATCH, trading some batching
	// for browsing latency without sending every small write immediately.
	if _, drive := w.store.(throttleStore); drive && !grew && w.held >= slotInlineStableTicks &&
		len(w.buf) < min(w.size/2, slotInlineLimit) {
		healthy := true
		if s, ok := w.store.(interface {
			cooling() bool
			lastRateLimitAt() time.Time
		}); ok {
			healthy = !s.cooling() && time.Since(s.lastRateLimitAt()) > 5*time.Second
		}
		// Preserve ordinary batching during quota recovery instead of splitting
		// a small suffix into another upload while live requests are throttled.
		if healthy {
			w.held = 0
			return true
		}
	}

	// While uploads are outstanding, hold only a buffer too small for an
	// inline segment, so the rest of a split burst leaves a piece every tick.
	if len(w.slots) > 0 && w.held < maxCoalescedTicks && len(w.buf) < min(w.size/2, slotInlineLimit) {
		w.held++
		return false
	}
	if grew && w.held < maxCoalescedTicks {
		w.held++
		return false
	}
	w.held = 0
	return true
}

// writeWindow is called with mu held. Two full laps distinguish a sustained
// transfer from a burst and finish lazy creation before widening the window.
func (w *slotWriter) writeWindow() int {
	return w.writeWindowFor(w.batch != nil && w.batch.inlineNext(len(w.buf), w.pendingUploadBytes, len(w.slots)))
}

func (w *slotWriter) writeWindowFor(inline bool) int {
	depth := len(w.table.Slots)
	base := min(depth, slotWriteInflight)
	if inline {
		return w.batch.window
	}
	if depth < slotBulkDepth || w.bulkSegments < 2*depth || w.bulkLimited {
		return base
	}
	if s, ok := w.store.(throttleStore); ok && s.throttled() != w.throttles {
		return base
	}
	if s, ok := w.store.(interface{ cooling() bool }); ok && s.cooling() {
		return base
	}
	// Beyond the bandwidth-delay product more bulk uploads only queue, and a
	// small frame written behind them waits for the whole queue.
	return max(base, w.uplink.current(cap(w.slots)))
}

// releaseSlotLocked wakes every flusher so a reduced window or a cancelled
// writer cannot leave another waiter asleep while a permit is available.
func (w *slotWriter) releaseSlotLocked() {
	<-w.slots
	close(w.available)
	w.available = make(chan struct{})
}

// flushLocked sends a segment off buf once a slot is free, which it waits for
// without w.mu, so that a reader that falls behind holds Write back. w.mu is
// held. Either stop channel, when closed, ends the wait with errWriteStopped
func (w *slotWriter) flushLocked(deadline, closing <-chan struct{}) error {
	return w.flushAdmittedLocked(deadline, closing, false)
}

func (w *slotWriter) flushAdmittedLocked(deadline, closing <-chan struct{}, allowClosed bool) error {
	if err := w.admissionErrorLocked(deadline, closing, allowClosed); err != nil {
		return err
	}
	var stop <-chan struct{}
	if !allowClosed {
		stop = w.flushStop
	}
	var permitWait time.Duration
	bulk := false
	for {
		inline := w.batch != nil && w.batch.inlineNext(len(w.buf), w.pendingUploadBytes, len(w.slots))
		// Returning upload permits does not make an already-backlogged burst
		// fresh. Keep its bulk decision through this admission wait instead of
		// renewing a two-wave inline plan as the old requests finish.
		bulk = bulk || w.batch != nil && !inline
		if len(w.slots) < w.writeWindowFor(inline && !bulk) {
			break
		}
		available := w.available
		w.flushing = true
		waitAt := time.Now()
		w.mu.Unlock()
		var stopped error
		select {
		case <-available:
		case <-w.ctx.Done():
			stopped = w.ctx.Err()
		case <-stop:
			stopped = io.ErrClosedPipe
		case <-deadline:
			stopped = errWriteStopped
		case <-closing:
			stopped = errWriteStopped
		}
		w.mu.Lock()
		permitWait += time.Since(waitAt)
		w.flushing = false
		if stopped != nil {
			return stopped
		}
		if err := w.admissionErrorLocked(deadline, closing, allowClosed); err != nil {
			return err
		}
	}
	if err := w.admissionErrorLocked(deadline, closing, allowClosed); err != nil {
		return err
	}
	if len(w.slots) == 0 {
		w.ackMu.Lock()
		w.progress = time.Now()
		w.ackMu.Unlock()
	}
	w.slots <- struct{}{}
	if w.err != nil || len(w.buf) == 0 {
		w.releaseSlotLocked()
		return w.err
	}

	base := min(len(w.table.Slots), slotWriteInflight)
	if w.size < w.segmentBytes && len(w.buf) >= w.size && len(w.slots)*4 > base*3 {
		w.size = w.segmentBytes
		w.full = 0
	}

	n := min(len(w.buf), w.size)
	inlineBudget, inlineBurstBudget, inlinePlanned := -1, -1, -1
	if w.batch != nil {
		n = min(len(w.buf), w.segmentBytes)
		// Concurrent flushers may leave a committed prefix or short suffix;
		// preserve those, but do not re-plan the rejected large burst.
		if !bulk || w.batch.planned > 0 || len(w.buf) <= 4*w.batch.unit {
			n = w.batch.frameSize(len(w.buf), w.pendingUploadBytes, len(w.slots)-1, w.segmentBytes)
		}
		inlineBudget, inlineBurstBudget, inlinePlanned = w.batch.budget, w.batch.burstBudget, w.batch.planned
		if n > slotInlineLimit {
			w.size, w.full = w.segmentBytes, 0
		}
	} else {
		if len(w.buf) > 4*slotInlineLimit {
			n = min(len(w.buf), w.segmentBytes)
		}
		free := base - len(w.slots) + 1
		if (len(w.buf)+slotInlineLimit-1)/slotInlineLimit <= free+base/2 {
			n = min(len(w.buf), slotInlineLimit, w.segmentBytes)
		}
	}
	if n >= w.size && w.size < w.segmentBytes {
		w.full += n / w.size
	}
	if n == w.segmentBytes && n > slotInlineLimit {
		w.bulkSegments = min(w.bulkSegments+1, 2*len(w.table.Slots))
	} else {
		w.bulkSegments = 0
	}

	selectedAt, bufferEpisodeAt, bufferBytes := time.Now(), w.bufEpisodeAt, len(w.buf)
	chunk := bytes.Clone(w.buf[:n])
	if w.bufBase == nil {
		w.bufBase = w.buf[:0]
	}
	w.buf = w.buf[n:]
	if len(w.buf) == 0 {
		w.buf = w.bufBase
	}
	seq := w.seq
	w.seq++
	if seq == 0 && w.first != nil {
		select {
		case w.first <- chunk:
		default:
		}
	}
	w.lastSize = len(w.buf)
	w.held = 0

	idx := int(seq % int64(len(w.table.Slots)))
	w.pendingUploadBytes += n
	prev, result := w.gates[idx], &slotWriteResult{
		done:                   make(chan struct{}),
		bufferEpisodeAt:        bufferEpisodeAt,
		selectedAt:             selectedAt,
		assignedAt:             time.Now(),
		permitWait:             permitWait,
		bufferBytes:            bufferBytes,
		localOutstandingBytes:  len(w.buf) + w.pendingUploadBytes,
		inlineBudgetBytes:      inlineBudget,
		inlineBurstBudgetBytes: inlineBurstBudget,
		inlinePlannedBytes:     inlinePlanned,
		accountedBytes:         n,
		accounted:              true,
		inlinePrefix:           w.table.streamPrefix && len(w.slots) == 1 && n >= 2*slotStreamBlockBytes,
	}
	w.gates[idx] = result
	w.wg.Add(1)
	go w.upload(seq, chunk, prev, result)
	return nil
}

func (w *slotWriter) slotID(seq int64) string {
	return w.table.Slots[seq%int64(len(w.table.Slots))]
}

func (w *slotWriter) upload(seq int64, chunk []byte, prev, result *slotWriteResult) {
	workerAt := time.Now()
	defer w.wg.Done()
	defer func() {
		w.mu.Lock()
		if result.accounted {
			w.pendingUploadBytes -= result.accountedBytes
		}
		w.releaseSlotLocked()
		w.mu.Unlock()
	}()
	defer close(result.done)

	mode := byte(slotMedia)
	if len(chunk) <= slotInlineLimit {
		mode = slotInline
	} else if s, ok := w.store.(interface{ streamingEnabled() bool }); ok && s.streamingEnabled() {
		mode = slotStream
	}
	prefetchAt := time.Now()
	w.prefetchWatermark(seq)
	began := time.Now()
	if err := w.awaitPrevious(seq, prev); err != nil {
		result.err = err
		w.fail(errors.New("failed to store segment ", seq).Base(err))
		return
	}
	awaited := time.Since(began)
	putAt := time.Now()
	var prefix []byte
	if mode == slotStream && result.inlinePrefix {
		prefix = chunk[:slotStreamPrefixBytes]
	}
	if err := w.write(seq, mode, chunk[len(prefix):], prefix); err != nil {
		w.fail(errors.New("failed to store segment ", seq).Base(err))
		// Completion includes the best-effort error frame, before reuse can
		// observe the failed predecessor and leave that frame intact.
		w.write(seq, slotErr, nil, nil)
		result.err = err
		return
	}
	put := time.Since(putAt)
	if mode != slotInline {
		w.mu.Lock()
		w.uplink.observe(put, bodyGateMin, cap(w.slots))
		w.mu.Unlock()
	}
	bufferEpisodeAge, permitWait, queueWait, prefetchSetup := result.durations(workerAt, prefetchAt, began)
	localOutstandingBytes, bufferBytes := -1, -1
	inlineBudget, inlineBurstBudget, inlinePlanned := -1, -1, -1
	if !result.assignedAt.IsZero() {
		localOutstandingBytes, bufferBytes = result.localOutstandingBytes, result.bufferBytes
		inlineBudget, inlineBurstBudget, inlinePlanned = result.inlineBudgetBytes, result.inlineBurstBudgetBytes, result.inlinePlannedBytes
	}
	errors.LogDebug(w.ctx, "xdrive write ", slotTag(w.table.Watermark), " seq=", seq, " bytes=", len(chunk),
		" mode=", mode, " await=", awaited, " put=", put,
		" buffer-episode-age=", bufferEpisodeAge, " upload-permit-wait=", permitWait,
		" upload-queue=", queueWait, " prefetch-setup=", prefetchSetup,
		" local-outstanding-bytes=", localOutstandingBytes, " buffer-bytes=", bufferBytes,
		" inline-budget-bytes=", inlineBudget, " inline-burst-budget-bytes=", inlineBurstBudget,
		" inline-planned-bytes=", inlinePlanned, " inline-prefix-bytes=", len(prefix))
}

func (w *slotWriter) write(seq int64, mode byte, data, prefix []byte) (err error) {
	defer func() {
		if err == nil && mode != slotErr {
			w.ackMu.Lock()
			w.progress = time.Now()
			w.ackMu.Unlock()
		}
	}()
	if err := w.ctx.Err(); err != nil {
		return err
	}
	f := newSlotFrame(seq, mode, data)
	f.prefix = prefix
	var body []byte
	if mode == slotMedia || mode == slotStream {
		body = data
	}
	id := w.slotID(seq)
	if w.lazy {
		idx := int(seq % int64(len(w.table.Slots)))
		w.mu.Lock()
		created := w.created[idx]
		w.mu.Unlock()
		if !created {
			if err := w.store.createSlot(w.ctx, id, w.table.name+"/s"+strconv.Itoa(idx), f.encode(), body); err != nil {
				return err
			}
			w.mu.Lock()
			w.created[idx] = true
			w.mu.Unlock()
			return nil
		}
	}
	return w.store.setSlot(w.ctx, id, f.encode(), body)
}

// drained reports whether the reader took what the slot of seq held a lap
// before. The watermark GET runs without ackMu and single-flight: only the
// upload that triggers a read waits for it; the others use the cached value and
// return false, and await() re-checks every slotAckRetry.
func (w *slotWriter) drained(seq int64) (bool, error) {
	before := seq - int64(len(w.table.Slots))
	w.ackMu.Lock()
	if w.acked >= before {
		w.ackMu.Unlock()
		return true, nil
	}
	if time.Since(w.ackedAt) < w.watermarkTTL || w.reading {
		w.ackMu.Unlock()
		return false, nil
	}
	w.reading = true
	w.ackMu.Unlock()

	if _, err := w.fetchWatermark(); err != nil {
		return false, err
	}
	w.ackMu.Lock()
	ok := w.acked >= before
	w.ackMu.Unlock()
	return ok, nil
}

// fetchWatermark reads the watermark without holding ackMu and publishes the
// result. The caller sets reading=true first; it is cleared here.
func (w *slotWriter) fetchWatermark() (int64, error) {
	acked, err := readWatermark(w.ctx, w.store, w.table.Watermark, w.table.lazy)
	w.ackMu.Lock()
	w.reading = false
	// The attempt counts even when it fails: a failing watermark must not be
	// read again by every upload that retries its acknowledgement wait.
	w.ackedAt = time.Now()
	if err == nil && acked > w.acked {
		w.acked, w.progress = acked, w.ackedAt
	}
	w.ackMu.Unlock()
	return acked, err
}

// prefetchWatermark starts one background watermark refresh before an upload
// truly needs the ack, so the value is cached by the time await() checks it.
// At most one watermark GET at a time and never more than one per
// slotWatermarkTTL (the stale check).
func (w *slotWriter) prefetchWatermark(seq int64) {
	depth := int64(len(w.table.Slots))
	margin := min(depth, max(depth/4, int64(slotWriteInflight)))
	w.ackMu.Lock()
	start := seq-depth+margin > w.acked && time.Since(w.ackedAt) >= w.watermarkTTL && !w.reading
	if start {
		w.reading = true
	}
	w.ackMu.Unlock()
	if !start {
		return
	}
	// The calling upload is already registered, so the group cannot reach
	// zero before this child is registered, even while Close is waiting.
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.fetchWatermark()
	}()
}

func (w *slotWriter) await(seq int64) error {
	w.mu.Lock()
	prev := w.gates[seq%int64(len(w.gates))]
	w.mu.Unlock()
	return w.awaitPrevious(seq, prev)
}

// Reuse needs both cumulative acknowledgement and completion of the previous
// physical write, including when the acknowledgement arrives first.
func (w *slotWriter) awaitPrevious(seq int64, prev *slotWriteResult) error {
	start := time.Now()
	wait := w.ackRetry
	for {
		w.mu.Lock()
		failure, failed := w.err, w.failed
		w.mu.Unlock()
		if failure != nil {
			return failure
		}
		if err := w.ctx.Err(); err != nil {
			return err
		}
		completed := prev == nil
		if prev != nil {
			select {
			case <-prev.done:
				if prev.err != nil {
					return prev.err
				}
				completed = true
			default:
			}
		}
		ok, err := w.drained(seq)
		if ok && completed {
			return nil
		}
		if time.Since(start) >= ackDelay {
			w.mu.Lock()
			// If the ring already held the producer back during warm-up,
			// extra upload permits would only queue more work behind it.
			if w.bulkSegments < 2*len(w.table.Slots) {
				w.bulkLimited = true
			}
			w.mu.Unlock()
		}
		if isAuthError(err) {
			return err
		}
		if err == errNotFound {
			// An eager ring whose watermark is gone was cleaned up; fail at
			// once instead of waiting out the stall timeout.
			return errors.New("the session watermark of slot ", w.slotID(seq), " is gone").Base(err)
		}
		w.mu.Lock()
		closed := w.closed
		w.mu.Unlock()
		w.ackMu.Lock()
		still := time.Since(w.progress)
		w.ackMu.Unlock()
		if !closed && !completed && time.Since(start) >= slotStallTimeout {
			return errors.New("the previous write to slot ", w.slotID(seq), " did not complete").Base(err)
		}
		if !closed && min(time.Since(start), still) > slotStallTimeout || closed && still >= slotStallTimeout {
			return errors.New("the reader took nothing from slot ", w.slotID(seq), " for ", still).Base(err)
		}

		if err != nil {
			wait = min(2*wait, 2*time.Second)
		} else {
			wait = w.ackRetry
		}
		select {
		case <-w.ctx.Done():
			return w.ctx.Err()
		case <-failed:
		case <-time.After(wait):
		}
	}
}

func (w *slotWriter) fail(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err == nil {
		w.err = err
		if w.failed != nil {
			close(w.failed)
		}
		if w.available != nil {
			close(w.available)
		}
		w.available = make(chan struct{})
	}
}

func waitCloseProgress(done <-chan error, ctx context.Context, progress func() time.Time) error {
	var canceled <-chan struct{}
	if ctx != nil {
		canceled = ctx.Done()
	}
	timer := time.NewTimer(max(0, slotStallTimeout-time.Since(progress())))
	defer timer.Stop()
	for {
		select {
		case err := <-done:
			return err
		case <-canceled:
			return ctx.Err()
		case <-timer.C:
			remaining := slotStallTimeout - time.Since(progress())
			if remaining <= 0 {
				return context.DeadlineExceeded
			}
			timer.Reset(remaining)
		}
	}
}

func (w *slotWriter) closeProgress() time.Time {
	w.ackMu.Lock()
	defer w.ackMu.Unlock()
	return w.progress
}

func (w *slotWriter) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		if len(w.slots) == 0 {
			w.ackMu.Lock()
			w.progress = time.Now()
			w.ackMu.Unlock()
		}
		w.mu.Unlock()
		w.closeDone = make(chan struct{})
		done := make(chan error, 1)
		go func() {
			defer close(w.closeDone)
			done <- w.closeOrdered()
		}()
		w.closeErr = waitCloseProgress(done, w.ctx, w.closeProgress)
		if w.cancel != nil {
			w.cancel()
		}
	})
	return w.closeErr
}

func (w *slotWriter) wait() {
	w.Close()
	<-w.closeDone
}

func (w *slotWriter) closeOrdered() error {
	w.mu.Lock()
	w.closed = true
	close(w.flushStop)
	var flushErr error
	for len(w.buf) > 0 && w.err == nil {
		if flushErr = w.flushAdmittedLocked(nil, nil, true); flushErr != nil {
			break
		}
	}
	seq := w.seq
	w.mu.Unlock()

	<-w.flushDone
	w.wg.Wait()

	if err := w.Err(); err != nil {
		return err
	}
	if flushErr != nil {
		return flushErr
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if err := w.await(seq); err != nil {
		return err
	}
	return w.write(seq, slotEnd, nil, nil)
}

func (w *slotWriter) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

type slotReader struct {
	ctx   context.Context
	store slotStore
	table slotTable
	params

	ch   chan []byte
	wake chan struct{}
	acks chan struct{}
	grow chan struct{}
	wg   sync.WaitGroup

	mu      sync.Mutex
	head    int64
	started bool
	// hitAt is when a segment last came and gap the time between them, activeAt
	// when a segment came, was seen coming or Wake. Nothing for the eager window
	// is idle, and active is closed when that ends
	hitAt      time.Time
	gap        time.Duration
	activeAt   time.Time
	active     chan struct{}
	frontier   int64
	frontierAt time.Time // last advance of the observed description frontier
	err        error
	stop       context.CancelFunc
	reads      atomic.Int64
	// headReads and tailReads split reads by the role the fetch had when the
	// walk started; staleReads counts valid frames of another lap, the normal
	// not-ready answer of a ring slot; emptyReads counts 404 and empty
	// descriptions. They are diagnostic only.
	headReads         atomic.Int64
	tailReads         atomic.Int64
	staleReads        atomic.Int64
	emptyReads        atomic.Int64
	quietTailBackoffs atomic.Int64
	tailMin           time.Duration
	tailMax           time.Duration
	huntFloor         time.Duration
	huntCeiling       time.Duration
	ackRetry          time.Duration
	stallTimeout      time.Duration
	ackDelay          time.Duration
	bodyRate          float64
	lazy              bool
	seen              []bool
	wmCreated         bool

	// Completed description latency, including store waits, guards only head
	// overlap admission; primary reads and the polling tick are unchanged.
	descriptionLatency time.Duration // guarded by mu

	// bodies owns ordered media admission and undelivered byte credit.
	bodies bodyGate
}

func newSlotReader(ctx context.Context, store slotStore, table slotTable, p params, first []byte) *slotReader {
	head := int64(0)
	if len(first) > 0 {
		head = 1
	}
	runCtx, stop := context.WithCancel(ctx)
	r := &slotReader{
		ctx:          runCtx,
		stop:         stop,
		store:        store,
		table:        table,
		params:       p,
		head:         head,
		started:      len(first) > 0,
		ch:           make(chan []byte, max(p.concurrency, 1)),
		wake:         make(chan struct{}, 1),
		acks:         make(chan struct{}, 1),
		grow:         make(chan struct{}, 1),
		frontier:     -1,
		frontierAt:   time.Now(),
		hitAt:        time.Now(),
		activeAt:     time.Now(),
		active:       make(chan struct{}),
		tailMin:      slotTailMin,
		tailMax:      slotTailMax,
		huntFloor:    slotHuntFloor,
		huntCeiling:  slotHuntCeiling,
		ackRetry:     slotAckRetry,
		stallTimeout: slotStallTimeout,
		ackDelay:     ackDelay,
		lazy:         table.lazy,
	}
	if r.lazy {
		r.seen = make([]bool, len(table.Slots))
	}
	r.bodies.configure(head, p.segmentBytes, len(table.Slots))
	if len(first) > 0 {
		r.ch <- first
		select {
		case r.acks <- struct{}{}:
		default:
		}
	}
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		r.run()
	}()
	go func() {
		defer r.wg.Done()
		r.ackLoop()
	}()
	return r
}

func (r *slotReader) chunks() <-chan []byte {
	return r.ch
}

func (r *slotReader) wait() {
	r.wg.Wait()
}

// Wake has the head asked for now, as an answer is likely to come
func (r *slotReader) Wake() {
	r.mu.Lock()
	r.touch()
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *slotReader) slotID(seq int64) string {
	return r.table.Slots[seq%int64(len(r.table.Slots))]
}

type slotFetch struct {
	ready        chan struct{}
	streamReady  chan struct{}
	stream       *slotStreamBuffer
	firstBlockAt time.Time // run-owned first verified delivery, before whole-frame ACK
	delivery     streamDeliveryTiming
	// promoted is closed when the segment becomes the head
	promoted      chan struct{}
	near          chan struct{}
	body          []byte
	end           bool
	err           error
	born          time.Time
	headAt        time.Time // run's first wait for this head, not prefetch admission
	readyAt       time.Time
	visibleAt     time.Time
	bodyTime      time.Duration
	bodyAdmission time.Duration
	prefixBytes   int
	mode          byte
	permit        *bodyPermit
}

func (f *slotFetch) enter() {
	select {
	case <-f.near:
	default:
		close(f.near)
	}
}

func (r *slotReader) run() {
	defer close(r.ch)
	ctx, cancel := context.WithCancel(r.ctx)
	defer func() {
		cancel()
		r.bodies.close()
	}()

	fetches := make(map[int64]*slotFetch)
	for {
		r.mu.Lock()
		head, started, frontier := r.head, r.started, r.frontier
		r.mu.Unlock()
		r.bodies.advance(head)

		window := 1
		if started || frontier >= head {
			window = min(max(int(frontier-head)+slotFetchAhead, slotFetchAhead), len(r.table.Slots))
		}
		for i := range window {
			seq := head + int64(i)
			if fetches[seq] == nil {
				fetches[seq] = &slotFetch{ready: make(chan struct{}), streamReady: make(chan struct{}), promoted: make(chan struct{}), near: make(chan struct{}), born: time.Now()}
				if int64(i) <= slotReadAhead {
					fetches[seq].enter()
				}
				r.wg.Add(1)
				go func(f *slotFetch) {
					defer r.wg.Done()
					r.fetch(ctx, seq, i, f)
				}(fetches[seq])
			}
		}

		f := fetches[head]
		if f.headAt.IsZero() {
			f.headAt = time.Now()
		}
		select {
		case <-f.ready:
		case <-f.streamReady:
		case <-r.grow:
			continue
		case <-ctx.Done():
			return
		}
		streamed := false
		select {
		case <-f.streamReady:
			complete, err := r.deliverStream(ctx, f)
			if err != nil {
				r.setErr(err)
				return
			}
			if !complete {
				continue
			}
			streamed = true
		default:
		}
		delete(fetches, head)
		if f.err != nil {
			r.setErr(f.err)
			return
		}
		if f.end {
			return
		}

		// the slot is free once the bytes are here, not once they are read
		r.mu.Lock()
		r.head++
		r.started = true
		r.mu.Unlock()
		size := len(f.body)
		firstBlockWait := time.Duration(0)
		if streamed {
			size = f.stream.frame.size + f.prefixBytes
			firstBlockWait = max(0, f.firstBlockAt.Sub(f.headAt))
		}
		bodyWindow, bodyActive, bodyOutstanding, bodyBuffered, bodyBudget := r.bodies.snapshot()
		errors.LogDebug(r.ctx, "xdrive read ", slotTag(r.table.Watermark), " seq=", head, " fetch=",
			f.readyAt.Sub(f.born), " blocked=", time.Since(f.readyAt), " bytes=", size,
			" reads=", r.reads.Load(), " heads=", r.headReads.Load(), " tails=", r.tailReads.Load(),
			" stale=", r.staleReads.Load(), " empty=", r.emptyReads.Load(),
			" mode=", f.mode, " discover=", f.visibleAt.Sub(f.born), " body=", f.bodyTime,
			" head-wait=", max(0, f.readyAt.Sub(f.headAt)), " overlap-delay=", r.headOverlapDelay(), " body-window=", bodyWindow,
			" body-active=", bodyActive, " body-outstanding-bytes=", bodyOutstanding,
			" body-buffered-bytes=", bodyBuffered, " body-budget-bytes=", bodyBudget,
			" quiet-tail-backoffs=", r.quietTailBackoffs.Load(), " first-block-wait=", firstBlockWait,
			" media-admission=", f.bodyAdmission, " stream-blocks=", f.delivery.blocks,
			" missing-block-gaps=", f.delivery.missingCount,
			" missing-block-wait=", f.delivery.missingTotal, " missing-block-max=", f.delivery.missingMax,
			" enqueue-wait=", f.delivery.enqueueTotal, " enqueue-max=", f.delivery.enqueueMax,
			" completion-after-blocks=", f.delivery.completionWait(f.readyAt), " slot=", slotTag(r.slotID(head)), " inline-prefix-bytes=", f.prefixBytes)
		if next := fetches[head+1]; next != nil {
			close(next.promoted)
			if nf := fetches[head+1+slotReadAhead]; nf != nil {
				nf.enter()
			}
		}
		select {
		case r.acks <- struct{}{}:
		default:
		}

		if !streamed {
			select {
			case r.ch <- f.body:
				f.permit.deliver(len(f.body))
			case <-ctx.Done():
				return
			}
		}
	}
}

func (r *slotReader) isHead(seq int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.head == seq
}

func (r *slotReader) bursting() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return time.Since(r.hitAt) <= slotReadAheadBurst
}

func (r *slotReader) reach(seq int64) {
	r.mu.Lock()
	grown := seq > r.frontier
	if grown {
		r.frontier = seq
		r.frontierAt = time.Now()
	}
	r.mu.Unlock()
	if grown {
		select {
		case r.grow <- struct{}{}:
		default:
		}
	}
}

// fetch asks for the slot of seq until it holds seq: the head often, a segment
// behind it soon after it was asked for, then at the pace that segments come
func (r *slotReader) fetch(ctx context.Context, seq int64, offset int, f *slotFetch) {
	defer func() {
		if len(f.body) == 0 && f.stream == nil && !f.end && f.err == nil {
			f.err = ctx.Err()
		}
		f.readyAt = time.Now()
		close(f.ready)
	}()
	if offset > 0 && !r.hold(ctx, r.tailMin, f.promoted, f.near) {
		return
	}

	id := r.slotID(seq)
	newDescriptionContext := func() (context.Context, context.CancelFunc) {
		return context.WithCancel(ctx)
	}
	descriptionCtx, cancelDescription := newDescriptionContext()
	defer func() { cancelDescription() }()
	hunt := slotDescriptionHunt{results: make(chan slotDescriptionResult, 4)}
	backoff := r.huntFloor
	tailMisses, absentMisses := 0, 0
	_, drive := r.store.(throttleStore)
	var goneSince, warnedAt, visibleSince, missingSince, failedSince time.Time
	for {
		observations, present := hunt.observations, hunt.present
		description, err := r.huntSlotDescription(descriptionCtx, id, seq, f.promoted, &hunt)
		if hunt.observations != observations {
			failedSince = time.Time{}
		}
		if hunt.present != present {
			goneSince = time.Time{}
		}
		frame, ok := decodeSlot(description)
		missing := err != nil || !ok || frame.seq != seq
		if missing {
			tailMisses = min(tailMisses+1, 3)
		} else {
			tailMisses = 0
		}
		notWritten := false
		if err == errNotFound && r.lazy {
			r.mu.Lock()
			notWritten = !r.seen[int(seq%int64(len(r.table.Slots)))]
			r.mu.Unlock()
		}
		// Count only consecutive clean absence. A 404 for a previously seen
		// slot, another HTTP error, or an invalid description must not earn the
		// longer quiet backoff. Other stores retain their existing schedule.
		if drive && (notWritten || err == nil && description == "") {
			absentMisses = min(absentMisses+1, 3)
		} else {
			absentMisses = 0
		}
		r.mu.Lock()
		progressPastHead := r.head == seq && r.frontier > seq
		r.mu.Unlock()
		if missing && progressPastHead {
			if missingSince.IsZero() {
				missingSince = time.Now()
			} else if time.Since(missingSince) > r.stallTimeout {
				f.err = errors.New("segment ", seq, " is missing despite later slot frames after ", r.stallTimeout).Base(err)
				return
			}
		} else if !missing {
			missingSince = time.Time{}
		}
		if err == errNotFound || (err == nil && description == "") {
			visibleSince = time.Time{}
		}
		switch {
		case err == nil && ok && frame.seq == seq:
			cancelDescription()
			if f.visibleAt.IsZero() {
				f.visibleAt = time.Now()
			}
			f.mode = frame.mode
			// what follows it is asked for while its body comes
			r.mu.Lock()
			r.touch()
			if r.lazy {
				r.seen[int(seq%int64(len(r.table.Slots)))] = true
			}
			r.mu.Unlock()
			r.reach(seq)
			switch frame.mode {
			case slotEnd:
				f.end = true
				return
			case slotErr:
				f.err = errors.New("the peer could not store segment ", seq)
				return
			case slotInline:
				f.body = frame.data
				r.hit()
				return
			case slotStream:
				store, streams := r.store.(interface {
					readFrameStream(context.Context, string, slotFrame, func(int, []byte) error) error
				})
				if len(frame.prefix) != 0 && (!r.table.streamPrefix || !streams) {
					f.err = errors.New("unnegotiated inline stream prefix")
					return
				}
				if streams {
					admissionAt := time.Now()
					f.prefixBytes = len(frame.prefix)
					permit, admitted := r.bodies.acquire(ctx, seq, frame.size+f.prefixBytes)
					if !admitted {
						f.err = ctx.Err()
						return
					}
					f.bodyAdmission += time.Since(admissionAt)
					f.permit = permit
					f.stream = newSlotStreamBuffer(frame)
					f.stream.credit = permit
					permit.received(f.prefixBytes)
					frame.prefix = nil
					close(f.streamReady)
					bodyCtx := context.WithValue(ctx, mediaPromotionKey{}, (<-chan struct{})(f.promoted))
					bodyCtx = context.WithValue(bodyCtx, mediaDeliveryKey{}, f.stream.deliveryState)
					timing := &bodyReadTiming{}
					bodyCtx = context.WithValue(bodyCtx, bodyReadTimingKey{}, timing)
					if r.isHead(seq) {
						bodyCtx = context.WithValue(bodyCtx, mediaHeadKey{}, true)
					}
					began := time.Now()
					f.err = store.readFrameStream(bodyCtx, id, frame, f.stream.offer)
					f.bodyTime = time.Since(began)
					if f.err == nil && !f.stream.complete() {
						f.err = errors.New("streamed slot ended without every verified block")
					}
					permit.finish(timing.result(f.bodyTime), f.err == nil)
					if f.err == nil {
						r.hit()
					} else {
						r.setErr(f.err)
					}
					return
				}
				fallthrough
			case slotMedia:
				admissionAt := time.Now()
				permit, admitted := r.bodies.acquire(ctx, seq, frame.size)
				if !admitted {
					return
				}
				f.bodyAdmission += time.Since(admissionAt)
				f.permit = permit
				began := time.Now()
				bodyCtx := context.WithValue(ctx, mediaPromotionKey{}, (<-chan struct{})(f.promoted))
				timing := &bodyReadTiming{}
				bodyCtx = context.WithValue(bodyCtx, bodyReadTimingKey{}, timing)
				body, bodyErr := r.readMediaBody(bodyCtx, id, seq, frame)
				bodyTime := time.Since(began)
				f.bodyTime += bodyTime
				if bodyErr == nil {
					permit.received(len(body))
				}
				permit.finish(timing.result(bodyTime), bodyErr == nil)
				if isAuthError(bodyErr) {
					f.err = bodyErr
					r.setErr(bodyErr)
					return
				}
				if bodyErr == nil {
					f.body = body
					r.hit()
					return
				}
				if ctx.Err() == nil && time.Since(warnedAt) > 10*time.Second {
					warnedAt = time.Now()
					if bodyErr != nil {
						errors.LogWarningInner(ctx, bodyErr, "failed to read body of slot ", id)
					} else {
						errors.LogWarning(ctx, "body of slot ", id, " does not match segment ", seq)
					}
				}
				if visibleSince.IsZero() {
					visibleSince = time.Now()
				}
				if time.Since(visibleSince) > r.stallTimeout {
					f.err = errors.New("segment ", seq, " has no usable body after ", r.stallTimeout).Base(bodyErr)
					return
				}
				descriptionCtx, cancelDescription = newDescriptionContext()
				hunt = slotDescriptionHunt{results: make(chan slotDescriptionResult, 4), observations: hunt.observations, present: hunt.present}
			}
		case err == nil && !ok && description != "":
			// A present but undecodable description for this slot is not idle;
			// fail once it has persisted beyond the stall timeout.
			if visibleSince.IsZero() {
				visibleSince = time.Now()
			}
			if time.Since(visibleSince) > r.stallTimeout {
				f.err = errors.New("slot ", id, " of segment ", seq, " holds an invalid frame")
				return
			}
		case err == errNotFound:
			if notWritten {
				// A lazy slot that has never held a frame is not written yet.
				goneSince = time.Time{}
			} else if goneSince.IsZero() {
				goneSince = time.Now()
			} else if time.Since(goneSince) > slotGoneTimeout && r.isHead(seq) {
				f.err = errors.New("slot ", id, " of segment ", seq, " is gone").Base(err)
				return
			}
		case err != nil:
			if isAuthError(err) {
				f.err = err
				r.setErr(err)
				return
			}
			if failedSince.IsZero() {
				failedSince = time.Now()
			}
			if time.Since(failedSince) >= r.stallTimeout {
				f.err = errors.New("failed to read segment ", seq, " for ", r.stallTimeout).Base(err)
				return
			}
			goneSince = time.Time{}
			if ctx.Err() == nil && time.Since(warnedAt) > 10*time.Second {
				warnedAt = time.Now()
				errors.LogWarningInner(ctx, err, "failed to read slot ", id)
			}
		}

		r.mu.Lock()
		head, idle, active := r.head == seq, time.Since(r.activeAt) > r.eagerWindow, r.active
		r.mu.Unlock()
		wait, a, b := backoff, (<-chan struct{})(r.wake), (<-chan struct{})(nil)
		switch {
		case head:
			ceiling := r.huntCeiling
			if idle {
				ceiling = max(ceiling, r.maxPollInterval)
			}
			backoff = min(2*backoff, ceiling)
		case idle:
			// Only the head is asked for until something comes. promoted stays:
			// run() may move the head here just after head was read, and its
			// closed channel must still wake the fetch now that it is the head.
			wait, a, b, backoff = 0, f.promoted, active, r.huntFloor
		default:
			wait, a, b, backoff = min(max(r.pace(), r.tailMin), r.tailMax), f.promoted, f.near, r.huntFloor
			extended := false
			// Repeated misses far ahead are predictions, not a stalled head.
			// Back these off instead of spending quota on the same old frame;
			// promotion/near admission still interrupts the wait immediately.
			r.mu.Lock()
			distant := seq > max(r.head, r.frontier)+slotReadAhead
			quietFrontier := time.Since(r.frontierAt) > r.eagerWindow
			r.mu.Unlock()
			if distant && tailMisses > 1 {
				ceiling := r.tailMax
				if tailMisses >= 3 && quietFrontier && (err == nil && ok && frame.seq < seq || absentMisses >= 3) {
					// Writes can keep a session eager even when the peer has no
					// new frames. Valid older laps and repeated clean Drive
					// absence slow down; errors/invalid frames do not qualify.
					// Head/near promotion still interrupts this wait.
					ceiling = max(ceiling, slotTailQuietMax)
				}
				wait = min(wait*time.Duration(1<<(tailMisses-1)), ceiling)
				extended = wait > r.tailMax
			}
			select {
			case <-f.near:
				b = nil
				// Near admission may race the distant snapshot above. Never
				// carry a far-future extension into the ordinary near schedule.
				wait = min(wait, r.tailMax)
				extended = false
				if r.bursting() {
					wait = slotReadAheadPoll
				}
			default:
			}
			if extended {
				r.quietTailBackoffs.Add(1)
			}
		}
		if !r.hold(ctx, wait, a, b) {
			return
		}
	}
}

type slotDescriptionResult struct {
	description string
	err         error
}

type slotDescriptionHunt struct {
	results      chan slotDescriptionResult
	outstanding  int
	lastLaunch   time.Time  // retained across observations and tail promotion
	spread       connSpread // keeps overlapped head reads on different connections
	last         slotDescriptionResult
	seen         bool
	err          error
	observations uint64
	present      uint64
}

// Frame-aware stores validate the complete size and checksum before returning;
// only generic body readers need the validation here.
func (r *slotReader) readMediaBody(ctx context.Context, id string, seq int64, frame slotFrame) ([]byte, error) {
	if store, ok := r.store.(interface {
		readFrameBody(context.Context, string, slotFrame) ([]byte, error)
	}); ok {
		if r.isHead(seq) {
			ctx = context.WithValue(ctx, mediaHeadKey{}, true)
		}
		return store.readFrameBody(ctx, id, frame)
	}
	body, err := r.readBodyHedged(ctx, id, frame.size)
	if err == nil && !frame.holds(body) {
		err = errors.New("slot body does not match the segment checksum")
	}
	return body, err
}

func (r *slotReader) readSlotDescription(ctx context.Context, id string, seq int64, promoted <-chan struct{}) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	hunt := slotDescriptionHunt{results: make(chan slotDescriptionResult, 4)}
	return r.huntSlotDescription(ctx, id, seq, promoted, &hunt)
}

func (r *slotReader) headOverlapDelay() time.Duration {
	r.mu.Lock()
	latency := r.descriptionLatency
	r.mu.Unlock()
	delay := 250 * time.Millisecond
	if latency > 0 {
		// Stagger up to three head polls across one request latency so the
		// head is sampled about every latency/3, not once per latency.
		delay = latency / 3
	}
	// The 100 ms floor keeps the overlap from multiplying requests; the cap
	// bounds recovery when requests stall or the latency estimate is cold.
	return min(max(delay, max(4*r.huntCeiling, 100*time.Millisecond)), time.Second)
}

func (r *slotReader) huntSlotDescription(ctx context.Context, id string, seq int64, promoted <-chan struct{}, pending *slotDescriptionHunt) (string, error) {
	r.mu.Lock()
	head := r.head == seq
	var wake <-chan struct{}
	if head {
		promoted = nil
		wake = r.wake
	}
	head = head && time.Since(r.activeAt) <= r.eagerWindow
	r.mu.Unlock()
	var ticker *time.Ticker
	var ticks <-chan time.Time
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	hunt := func() {
		head = true
		promoted = nil
		if ticker == nil {
			ticker = time.NewTicker(r.huntCeiling)
		}
		ticks, wake = ticker.C, r.wake
	}
	if head {
		hunt()
	}
	// Drive already hedges individual slow metadata requests. During quota
	// recovery, overlapping three hunts multiplies attempts without helping
	// visibility; leave existing requests alone but admit only one new read.
	budget := func() int {
		if s, ok := r.store.(interface {
			cooling() bool
			lastRateLimitAt() time.Time
		}); ok && (s.cooling() || time.Since(s.lastRateLimitAt()) <= 5*time.Second) {
			return 1
		}
		return 3
	}

	// Yield observations for fetch's stall checks without canceling or
	// restarting sibling requests. Their lifetime belongs to the description round.
	observationWindow := max(3*r.huntCeiling, min(r.stallTimeout/4, r.eagerWindow))
	observationTimer := time.NewTimer(observationWindow)
	defer observationTimer.Stop()
	launch := func() {
		pending.outstanding++
		pending.lastLaunch = time.Now()
		readCtx := ctx
		role := head
		if head {
			readCtx = context.WithValue(ctx, laneKey{}, true)
			readCtx = context.WithValue(readCtx, avoidConnKey{}, &pending.spread)
		}
		results := pending.results
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.reads.Add(1)
			if role {
				r.headReads.Add(1)
			} else {
				r.tailReads.Add(1)
			}
			began := time.Now()
			description, err := r.store.readDescription(readCtx, id)
			if elapsed := time.Since(began); elapsed > 0 && (err == nil || err == errNotFound) && ctx.Err() == nil {
				// Empty/stale frames and 404s are completed observations too;
				// cancelled siblings and errors must not train the estimate.
				r.mu.Lock()
				if r.descriptionLatency == 0 {
					r.descriptionLatency = elapsed
				} else {
					r.descriptionLatency += (elapsed - r.descriptionLatency) / 8
				}
				r.mu.Unlock()
			}
			results <- slotDescriptionResult{description, err}
		}()
	}
	admit := func() {
		// Never hold back a primary. Every additional head read waits from
		// the latest launch, including a pending tail read on promotion, so
		// observation yields and Wake cannot refill the overlap at 25ms.
		if pending.outstanding < budget() && (pending.outstanding == 0 || head && time.Since(pending.lastLaunch) >= r.headOverlapDelay()) {
			launch()
		}
	}
	admit()
	for {
		select {
		case res := <-pending.results:
			pending.outstanding--
			if isAuthError(res.err) {
				return "", res.err
			}
			if res.err == nil || res.err == errNotFound {
				pending.observations++
			}
			if res.err == nil {
				pending.present++
				if res.description == "" {
					r.emptyReads.Add(1)
				} else if frame, ok := decodeSlot(res.description); ok && frame.seq != seq {
					r.staleReads.Add(1)
				}
			} else if res.err == errNotFound {
				r.emptyReads.Add(1)
			}
			if !head {
				return res.description, res.err
			}
			pending.last, pending.seen = res, true
			if res.err != nil {
				if pending.err == nil || isAuthError(res.err) {
					pending.err = res.err
				}
			} else if frame, ok := decodeSlot(res.description); ok && frame.seq == seq {
				pending.err = nil
				return res.description, nil
			}
			if pending.outstanding == 0 && pending.err != nil {
				err := pending.err
				pending.err = nil
				return "", err
			}
		case <-promoted:
			// Keep the pending tail read in the head's overlap budget rather
			// than waiting for it to finish before starting the head hunt.
			hunt()
			admit()
		case <-ticks:
			r.mu.Lock()
			idle := time.Since(r.activeAt) > r.eagerWindow
			r.mu.Unlock()
			switch {
			case pending.seen && idle:
				err := pending.err
				pending.err = nil
				return pending.last.description, err
			case !idle || !pending.seen:
				// A blocked first observation still needs bounded recovery even
				// after the eager window expires; otherwise it can hide all data.
				admit()
			}
		case <-wake:
			hunt()
			admit()
		case <-observationTimer.C:
			if pending.seen {
				err := pending.err
				pending.err = nil
				return pending.last.description, err
			}
			r.mu.Lock()
			progressPastHead := r.head == seq && r.frontier > seq
			r.mu.Unlock()
			if progressPastHead {
				return "", nil
			}
			observationTimer.Reset(observationWindow)
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

func (r *slotReader) readBodyHedged(ctx context.Context, id string, size int) ([]byte, error) {
	if slotBodyHedge <= 0 {
		return r.store.readBody(ctx, id)
	}
	delay := slotBodyHedge
	r.mu.Lock()
	rate := r.bodyRate
	r.mu.Unlock()
	if rate > 0 && size > 0 {
		if scaled := time.Duration(float64(2*size) / rate * float64(time.Second)); scaled > delay {
			delay = scaled
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := time.Now()
	var progress struct {
		sync.Mutex
		at time.Time
	}
	ctx = context.WithValue(ctx, bodyProgressKey{}, func() {
		progress.Lock()
		progress.at = time.Now()
		progress.Unlock()
	})

	type result struct {
		body []byte
		err  error
	}
	results := make(chan result, 2)
	launch := func() {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			body, err := r.store.readBody(ctx, id)
			results <- result{body, err}
		}()
	}
	launch()
	outstanding := 1
	timer := time.NewTimer(delay)
	defer timer.Stop()
	var last result
	for {
		select {
		case res := <-results:
			outstanding--
			if res.err == nil {
				if elapsed := time.Since(start); elapsed > 0 && len(res.body) > 0 {
					instant := float64(len(res.body)) / elapsed.Seconds()
					r.mu.Lock()
					if r.bodyRate == 0 {
						r.bodyRate = instant
					} else {
						r.bodyRate = (r.bodyRate + instant) / 2
					}
					r.mu.Unlock()
				}
				return res.body, nil
			}
			last = res
			if outstanding == 0 {
				return nil, last.err
			}
		case <-timer.C:
			// A body that is moving shares the bulk connection with its hedge;
			// duplicating it would compete with the bytes already on their way.
			progress.Lock()
			remaining := delay - time.Since(progress.at)
			progress.Unlock()
			if remaining > 0 {
				timer.Reset(remaining)
				continue
			}
			if outstanding == 1 {
				if s, ok := r.store.(interface {
					cooling() bool
					lastRateLimitAt() time.Time
				}); ok && (s.cooling() || time.Since(s.lastRateLimitAt()) <= 5*time.Second) {
					// The timer just fired and is spent; re-arm it so the
					// hedge can still start once the cooldown ends.
					timer.Reset(delay)
					continue
				}
				launch()
				outstanding++
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// pace is about how long segments take to come one after the other
func (r *slotReader) pace() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return min(max(r.gap, 300*time.Millisecond), 3*time.Second)
}

func (r *slotReader) hit() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if gap := time.Since(r.hitAt); r.gap == 0 {
		r.gap = gap
	} else {
		r.gap = (r.gap + gap) / 2
	}
	r.hitAt = time.Now()
	r.touch()
}

// touch notes that something came or is likely to. r.mu is held
func (r *slotReader) touch() {
	if time.Since(r.activeAt) > r.eagerWindow {
		close(r.active)
		r.active = make(chan struct{})
	}
	r.activeAt = time.Now()
}

// hold waits d, unless d is 0, or until a or b is closed or has a value
func (r *slotReader) hold(ctx context.Context, d time.Duration, a, b <-chan struct{}) bool {
	var timeout <-chan time.Time
	if d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-ctx.Done():
		return false
	case <-a:
	case <-b:
	case <-timeout:
	}
	return true
}

// ackLoop writes the watermark when the head moves, the last head only if it
// moved more than once meanwhile
func (r *slotReader) ackLoop() {
	acked := int64(-1)
	var oldest, failedSince, warnedAt time.Time
	depth := int64(len(r.table.Slots))
	// Writes run one at a time and each takes most of a second: a larger
	// batch holds back the writer's credit, a smaller one only keeps writes
	// back to back and makes uploads, and so body reads, come in bursts.
	batch := int64(1)
	if depth >= 16 {
		batch = depth / 6
	}
	wait := r.ackRetry
	timer := time.NewTimer(r.ackDelay)
	defer timer.Stop()
	writeWM := func(want int64) error {
		var err error
		ctx := context.WithValue(r.ctx, laneKey{}, true)
		ctx = context.WithValue(ctx, watermarkWriteKey{}, true)
		if r.lazy {
			r.mu.Lock()
			created := r.wmCreated
			r.mu.Unlock()
			if !created {
				err = r.store.createSlot(ctx, r.table.Watermark, r.table.name+"/w", strconv.FormatInt(want, 10), nil)
				if err == nil {
					r.mu.Lock()
					r.wmCreated = true
					r.mu.Unlock()
				}
			} else {
				err = r.store.setSlot(ctx, r.table.Watermark, strconv.FormatInt(want, 10), nil)
			}
		} else {
			err = r.store.setSlot(ctx, r.table.Watermark, strconv.FormatInt(want, 10), nil)
		}
		return err
	}
	for {
		r.mu.Lock()
		want := r.head - 1
		idle := time.Since(r.activeAt) > r.eagerWindow
		r.mu.Unlock()
		if want > acked && oldest.IsZero() {
			oldest = time.Now()
		}
		if want <= acked {
			oldest = time.Time{}
		}
		if want > acked && (want-acked >= batch || (!oldest.IsZero() && time.Since(oldest) >= r.ackDelay) || idle) {
			if err := writeWM(want); err == nil {
				acked, wait, oldest, failedSince = want, r.ackRetry, time.Time{}, time.Time{}
			} else {
				if r.ctx.Err() != nil {
					return
				}
				if failedSince.IsZero() {
					failedSince = time.Now()
				}
				if isAuthError(err) {
					r.setErr(err)
					return
				}
				if time.Since(failedSince) >= r.stallTimeout {
					r.setErr(errors.New("failed to acknowledge segment ", want).Base(err))
					return
				}
				if time.Since(warnedAt) > 10*time.Second {
					warnedAt = time.Now()
					errors.LogWarningInner(r.ctx, err, "failed to write session watermark")
				}
				wait = min(2*wait, 5*time.Second)
				select {
				case <-r.ctx.Done():
					return
				case <-time.After(wait):
				}
				continue
			}
		}
		// With an ack pending, the timer is due ackDelay after the oldest
		// unacknowledged segment, not after the latest event.
		next := r.ackDelay
		if !oldest.IsZero() {
			next = max(r.ackDelay-time.Since(oldest), 0)
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(next)
		select {
		case <-r.ctx.Done():
			if want > acked {
				writeWM(want)
			}
			return
		case <-r.acks:
		case <-timer.C:
		}
	}
}

func (r *slotReader) setErr(err error) {
	r.mu.Lock()
	if r.err == nil {
		r.err = err
	}
	r.mu.Unlock()
	// A stored error means the connection is dead: stop the run loop so the
	// chunk channel closes and Read returns the error instead of blocking.
	if r.stop != nil {
		r.stop()
	}
}

func (r *slotReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	return io.EOF
}

func slotTag(id string) string {
	if len(id) > 6 {
		return id[len(id)-6:]
	}
	return id
}
