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
	slotHuntFloor   = 10 * time.Millisecond
	slotHuntCeiling = 25 * time.Millisecond
	slotTailMin     = 150 * time.Millisecond
	slotTailMax     = 800 * time.Millisecond
	slotFetchAhead  = 12

	slotAckRetry     = 30 * time.Millisecond
	slotWatermarkTTL = 100 * time.Millisecond
	ackDelay         = 1500 * time.Millisecond
	slotStallTimeout = 60 * time.Second
	slotCloseGrace   = 10 * time.Second
	slotGoneTimeout  = 60 * time.Second
	slotBodyHedge    = 2 * time.Second

	slotReadAhead      int64 = 3
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
	seq  int64
	mode byte
	size int
	crc  uint32
	data []byte
}

func newSlotFrame(seq int64, mode byte, data []byte) slotFrame {
	f := slotFrame{seq: seq, mode: mode, size: len(data), crc: crc32.ChecksumIEEE(data)}
	if mode == slotInline {
		f.data = data
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
	return base64.StdEncoding.EncodeToString(append(b, f.data...))
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
	if f.mode == slotInline {
		f.data = b[slotHeaderLen:]
		if !f.holds(f.data) {
			return slotFrame{}, false
		}
	}
	return f, true
}

// holds reports whether body is the one that f is about, which a body read
// right after its description need not be yet
func (f slotFrame) holds(body []byte) bool {
	return len(body) == f.size && crc32.ChecksumIEEE(body) == f.crc
}

// slotTable is the ring of one direction: the ids of its slots and of the
// watermark of its reader. name and lazy are local carriers, not serialized.
type slotTable struct {
	Slots     []string `json:"s"`
	Watermark string   `json:"w"`
	name      string
	lazy      bool
}

// ring is what a client announces a session with, the tables are named from
// its side. Lazy rings create their slots on first write instead of up front.
type ring struct {
	Up    slotTable `json:"u"`
	Down  slotTable `json:"d"`
	First []byte    `json:"first,omitempty"`
	Lazy  bool      `json:"lazy,omitempty"`
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
	for _, t := range []slotTable{r.Up, r.Down} {
		if len(t.Watermark) > maxSlotIDLen {
			return ring{}, false
		}
		for _, id := range t.Slots {
			if len(id) == 0 || len(id) > maxSlotIDLen {
				return ring{}, false
			}
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

type slotWriter struct {
	ctx   context.Context
	store slotStore
	table slotTable
	params
	// slots has a token for each segment on its way, at most one for each slot
	slots chan struct{}
	wg    sync.WaitGroup

	mu           sync.Mutex
	buf          []byte
	seq          int64
	size         int
	full         int
	lastSize     int
	held         int
	flushing     bool          // a flush is waiting for a token with mu released
	available    chan struct{} // closed when an upload permit is returned; guarded by mu
	bulkSegments int
	bulkLimited  bool
	throttles    int64
	closed       bool
	err          error
	first        chan []byte
	lazy         bool
	created      []bool
	gates        []chan struct{} // completion of the last assigned write to each physical slot

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
	w := &slotWriter{
		ctx:       ctx,
		store:     store,
		table:     table,
		params:    p,
		slots:     make(chan struct{}, min(len(table.Slots), max(slotWriteInflight, slotBulkInflight))),
		available: make(chan struct{}),
		size:      min(slotInlineLimit, p.segmentBytes),
		acked:     -1,
		progress:  time.Now(),
		lazy:      table.lazy,
		gates:     make([]chan struct{}, len(table.Slots)),

		ackRetry:     slotAckRetry,
		watermarkTTL: slotWatermarkTTL,
	}
	if w.lazy {
		w.created = make([]bool, len(table.Slots))
	}
	if s, ok := store.(throttleStore); ok {
		w.throttles = s.throttled()
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

	if w.err != nil {
		return 0, w.err
	}
	if w.closed {
		return 0, io.ErrClosedPipe
	}

	accepted := 0
	for len(p) > 0 {
		if w.err != nil {
			return accepted, w.err
		}
		if w.closed {
			return accepted, io.ErrClosedPipe
		}
		n := min(len(p), w.segmentBytes-len(w.buf))
		w.buf = append(w.buf, p[:n]...)
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

func (w *slotWriter) flushLoop() {
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.mu.Lock()
			if !w.flushing {
				w.applyGrowLocked()
			}
			if !w.closed && w.err == nil && !w.flushing && len(w.buf) > 0 && w.readyToFlush() {
				w.flushLocked(nil, nil)
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
	depth := len(w.table.Slots)
	base := min(depth, slotWriteInflight)
	if depth < slotBulkDepth || w.bulkSegments < 2*depth || w.bulkLimited {
		return base
	}
	if s, ok := w.store.(throttleStore); ok && s.throttled() != w.throttles {
		return base
	}
	if s, ok := w.store.(interface{ cooling() bool }); ok && s.cooling() {
		return base
	}
	return cap(w.slots)
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
	select {
	case <-deadline:
		return errWriteStopped
	case <-closing:
		return errWriteStopped
	default:
	}
	for len(w.slots) >= w.writeWindow() {
		available := w.available
		w.flushing = true
		w.mu.Unlock()
		var stopped error
		select {
		case <-available:
		case <-w.ctx.Done():
			stopped = w.ctx.Err()
		case <-deadline:
			stopped = errWriteStopped
		case <-closing:
			stopped = errWriteStopped
		}
		w.mu.Lock()
		w.flushing = false
		if stopped != nil {
			return stopped
		}
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
	if len(w.buf) > 4*slotInlineLimit {
		// a burst goes at the full size at once
		n = min(len(w.buf), w.segmentBytes)
	}
	// Count the token acquired above as available to this batch. Fan out
	// inline if the whole buffer fits in the free tokens and half a window
	// more, even after the segment size has ramped during an earlier burst:
	// an inline segment that waits for one upload still arrives before media.
	free := base - len(w.slots) + 1
	if (len(w.buf)+slotInlineLimit-1)/slotInlineLimit <= free+base/2 {
		n = min(len(w.buf), slotInlineLimit, w.segmentBytes)
	}
	if n >= w.size && w.size < w.segmentBytes {
		w.full += n / w.size
	}
	if n == w.segmentBytes && n > slotInlineLimit {
		w.bulkSegments = min(w.bulkSegments+1, 2*len(w.table.Slots))
	} else {
		w.bulkSegments = 0
	}

	chunk := bytes.Clone(w.buf[:n])
	w.buf = append(w.buf[:0], w.buf[n:]...)
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
	prev, done := w.gates[idx], make(chan struct{})
	w.gates[idx] = done
	w.wg.Add(1)
	go w.upload(seq, chunk, prev, done)
	return nil
}

func (w *slotWriter) slotID(seq int64) string {
	return w.table.Slots[seq%int64(len(w.table.Slots))]
}

func (w *slotWriter) upload(seq int64, chunk []byte, prev, done chan struct{}) {
	defer w.wg.Done()
	defer func() {
		w.mu.Lock()
		w.releaseSlotLocked()
		w.mu.Unlock()
	}()
	defer close(done)

	mode := byte(slotMedia)
	if len(chunk) <= slotInlineLimit {
		mode = slotInline
	}
	w.prefetchWatermark(seq)
	began := time.Now()
	if err := w.await(seq); err != nil {
		w.fail(errors.New("failed to store segment ", seq).Base(err))
		return
	}
	// A seeded first frame can be acknowledged before its upload finishes.
	// The watermark permits reuse, but only this gate orders physical writes
	// (including lazy creation and an error marker) across laps.
	if prev != nil {
		select {
		case <-prev:
		case <-w.ctx.Done():
			w.fail(w.ctx.Err())
			return
		}
	}
	awaited := time.Since(began)
	putAt := time.Now()
	if err := w.write(seq, mode, chunk); err != nil {
		w.fail(errors.New("failed to store segment ", seq).Base(err))
		// the slot is free, the reader learns there what happened
		w.write(seq, slotErr, nil)
		return
	}
	errors.LogDebug(w.ctx, "xdrive write ", slotTag(w.table.Watermark), " seq=", seq, " bytes=", len(chunk),
		" mode=", mode, " await=", awaited, " put=", time.Since(putAt))
}

func (w *slotWriter) write(seq int64, mode byte, data []byte) error {
	f := newSlotFrame(seq, mode, data)
	var body []byte
	if mode == slotMedia {
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
	if err == nil {
		w.ackedAt = time.Now()
		if acked > w.acked {
			w.acked, w.progress = acked, w.ackedAt
		}
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
	go w.fetchWatermark()
}

// await waits for drained, for slotStallTimeout at most, or slotCloseGrace
// without progress once the writer is closed
func (w *slotWriter) await(seq int64) error {
	start := time.Now()
	wait := w.ackRetry
	for {
		ok, err := w.drained(seq)
		if ok {
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
		if min(time.Since(start), still) > slotStallTimeout || closed && still > slotCloseGrace {
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
		case <-time.After(wait):
		}
	}
}

func (w *slotWriter) fail(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err == nil {
		w.err = err
	}
}

func (w *slotWriter) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	// Publish a fresh grace period before uploads can observe closed while
	// Close is still waiting for tokens to flush the buffered tail.
	w.ackMu.Lock()
	w.progress = time.Now()
	w.ackMu.Unlock()
	w.closed = true
	for len(w.buf) > 0 && w.err == nil {
		if err := w.flushLocked(nil, nil); err != nil {
			w.mu.Unlock()
			return err
		}
	}
	seq := w.seq
	w.mu.Unlock()

	w.ackMu.Lock()
	w.progress = time.Now()
	w.ackMu.Unlock()

	w.wg.Wait()

	w.mu.Lock()
	err := w.err
	w.mu.Unlock()
	if err != nil {
		return err
	}
	if err := w.await(seq); err != nil {
		return err
	}
	return w.write(seq, slotEnd, nil)
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

	mu      sync.Mutex
	head    int64
	started bool
	// hitAt is when a segment last came and gap the time between them, activeAt
	// when a segment came, was seen coming or Wake. Nothing for the eager window
	// is idle, and active is closed when that ends
	hitAt        time.Time
	gap          time.Duration
	activeAt     time.Time
	active       chan struct{}
	frontier     int64
	err          error
	stop         context.CancelFunc
	reads        atomic.Int64
	tailMin      time.Duration
	tailMax      time.Duration
	huntFloor    time.Duration
	huntCeiling  time.Duration
	ackRetry     time.Duration
	stallTimeout time.Duration
	ackDelay     time.Duration
	bodyRate     float64
	lazy         bool
	seen         []bool
	wmCreated    bool
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
	if len(first) > 0 {
		r.ch <- first
		select {
		case r.acks <- struct{}{}:
		default:
		}
	}
	go r.run()
	go r.ackLoop()
	return r
}

func (r *slotReader) chunks() <-chan []byte {
	return r.ch
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
	ready chan struct{}
	// promoted is closed when the segment becomes the head
	promoted  chan struct{}
	near      chan struct{}
	media     chan struct{} // body prefetch admission, independent of description discovery
	body      []byte
	end       bool
	err       error
	born      time.Time
	readyAt   time.Time
	visibleAt time.Time
	bodyTime  time.Duration
	mode      byte
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
	defer cancel()

	fetches := make(map[int64]*slotFetch)
	for {
		r.mu.Lock()
		head, started, frontier := r.head, r.started, r.frontier
		r.mu.Unlock()

		window := 1
		if started || frontier >= head {
			window = min(max(int(frontier-head)+slotFetchAhead, slotFetchAhead), len(r.table.Slots))
		}
		for i := range window {
			seq := head + int64(i)
			if fetches[seq] == nil {
				fetches[seq] = &slotFetch{ready: make(chan struct{}), promoted: make(chan struct{}), near: make(chan struct{}), media: make(chan struct{}), born: time.Now()}
				if int64(i) <= slotReadAhead {
					fetches[seq].enter()
				}
				go r.fetch(ctx, seq, i, fetches[seq])
			}
			// Mixed inline/media traffic needs the same bound as a bulk stream.
			if i < slotBodyAhead {
				select {
				case <-fetches[seq].media:
				default:
					close(fetches[seq].media)
				}
			}
		}

		f := fetches[head]
		select {
		case <-f.ready:
		case <-r.grow:
			continue
		case <-ctx.Done():
			return
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
		errors.LogDebug(r.ctx, "xdrive read ", slotTag(r.table.Watermark), " seq=", head, " fetch=",
			f.readyAt.Sub(f.born), " blocked=", time.Since(f.readyAt), " bytes=", len(f.body),
			" reads=", r.reads.Load(), " mode=", f.mode, " discover=", f.visibleAt.Sub(f.born), " body=", f.bodyTime)
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

		select {
		case r.ch <- f.body:
		case <-ctx.Done():
			return
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
		f.readyAt = time.Now()
		close(f.ready)
	}()
	if offset > 0 && !r.hold(ctx, r.tailMin, f.promoted, f.near) {
		return
	}

	id := r.slotID(seq)
	backoff := r.huntFloor
	var goneSince, warnedAt, visibleSince time.Time
	for {
		description, err := r.readSlotDescription(ctx, id, seq, f.promoted)
		frame, ok := decodeSlot(description)
		if err == errNotFound || (err == nil && description == "") {
			// An absent or empty slot is idle, not a failure.
			visibleSince = time.Time{}
		}
		switch {
		case err == nil && ok && frame.seq == seq:
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
			case slotMedia:
				select {
				case <-f.media:
				case <-ctx.Done():
					return
				}
				began := time.Now()
				var body []byte
				var bodyErr error
				if store, ok := r.store.(interface {
					readFrameBody(context.Context, string, slotFrame) ([]byte, error)
				}); ok {
					bodyCtx := ctx
					if r.isHead(seq) {
						bodyCtx = context.WithValue(ctx, mediaHeadKey{}, true)
					}
					body, bodyErr = store.readFrameBody(bodyCtx, id, frame)
				} else {
					body, bodyErr = r.readBodyHedged(ctx, id, frame.size)
				}
				f.bodyTime += time.Since(began)
				if bodyErr == nil && frame.holds(body) {
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
			notWritten := false
			if r.lazy {
				r.mu.Lock()
				notWritten = !r.seen[int(seq%int64(len(r.table.Slots)))]
				r.mu.Unlock()
			}
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
			// only the head is asked for until something comes
			wait, a, b, backoff = 0, f.promoted, active, r.huntFloor
		default:
			wait, a, b, backoff = min(max(r.pace(), r.tailMin), r.tailMax), f.promoted, f.near, r.huntFloor
			select {
			case <-f.near:
				b = nil
				if r.bursting() {
					wait = slotReadAheadPoll
				}
			default:
			}
		}
		if !r.hold(ctx, wait, a, b) {
			return
		}
	}
}

func (r *slotReader) readSlotDescription(ctx context.Context, id string, seq int64, promoted <-chan struct{}) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.mu.Lock()
	head := r.head == seq
	if head {
		promoted = nil
	}
	head = head && time.Since(r.activeAt) <= r.eagerWindow
	r.mu.Unlock()
	var ticker *time.Ticker
	var ticks <-chan time.Time
	var wake <-chan struct{}
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	hunt := func() {
		head = true
		promoted = nil
		ticker = time.NewTicker(r.huntCeiling)
		ticks, wake = ticker.C, r.wake
	}
	if head {
		hunt()
	}

	type result struct {
		description string
		err         error
	}
	results := make(chan result, 4)
	outstanding := 0
	launch := func() {
		outstanding++
		readCtx := ctx
		if head {
			readCtx = context.WithValue(ctx, laneKey{}, true)
		}
		go func() {
			r.reads.Add(1)
			description, err := r.store.readDescription(readCtx, id)
			results <- result{description, err}
		}()
	}
	launch()
	var last result
	seen := false
	for {
		select {
		case res := <-results:
			outstanding--
			if !head || res.err != nil {
				if head && outstanding > 0 {
					// A failed sibling of the overlapping head hunt is not
					// definitive while another read may be about to succeed,
					// and returning would cancel it.
					continue
				}
				return res.description, res.err
			}
			if frame, ok := decodeSlot(res.description); ok && frame.seq == seq {
				return res.description, nil
			}
			last, seen = res, true
		case <-promoted:
			// Keep the pending tail read in the head's overlap budget rather
			// than waiting for it to finish before starting the head hunt.
			hunt()
		case <-ticks:
			r.mu.Lock()
			idle := time.Since(r.activeAt) > r.eagerWindow
			r.mu.Unlock()
			switch {
			case idle && seen:
				return last.description, nil
			case !idle && outstanding < 3:
				launch()
			}
		case <-wake:
			if outstanding < 3 {
				launch()
			}
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
		go func() {
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
	var oldest time.Time
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
	writeWM := func(want int64) bool {
		var err error
		ctx := context.WithValue(r.ctx, laneKey{}, true)
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
		return err == nil
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
			if writeWM(want) {
				acked, wait, oldest = want, r.ackRetry, time.Time{}
			} else {
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
