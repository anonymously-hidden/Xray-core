package xdrive

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"math/bits"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

const (
	slotMagic        = "XDRVSL01"
	slotHeaderLen    = len(slotMagic) + 17
	slotInlineLimit  = 14 << 10
	slotInline       = 1
	slotEnd          = 3
	slotErr          = 4
	maxSlotIDLen     = 128
	slotStallTimeout = time.Minute

	slotStream       byte = 5
	slotStreamPrefix byte = 6
	// Leave room for checksums and headers under the existing inline description size.
	slotStreamPrefixBytes = 11 << 10
	slotStreamBlockBytes  = 32 << 10
	// slotProgressiveMax bounds a frame read progressively by one GET.
	slotProgressiveMax = defaultSegmentBytes
)

var errWriteStopped = errors.New("write stopped")

type slotStore interface {
	takeIDs(context.Context, int) ([]string, error)
	createSlot(context.Context, string, string, string, []byte) error
	setSlot(context.Context, string, string, []byte) error
	readDescription(context.Context, string) (string, error)
	readFrameStream(context.Context, string, slotFrame, func([]byte) error) error
}

// A slot description identifies its sequence and carries either the payload
// or the checksums needed to verify a separate media body.
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
		f.blocks = make([]uint32, 0, (len(data)+slotStreamBlockBytes-1)/slotStreamBlockBytes)
		for block := range slices.Chunk(data, slotStreamBlockBytes) {
			f.blocks = append(f.blocks, crc32.ChecksumIEEE(block))
		}
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
		for _, check := range f.blocks {
			b = binary.BigEndian.AppendUint32(b, check)
		}
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
		if f.size == 0 || f.size > slotInlineLimit || len(f.data) != f.size || crc32.ChecksumIEEE(f.data) != f.crc {
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

// Validate the whole CRC table before allowing progressive delivery.
func decodeStreamChecksums(size int, wholeCRC uint32, trailer []byte) ([]uint32, bool) {
	if size <= 0 || size > maxSegmentBytes {
		return nil, false
	}
	count := (size-1)/slotStreamBlockBytes + 1
	if len(trailer) != 4*count {
		return nil, false
	}
	checks := make([]uint32, count)
	var combined uint32
	for i := range checks {
		checks[i] = binary.BigEndian.Uint32(trailer[4*i:])
		combined = crc32Combine(combined, checks[i], streamBlockSize(size, i))
	}
	if combined != wholeCRC {
		return nil, false
	}
	return checks, true
}

// streamBlockSize returns zero for a size or index outside a stream frame.
func streamBlockSize(size, index int) int {
	if size <= 0 || size > maxSegmentBytes || index < 0 || index >= (size-1)/slotStreamBlockBytes+1 {
		return 0
	}
	return min(slotStreamBlockBytes, size-index*slotStreamBlockBytes)
}

// The caller has already validated the checksum table root.
func validStreamBlock(frame slotFrame, index int, data []byte) bool {
	size := streamBlockSize(frame.size, index)
	if frame.mode != slotStream || size == 0 || len(data) != size ||
		len(frame.blocks) != (frame.size-1)/slotStreamBlockBytes+1 {
		return false
	}
	return crc32.ChecksumIEEE(data) == frame.blocks[index]
}

// Combine two finalized IEEE CRCs by advancing the first by secondLen bytes.
func crc32Combine(first, second uint32, secondLen int) uint32 {
	if secondLen <= 0 {
		return first
	}
	for n := uint(secondLen); n != 0; n &= n - 1 {
		first = crc32MatrixTimes(&crc32BytePowers[bits.TrailingZeros(n)], first)
	}
	return first ^ second
}

// Cache the operators for advancing by powers of two bytes.
var crc32BytePowers = func() [bits.UintSize - 1][32]uint32 {
	var powers [bits.UintSize - 1][32]uint32
	var bit, two, four [32]uint32
	bit[0] = crc32.IEEE
	for i := 1; i < len(bit); i++ {
		bit[i] = 1 << (i - 1)
	}
	crc32MatrixSquare(&two, &bit)
	crc32MatrixSquare(&four, &two)
	crc32MatrixSquare(&powers[0], &four)
	for i := 1; i < len(powers); i++ {
		crc32MatrixSquare(&powers[i], &powers[i-1])
	}
	return powers
}()

func crc32MatrixTimes(matrix *[32]uint32, vector uint32) uint32 {
	var sum uint32
	for i := 0; vector != 0; i++ {
		if vector&1 != 0 {
			sum ^= matrix[i]
		}
		vector >>= 1
	}
	return sum
}

func crc32MatrixSquare(square, matrix *[32]uint32) {
	for i := range square {
		square[i] = crc32MatrixTimes(matrix, matrix[i])
	}
}

// slotTable is the ring of one direction: the ids of its slots and of the
// watermark of its reader. name and lazy are local, not serialized.
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

// IDs are single opaque URL path components, never paths or special segments.
func validSlotIDs(seen map[string]bool, ids ...string) bool {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
	for _, id := range ids {
		if id == "" || len(id) > maxSlotIDLen || strings.Trim(id, alphabet) != "" || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}

func decodeRing(b []byte) (ring, bool) {
	var r ring
	if json.Unmarshal(b, &r) != nil || len(r.First) > slotInlineLimit {
		return ring{}, false
	}
	seen := make(map[string]bool)
	for _, t := range []slotTable{r.Up, r.Down} {
		if len(t.Slots) == 0 || len(t.Slots) > maxConcurrency || !validSlotIDs(seen, t.Watermark) || !validSlotIDs(seen, t.Slots...) {
			return ring{}, false
		}
	}
	return r, true
}

// IDs are allocated in direction order: slots, watermark, slots, watermark.
func lazyRing(session string, depth int, ids []string) ring {
	table := func(name string, batch []string) slotTable {
		return slotTable{Slots: slices.Clone(batch[:depth]), Watermark: batch[depth], name: name, lazy: true}
	}
	return ring{Lazy: true, Up: table(uplinkPrefix(session), ids), Down: table(downlinkPrefix(session), ids[depth+1:])}
}

// Each object has one writer. Only its first publication in a lazy ring is a create.
func (t slotTable) publish(ctx context.Context, store slotStore, seq int64, mode byte, data []byte) error {
	idx := int(seq % int64(len(t.Slots)))
	var prefix, body []byte
	if mode == slotStream {
		if len(data) >= 2*slotStreamBlockBytes {
			prefix, data = data[:slotStreamPrefixBytes], data[slotStreamPrefixBytes:]
		}
		body = data
	}
	f := newSlotFrame(seq, mode, data)
	f.prefix = prefix
	if t.lazy && seq < int64(len(t.Slots)) {
		return store.createSlot(ctx, t.Slots[idx], t.name+"/s"+strconv.Itoa(idx), f.encode(), body)
	}
	return store.setSlot(ctx, t.Slots[idx], f.encode(), body)
}

func isCooling(s slotStore) bool {
	c, ok := s.(interface{ cooling() bool })
	return ok && c.cooling()
}

// Outbound: run assigns sequence numbers and owns the coalescing buffer.
// Uploads may finish out of order. Each physical position has a completion fence.
type slotWriter struct {
	ctx   context.Context
	fail  context.CancelCauseFunc
	store slotStore
	table slotTable
	params
	input               chan []byte
	writeToken, permits chan struct{}
	stop, done          chan struct{}
	first               chan []byte
	closeOnce           sync.Once
	workers             sync.WaitGroup
	mu                  sync.Mutex
	progress            time.Time
	wmMu                sync.Mutex
	acked               int64
	checked             time.Time
}

func newSlotWriter(ctx context.Context, fail context.CancelCauseFunc, store slotStore, table slotTable, p params) *slotWriter {
	w := &slotWriter{
		ctx: ctx, fail: fail, store: store, table: table, params: p,
		input: make(chan []byte), writeToken: make(chan struct{}, 1),
		permits: make(chan struct{}, min(len(table.Slots), 32)),
		stop:    make(chan struct{}), done: make(chan struct{}),
		first: make(chan []byte, 1), acked: -1, progress: time.Now(),
	}
	go w.run()
	return w
}

// Both directions observe the first fatal error, but END leaves the other half open.
func (w *slotWriter) Err() error { return context.Cause(w.ctx) }

func (w *slotWriter) stopped() error {
	if err := w.Err(); err != nil {
		return err
	}
	return io.ErrClosedPipe
}

func (w *slotWriter) Write(p []byte) (int, error) { return w.writeUntil(p, nil, nil) }

func (w *slotWriter) writeUntil(p []byte, deadline, closing <-chan struct{}) (int, error) {
	select {
	case w.writeToken <- struct{}{}:
		defer func() { <-w.writeToken }()
	case <-deadline:
		return 0, errWriteStopped
	case <-closing:
		return 0, errWriteStopped
	case <-w.ctx.Done():
		return 0, w.stopped()
	case <-w.stop:
		return 0, io.ErrClosedPipe
	}
	n := 0
	for len(p) > 0 {
		if isClosedChan(deadline) || isClosedChan(closing) {
			return n, errWriteStopped
		}
		if w.ctx.Err() != nil || isClosedChan(w.stop) {
			return n, w.stopped()
		}
		k := min(len(p), w.segmentBytes)
		select {
		case w.input <- bytes.Clone(p[:k]):
			n += k
			p = p[k:]
		case <-deadline:
			return n, errWriteStopped
		case <-closing:
			return n, errWriteStopped
		case <-w.ctx.Done():
			return n, w.stopped()
		case <-w.stop:
			return n, io.ErrClosedPipe
		}
	}
	return n, nil
}

func (w *slotWriter) markProgress() {
	w.mu.Lock()
	w.progress = time.Now()
	w.mu.Unlock()
}

func (w *slotWriter) closeProgress() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.progress
}

var errDriveWatermark = errors.New("invalid receiver watermark")

// Only one watermark GET per refresh interval, even when a wave wraps together.
func (w *slotWriter) credit(seq int64) (bool, error) {
	need := seq - int64(len(w.table.Slots))
	if need < 0 {
		return true, nil
	}
	w.wmMu.Lock()
	defer w.wmMu.Unlock()
	if w.acked >= seq {
		return false, errDriveWatermark
	}
	if w.acked >= need || time.Since(w.checked) < 100*time.Millisecond {
		return w.acked >= need, nil
	}
	ctx, cancel := context.WithTimeout(context.WithValue(w.ctx, headKey{}, true), 15*time.Second)
	defer cancel()
	d, err := w.store.readDescription(ctx, w.table.Watermark)
	w.checked = time.Now()
	if (err == errNotFound && w.table.lazy) || (err == nil && d == "") {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	n, err := strconv.ParseInt(d, 10, 64)
	if err != nil || n < 0 || n >= seq {
		return false, errDriveWatermark // this worker has not published seq yet
	}
	if n > w.acked {
		w.acked = n
		w.markProgress()
	}
	return w.acked >= need, nil
}

func (w *slotWriter) publish(seq int64, previous <-chan struct{}, mode byte, chunk []byte) error {
	if previous != nil {
		select {
		case <-previous:
		case <-w.ctx.Done():
			return w.stopped()
		}
	}
	for since := time.Now(); ; {
		ok, err := w.credit(seq)
		if isAuthError(err) || err == errNotFound || err == errDriveScope || err == errDriveWatermark {
			return err
		}
		if ok {
			break
		}
		if time.Since(since) > slotStallTimeout && time.Since(w.closeProgress()) > slotStallTimeout {
			return errors.New("timed out waiting for receiver watermark").Base(err)
		}
		if err := sleepContext(w.ctx, 30*time.Millisecond); err != nil {
			return err
		}
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if err := w.table.publish(w.ctx, w.store, seq, mode, chunk); err != nil {
		// A lost reply is not a refusal: never overwrite possibly committed data.
		if _, refused := err.(refusedPayload); refused && w.ctx.Err() == nil {
			ctx, cancel := context.WithTimeout(w.ctx, 2*time.Second)
			_ = w.table.publish(ctx, w.store, seq, slotErr, nil)
			cancel()
		}
		return err
	}
	w.markProgress()
	return nil
}

func (w *slotWriter) run() {
	defer close(w.done)
	defer w.workers.Wait()
	var seq int64
	fences := make([]chan struct{}, len(w.table.Slots))
	launch := func(chunk []byte) bool {
		select {
		case w.permits <- struct{}{}:
		case <-w.ctx.Done():
			return false
		}
		n := seq
		seq++
		i := n % int64(len(fences))
		prev, done := fences[i], make(chan struct{})
		fences[i] = done
		mode := byte(slotStream)
		if len(chunk) <= slotInlineLimit {
			mode = slotInline
			if n == 0 {
				w.first <- chunk
			}
		}
		w.workers.Go(func() {
			defer close(done)
			defer func() { <-w.permits }()
			if err := w.publish(n, prev, mode, chunk); err != nil {
				w.fail(err)
			}
		})
		return true
	}
	var buf []byte
	timer := time.NewTimer(w.flushInterval)
	timer.Stop()
	defer timer.Stop()
	flush := func() bool {
		timer.Stop()
		unit := min(w.segmentBytes, slotInlineLimit)
		wave := min(24, cap(w.permits)-len(w.permits))
		inline := min(len(buf), wave*unit)
		if isCooling(w.store) || len(buf) > wave*unit && (wave < 24 || len(buf) > w.segmentBytes) {
			inline = 0
		}
		for len(buf) > 0 {
			n := min(len(buf), w.segmentBytes)
			if inline > 0 {
				n = min(inline, unit)
				inline -= n
			}
			if !launch(buf[:n:n]) {
				return false
			}
			buf = buf[n:]
		}
		buf = nil // the upload workers own their slices
		return true
	}
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.stop:
			if !flush() {
				return
			}
			w.workers.Wait()
			if w.ctx.Err() == nil {
				if err := w.publish(seq, fences[seq%int64(len(fences))], slotEnd, nil); err != nil {
					w.fail(err)
				}
			}
			return
		case b := <-w.input:
			if len(buf) == 0 {
				buf = b
				d := w.flushInterval
				if len(w.permits) == 0 && len(b) <= slotInlineLimit && !isCooling(w.store) {
					d = min(d, 2*time.Millisecond)
				}
				timer.Reset(d)
			} else {
				buf = append(buf, b...)
			}
			if len(buf) >= w.segmentBytes && !flush() {
				return
			}
		case <-timer.C:
			if !flush() {
				return
			}
		}
	}
}

func (w *slotWriter) Close() error {
	w.closeOnce.Do(func() { close(w.stop) })
	w.wait() // Conn.Close owns the watchdog for the entire shutdown.
	return w.Err()
}

func (w *slotWriter) wait() { <-w.done }

func waitCloseProgress(done <-chan struct{}, progress func() time.Time) error {
	for {
		remaining := slotStallTimeout - time.Since(progress())
		if remaining <= 0 {
			select {
			case <-done:
				return nil
			default:
				return context.DeadlineExceeded
			}
		}
		select {
		case <-done:
			return nil
		case <-time.After(remaining):
		}
	}
}

// Inbound: workers discover known IDs concurrently. Only run delivers bytes and moves
// head. Lookahead media retains at most two whole frames, plus the ordered head.
type slotReader struct {
	ctx     context.Context
	setErr  context.CancelCauseFunc
	discard <-chan struct{}
	store   slotStore
	table   slotTable
	params
	ch                       chan []byte
	wake, ack, media         chan struct{}
	head, frontier, activeAt atomic.Int64
	drainCredit              atomic.Pointer[time.Time] // last credit returned during Close
	probing                  atomic.Bool
	wg                       sync.WaitGroup
}

type readSlot struct {
	seq                  int64
	head, retired, ready chan struct{}
	data                 chan []byte // published by ready; closed after the last verified block
	end                  bool
	err                  error // published by closing ready (no data) or data
}

func newSlotReader(ctx context.Context, fail context.CancelCauseFunc, store slotStore, table slotTable, p params, first []byte, discard <-chan struct{}) *slotReader {
	r := &slotReader{
		ctx: ctx, setErr: fail, discard: discard, store: store, table: table, params: p,
		ch: make(chan []byte, 2), wake: make(chan struct{}, 1),
		ack: make(chan struct{}, 1), media: make(chan struct{}, 2),
	}
	r.frontier.Store(-1)
	r.Wake()
	if len(first) != 0 {
		r.ch <- first
		r.head.Store(1)
		r.ack <- struct{}{}
	}
	readDone := make(chan struct{})
	r.wg.Go(func() {
		defer close(readDone)
		r.run()
	})
	r.wg.Go(func() { r.ackLoop(readDone) })
	return r
}

func (r *slotReader) chunks() <-chan []byte { return r.ch }
func (r *slotReader) wait()                 { r.wg.Wait() }

func (r *slotReader) Wake() {
	r.activeAt.Store(time.Now().UnixNano())
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *slotReader) Err() error {
	if err := context.Cause(r.ctx); err != nil {
		return err
	}
	return io.EOF
}

// A GET may have sampled an old lap. Allow one later sample of the active
// head, but never require the second GET when the first one already matches.
func (r *slotReader) sample(ctx context.Context, id string, seq int64, overlap bool) (slotFrame, bool, error) {
	type observation struct {
		f       slotFrame
		invalid bool
		err     error
	}
	read := func(ctx context.Context) observation {
		d, err := r.store.readDescription(ctx, id)
		f, ok := decodeSlot(d)
		return observation{f, d != "" && !ok, err}
	}
	if !overlap {
		v := read(ctx)
		return v.f, v.invalid, v.err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan observation, 2)
	launch := func() {
		r.wg.Go(func() { results <- read(ctx) })
	}
	launch()
	t := time.NewTimer(125 * time.Millisecond)
	defer t.Stop()
	pending := 1
	var realErr error
	var invalid bool
	for {
		select {
		case v := <-results:
			invalid = invalid || v.invalid
			if v.err == errDriveScope || isAuthError(v.err) || (v.f.mode != 0 && v.f.seq >= seq && v.err == nil) {
				return v.f, v.invalid, v.err
			}
			if v.err != nil && v.err != errNotFound {
				realErr = v.err
			}
			pending--
			if pending == 0 {
				if realErr != nil {
					return slotFrame{}, false, realErr
				}
				return v.f, invalid, v.err
			}
		case <-t.C:
			pending++
			launch()
		case <-ctx.Done():
			return slotFrame{}, false, ctx.Err()
		}
	}
}

func (r *slotReader) observe(n int64) {
	for old := r.frontier.Load(); old < n && !r.frontier.CompareAndSwap(old, n); old = r.frontier.Load() {
	}
}

func (r *slotReader) lookup(ctx context.Context, job *readSlot) (slotFrame, error) {
	depth := int64(len(r.table.Slots))
	id := r.table.Slots[job.seq%depth]
	probeAt, offset := time.Now().Add(slotStallTimeout), int64(1)
	var unhealthy time.Time
	for {
		head := job.seq == r.head.Load()
		active := time.Since(time.Unix(0, r.activeAt.Load())) < r.eagerWindow
		if head || active {
			readCtx := ctx
			if head {
				readCtx = context.WithValue(ctx, headKey{}, true)
			}
			overlap := head && active && !isCooling(r.store)
			f, invalid, err := r.sample(readCtx, id, job.seq, overlap)
			if err == nil && f.seq > job.seq {
				return slotFrame{}, errors.New("Drive slot ", job.seq, " was overwritten by sequence ", f.seq)
			}
			if err == nil && f.mode != 0 && f.seq == job.seq {
				r.activeAt.Store(time.Now().UnixNano())
				r.observe(f.seq)
				return f, nil
			}
			if err == errDriveScope || isAuthError(err) {
				return slotFrame{}, err
			}
			// A lazy missing head is also a valid idle state. Occasionally inspect a
			// different description for evidence of a gap, never speculative media.
			if head && depth > 1 && time.Now().After(probeAt) && r.frontier.Load() <= job.seq && !isCooling(r.store) && r.probing.CompareAndSwap(false, true) {
				idx := (job.seq + offset) % depth
				offset, probeAt = offset%(depth-1)+1, time.Now().Add(slotStallTimeout/12)
				r.wg.Go(func() {
					defer r.probing.Store(false)
					probe, cancel := context.WithTimeout(ctx, slotStallTimeout)
					defer cancel()
					d, err := r.store.readDescription(probe, r.table.Slots[idx])
					if f, ok := decodeSlot(d); err == nil && ok && f.seq > job.seq && f.seq%depth == idx {
						r.observe(f.seq)
						r.Wake()
					}
				})
			}
			missing := err == errNotFound && (!r.table.lazy || job.seq >= int64(len(r.table.Slots)))
			if head && (missing || (err != nil && err != errNotFound) || invalid || r.frontier.Load() > job.seq) {
				if unhealthy.IsZero() {
					unhealthy = time.Now()
				} else if time.Since(unhealthy) >= slotStallTimeout {
					return slotFrame{}, errors.New("Drive slot ", job.seq, " did not become readable").Base(err)
				}
			} else {
				unhealthy = time.Time{}
			}
		}
		wake, delay := job.head, 40*time.Millisecond
		if head {
			wake, delay = r.wake, 25*time.Millisecond
		}
		if !active {
			delay = r.maxPollInterval
		}
		select {
		case <-ctx.Done():
			return slotFrame{}, ctx.Err()
		case <-wake:
		case <-time.After(delay):
		}
	}
}

func (r *slotReader) fetch(ctx context.Context, j *readSlot) {
	f, err := r.lookup(ctx, j)
	if err != nil || f.mode == slotErr || f.mode == slotEnd {
		j.err, j.end = err, f.mode == slotEnd
		if f.mode == slotErr {
			j.err = errors.New("peer could not store segment ", j.seq)
		}
		close(j.ready)
		return
	}
	if f.mode == slotInline {
		j.data = make(chan []byte, 1)
		j.data <- f.data
		close(j.data)
		close(j.ready)
		return
	}
	// A promoted head must never queue behind a prefetched successor. A tail
	// holds its credit until retirement, even after a short or failed body.
	reserved := false
	if j.seq != r.head.Load() {
		select {
		case r.media <- struct{}{}:
			reserved = true
		case <-j.head:
		case <-ctx.Done():
			j.err = ctx.Err()
			close(j.ready)
			return
		}
	}
	j.data = make(chan []byte, len(f.blocks)+1)
	if len(f.prefix) != 0 {
		j.data <- f.prefix
	}
	close(j.ready)
	j.err = r.store.readFrameStream(ctx, r.table.Slots[j.seq%int64(len(r.table.Slots))], f, func(b []byte) error {
		select {
		case j.data <- b:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	close(j.data)
	if reserved {
		select {
		case <-j.retired:
		case <-ctx.Done():
		}
		<-r.media
	}
}

func (r *slotReader) run() {
	defer close(r.ch)
	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()
	jobs := make(map[int64]*readSlot)
	next := r.head.Load()
	for ctx.Err() == nil {
		head := r.head.Load()
		window := min(12, len(r.table.Slots))
		if head == 0 {
			window = 1
		}
		for next < head+int64(window) {
			j := &readSlot{seq: next, head: make(chan struct{}), retired: make(chan struct{}), ready: make(chan struct{})}
			jobs[next] = j
			r.wg.Go(func() { r.fetch(ctx, j) })
			next++
		}
		j := jobs[head]
		close(j.head)
		select {
		case <-j.ready:
		case <-ctx.Done():
			return
		}
		if j.data != nil {
			for b := range j.data {
				select {
				case r.ch <- b:
				case <-r.discard: // Close abandons input but must keep returning verified ring credit.
				case <-ctx.Done():
					return
				}
			}
		}
		close(j.retired)
		delete(jobs, head)
		if j.err != nil {
			if ctx.Err() == nil {
				r.setErr(j.err)
			}
			return
		}
		if j.end {
			return
		}
		r.head.Store(head + 1)
		select {
		case r.ack <- struct{}{}:
		default:
		}
	}
}

// This goroutine is the only watermark writer. END triggers one last snapshot
// after the ordered reader has stopped, including credit below the batch size.
func (r *slotReader) ackLoop(readDone <-chan struct{}) {
	ctx := context.WithValue(r.ctx, headKey{}, true)
	t := time.NewTicker(1500 * time.Millisecond)
	defer t.Stop()
	acked := int64(-1)
	batch := max(int64(1), int64(len(r.table.Slots))/6)
	for {
		final := false
		select {
		case <-ctx.Done():
			return
		case <-readDone:
			final = true
		case <-r.ack:
			if r.head.Load()-1-acked < batch {
				continue
			}
		case <-t.C:
		}
		if ctx.Err() != nil {
			return
		}
		if n := r.head.Load() - 1; n > acked {
			d := strconv.FormatInt(n, 10)
			var err error
			if r.table.lazy && acked < 0 {
				err = r.store.createSlot(ctx, r.table.Watermark, r.table.name+"/w", d, nil)
			} else {
				err = r.store.setSlot(ctx, r.table.Watermark, d, nil)
			}
			if err != nil {
				r.setErr(errors.New("cannot publish Drive watermark").Base(err))
				return
			}
			acked = n
			if isClosedChan(r.discard) {
				now := time.Now()
				r.drainCredit.Store(&now)
			}
		}
		if final {
			return
		}
	}
}
