package xdrive

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/signal"
)

const (
	slotMagic        = "XDRVSL04"
	slotHeaderLen    = len(slotMagic) + 13
	slotInlineLimit  = 14 << 10
	slotInline       = 1
	slotEnd          = 3
	slotErr          = 4
	slotStream       = 5
	slotStallTimeout = time.Minute

	slotBodyHeader     = 8 // Per-block sequence tag rejects older laps.
	slotTagBlock       = 256 << 10
	slotReadChunk      = 32 << 10 // Payload delivery granularity.
	slotMaxFrame       = 4 << 20  // Below Drive's 5 MB multipart upload limit.
	slotProgressiveMax = 1 << 20  // One progressive GET amortizes latency for small frames.
	slotReadAhead      = 24
	slotPollAhead      = 4 // Empty positions polled beyond the observed frontier.
	slotBusyUploads    = 4
	slotBusyGather     = 200 * time.Millisecond // A full frame or input pause ends gathering sooner.
	slotMediaAck       = 250 * time.Millisecond
	slotIdleAck        = 1500 * time.Millisecond
)

func slotBlocks(size int) int { return (size + slotTagBlock - 1) / slotTagBlock }

var errWriteStopped = errors.New("write stopped")

type slotFrame struct {
	seq      int64
	mode     byte
	size     int
	data     []byte
	previous int  // Previous payload length; media above slotProgressiveMax has no description prefix.
	hinted   bool // Local only: buffer this head until its own media verifies.
}

func (f slotFrame) wireBytes() []byte {
	b := append(make([]byte, 0, slotHeaderLen+len(f.data)), slotMagic...)
	b = binary.BigEndian.AppendUint64(b, uint64(f.seq))
	b = binary.BigEndian.AppendUint32(append(b, f.mode), uint32(f.size))
	return append(b, f.data...)
}

func decodeSlot(description string) (slotFrame, bool) {
	b, err := base64.StdEncoding.DecodeString(description)
	if err != nil || len(b) < slotHeaderLen || !bytes.HasPrefix(b, []byte(slotMagic)) {
		return slotFrame{}, false
	}
	f := slotFrame{seq: int64(binary.BigEndian.Uint64(b[8:])), mode: b[16], size: int(binary.BigEndian.Uint32(b[17:])), data: b[slotHeaderLen:]}
	n := len(f.data)
	inline := f.mode == slotInline && n > 0 && n == f.size
	stream := f.mode == slotStream && n < f.size
	terminal := (f.mode == slotEnd || f.mode == slotErr) && n == 0 && f.size == 0
	return f, f.seq >= 0 && uint(f.size) <= maxSegmentBytes && n <= slotInlineLimit && (inline || stream || terminal)
}

// A direction publishes its slot IDs and reader-owned watermark; name stays local.
type slotTable struct {
	Slots     []string `json:"s"`
	Watermark string   `json:"w"`
	name      string
}

// Up and Down are relative to the client. Lazy slots are created on first write.
type ring struct {
	Up    slotTable `json:"u"`
	Down  slotTable `json:"d"`
	First []byte    `json:"first,omitempty"`
	Lazy  bool      `json:"lazy,omitempty"`
}

var slotIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// The session owns unchecked exclusively until admission succeeds.
func decodeRing(b []byte, session string, unchecked *sync.Map) *ring {
	var r ring
	if json.Unmarshal(b, &r) != nil || !r.Lazy || len(r.First) > slotInlineLimit {
		return nil
	}
	bind := func(id, name string) bool {
		_, duplicate := unchecked.LoadOrStore(id, flatten(name))
		return !duplicate && slotIDPattern.MatchString(id)
	}
	r.Up.name, r.Down.name = uplinkPrefix(session), downlinkPrefix(session)
	for _, t := range []*slotTable{&r.Up, &r.Down} {
		if len(t.Slots) == 0 || len(t.Slots) > maxConcurrency || !bind(t.Watermark, t.name+"/w") {
			return nil
		}
		for i, id := range t.Slots {
			if !bind(id, t.name+"/s"+strconv.Itoa(i)) {
				return nil
			}
		}
	}
	return &r
}

func lazyRing(session string, depth int, ids []string) ring {
	table := func(name string, batch []string) slotTable {
		return slotTable{Slots: batch[:depth:depth], Watermark: batch[depth], name: name}
	}
	return ring{Lazy: true, Up: table(uplinkPrefix(session), ids), Down: table(downlinkPrefix(session), ids[depth+1:])}
}

func (w *slotWriter) writeFrame(ctx context.Context, f slotFrame) error {
	depth := int64(len(w.table.Slots))
	id := w.table.Slots[f.seq%depth]
	var body []byte
	switch {
	case f.size > slotProgressiveMax:
		body, f.data = f.data, nil
	case f.size > slotInlineLimit:
		body, f.data = f.data[slotInlineLimit:], f.data[:slotInlineLimit]
	}
	name := ""
	if f.seq < depth {
		name = w.table.name + "/s" + strconv.FormatInt(f.seq, 10)
	}
	var checkACK func(context.Context) (bool, error)
	if f.seq > 0 && f.size > 0 { // The announcement can deliver zero without its slot.
		advance(&w.started, f.seq)
		checkACK = func(ctx context.Context) (bool, error) {
			if _, pending := w.store.unchecked.Load(id); pending {
				return false, nil // Exact readback must bind a peer-selected slot to this session first.
			}
			ack := slotCredit{Seq: w.acked.Load()} // Reuse the regular credit poll's receipt.
			var err error
			if ack.Seq < f.seq {
				ack, err = w.readCredit(ctx, w.started.Load())
			}
			return err == nil && ack.Seq >= f.seq && ack.Seq-f.seq < depth, err // The fence excludes the next lap.
		}
	}
	wire := f.wireBytes()
	file := driveFile{Description: base64.StdEncoding.EncodeToString(wire)}
	if f.previous > slotProgressiveMax {
		prior := slotFrame{seq: f.seq - 1, mode: slotStream, size: f.previous}
		file.Properties = map[string]string{"xdrivePrev": base64.StdEncoding.EncodeToString(prior.wireBytes())}
	}
	return w.store.writeFile(ctx, id, name, file, body, wire[8:16], checkACK) // Media blocks reuse the description's sequence tag.
}

type slotDirection struct {
	*Conn
	store *driveStorage
	table slotTable
	params
}

type slotWriter struct {
	slotDirection
	flight                        *semaphore.Weighted
	limit                         int64
	input, first                  chan []byte
	writeToken, done              chan struct{}
	stored                        []chan struct{} // Per-slot completion, independent of cumulative receiver credit.
	active                        atomic.Int32
	through                       []int64 // Cumulative media bytes through the last assignment at each slot.
	previous, frame               int
	next                          int64
	acked, started                atomic.Int64 // Last credited sequence; highest ordinary sequence past its slot fence.
	nextCredit, lastInput         time.Time
	mediaSent, mediaAcked, window int64
}

type slotCredit struct {
	Seq      int64 `json:"s"`
	Rate     int64 `json:"r"` // Ordered payload bytes per second over the last media fetch.
	mediaSeq int64 // Last media sequence delivered locally; controls early ACKs.
}

func (w *slotWriter) readCredit(ctx context.Context, last int64) (slotCredit, error) {
	f, err := w.store.readFile(ctx, w.table.Watermark, "description")
	a := slotCredit{Seq: -1, Rate: -1}
	if err == nil && f.Description != "" && (json.Unmarshal([]byte(f.Description), &a) != nil || a.Seq < 0 || a.Seq > last || a.Rate < 0 || a.Rate > slotMaxFrame*maxConcurrency) {
		err = errCredit
	}
	return a, err
}

func newSlotWriter(dir slotDirection) *slotWriter {
	limit := int64(len(dir.store.pools[mediaLane].lanes)) * slotMaxFrame
	w := &slotWriter{
		slotDirection: dir, flight: semaphore.NewWeighted(limit), limit: limit,
		through: make([]int64, len(dir.table.Slots)),
		input:   make(chan []byte), first: make(chan []byte, 1),
		writeToken: make(chan struct{}, 1), done: make(chan struct{}),
		stored: make([]chan struct{}, len(dir.table.Slots)),
	}
	w.acked.Store(-1)
	w.setRate(0)
	w.markProgress()
	go w.run()
	return w
}

func sendUntil[T any](ctx context.Context, dst chan<- T, value T, deadline, closing <-chan struct{}) error {
	if ctx.Err() == nil && !isClosedChan(deadline) && !isClosedChan(closing) {
		select {
		case dst <- value:
			return nil
		case <-ctx.Done():
		case <-deadline:
		case <-closing:
		}
	}
	return cmp.Or[error](context.Cause(ctx), errWriteStopped)
}

func (w *slotWriter) writeUntil(p []byte, deadline, _ <-chan struct{}) (int, error) {
	if err := sendUntil(w.ctx, w.writeToken, struct{}{}, deadline, w.closing); err != nil {
		return 0, err
	}
	defer func() { <-w.writeToken }()
	n := 0
	for chunk := range slices.Chunk(p, min(w.segmentBytes, slotMaxFrame)) {
		if err := sendUntil(w.ctx, w.input, bytes.Clone(chunk), deadline, w.closing); err != nil {
			return n, err
		}
		n += len(chunk)
	}
	return n, nil
}

func (c *Conn) markProgress() { c.progress.Store(new(time.Now())) }

func (w *slotWriter) setRate(rate int64) {
	seed := int64(min(w.segmentBytes, slotProgressiveMax))
	frame := min(slotMaxFrame, max(slotTagBlock, cmp.Or(rate, seed)))
	unit := frame // Start with at least 512 KiB per allowance until the reader measures delivery.
	if rate == 0 {
		unit = max(unit, defaultSegmentBytes)
	}
	lanes := int64(len(w.store.pools[mediaLane].lanes))
	w.frame, w.window = int(frame), min(w.limit+frame, max(4, 2*lanes)*unit+rate/4)
}

func (w *slotWriter) waitCredit(size int64) bool {
	start := time.Now()
	depth, media := int64(len(w.through)), size > slotInlineLimit
	for w.ctx.Err() == nil {
		queued := w.mediaSent - w.mediaAcked
		acked := w.acked.Load()
		full := w.next-acked > depth || media && queued+size > max(w.window, size)
		if !full && (!media || queued < w.window/2 || time.Since(w.nextCredit) < slotIdleAck) {
			return true
		}
		if sleepContext(w.ctx, time.Until(w.nextCredit)) != nil {
			break
		}
		ctx, cancel := context.WithTimeout(context.WithValue(w.ctx, headKey{}, true), 15*time.Second)
		ack, err := w.readCredit(ctx, w.next-1)
		cancel()
		w.nextCredit = time.Now().Add(100 * time.Millisecond)
		switch {
		case err == nil && ack.Seq > acked:
			if !w.store.cooling() {
				w.setRate(ack.Rate)
			}
			w.mediaAcked = w.through[ack.Seq%depth]
			w.acked.Store(ack.Seq)
			w.markProgress()
			errors.LogDebug(w.ctx, "xdrive credit seq ", w.next, " ack ", ack.Seq, " frame ", w.frame)
		case isAuthError(err), err == errCredit, err == errDriveScope:
			w.fail(err)
		case time.Since(start) > slotStallTimeout && time.Since(*w.progress.Load()) > slotStallTimeout:
			w.fail(errors.New("timed out waiting for receiver watermark").Base(err))
		}
	}
	return false
}

func (w *slotWriter) publish(f slotFrame, credit time.Duration) {
	started := time.Now()
	if err := w.writeFrame(w.ctx, f); err != nil {
		if _, refused := err.(refusedPayload); refused && w.ctx.Err() == nil { // A lost reply cannot authorize an overwrite.
			ctx, cancel := context.WithTimeout(w.ctx, 2*time.Second)
			_ = w.writeFrame(ctx, slotFrame{seq: f.seq, mode: slotErr})
			cancel()
		}
		w.fail(err)
		return
	}
	if f.mode == slotStream {
		errors.LogDebug(w.ctx, "xdrive upload seq ", f.seq, " bytes ", f.size, " credit ", credit, " put ", time.Since(started))
	}
	w.markProgress()
}

func (w *slotWriter) busy() int { return int(w.active.Load()) }

func (w *slotWriter) submit(data []byte) {
	size := int64(len(data))
	start := time.Now()
	// A minimum charge bounds the worker count as well as the total unfinished bytes.
	charge := max(size, w.limit/int64(min(len(w.stored), 32)))
	if !w.waitCredit(size) || w.flight.Acquire(w.ctx, charge) != nil {
		return
	}
	credit := time.Since(start)
	f := slotFrame{seq: w.next, mode: slotInline, data: data, size: len(data), previous: w.previous}
	switch {
	case size > slotInlineLimit:
		f.mode = slotStream
		w.mediaSent += size
	case w.next == 0:
		w.first <- data
	}
	w.previous, w.next = f.size, f.seq+1
	pos := f.seq % int64(len(w.stored))
	prior, stored := w.stored[pos], make(chan struct{})
	w.stored[pos], w.through[pos] = stored, w.mediaSent
	w.active.Add(1)
	go func() {
		defer close(stored)
		defer w.flight.Release(charge)
		defer w.active.Add(-1)
		if prior != nil {
			<-prior // Only this position waits for its previous mutation; other slots can publish.
		}
		if w.ctx.Err() == nil {
			w.publish(f, credit)
		}
	}()
}

// Start with any overflow from the preceding frame, preserving its arrival time.
func (w *slotWriter) gather(buf []byte) ([]byte, []byte) {
	var flush <-chan time.Time
	var oldest time.Time
	for len(buf) < w.frame {
		switch {
		case len(buf) == 0:
			buf = nil // Do not retain a fully consumed handoff while idle.
		case flush == nil:
			delay := w.flushInterval
			if len(buf) <= slotInlineLimit && w.busy() == 0 && !w.store.cooling() {
				delay = min(delay, 2*time.Millisecond)
			}
			oldest, flush = w.lastInput, time.After(delay)
		}
		select {
		case p := <-w.input:
			w.lastInput = time.Now()
			if len(buf) != 0 {
				p = append(buf, p...)
			}
			buf = p
			continue
		case <-w.ctx.Done():
		case <-w.closing:
		case <-flush:
			wait := min(w.flushInterval-time.Since(w.lastInput), slotBusyGather-time.Since(oldest))
			if w.busy() >= slotBusyUploads && wait > 0 {
				flush = time.After(wait)
				continue
			}
		}
		return buf, nil
	}
	return buf[:w.frame:w.frame], buf[w.frame:]
}

func (w *slotWriter) run() {
	defer close(w.done)
	var lastBurst time.Time
	for buf, tail := w.gather(nil); len(buf) > 0 && w.ctx.Err() == nil; buf, tail = w.gather(tail) {
		busy := w.busy()
		free := len(w.through) - int(w.next-w.acked.Load()-1)
		wave := min(slotReadAhead, min(32, len(w.stored))-busy, free)
		if busy >= slotBusyUploads || len(buf) == w.frame || time.Since(lastBurst) < w.eagerWindow {
			wave = min(wave, slotPollAhead)
		}
		if len(buf) > slotPollAhead*slotInlineLimit {
			lastBurst = time.Now()
		}
		size := w.frame
		if busy < 2*slotBusyUploads && len(buf) <= wave*slotInlineLimit && !w.store.cooling() {
			size = slotInlineLimit
		}
		for chunk := range slices.Chunk(buf, size) {
			w.submit(chunk)
		}
	}
	for _, done := range w.stored {
		if done != nil {
			<-done
		}
	}
	if w.waitCredit(0) {
		w.publish(slotFrame{seq: w.next, mode: slotEnd, previous: w.previous}, 0)
	}
}

func (w *slotWriter) Close() error {
	<-w.done // Conn.Close owns the watchdog for the entire shutdown.
	return context.Cause(w.ctx)
}

// Workers discover descriptions independently; only run delivers bytes and advances the head.
type slotReader struct {
	slotDirection
	budget                                                 *semaphore.Weighted
	hints                                                  []atomic.Pointer[slotFrame]
	ch                                                     chan []byte
	wake, ack                                              *signal.Notifier
	credit                                                 atomic.Pointer[slotCredit]
	delivered, frontier, verified, activeAt, inlineThrough atomic.Int64
	wg                                                     sync.WaitGroup
}

type readSlot struct {
	ahead   context.Context // Promotion releases speculative bytes and selects head HTTP capacity.
	promote context.CancelFunc
	stream  chan (<-chan []byte) // A direct stream opens immediately; a hinted stream opens only after EOF.
	end     bool
	started time.Time
	before  int64 // Ordered bytes at media admission, including overlapping fetches.
	err     error
}

func newSlotReader(dir slotDirection, first []byte, writerDone <-chan struct{}) *slotReader {
	r := &slotReader{
		budget:        semaphore.NewWeighted(int64(len(dir.store.pools[mediaLane].lanes)) * slotMaxFrame),
		slotDirection: dir, hints: make([]atomic.Pointer[slotFrame], len(dir.table.Slots)),
		ch: make(chan []byte, 2), wake: signal.NewNotifier(), ack: signal.NewNotifier(),
	}
	credit := &slotCredit{Seq: -1, mediaSeq: -1}
	r.activeAt.Store(time.Now().UnixNano())
	if len(first) != 0 {
		r.ch <- first
		credit.Seq = 0
		r.ack.Signal()
	}
	r.frontier.Store(credit.Seq) // Establish frame zero before eagerly probing successors.
	r.credit.Store(credit)
	r.wg.Go(func() { r.run(writerDone) })
	return r
}

func (r *slotReader) chunks() <-chan []byte { return r.ch }
func (r *slotReader) head() int64           { return r.credit.Load().Seq + 1 }
func (r *slotReader) wait()                 { r.wg.Wait() }
func (r *slotReader) Err() error            { return cmp.Or(context.Cause(r.ctx), io.EOF) }

func (r *slotReader) Wake() {
	r.activeAt.Store(time.Now().UnixNano())
	r.wake.Signal()
}

func advance(counter *atomic.Int64, n int64) {
	for old := counter.Load(); old < n && !counter.CompareAndSwap(old, n); old = counter.Load() {
	}
}

func (r *slotReader) readFrame(ctx context.Context, j *readSlot, f slotFrame, start time.Time) error {
	switch {
	case f.mode == slotEnd:
		j.end = true
		return nil
	case f.mode == slotErr:
		return errors.New("peer could not store segment ", f.seq)
	case f.size > slotMaxFrame:
		return errDriveSize
	}
	found := time.Now()
	body := make(chan []byte, f.size/slotReadChunk+3) // First 4 KiB, final partial chunk and description prefix.
	defer close(body)
	if !f.hinted {
		j.stream <- body
	}
	if len(f.data) != 0 {
		body <- f.data
	}
	if f.mode != slotStream {
		return nil
	}
	charge := int64(f.size)
	if r.budget.Acquire(j.ahead, charge) == nil {
		context.AfterFunc(j.ahead, func() { r.budget.Release(charge) }) // Promotion gives the head a separate allowance.
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	j.started, j.before = time.Now(), r.delivered.Load()
	f.size -= len(f.data) // Media offsets exclude the description prefix.
	ctx = context.WithValue(ctx, mediaHeadKey{}, j.ahead.Done())
	err := r.store.readFrameStream(ctx, r.table.Slots[f.seq%int64(len(r.table.Slots))], f, body, &r.verified)
	errors.LogDebug(ctx, "xdrive read seq ", f.seq, " bytes ", f.size+len(f.data), " find ", found.Sub(start), " budget ", j.started.Sub(found), " body ", time.Since(j.started), " err ", err)
	if err == nil {
		advance(&r.verified, f.seq)
		if f.hinted {
			j.stream <- body
		}
	}
	return err
}

func (r *slotReader) fetch(ctx context.Context, seq int64, j *readSlot) error {
	depth, start := int64(len(r.hints)), time.Now()
	gap := func() bool { return seq == r.head() && r.frontier.Load() > seq && !r.store.cooling() }
	search := context.WithValue(ctx, spanRescueKey{}, gap)
	priority := context.WithValue(search, headKey{}, true)
	probeAt, offset := time.Now().Add(slotStallTimeout), int64(0)
	var gapSince time.Time
	triedHint := false
	delay, wake := r.maxPollInterval, j.ahead.Done()
	for first := true; ctx.Err() == nil; waitFor(ctx, time.After(delay), wake) {
		head, frontier := r.head(), r.frontier.Load()
		active := time.Since(time.Unix(0, r.activeAt.Load())) < r.eagerWindow
		readCtx, target := search, seq // A progressing lookup survives head promotion.
		wake, delay = j.ahead.Done(), 40*time.Millisecond
		if !active {
			delay = r.maxPollInterval
		}
		if seq == head {
			readCtx, wake = priority, r.wake.Wait()
			if active {
				delay = 25 * time.Millisecond
			}
		} else if seq == head+1 && frontier < head && time.Now().After(probeAt) && !r.store.cooling() {
			// The first successor rotates through the entire idle ring without delaying the head.
			target = seq + offset%(depth-1)
			offset++
			probeAt = time.Now().Add(slotStallTimeout / 12)
		} else if !active || frontier < 0 || seq > max(frontier, head)+slotPollAhead && (!first || seq > r.inlineThrough.Load()) {
			continue
		}
		first = false // Only one speculative GET outside the ordinary polling window.
		id := r.table.Slots[target%depth]
		file, err := r.store.readFile(readCtx, id, "description,properties")
		f, ok := decodeSlot(file.Description)
		if err == nil && ok && f.seq >= target && f.seq%depth == target%depth {
			if p, valid := decodeSlot(file.Properties["xdrivePrev"]); valid && p.seq == f.seq-1 && p.size > slotProgressiveMax && p.size <= slotMaxFrame && len(p.data) == 0 {
				p.hinted = true
				r.hints[p.seq%depth].Store(&p)
			}
			if f.mode == slotInline { // The entire frame is already verified, even before its media window opens.
				advance(&r.verified, f.seq)
				if f.size == slotInlineLimit {
					advance(&r.inlineThrough, f.seq+slotReadAhead-1)
				}
			}
			advance(&r.frontier, f.seq)
			r.activeAt.Store(time.Now().UnixNano())
		}
		switch {
		case target != seq: // Probe results are evidence of a gap, never authority to read a body.
			continue
		case err == errDriveScope || isAuthError(err):
			return err
		case err == nil && ok && f.seq > seq:
			return errors.New("Drive slot ", seq, " was overwritten by sequence ", f.seq)
		case err == nil && ok && f.seq == seq:
			return r.readFrame(ctx, j, f, start)
		}
		if _, pending := r.store.unchecked.Load(id); !triedHint && !pending && gap() {
			if p := r.hints[seq%depth].Swap(nil); p != nil && p.seq == seq {
				triedHint = true
				if err := r.readFrame(ctx, j, *p, start); err == nil || err == errDriveScope || isAuthError(err) {
					return err
				}
				j.started = time.Time{} // Discard the stale read and return to authoritative metadata.
			}
		}
		idle := err == errNotFound && seq < depth // First-lap files are created on first publication.
		if head != seq || (err == nil || idle) && (file.Description == "" || ok) && r.frontier.Load() <= seq {
			gapSince = time.Time{}
		} else if gapSince.IsZero() {
			gapSince = time.Now()
		} else if time.Since(gapSince) >= slotStallTimeout {
			return errors.New("Drive slot ", seq, " did not become readable").Base(err)
		}
	}
	return ctx.Err()
}

func (r *slotReader) run(writerDone <-chan struct{}) {
	ctx, finish := context.WithCancel(r.ctx)
	defer finish()
	defer close(r.ch)
	r.wg.Go(func() { r.ackLoop(ctx, writerDone) })
	width := min(slotReadAhead, len(r.table.Slots))
	var jobs []*readSlot
	for ctx.Err() == nil {
		if len(jobs) < width {
			seq := r.head() + int64(len(jobs))
			j := &readSlot{stream: make(chan (<-chan []byte), 1)}
			j.ahead, j.promote = context.WithCancel(ctx)
			jobs = append(jobs, j)
			r.wg.Go(func() {
				j.err = r.fetch(ctx, seq, j)
				close(j.stream)
			})
			continue
		}
		j := jobs[0]
		jobs = jobs[1:]
		j.promote()
		for body := range j.stream {
			for b := range body {
				// Close discards input but continues verification and credit; cancellation stops both.
				if err := sendUntil(ctx, r.ch, b, nil, r.closing); err != nil && ctx.Err() != nil {
					return
				}
				r.activeAt.Store(time.Now().UnixNano())
				r.delivered.Add(int64(len(b)))
			}
		}
		if j.err != nil {
			r.fail(j.err) // A prior cancellation keeps its original cause.
		}
		if j.end || ctx.Err() != nil {
			return
		}
		credit := *r.credit.Load()
		credit.Seq++
		if !j.started.IsZero() {
			rate := float64(r.delivered.Load()-j.before) / time.Since(j.started).Seconds()
			credit.Rate, credit.mediaSeq = int64(min(slotMaxFrame*maxConcurrency, rate)), credit.Seq
		}
		r.credit.Store(&credit)
		r.ack.Signal()
	}
}

// The sole watermark writer flushes even a partial batch when the ordered reader reaches END.
func (r *slotReader) ackLoop(live context.Context, writerDone <-chan struct{}) {
	ctx := context.WithValue(r.ctx, headKey{}, true)
	t := time.NewTicker(slotMediaAck)
	defer t.Stop()
	acked, name := int64(-1), r.table.name+"/w"
	var lastWrite time.Time
	batch := max(int64(1), int64(len(r.table.Slots))/6)
	for open := true; open && ctx.Err() == nil; {
		open = waitFor(live, t.C, r.ack.Wait()) == nil
		credit := r.credit.Load()
		elapsed := time.Since(lastWrite)
		early := elapsed < slotMediaAck || credit.mediaSeq <= acked && elapsed < slotIdleAck
		if credit.Seq <= acked || ctx.Err() != nil || open && credit.Seq-acked < batch && early {
			continue
		}
		lastWrite = time.Now()
		description, _ := json.Marshal(credit)
		if err := r.store.writeFile(ctx, r.table.Watermark, name, driveFile{Description: string(description)}, nil, nil, nil); err != nil {
			r.fail(errors.New("cannot publish Drive watermark").Base(err))
			return
		}
		acked, name = credit.Seq, "" // Subsequent watermark publications patch the existing file.
		errors.LogDebug(ctx, "xdrive watermark seq ", acked, " put ", time.Since(lastWrite))
		if isClosedChan(writerDone) { // Only ACKs after our writer finishes extend the close watchdog.
			r.markProgress()
		}
	}
}
