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

	// Every block starts with a sequence tag so each range can reject an older lap.
	slotBodyHeader     = 8
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

// A slot description carries the sequence, total payload length and any inline bytes.
type slotFrame struct {
	seq  int64
	mode byte
	size int
	data []byte
}

func (f slotFrame) encode() string {
	b := append(make([]byte, 0, slotHeaderLen+len(f.data)), slotMagic...)
	b = binary.BigEndian.AppendUint64(b, uint64(f.seq))
	b = append(b, f.mode)
	b = binary.BigEndian.AppendUint32(b, uint32(f.size))
	return base64.StdEncoding.EncodeToString(append(b, f.data...))
}

func decodeSlot(description string) (slotFrame, bool) {
	b, err := base64.StdEncoding.DecodeString(description)
	if err != nil || len(b) < slotHeaderLen || !bytes.HasPrefix(b, []byte(slotMagic)) {
		return slotFrame{}, false
	}
	f := slotFrame{seq: int64(binary.BigEndian.Uint64(b[8:])), mode: b[16], size: int(binary.BigEndian.Uint32(b[17:])), data: b[slotHeaderLen:]}
	if f.seq < 0 || uint(f.size) > maxSegmentBytes {
		return slotFrame{}, false
	}
	switch f.mode {
	case slotInline:
		return f, f.size > 0 && f.size <= slotInlineLimit && len(f.data) == f.size
	case slotStream:
		return f, len(f.data) < f.size && len(f.data) <= slotInlineLimit
	case slotEnd, slotErr:
		return f, f.size == 0 && len(f.data) == 0
	}
	return slotFrame{}, false
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
var slotIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// The session owns unchecked exclusively until admission succeeds.
func decodeRing(b []byte, session string, unchecked *sync.Map) *ring {
	var r ring
	if json.Unmarshal(b, &r) != nil || len(r.First) > slotInlineLimit {
		return nil
	}
	r.Up.name, r.Down.name = uplinkPrefix(session), downlinkPrefix(session)
	for _, t := range []*slotTable{&r.Up, &r.Down} {
		t.lazy = r.Lazy
		if len(t.Slots) == 0 || len(t.Slots) > maxConcurrency {
			return nil
		}
		for i, id := range append(slices.Clip(t.Slots), t.Watermark) {
			name := t.name + "/w"
			if i < len(t.Slots) {
				name = t.name + "/s" + strconv.Itoa(i)
			}
			if _, duplicate := unchecked.LoadOrStore(id, flatten(name)); duplicate || !slotIDPattern.MatchString(id) {
				return nil
			}
		}
	}
	return &r
}

// IDs are allocated in direction order: slots, watermark, slots, watermark.
func lazyRing(session string, depth int, ids []string) ring {
	table := func(name string, batch []string) slotTable {
		return slotTable{Slots: batch[:depth:depth], Watermark: batch[depth], name: name, lazy: true}
	}
	return ring{Lazy: true, Up: table(uplinkPrefix(session), ids), Down: table(downlinkPrefix(session), ids[depth+1:])}
}

// Only the first publication of a lazy slot is a create. Inline data and the
// progressive prefix live in the description; every media block carries seq.
func (d *slotDirection) writeFrame(ctx context.Context, f slotFrame) error {
	t := d.table
	idx := int(f.seq % int64(len(t.Slots)))
	var body, tag []byte
	if f.mode == slotStream {
		body, f.data = f.data, nil
		if f.size <= slotProgressiveMax {
			f.data, body = body[:slotInlineLimit], body[slotInlineLimit:]
		}
		tag = binary.BigEndian.AppendUint64(nil, uint64(f.seq))
	}
	name := ""
	if t.lazy && f.seq < int64(len(t.Slots)) {
		name = t.name + "/s" + strconv.Itoa(idx)
	}
	return d.store.writeFile(ctx, t.Slots[idx], name, f.encode(), body, tag)
}

// Each direction has its own table and byte allowance; both share the session's failure cause.
type slotDirection struct {
	*Conn
	store  *driveStorage
	table  slotTable
	flight *semaphore.Weighted
	limit  int64
	params
}

// The writer owns batching and sequencing; per-slot fences order reuse across concurrent uploads.
type slotWriter struct {
	slotDirection
	input                 chan []byte
	writeToken, permits   chan struct{}
	done                  chan struct{}
	first                 chan []byte
	next, acked           int64 // Writer-owned: first unassigned and last credited sequence.
	nextCredit            time.Time
	through               []int64 // Total media payload through the last assignment of each position.
	mediaSent, mediaAcked int64
	rate                  int64 // Reported ordered bytes per second; zero until the first media credit.
}

type slotCredit struct {
	Seq      int64 `json:"s"`
	Rate     int64 `json:"r"` // Ordered payload bytes per second over the last media fetch.
	mediaSeq int64 // Last media sequence delivered locally; controls early ACKs.
}

func newSlotWriter(dir slotDirection) *slotWriter {
	w := &slotWriter{
		slotDirection: dir,
		input:         make(chan []byte), writeToken: make(chan struct{}, 1),
		permits: make(chan struct{}, min(len(dir.table.Slots), 32)),
		done:    make(chan struct{}),
		first:   make(chan []byte, 1), acked: -1,
		through: make([]int64, len(dir.table.Slots)),
	}
	w.markProgress()
	go w.run()
	return w
}

func sendUntil[T any](ctx context.Context, dst chan<- T, value T, deadline, closing <-chan struct{}) error {
	select {
	case dst <- value:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-deadline:
	case <-closing:
	}
	return errWriteStopped
}

func (w *slotWriter) writeUntil(p []byte, deadline, _ <-chan struct{}) (int, error) {
	if err := sendUntil(w.ctx, w.writeToken, struct{}{}, deadline, w.closing); err != nil {
		return 0, err
	}
	defer func() { <-w.writeToken }()
	n := 0
	for chunk := range slices.Chunk(p, w.segmentBytes) {
		if isClosedChan(deadline) || isClosedChan(w.closing) {
			return n, errWriteStopped
		}
		if err := context.Cause(w.ctx); err != nil {
			return n, err
		}
		if err := sendUntil(w.ctx, w.input, bytes.Clone(chunk), deadline, w.closing); err != nil {
			return n, err
		}
		n += len(chunk)
	}
	return n, nil
}

func (c *Conn) markProgress() { c.progress.Store(new(time.Now())) }

func (w *slotWriter) frameSize(seedFloor int) int {
	seed := min(max(w.segmentBytes, seedFloor), slotProgressiveMax)
	return int(min(slotMaxFrame, max(slotTagBlock, cmp.Or(w.rate, int64(seed)))))
}

// Admit cumulative slot and byte credit in order, before starting an upload worker.
func (w *slotWriter) waitCredit(size int64) (time.Duration, bool) {
	start := time.Now()
	depth, media := int64(len(w.through)), size > slotInlineLimit
	for w.ctx.Err() == nil {
		// Keep a pair of requests per lane in flight plus the bytes deliverable between ACK ticks.
		window := int64(max(4, 2*len(w.store.pools[mediaLane].lanes))) * int64(w.frameSize(defaultSegmentBytes))
		window = min(w.limit+int64(w.frameSize(slotTagBlock)), window+w.rate/4) // One head beyond look-ahead.
		queued := w.mediaSent - w.mediaAcked
		full := w.next-w.acked > depth || media && queued+size > max(window, size)
		if !full && (!media || queued < window/2 || time.Since(w.nextCredit) < slotIdleAck) {
			break
		}
		if sleepContext(w.ctx, time.Until(w.nextCredit)) != nil {
			return 0, false
		}
		ctx, cancel := context.WithTimeout(context.WithValue(w.ctx, headKey{}, true), 15*time.Second)
		d, err := w.store.readDescription(ctx, w.table.Watermark)
		cancel()
		w.nextCredit = time.Now().Add(100 * time.Millisecond)
		switch {
		case err == nil && d != "":
			ack := slotCredit{Seq: -1}
			if json.Unmarshal([]byte(d), &ack) != nil || ack.Seq < 0 || ack.Seq >= w.next || ack.Rate < 0 || ack.Rate > slotMaxFrame*maxConcurrency {
				w.fail(errors.New("invalid receiver watermark"))
				return 0, false
			}
			if ack.Seq > w.acked {
				if !w.store.cooling() {
					w.rate = ack.Rate
				}
				w.acked, w.mediaAcked = ack.Seq, w.through[ack.Seq%depth]
				w.markProgress()
				errors.LogDebug(w.ctx, "xdrive credit seq ", w.next, " ack ", w.acked, " frame ", w.frameSize(slotTagBlock))
			}
		case isAuthError(err), err == errDriveScope, err == errNotFound && !w.table.lazy:
			w.fail(err)
			return 0, false
		}
		if time.Since(start) > slotStallTimeout && time.Since(*w.progress.Load()) > slotStallTimeout {
			w.fail(errors.New("timed out waiting for receiver watermark").Base(err))
			return 0, false
		}
	}
	return time.Since(start), w.ctx.Err() == nil
}

func (w *slotWriter) publish(f slotFrame, previous <-chan struct{}, credit time.Duration) {
	if previous != nil && waitFor(w.ctx, previous, nil) != nil {
		return
	}
	started := time.Now()
	if err := w.writeFrame(w.ctx, f); err != nil {
		// A lost reply is not a refusal: never overwrite possibly committed data.
		if _, refused := err.(refusedPayload); refused && w.ctx.Err() == nil {
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

// gather yields immutable chunks synchronously: a blocked upload admission also
// holds back the next input handoff. Overflow keeps its original arrival time.
func (w *slotWriter) gather(yield func([]byte) bool) {
	var buf, pending []byte
	var bufAt, lastIn, lastBurst time.Time
	timer := time.NewTimer(w.flushInterval)
	timer.Stop()
	defer timer.Stop()
	closing := false
	for !closing {
		frame := w.frameSize(slotTagBlock)
		if len(pending) == 0 {
			pending = nil // Release the last handoff before waiting on an idle stream.
			select {
			case <-w.ctx.Done():
				return
			case <-w.closing:
				closing = true
			case pending = <-w.input:
				lastIn = time.Now()
			case <-timer.C:
				// Busy streams gather until a full frame, a pause or the oldest byte's deadline.
				wait := min(w.flushInterval-time.Since(lastIn), slotBusyGather-time.Since(bufAt))
				if len(w.permits) >= slotBusyUploads && wait > 0 {
					timer.Reset(wait)
					continue
				}
			}
		}
		if len(pending) != 0 {
			n := min(len(pending), frame-len(buf))
			if len(buf) == 0 {
				buf, bufAt = pending[:n:n], lastIn
				d := w.flushInterval
				if len(w.permits) == 0 && n <= slotInlineLimit && !w.store.cooling() {
					d = min(d, 2*time.Millisecond)
				}
				timer.Reset(d)
			} else {
				buf = append(buf, pending[:n]...)
			}
			pending = pending[n:]
			if len(buf) < frame {
				continue
			}
		}
		timer.Stop()
		size, busy, cooling := frame, len(w.permits), w.store.cooling()
		wave := min(slotReadAhead, cap(w.permits)-busy, len(w.table.Slots)-int(w.next-w.acked-1)) // Both local and remote positions must fit.
		if busy >= slotBusyUploads || len(buf) == size || time.Since(lastBurst) < w.eagerWindow {
			wave = min(wave, slotPollAhead)
		}
		if len(buf) > slotPollAhead*slotInlineLimit {
			lastBurst = time.Now()
		}
		if !cooling && busy < 2*slotBusyUploads && len(buf) <= wave*slotInlineLimit {
			size = slotInlineLimit
		}
		for chunk := range slices.Chunk(buf, size) {
			if !yield(chunk) {
				return
			}
		}
		buf = nil // The upload workers now own the backing array.
	}
}

func (w *slotWriter) run() {
	defer close(w.done)
	var workers sync.WaitGroup
	fences := make([]chan struct{}, len(w.through))
	for chunk := range w.gather {
		charge := int64(len(chunk))
		credit, ok := w.waitCredit(charge)
		if !ok || sendUntil(w.ctx, w.permits, struct{}{}, nil, nil) != nil {
			break
		}
		if w.flight.Acquire(w.ctx, charge) != nil {
			<-w.permits
			break
		}
		f := slotFrame{seq: w.next, mode: slotInline, size: len(chunk), data: chunk}
		w.next++
		i := f.seq % int64(len(fences))
		prev, done := fences[i], make(chan struct{})
		fences[i] = done
		if f.size > slotInlineLimit {
			f.mode = slotStream
			w.mediaSent += charge
		} else if f.seq == 0 {
			w.first <- chunk
		}
		w.through[i] = w.mediaSent // Inline ACKs may cover earlier media.
		workers.Go(func() {
			defer close(done)
			defer func() { <-w.permits }()
			defer w.flight.Release(charge)
			w.publish(f, prev, credit)
		})
	}
	workers.Wait()
	if _, ok := w.waitCredit(0); ok {
		w.publish(slotFrame{seq: w.next, mode: slotEnd}, nil, 0)
	}
}

func (w *slotWriter) Close() error {
	<-w.done // Conn.Close owns the watchdog for the entire shutdown.
	return context.Cause(w.ctx)
}

// Inbound: workers discover known IDs concurrently. Only run delivers bytes and moves
// head. Lookahead media uses the shared byte allowance; the head has its own.
type slotReader struct {
	slotDirection
	ch                                                     chan []byte
	wake                                                   *signal.Notifier
	ack                                                    chan struct{} // buffered wakeup; the ordered reader closes it for the final snapshot
	credit                                                 atomic.Pointer[slotCredit]
	delivered, frontier, verified, activeAt, inlineThrough atomic.Int64
	writerDone                                             <-chan struct{}
	probing                                                atomic.Bool
	wg                                                     sync.WaitGroup
}

type readSlot struct {
	seq       int64
	prior     <-chan struct{} // ready fence of the preceding frame
	lookahead context.Context // canceled on head promotion or reader shutdown
	promote   context.CancelFunc
	ready     chan struct{}
	data      chan []byte // published by ready; closed after the last verified block
	reserved  int64       // published by ready; returned by the ordered reader
	end       bool
	started   time.Time // Media admission; observed ordered bytes since this point include overlapping fetches.
	before    int64
	err       error // final after data closes
}

func newSlotReader(dir slotDirection, first []byte, writerDone <-chan struct{}) *slotReader {
	r := &slotReader{
		slotDirection: dir, writerDone: writerDone,
		ch: make(chan []byte, 2), wake: signal.NewNotifier(), ack: make(chan struct{}, 1),
	}
	r.frontier.Store(-1)
	credit := &slotCredit{Seq: -1, mediaSeq: -1}
	r.Wake()
	if len(first) != 0 {
		r.ch <- first
		credit.Seq = 0
		r.ack <- struct{}{}
	}
	r.credit.Store(credit)
	r.wg.Go(r.run)
	r.wg.Go(r.ackLoop)
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

// A lazy missing head may be idle. A valid successor at its own ring position
// is evidence of a gap. Run at most one probe without delaying the head poll.
func (r *slotReader) probe(ctx context.Context, seq, idx, depth int64) {
	defer r.probing.Store(false)
	ctx, cancel := context.WithTimeout(ctx, slotStallTimeout)
	defer cancel()
	d, err := r.store.readDescription(ctx, r.table.Slots[idx])
	if f, ok := decodeSlot(d); err == nil && ok && f.seq > seq && f.seq%depth == idx {
		advance(&r.frontier, f.seq)
		r.Wake()
	}
}

func (r *slotReader) lookup(ctx context.Context, job *readSlot, id string) (slotFrame, error) {
	depth := int64(len(r.table.Slots))
	probeAt, offset := time.Now().Add(slotStallTimeout), int64(1)
	var gapDeadline time.Time
	priority := context.WithValue(ctx, headKey{}, true)
	first := true
	for {
		head := job.seq == r.head()
		active := time.Since(time.Unix(0, r.activeAt.Load())) < r.eagerWindow
		readCtx, wake, delay := ctx, job.lookahead.Done(), 40*time.Millisecond
		if head {
			readCtx, wake, delay = priority, r.wake.Wait(), 25*time.Millisecond
		}
		// Empty distant slots must not spend the quota of useful media and ACKs.
		if head || active && (job.seq <= max(r.frontier.Load(), r.head())+slotPollAhead || first && job.seq <= r.inlineThrough.Load()) {
			first = false // Only one speculative GET outside the ordinary polling window.
			d, err := r.store.readDescription(readCtx, id)
			f, ok := decodeSlot(d)
			switch {
			case err == errDriveScope || isAuthError(err):
				return slotFrame{}, err
			case err == nil && ok && f.seq > job.seq:
				return slotFrame{}, errors.New("Drive slot ", job.seq, " was overwritten by sequence ", f.seq)
			case err == nil && ok && f.seq == job.seq:
				if f.mode == slotInline && f.size == slotInlineLimit {
					advance(&r.inlineThrough, f.seq+slotReadAhead-1)
				}
				r.activeAt.Store(time.Now().UnixNano())
				advance(&r.frontier, f.seq)
				return f, nil
			}
			if head && depth > 1 && time.Now().After(probeAt) && r.frontier.Load() <= job.seq && !r.store.cooling() && r.probing.CompareAndSwap(false, true) {
				idx := (job.seq + offset) % depth
				offset, probeAt = offset%(depth-1)+1, time.Now().Add(slotStallTimeout/12)
				r.wg.Go(func() { r.probe(ctx, job.seq, idx, depth) })
			}
			idle := err == errNotFound && r.table.lazy && job.seq < depth
			gap := err != nil && !idle || d != "" && !ok || r.frontier.Load() > job.seq
			switch {
			case !head || !gap:
				gapDeadline = time.Time{}
			case gapDeadline.IsZero():
				gapDeadline = time.Now().Add(slotStallTimeout)
			case !time.Now().Before(gapDeadline):
				return slotFrame{}, errors.New("Drive slot ", job.seq, " did not become readable").Base(err)
			}
		}
		if !active {
			delay = r.maxPollInterval
		}
		if err := waitFor(ctx, time.After(delay), wake); err != nil {
			return slotFrame{}, err
		}
	}
}

func (r *slotReader) fetch(ctx context.Context, j *readSlot) {
	start := time.Now()
	id := r.table.Slots[j.seq%int64(len(r.table.Slots))]
	f, err := r.lookup(ctx, j, id)
	found := time.Now()
	j.end = f.mode == slotEnd
	if f.mode == slotErr {
		err = errors.New("peer could not store segment ", j.seq)
	}
	// Discover concurrently, but reserve body bytes in sequence order.
	err = cmp.Or(waitFor(ctx, j.prior, j.lookahead.Done()), err) // The first job has no predecessor.
	charge := min(int64(f.size), r.limit)                        // A larger peer-supplied frame can run alone.
	if err == nil && f.mode == slotStream && j.seq != r.head() && r.flight.Acquire(j.lookahead, charge) == nil {
		j.reserved = charge
	}
	j.err = cmp.Or(err, ctx.Err())
	media := f.size - len(f.data) // Media offsets exclude the prefix in the description.
	j.data = make(chan []byte, media/slotReadChunk+slotBlocks(media)+2)
	defer close(j.data)
	if j.err == nil && len(f.data) != 0 {
		j.data <- f.data
	}
	close(j.ready)
	if j.err == nil && media > 0 {
		j.started, j.before = time.Now(), r.delivered.Load() // Published to run by closing j.data.
		ctx = context.WithValue(ctx, mediaHeadKey{}, j.lookahead.Done())
		f.size = media
		j.err = r.store.readFrameStream(ctx, id, f, j.data, &r.verified)
		errors.LogDebug(ctx, "xdrive read seq ", j.seq, " bytes ", media+len(f.data), " find ", found.Sub(start), " budget ", j.started.Sub(found), " body ", time.Since(j.started), " err ", j.err)
	}
	if j.err == nil && !j.end {
		advance(&r.verified, j.seq)
	}
}

func (r *slotReader) run() {
	ctx, cancel := context.WithCancel(r.ctx)
	defer close(r.ack)
	defer close(r.ch)
	defer cancel() // Wakes every waiter; the look-ahead budget belongs only to this reader.
	jobs := make(chan *readSlot, min(slotReadAhead, len(r.table.Slots)))
	var prior <-chan struct{}
	for head := r.head(); ctx.Err() == nil; head++ {
		for len(jobs) < cap(jobs) {
			lookahead, promote := context.WithCancel(ctx)
			j := &readSlot{
				seq: head + int64(len(jobs)), prior: prior,
				lookahead: lookahead, promote: promote, ready: make(chan struct{}),
			}
			jobs <- j
			prior = j.ready
			r.wg.Go(func() { r.fetch(ctx, j) })
			if head == 0 { // Establish frame zero before probing ahead.
				break
			}
		}
		j := <-jobs
		j.promote()
		if waitFor(ctx, j.ready, nil) != nil {
			return
		}
		if j.reserved != 0 {
			r.flight.Release(j.reserved) // The head now uses its own allowance.
		}
		for b := range j.data {
			// Close discards input but continues verification and credit; cancellation stops both.
			if err := sendUntil(ctx, r.ch, b, nil, r.closing); err != nil && ctx.Err() != nil {
				return
			}
			r.activeAt.Store(time.Now().UnixNano()) // ordered input keeps look-ahead eager
			r.delivered.Add(int64(len(b)))
		}
		if j.err != nil {
			r.fail(j.err) // A prior cancellation keeps its original cause.
			return
		}
		if j.end {
			return
		}
		credit := *r.credit.Load()
		credit.Seq = head
		if !j.started.IsZero() {
			rate := float64(r.delivered.Load()-j.before) / time.Since(j.started).Seconds()
			credit.Rate, credit.mediaSeq = int64(min(slotMaxFrame*maxConcurrency, rate)), head
		}
		r.credit.Store(&credit)
		select {
		case r.ack <- struct{}{}:
		default:
		}
	}
}

// This goroutine is the only watermark writer. END triggers one last snapshot
// after the ordered reader has stopped, including credit below the batch size.
func (r *slotReader) ackLoop() {
	ctx := context.WithValue(r.ctx, headKey{}, true)
	t := time.NewTicker(slotMediaAck)
	defer t.Stop()
	acked := int64(-1)
	var lastWrite time.Time
	batch := max(int64(1), int64(len(r.table.Slots))/6)
	open := true
	for open && ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case _, open = <-r.ack:
		case <-t.C:
		}
		credit := r.credit.Load()
		interval := slotIdleAck
		if credit.mediaSeq > acked {
			interval = slotMediaAck
		}
		if credit.Seq <= acked || ctx.Err() != nil || open && credit.Seq-acked < batch && time.Since(lastWrite) < interval {
			continue
		}
		started := time.Now()
		name := ""
		if r.table.lazy && acked < 0 {
			name = r.table.name + "/w"
		}
		description, _ := json.Marshal(credit)
		if err := r.store.writeFile(ctx, r.table.Watermark, name, string(description), nil, nil); err != nil {
			r.fail(errors.New("cannot publish Drive watermark").Base(err))
			return
		}
		acked, lastWrite = credit.Seq, started
		errors.LogDebug(ctx, "xdrive watermark seq ", acked, " put ", time.Since(started))
		if isClosedChan(r.writerDone) { // Only ACKs after our writer finishes extend the close watchdog.
			r.markProgress()
		}
	}
}
