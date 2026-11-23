package xdrive

import (
	"context"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// Readers discover known IDs concurrently. Only run delivers bytes and moves
// head. Lookahead media retains at most two whole frames, plus the ordered head.
type slotReader struct {
	ctx      context.Context
	setErr   context.CancelCauseFunc
	discard  <-chan struct{}
	stopRead context.CancelFunc
	readDone chan struct{}
	store    slotStore
	table    slotTable
	params
	ch                              chan []byte
	wake, ack, ackTurn, media       chan struct{}
	head, frontier, activeAt, acked atomic.Int64
	probing                         atomic.Bool
	wg                              sync.WaitGroup
}

type readSlot struct {
	seq                  int64
	head, retired, ready chan struct{}
	data                 chan []byte // published by ready; closed after the last verified block
	end                  bool
	err                  error // published by closing ready (no data) or data
}

func newSlotReader(ctx context.Context, fail context.CancelCauseFunc, store slotStore, table slotTable, p params, first []byte, discard <-chan struct{}) *slotReader {
	readCtx, stopRead := context.WithCancel(ctx)
	r := &slotReader{
		ctx: ctx, setErr: fail, discard: discard, stopRead: stopRead, readDone: make(chan struct{}), store: store, table: table, params: p,
		ch: make(chan []byte, 2), wake: make(chan struct{}, 1), ack: make(chan struct{}, 1),
		ackTurn: make(chan struct{}, 1), media: make(chan struct{}, 2),
	}
	r.frontier.Store(-1)
	r.acked.Store(-1)
	r.Wake()
	if len(first) != 0 {
		r.ch <- first
		r.head.Store(1)
		r.ack <- struct{}{}
	}
	r.wg.Go(func() {
		defer close(r.readDone)
		r.run(readCtx)
	})
	r.wg.Go(r.ackLoop)
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

func (r *slotReader) run(parent context.Context) {
	defer close(r.ch)
	ctx, cancel := context.WithCancel(parent)
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

// Normal and final watermarks share one writer. Snapshot head after acquiring
// the turn so a delayed final flush cannot overwrite a newer acknowledgement.
func (r *slotReader) writeAck(ctx context.Context) error {
	select {
	case r.ackTurn <- struct{}{}:
		defer func() { <-r.ackTurn }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := context.Cause(r.ctx); err != nil {
		return err
	}
	n := r.head.Load() - 1
	prev := r.acked.Load()
	if n <= prev {
		return nil
	}
	ctx = context.WithValue(ctx, headKey{}, true)
	d := strconv.FormatInt(n, 10)
	var err error
	if r.table.lazy && prev < 0 {
		err = r.store.createSlot(ctx, r.table.Watermark, r.table.name+"/w", d, nil)
	} else {
		err = r.store.setSlot(ctx, r.table.Watermark, d, nil)
	}
	if err != nil {
		r.setErr(errors.New("cannot publish Drive watermark").Base(err))
		return context.Cause(r.ctx)
	}
	r.acked.Store(n)
	return nil
}

// Stop advancing head before taking the last snapshot. The connection owns ctx,
// so an explicit interrupt still cancels both the drain and the HTTP write.
func (r *slotReader) finish(ctx context.Context) error {
	r.stopRead()
	select {
	case <-r.readDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	return r.writeAck(ctx)
}

func (r *slotReader) ackLoop() {
	t := time.NewTicker(1500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.ack:
			if r.head.Load()-1-r.acked.Load() < max(int64(1), int64(len(r.table.Slots))/6) {
				continue
			}
		case <-t.C:
		}
		if r.writeAck(r.ctx) != nil {
			return
		}
	}
}
