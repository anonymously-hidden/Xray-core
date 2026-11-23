package xdrive

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// run is the sole allocator of sequence numbers and owns the coalescing buffer.
// Uploads may finish out of order. Each physical position has a completion fence.
type slotWriter struct {
	ctx    context.Context
	cancel context.CancelFunc
	store  slotStore
	table  slotTable
	params
	input               chan []byte
	writeToken, permits chan struct{}
	stop, done          chan struct{}
	first               chan []byte
	closeOnce           sync.Once
	workers             sync.WaitGroup
	mu                  sync.Mutex
	err                 error
	progress            time.Time
	issued              atomic.Int64
	wmMu                sync.Mutex
	acked               int64
	checked             time.Time
}

func newSlotWriter(parent context.Context, store slotStore, table slotTable, p params) *slotWriter {
	ctx, cancel := context.WithCancel(parent)
	w := &slotWriter{
		ctx: ctx, cancel: cancel, store: store, table: table, params: p,
		input: make(chan []byte), writeToken: make(chan struct{}, 1), permits: make(chan struct{}, min(len(table.Slots), 32)),
		stop: make(chan struct{}), done: make(chan struct{}), first: make(chan []byte, 1), acked: -1, progress: time.Now(),
	}
	go w.run()
	return w
}

func (w *slotWriter) fail(err error) {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.mu.Unlock()
	w.cancel()
}

func (w *slotWriter) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

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

// Only one watermark GET per refresh interval, even when a wave wraps together.
func (w *slotWriter) credit(seq int64) (bool, error) {
	need := seq - int64(len(w.table.Slots))
	if need < 0 {
		return true, nil
	}
	w.wmMu.Lock()
	defer w.wmMu.Unlock()
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
	if err != nil || n < 0 || n >= w.issued.Load() {
		return false, errors.New("invalid receiver watermark")
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
		if err != nil && (isAuthError(err) || err == errNotFound) {
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
		// Best effort only, and never on a position whose reuse wasn't fenced.
		if mode != slotErr && w.ctx.Err() == nil {
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
	defer w.cancel()
	defer func() {
		if err := w.ctx.Err(); err != nil {
			w.fail(err)
		}
	}()
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
		w.issued.Store(seq)
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
		w.workers.Add(1)
		go func() {
			defer w.workers.Done()
			defer close(done)
			defer func() { <-w.permits }()
			if err := w.publish(n, prev, mode, chunk); err != nil {
				w.fail(err)
			}
		}()
		return true
	}
	var buf []byte
	var timer *time.Timer
	var tick <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	flush := func() bool {
		if timer != nil {
			timer.Stop()
		}
		tick = nil
		unit := min(w.segmentBytes, slotInlineLimit)
		wave := min(24, cap(w.permits)-len(w.permits))
		inline := min(len(buf), wave*unit)
		if len(buf) > wave*unit && (wave < 24 || len(buf) > w.segmentBytes) {
			inline = 0
		}
		if isCooling(w.store) {
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
				timer = time.NewTimer(d)
				tick = timer.C
			} else {
				buf = append(buf, b...)
			}
			if len(buf) >= w.segmentBytes && !flush() {
				return
			}
		case <-tick:
			if !flush() {
				return
			}
		}
	}
}

func (w *slotWriter) Close() error {
	w.closeOnce.Do(func() {
		w.markProgress()
		close(w.stop)
	})
	if err := waitCloseProgress(w.done, w.closeProgress); err != nil {
		w.fail(err)
	}
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
		t := time.NewTimer(remaining)
		select {
		case <-done:
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}
