package xdrive

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

const (
	ringVersion      = 2
	slotMagic        = "XDRVSL02"
	slotHeaderLen    = len(slotMagic) + 17
	slotInlineLimit  = 14 << 10
	slotInline       = 1
	slotMedia        = 2
	slotEnd          = 3
	slotErr          = 4
	maxSlotIDLen     = 128
	slotStallTimeout = time.Minute
)

var errWriteStopped = errors.New("write stopped")

type slotStore interface {
	takeIDs(context.Context, int) ([]string, error)
	createSlot(context.Context, string, string, string, []byte) error
	setSlot(context.Context, string, string, []byte) error
	readDescription(context.Context, string) (string, error)
	readMedia(context.Context, string, slotFrame) ([]byte, error)
}

// The description carries the payload for inline frames. A media frame has
// only this header in its description and the complete payload in its body.
type slotFrame struct {
	seq  int64
	mode byte
	size int
	crc  uint32
	data []byte
}

func encodeSlot(seq int64, mode byte, data []byte) string {
	b := binary.BigEndian.AppendUint64([]byte(slotMagic), uint64(seq))
	b = append(b, mode)
	b = binary.BigEndian.AppendUint32(b, uint32(len(data)))
	b = binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(data))
	if mode == slotInline {
		b = append(b, data...)
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
	case slotMedia:
		if f.size <= slotInlineLimit || len(b) != slotHeaderLen {
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

type slotTable struct {
	Slots     []string
	Watermark string
	name      string
}

type ring struct {
	Up, Down slotTable
}

// Each half contains the slot IDs followed by its reader's watermark ID.
type ringAnnouncement struct {
	Version int      `json:"v"`
	IDs     []string `json:"ids"`
}

// IDs are single opaque URL path components, never paths or special segments.
func validSlotIDs(ids []string) bool {
	seen := make(map[string]bool, len(ids))
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
	for _, id := range ids {
		if id == "" || len(id) > maxSlotIDLen || strings.Trim(id, alphabet) != "" || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}

func decodeRing(session string, b []byte) (ring, bool) {
	var a ringAnnouncement
	if json.Unmarshal(b, &a) != nil || a.Version != ringVersion {
		return ring{}, false
	}
	n := len(a.IDs)
	if n < 6 || n > 2*(maxConcurrency+1) || n%2 != 0 || !validSlotIDs(a.IDs) {
		return ring{}, false
	}
	return sessionRing(session, a.IDs), true
}

// IDs are immutable for the lifetime of the connection. The dialer reserves
// them locally; the listener validates the entire batch before splitting it.
func sessionRing(session string, ids []string) ring {
	table := func(name string, half []string) slotTable {
		depth := len(half) - 1
		return slotTable{Slots: half[:depth:depth], Watermark: half[depth], name: name}
	}
	mid := len(ids) / 2
	return ring{Up: table(uplinkPrefix(session), ids[:mid]), Down: table(downlinkPrefix(session), ids[mid:])}
}

// A single publisher owns each direction. No local completion fences are
// needed; only a verified remote watermark permits overwriting an old lap.
func (t slotTable) publish(ctx context.Context, store slotStore, seq int64, mode byte, data []byte) error {
	i := int(seq % int64(len(t.Slots)))
	description := encodeSlot(seq, mode, data)
	var body []byte
	if mode == slotMedia {
		body = data
	}
	if seq < int64(len(t.Slots)) {
		return store.createSlot(ctx, t.Slots[i], t.name+"/s"+strconv.Itoa(i), description, body)
	}
	return store.setSlot(ctx, t.Slots[i], description, body)
}

type slotWriter struct {
	ctx   context.Context
	store slotStore
	table slotTable
	params
	input      chan []byte
	writeToken chan struct{}
	stop, done chan struct{}
	closeOnce  sync.Once
	mu         sync.Mutex
	progress   time.Time
	acked      int64 // owned by run
}

func newSlotWriter(ctx context.Context, fail context.CancelCauseFunc, store slotStore, table slotTable, p params) *slotWriter {
	w := &slotWriter{
		ctx:        ctx,
		store:      store,
		table:      table,
		params:     p,
		input:      make(chan []byte),
		writeToken: make(chan struct{}, 1),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
		acked:      -1,
	}
	go func() {
		defer close(w.done)
		if err := w.run(); err != nil {
			fail(err)
		}
	}()
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

func (w *slotWriter) waitCredit(seq int64) error {
	need := seq - int64(len(w.table.Slots))
	if w.acked >= need {
		return nil
	}
	ctx, cancel := context.WithTimeout(w.ctx, slotStallTimeout)
	defer cancel()
	for {
		d, err := w.store.readDescription(ctx, w.table.Watermark)
		switch {
		case err == errNotFound:
			// Missing or temporarily invisible: never grant credit on a 404.
		case err != nil:
			return err
		default:
			n, err := strconv.ParseInt(d, 10, 64)
			if err != nil || n < 0 || n >= seq {
				return errors.New("invalid receiver watermark")
			}
			w.acked = max(w.acked, n)
			if w.acked >= need {
				return nil
			}
		}
		if err := sleepContext(ctx, w.maxPollInterval); err != nil {
			return err
		}
	}
}

func (w *slotWriter) publish(seq int64, mode byte, data []byte) error {
	if err := w.waitCredit(seq); err != nil {
		return err
	}
	if err := w.table.publish(w.ctx, w.store, seq, mode, data); err != nil {
		// An explicit refusal, unlike a lost reply, did not publish this frame.
		if _, refused := err.(refusedPayload); refused && w.ctx.Err() == nil {
			ctx, cancel := context.WithTimeout(w.ctx, 2*time.Second)
			defer cancel()
			_ = w.table.publish(ctx, w.store, seq, slotErr, nil)
		}
		return err
	}
	w.markProgress()
	return nil
}

// Start one flush timer with the first small write. Never queue a second upload.
func (w *slotWriter) run() error {
	var flush <-chan time.Time
	var seq int64
	var buf []byte
	for {
		closing := false
		select {
		case <-w.ctx.Done():
			return context.Cause(w.ctx)
		case <-w.stop:
			closing = true
		case <-flush:
		case b := <-w.input:
			if buf == nil {
				buf = b // writeUntil transferred ownership of this copy
			} else {
				buf = append(buf, b...)
			}
			if len(buf) < w.segmentBytes {
				if flush == nil {
					flush = time.After(w.flushInterval)
				}
				continue
			}
		}
		for chunk := range slices.Chunk(buf, w.segmentBytes) {
			mode := byte(slotInline)
			if len(chunk) > slotInlineLimit {
				mode = slotMedia
			}
			if err := w.publish(seq, mode, chunk); err != nil {
				return err
			}
			seq++
		}
		buf, flush = nil, nil
		if closing {
			return w.publish(seq, slotEnd, nil)
		}
	}
}

func (w *slotWriter) Close() error {
	w.closeOnce.Do(func() { close(w.stop) })
	<-w.done // Conn.Close owns the watchdog for the entire shutdown.
	return w.Err()
}

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

// One reader polls, verifies, delivers, and acknowledges in sequence.
// A failed or interrupted media GET cannot expose any part of a frame.
type slotReader struct {
	ctx         context.Context
	store       slotStore
	table       slotTable
	poll        time.Duration
	discard     <-chan struct{}
	ch          chan []byte
	done        chan struct{}
	drainCredit atomic.Pointer[time.Time]
}

func newSlotReader(ctx context.Context, fail context.CancelCauseFunc, store slotStore, table slotTable, p params, discard <-chan struct{}) *slotReader {
	r := &slotReader{
		ctx:     ctx,
		store:   store,
		table:   table,
		poll:    p.maxPollInterval,
		discard: discard,
		ch:      make(chan []byte),
		done:    make(chan struct{}),
	}
	go func() {
		defer close(r.done)
		defer close(r.ch)
		if err := r.run(); err != nil {
			fail(err) // publish the cause before closing the data channel
		}
	}()
	return r
}

func (r *slotReader) chunks() <-chan []byte { return r.ch }
func (r *slotReader) wait()                 { <-r.done }
func (r *slotReader) Wake()                 {} // fixed polling; no speculative GET

func (r *slotReader) Err() error {
	if err := context.Cause(r.ctx); err != nil {
		return err
	}
	return io.EOF
}

func (r *slotReader) next(seq int64) (slotFrame, error) {
	depth := int64(len(r.table.Slots))
	id := r.table.Slots[seq%depth]
	var missingSince time.Time
	for {
		d, err := r.store.readDescription(r.ctx, id)
		if err != errNotFound {
			missingSince = time.Time{}
		}
		switch {
		case err == errNotFound && seq < depth:
			// This position has not been observed yet.
		case err == errNotFound:
			// Allow a transient 404, but do not poll a vanished slot forever.
			if missingSince.IsZero() {
				missingSince = time.Now()
			} else if time.Since(missingSince) >= slotStallTimeout {
				return slotFrame{}, err
			}
		case err != nil:
			return slotFrame{}, err
		default:
			f, ok := decodeSlot(d)
			if !ok || f.seq%depth != seq%depth {
				return slotFrame{}, errors.New("invalid Drive slot for sequence ", seq)
			}
			if f.seq > seq {
				return slotFrame{}, errors.New("Drive slot ", seq, " was overwritten by sequence ", f.seq)
			}
			if f.seq == seq {
				return f, nil
			}
		}
		if err := sleepContext(r.ctx, r.poll); err != nil {
			return slotFrame{}, err
		}
	}
}

func (r *slotReader) run() error {
	for seq := int64(0); ; seq++ {
		f, err := r.next(seq)
		if err != nil {
			return err
		}
		data := f.data
		switch f.mode {
		case slotEnd:
			return nil
		case slotErr:
			return errors.New("peer could not store segment ", seq)
		case slotMedia:
			id := r.table.Slots[seq%int64(len(r.table.Slots))]
			data, err = r.store.readMedia(r.ctx, id, f)
			if err != nil {
				return err
			}
		}
		select {
		case r.ch <- data:
		case <-r.discard: // Close abandons input, not the peer's ring credit.
		case <-r.ctx.Done():
			return context.Cause(r.ctx)
		}
		d := strconv.FormatInt(seq, 10)
		if seq == 0 {
			err = r.store.createSlot(r.ctx, r.table.Watermark, r.table.name+"/w", d, nil)
		} else {
			err = r.store.setSlot(r.ctx, r.table.Watermark, d, nil)
		}
		if err != nil {
			return errors.New("cannot publish Drive watermark").Base(err)
		}
		if isClosedChan(r.discard) {
			now := time.Now()
			r.drainCredit.Store(&now)
		}
	}
}
