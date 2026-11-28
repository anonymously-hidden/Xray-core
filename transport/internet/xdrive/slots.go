package xdrive

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/xtls/xray-core/common/errors"
)

const (
	slotMagic        = "XDRVGET1"
	slotHeader       = 64
	slotBlock        = 16 << 10
	slotMaxFrame     = 2 << 20
	slotByteWindow   = 96 << 20
	slotReadAhead    = 24
	slotStallTimeout = time.Minute
)

var (
	slotIDPattern   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	errWriteStopped = errors.New("write stopped")
	errSlotPending  = errors.New("slot not published yet")
	errSlotFrame    = errors.New("invalid media frame")
	blockCRC        = crc32.MakeTable(crc32.Castagnoli)
)

type ring struct {
	Up   slotTable `json:"u"`
	Down slotTable `json:"d"`
	Tick int       `json:"ms"`
}

type slotTable struct {
	Slots     []string `json:"s"`
	Watermark string   `json:"w"`
	name      string
}

func (t slotTable) ids() []string { return append(slices.Clone(t.Slots), t.Watermark) }

func (t slotTable) filename(i int) string {
	if i == len(t.Slots) {
		return t.name + "/w"
	}
	return t.name + "/s" + strconv.Itoa(i)
}

func (s *driveStorage) createRing(ctx context.Context, session string, p params) (ring, string, error) {
	depth := max(2, p.concurrency)
	count := 2*(depth+1) + 1 // Both slot tables, their watermarks and the announcement.
	var batch struct{ IDs []string }
	target := fmt.Sprintf("%s/generateIds?count=%d&space=%s", filesURL, count, s.space)
	if err := s.getJSON(ctx, metadataLane, target, 256<<10, &batch, nil); err != nil {
		return ring{}, "", err
	}
	ids := batch.IDs
	seen := make(map[string]bool, count)
	for _, id := range ids {
		if slotIDPattern.MatchString(id) {
			seen[id] = true
		}
	}
	if len(ids) != count || len(seen) != count {
		return ring{}, "", errors.New("invalid Drive ID batch")
	}
	table := func(batch []string, name string) slotTable { return slotTable{batch[:depth:depth], batch[depth], name} }
	r := ring{table(ids, uplinkPrefix(session)), table(ids[depth+1:], downlinkPrefix(session)), int(p.flushInterval.Milliseconds())}
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(16)
	for _, t := range []slotTable{r.Up, r.Down} {
		for i, id := range t.ids() {
			g.Go(func() error {
				if i == depth {
					return s.writeFile(ctx, id, t.filename(i), "-1", nil, nil)
				}
				return s.writeFile(ctx, id, t.filename(i), "INIT", frameBody('I', -1, nil), nil)
			})
		}
	}
	return r, ids[len(ids)-1], g.Wait()
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

func (c *Conn) markProgress() { c.progress.Store(new(time.Now())) }

func (c *Conn) failOn(err error) {
	if err != nil {
		c.fail(err)
	}
}

type slotDirection struct {
	*Conn
	store *driveStorage
	table slotTable
	params
}

func (d slotDirection) id(seq int64) string { return d.table.Slots[seq%int64(len(d.table.Slots))] }

func (d slotDirection) remaining(last time.Time) time.Duration {
	return max(slotStallTimeout, d.sessionTTL) - max(0, d.store.sinceCooldown(last))
}

func wireSize(size int) int { return size + max(1, (size+slotBlock-1)/slotBlock)*slotHeader }

// Every block, including the first block of a resumed range, identifies its
// generation and byte offset. IDLE, EMPTY, READ and FIN occupy exactly 64 bytes.
func frameBody(kind byte, seq int64, payload []byte) []byte {
	wire := make([]byte, wireSize(len(payload)))
	for offset, pos := 0, 0; pos < len(wire); {
		block := payload[offset:min(offset+slotBlock, len(payload))]
		h := wire[pos : pos+slotHeader]
		copy(h, slotMagic)
		h[8] = kind
		binary.BigEndian.PutUint64(h[9:], uint64(seq))
		binary.BigEndian.PutUint32(h[17:], uint32(len(payload)))
		binary.BigEndian.PutUint32(h[21:], uint32(offset))
		binary.BigEndian.PutUint32(h[25:], crc32.Checksum(block, blockCRC))
		copy(wire[pos+slotHeader:], block)
		offset += len(block)
		pos += slotHeader + len(block)
	}
	return wire
}

type slotWriter struct {
	slotDirection
	input          chan []byte
	lock, done     chan struct{}
	acked, started atomic.Int64
}

func newSlotWriter(d slotDirection) *slotWriter {
	w := &slotWriter{slotDirection: d, input: make(chan []byte), lock: make(chan struct{}, 1), done: make(chan struct{})}
	w.acked.Store(-1)
	w.started.Store(-1)
	go w.run()
	return w
}

func (w *slotWriter) writeUntil(p []byte, deadline, closing <-chan struct{}) (int, error) {
	if err := sendUntil(w.ctx, w.lock, struct{}{}, deadline, closing); err != nil {
		return 0, err
	}
	defer func() { <-w.lock }()
	n := 0
	for chunk := range slices.Chunk(p, min(w.segmentBytes, 64<<10)) {
		if err := sendUntil(w.ctx, w.input, bytes.Clone(chunk), deadline, closing); err != nil {
			return n, err
		}
		n += len(chunk)
	}
	return n, nil
}

func (w *slotWriter) pollCredit(ctx context.Context) {
	for sleepContext(ctx, w.store.cadence(300*time.Millisecond)) == nil {
		f, err := w.store.readFile(ctx, w.table.Watermark, "description")
		if err == nil {
			n, err := strconv.ParseInt(f.Description, 10, 64)
			if err != nil || n < -1 || n > w.started.Load() {
				w.fail(errCredit)
				return
			}
			w.acked.Store(max(n, w.acked.Load()))
		} else if isAuthError(err) {
			w.fail(err)
			return
		}
	}
}

func (w *slotWriter) run() {
	defer close(w.done)
	writes, ctx := errgroup.WithContext(w.ctx)
	writes.SetLimit(32)
	var poller sync.WaitGroup
	defer poller.Wait()
	defer func() { w.failOn(writes.Wait()) }() // Wait cancels the credit poller.
	poller.Go(func() { w.pollCredit(ctx) })
	tick := time.NewTicker(w.flushInterval)
	defer tick.Stop()
	depth := int64(len(w.table.Slots))
	through := make([]int64, depth)
	fences := make([]chan struct{}, depth)
	var buf bytes.Buffer
	var sent, credited int64
	lastAck := int64(-1)
	lastProgress := time.Now()
	closing := w.closing
	finished := false
	for ctx.Err() == nil {
		var input chan []byte
		if buf.Len() < w.segmentBytes && closing != nil {
			input = w.input
		}
		select {
		case <-ctx.Done():
			return
		case <-closing:
			closing = nil
			continue
		case b := <-input:
			buf.Write(b)
			continue
		case <-tick.C:
		}
		cadence := w.store.cadence(w.flushInterval)
		tick.Reset(cadence)
		if ack := w.acked.Load(); ack > lastAck {
			lastAck, credited, lastProgress = ack, through[ack%depth], time.Now()
			if closing == nil {
				w.markProgress()
			}
		}
		seq := w.started.Load() + 1
		pos := seq % depth
		room := slotByteWindow - (sent - credited)
		if finished && lastAck == seq-1 {
			return
		}
		slowPeer := cadence > w.flushInterval || time.Since(lastProgress) >= 2*time.Second
		idleAhead := slowPeer && closing != nil && buf.Len() == 0 && seq-lastAck > 8
		blocked := seq-depth > lastAck || fences[pos] != nil && !isClosedChan(fences[pos]) || room == 0 && buf.Len() > 0
		if finished || blocked || idleAhead || w.store.cooling() {
			if w.remaining(lastProgress) <= 0 {
				w.fail(errors.New("timed out waiting for slot reuse"))
				return
			}
			continue
		}
		n := min(buf.Len(), w.segmentBytes, int(room))
		kind := byte('D')
		switch {
		case n == 0 && closing == nil:
			kind, finished = 'F', true
		case n == 0:
			kind = 'E'
		}
		wire := frameBody(kind, seq, buf.Next(n))
		sent += int64(n)
		stored := make(chan struct{})
		fences[pos], through[pos] = stored, sent
		w.started.Store(seq)
		writes.Go(func() error {
			defer close(stored)
			desc := fmt.Sprintf("%c:%d", kind, seq)
			return w.store.writeFile(ctx, w.id(seq), "", desc, wire, func() bool { return w.acked.Load() >= seq })
		})
	}
}

func (w *slotWriter) Close() error {
	<-w.done
	return context.Cause(w.ctx)
}

type slotReader struct {
	slotDirection
	ch   chan []byte
	done chan struct{}
}

type slotRead struct {
	seq      int64
	data     chan []byte
	head     chan struct{}
	promoted time.Time // Set before closing head.
	kind     byte
	size     int
	hit      time.Time // Local start of the GET that first observed this generation.
	miss     time.Time // Start of the last GET which saw an older generation.
	err      error
}

func (r *slotReader) chunks() <-chan []byte { return r.ch }
func (r *slotReader) wait()                 { <-r.done }
func (r *slotReader) Err() error            { return cmp.Or(context.Cause(r.ctx), io.EOF) }
func (r *slotReader) Wake()                 {} // Predictive reads run independently of local writes.

// pos is the next undelivered payload byte. A 200 fallback may repeat the
// beginning of the frame; verify those blocks but never emit them again.
func (r *slotReader) get(ctx context.Context, j *slotRead, read *mediaRead, pos *int, end int, out chan<- []byte) error {
	start := *pos
	read.byteRange = ""
	if end > 0 {
		read.byteRange = fmt.Sprintf("bytes=%d-%d", start/slotBlock*(slotHeader+slotBlock), wireSize(end)-1)
		read.size = end - start
	}
	issued := time.Now()
	read.consume = func(resp *http.Response) error {
		at := 0
		switch resp.StatusCode {
		case http.StatusPartialContent:
			if end <= 0 || resp.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", start/slotBlock*(slotHeader+slotBlock), wireSize(end)-1, wireSize(j.size)) {
				return errDriveRange
			}
			at = start
		case http.StatusOK:
		default:
			return errSlotFrame
		}
		var h [slotHeader]byte
		for {
			if _, err := io.ReadFull(resp.Body, h[:]); err != nil {
				return err
			}
			if string(h[:8]) != slotMagic {
				return errSlotFrame
			}
			seq := int64(binary.BigEndian.Uint64(h[9:]))
			kind, size := h[8], int(binary.BigEndian.Uint32(h[17:]))
			if seq < j.seq && (seq >= 0 || seq == -1 && kind == 'I') {
				return errSlotPending
			}
			if seq != j.seq || uint(size) > slotMaxFrame || binary.BigEndian.Uint32(h[21:]) != uint32(at) {
				return errSlotFrame
			}
			if !(kind == 'D' && size > 0 || (kind == 'E' || kind == 'F') && size == 0) {
				return errSlotFrame
			}
			if j.kind == 0 {
				j.kind, j.size, j.hit = kind, size, issued
			} else if kind != j.kind || size != j.size {
				return errSlotFrame
			}
			stop := cmp.Or(end, size)
			block := make([]byte, min(slotBlock, size-at))
			if _, err := io.ReadFull(resp.Body, block); err != nil {
				return err
			}
			if crc32.Checksum(block, blockCRC) != binary.BigEndian.Uint32(h[25:]) {
				return errSlotFrame
			}
			at += len(block)
			if at == stop && (at == size || resp.StatusCode == http.StatusPartialContent) {
				if _, err := io.ReadFull(resp.Body, h[:1]); err != io.EOF {
					return cmp.Or[error](err, errSlotFrame)
				}
			}
			if at > *pos {
				out <- block // Each queue fits every block in its assigned span.
				*pos = at
			}
			if at == stop {
				return nil
			}
		}
	}
	_, err := r.store.getMedia(context.WithValue(ctx, mediaReadKey{}, read), r.id(j.seq), 0)
	return err
}

func (r *slotReader) fetch(ctx context.Context, j *slotRead) error {
	ctx = context.WithValue(ctx, mediaHeadKey{}, (<-chan struct{})(j.head))
	ctx = context.WithValue(ctx, spanRescueKey{}, func() bool { return isClosedChan(j.head) && !r.store.cooling() })
	pos := 0
	var read mediaRead
	for backoff := r.flushInterval / 4; ; backoff = min(backoff*2, 400*time.Millisecond) {
		issued := time.Now()
		err := r.get(ctx, j, &read, &pos, 0, j.data)
		if err == nil || err == errSlotFrame || isAuthError(err) {
			return err
		}
		if j.kind == 'D' { // Resume the verified prefix without metadata discovery.
			break
		}
		if isClosedChan(j.head) && r.remaining(j.promoted) <= 0 {
			return errors.New("missing media sequence ", j.seq).Base(err)
		}
		if !j.miss.IsZero() && waitContext(ctx, j.head) != nil {
			return ctx.Err()
		}
		if err == errSlotPending || err == errNotFound {
			j.miss = issued
		}
		if sleepContext(ctx, r.store.cadence(backoff)) != nil {
			return ctx.Err()
		}
	}
	resume := func(ctx context.Context, from, to int, read mediaRead, out chan<- []byte) error {
		for attempt := 1; ; attempt++ {
			err := r.get(ctx, j, &read, &from, to, out)
			if err == nil || err == errSlotFrame || isAuthError(err) || attempt == driveMaxAttempts {
				return err
			}
			if sleepContext(ctx, driveInitialBackoff) != nil {
				return ctx.Err()
			}
		}
	}
	// Resume disjoint ranges in parallel, even without a readable successor.
	mid := (pos + (j.size-pos)/2) / slotBlock * slotBlock
	if mid <= pos || j.size-pos <= 256<<10 {
		return resume(ctx, pos, j.size, read, j.data)
	}
	tail := make(chan []byte, (j.size-mid+slotBlock-1)/slotBlock)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return resume(ctx, mid, j.size, read, tail) })
	g.Go(func() error { return resume(ctx, pos, mid, read, j.data) })
	if err := g.Wait(); err != nil {
		return err
	}
	close(tail)
	for b := range tail {
		j.data <- b
	}
	return nil
}

// Drain the ordered delivery queue in batches. Every clear in a batch must
// have committed before its watermark can free even one slot for the next lap.
func (r *slotReader) receipts(in <-chan int64) error {
	for first := range in {
		last := first
		for range len(in) {
			last = <-in
		}
		g, ctx := errgroup.WithContext(r.ctx)
		g.SetLimit(16)
		for n := first; n <= last; n++ {
			g.Go(func() error {
				return r.store.writeFile(ctx, r.id(n), "", fmt.Sprintf("READ:%d", n), frameBody('R', n, nil), nil)
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		if err := r.store.writeFile(r.ctx, r.table.Watermark, "", strconv.FormatInt(last, 10), nil, nil); err != nil {
			return err
		}
	}
	return nil
}

func (r *slotReader) run() {
	defer close(r.done)
	defer close(r.ch)
	ctx, cancel := context.WithCancel(r.ctx)
	var workers sync.WaitGroup
	defer workers.Wait()
	acks := make(chan int64, len(r.table.Slots))
	workers.Go(func() { r.failOn(r.receipts(acks)) })
	defer close(acks)
	defer cancel()
	width := min(slotReadAhead, len(r.table.Slots))
	jobs := make(chan *slotRead, width-1)
	var phase atomic.Int64
	origin := time.Now()
	workers.Go(func() {
		defer close(jobs)
		for n := int64(0); ctx.Err() == nil; n++ {
			due := origin.Add(time.Duration(n)*r.flushInterval + time.Duration(phase.Load()))
			if sleepContext(ctx, time.Until(due)) != nil {
				return
			}
			j := &slotRead{seq: n, data: make(chan []byte, slotMaxFrame/slotBlock), head: make(chan struct{})}
			if sendUntil(ctx, jobs, j, nil, nil) != nil {
				return
			}
			workers.Go(func() {
				defer close(j.data)
				j.err = r.fetch(ctx, j)
			})
			if r.store.cadence(0) > r.flushInterval && waitContext(ctx, j.head) != nil {
				return // One speculative GET while the request budget is tight.
			}
		}
	})
	var misses int
	var adjusted time.Time
	for j := range jobs {
		j.promoted = time.Now()
		close(j.head) // Promotion never cancels a progressing request.
		for b := range j.data {
			if err := sendUntil(ctx, r.ch, b, nil, r.closing); err != nil && ctx.Err() != nil {
				return
			}
			if isClosedChan(r.closing) {
				r.markProgress()
			}
		}
		if j.err != nil {
			r.fail(j.err)
			return
		}
		// Probe slightly earlier on hits. Re-anchor after consecutive stale
		// generations, using local GET start times (no synchronized clocks).
		if !j.miss.IsZero() {
			misses++
		} else {
			misses = 0
			phase.Add(-int64(r.flushInterval / 16))
		}
		if misses >= 2 && time.Since(adjusted) >= time.Second {
			visible := j.miss.Add(j.hit.Sub(j.miss) / 2)
			phase.Store(int64(visible.Sub(origin) - time.Duration(j.seq)*r.flushInterval))
			adjusted = time.Now()
			misses = 0
		}
		if sendUntil(ctx, acks, j.seq, nil, nil) != nil || j.kind == 'F' {
			return
		}
	}
}
