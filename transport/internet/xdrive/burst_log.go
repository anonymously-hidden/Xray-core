package xdrive

import (
	"context"
	"io"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/serial"
)

// Measurement only: nothing here changes what is sent, when, or how often.

const (
	// slotBurstGap is how long one direction of a session stays idle before
	// its burst is summarized. The frames of a live segment follow each other
	// more closely than this, while the request leg between two segments
	// fetched one at a time (an upload, a discovery and the origin fetch) is
	// longer, so such a segment is one burst on each peer.
	slotBurstGap = 1500 * time.Millisecond
	// slotBurstSamples bounds the upload durations a burst keeps for its
	// percentiles; a sustained transfer reports its first ones.
	slotBurstSamples = 256
)

// testSummaryLog, when a test sets it, receives the burst and download lines
// instead of the log.
var testSummaryLog atomic.Pointer[func(line string)]

func logSummary(ctx context.Context, severity log.Severity, parts ...interface{}) {
	if hook := testSummaryLog.Load(); hook != nil {
		(*hook)(serial.Concat(parts...))
		return
	}
	if log.GetSeverity() < severity {
		return
	}
	line := serial.Concat(parts...)
	if severity == log.Severity_Debug {
		errors.LogDebug(ctx, line)
	} else {
		errors.LogInfo(ctx, line)
	}
}

func mbps(bytes int64, d time.Duration) string {
	if d <= 0 {
		return "-1"
	}
	return strconv.FormatFloat(float64(bytes)*8/d.Seconds()/1e6, 'f', 2, 64)
}

// sinceOrUnknown is end-start, or -1 when either is unknown.
func sinceOrUnknown(start, end time.Time) time.Duration {
	if start.IsZero() || end.IsZero() {
		return -1
	}
	return end.Sub(start)
}

func p50Max(samples []time.Duration) (time.Duration, time.Duration) {
	v := slices.Clone(samples)
	slices.Sort(v)
	return percentile(v, .5), percentile(v, 1)
}

// transferMeter counts the media downloads whose bodies are being read, so
// that each one can report how many others shared the link with it.
type transferMeter struct {
	mu     sync.Mutex
	active map[*transferSpan]struct{}
}

type transferSpan struct {
	others int // other downloads in flight when this one began
	peak   int // most other downloads in flight at once while it ran
}

func (m *transferMeter) begin() *transferSpan {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil {
		m.active = make(map[*transferSpan]struct{})
	}
	n := len(m.active)
	for other := range m.active {
		other.peak = max(other.peak, n)
	}
	span := &transferSpan{others: n, peak: n}
	m.active[span] = struct{}{}
	return span
}

func (m *transferMeter) end(span *transferSpan) (others, peak int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.active, span)
	return span.others, span.peak
}

// downloadBody counts the bytes of a media response body. Only the request's
// own goroutine reads it.
type downloadBody struct {
	io.ReadCloser
	span *transferSpan
	n    int64
}

func (b *downloadBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.n += int64(n)
	return n, err
}

// downloadRecord is one media download attempt that got response headers.
type downloadRecord struct {
	path         string
	media        *driveMediaRequest
	status       int
	attempt      int
	bytes        int64
	ttfb         time.Duration // request written to first response byte, -1 unknown
	body         time.Duration // response headers to the end of the body
	dispatch     time.Duration // admission to the end of the body, -1 unknown
	lane         string
	conn         uint64
	fresh        bool
	others, peak int
	head         bool
	err          error
}

func (d downloadRecord) log(ctx context.Context) {
	seq, start, end, rescue := int64(-1), -1, -1, false
	if d.media != nil {
		seq, start, end, rescue = d.media.seq, d.media.start, d.media.end, d.media.rescue
	}
	result := "ok"
	if d.err != nil {
		result = d.err.Error()
	}
	logSummary(ctx, log.Severity_Debug, "xdrive download slot=", slotTag(d.path), " seq=", seq,
		" range=", start, "-", end, " status=", d.status, " attempt=", d.attempt,
		" bytes=", d.bytes, " ttfb=", d.ttfb, " body=", d.body, " dispatch-to-eof=", d.dispatch,
		" mbps=", mbps(d.bytes, d.body), " conn=", d.conn, " new-conn=", d.fresh, " lane=", d.lane,
		" overlap-start=", d.others, " overlap-max=", d.peak, " head=", d.head, " rescue=", rescue,
		" result=", result)
}

// slotLink lets the writer and the reader of one Conn time each other. On a
// client, a read burst ending some time after the write burst that carried
// the request gives request-to-last-byte on one clock.
type slotLink struct {
	mu         sync.Mutex
	writeStart time.Time // first buffered byte of the last write burst
	readLast   time.Time // last frame the reader delivered
}

func (l *slotLink) noteWrite(at time.Time) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.writeStart = at
	l.mu.Unlock()
}

func (l *slotLink) noteRead(at time.Time) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.readLast = at
	l.mu.Unlock()
}

func (l *slotLink) lastWrite() time.Time {
	if l == nil {
		return time.Time{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.writeStart
}

func (l *slotLink) lastRead() time.Time {
	if l == nil {
		return time.Time{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.readLast
}

// slotWriteBurst summarizes the frames a writer cut while it was not idle for
// slotBurstGap. It is guarded by the writer's mu.
type slotWriteBurst struct {
	open       bool
	startAt    time.Time // the burst's first byte entered the write buffer
	firstPutAt time.Time // the first upload left, after any slot reuse wait
	lastCutAt  time.Time // the last frame was cut off the buffer
	doneAt     time.Time // the last upload finished
	afterRead  time.Duration
	firstSeq   int64
	lastSeq    int64
	frames     int
	pending    int
	inline     int
	media      int
	failed     int
	bytes      int64
	inlinePuts []time.Duration
	mediaPuts  []time.Duration
	awaitMax   time.Duration
	permitMax  time.Duration
}

// add counts a frame cut at now from bytes buffered since bufferedAt. It
// returns the previous burst when that one had been idle for slotBurstGap
// before these bytes came, so that it is reported before this one begins.
func (b *slotWriteBurst) add(seq int64, size int, bufferedAt, now time.Time, permitWait time.Duration, link *slotLink) *slotWriteBurst {
	start := bufferedAt
	if start.IsZero() || start.After(now) {
		start = now
	}
	var done *slotWriteBurst
	if b.open && b.pending == 0 && start.Sub(b.doneAt) >= slotBurstGap {
		done = b.take()
	}
	if !b.open {
		*b = slotWriteBurst{open: true, startAt: start, firstSeq: seq, afterRead: -1}
		if read := link.lastRead(); !read.IsZero() && !read.After(start) {
			b.afterRead = start.Sub(read)
		}
		link.noteWrite(start)
	}
	b.frames++
	b.pending++
	b.bytes += int64(size)
	b.lastSeq = seq
	b.lastCutAt = now
	b.permitMax = max(b.permitMax, permitWait)
	return done
}

// finish records the end of the upload of a frame of the open burst.
func (b *slotWriteBurst) finish(mode byte, putAt time.Time, put, await time.Duration, ok bool, now time.Time) {
	if !b.open || b.pending == 0 {
		return
	}
	b.pending--
	b.doneAt = now
	b.awaitMax = max(b.awaitMax, await)
	switch {
	case !ok:
		b.failed++
	case mode == slotInline:
		b.inline++
		if len(b.inlinePuts) < slotBurstSamples {
			b.inlinePuts = append(b.inlinePuts, put)
		}
	default:
		b.media++
		if len(b.mediaPuts) < slotBurstSamples {
			b.mediaPuts = append(b.mediaPuts, put)
		}
	}
	if !putAt.IsZero() && (b.firstPutAt.IsZero() || putAt.Before(b.firstPutAt)) {
		b.firstPutAt = putAt
	}
}

func (b *slotWriteBurst) idle(now time.Time) bool {
	return b.open && b.pending == 0 && now.Sub(b.doneAt) >= slotBurstGap
}

func (b *slotWriteBurst) take() *slotWriteBurst {
	done := *b
	*b = slotWriteBurst{}
	return &done
}

func (b *slotWriteBurst) log(ctx context.Context, watermark string) {
	end := b.doneAt
	if end.IsZero() {
		end = b.lastCutAt
	}
	span := end.Sub(b.startAt)
	inlineP50, inlineMax := p50Max(b.inlinePuts)
	mediaP50, mediaMax := p50Max(b.mediaPuts)
	logSummary(ctx, log.Severity_Info, "xdrive burst write ", slotTag(watermark),
		" seqs=", b.firstSeq, "-", b.lastSeq, " frames=", b.frames,
		" inline=", b.inline, " media=", b.media, " failed=", b.failed, " bytes=", b.bytes,
		" span=", span, " first-put=", sinceOrUnknown(b.startAt, b.firstPutAt),
		" last-cut=", b.lastCutAt.Sub(b.startAt),
		" inline-put-p50/max=", inlineP50, "/", inlineMax,
		" media-put-p50/max=", mediaP50, "/", mediaMax,
		" await-max=", b.awaitMax, " permit-wait-max=", b.permitMax,
		" mbps=", mbps(b.bytes, span), " after-read=", b.afterRead)
}

// slotReadBurst summarizes the frames a reader delivered while it was not
// idle for slotBurstGap. Only the reader's run goroutine uses it.
type slotReadBurst struct {
	open          bool
	startAt       time.Time // the burst's first frame was found
	firstAt       time.Time // first delivery
	lastAt        time.Time // last delivery
	lastFoundAt   time.Time
	writeAt       time.Time // this Conn's last write burst began, if before startAt
	firstSeq      int64
	lastSeq       int64
	frames        int
	inline        int
	media         int
	bytes         int64
	headWaitMax   time.Duration
	firstBlockMax time.Duration
	bodyMax       time.Duration
	gapMax        time.Duration
}

// add counts a frame delivered at now; foundAt is when its description was
// found. Only an idle check closes a burst, see slotReader.run.
func (b *slotReadBurst) add(seq int64, mode byte, size int, foundAt time.Time, headWait, firstBlock, body time.Duration, now time.Time, link *slotLink) {
	if !b.open {
		start := foundAt
		if start.IsZero() || start.After(now) {
			start = now
		}
		*b = slotReadBurst{open: true, startAt: start, firstAt: now, lastAt: now, firstSeq: seq}
		if write := link.lastWrite(); !write.IsZero() && !write.After(start) {
			b.writeAt = write
		}
	}
	b.gapMax = max(b.gapMax, now.Sub(b.lastAt))
	b.lastAt = now
	b.lastSeq = seq
	b.frames++
	if mode == slotInline {
		b.inline++
	} else {
		b.media++
	}
	b.bytes += int64(size)
	if foundAt.After(b.lastFoundAt) {
		b.lastFoundAt = foundAt
	}
	b.headWaitMax = max(b.headWaitMax, headWait)
	b.firstBlockMax = max(b.firstBlockMax, firstBlock)
	b.bodyMax = max(b.bodyMax, body)
	link.noteRead(now)
}

func (b *slotReadBurst) idle(now time.Time) bool {
	return b.open && now.Sub(b.lastAt) >= slotBurstGap
}

func (b *slotReadBurst) take() *slotReadBurst {
	done := *b
	*b = slotReadBurst{}
	return &done
}

func (b *slotReadBurst) log(ctx context.Context, watermark string) {
	span := b.lastAt.Sub(b.startAt)
	logSummary(ctx, log.Severity_Info, "xdrive burst read ", slotTag(watermark),
		" seqs=", b.firstSeq, "-", b.lastSeq, " frames=", b.frames,
		" inline=", b.inline, " media=", b.media, " bytes=", b.bytes,
		" span=", span, " first-delivery=", b.firstAt.Sub(b.startAt),
		" last-found=", sinceOrUnknown(b.startAt, b.lastFoundAt),
		" delivery-gap-max=", b.gapMax, " head-wait-max=", b.headWaitMax,
		" first-block-wait-max=", b.firstBlockMax, " body-max=", b.bodyMax,
		" mbps=", mbps(b.bytes, span), " after-write=", sinceOrUnknown(b.writeAt, b.lastAt))
}
