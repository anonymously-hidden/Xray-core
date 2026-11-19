package xdrive

import (
	"context"
	"encoding/json"
	"mime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// timing kinds for the per-minute Drive request breakdown.
const (
	kindCreate = iota
	kindSlotInline
	kindSlotMedia
	kindWatermark
	kindHeadRead
	kindOtherRead
	kindBodyRead
	kindDelete
	kindList
	kindToken
	kindCount
)

var kindNames = [kindCount]string{
	"create", "slot-write-inline", "slot-write-media", "watermark-write",
	"head-read", "other-read", "body-read", "delete", "list", "token-fetch",
}

// timingSample is the per-attempt breakdown. Parts the transport cannot
// observe stay zero and are reported as unavailable via the has* flags.
type timingSample struct {
	total     time.Duration
	waitCool  time.Duration
	waitPool  time.Duration
	waitToken time.Duration
	waitConn  time.Duration
	dialTLS   time.Duration
	send      time.Duration
	server    time.Duration
	body      time.Duration
	reused    bool
	newConn   bool
	hedge     bool
	won       bool
	hasConn   bool
	hasDial   bool
	hasSend   bool
	hasServer bool
}

const (
	timingWindow      = 60 * time.Second
	timingRing        = 512
	slowRequestTiming = 2 * time.Second
)

type timingAggregator struct {
	mu         sync.Mutex
	started    bool
	stopOnce   sync.Once
	done       chan struct{}
	n          [kindCount]int
	pos        [kindCount]int
	ring       [kindCount][timingRing]timingSample
	conns      [kindCount]int
	hedges     [kindCount]int
	won        [kindCount]int
	requests   [kindCount]int
	failed     [kindCount]int
	canceled   [kindCount]int
	rateLimits [kindCount]int
	deferred   [kindCount]int
}

// Transport/body failures and cancelled attempts often have no latency sample.
// Keep their counters outside the reservoir so low percentiles cannot hide
// discarded API work.
func (a *timingAggregator) noteFailure(kind int, canceled bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if canceled {
		a.canceled[kind]++
	} else {
		a.failed[kind]++
	}
}

// Only a confirmed 429 or rate-limit 403 counts, not an arbitrary 403 or timeout.
func (a *timingAggregator) noteRateLimit(kind int) {
	a.mu.Lock()
	a.rateLimits[kind]++
	a.mu.Unlock()
}

// Count every sent API attempt, including retries and cancelled hedges. The
// latency reservoir is capped at 512 samples; it is not a request counter.
func (a *timingAggregator) request(kind int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started {
		a.started = true
		go a.loop()
	}
	a.requests[kind]++
}

func driveQuotaCost(kind int) int {
	switch kind {
	case kindToken:
		return 0
	case kindCreate, kindSlotInline, kindSlotMedia, kindWatermark, kindDelete:
		return 50
	case kindBodyRead:
		return 200
	case kindList:
		return 100
	default:
		return 5
	}
}

func newTimingAggregator() *timingAggregator {
	return &timingAggregator{done: make(chan struct{})}
}

func (a *timingAggregator) record(kind int, s timingSample) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started {
		a.started = true
		go a.loop()
	}
	i := a.pos[kind]
	a.ring[kind][i] = s
	a.pos[kind] = (i + 1) % timingRing
	if a.n[kind] < timingRing {
		a.n[kind]++
	}
	if s.newConn {
		a.conns[kind]++
	}
	if s.hedge {
		a.hedges[kind]++
	}
	if s.won {
		a.won[kind]++
	}
}

func (a *timingAggregator) stop() {
	a.stopOnce.Do(func() { close(a.done) })
}

// noteHedge counts a started hedge (and a win) without adding a traffic sample.
func (a *timingAggregator) noteHedge(kind int, won bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hedges[kind]++
	if won {
		a.won[kind]++
	}
}

// Read hedges are counted at dispatch, with the win published later.
func (a *timingAggregator) noteHedgeWin(kind int) {
	a.mu.Lock()
	a.won[kind]++
	a.mu.Unlock()
}

// A deferral is an expired nonhead recovery opportunity, not a saved API call:
// promotion may later admit it, and ordinary quota/budget checks still apply.
func (a *timingAggregator) noteDeferredHedge(kind int) {
	a.mu.Lock()
	a.deferred[kind]++
	a.mu.Unlock()
}

func (a *timingAggregator) loop() {
	t := time.NewTicker(timingWindow)
	defer t.Stop()
	for {
		select {
		case <-a.done:
			return
		case <-t.C:
			a.flush()
		}
	}
}

func percentile(v []time.Duration, p float64) time.Duration {
	if len(v) == 0 {
		return -1
	}
	return v[min(int(p*float64(len(v))), len(v)-1)]
}

func dialEnd(tlsDone, connDone time.Time) time.Time {
	if !tlsDone.IsZero() {
		return tlsDone
	}
	return connDone
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// recordTiming records one attempt sample of a classified request.
func (s *driveStorage) recordTiming(kind int, sample timingSample) {
	if s.timing == nil {
		return
	}
	s.timing.record(kind, sample)
}

// Immediate slow-attempt detail complements the capped minute sample, including
// bursts near the end of an uploaded log. It never issues another API request.
func (s *driveStorage) logSlowRequest(ctx context.Context, kind int, path string, sample timingSample, media *driveMediaRequest, headAtDispatch, headAtCompletion bool, status, attempt int) {
	if kind < 0 || kind >= kindCount || sample.total < slowRequestTiming {
		return
	}
	send, server, connection := time.Duration(-1), time.Duration(-1), time.Duration(-1)
	if sample.hasSend {
		send = sample.send
	}
	if sample.hasServer {
		server = sample.server
	}
	if sample.hasConn {
		connection = sample.waitConn
	}
	if media != nil {
		errors.LogDebug(ctx, "xdrive slow timing kind=", kindNames[kind], " slot=", slotTag(path),
			" status=", status, " attempt=", attempt,
			" seq=", media.seq, " range=", media.start, "-", media.end,
			" frame-bytes=", media.size, " requested-bytes=", media.end-media.start,
			" total=", sample.total, " pool=", sample.waitPool, " cool=", sample.waitCool,
			" token=", sample.waitToken, " connection=", connection, " send=", send,
			" server=", server, " body=", sample.body, " rescue=", media.rescue,
			" head-at-dispatch=", headAtDispatch, " head-at-completion=", headAtCompletion)
		return
	}
	errors.LogDebug(ctx, "xdrive slow timing kind=", kindNames[kind], " slot=", slotTag(path),
		" status=", status, " attempt=", attempt,
		" total=", sample.total, " pool=", sample.waitPool, " cool=", sample.waitCool,
		" token=", sample.waitToken, " connection=", connection, " send=", send,
		" server=", server, " body=", sample.body)
}

type timingBatch struct {
	samples                      []timingSample
	requests, conns, hedges, won int
	failed, canceled, rateLimits int
	deferred                     int
}

func (a *timingAggregator) snapshot() [kindCount]timingBatch {
	a.mu.Lock()
	defer a.mu.Unlock()
	var batches [kindCount]timingBatch
	for k := range batches {
		batches[k] = timingBatch{
			samples:  append([]timingSample(nil), a.ring[k][:a.n[k]]...),
			requests: a.requests[k], conns: a.conns[k], hedges: a.hedges[k], won: a.won[k],
			failed: a.failed[k], canceled: a.canceled[k], rateLimits: a.rateLimits[k],
			deferred: a.deferred[k],
		}
		a.n[k], a.pos[k], a.conns[k], a.hedges[k], a.won[k], a.requests[k] = 0, 0, 0, 0, 0, 0
		a.failed[k], a.canceled[k], a.rateLimits[k] = 0, 0, 0
		a.deferred[k] = 0
	}
	return batches
}

func (a *timingAggregator) flush() {
	for k, batch := range a.snapshot() {
		n := len(batch.samples)
		if n == 0 && batch.requests == 0 && batch.hedges == 0 && batch.won == 0 && batch.failed == 0 && batch.canceled == 0 && batch.rateLimits == 0 && batch.deferred == 0 {
			continue
		}
		total := make([]time.Duration, 0, n)
		pool := make([]time.Duration, 0, n)
		cool := make([]time.Duration, 0, n)
		token := make([]time.Duration, 0, n)
		conn := make([]time.Duration, 0, n)
		dial := make([]time.Duration, 0, n)
		send := make([]time.Duration, 0, n)
		server := make([]time.Duration, 0, n)
		body := make([]time.Duration, 0, n)
		for _, s := range batch.samples {
			total = append(total, s.total)
			pool = append(pool, s.waitPool)
			cool = append(cool, s.waitCool)
			token = append(token, s.waitToken)
			if s.hasConn {
				conn = append(conn, s.waitConn)
			}
			if s.hasDial {
				dial = append(dial, s.dialTLS)
			}
			if s.hasSend {
				send = append(send, s.send)
			}
			if s.hasServer {
				server = append(server, s.server)
			}
			body = append(body, s.body)
		}
		sort.Slice(total, func(i, j int) bool { return total[i] < total[j] })
		sort.Slice(pool, func(i, j int) bool { return pool[i] < pool[j] })
		sort.Slice(cool, func(i, j int) bool { return cool[i] < cool[j] })
		sort.Slice(token, func(i, j int) bool { return token[i] < token[j] })
		sort.Slice(conn, func(i, j int) bool { return conn[i] < conn[j] })
		sort.Slice(dial, func(i, j int) bool { return dial[i] < dial[j] })
		sort.Slice(send, func(i, j int) bool { return send[i] < send[j] })
		sort.Slice(server, func(i, j int) bool { return server[i] < server[j] })
		sort.Slice(body, func(i, j int) bool { return body[i] < body[j] })
		errors.LogInfo(context.Background(), "xdrive timing ", kindNames[k],
			" n=", n,
			" requests=", batch.requests, " units=", batch.requests*driveQuotaCost(k),
			" failed=", batch.failed, " canceled=", batch.canceled, " rate-limits=", batch.rateLimits,
			" total p50/p95/max ", percentile(total, .5), "/", percentile(total, .95), "/", percentile(total, 1),
			" wait-pool p50 ", percentile(pool, .5),
			" wait-cool p50 ", percentile(cool, .5),
			" wait-token p50 ", percentile(token, .5),
			" wait-conn p50 ", percentile(conn, .5),
			" dial+tls p50 ", percentile(dial, .5),
			" server p50 ", percentile(server, .5),
			" body p50 ", percentile(body, .5),
			" newconns=", batch.conns, " hedges=", batch.hedges, " won=", batch.won,
			" hedge-deferrals=", batch.deferred,
			" send-p50/p95/max=", percentile(send, .5), "/", percentile(send, .95), "/", percentile(send, 1),
			" pool-p95/max=", percentile(pool, .95), "/", percentile(pool, 1),
			" connection-p95/max=", percentile(conn, .95), "/", percentile(conn, 1),
			" server-p95/max=", percentile(server, .95), "/", percentile(server, 1),
			" body-p95/max=", percentile(body, .95), "/", percentile(body, 1))
	}
}

// classifyDriveRequest maps a request to a timing kind.
func classifyDriveRequest(ctx context.Context, method, target, contentType string, body []byte) int {
	switch method {
	case "DELETE":
		return kindDelete
	case "POST":
		return kindCreate
	case "PATCH":
		mediaType, _, err := mime.ParseMediaType(contentType)
		if err == nil && mediaType == "multipart/related" {
			return kindSlotMedia
		}
		if isWatermarkBody(body) {
			return kindWatermark
		}
		return kindSlotInline
	case "GET":
		switch {
		case strings.Contains(target, "alt=media"):
			return kindBodyRead
		case strings.Contains(target, "q="):
			return kindList
		case ctx.Value(laneKey{}) != nil:
			return kindHeadRead
		default:
			return kindOtherRead
		}
	}
	return kindOtherRead
}

// isWatermarkBody reports whether a PATCH metadata body carries a plain
// integer description (a watermark ack) rather than a slot frame.
func isWatermarkBody(body []byte) bool {
	// A watermark body is {"description":"<integer>"}: well under 100 bytes.
	// Inline slot frames are far larger; do not JSON-decode them just to
	// label a timing line.
	if len(body) > 128 {
		return false
	}
	var doc struct {
		Description string `json:"description"`
	}
	if json.Unmarshal(body, &doc) != nil || doc.Description == "" {
		return false
	}
	for _, c := range doc.Description {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
