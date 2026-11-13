package xdrive

import (
	"context"
	"encoding/json"
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
	server    time.Duration
	body      time.Duration
	reused    bool
	newConn   bool
	hedge     bool
	won       bool
	hasConn   bool
	hasDial   bool
	hasServer bool
}

const (
	timingWindow = 60 * time.Second
	timingRing   = 512
)

type timingAggregator struct {
	mu       sync.Mutex
	started  bool
	stopOnce sync.Once
	done     chan struct{}
	n        [kindCount]int
	pos      [kindCount]int
	ring     [kindCount][timingRing]timingSample
	conns    [kindCount]int
	hedges   [kindCount]int
	won      [kindCount]int
	requests [kindCount]int
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

func (a *timingAggregator) flush() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for k := 0; k < kindCount; k++ {
		n := a.n[k]
		if n == 0 && a.requests[k] == 0 && a.hedges[k] == 0 {
			continue
		}
		total := make([]time.Duration, 0, n)
		pool := make([]time.Duration, 0, n)
		conn := make([]time.Duration, 0, n)
		dial := make([]time.Duration, 0, n)
		server := make([]time.Duration, 0, n)
		body := make([]time.Duration, 0, n)
		for i := 0; i < n; i++ {
			s := a.ring[k][i]
			total = append(total, s.total)
			pool = append(pool, s.waitPool)
			if s.hasConn {
				conn = append(conn, s.waitConn)
			}
			if s.hasDial {
				dial = append(dial, s.dialTLS)
			}
			if s.hasServer {
				server = append(server, s.server)
			}
			body = append(body, s.body)
		}
		sort.Slice(total, func(i, j int) bool { return total[i] < total[j] })
		sort.Slice(pool, func(i, j int) bool { return pool[i] < pool[j] })
		sort.Slice(conn, func(i, j int) bool { return conn[i] < conn[j] })
		sort.Slice(dial, func(i, j int) bool { return dial[i] < dial[j] })
		sort.Slice(server, func(i, j int) bool { return server[i] < server[j] })
		sort.Slice(body, func(i, j int) bool { return body[i] < body[j] })
		errors.LogInfo(context.Background(), "xdrive timing ", kindNames[k],
			" n=", n,
			" requests=", a.requests[k], " units=", a.requests[k]*driveQuotaCost(k),
			" total p50/p95/max ", percentile(total, .5), "/", percentile(total, .95), "/", percentile(total, 1),
			" wait-pool p50 ", percentile(pool, .5),
			" wait-conn p50 ", percentile(conn, .5),
			" dial+tls p50 ", percentile(dial, .5),
			" server p50 ", percentile(server, .5),
			" body p50 ", percentile(body, .5),
			" newconns=", a.conns[k], " hedges=", a.hedges[k], " won=", a.won[k])
		a.n[k] = 0
		a.pos[k] = 0
		a.conns[k] = 0
		a.hedges[k] = 0
		a.won[k] = 0
		a.requests[k] = 0
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
		if contentType == "multipart/related; boundary="+driveBoundary {
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
