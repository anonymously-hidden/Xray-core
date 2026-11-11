package xdrive

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
)

// A real driveStorage over an in-memory HTTP transport. All request attempts
// (including misses, polling, ACKs, token and ID requests) are counted. Every
// upload AND download, including inline base64 metadata, shares one constrained
// FIFO byte budget. RPC delays overlap; the bandwidth budget does not. No OS
// sockets or credential files are used, so the whole workload uses fake time.
type browsingDrive struct {
	mu                    sync.Mutex
	files                 map[string]browsingFile
	next                  int
	counts                map[string]int
	frames, frameBytes    int
	downFrames, downBytes int
	hits                  int
	link                  chan *browsingTransfer
	linkCancel            context.CancelFunc
	linkDone              chan struct{}
	rate                  int
	writeDelay            time.Duration
}

type browsingFile struct {
	name, description string
	body              []byte
}

type browsingTransfer struct {
	ctx  context.Context
	at   time.Time
	key  string
	n    int
	done chan error
}

func (m *browsingDrive) runLink(ctx context.Context) {
	defer close(m.linkDone)
	var pending []*browsingTransfer
	for {
		if len(pending) == 0 {
			select {
			case p := <-m.link:
				pending = append(pending, p)
			case <-ctx.Done():
				return
			}
		}
		// Collect simultaneous arrivals before choosing a stable tie-breaker;
		// goroutine launch order must not determine who owns the fake bandwidth.
		if regressionDelay(ctx, time.Microsecond) != nil {
			return
		}
	collect:
		for {
			select {
			case p := <-m.link:
				pending = append(pending, p)
			default:
				break collect
			}
		}
		slices.SortFunc(pending, func(a, b *browsingTransfer) int {
			if !a.at.Equal(b.at) {
				if a.at.Before(b.at) {
					return -1
				}
				return 1
			}
			return strings.Compare(a.key, b.key)
		})
		p := pending[0]
		pending = pending[1:]
		// Cancellation releases the link immediately, rather than charging an
		// entire losing hedge's body after it has already been cancelled.
		err := p.ctx.Err()
		if err == nil {
			err = regressionDelay(p.ctx, time.Duration(p.n)*time.Second/time.Duration(m.rate))
		}
		p.done <- err
	}
}

func (m *browsingDrive) transfer(ctx context.Context, key string, n int) error {
	p := &browsingTransfer{ctx: ctx, at: time.Now(), key: key, n: n, done: make(chan error, 1)}
	select {
	case m.link <- p:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-p.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *browsingDrive) close() {
	m.linkCancel()
	<-m.linkDone
}

func browsingReply(status int, body []byte) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}
}

func (m *browsingDrive) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host != "browsing.invalid" {
		return nil, fmt.Errorf("unexpected test URL %s", r.URL)
	}
	var raw []byte
	if r.Body != nil {
		raw, _ = io.ReadAll(r.Body)
		r.Body.Close()
	}
	var meta struct{ ID, Name, Description string }
	var body []byte
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/") {
		_, p, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			return nil, err
		}
		parts := multipart.NewReader(bytes.NewReader(raw), p["boundary"])
		a, err := parts.NextPart()
		if err != nil {
			return nil, err
		}
		if err := json.NewDecoder(a).Decode(&meta); err != nil {
			return nil, err
		}
		b, err := parts.NextPart()
		if err != nil {
			return nil, err
		}
		body, err = io.ReadAll(b)
		if err != nil {
			return nil, err
		}
	} else if len(raw) > 0 && r.URL.Path != "/token" {
		if err := json.Unmarshal(raw, &meta); err != nil {
			return nil, err
		}
	}
	id := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/files/"), "/upload/")
	kind := "description"
	delay := 5 * time.Millisecond
	switch {
	case r.URL.Path == "/token":
		kind = "token"
	case strings.HasSuffix(r.URL.Path, "/generateIds"):
		kind = "IDs"
	case r.Method == http.MethodPost || r.Method == http.MethodPatch:
		delay = m.writeDelay
		if f, ok := decodeSlot(meta.Description); ok {
			if f.mode == slotMedia {
				kind = "mediaWrite"
			} else {
				kind = "inlineWrite"
			}
		} else if strings.HasPrefix(meta.Name, "sessions~") {
			kind = "announce"
		} else if meta.Description != "" {
			kind = "ACK"
		} else {
			kind = "create"
		}
	case r.URL.Query().Get("alt") == "media":
		kind = "body"
	case r.Method == http.MethodDelete:
		kind = "delete"
	case r.URL.Path == "/files":
		kind = "list"
	default:
		m.mu.Lock()
		if strings.HasSuffix(m.files[id].name, "~w") {
			kind = "watermark"
		}
		m.mu.Unlock()
	}
	m.mu.Lock()
	m.counts[kind]++
	m.mu.Unlock()
	if err := m.transfer(r.Context(), r.URL.String()+":request", len(raw)+256); err != nil {
		return nil, err
	}
	if err := regressionDelay(r.Context(), delay); err != nil {
		return nil, err
	}
	m.mu.Lock()
	status := http.StatusOK
	var reply any = map[string]string{"id": id}
	var payload []byte
	switch kind {
	case "token":
		reply = map[string]any{"access_token": "fake", "expires_in": 3600}
	case "IDs":
		n, _ := strconv.Atoi(r.URL.Query().Get("count"))
		ids := make([]string, n)
		for i := range ids {
			m.next++
			ids[i] = fmt.Sprintf("id-%d", m.next)
		}
		reply = map[string]any{"ids": ids}
	case "list":
		reply = map[string]any{"files": []any{}}
	case "delete":
		delete(m.files, id)
	case "body":
		f, ok := m.files[id]
		if !ok {
			status = http.StatusNotFound
		}
		payload = bytes.Clone(f.body)
	case "description", "watermark":
		f, ok := m.files[id]
		if !ok {
			status = http.StatusNotFound
		}
		if _, valid := decodeSlot(f.description); valid {
			m.hits++
		}
		reply = map[string]string{"description": f.description}
	default:
		if r.Method == http.MethodPost {
			id = meta.ID
			if id == "" {
				m.next++
				id = fmt.Sprintf("id-%d", m.next)
			}
		}
		f, ok := m.files[id]
		if r.Method == http.MethodPatch && !ok {
			status = http.StatusNotFound
		} else {
			if meta.Name != "" {
				f.name = meta.Name
			}
			f.description = meta.Description
			if body != nil {
				f.body = bytes.Clone(body)
			}
			m.files[id] = f
			if frame, ok := decodeSlot(meta.Description); ok && (frame.mode == slotInline || frame.mode == slotMedia) {
				m.frames++
				m.frameBytes += frame.size
				if strings.HasPrefix(id, "down-") {
					m.downFrames++
					m.downBytes += frame.size
				}
			}
		}
		reply = map[string]string{"id": id}
	}
	if payload == nil {
		payload, _ = json.Marshal(reply)
	}
	m.mu.Unlock()
	if err := m.transfer(r.Context(), r.URL.String()+":response", len(payload)+128); err != nil {
		return nil, err
	}
	return browsingReply(status, payload), nil
}

func newBrowsingDrive(rate int, writeDelay time.Duration) (*browsingDrive, *driveStorage) {
	m := &browsingDrive{files: make(map[string]browsingFile), counts: make(map[string]int), rate: rate, writeDelay: writeDelay}
	ctx, cancel := context.WithCancel(context.Background())
	m.link, m.linkCancel, m.linkDone = make(chan *browsingTransfer, 4096), cancel, make(chan struct{})
	go m.runLink(ctx)
	client := &http.Client{Transport: m}
	s := &driveStorage{driveShared: &driveShared{
		filesURL: "https://browsing.invalid/files", uploadURL: "https://browsing.invalid/upload",
		tokenURL: "https://browsing.invalid/token", tokenClient: client,
		token: "fake", tokenExpiry: time.Now().Add(time.Hour), folder: "fake-folder",
		ids: make(map[string]string), backoff: time.Millisecond,
	}, client: client, bulkClient: client, inflight: make(chan struct{}, 64), reserved: make(chan struct{}, 8)}
	s.owner = s
	s.conns = newConnPool(nil, func(context.Context) *http.Client { return client })
	s.bulkConns = s.conns
	return m, s
}

func (m *browsingDrive) snapshot() (map[string]int, int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	counts := make(map[string]int)
	for k, v := range m.counts {
		counts[k] = v
	}
	return counts, m.frames, m.frameBytes
}

func browsingTotal(c map[string]int) int {
	n := 0
	for _, v := range c {
		n += v
	}
	return n
}

func browsingPacket(kind byte, id, size int) []byte {
	p := bytes.Repeat([]byte{byte(id + 17)}, size)
	p[0] = kind
	binary.BigEndian.PutUint32(p[1:5], uint32(id))
	binary.BigEndian.PutUint32(p[5:9], uint32(size))
	return p
}

func browsingRead(c *Conn) (byte, int, int, error) {
	header := make([]byte, 9)
	if _, err := io.ReadFull(c, header); err != nil {
		return 0, 0, 0, err
	}
	id, n := int(binary.BigEndian.Uint32(header[1:5])), int(binary.BigEndian.Uint32(header[5:9]))
	if n < 9 || n > 32<<10 {
		return 0, 0, 0, fmt.Errorf("invalid packet size %d", n)
	}
	data := make([]byte, n-9)
	if _, err := io.ReadFull(c, data); err != nil {
		return 0, 0, 0, err
	}
	if !bytes.Equal(data, bytes.Repeat([]byte{byte(id + 17)}, len(data))) {
		return 0, 0, 0, fmt.Errorf("corrupt packet %d", id)
	}
	return header[0], id, n, nil
}

func browsingStop(conns ...*Conn) {
	for _, c := range conns {
		c.cancel()
		c.SetDeadline(time.Time{})
	}
	synctest.Wait()
}

// Baseline mode changes assertions only, not the workload. The external HEAD
// overlay runs this exact file against all HEAD production files and tests.
func TestBrowsingPerformance(t *testing.T) {
	for _, rate := range []int{2 << 20, 512 << 10} {
		t.Run(fmt.Sprintf("bandwidth%d", rate), func(t *testing.T) {
			regressionClock(t, func(t *testing.T) {
				m, root := newBrowsingDrive(rate, 550*time.Millisecond)
				defer m.close()
				defer root.Close()
				p := regressionParams()
				up, down := regressionTable(64, false), regressionTable(64, false)
				for i := range up.Slots {
					up.Slots[i] = fmt.Sprintf("up-%d", i)
					down.Slots[i] = fmt.Sprintf("down-%d", i)
				}
				up.Watermark, down.Watermark = "up-w", "down-w"
				for _, table := range []slotTable{up, down} {
					for _, id := range table.Slots {
						m.files[id] = browsingFile{name: id}
					}
					m.files[table.Watermark] = browsingFile{name: table.Watermark + "~w"}
				}
				cs, ps := root.session().(*driveStorage), root.session().(*driveStorage)
				defer cs.Close()
				defer ps.Close()
				client := newSlotConn(context.Background(), cs, up, down, p, nil, nil)
				peer := newSlotConn(context.Background(), ps, down, up, p, nil, nil)
				defer browsingStop(client, peer)
				var sendMu sync.Mutex
				errs := make(chan error, 4)
				bulkDone := make(chan time.Duration, 1)
				var bulkReceived atomic.Bool
				var duringBulk atomic.Int32
				const exchanges = 20
				arrived := make([]chan time.Time, exchanges)
				for i := range arrived {
					arrived[i] = make(chan time.Time, 1)
				}
				start := time.Now()
				go func() {
					for i := range 128 {
						sendMu.Lock()
						_, err := peer.Write(browsingPacket(0, i, 32<<10))
						sendMu.Unlock()
						if err != nil {
							errs <- err
							return
						}
					}
				}()
				go func() {
					for range exchanges {
						_, id, _, err := browsingRead(peer)
						if err != nil {
							errs <- err
							return
						}
						size := 1024
						if id%2 != 0 {
							size = 14 << 10
						}
						sendMu.Lock()
						_, err = peer.Write(browsingPacket(1, id, size))
						sendMu.Unlock()
						if err != nil {
							errs <- err
							return
						}
					}
				}()
				go func() {
					bulk, markers := 0, 0
					for bulk < 4<<20 || markers < exchanges {
						kind, id, n, err := browsingRead(client)
						if err != nil {
							errs <- err
							return
						}
						if kind == 0 {
							if id != bulk/(32<<10) {
								errs <- fmt.Errorf("bulk packet order %d, expected %d", id, bulk/(32<<10))
								return
							}
							bulk += n
							if bulk == 4<<20 {
								bulkReceived.Store(true)
								bulkDone <- time.Since(start)
							}
						} else if kind == 1 && id < exchanges {
							markers++
							arrived[id] <- time.Now()
						} else {
							errs <- fmt.Errorf("unexpected kind/id %d/%d", kind, id)
							return
						}
					}
				}()
				latencies := make(chan time.Duration, exchanges)
				for i := range exchanges {
					time.Sleep(100 * time.Millisecond)
					if !bulkReceived.Load() {
						duringBulk.Add(1)
					}
					at := time.Now()
					if _, err := client.Write(browsingPacket(1, i, 1024)); err != nil {
						t.Fatal(err)
					}
					go func(i int, at time.Time) { latencies <- (<-arrived[i]).Sub(at) }(i, at)
				}
				lat := make([]time.Duration, 0, exchanges)
				for range exchanges {
					select {
					case d := <-latencies:
						lat = append(lat, d)
					case err := <-errs:
						t.Fatal(err)
					}
				}
				var elapsed time.Duration
				select {
				case elapsed = <-bulkDone:
				case err := <-errs:
					t.Fatal(err)
				}
				// Include final ACKs and all watermark/polling calls through settling.
				time.Sleep(2 * time.Second)
				browsingStop(client, peer)
				counts, frames, n := m.snapshot()
				m.mu.Lock()
				mean := m.downBytes / m.downFrames
				m.mu.Unlock()
				slices.Sort(lat)
				p50, p95 := lat[9], lat[18]
				t.Logf("SAME SESSION shared %dB/s, write RPC 550ms/read 5ms: small RTT p50=%s p95=%s; bulk=4MiB delivered=%s throughput=%.3fMiB/s frames=%d bytes=%d downlink mean=%dB all requests=%v total=%d requests/MiB=%.2f markers-started-during-bulk=%d", rate, p50, p95, elapsed, 4/elapsed.Seconds(), frames, n, mean, counts, browsingTotal(counts), float64(browsingTotal(counts))/4, duringBulk.Load())
				if duringBulk.Load() != exchanges {
					t.Fatalf("only %d/%d markers overlapped bulk", duringBulk.Load(), exchanges)
				}
				if os.Getenv("XDRIVE_PERF_BASELINE") == "" {
					if mean <= 100<<10 {
						t.Errorf("bulk ramp mean %dB <=100KiB", mean)
					}
					// Same-file HEAD overlay: p50/p95 3.640/4.097s and
					// 16.001/16.650s, totals <=476/344, rates .971/.238.
					// Latency/throughput tolerate 3% for simultaneous RPC ties;
					// request ceilings tolerate only a single speculative poll.
					max50, max95, maxRequests, minRate := 3760*time.Millisecond, 4220*time.Millisecond, 477, 0.94
					if rate == 512<<10 {
						max50, max95, maxRequests, minRate = 16490*time.Millisecond, 17150*time.Millisecond, 345, 0.23
					}
					if p50 > max50 || p95 > max95 {
						t.Errorf("same-session RTT regression: p50=%s p95=%s", p50, p95)
					}
					if browsingTotal(counts) > maxRequests {
						t.Errorf("all-request budget grew: %d > %d", browsingTotal(counts), maxRequests)
					}
					if 4/elapsed.Seconds() < minRate {
						t.Errorf("delivered throughput regressed: %.3f", 4/elapsed.Seconds())
					}
				}
			})
		})
	}
}

func TestBrowsingActualDialAndIdle(t *testing.T) {
	regressionClock(t, func(t *testing.T) {
		m, root := newBrowsingDrive(2<<20, 5*time.Millisecond)
		defer m.close()
		root.token = ""
		defer root.Close()
		factory := func(*Config) (Storage, error) { return root, nil }
		old := testStorageFactory.Swap(&factory)
		defer testStorageFactory.Store(old)
		cfg := &internet.MemoryStreamConfig{ProtocolSettings: &Config{Service: "google-drive", Secrets: []string{"fake-client", "fake-secret", "fake-refresh"}}}
		var times []time.Duration
		for range 6 {
			at := time.Now()
			conn, err := Dial(context.Background(), xnet.Destination{}, cfg)
			if err != nil {
				t.Fatal(err)
			}
			times = append(times, time.Since(at))
			browsingStop(conn.(*Conn))
			// Join the asynchronous announcement and reserve before the next Dial.
			time.Sleep(time.Second)
			synctest.Wait()
			conn.(*Conn).onClose() // only after the asynchronous announce completes
		}
		counts, _, _ := m.snapshot()
		t.Logf("ACTUAL Dial cold=%s next/warm=%s six timings=%v all requests=%v total=%d requests/connection=%.2f", times[0], times[1], times, counts, browsingTotal(counts), float64(browsingTotal(counts))/6)
		if times[1] != 0 {
			t.Errorf("warm Dial waited for HTTP: %s", times[1])
		}
		if counts["announce"] != 6 || counts["token"] != 1 {
			t.Errorf("setup accounting incomplete: %v", counts)
		}
		if os.Getenv("XDRIVE_PERF_BASELINE") == "" {
			if times[0] > 11*time.Millisecond || browsingTotal(counts) > 13 {
				t.Errorf("cold/setup budget regressed: times=%v requests=%v", times, counts)
			}
			for _, d := range times[1:] {
				if d != 0 {
					t.Errorf("replenished Dial made a synchronous request: %v", times)
				}
			}
		}
		table := regressionTable(64, true)
		idle := newSlotReader(context.Background(), root, table, regressionParams(), nil)
		time.Sleep(3 * time.Second)
		synctest.Wait()
		before, _, _ := m.snapshot()
		time.Sleep(time.Second)
		synctest.Wait()
		after, _, _ := m.snapshot()
		idle.stop()
		synctest.Wait()
		polls := browsingTotal(after) - browsingTotal(before)
		t.Logf("ACTUAL Drive idle polling all requests/s=%d", polls)
		if polls > 5 {
			t.Errorf("idle polling increased: %d/s", polls)
		}
	})
}

func TestBrowsingSmallExchangeDelivery(t *testing.T) {
	regressionClock(t, func(t *testing.T) {
		m, root := newBrowsingDrive(2<<20, 5*time.Millisecond)
		defer m.close()
		defer root.Close()
		p := regressionParams()
		up, down := regressionTable(64, false), regressionTable(64, false)
		for i := range up.Slots {
			up.Slots[i] = fmt.Sprintf("up-%d", i)
			down.Slots[i] = fmt.Sprintf("down-%d", i)
		}
		up.Watermark, down.Watermark = "up-w", "down-w"
		for _, table := range []slotTable{up, down} {
			for _, id := range table.Slots {
				m.files[id] = browsingFile{name: id}
			}
			m.files[table.Watermark] = browsingFile{name: table.Watermark + "~w"}
		}
		cs, ps := root.session().(*driveStorage), root.session().(*driveStorage)
		defer cs.Close()
		defer ps.Close()
		client := newSlotConn(context.Background(), cs, up, down, p, nil, nil)
		peer := newSlotConn(context.Background(), ps, down, up, p, nil, nil)
		defer browsingStop(client, peer)
		var lat []time.Duration
		for i, size := range []int{1024, 14 << 10} {
			before, _, _ := m.snapshot()
			m.mu.Lock()
			hits := m.hits
			m.mu.Unlock()
			at := time.Now()
			if _, err := client.Write(browsingPacket(1, i, 1024)); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := browsingRead(peer); err != nil {
				t.Fatal(err)
			}
			if _, err := peer.Write(browsingPacket(1, i, size)); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := browsingRead(client); err != nil {
				t.Fatal(err)
			}
			lat = append(lat, time.Since(at))
			after, _, _ := m.snapshot()
			m.mu.Lock()
			hitDelta := m.hits - hits
			m.mu.Unlock()
			if after["inlineWrite"]-before["inlineWrite"] != 2 || after["mediaWrite"] != 0 || after["body"] != 0 || hitDelta != 2 {
				t.Errorf("small exchange changed its required RPCs: before=%v after=%v successful description reads=%d", before, after, hitDelta)
			}
			if lat[i] > 100*time.Millisecond {
				t.Errorf("small exchange exceeded flush/RPC budget: %s", lat[i])
			}
		}
		browsingStop(client, peer)
		counts, _, _ := m.snapshot()
		t.Logf("ACTUAL small exchange RTTs=%v; each direction inline writes=1 successful description reads=1 body=0; all requests including speculative/missed polls=%v total=%d", lat, counts, browsingTotal(counts))
		// HEAD emits 39-42 attempts, depending on cancellation of speculative
		// polls/ACKs. Required successful RPCs above are exact; none are hidden.
		if os.Getenv("XDRIVE_PERF_BASELINE") == "" && browsingTotal(counts) > 42 {
			t.Errorf("small-exchange all-request budget grew: %v", counts)
		}
	})
}
