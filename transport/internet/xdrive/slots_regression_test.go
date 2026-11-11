package xdrive

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xtls/xray-core/common/log"
)

// All delays in this file run inside synctest bubbles. No Drive credentials or
// HTTP clients are used. Delays model 5ms metadata RPCs and 8MiB/s media RPCs.
type regressionSlots struct {
	*memSlots
	mu                                                       sync.Mutex
	before                                                   func(context.Context, slotFrame) error
	bodyBefore                                               func(context.Context, string) error
	delay                                                    bool
	frames                                                   []slotFrame
	writes, descriptions, hits, bodies, acks, wmReads, setup int
	ids                                                      map[string]int
}

func regressionTable(depth int, lazy bool) slotTable {
	t := slotTable{Slots: make([]string, depth), Watermark: "wm", name: "regression", lazy: lazy}
	for i := range t.Slots {
		t.Slots[i] = fmt.Sprintf("s%d", i)
	}
	return t
}

func regressionStore(table slotTable) *regressionSlots {
	s := &regressionSlots{memSlots: newMemSlots(), ids: make(map[string]int)}
	if !table.lazy {
		for _, id := range table.Slots {
			s.descs[id] = ""
		}
		s.descs[table.Watermark] = ""
	}
	return s
}

func regressionParams() params {
	return params{segmentBytes: 512 << 10, concurrency: 64, flushInterval: 10 * time.Millisecond,
		minPollInterval: 50 * time.Millisecond, maxPollInterval: 200 * time.Millisecond,
		eagerWindow: 2 * time.Second, holeTimeout: 30 * time.Second}
}

func (s *regressionSlots) makeSlot(ctx context.Context, name string) (string, error) {
	s.mu.Lock()
	s.setup++
	s.mu.Unlock()
	return s.memSlots.makeSlot(ctx, name)
}

func regressionDelay(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func (s *regressionSlots) put(ctx context.Context, id, name, description string, body []byte, create bool) error {
	f, ok := decodeSlot(description)
	if ok && s.before != nil {
		if err := s.before(ctx, f); err != nil {
			return err
		}
	}
	if s.delay {
		if err := regressionDelay(ctx, 5*time.Millisecond+time.Duration(len(body))*time.Second/(8<<20)); err != nil {
			return err
		}
	}
	s.mu.Lock()
	if ok {
		s.writes++
		s.frames = append(s.frames, f)
	} else {
		s.acks++
	}
	s.mu.Unlock()
	if create {
		return s.memSlots.createSlot(ctx, id, name, description, body)
	}
	return s.memSlots.setSlot(ctx, id, description, body)
}
func (s *regressionSlots) setSlot(ctx context.Context, id, description string, body []byte) error {
	return s.put(ctx, id, "", description, body, false)
}
func (s *regressionSlots) createSlot(ctx context.Context, id, name, description string, body []byte) error {
	return s.put(ctx, id, name, description, body, true)
}
func (s *regressionSlots) readDescription(ctx context.Context, id string) (string, error) {
	if s.delay {
		if err := regressionDelay(ctx, 5*time.Millisecond); err != nil {
			return "", err
		}
	}
	d, err := s.memSlots.readDescription(ctx, id)
	s.mu.Lock()
	if id == "wm" {
		s.wmReads++
	} else {
		s.descriptions++
		s.ids[id]++
		if _, ok := decodeSlot(d); ok {
			s.hits++
		}
	}
	s.mu.Unlock()
	return d, err
}
func (s *regressionSlots) readBody(ctx context.Context, id string) ([]byte, error) {
	if s.bodyBefore != nil {
		if err := s.bodyBefore(ctx, id); err != nil {
			return nil, err
		}
	}
	body, err := s.memSlots.readBody(ctx, id)
	if s.delay {
		if e := regressionDelay(ctx, 5*time.Millisecond+time.Duration(len(body))*time.Second/(8<<20)); e != nil {
			return nil, e
		}
	}
	s.mu.Lock()
	s.bodies++
	s.mu.Unlock()
	return body, err
}

// Existing slots_test.init shortens globals. Restore production cadence only
// around a fully joined bubble; do not use this helper in parallel tests.
func regressionBubble(t *testing.T, f func(*testing.T)) {
	t.Helper()
	// The default async logger owns process-global channels. Start it outside
	// the bubble so its worker cannot keep fake time from advancing.
	log.Record(&log.GeneralMessage{Severity: log.Severity_Debug, Content: "xdrive fake-clock regression"})
	synctest.Test(t, f)
}

func regressionClock(t *testing.T, f func(*testing.T)) {
	t.Helper()
	a, b, c, d, e, g := slotHuntFloor, slotHuntCeiling, slotTailMin, slotTailMax, slotAckRetry, slotWatermarkTTL
	slotHuntFloor, slotHuntCeiling, slotTailMin, slotTailMax = 10*time.Millisecond, 25*time.Millisecond, 150*time.Millisecond, 800*time.Millisecond
	slotAckRetry, slotWatermarkTTL = 30*time.Millisecond, 100*time.Millisecond
	defer func() {
		slotHuntFloor, slotHuntCeiling, slotTailMin, slotTailMax, slotAckRetry, slotWatermarkTTL = a, b, c, d, e, g
	}()
	regressionBubble(t, f)
}

// Isolated RPC accounting only, not browsing/QoS or actual Dial measurements.
// TestBrowsingPerformance and TestBrowsingActualDialAndIdle cover those guards.
func TestRegressionMetrics(t *testing.T) {
	regressionClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer func() { cancel(); synctest.Wait() }()
		p := regressionParams()
		table := regressionTable(64, true)
		bulk := regressionStore(table)
		bulk.delay = true
		bw := newSlotWriter(ctx, bulk, table, p)
		br := newSlotReader(ctx, bulk, table, p, nil)
		bulkDone := make(chan error, 1)
		go func() {
			for range 128 {
				if _, err := bw.Write(make([]byte, 32<<10)); err != nil {
					bulkDone <- err
					return
				}
			}
			bulkDone <- bw.Close()
		}()
		readDone := make(chan int, 1)
		go func() {
			n := 0
			for b := range br.chunks() {
				n += len(b)
			}
			readDone <- n
		}()
		// Independent interactive directions share the same modeled RPC latency,
		// not a bandwidth limiter; this measures scheduling/coalescing, not WAN QoS.
		var latencies []time.Duration
		for i := 0; i < 20; i++ {
			size := 1024
			if i%2 != 0 {
				size = 14 << 10
			}
			s := regressionStore(table)
			s.delay = true
			w := newSlotWriter(ctx, s, table, p)
			start := time.Now()
			if _, err := w.Write(make([]byte, size)); err != nil {
				t.Fatal(err)
			}
			// Fetch the announced frame once visible: counts foreground requests
			// separately from speculative/idle polling in the bulk reader below.
			for {
				synctest.Wait()
				s.mu.Lock()
				done := s.writes > 0
				s.mu.Unlock()
				if done {
					break
				}
				time.Sleep(time.Millisecond)
			}
			d, err := s.readDescription(ctx, table.Slots[0])
			if err != nil {
				t.Fatal(err)
			}
			f, ok := decodeSlot(d)
			if !ok || f.mode != slotInline || len(f.data) != size {
				t.Fatalf("small frame: %+v", f)
			}
			latencies = append(latencies, time.Since(start))
			s.mu.Lock()
			writes, reads, bodies, setup, acks := s.writes, s.descriptions, s.bodies, s.setup, s.acks
			s.mu.Unlock()
			if writes != 1 || reads != 1 || bodies != 0 || setup != 0 || acks != 0 {
				t.Fatalf("small guardrail writes=%d descriptions=%d body=%d setup=%d ack=%d", writes, reads, bodies, setup, acks)
			}
		}
		if err := <-bulkDone; err != nil {
			t.Fatal(err)
		}
		if n := <-readDone; n != 4<<20 {
			t.Fatalf("bulk bytes=%d", n)
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		bulk.mu.Lock()
		frames, total := 0, 0
		for _, f := range bulk.frames {
			if f.mode == slotInline || f.mode == slotMedia {
				frames++
				total += f.size
			}
		}
		t.Logf("MODEL production cadence; independent RPCs 5ms + media/8MiB/s: small p50=%s p95=%s; small/direction write=1 description=1 body=0 setup=0 standaloneACK=0; bulk frames=%d mean=%dB writes=%d descriptions=%d body=%d wmGET=%d ACK=%d requests/MiB=%.2f setup=%d", latencies[9], latencies[18], frames, total/frames, bulk.writes, bulk.descriptions, bulk.bodies, bulk.wmReads, bulk.acks, float64(bulk.writes+bulk.descriptions+bulk.bodies+bulk.wmReads+bulk.acks)/4, bulk.setup)
		bulk.mu.Unlock()
		idle := regressionStore(table)
		ir := newSlotReader(ctx, idle, table, p, nil)
		time.Sleep(3 * time.Second)
		synctest.Wait()
		idle.mu.Lock()
		before := idle.descriptions
		idle.mu.Unlock()
		time.Sleep(time.Second)
		synctest.Wait()
		idle.mu.Lock()
		count := idle.descriptions - before
		ids := len(idle.ids)
		idle.mu.Unlock()
		t.Logf("MODEL never-started idle: %d metadata GET/s, physical slots polled=%d", count, ids)
		if count > 5 || ids != 1 {
			t.Fatalf("idle polling grew: %d / %d", count, ids)
		}
		ir.stop()
	})
}

func TestRegressionSlotGate(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		t.Run(strconv.FormatBool(lazy), func(t *testing.T) {
			regressionBubble(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer func() { cancel(); synctest.Wait() }()
				table := regressionTable(2, lazy)
				s := regressionStore(table)
				s.rejectDup = true
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				s.before = func(ctx context.Context, f slotFrame) error {
					if f.seq == 0 {
						once.Do(func() { close(entered) })
						select {
						case <-release:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return nil
				}
				p := regressionParams()
				p.segmentBytes = 1
				p.concurrency = 2
				w := newSlotWriter(ctx, s, table, p)
				w.first = make(chan []byte, 1)
				if _, err := w.Write([]byte("a")); err != nil {
					t.Fatal(err)
				}
				<-entered
				r := newSlotReader(ctx, s, table, p, <-w.first)
				if string(<-r.chunks()) != "a" {
					t.Fatal("seed missing")
				}
				synctest.Wait()
				if _, err := w.Write([]byte("b")); err != nil {
					t.Fatal(err)
				}
				if string(<-r.chunks()) != "b" {
					t.Fatal("seq1 missing")
				}
				synctest.Wait()
				if _, err := w.Write([]byte("c")); err != nil {
					t.Fatal(err)
				}
				time.Sleep(50 * time.Millisecond)
				synctest.Wait()
				s.mu.Lock()
				early := false
				for _, f := range s.frames {
					early = early || f.seq == 2
				}
				s.mu.Unlock()
				if early {
					t.Error("seq2 overwrote physical slot while seq0 upload was held")
				}
				close(release)
				if string(<-r.chunks()) != "c" {
					t.Fatal("seq2 missing")
				}
				w.wg.Wait()
				d, _ := s.readDescription(ctx, table.Slots[0])
				f, _ := decodeSlot(d)
				if f.seq != 2 {
					t.Errorf("final physical slot seq=%d, want 2", f.seq)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				for range r.chunks() {
				}
			})
		})
	}
}

func TestRegressionSlotGateExit(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(strconv.FormatBool(cancelled), func(t *testing.T) {
			regressionBubble(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer func() { cancel(); synctest.Wait() }()
				table := regressionTable(2, true)
				s := regressionStore(table)
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				s.before = func(ctx context.Context, f slotFrame) error {
					if f.seq == 0 && f.mode != slotErr {
						once.Do(func() { close(entered) })
						select {
						case <-release:
							return io.ErrUnexpectedEOF
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return nil
				}
				p := regressionParams()
				p.segmentBytes = 1
				w := newSlotWriter(ctx, s, table, p)
				w.ackMu.Lock()
				w.acked = 10
				w.ackMu.Unlock()
				w.Write([]byte("a"))
				<-entered
				w.Write([]byte("b"))
				synctest.Wait()
				w.Write([]byte("c"))
				synctest.Wait()
				if cancelled {
					cancel()
				} else {
					close(release)
				}
				w.wg.Wait()
				if !cancelled {
					s.mu.Lock()
					last := s.frames[len(s.frames)-1]
					s.mu.Unlock()
					if last.seq != 2 {
						t.Errorf("failure marker overtook next lap: final seq=%d", last.seq)
					}
				}
			})
		})
	}
}

func TestRegressionSlotCloseOrdering(t *testing.T) {
	regressionBubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer func() { cancel(); synctest.Wait() }()
		table := regressionTable(2, true)
		s := regressionStore(table)
		release := make(chan struct{})
		s.before = func(ctx context.Context, f slotFrame) error {
			if f.seq == 0 {
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}
		p := regressionParams()
		p.segmentBytes = 1
		w := newSlotWriter(ctx, s, table, p)
		w.Write([]byte("a"))
		closed := make(chan error, 1)
		go func() { closed <- w.Close() }()
		synctest.Wait()
		select {
		case err := <-closed:
			t.Errorf("Close overtook upload: %v", err)
		default:
		}
		close(release)
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if len(s.frames) != 2 || s.frames[0].seq != 0 || s.frames[1].mode != slotEnd {
			t.Fatalf("Close order=%+v", s.frames)
		}
	})
}

func TestRegressionCloseGrace(t *testing.T) {
	for _, dead := range []bool{false, true} {
		t.Run(strconv.FormatBool(dead), func(t *testing.T) {
			regressionBubble(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer func() { cancel(); synctest.Wait() }()
				table := regressionTable(2, false)
				s := regressionStore(table)
				p := regressionParams()
				p.segmentBytes = 2
				p.flushInterval = time.Hour
				w := newSlotWriter(ctx, s, table, p)
				time.Sleep(slotCloseGrace + time.Second)
				w.mu.Lock()
				w.seq = 2
				w.buf = []byte("abcd")
				w.size = 1
				w.mu.Unlock()
				if !dead {
					go func() {
						if regressionDelay(ctx, 100*time.Millisecond) == nil {
							s.memSlots.setSlot(ctx, table.Watermark, "10", nil)
						}
					}()
				}
				start := time.Now()
				err := w.Close()
				w.wg.Wait()
				if dead {
					if err == nil || time.Since(start) < slotCloseGrace {
						t.Errorf("dead peer grace=%s err=%v", time.Since(start), err)
					}
				} else if err != nil {
					t.Errorf("idle writer lost close grace before buffered wrap: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Errorf("idempotent Close: %v", err)
				}
			})
		})
	}
}

func TestRegressionSlotSizing(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		size, busy, segment, want int
		media                     bool
	}{
		{"all-free", 100 << 10, 0, 512 << 10, 8, false},
		{"eleven-busy", 100 << 10, 11, 512 << 10, 1, true},
		{"small", 1024, 0, 512 << 10, 1, false},
		{"small-segment", 3000, 0, 1000, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			regressionBubble(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer func() { cancel(); synctest.Wait() }()
				table := regressionTable(64, true)
				s := regressionStore(table)
				p := regressionParams()
				p.segmentBytes = tc.segment
				p.flushInterval = time.Hour
				w := newSlotWriter(ctx, s, table, p)
				release := make(chan struct{})
				s.before = func(ctx context.Context, _ slotFrame) error {
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				for range tc.busy {
					w.slots <- struct{}{}
				}
				w.mu.Lock()
				w.size = tc.segment
				w.buf = make([]byte, tc.size)
				// Exactly the number of uploads that should fit in the available tokens.
				for i := 0; i < tc.want && len(w.buf) > 0; i++ {
					if err := w.flushLocked(nil, nil); err != nil {
						t.Fatal(err)
					}
				}
				remaining := len(w.buf)
				w.mu.Unlock()
				if remaining != 0 {
					t.Errorf("buffer left=%d, want one media when eleven tokens busy", remaining)
				}
				close(release)
				w.wg.Wait()
				s.mu.Lock()
				defer s.mu.Unlock()
				if len(s.frames) != tc.want {
					t.Errorf("frames=%d want=%d", len(s.frames), tc.want)
				}
				for _, f := range s.frames {
					if (f.mode == slotMedia) != tc.media || f.size > tc.segment {
						t.Errorf("frame mode=%d size=%d limit=%d", f.mode, f.size, tc.segment)
					}
				}
			})
		})
	}
}

func TestRegressionSlotRamp(t *testing.T) {
	regressionClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer func() { cancel(); synctest.Wait() }()
		table := regressionTable(64, true)
		s := regressionStore(table)
		s.delay = true
		p := regressionParams()
		w := newSlotWriter(ctx, s, table, p)
		r := newSlotReader(ctx, s, table, p, nil)
		payload := randomPayload(4 << 20)
		done := make(chan error, 1)
		go func() {
			for off := 0; off < len(payload); off += 32 << 10 {
				if _, err := w.Write(payload[off : off+32<<10]); err != nil {
					done <- err
					return
				}
				if err := regressionDelay(ctx, 2*time.Millisecond); err != nil {
					done <- err
					return
				}
			}
			done <- w.Close()
		}()
		var got []byte
		for b := range r.chunks() {
			got = append(got, b...)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("bulk stream differs")
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		frames, total, settled, settledTotal := 0, 0, 0, 0
		for _, f := range s.frames {
			if f.mode == slotInline || f.mode == slotMedia {
				frames++
				total += f.size
				if frames > 4*inlineRampSegments {
					settled++
					settledTotal += f.size
				}
			}
		}
		requests := s.writes + s.descriptions + s.bodies + s.wmReads + s.acks
		t.Logf("MODEL paced 4MiB/32KiB every 2ms: frames=%d mean=%dB settled mean=%dB write=%d description=%d body=%d wmGET=%d ACK=%d requests/MiB=%.2f", frames, total/frames, settledTotal/max(settled, 1), s.writes, s.descriptions, s.bodies, s.wmReads, s.acks, float64(requests)/4)
		if settled == 0 || settledTotal/settled <= 100<<10 {
			t.Errorf("settled ramp mean=%dB over %d frames, want >100KiB", settledTotal/max(settled, 1), settled)
		}
		if requests >= 600 {
			t.Errorf("ramp request count=%d, want less than inline baseline", requests)
		}
	})
}

func TestRegressionFirstMetadataAhead(t *testing.T) {
	regressionClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer func() { cancel(); synctest.Wait() }()
		table := regressionTable(64, true)
		s := regressionStore(table)
		p := regressionParams()
		body := make([]byte, 20<<10)
		s.createSlot(ctx, "s0", "", newSlotFrame(0, slotMedia, body).encode(), body)
		s.createSlot(ctx, "s1", "", newSlotFrame(1, slotInline, []byte("next")).encode(), nil)
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		s.bodyBefore = func(ctx context.Context, id string) error {
			if id == "s0" {
				once.Do(func() { close(entered) })
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}
		r := newSlotReader(ctx, s, table, p, nil)
		<-entered
		synctest.Wait()
		s.mu.Lock()
		ahead := s.ids["s1"]
		s.mu.Unlock()
		t.Logf("next metadata requests while first body held=%d", ahead)
		if ahead == 0 {
			t.Error("fetch-ahead waited for first media body")
		}
		close(release)
		if got := <-r.chunks(); !bytes.Equal(got, body) {
			t.Fatal("first body differs")
		}
		if got := <-r.chunks(); string(got) != "next" {
			t.Fatal("next body differs")
		}
	})
}

func TestRegressionSlotBufferBound(t *testing.T) {
	regressionBubble(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer func() { cancel(); synctest.Wait() }()
		table := regressionTable(64, true)
		s := regressionStore(table)
		p := regressionParams()
		s.before = func(ctx context.Context, _ slotFrame) error { <-ctx.Done(); return ctx.Err() }
		w := newSlotWriter(ctx, s, table, p)
		done := make(chan struct{})
		go func() { w.Write(make([]byte, 32<<20)); close(done) }()
		synctest.Wait()
		select {
		case <-done:
			t.Error("large Write returned with all uploads blocked")
		default:
		}
		w.mu.Lock()
		buffer, capacity, seq := len(w.buf), cap(w.buf), w.seq
		w.mu.Unlock()
		t.Logf("held uploads=%d buffer=%d capacity=%d limit=%d", seq, buffer, capacity, cap(w.slots)*p.segmentBytes)
		if seq != 12 || buffer > cap(w.slots)*p.segmentBytes || capacity > cap(w.slots)*p.segmentBytes {
			t.Error("unbounded slot pending data")
		}
		cancel()
		<-done
		w.wg.Wait()
	})
}
