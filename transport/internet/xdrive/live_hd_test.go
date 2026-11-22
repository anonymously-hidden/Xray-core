package xdrive

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// F1. The first inline wave of a burst too large for it is planned inline, the
// rest goes to media, and a later small tail stays inline.
func TestSlotBatchSplitsLiveBurstIntoInlineWaveAndMedia(t *testing.T) {
	newWindow := func() *slotBatchWindow {
		b := newSlotBatchWindow(defaultSegmentBytes, slotBulkDepth)
		b.live, b.hybridOn, b.planCap = true, true, slotInlineBurstFrames
		return b
	}
	b := newWindow()
	buffered := defaultSegmentBytes
	if !b.inlineNext(buffered, 0, 0) {
		t.Fatal("a live 512 KiB burst was not split")
	}
	for i := range slotInlineWriteInflight {
		if n := b.frameSize(buffered, i*b.unit, i, defaultSegmentBytes); n != b.unit {
			t.Fatalf("inline wave frame %d was %d bytes", i, n)
		}
		buffered -= b.unit
	}
	if !b.hybrid || b.planned != 0 {
		t.Fatalf("after the wave: hybrid=%v planned=%d", b.hybrid, b.planned)
	}
	if b.inlineNext(buffered, b.budget, slotInlineWriteInflight) {
		t.Fatal("the rest of a split burst was planned inline")
	}
	if n := b.frameSize(buffered, b.budget, slotInlineWriteInflight, defaultSegmentBytes); n != buffered {
		t.Fatalf("the rest went as a %d byte frame, want one %d byte media frame", n, buffered)
	}
	if !b.inlineNext(3*b.unit, b.budget, slotInlineWriteInflight) {
		t.Fatal("a short tail of a split burst left inline publication")
	}

	for _, tc := range []struct {
		name   string
		change func(*slotBatchWindow) int
	}{
		{"busy", func(*slotBatchWindow) int { return slotInlineBurstFrames - slotInlineWriteInflight + 1 }},
		{"not live", func(b *slotBatchWindow) int { b.live = false; return 0 }},
		{"disabled", func(b *slotBatchWindow) int { b.hybridOn = false; return 0 }},
		{"small ring", func(b *slotBatchWindow) int { b.window = slotInlineWriteInflight - 1; return 0 }},
	} {
		b := newWindow()
		active := tc.change(b)
		if b.splits(defaultSegmentBytes, active) {
			t.Fatalf("%s: a burst was split", tc.name)
		}
	}
	b = newWindow()
	if !b.fits(b.budget, 0, 0) || b.splits(b.budget, 0) {
		t.Fatal("a burst of one inline wave left the existing live inline rule")
	}
}

// F1. A 1 MiB live burst sends its first wave inline and the rest as media,
// all at once: before, media admission waited until fewer than
// slotWriteInflight uploads were outstanding, behind the inline wave.
func TestSlotWriterSplitBurstUploadsMediaBesideInlineWave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		store := &batchTestSlots{memSlots: newMemSlots(), release: make(chan struct{})}
		p := paramsFromConfig(&Config{Concurrency: 64, FlushIntervalMs: 10})
		ring, err := makeRing(ctx, store, "split", p)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		w := newSlotWriter(ctx, store, ring.Up, p)
		defer func() { cancel(); w.wait() }()
		w.mu.Lock()
		w.liveHybrid = true
		w.mu.Unlock()
		payload := randomPayload(2 * p.segmentBytes)
		if _, err := w.Write(payload); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * p.flushInterval)
		synctest.Wait()

		wave := int64(slotInlineWriteInflight)
		w.mu.Lock()
		frames := w.seq
		var sizes []int
		for seq := range frames {
			if g := w.gates[seq%int64(len(w.gates))]; g != nil {
				sizes = append(sizes, g.accountedBytes)
			}
		}
		w.mu.Unlock()
		rest := 2*p.segmentBytes - int(wave)*slotInlineLimit
		if frames != wave+2 || len(sizes) != int(frames) {
			t.Fatalf("frames=%d sizes=%v, want %d inline and 2 media", frames, sizes, wave)
		}
		for i, n := range sizes {
			if i < int(wave) && n != slotInlineLimit || i == int(wave) && n != p.segmentBytes || i == int(wave)+1 && n != rest-p.segmentBytes {
				t.Fatalf("frame %d is %d bytes: %v", i, n, sizes)
			}
		}
		if active := store.active.Load(); active != int32(frames) {
			t.Fatalf("%d of %d uploads in flight: media waited behind the inline wave", active, frames)
		}

		close(store.release)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		store.mu.Lock()
		defer store.mu.Unlock()
		var got []byte
		for seq := range frames {
			id := w.slotID(seq)
			frame, ok := decodeSlot(store.descs[id])
			if !ok || frame.seq != seq || (frame.mode == slotInline) != (seq < wave) {
				t.Fatalf("frame %d has the wrong publication class", seq)
			}
			got = append(got, frame.data...)
			got = append(got, store.bodies[id]...)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("the split burst lost, duplicated or reordered bytes")
		}
		if store.peak.Load() > int32(liveUploadCeiling(w, slotInlineWriteInflight)) {
			t.Fatalf("peak uploads %d above the live ceiling", store.peak.Load())
		}
	})
}

// F6. A copy goes out only once an update has run longer than recent ones
// usually take, between slotUploadHedge and twice that.
func TestUploadHedgeDelayFollowsRecentUpdates(t *testing.T) {
	if slotUploadHedge <= 0 {
		t.Skip("upload copies are disabled")
	}
	for _, tc := range []struct {
		sample, want time.Duration
		n            int
	}{
		{0, slotUploadHedge, 0},
		{3 * time.Second, slotUploadHedge, rangeLatencyMin - 1},
		{500 * time.Millisecond, slotUploadHedge, rangeLatencyRing},
		{1800 * time.Millisecond, 1800 * time.Millisecond, rangeLatencyRing},
		{5 * time.Second, 2 * slotUploadHedge, rangeLatencyRing},
	} {
		s := memoryDrive(nil)
		for range tc.n {
			s.inlineUpdates.note(tc.sample)
		}
		if got := s.uploadHedgeDelay(); got != tc.want {
			t.Fatalf("%d samples of %v: delay %v, want %v", tc.n, tc.sample, got, tc.want)
		}
	}
}

// F6. A 1.5 s update is no straggler after updates of 1.8 s, so it gets no
// copy; without that history the 1.2 s floor still sends one. Each successful
// primary teaches the history.
func TestUploadHedgeWaitsForRecentUpdateLatency(t *testing.T) {
	if slotUploadHedge <= 0 {
		t.Skip("upload copies are disabled")
	}
	for _, tc := range []struct {
		name    string
		history time.Duration
		copies  int32
	}{{"cold", 0, 1}, {"slow history", 1800 * time.Millisecond, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls atomic.Int32
				s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
					if r.Method != http.MethodPatch {
						return memoryResponse(http.StatusNotFound, ""), nil
					}
					delay := 100 * time.Millisecond
					if calls.Add(1) == 1 {
						delay = 1500 * time.Millisecond
					}
					if err := batchTestWait(r.Context(), delay); err != nil {
						return nil, err
					}
					return memoryResponse(http.StatusOK, `{"id":"slot"}`), nil
				}))
				s.uploadHedges = make(chan struct{}, slotUploadHedgeMax)
				if tc.history > 0 {
					for range rangeLatencyRing {
						s.inlineUpdates.note(tc.history)
					}
				}
				before := s.inlineUpdates.n
				ctx := context.WithValue(context.Background(), slotUploadHedgeKey{}, true)
				if err := s.setSlot(ctx, "slot", "frame", nil); err != nil {
					t.Fatal(err)
				}
				time.Sleep(2 * time.Second) // let a copy and the primary finish
				synctest.Wait()
				if copies := calls.Load() - 1; copies != tc.copies {
					t.Fatalf("%d copies, want %d", copies, tc.copies)
				}
				s.inlineUpdates.mu.Lock()
				learned := s.inlineUpdates.n > before || before == rangeLatencyRing
				s.inlineUpdates.mu.Unlock()
				if !learned {
					t.Fatal("a successful update was not recorded")
				}
				s.closeWork()
			})
		})
	}
}

// enableConnRecovery turns the spare connection and fast dead-connection
// retirement on for one test; TestMain turns them off for the others.
func enableConnRecovery(t *testing.T) {
	t.Helper()
	connSpare.Store(true)
	connDeadFast.Store(true)
	connHeardAny.Store(0)
	t.Cleanup(func() {
		connSpare.Store(false)
		connDeadFast.Store(false)
		connHeardAny.Store(0)
	})
}

// fakeConns makes the connections of a pool from fake transports: those
// numbered in dead never answer, the others answer at once. heads counts the
// HEAD requests each connection received.
type fakeConns struct {
	mu    sync.Mutex
	made  int
	dead  map[int]bool
	heads map[int]int
	gets  map[int]int
}

func (f *fakeConns) newClient(context.Context) *http.Client {
	f.mu.Lock()
	f.made++
	index := f.made
	f.mu.Unlock()
	return &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		if trace := httptrace.ContextClientTrace(r.Context()); trace != nil && trace.WroteRequest != nil {
			trace.WroteRequest(httptrace.WroteRequestInfo{})
		}
		f.mu.Lock()
		dead := f.dead[index]
		if r.Method == http.MethodHead {
			f.heads[index]++
		} else {
			f.gets[index]++
		}
		f.mu.Unlock()
		if dead {
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		return memoryResponse(http.StatusOK, "ok"), nil
	})}
}

func (f *fakeConns) count() (made int, heads, gets map[int]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	heads, gets = make(map[int]int), make(map[int]int)
	for k, v := range f.heads {
		heads[k] = v
	}
	for k, v := range f.gets {
		gets[k] = v
	}
	return f.made, heads, gets
}

func newFakeConns(dead ...int) *fakeConns {
	f := &fakeConns{dead: make(map[int]bool), heads: make(map[int]int), gets: make(map[int]int)}
	for _, i := range dead {
		f.dead[i] = true
	}
	return f
}

func poolConnFor(pool *connPool) *poolConn {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return pool.xmux.GetXmuxClient(context.Background()).XmuxConn.(*poolConn)
}

func getOn(c *poolConn) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, "https://drive.invalid/files/x", nil)
	if err != nil {
		return nil, err
	}
	if !c.acquire() {
		return nil, errSessionClosed
	}
	return c.roundTrip(req)
}

// F3. Two small requests wait on a connection that went silent while another
// connection answers: it takes no new requests after 1.5 s and is closed after
// 2.5 s, which fails the waiting requests so that they are retried elsewhere.
// Before, they waited for the HTTP/2 keepalive (up to 4 s) or the stall timer.
func TestSilentConnectionRetiredWhileAnotherAnswers(t *testing.T) {
	enableConnRecovery(t)
	for _, tc := range []struct {
		name           string
		waiting        int
		othersAnswer   bool
		drained, ended bool
	}{
		{"dead", 2, true, true, true},
		{"one straggler", 1, true, true, false},
		{"path down", 2, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				connHeardAny.Store(0)
				fakes := newFakeConns(1)
				pool := newConnPool(xmuxWithConnections(nil, 2), fakes.newClient)
				defer pool.Close()
				silent, alive := poolConnFor(pool), poolConnFor(pool)
				if silent == alive {
					t.Fatal("the pool did not open a second connection")
				}
				began := time.Now()
				ended := make(chan time.Duration, tc.waiting)
				for range tc.waiting {
					go func() {
						if _, err := getOn(silent); err == nil {
							t.Error("a silent connection answered")
						}
						ended <- time.Since(began)
					}()
				}
				if tc.othersAnswer {
					time.Sleep(500 * time.Millisecond)
					resp, err := getOn(alive)
					if err != nil {
						t.Fatal(err)
					}
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
				time.Sleep(deadConnSuspect + 100*time.Millisecond - time.Since(began))
				if got := silent.IsClosed(); got != tc.drained {
					t.Fatalf("at %v: drained=%v, want %v", time.Since(began), got, tc.drained)
				}
				time.Sleep(deadConnConfirm + 100*time.Millisecond - time.Since(began))
				synctest.Wait()
				done := len(ended)
				if (done == tc.waiting) != tc.ended || done != 0 && done != tc.waiting {
					t.Fatalf("at %v: %d of %d waiting requests ended", time.Since(began), done, tc.waiting)
				}
				for range done {
					if at := <-ended; at < deadConnConfirm || at > deadConnConfirm+100*time.Millisecond {
						t.Fatalf("a waiting request ended at %v, want %v", at, deadConnConfirm)
					}
				}
				silent.forceClose()
				for range tc.waiting - done {
					<-ended
				}
				if alive.IsClosed() || tc.othersAnswer && !alive.ready(time.Now()) {
					t.Fatal("the answering connection was retired")
				}
			})
		})
	}
}

// F3. A pool that carries a request warms a spare connection with a
// quota-free HEAD. When the session's connection dies, the session moves to
// the spare without opening a connection, and a new spare is warmed.
func TestSpareConnectionTakesOverAfterConnectionLoss(t *testing.T) {
	enableConnRecovery(t)
	synctest.Test(t, func(t *testing.T) {
		fakes := newFakeConns()
		pool := newConnPool(xmuxWithConnections(nil, 2), fakes.newClient)
		session := &sessionTransport{pool: pool}
		defer func() { session.Close(); pool.Close() }()
		client := &http.Client{Transport: session}
		get := func() {
			t.Helper()
			resp, err := client.Get("https://drive.invalid/files/x")
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			synctest.Wait()
		}

		get()
		made, heads, gets := fakes.count()
		if made != 2 || heads[2] != 1 || gets[1] != 1 {
			t.Fatalf("after the first request: %d connections, HEADs %v, GETs %v; want a warmed spare", made, heads, gets)
		}
		time.Sleep(spareCheckEvery)
		get()
		if made, heads, _ = fakes.count(); made != 2 || heads[2] != 1 {
			t.Fatalf("a ready spare was warmed again or another opened: %d connections, HEADs %v", made, heads)
		}

		session.mu.Lock()
		first := session.client.XmuxConn.(*poolConn)
		session.mu.Unlock()
		first.forceClose()
		time.Sleep(spareCheckEvery)
		get()
		made, heads, gets = fakes.count()
		if gets[2] != 1 {
			t.Fatalf("the session did not move to the spare: GETs %v", gets)
		}
		if made != 3 || heads[3] != 1 {
			t.Fatalf("no new spare after the move: %d connections, HEADs %v", made, heads)
		}
	})
}
