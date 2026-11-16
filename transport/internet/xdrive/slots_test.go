package xdrive

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type memSlots struct {
	mu        sync.Mutex
	next      int
	descs     map[string]string
	bodies    map[string][]byte
	reads     int
	creates   int
	updates   int
	rejectDup bool
}

func newMemSlots() *memSlots {
	return &memSlots{descs: make(map[string]string), bodies: make(map[string][]byte)}
}

func (m *memSlots) makeSlot(ctx context.Context, name string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.next++
	id := "slot-" + strconv.Itoa(m.next)
	m.descs[id] = ""
	return id, nil
}

func (m *memSlots) generateIDs(ctx context.Context, n int) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		m.next++
		ids = append(ids, "gen-"+strconv.Itoa(m.next))
	}
	return ids, nil
}

func (m *memSlots) takeIDs(ctx context.Context, n, ringWorth int) ([]string, error) {
	return m.generateIDs(ctx, n)
}

func (m *memSlots) createSlot(ctx context.Context, id, name, description string, body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.descs[id]; ok {
		if m.rejectDup {
			return errDuplicateID
		}
	} else {
		m.creates++
	}
	m.descs[id] = description
	if body != nil {
		m.bodies[id] = bytes.Clone(body)
	}
	return nil
}

func (m *memSlots) setSlot(ctx context.Context, id, description string, body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.descs[id]; !ok {
		return errNotFound
	}
	m.updates++
	m.descs[id] = description
	if body != nil {
		m.bodies[id] = bytes.Clone(body)
	}
	return nil
}

func (m *memSlots) readDescription(ctx context.Context, id string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads++
	d, ok := m.descs[id]
	if !ok {
		return "", errNotFound
	}
	return d, nil
}

func (m *memSlots) readBody(ctx context.Context, id string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bodies[id], nil
}

// the slots of all tests are asked for often, set once as a goroutine of a
// closed Conn may still read them
func slotPair(t *testing.T, store slotStore, concurrency, segmentBytes uint32) (*Conn, *Conn) {
	t.Helper()
	p := paramsFromConfig(&Config{
		SegmentBytes:      segmentBytes,
		FlushIntervalMs:   1,
		PollIntervalMs:    1,
		MaxPollIntervalMs: 20,
		EagerWindowMs:     50,
		Concurrency:       concurrency,
	})
	r, err := makeRing(context.Background(), store, "session", p)
	if err != nil {
		t.Fatal(err)
	}
	client := newSlotConn(context.Background(), store, r.Up, r.Down, p, nil, nil)
	server := newSlotConn(context.Background(), store, r.Down, r.Up, p, nil, nil)
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	for _, c := range []*Conn{client, server} {
		c.SetReadDeadline(time.Now().Add(30 * time.Second))
	}
	return client, server
}

// writeAll writes payload in pieces of random size, and closes c if end, the
// error goes to errs
func writeAll(c *Conn, payload []byte, end bool, errs chan<- error) {
	go func() {
		rng := rand.New(rand.NewSource(1))
		for len(payload) > 0 {
			n := min(len(payload), 1+rng.Intn(40<<10))
			if _, err := c.Write(payload[:n]); err != nil {
				errs <- err
				return
			}
			payload = payload[n:]
		}
		var err error
		if end {
			err = c.Close()
		}
		errs <- err
	}()
}

func readAll(t *testing.T, c *Conn, want []byte) {
	t.Helper()
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %d bytes, not the %d that were sent", len(got), len(want))
	}
}

func randomPayload(n int) []byte {
	payload := make([]byte, n)
	rand.New(rand.NewSource(int64(n))).Read(payload)
	return payload
}

func TestSlotFrame(t *testing.T) {
	data := []byte("hello")
	f, ok := decodeSlot(newSlotFrame(7, slotInline, data).encode())
	if !ok || f.seq != 7 || f.mode != slotInline || !bytes.Equal(f.data, data) {
		t.Fatalf("inline frame came back as %+v %v", f, ok)
	}

	body := bytes.Repeat([]byte{7}, 100)
	f, ok = decodeSlot(newSlotFrame(8, slotMedia, body).encode())
	if !ok || f.seq != 8 || f.mode != slotMedia || f.data != nil || !f.holds(body) || f.holds(body[1:]) {
		t.Fatalf("media frame came back as %+v %v", f, ok)
	}

	for _, mode := range []byte{slotEnd, slotErr} {
		if f, ok := decodeSlot(newSlotFrame(0, mode, nil).encode()); !ok || f.seq != 0 || f.mode != mode {
			t.Errorf("terminal mode %d rejected", mode)
		}
	}
	if _, ok := decodeSlot((slotFrame{seq: 1, mode: slotMedia, size: maxSegmentBytes}).encode()); !ok {
		t.Error("maximum media frame size rejected")
	}

	corrupt, _ := base64.StdEncoding.DecodeString(newSlotFrame(1, slotInline, data).encode())
	corrupt[len(corrupt)-1]++
	for _, broken := range []string{
		"", "not base64", newSlotFrame(1, slotInline, data).encode()[:20], "WERSVlNMMDE=",
		base64.StdEncoding.EncodeToString(corrupt),
		newSlotFrame(1, 0, nil).encode(),
		newSlotFrame(1, slotErr+1, nil).encode(),
		newSlotFrame(-1, slotInline, data).encode(),
		newSlotFrame(1, slotInline, nil).encode(),
		newSlotFrame(1, slotMedia, nil).encode(),
		(slotFrame{seq: 1, mode: slotMedia, size: maxSegmentBytes + 1}).encode(),
		(slotFrame{seq: 1, mode: slotMedia, size: -1}).encode(),
		(slotFrame{seq: 1, mode: slotMedia, size: len(body), data: data}).encode(),
		newSlotFrame(1, slotEnd, data).encode(),
		newSlotFrame(1, slotErr, data).encode(),
		(slotFrame{seq: 1, mode: slotEnd, crc: 1}).encode(),
		(slotFrame{seq: 1, mode: slotErr, data: data}).encode(),
	} {
		if _, ok := decodeSlot(broken); ok {
			t.Errorf("%q decoded", broken)
		}
	}
}

// both sides send more segments than the ring has slots at once, small ones
// that go in descriptions and large ones that go in bodies, then one closes
// and the other reads EOF
func TestSlotConnRoundTrip(t *testing.T) {
	client, server := slotPair(t, newMemSlots(), 4, 64<<10)
	up, down := randomPayload(1<<20), randomPayload(300<<10)
	errs := make(chan error, 2)
	writeAll(client, up, false, errs)
	writeAll(server, down, false, errs)

	for _, c := range []struct {
		conn *Conn
		want []byte
	}{{server, up}, {client, down}} {
		got := make([]byte, len(c.want))
		if _, err := io.ReadFull(c.conn, got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, c.want) {
			t.Fatal("what came is not what was sent")
		}
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	readAll(t, server, nil)
}

// a reader that does not read holds the writer back instead of losing what a
// lap of the ring would overwrite
func TestSlotConnSlowReader(t *testing.T) {
	client, server := slotPair(t, newMemSlots(), 2, 16<<10)
	payload := randomPayload(400 << 10)
	errs := make(chan error, 1)
	writeAll(client, payload, true, errs)

	time.Sleep(200 * time.Millisecond)
	readAll(t, server, payload)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}

// the next segment is asked for at once when it becomes the head, not after
// the wait it had as one that was fetched ahead
func TestSlotLatency(t *testing.T) {
	client, server := slotPair(t, newMemSlots(), 8, 64<<10)
	start := time.Now()
	for range 10 {
		for _, c := range [][2]*Conn{{client, server}, {server, client}} {
			c[0].Write([]byte("ping"))
			if _, err := io.ReadFull(c[1], make([]byte, 4)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("10 round trips took %v", took)
	}
}

// once nothing comes for the eager window only the heads are asked for, not
// the segments that a reader fetches ahead while things come
func TestSlotIdle(t *testing.T) {
	store := newMemSlots()
	client, server := slotPair(t, store, 8, 64<<10)
	for _, c := range [][2]*Conn{{client, server}, {server, client}} {
		c[0].Write([]byte("ping"))
		if _, err := io.ReadFull(c[1], make([]byte, 4)); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	store.mu.Lock()
	before := store.reads
	store.mu.Unlock()
	time.Sleep(500 * time.Millisecond)
	store.mu.Lock()
	reads := store.reads - before
	store.mu.Unlock()
	// two heads, each asked for every 20 ms at most
	if reads > 75 {
		t.Fatalf("an idle session asked for slots %d times in 500 ms", reads)
	}
}

type failingSlots struct {
	*memSlots
}

// setSlot fails for segments, not for the error frame
func (f failingSlots) setSlot(ctx context.Context, id, description string, body []byte) error {
	if frame, ok := decodeSlot(description); ok && (frame.mode == slotInline || frame.mode == slotMedia) {
		return io.ErrUnexpectedEOF
	}
	return f.memSlots.setSlot(ctx, id, description, body)
}

func TestSlotWriterFailure(t *testing.T) {
	client, server := slotPair(t, failingSlots{newMemSlots()}, 2, 16<<10)
	client.Write([]byte("lost"))
	server.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := server.Read(make([]byte, 10)); err == nil || err == io.EOF || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("the reader got %v, not the failure of the writer", err)
	}
	for start := time.Now(); time.Since(start) < 5*time.Second; time.Sleep(time.Millisecond) {
		if _, err := client.Write([]byte("more")); err != nil {
			return
		}
	}
	t.Fatal("Write did not fail")
}

type staleSlots struct {
	*memSlots
	mu     sync.Mutex
	served map[string]bool
}

// readBody gives what a slot held before once, as a body read right after its
// description may
func (s *staleSlots) readBody(ctx context.Context, id string) ([]byte, error) {
	s.mu.Lock()
	first := !s.served[id]
	s.served[id] = true
	s.mu.Unlock()
	if first {
		return []byte("stale"), nil
	}
	return s.memSlots.readBody(ctx, id)
}

func TestSlotStaleBody(t *testing.T) {
	client, server := slotPair(t, &staleSlots{memSlots: newMemSlots(), served: make(map[string]bool)}, 2, 64<<10)
	payload := randomPayload(200 << 10)
	errs := make(chan error, 1)
	go func() {
		_, err := client.Write(payload)
		if err == nil {
			err = client.Close()
		}
		errs <- err
	}()
	readAll(t, server, payload)
}

func TestSlotBurstSplitsInline(t *testing.T) {
	store := newMemSlots()
	client, server := slotPair(t, store, 8, 512<<10)
	payload := randomPayload(90 << 10)
	errs := make(chan error, 1)
	writeAll(client, payload, true, errs)
	readAll(t, server, payload)
	store.mu.Lock()
	bodies := len(store.bodies)
	store.mu.Unlock()
	if bodies != 0 {
		t.Fatalf("a burst of 90 KiB went in %d bodies, want none", bodies)
	}
}

type stuckBodySlots struct {
	*memSlots
	calls atomic.Int32
}

func (s *stuckBodySlots) readBody(ctx context.Context, id string) ([]byte, error) {
	if s.calls.Add(1) == 1 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return s.memSlots.readBody(ctx, id)
}

func TestSlotBodyHedge(t *testing.T) {
	store := &stuckBodySlots{memSlots: newMemSlots()}
	client, server := slotPair(t, store, 2, 64<<10)
	payload := randomPayload(200 << 10)
	errs := make(chan error, 1)
	go func() {
		_, err := client.Write(payload)
		if err == nil {
			err = client.Close()
		}
		errs <- err
	}()
	readAll(t, server, payload)
	if store.calls.Load() < 2 {
		t.Fatalf("the stuck body read was not hedged, %d reads", store.calls.Load())
	}
}

type coolingBodySlots struct {
	*memSlots
	cool  atomic.Bool
	calls atomic.Int32
}

func (s *coolingBodySlots) cooling() bool              { return s.cool.Load() }
func (s *coolingBodySlots) lastRateLimitAt() time.Time { return time.Time{} }

func (s *coolingBodySlots) readBody(ctx context.Context, id string) ([]byte, error) {
	s.calls.Add(1)
	if s.cool.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return []byte("late"), nil
}

// A body whose hedge timer fires during a cooldown must still get its hedge
// once the cooldown ends, instead of waiting out the primary's whole
// no-progress timeout with the timer spent.
func TestSlotBodyHedgeWaitsOutCooldown(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		store := &coolingBodySlots{}
		store.cool.Store(true)
		r := &slotReader{store: store}
		type bodyResult struct {
			body []byte
			err  error
		}
		done := make(chan bodyResult, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			body, err := r.readBodyHedged(ctx, "body", 0)
			done <- bodyResult{body, err}
		}()
		go func() {
			<-time.NewTimer(3 * slotBodyHedge / 2).C
			store.cool.Store(false)
		}()
		start := time.Now()
		select {
		case got := <-done:
			if got.err != nil || string(got.body) != "late" || store.calls.Load() != 2 {
				t.Fatalf("body=%q err=%v reads=%d", got.body, got.err, store.calls.Load())
			}
			if elapsed := time.Since(start); elapsed != 2*slotBodyHedge {
				t.Fatalf("the hedged body took %v, want %v", elapsed, 2*slotBodyHedge)
			}
		case <-time.After(3 * slotBodyHedge):
			cancel()
			synctest.Wait()
			t.Fatal("the hedge never fired after the cooldown ended")
		}
	})
}

func TestTakeSpareSkipsStale(t *testing.T) {
	cfg := &Config{}
	p := params{sessionTTL: 5 * time.Minute}
	st1, err := newLocalStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st2, err := newLocalStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	box := spareBoxFor(cfg)
	t.Cleanup(func() { stopSpares(cfg) })
	box.ch <- spareRing{storage: st1, session: "old", at: time.Now().Add(-time.Hour)}
	box.ch <- spareRing{storage: st2, session: "new", at: time.Now()}
	got, ok := takeSpare(cfg, p)
	if !ok || got.session != "new" {
		t.Fatalf("takeSpare gave %q, %v, want the fresh one", got.session, ok)
	}
	if _, ok := takeSpare(cfg, p); ok {
		t.Fatal("takeSpare returned a second spare")
	}
}

func TestDropSpareLeavesDataForServerCollection(t *testing.T) {
	st, err := newLocalStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	name := sessionPrefix("gone") + "/" + uplinkDir + "/s0"
	if err := st.Put(context.Background(), name, []byte("x")); err != nil {
		t.Fatal(err)
	}
	dropSpare(context.Background(), spareRing{storage: st, session: "gone"})
	if got, err := st.Get(context.Background(), name); err != nil || string(got) != "x" {
		t.Fatalf("dropped spare data was removed before server collection: %q, %v", got, err)
	}
}

type stuckHeadSlots struct {
	*memSlots
	calls atomic.Int32
}

func (s *stuckHeadSlots) readDescription(ctx context.Context, id string) (string, error) {
	if s.calls.Add(1) == 1 {
		<-ctx.Done()
		return "", ctx.Err()
	}
	return s.memSlots.readDescription(ctx, id)
}

func TestSlotHeadReadOverlaps(t *testing.T) {
	client, server := slotPair(t, &stuckHeadSlots{memSlots: newMemSlots()}, 4, 64<<10)
	for _, c := range [][2]*Conn{{client, server}, {server, client}} {
		c[0].Write([]byte("ping"))
		if _, err := io.ReadFull(c[1], make([]byte, 4)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSlotBurstReadsTogether(t *testing.T) {
	client, server := slotPair(t, newMemSlots(), 16, 512<<10)
	payload := randomPayload(8 * slotInlineLimit)
	go client.Write(payload)
	start := time.Now()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("the burst came back changed")
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("a burst of 8 segments took %v to read", took)
	}
}

type lagSlots struct{ *memSlots }

func (s lagSlots) readDescription(ctx context.Context, id string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(200 * time.Millisecond):
	}
	return s.memSlots.readDescription(ctx, id)
}

func (s lagSlots) setSlot(ctx context.Context, id, description string, body []byte) error {
	if _, ok := decodeSlot(description); ok {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(600 * time.Millisecond):
		}
	}
	return s.memSlots.setSlot(ctx, id, description, body)
}

func TestSlotReadAheadBurst(t *testing.T) {
	client, server := slotPair(t, lagSlots{newMemSlots()}, 16, 512<<10)
	payload := randomPayload(8 * slotInlineLimit)
	go client.Write(payload)
	start := time.Now()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("the burst came back changed")
	}
	if took := time.Since(start); took > 1700*time.Millisecond {
		t.Fatalf("a burst of 8 segments took %v to read", took)
	}
	client.Close()
	server.Close()
}

func TestSlotLargeBurstInline(t *testing.T) {
	store := newMemSlots()
	client, server := slotPair(t, store, 16, 512<<10)
	payload := randomPayload(10 * slotInlineLimit)
	go client.Write(payload)
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("the burst came back changed")
	}
	store.mu.Lock()
	bodies := len(store.bodies)
	store.mu.Unlock()
	if bodies != 0 {
		t.Fatalf("a burst of 140 KiB went in %d bodies, want none", bodies)
	}
}

func TestSlotDepthFollowsConcurrency(t *testing.T) {
	for _, c := range []struct{ set, want int }{{100, 100}, {maxConcurrency, maxConcurrency}, {1 << 20, maxConcurrency}} {
		p := paramsFromConfig(&Config{Concurrency: uint32(c.set)})
		r, err := makeRing(context.Background(), newMemSlots(), "session", p)
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Up.Slots) != c.want || len(r.Down.Slots) != c.want {
			t.Fatalf("concurrency %d made %d and %d slots, want %d", c.set, len(r.Up.Slots), len(r.Down.Slots), c.want)
		}
	}
}

func TestSlotDeepRingRoundTrip(t *testing.T) {
	client, server := slotPair(t, newMemSlots(), 64, 64<<10)
	payload := randomPayload(2 << 20)
	errs := make(chan error, 1)
	writeAll(client, payload, true, errs)
	readAll(t, server, payload)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}

type pollSlots struct {
	*memSlots
	pmu    sync.Mutex
	polled map[string]bool
}

func (s *pollSlots) readDescription(ctx context.Context, id string) (string, error) {
	s.pmu.Lock()
	s.polled[id] = true
	s.pmu.Unlock()
	return s.memSlots.readDescription(ctx, id)
}

func TestSlotReaderReachesPastWindow(t *testing.T) {
	store := &pollSlots{memSlots: newMemSlots(), polled: make(map[string]bool)}
	p := paramsFromConfig(&Config{FlushIntervalMs: 1, PollIntervalMs: 1, MaxPollIntervalMs: 20, EagerWindowMs: 50, Concurrency: 64})
	ring, err := makeRing(context.Background(), store, "session", p)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	put := func(seq int64) {
		slot := ring.Down.Slots[seq%int64(len(ring.Down.Slots))]
		if err := store.setSlot(ctx, slot, newSlotFrame(seq, slotInline, []byte{byte(seq)}).encode(), nil); err != nil {
			t.Fatal(err)
		}
	}
	reader := newSlotReader(ctx, store, ring.Down, p, nil)
	next := func() []byte {
		select {
		case chunk := <-reader.chunks():
			return chunk
		case <-time.After(10 * time.Second):
			t.Fatal("no segment came")
			return nil
		}
	}
	put(0)
	if got := next(); !bytes.Equal(got, []byte{0}) {
		t.Fatalf("segment 0 came as %v", got)
	}
	for seq := int64(2); seq <= 40; seq++ {
		put(seq)
	}
	last := ring.Down.Slots[40]
	deadline := time.Now().Add(10 * time.Second)
	for {
		reader.Wake()
		store.pmu.Lock()
		polled := store.polled[last]
		store.pmu.Unlock()
		if polled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("segment 40 was never asked for while segment 1 was missing")
		}
		time.Sleep(10 * time.Millisecond)
	}
	put(1)
	for seq := int64(1); seq <= 40; seq++ {
		if got := next(); !bytes.Equal(got, []byte{byte(seq)}) {
			t.Fatalf("segment %d came as %v", seq, got)
		}
	}
}

type bulkPermitSlots struct {
	*memSlots
	finish   chan struct{}
	writes   atomic.Int32
	throttle atomic.Int64
	cool     atomic.Bool
}

func (s *bulkPermitSlots) throttled() int64 { return s.throttle.Load() }
func (s *bulkPermitSlots) cooling() bool    { return s.cool.Load() }
func (s *bulkPermitSlots) setSlot(ctx context.Context, id, description string, body []byte) error {
	if f, ok := decodeSlot(description); ok && (f.mode == slotInline || f.mode == slotMedia) {
		s.writes.Add(1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.finish:
		}
	}
	return s.memSlots.setSlot(ctx, id, description, body)
}

type countSlots struct {
	*memSlots
	cur  atomic.Int32
	peak atomic.Int32
}

func (s *countSlots) setSlot(ctx context.Context, id, description string, body []byte) error {
	if _, ok := decodeSlot(description); !ok {
		return s.memSlots.setSlot(ctx, id, description, body)
	}
	n := s.cur.Add(1)
	for {
		p := s.peak.Load()
		if n <= p || s.peak.CompareAndSwap(p, n) {
			break
		}
	}
	time.Sleep(20 * time.Millisecond)
	s.cur.Add(-1)
	return s.memSlots.setSlot(ctx, id, description, body)
}

func TestSlotWriteInflightCap(t *testing.T) {
	store := &countSlots{memSlots: newMemSlots()}
	client, server := slotPair(t, store, 48, 512<<10)
	payload := randomPayload(60 * slotInlineLimit)
	errs := make(chan error, 1)
	go func() {
		for off := 0; off < len(payload); off += slotInlineLimit {
			if _, err := client.Write(payload[off : off+slotInlineLimit]); err != nil {
				errs <- err
				return
			}
		}
		errs <- nil
	}()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatal(err)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("the payload came back changed")
	}
	if p := int(store.peak.Load()); p > slotWriteInflight {
		t.Fatalf("%d segments were written at once, want at most %d", p, slotWriteInflight)
	}
}

func TestSlotBurstInlineWaves(t *testing.T) {
	for _, c := range []struct {
		pieces int
		media  bool
	}{{18, false}, {19, true}} {
		store := newMemSlots()
		client, server := slotPair(t, store, 16, 512<<10)
		payload := randomPayload(c.pieces * slotInlineLimit)
		go client.Write(payload)
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(server, got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("the burst came back changed")
		}
		store.mu.Lock()
		bodies := len(store.bodies)
		store.mu.Unlock()
		if (bodies > 0) != c.media {
			t.Fatalf("a burst of %d pieces went in %d bodies, media wanted %v", c.pieces, bodies, c.media)
		}
	}
}

func TestSlotRampedBurstSplitsInline(t *testing.T) {
	for _, c := range []struct {
		pieces int
		media  bool
	}{{1 + slotWriteInflight/2, false}, {2 + slotWriteInflight/2, true}} {
		t.Run(strconv.Itoa(c.pieces), func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				store := &bulkPermitSlots{memSlots: newMemSlots(), finish: make(chan struct{})}
				p := paramsFromConfig(&Config{Concurrency: 64})
				ring, err := makeRing(ctx, store, "ramped", p)
				if err != nil {
					t.Fatal(err)
				}
				w := newSlotWriter(ctx, store, ring.Up, p)
				w.mu.Lock()
				w.size = p.segmentBytes
				w.mu.Unlock()
				// all permits but one stay with uploads that never finish
				for range slotWriteInflight - 1 {
					w.slots <- struct{}{}
				}
				payload := randomPayload(c.pieces * slotInlineLimit)
				if _, err := w.Write(payload); err != nil {
					t.Fatal(err)
				}
				segments := c.pieces
				if c.media {
					segments = 1
				}
				// a tick for the burst to settle, then a segment every tick
				deadline := time.After(time.Duration(segments+2) * p.flushInterval)
				for range segments {
					select {
					case store.finish <- struct{}{}:
					case <-deadline:
						t.Fatalf("%d pieces: %d of %d uploads began in time", c.pieces, store.writes.Load(), segments)
					}
				}
				synctest.Wait()
				w.mu.Lock()
				sent := w.seq
				w.mu.Unlock()
				store.mu.Lock()
				defer store.mu.Unlock()
				var got []byte
				for seq := range sent {
					id := w.slotID(seq)
					frame, ok := decodeSlot(store.descs[id])
					if !ok || frame.seq != seq {
						t.Fatalf("invalid frame for segment %d", seq)
					}
					got = append(got, frame.data...)
					got = append(got, store.bodies[id]...)
				}
				if sent != int64(segments) || !bytes.Equal(got, payload) || (len(store.bodies) > 0) != c.media {
					t.Fatalf("%d pieces: %d bytes in %d segments and %d bodies", c.pieces, len(got), sent, len(store.bodies))
				}
			})
		})
	}
}

type orderedSlots struct {
	*memSlots
	wm                              string
	entered, release, acked, reused chan struct{}
	enterOnce, ackOnce, reuseOnce   sync.Once
	uploading, overlap              atomic.Bool
	fail                            bool
}

func (s *orderedSlots) write(ctx context.Context, id, name, description string, body []byte, create bool) error {
	if f, ok := decodeSlot(description); ok {
		if f.seq == 0 && f.mode != slotErr {
			s.enterOnce.Do(func() { close(s.entered) })
			select {
			case <-s.release:
				s.uploading.Store(false)
				if s.fail {
					return io.ErrUnexpectedEOF
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if f.seq == 2 {
			s.overlap.Store(s.uploading.Load())
			s.reuseOnce.Do(func() { close(s.reused) })
		}
	}
	var err error
	if create {
		err = s.memSlots.createSlot(ctx, id, name, description, body)
	} else {
		err = s.memSlots.setSlot(ctx, id, description, body)
	}
	if err == nil && id == s.wm && description == "1" {
		s.ackOnce.Do(func() { close(s.acked) })
	}
	return err
}

func (s *orderedSlots) setSlot(ctx context.Context, id, description string, body []byte) error {
	return s.write(ctx, id, "", description, body, false)
}

func (s *orderedSlots) createSlot(ctx context.Context, id, name, description string, body []byte) error {
	return s.write(ctx, id, name, description, body, true)
}

func TestSlotZeroUploadPrecedesReuse(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		for _, fail := range []bool{false, true} {
			t.Run(strconv.FormatBool(lazy)+"/failure="+strconv.FormatBool(fail), func(t *testing.T) {
				driveClock(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					p := paramsFromConfig(&Config{SegmentBytes: 1, Concurrency: 2})
					table := slotTable{Slots: []string{"s0", "s1"}, Watermark: "wm", name: "ordered", lazy: lazy}
					s := &orderedSlots{
						memSlots: newMemSlots(), wm: table.Watermark, fail: fail,
						entered: make(chan struct{}), release: make(chan struct{}), acked: make(chan struct{}), reused: make(chan struct{}),
					}
					if !lazy {
						for _, id := range append(table.Slots, table.Watermark) {
							s.descs[id] = ""
						}
					}
					s.uploading.Store(true)
					w := newSlotWriter(ctx, s, table, p)
					defer func() { cancel(); w.wg.Wait(); synctest.Wait() }()
					w.first = make(chan []byte, 1)
					if _, err := w.Write([]byte("a")); err != nil {
						t.Fatal(err)
					}
					<-s.entered
					r := newSlotReader(ctx, s, table, p, <-w.first)
					if string(<-r.chunks()) != "a" {
						t.Fatal("announcement lost seq 0")
					}
					if _, err := w.Write([]byte("b")); err != nil {
						t.Fatal(err)
					}
					if string(<-r.chunks()) != "b" {
						t.Fatal("seq 1 missing")
					}
					<-s.acked
					if _, err := w.Write([]byte("c")); err != nil {
						t.Fatal(err)
					}
					<-time.NewTimer(p.maxPollInterval).C
					synctest.Wait()
					select {
					case <-s.reused:
						t.Error("slot 0 was reused while upload 0 was held")
					default:
					}
					close(s.release)
					w.wg.Wait()
					desc, err := s.readDescription(ctx, table.Slots[0])
					frame, ok := decodeSlot(desc)
					if fail {
						select {
						case <-s.reused:
							t.Fatal("slot 0 was reused after its predecessor failed")
						default:
						}
						if err != nil || !ok || frame.seq != 0 || frame.mode != slotErr {
							t.Fatalf("failed predecessor marker lost: seq=%d mode=%d err=%v", frame.seq, frame.mode, err)
						}
						if err := w.Close(); err == nil {
							t.Fatal("Close lost the predecessor upload failure")
						}
						return
					}
					select {
					case <-s.reused:
					default:
						t.Fatal("slot 0 was not reused after upload completion and ACK")
					}
					if err != nil || !ok || frame.seq != 2 || s.overlap.Load() {
						t.Fatalf("slot reuse after upload success: seq=%d overlap=%v err=%v", frame.seq, s.overlap.Load(), err)
					}
				})
			})
		}
	}
}

func TestSlotHuntPreservesSiblingError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		want := errors.New("description failed")
		for _, matching := range []bool{false, true} {
			r := &slotReader{params: params{eagerWindow: time.Hour}, head: 2, activeAt: time.Now(), huntCeiling: time.Hour, stallTimeout: time.Hour}
			pending := slotDescriptionHunt{results: make(chan slotDescriptionResult, 3), outstanding: 3}
			pending.results <- slotDescriptionResult{err: want}
			pending.results <- slotDescriptionResult{description: newSlotFrame(1, slotInline, []byte("old")).encode()}
			seq := int64(1)
			if matching {
				seq = 2
			}
			desc := newSlotFrame(seq, slotInline, []byte("frame")).encode()
			pending.results <- slotDescriptionResult{description: desc}
			got, err := r.huntSlotDescription(context.Background(), "slot", 2, nil, &pending)
			if matching && (got != desc || err != nil) || !matching && err != want {
				t.Fatalf("matching=%v: sibling error lost: %v", matching, err)
			}
		}
	})
}

func TestSlotAcknowledgedPredecessorHasCloseBound(t *testing.T) {
	w := &slotWriter{ctx: context.Background(), table: slotTable{Slots: []string{"slot"}}, closed: true, acked: 0, progress: time.Now().Add(-slotStallTimeout - time.Second)}
	if err := w.awaitPrevious(1, &slotWriteResult{done: make(chan struct{})}); err == nil {
		t.Fatal("unfinished acknowledged predecessor ignored close stall bound")
	}
}

func TestSlotCloseBoundsAdmittedWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		w := &slotWriter{ctx: ctx, cancel: cancel, flushStop: make(chan struct{}), flushDone: make(chan struct{})}
		close(w.flushDone)
		w.wg.Add(1)
		cancel()
		results := make(chan error, 2)
		for range 2 {
			go func() { results <- w.Close() }()
		}
		for range 2 {
			if err := <-results; err != context.Canceled {
				t.Fatalf("Close: %v", err)
			}
		}
		w.wg.Done()
	})
}

type closeDrainSlots struct {
	*memSlots
	stalled bool
	ended   bool
}

func (s *closeDrainSlots) setSlot(ctx context.Context, id, description string, body []byte) error {
	frame, _ := decodeSlot(description)
	if frame.mode == slotEnd {
		s.ended = true
	} else if s.stalled {
		<-ctx.Done()
		return ctx.Err()
	} else {
		timer := time.NewTimer(slotStallTimeout / 2)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return s.memSlots.setSlot(ctx, id, description, body)
}

type failedObservationSlots struct {
	*memSlots
	err error
}

func (s *failedObservationSlots) readDescription(context.Context, string) (string, error) {
	return "", s.err
}

func (s *failedObservationSlots) setSlot(context.Context, string, string, []byte) error {
	return s.err
}

func TestSlotDescriptionErrorsStopReader(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store := &failedObservationSlots{memSlots: newMemSlots(), err: io.ErrUnexpectedEOF}
		r := &slotReader{ctx: ctx, store: store, table: slotTable{Slots: []string{"slot"}}, params: paramsFromConfig(&Config{}), ch: make(chan []byte, 1), wake: make(chan struct{}, 1), acks: make(chan struct{}, 1), grow: make(chan struct{}, 1), active: make(chan struct{}), stop: cancel, huntCeiling: slotHuntCeiling, huntFloor: slotHuntFloor, activeAt: time.Now(), frontier: -1}
		r.run()
		r.wait()
		if _, ok := <-r.ch; ok || !errors.Is(r.Err(), io.ErrUnexpectedEOF) {
			t.Fatalf("reader did not close with the description error: %v", r.Err())
		}
	})
}

func TestSlotAcknowledgmentErrorsStopReader(t *testing.T) {
	for _, err := range []error{authError{code: "invalid_grant"}, io.ErrUnexpectedEOF} {
		driveClock(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := &slotReader{ctx: ctx, stop: cancel, store: &failedObservationSlots{memSlots: newMemSlots(), err: err}, table: slotTable{Slots: []string{"slot"}, Watermark: "watermark"}, head: 1, ackRetry: slotAckRetry, ackDelay: ackDelay, acks: make(chan struct{}, 1)}
			r.ackLoop()
			if ctx.Err() == nil || !errors.Is(r.Err(), err) {
				t.Fatalf("acknowledgment error was lost: %v", r.Err())
			}
		})
	}
}

func TestSlotClosePreservesPendingProgress(t *testing.T) {
	for _, pending := range []bool{false, true} {
		driveClock(t, func(t *testing.T) {
			store := &closeDrainSlots{memSlots: newMemSlots(), stalled: pending}
			store.descs["slot"] = ""
			w := newSlotWriter(context.Background(), store, slotTable{Slots: []string{"slot"}}, paramsFromConfig(&Config{SegmentBytes: slotInlineLimit, Concurrency: 1}))
			w.ackMu.Lock()
			w.progress = time.Now().Add(-slotStallTimeout)
			w.ackMu.Unlock()
			if pending {
				if _, err := w.Write(make([]byte, slotInlineLimit)); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
			}
			w.ackMu.Lock()
			if pending && time.Since(w.progress) != 0 {
				t.Fatal("admission failed to initialize progress")
			}
			w.progress = time.Now().Add(-slotStallTimeout)
			w.ackMu.Unlock()
			started := time.Now()
			err := w.Close()
			<-w.flushDone
			w.wg.Wait()
			synctest.Wait()
			if pending {
				if err != context.DeadlineExceeded || time.Now() != started {
					t.Fatalf("stalled pending drain received a new budget: %v", err)
				}
			} else if err != nil || !store.ended {
				t.Fatalf("idle terminal write had no fresh budget: %v", err)
			}
		})
	}
}

type controlledSlots struct {
	*memSlots
	put  func(context.Context, string, string, []byte) error
	desc func(context.Context, string) (string, error)
}

func (s *controlledSlots) setSlot(ctx context.Context, id, description string, body []byte) error {
	if s.put != nil {
		return s.put(ctx, id, description, body)
	}
	return s.memSlots.setSlot(ctx, id, description, body)
}

func (s *controlledSlots) readDescription(ctx context.Context, id string) (string, error) {
	if s.desc != nil {
		return s.desc(ctx, id)
	}
	return s.memSlots.readDescription(ctx, id)
}

func TestSlotFailureWakesAdmissionWithoutReturningPermit(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		w := newSlotWriter(context.Background(), newMemSlots(), slotTable{Slots: []string{"slot"}}, paramsFromConfig(&Config{}))
		w.slots <- struct{}{}
		done := make(chan error, 1)
		go func() { _, err := w.Write(make([]byte, slotInlineLimit)); done <- err }()
		synctest.Wait()
		failure := io.ErrUnexpectedEOF
		started := time.Now()
		w.fail(failure)
		if err := <-done; err != failure || time.Now() != started || len(w.slots) != 1 {
			t.Fatalf("admission failed to wake without freeing the permit: %v", err)
		}
		<-w.slots
		w.cancel()
		w.wait()
	})
}

func TestConnCleanupJoinsActualSlotWriterCompletion(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		entered, release, cleaned := make(chan struct{}), make(chan struct{}), make(chan struct{})
		store := &controlledSlots{memSlots: newMemSlots(), put: func(ctx context.Context, _, _ string, _ []byte) error {
			close(entered)
			<-ctx.Done()
			<-release
			return ctx.Err()
		}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		w := newSlotWriter(ctx, store, slotTable{Slots: []string{"slot"}}, paramsFromConfig(&Config{}))
		if _, err := w.Write(make([]byte, slotInlineLimit)); err != nil {
			t.Fatal(err)
		}
		<-entered
		w.ackMu.Lock()
		w.progress = time.Now().Add(-slotStallTimeout)
		w.ackMu.Unlock()
		c := &Conn{ctx: ctx, cancel: cancel, writer: w, closing: make(chan struct{}), readDeadline: newPipeDeadline(), writeDeadline: newPipeDeadline(), onClose: func() { close(cleaned) }}
		started := time.Now()
		if err := c.Close(); err != context.DeadlineExceeded || time.Now() != started {
			t.Fatalf("public Close received a new budget: %v", err)
		}
		synctest.Wait()
		select {
		case <-cleaned:
			t.Fatal("cleanup overlapped writer teardown")
		default:
		}
		close(release)
		w.wait()
		<-cleaned
	})
}

func TestSlotWriterWatermarkAuthFailureIsTerminal(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		failure := authError{code: "invalid_grant"}
		calls := 0
		store := &controlledSlots{memSlots: newMemSlots(), desc: func(context.Context, string) (string, error) { calls++; return "", failure }}
		w := newSlotWriter(context.Background(), store, slotTable{Slots: []string{"slot"}, Watermark: "watermark"}, paramsFromConfig(&Config{}))
		started := time.Now()
		if err := w.awaitPrevious(1, nil); err != failure || calls != 1 || time.Now() != started {
			t.Fatalf("authentication error retried or replaced: %v", err)
		}
		w.cancel()
		w.wait()
	})
}

func TestSlotFreshPresenceResetsAbsenceBudget(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		requests := make(chan struct{}, 1)
		results := make(chan slotDescriptionResult)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store := &controlledSlots{memSlots: newMemSlots(), desc: func(ctx context.Context, _ string) (string, error) {
			requests <- struct{}{}
			select {
			case res := <-results:
				return res.description, res.err
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}}
		r := &slotReader{ctx: ctx, store: store, table: slotTable{Slots: []string{"slot"}}, params: paramsFromConfig(&Config{}), head: 1, frontier: -1, active: make(chan struct{}), huntFloor: slotHuntFloor, huntCeiling: slotHuntCeiling, stallTimeout: slotStallTimeout}
		f := &slotFetch{ready: make(chan struct{}), promoted: make(chan struct{}), media: make(chan struct{}), near: make(chan struct{})}
		done := make(chan struct{})
		go func() { defer close(done); r.fetch(ctx, 1, 0, f) }()
		<-requests
		results <- slotDescriptionResult{err: errNotFound}
		<-requests
		<-time.NewTimer(slotGoneTimeout).C
		results <- slotDescriptionResult{description: newSlotFrame(0, slotInline, []byte("old")).encode()}
		<-requests
		results <- slotDescriptionResult{err: errNotFound}
		synctest.Wait()
		select {
		case <-f.ready:
			t.Fatalf("fresh presence did not reset absence budget: %v", f.err)
		default:
		}
		cancel()
		<-done
		r.wait()
	})
}

func TestSlotCloseUsesDrainProgress(t *testing.T) {
	for _, stalled := range []bool{false, true} {
		t.Run(strconv.FormatBool(stalled), func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				store := &closeDrainSlots{memSlots: newMemSlots(), stalled: stalled}
				store.descs["slot"] = ""
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				w := newSlotWriter(ctx, store, slotTable{Slots: []string{"slot"}}, paramsFromConfig(&Config{SegmentBytes: slotInlineLimit, Concurrency: 1}))
				w.mu.Lock()
				w.buf = make([]byte, 3*slotInlineLimit)
				w.mu.Unlock()
				w.ackMu.Lock()
				w.acked = 100
				w.ackMu.Unlock()
				c := &Conn{ctx: ctx, cancel: cancel, writer: w, closing: make(chan struct{}), readDeadline: newPipeDeadline(), writeDeadline: newPipeDeadline()}
				started := time.Now()
				err := c.Close()
				synctest.Wait()
				if stalled {
					if err == nil || time.Since(started) != slotStallTimeout || store.ended {
						t.Fatalf("stalled drain: elapsed=%v ended=%v err=%v", time.Since(started), store.ended, err)
					}
				} else if err != nil || time.Since(started) <= slotStallTimeout || !store.ended || store.updates != 4 {
					t.Fatalf("advancing drain: elapsed=%v writes=%d ended=%v err=%v", time.Since(started), store.updates, store.ended, err)
				}
			})
		})
	}
}
