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

	body := randomPayload(100)
	f, ok = decodeSlot(newSlotFrame(8, slotMedia, body).encode())
	if !ok || f.seq != 8 || f.mode != slotMedia || f.data != nil || !f.holds(body) || f.holds(body[1:]) {
		t.Fatalf("media frame came back as %+v %v", f, ok)
	}

	corrupt, _ := base64.StdEncoding.DecodeString(newSlotFrame(1, slotInline, data).encode())
	corrupt[len(corrupt)-1]++
	for _, broken := range []string{
		"", "not base64", newSlotFrame(1, slotInline, data).encode()[:20], "WERSVlNMMDE=",
		base64.StdEncoding.EncodeToString(corrupt),
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

func TestDropSpareDeletesSession(t *testing.T) {
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

type promotionSlots struct {
	*memSlots
	first time.Duration
	calls atomic.Int32
}

func (s *promotionSlots) readDescription(ctx context.Context, id string) (string, error) {
	delay := 340 * time.Millisecond
	if s.calls.Add(1) == 1 {
		delay = s.first
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-timer.C:
		return s.memSlots.readDescription(ctx, id)
	}
}

func TestSlotPromotesPendingRead(t *testing.T) {
	for _, first := range []time.Duration{200 * time.Millisecond, 1100 * time.Millisecond} {
		t.Run(first.String(), func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				store := &promotionSlots{memSlots: newMemSlots(), first: first}
				want := newSlotFrame(1, slotInline, []byte("tail")).encode()
				store.descs["tail"] = want
				r := &slotReader{
					store: store, huntCeiling: slotHuntCeiling,
					params: paramsFromConfig(&Config{}), activeAt: time.Now(),
				}
				promoted := make(chan struct{})
				done := make(chan string, 1)
				start := time.Now()
				go func() {
					description, _ := r.readSlotDescription(context.Background(), "tail", 1, promoted)
					done <- description
				}()
				<-time.NewTimer(100 * time.Millisecond).C
				if store.calls.Load() != 1 {
					t.Fatal("tail read overlapped before promotion")
				}
				r.mu.Lock()
				r.head = 1
				r.mu.Unlock()
				close(promoted)
				got := <-done
				synctest.Wait()
				elapsed := min(first, 100*time.Millisecond+slotHuntCeiling+340*time.Millisecond)
				if got != want || time.Since(start) != elapsed || store.calls.Load() != 3 {
					t.Fatalf("valid=%v elapsed=%v calls=%d", got == want, time.Since(start), store.calls.Load())
				}
			})
		})
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

func TestSlotBulkPermits(t *testing.T) {
	payload := randomPayload((slotBulkInflight + 1) * defaultSegmentBytes)
	for _, mode := range []string{"cold", "warm", "limited", "throttled", "cooling", "small-ring"} {
		t.Run(mode, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				store := &bulkPermitSlots{memSlots: newMemSlots(), finish: make(chan struct{})}
				p := paramsFromConfig(&Config{Concurrency: slotBulkDepth})
				if mode == "small-ring" {
					p.concurrency = 16
				}
				ring, err := makeRing(ctx, store, "bulk", p)
				if err != nil {
					t.Fatal(err)
				}
				w := newSlotWriter(ctx, store, ring.Down, p)
				if _, err := w.Write(payload[:p.segmentBytes]); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				w.mu.Lock()
				if mode != "cold" {
					w.bulkSegments = 2 * p.concurrency
					w.seq = int64(w.bulkSegments + 1)
					w.ackMu.Lock()
					w.acked = w.seq
					w.ackMu.Unlock()
				}
				w.bulkLimited = mode == "limited"
				w.mu.Unlock()
				if mode == "throttled" {
					store.throttle.Add(1)
				}
				store.cool.Store(mode == "cooling")
				closing, done := make(chan struct{}), make(chan error, 1)
				go func() { _, err := w.writeUntil(payload, nil, closing); done <- err }()
				synctest.Wait()
				want := slotWriteInflight
				if mode == "warm" {
					want = slotBulkInflight
				}
				if got := int(store.writes.Load()); got != want {
					t.Fatalf("uploads=%d, want %d", got, want)
				}
				if mode == "warm" {
					store.cool.Store(true)
					for range slotBulkInflight - slotWriteInflight {
						store.finish <- struct{}{}
					}
					synctest.Wait()
					if int(store.writes.Load()) != want {
						t.Fatal("cooling admitted an upload above the base window")
					}
					store.finish <- struct{}{}
					synctest.Wait()
					if int(store.writes.Load()) != want+1 {
						t.Fatal("returning a permit did not wake the writer")
					}
				}
				close(closing)
				if err := <-done; err != errWriteStopped {
					t.Fatalf("stopped write: %v", err)
				}
				cancel()
				w.wg.Wait()
			})
		})
	}
}

type bulkPrefetchSlots struct {
	*memSlots
	start       chan struct{}
	seen        chan struct{}
	last        string
	bodiesReady map[string]chan struct{}
	calls       atomic.Int32
}

func (s *bulkPrefetchSlots) readDescription(ctx context.Context, id string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-s.start:
		description, err := s.memSlots.readDescription(ctx, id)
		if id == s.last {
			select {
			case s.seen <- struct{}{}:
			default:
			}
		}
		return description, err
	}
}

func (s *bulkPrefetchSlots) readBody(ctx context.Context, id string) ([]byte, error) {
	s.calls.Add(1)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.bodiesReady[id]:
		return s.memSlots.readBody(ctx, id)
	}
}

func TestSlotBulkPrefetch(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		t.Run(strconv.FormatBool(bulk), func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				store := &bulkPrefetchSlots{memSlots: newMemSlots(), start: make(chan struct{}), seen: make(chan struct{}, 1), bodiesReady: make(map[string]chan struct{})}
				p := paramsFromConfig(&Config{Concurrency: slotBulkDepth})
				ring, err := makeRing(ctx, store, "prefetch", p)
				if err != nil {
					t.Fatal(err)
				}
				payload := randomPayload(slotInlineLimit + 1)
				store.last = ring.Down.Slots[p.concurrency-1]
				for i, id := range ring.Down.Slots {
					store.bodiesReady[id] = make(chan struct{})
					if err := store.setSlot(ctx, id, newSlotFrame(int64(i), slotMedia, payload).encode(), payload); err != nil {
						t.Fatal(err)
					}
				}
				r := newSlotReader(ctx, store, ring.Down, p, nil)
				if bulk {
					r.mu.Lock()
					r.bulkBytes = int64(p.concurrency) * int64(p.segmentBytes)
					r.mu.Unlock()
				}
				close(store.start)
				<-store.seen
				synctest.Wait()
				want := p.concurrency
				if bulk {
					want = slotBodyAhead
				}
				r.mu.Lock()
				frontier := r.frontier
				r.mu.Unlock()
				if int(store.calls.Load()) != want || frontier != int64(p.concurrency-1) {
					t.Fatalf("bodies=%d frontier=%d", store.calls.Load(), frontier)
				}
				close(store.bodiesReady[ring.Down.Slots[0]])
				synctest.Wait()
				if bulk && int(store.calls.Load()) != want+1 {
					t.Fatal("head advancement did not admit the next body")
				}
				for _, id := range ring.Down.Slots[1:] {
					close(store.bodiesReady[id])
				}
				synctest.Wait()
				<-time.NewTimer(r.eagerWindow + r.huntCeiling).C
				r.Wake()
				r.mu.Lock()
				remaining := r.bulkBytes
				r.mu.Unlock()
				if remaining != 0 {
					t.Fatal("idle reader retained its bulk window")
				}
				cancel()
				synctest.Wait()
			})
		})
	}
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
					<-s.reused
					w.wg.Wait()
					desc, err := s.readDescription(ctx, table.Slots[0])
					frame, ok := decodeSlot(desc)
					if err != nil || !ok || frame.seq != 2 || s.overlap.Load() {
						t.Fatalf("slot reuse after upload success/failure: seq=%d overlap=%v err=%v", frame.seq, s.overlap.Load(), err)
					}
				})
			})
		}
	}
}
