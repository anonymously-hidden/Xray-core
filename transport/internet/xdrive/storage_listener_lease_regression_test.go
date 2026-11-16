package xdrive

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	"github.com/xtls/xray-core/transport/internet/stat"
)

type leaseRegressionStorage struct {
	mu            sync.Mutex
	closed        bool
	closes        atomic.Int32
	deletes       atomic.Int32
	inflight      atomic.Int32
	peak          atomic.Int32
	entries       []Entry
	objects       map[string][]byte
	deleteStarted chan string
	deleteGate    <-chan struct{}
	closeStarted  chan struct{}
	closeGate     <-chan struct{}
	listStarted   chan struct{}
	listExit      <-chan struct{}
}

func (s *leaseRegressionStorage) Put(ctx context.Context, name string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errSessionClosed
	}
	if s.objects == nil {
		s.objects = make(map[string][]byte)
	}
	s.objects[name] = append([]byte(nil), data...)
	return nil
}

func (s *leaseRegressionStorage) Get(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errSessionClosed
	}
	if data, ok := s.objects[name]; ok {
		return append([]byte(nil), data...), nil
	}
	return nil, errNotFound
}

func (s *leaseRegressionStorage) List(ctx context.Context, _ string) ([]Entry, error) {
	if s.listStarted != nil {
		close(s.listStarted)
		<-ctx.Done()
		<-s.listExit
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errSessionClosed
	}
	return append([]Entry(nil), s.entries...), nil
}

func (s *leaseRegressionStorage) Delete(ctx context.Context, name string) error {
	s.deletes.Add(1)
	n := s.inflight.Add(1)
	defer s.inflight.Add(-1)
	for old := s.peak.Load(); n > old && !s.peak.CompareAndSwap(old, n); old = s.peak.Load() {
	}
	if s.deleteStarted != nil {
		s.deleteStarted <- name
	}
	if s.deleteGate != nil {
		select {
		case <-s.deleteGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errSessionClosed
	}
	delete(s.objects, name)
	return nil
}

func (s *leaseRegressionStorage) Close() error {
	s.closes.Add(1)
	if s.closeStarted != nil {
		close(s.closeStarted)
	}
	if s.closeGate != nil {
		<-s.closeGate
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func leaseRegressionListener(storage Storage) *Listener {
	ctx, cancel := context.WithCancel(context.Background())
	return &Listener{
		ctx: ctx, cancel: cancel, storage: storage, params: paramsFromConfig(&Config{SessionTtlSeconds: 1}),
		active: make(map[string]bool), handled: make(map[string]time.Time), idleSince: make(map[string]time.Time),
	}
}

func TestStorageLeaseRegressionSiblingListenerSessionRestart(t *testing.T) {
	cfg := &Config{Service: "lease-test", RemoteFolder: t.Name()}
	root := &leaseRegressionStorage{}
	builds := 0
	build := func() (Storage, error) {
		builds++
		if builds == 1 {
			return root, nil
		}
		return &leaseRegressionStorage{}, nil
	}
	first, err := sharedStorage(nil, cfg, build)
	if err != nil {
		t.Fatal(err)
	}
	second, err := sharedStorage(nil, cfg, build)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("acquisitions share a close token")
	}
	l1, l2 := leaseRegressionListener(first), leaseRegressionListener(second)
	child := l1.newSessionConn(context.Background(), "child", nil)
	if child == nil {
		t.Fatal("listener failed to acquire child")
	}
	childStorage, err := acquireSessionStorage(second)
	if err != nil {
		t.Fatal(err)
	}
	if err := l1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Put(context.Background(), "sibling", nil); err != nil {
		t.Fatalf("sibling after close: %v", err)
	}
	if err := childStorage.Put(context.Background(), "child", nil); err != nil {
		t.Fatalf("child after parent close: %v", err)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
	if root.closes.Load() != 0 {
		t.Fatal("root closed with live children")
	}
	child.Interrupt()
	childStorage.Close()
	if root.closes.Load() != 1 {
		t.Fatalf("final close count %d", root.closes.Load())
	}
	restarted, err := sharedStorage(nil, cfg, build)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if builds != 2 {
		t.Fatalf("restart reused closed owner: builds=%d", builds)
	}
	if err := restarted.Put(context.Background(), "restart", nil); err != nil {
		t.Fatal(err)
	}
}

func TestStorageLeaseRegressionServeSiblingFinalRestart(t *testing.T) {
	var roots []*leaseRegressionStorage
	factory := func(*Config) (Storage, error) {
		s := &leaseRegressionStorage{}
		roots = append(roots, s)
		return s, nil
	}
	old := testStorageFactory.Swap(&factory)
	defer testStorageFactory.Store(old)
	settings := &internet.MemoryStreamConfig{ProtocolSettings: &Config{Service: "lease-test", RemoteFolder: t.Name()}}
	serve := func() *Listener {
		listener, err := Serve(context.Background(), xnet.LocalHostIP, 0, settings, func(stat.Connection) {})
		if err != nil {
			t.Fatal(err)
		}
		return listener.(*Listener)
	}
	first, second := serve(), serve()
	child, err := acquireSessionStorage(first.storage)
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	if err := second.storage.Put(context.Background(), "sibling", nil); err != nil {
		t.Fatal(err)
	}
	second.Close()
	if err := child.Put(context.Background(), "orphan", nil); err != nil {
		t.Fatal(err)
	}
	if roots[0].closes.Load() != 0 {
		t.Fatal("listeners terminated child lease")
	}
	child.Close()
	restarted := serve()
	if len(roots) != 2 {
		t.Fatal("Serve restart reused terminal owner")
	}
	restarted.Close()
	if roots[0].closes.Load() != 1 || roots[1].closes.Load() != 1 {
		t.Fatal("Serve owners not finalized exactly once")
	}
}

func TestStorageLeaseRegressionTransferAndIdempotentRelease(t *testing.T) {
	root := &leaseRegressionStorage{}
	storage := ownedStorage(root)
	session, err := transferSessionStorage(storage)
	if err != nil {
		t.Fatal(err)
	}
	if root.closes.Load() != 0 {
		t.Fatal("transfer closed root")
	}
	storage.Close()
	if err := session.Put(context.Background(), "live", nil); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() { session.Close() })
	}
	wg.Wait()
	if root.closes.Load() != 1 {
		t.Fatalf("close count %d", root.closes.Load())
	}
	if _, err := acquireSessionStorage(storage); err == nil {
		t.Fatal("closed acquisition produced a child")
	}
}

func TestStorageLeaseRegressionAtomicFinalEviction(t *testing.T) {
	cfg := &Config{Service: "lease-test", RemoteFolder: t.Name()}
	gate := make(chan struct{})
	old := &leaseRegressionStorage{closeStarted: make(chan struct{}), closeGate: gate}
	first, err := sharedStorage(nil, cfg, func() (Storage, error) { return old, nil })
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- first.Close() }()
	<-old.closeStarted
	fresh := &leaseRegressionStorage{}
	second, err := sharedStorage(nil, cfg, func() (Storage, error) { return fresh, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Put(context.Background(), "new", nil); err != nil {
		t.Fatal(err)
	}
	close(gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	third, err := sharedStorage(nil, cfg, func() (Storage, error) { t.Fatal("old release evicted replacement"); return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	second.Close()
	third.Close()
	if old.closes.Load() != 1 || fresh.closes.Load() != 1 {
		t.Fatal("owner closes not exactly once")
	}
}

func TestStorageLeaseRegressionMaskedOwnersIsolated(t *testing.T) {
	cfg := &Config{Service: "lease-test", RemoteFolder: t.Name()}
	settings := &internet.MemoryStreamConfig{FinalMask: finalmask.NewFinalMask(nil, nil, nil, nil, nil, nil)}
	roots := []*leaseRegressionStorage{}
	build := func() (Storage, error) { s := &leaseRegressionStorage{}; roots = append(roots, s); return s, nil }
	first, _ := sharedStorage(settings, cfg, build)
	second, _ := sharedStorage(settings, cfg, build)
	child, err := acquireSessionStorage(first)
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	second.Close()
	if len(roots) != 2 || roots[0].closes.Load() != 0 || roots[1].closes.Load() != 1 {
		t.Fatal("masked ownership sharing or early closure")
	}
	child.Close()
	if roots[0].closes.Load() != 1 {
		t.Fatal("masked final owner leaked")
	}
}

type leaseRegressionSlots struct {
	*leaseRegressionStorage
	*memSlots
	announceStarted chan struct{}
	announceExit    <-chan struct{}
}

func (s *leaseRegressionSlots) Put(ctx context.Context, name string, data []byte) error {
	if strings.HasPrefix(name, sessionsDir+"/") {
		close(s.announceStarted)
		<-ctx.Done()
		<-s.announceExit
		return ctx.Err()
	}
	return s.leaseRegressionStorage.Put(ctx, name, data)
}

func TestStorageLeaseRegressionDialPublicationCanceledAndJoined(t *testing.T) {
	exit := make(chan struct{})
	root := &leaseRegressionSlots{leaseRegressionStorage: &leaseRegressionStorage{}, memSlots: newMemSlots(), announceStarted: make(chan struct{}), announceExit: exit}
	factory := func(*Config) (Storage, error) { return root, nil }
	old := testStorageFactory.Swap(&factory)
	defer testStorageFactory.Store(old)
	settings := &internet.MemoryStreamConfig{
		ProtocolSettings: &Config{Concurrency: 2},
		FinalMask:        finalmask.NewFinalMask(nil, nil, nil, nil, nil, nil),
	}
	connection, err := Dial(context.Background(), xnet.Destination{}, settings)
	if err != nil {
		t.Fatal(err)
	}
	<-root.announceStarted
	conn := connection.(*Conn)
	done := make(chan struct{})
	go func() { conn.Interrupt(); close(done) }()
	<-conn.ctx.Done()
	if root.closes.Load() != 0 {
		t.Fatal("storage released before canceled publication unwound")
	}
	close(exit)
	<-done
	if root.closes.Load() != 1 {
		t.Fatal("Dial root acquisition was not transferred/released")
	}
}

func TestListenerLeaseRegressionCollectBlocksClaimDuringDelete(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprint(shutdown), func(t *testing.T) {
			gate := make(chan struct{})
			root := &leaseRegressionStorage{entries: []Entry{{Name: "dead"}}, deleteStarted: make(chan string, 1), deleteGate: gate}
			l := leaseRegressionListener(ownedStorage(root))
			l.idleSince["dead"] = time.Now().Add(-2 * time.Second)
			done := make(chan error, 1)
			go func() { done <- l.collect() }()
			<-root.deleteStarted
			if l.claim("dead") {
				t.Fatal("claimed session during Delete")
			}
			if shutdown {
				l.cancel()
			} else {
				close(gate)
			}
			<-done
			l.mu.Lock()
			reserved := l.collecting["dead"]
			l.mu.Unlock()
			if reserved {
				t.Fatal("completed collection left reservation")
			}
			if !shutdown && !l.claim("dead") {
				t.Fatal("resolved deletion kept claim blocked")
			}
			l.Close()
		})
	}
}

func TestListenerLeaseRegressionDropDedupBoundAndShutdownJoin(t *testing.T) {
	root := &leaseRegressionStorage{deleteStarted: make(chan string, 256), deleteGate: make(chan struct{})}
	l := leaseRegressionListener(ownedStorage(root))
	for range 20 {
		l.queueDrop("sessions/repeated")
	}
	<-root.deleteStarted
	if root.deletes.Load() != 1 {
		t.Fatalf("duplicate deletes %d", root.deletes.Load())
	}
	for i := range 256 {
		l.queueDrop(fmt.Sprintf("sessions/job-%d", i))
	}
	<-root.deleteStarted
	l.mu.Lock()
	pending, queued := len(l.cleanupPending), len(l.cleanupQueue)
	l.mu.Unlock()
	if pending > listenerCleanupQueue+listenerCleanupWorkers || queued > listenerCleanupQueue {
		t.Fatalf("unbounded admission: pending=%d queued=%d", pending, queued)
	}
	if root.peak.Load() > listenerCleanupWorkers {
		t.Fatalf("unbounded workers %d", root.peak.Load())
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if root.inflight.Load() != 0 || root.closes.Load() != 1 {
		t.Fatal("Close released root before cleanup joined")
	}
	l.queueDrop("sessions/late")
	if len(l.cleanupPending) != 0 {
		t.Fatal("shutdown admitted cleanup")
	}
}

func TestListenerLeaseRegressionStaleDropReservesSession(t *testing.T) {
	gate := make(chan struct{})
	root := &leaseRegressionStorage{deleteStarted: make(chan string, 1), deleteGate: gate}
	l := leaseRegressionListener(ownedStorage(root))
	l.queueSessionDrop("stale")
	<-root.deleteStarted
	if l.claim("stale") {
		t.Fatal("claim overlapped pending stale cleanup")
	}
	close(gate)
	l.cleanupWG.Wait()
	if !l.claim("stale") {
		t.Fatal("stale cleanup left reservation")
	}
	l.Close()
}

func TestSpareLeaseRegressionExpiresWithoutDialAndDrainsStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := &Config{}
		box := spareBoxFor(cfg)
		box.ttl.Store(int64(3 * time.Second))
		root := &leaseRegressionStorage{}
		box.mu.Lock()
		box.ch <- spareRing{storage: ownedStorage(root), at: time.Now()}
		box.mu.Unlock()
		box.signal()
		synctest.Wait()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if root.closes.Load() != 1 {
			t.Fatal("expired idle spare retained its lease")
		}
		second := &leaseRegressionStorage{}
		box.mu.Lock()
		box.ch <- spareRing{storage: ownedStorage(second), at: time.Now()}
		box.mu.Unlock()
		stopSpares(cfg)
		<-box.done
		if second.closes.Load() != 1 {
			t.Fatal("stopped spare not drained")
		}
		if _, ok := spareBoxes.Load(cfg); ok {
			t.Fatal("stopped config retained")
		}
		replacement := spareBoxFor(cfg)
		box.close()
		if got, ok := spareBoxes.Load(cfg); !ok || got != replacement {
			t.Fatal("old shutdown removed replacement box")
		}
		stopSpares(cfg)
		<-replacement.done
	})
}

func TestListenerLeaseRegressionAnnounceOmissionDoesNotReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entry := strings.TrimPrefix(announceName("handled", time.Now()), sessionsDir+"/")
		root := &leaseRegressionStorage{entries: []Entry{{Name: entry}}}
		l := leaseRegressionListener(ownedStorage(root))
		l.handled["handled"] = time.Now()
		l.firstSeen = map[string]time.Time{entry: time.Now()}
		l.lastSeen = map[string]time.Time{entry: time.Now()}
		root.entries = nil
		time.Sleep(l.sessionTTL)
		if accepted, err := l.acceptPending(context.Background()); accepted || err != nil {
			t.Fatalf("omission accepted=%v err=%v", accepted, err)
		}
		if err := l.collect(); err != nil {
			t.Fatal(err)
		}
		if l.handled["handled"].IsZero() {
			t.Fatal("brief omission lost replay protection")
		}
		root.entries = []Entry{{Name: entry}}
		if accepted, err := l.acceptPending(context.Background()); accepted || err != nil {
			t.Fatalf("reappearance accepted=%v err=%v", accepted, err)
		}
		l.cleanupWG.Wait()
		root.entries = nil
		time.Sleep(2 * l.sessionTTL)
		if _, err := l.acceptPending(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := l.collect(); err != nil {
			t.Fatal(err)
		}
		if len(l.handled) != 0 || len(l.firstSeen) != 0 || len(l.lastSeen) != 0 {
			t.Fatal("absent tombstones retained forever")
		}
		l.Close()
	})
}

func TestListenerLeaseRegressionAcceptShutdownJoinsBeforeRelease(t *testing.T) {
	exit := make(chan struct{})
	root := &leaseRegressionStorage{listStarted: make(chan struct{}), listExit: exit}
	l := leaseRegressionListener(ownedStorage(root))
	l.loopWG.Add(1)
	go func() { defer l.loopWG.Done(); l.acceptLoop(context.Background()) }()
	<-root.listStarted
	done := make(chan error, 1)
	go func() { done <- l.Close() }()
	<-l.ctx.Done()
	if root.closes.Load() != 0 {
		t.Fatal("root released before accept List unwound")
	}
	close(exit)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if root.closes.Load() != 1 {
		t.Fatal("listener root not released")
	}
}

func TestStorageLeaseRegressionConcreteCapabilitiesAndTerminalPools(t *testing.T) {
	newPool := func() *connPool {
		return newConnPool(nil, func(context.Context) *http.Client { return &http.Client{} })
	}
	template := &templateStorage{templateShared: &templateShared{conns: newPool()}, inflight: make(chan struct{}, 2)}
	s3 := &s3Storage{s3Shared: &s3Shared{conns: newPool()}, inflight: make(chan struct{}, 2)}
	for name, root := range map[string]Storage{"template": template, "s3": s3} {
		t.Run(name, func(t *testing.T) {
			parent := ownedStorage(root)
			child, err := acquireSessionStorage(parent)
			if err != nil {
				t.Fatal(err)
			}
			if name == "template" {
				for _, storage := range []Storage{parent, child} {
					commit, ok := storage.(interface{ requiresWALCommit() bool })
					if !ok || !commit.requiresWALCommit() {
						t.Fatal("template parent/child lease hid WAL completion requirement")
					}
				}
			}
			parent.Close()
			var pool *connPool
			switch s := root.(type) {
			case *templateStorage:
				pool = s.conns
			case *s3Storage:
				pool = s.conns
			}
			if pool.ctx.Err() != nil {
				t.Fatal("parent release closed live session pool")
			}
			child.Close()
			if pool.ctx.Err() == nil {
				t.Fatal("final release did not cancel pool")
			}
		})
	}
	drive := ownedStorage(&driveStorage{})
	if _, ok := drive.(slotStore); !ok {
		t.Fatal("Drive slot capability lost")
	}
	if _, ok := drive.(throttleStore); !ok {
		t.Fatal("Drive throttle capability lost")
	}
	if _, ok := drive.(interface{ waitCool(context.Context) error }); !ok {
		t.Fatal("Drive cooldown capability lost")
	}
	if _, ok := drive.(interface {
		takeIDs(context.Context, int, int) ([]string, error)
	}); !ok {
		t.Fatal("Drive ID capability lost")
	}
	if got := paramsForStorage(&Config{}, drive); got.concurrency != 64 {
		t.Fatalf("slot defaults lost: concurrency=%d", got.concurrency)
	}
}

func TestSpareLeaseRegressionIdleConfigRetired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := &Config{}
		box := spareBoxFor(cfg)
		root := &leaseRegressionStorage{}
		box.ttl.Store(int64(time.Hour))
		box.mu.Lock()
		box.ch <- spareRing{storage: ownedStorage(root), at: time.Now()}
		box.mu.Unlock()
		box.signal()
		synctest.Wait()
		time.Sleep(spareIdleStop)
		<-box.done
		if root.closes.Load() != 1 {
			t.Fatal("inactive box retained spare")
		}
		if _, ok := spareBoxes.Load(cfg); ok {
			t.Fatal("inactive config remained cached")
		}
	})
}
