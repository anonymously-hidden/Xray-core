package xdrive

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xtls/xray-core/transport/internet"
)

type lifecycleSpareStorage struct {
	*memSlots
	entered chan struct{}
	once    sync.Once
	closed  atomic.Int32
}

func (s *lifecycleSpareStorage) makeSlot(ctx context.Context, name string) (string, error) {
	if s.entered != nil {
		s.once.Do(func() { close(s.entered) })
		<-ctx.Done()
		return "", ctx.Err()
	}
	return s.memSlots.makeSlot(ctx, name)
}
func (*lifecycleSpareStorage) Put(context.Context, string, []byte) error     { return nil }
func (*lifecycleSpareStorage) Get(context.Context, string) ([]byte, error)   { return nil, errNotFound }
func (*lifecycleSpareStorage) List(context.Context, string) ([]Entry, error) { return nil, nil }
func (*lifecycleSpareStorage) Delete(ctx context.Context, _ string) error    { return ctx.Err() }
func (s *lifecycleSpareStorage) Close() error                                { s.closed.Add(1); return nil }

func TestSpareShutdownJoinsWorkersAndTimers(t *testing.T) {
	for _, mode := range []string{"initial-delay", "active-create", "replaced-timer"} {
		t.Run(mode, func(t *testing.T) {
			regressionBubble(t, func(t *testing.T) {
				cfg := &Config{}
				settings := &internet.MemoryStreamConfig{ProtocolSettings: cfg}
				p := regressionParams()
				p.concurrency, p.sessionTTL = 2, 20*time.Second
				s := &lifecycleSpareStorage{memSlots: newMemSlots()}
				if mode == "active-create" {
					s.entered = make(chan struct{})
				}
				var factories atomic.Int32
				factory := func(*Config) (Storage, error) { factories.Add(1); return s, nil }
				old := testStorageFactory.Swap(&factory)
				defer testStorageFactory.Store(old)
				defer stopSpares(cfg)
				warmSpares(settings, cfg, p)
				switch mode {
				case "active-create":
					<-s.entered
				case "replaced-timer":
					time.Sleep(4 * time.Second)
					synctest.Wait()
					fillSpares(settings, cfg, p)
					synctest.Wait()
				}
				start := time.Now()
				stopSpares(cfg)
				if elapsed := time.Since(start); elapsed != 0 {
					t.Errorf("shutdown waited %v for an uncancelled timer", elapsed)
				}
				before := factories.Load()
				warmSpares(settings, cfg, p)
				time.Sleep(time.Hour)
				synctest.Wait()
				if factories.Load() != before || spareBoxFor(cfg).busy.Load() {
					t.Error("shutdown timer restarted a spare worker")
				}
				if mode == "initial-delay" && before != 0 {
					t.Error("cancelled delay created a storage")
				}
				if mode == "active-create" && s.closed.Load() != 1 {
					t.Error("cancelled worker did not close its storage")
				}
			})
		})
	}
}
