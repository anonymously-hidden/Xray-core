package xdrive

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xtls/xray-core/transport/internet"
)

func TestSpareLeaseRegressionCrossBubbleStop(t *testing.T) {
	config := &Config{}
	request, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		<-request
		stopSpares(config)
		close(stopped)
	}()
	synctest.Test(t, func(t *testing.T) {
		box := spareBoxFor(config)
		root := &leaseRegressionStorage{}
		timerFired := false
		box.mu.Lock()
		box.ch <- spareRing{storage: ownedStorage(root), at: time.Now()}
		box.timer = time.AfterFunc(time.Hour, func() { timerFired = true })
		box.mu.Unlock()
		box.signal()
		synctest.Wait()
		close(request)
		<-stopped
		if !box.stop.Load() || !box.closed.Load() {
			t.Fatal("external stop did not exclude admission")
		}
		if _, ok := spareBoxes.Load(config); ok {
			t.Fatal("external stop retained the exact map entry")
		}
		at := time.Now()
		<-box.done
		if elapsed := time.Since(at); elapsed > spareStopPollInterval {
			t.Fatalf("stop dispatch took %v", elapsed)
		}
		if root.closes.Load() != 1 || len(box.ch) != 0 || box.timer != nil {
			t.Fatal("external stop did not drain the spare and retire its timer")
		}
		time.Sleep(time.Hour)
		synctest.Wait()
		if timerFired {
			t.Fatal("stopped box left its refill timer active")
		}
	})
}

type spareCrossBubbleRegressionSlots struct {
	*leaseRegressionStorage
	*memSlots
	first    sync.Once
	started  chan struct{}
	canceled chan time.Time
	exit     <-chan struct{}
}

func (s *spareCrossBubbleRegressionSlots) makeSlot(ctx context.Context, name string) (string, error) {
	paused := false
	s.first.Do(func() {
		paused = true
		close(s.started)
	})
	if paused {
		<-ctx.Done()
		s.canceled <- time.Now()
		<-s.exit
		return "", ctx.Err()
	}
	return s.memSlots.makeSlot(ctx, name)
}

func TestSpareLeaseRegressionCrossBubbleStopCancelsAndJoinsRefill(t *testing.T) {
	config := &Config{Service: "cross-bubble-spare", RemoteFolder: t.Name(), Concurrency: 2}
	request, stopped := make(chan struct{}), make(chan struct{})
	repeatRequest, repeatDone := make(chan struct{}), make(chan struct{})
	go func() {
		<-request
		stopAllSpares()
		close(stopped)
		<-repeatRequest
		stopSpares(config)
		close(repeatDone)
	}()
	synctest.Test(t, func(t *testing.T) {
		exit := make(chan struct{})
		root := &leaseRegressionStorage{}
		slots := &spareCrossBubbleRegressionSlots{
			leaseRegressionStorage: root, memSlots: newMemSlots(),
			started: make(chan struct{}), canceled: make(chan time.Time, 1), exit: exit,
		}
		factory := func(*Config) (Storage, error) { return slots, nil }
		old := testStorageFactory.Swap(&factory)
		defer testStorageFactory.Store(old)
		settings := &internet.MemoryStreamConfig{ProtocolSettings: config}
		box := spareBoxFor(config)
		queued := &leaseRegressionStorage{}
		box.mu.Lock()
		box.ch <- spareRing{storage: ownedStorage(queued), at: time.Now()}
		box.mu.Unlock()
		fillSparesBox(box, settings, config, paramsFromConfig(config))
		time.Sleep(2 * time.Second)
		<-slots.started
		synctest.Wait()
		close(request)
		<-stopped
		if _, ok := spareBoxes.Load(config); ok {
			t.Fatal("external stop retained the exact map entry")
		}
		at := time.Now()
		canceledAt := <-slots.canceled
		if elapsed := canceledAt.Sub(at); elapsed < 0 || elapsed > spareStopPollInterval {
			t.Fatalf("refill cancellation dispatch took %v", elapsed)
		}
		synctest.Wait()
		if !box.busy.Load() || root.closes.Load() != 0 || queued.closes.Load() != 0 {
			t.Fatal("reaper drained before admitted refill unwound")
		}
		select {
		case <-box.done:
			t.Fatal("reaper exited before admitted refill unwound")
		default:
		}
		replacement := spareBoxFor(config)
		box.close()
		if current, ok := spareBoxes.Load(config); !ok || current != replacement {
			t.Fatal("old shutdown removed the replacement box")
		}
		close(exit)
		<-box.done
		if root.closes.Load() != 1 || queued.closes.Load() != 1 || box.busy.Load() || len(box.ch) != 0 {
			t.Fatal("canceled refill or queued spare leaked")
		}
		close(repeatRequest)
		<-repeatDone
		<-replacement.done
	})
}
