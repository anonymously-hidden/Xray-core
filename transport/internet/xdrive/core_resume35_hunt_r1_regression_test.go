package xdrive

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func core35HuntR1Wake(r *slotReader) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.ctx.Done():
				return
			case <-ticker.C:
				r.Wake()
			}
		}
	}()
	return done
}

func TestCoreResume35HuntR1PendingHealthySibling(t *testing.T) {
	for _, observation := range []string{"invalid", "empty-with-later-progress"} {
		for _, stuckSibling := range []bool{false, true} {
			for _, active := range []bool{false, true} {
				name := observation + map[bool]string{false: "/no-stuck", true: "/stuck"}[stuckSibling] + map[bool]string{false: "/idle", true: "/wake"}[active]
				t.Run(name, func(t *testing.T) {
					driveClock(t, func(t *testing.T) {
						ctx, cancel := context.WithCancel(context.Background())
						s := &core35Slots{memSlots: newMemSlots()}
						table := core35Table(s.memSlots)
						if observation == "empty-with-later-progress" {
							s.descs["s0"] = newSlotFrame(2, slotEnd, nil).encode()
						}
						var reads, completed, canceled, outstanding, peak atomic.Int32
						var healthyBegan atomic.Int64
						s.read = func(ctx context.Context, id string) (string, error) {
							if id != "s1" {
								return s.memSlots.readDescription(ctx, id)
							}
							current := outstanding.Add(1)
							defer outstanding.Add(-1)
							for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
							}
							call := reads.Add(1)
							if stuckSibling && call == 2 {
								<-ctx.Done()
								return "", ctx.Err()
							}
							if stuckSibling && call == 3 {
								select {
								case <-time.After(30 * time.Millisecond):
								case <-ctx.Done():
									return "", ctx.Err()
								}
							}
							validCall := call
							if stuckSibling && call > 2 {
								validCall--
							}
							if validCall%2 == 1 {
								if observation == "invalid" {
									return "stale-invalid", nil
								}
								return "", nil
							}
							healthyBegan.CompareAndSwap(0, time.Now().UnixNano())
							select {
							case <-time.After(200 * time.Millisecond):
								completed.Add(1)
								return newSlotFrame(1, slotInline, []byte("valid")).encode(), nil
							case <-ctx.Done():
								canceled.Add(1)
								return "", ctx.Err()
							}
						}
						p := core35Params()
						r := newSlotReader(ctx, s, table, p, []byte("seed"))
						if string(<-r.chunks()) != "seed" {
							t.Fatal("announcement seed missing")
						}
						var wake <-chan struct{}
						if active {
							wake = core35HuntR1Wake(r)
						}
						defer func() {
							cancel()
							r.wait()
							if wake != nil {
								<-wake
							}
						}()
						select {
						case got, ok := <-r.chunks():
							if !ok || string(got) != "valid" {
								t.Fatalf("healthy pending sibling starved: open=%v got=%q err=%v completed=%d canceled=%d", ok, got, r.Err(), completed.Load(), canceled.Load())
							}
						case <-time.After(time.Second):
							t.Fatal("healthy 200ms request did not finish within one second")
						}
						age := time.Since(time.Unix(0, healthyBegan.Load()))
						if completed.Load() == 0 || age < 200*time.Millisecond || peak.Load() > 3 {
							t.Fatalf("pending read lifecycle: completed=%d age=%v peak=%d", completed.Load(), age, peak.Load())
						}
					})
				})
			}
		}
	}
}

func TestCoreResume35HuntR1IdleHeadArrivalWithStuckSibling(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		s := &core35Slots{memSlots: newMemSlots()}
		table := core35Table(s.memSlots)
		var calls, canceled atomic.Int32
		s.read = func(ctx context.Context, id string) (string, error) {
			if id == "s0" && calls.Add(1) == 1 {
				<-ctx.Done()
				canceled.Add(1)
				return "", ctx.Err()
			}
			return s.memSlots.readDescription(ctx, id)
		}
		r := core35Reader(ctx, s, table)
		core35RunReader(r)
		defer func() { cancel(); r.wait() }()
		time.Sleep(4 * r.eagerWindow)
		if canceled.Load() != 0 {
			t.Fatal("empty observations canceled the pending request")
		}
		if err := s.memSlots.setSlot(ctx, "s0", newSlotFrame(0, slotInline, []byte("arrived")).encode(), nil); err != nil {
			t.Fatal(err)
		}
		select {
		case got, ok := <-r.chunks():
			if !ok || string(got) != "arrived" {
				t.Fatalf("idle arrival result: open=%v got=%q err=%v", ok, got, r.Err())
			}
		case <-time.After(5 * r.stallTimeout):
			t.Fatal("a stuck sibling stopped the idle head from probing new data")
		}
		cancel()
		r.wait()
		if canceled.Load() != 1 {
			t.Fatalf("losing request not joined: canceled=%d", canceled.Load())
		}
	})
}

func TestCoreResume35HuntR1IdleEmptyWithStuckSibling(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		s := &core35Slots{memSlots: newMemSlots()}
		table := core35Table(s.memSlots)
		var calls, canceled atomic.Int32
		s.read = func(ctx context.Context, id string) (string, error) {
			if id != "s0" {
				return s.memSlots.readDescription(ctx, id)
			}
			if calls.Add(1) == 1 {
				return "", nil
			}
			<-ctx.Done()
			canceled.Add(1)
			return "", ctx.Err()
		}
		r := core35Reader(ctx, s, table)
		core35RunReader(r)
		defer func() { cancel(); r.wait() }()
		time.Sleep(10 * r.stallTimeout)
		select {
		case _, ok := <-r.chunks():
			t.Fatalf("idle stream timed out without peer progress: open=%v err=%v", ok, r.Err())
		default:
		}
		if calls.Load() < 2 || calls.Load() > 4 || canceled.Load() != 0 {
			t.Fatalf("observation expiry restarted idle requests: calls=%d canceled=%d", calls.Load(), canceled.Load())
		}
	})
}

func TestCoreResume35HuntR1StuckSiblingsKeepObservationBound(t *testing.T) {
	for _, observation := range []string{"invalid", "initially-idle-invalid", "empty-with-later-progress", "transient-with-later-progress", "no-observation-with-later-progress"} {
		for _, active := range []bool{false, true} {
			name := observation + map[bool]string{false: "/idle", true: "/wake"}[active]
			t.Run(name, func(t *testing.T) {
				driveClock(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					s := &core35Slots{memSlots: newMemSlots()}
					table := core35Table(s.memSlots)
					invalid := strings.Contains(observation, "invalid")
					if !invalid {
						s.descs["s0"] = newSlotFrame(2, slotEnd, nil).encode()
					}
					var calls, canceled atomic.Int32
					s.read = func(ctx context.Context, id string) (string, error) {
						if id != "s1" {
							return s.memSlots.readDescription(ctx, id)
						}
						if calls.Add(1) == 1 && observation != "no-observation-with-later-progress" {
							switch observation {
							case "invalid", "initially-idle-invalid":
								return "stale-invalid", nil
							case "transient-with-later-progress":
								return "", io.ErrUnexpectedEOF
							default:
								return "", nil
							}
						}
						<-ctx.Done()
						canceled.Add(1)
						return "", ctx.Err()
					}
					r := core35Reader(ctx, s, table)
					r.head, r.started = 1, true
					if observation == "initially-idle-invalid" {
						r.activeAt = time.Now().Add(-2 * r.eagerWindow)
					}
					core35RunReader(r)
					var wake <-chan struct{}
					if active {
						wake = core35HuntR1Wake(r)
					}
					defer func() {
						cancel()
						r.wait()
						if wake != nil {
							<-wake
						}
					}()
					select {
					case _, ok := <-r.chunks():
						want := "missing despite later"
						if invalid {
							want = "invalid frame"
						}
						if ok || !strings.Contains(r.Err().Error(), want) {
							t.Fatalf("bound result: open=%v err=%v", ok, r.Err())
						}
					case <-time.After(5 * r.stallTimeout):
						t.Fatalf("stuck siblings bypassed the head observation bound: calls=%d canceled=%d", calls.Load(), canceled.Load())
					}
					r.wait()
					wantCanceled := calls.Load() - 1
					if observation == "no-observation-with-later-progress" {
						wantCanceled++
					}
					if calls.Load() < 2 || calls.Load() > 4 || canceled.Load() != wantCanceled {
						t.Fatalf("requests restarted or not joined: calls=%d canceled=%d", calls.Load(), canceled.Load())
					}
				})
			})
		}
	}
}
