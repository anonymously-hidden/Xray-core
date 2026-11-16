package xdrive

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type core35Slots struct {
	*memSlots
	put  func(context.Context, string, string, []byte) error
	read func(context.Context, string) (string, error)
}

func (s *core35Slots) setSlot(ctx context.Context, id, description string, body []byte) error {
	if s.put != nil {
		return s.put(ctx, id, description, body)
	}
	return s.memSlots.setSlot(ctx, id, description, body)
}

func (s *core35Slots) readDescription(ctx context.Context, id string) (string, error) {
	if s.read != nil {
		return s.read(ctx, id)
	}
	return s.memSlots.readDescription(ctx, id)
}

func core35Table(s *memSlots) slotTable {
	s.descs["s0"], s.descs["s1"], s.descs["wm"] = "", "", "-1"
	return slotTable{Slots: []string{"s0", "s1"}, Watermark: "wm"}
}

func core35Params() params {
	return paramsFromConfig(&Config{SegmentBytes: 1, Concurrency: 2, FlushIntervalMs: 1000, EagerWindowMs: 50, MaxPollIntervalMs: 20})
}

func TestCoreResume35CanceledSlotAdmission(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		s := newMemSlots()
		w := newSlotWriter(ctx, s, core35Table(s), core35Params())
		cancel()
		synctest.Wait()
		for _, payload := range [][]byte{nil, []byte("x")} {
			if n, err := w.Write(payload); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("canceled admission = %d, %v", n, err)
			}
		}
		if err := w.Close(); !errors.Is(err, context.Canceled) {
			t.Fatalf("Close = %v", err)
		}
		if w.seq != 0 || len(w.buf) != 0 || s.updates != 0 {
			t.Fatalf("canceled writer changed state: seq=%d buf=%d updates=%d", w.seq, len(w.buf), s.updates)
		}
	})
}

type core35AdmissionWriter struct {
	connWriter
	before func()
}

func (w core35AdmissionWriter) writeUntil(p []byte, deadline, closing <-chan struct{}) (int, error) {
	w.before()
	return w.connWriter.writeUntil(p, deadline, closing)
}

func TestCoreResume35DeadlineAdmissionGap(t *testing.T) {
	for _, stop := range []string{"deadline", "closing"} {
		t.Run(stop, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				s := newMemSlots()
				p := core35Params()
				p.segmentBytes = 1024
				w := newSlotWriter(ctx, s, core35Table(s), p)
				c := &Conn{writer: w, reader: queuedReader{}, writeDeadline: newPipeDeadline(), closing: make(chan struct{})}
				c.writer = core35AdmissionWriter{connWriter: w, before: func() {
					w.mu.Lock()
					defer w.mu.Unlock()
					if stop == "deadline" {
						c.SetWriteDeadline(time.Now().Add(-time.Second))
					} else {
						close(c.closing)
					}
				}}
				n, err := c.Write([]byte("x"))
				want := os.ErrDeadlineExceeded
				if stop == "closing" {
					want = io.ErrClosedPipe
				}
				if n != 0 || !errors.Is(err, want) || len(w.buf) != 0 || w.seq != 0 {
					t.Fatalf("admission = %d, %v, buf=%d seq=%d", n, err, len(w.buf), w.seq)
				}
				cancel()
				w.Close()
			})
		})
	}
}

func TestCoreResume35AdmissionAfterPermitWait(t *testing.T) {
	for _, stop := range []string{"deadline", "closing", "cancel"} {
		t.Run(stop, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				s := &core35Slots{memSlots: newMemSlots()}
				table := core35Table(s.memSlots)
				release := make(chan struct{})
				s.put = func(ctx context.Context, id, description string, body []byte) error {
					<-release
					return s.memSlots.setSlot(ctx, id, description, body)
				}
				w := newSlotWriter(ctx, s, table, core35Params())
				if n, err := w.Write([]byte("ab")); n != 2 || err != nil {
					t.Fatalf("first Write = %d, %v", n, err)
				}
				deadline, closing := make(chan struct{}), make(chan struct{})
				type result struct {
					n   int
					err error
				}
				done := make(chan result, 1)
				go func() {
					n, err := w.writeUntil([]byte("c"), deadline, closing)
					done <- result{n, err}
				}()
				synctest.Wait()
				switch stop {
				case "deadline":
					close(deadline)
				case "closing":
					close(closing)
				case "cancel":
					cancel()
				}
				close(release)
				got := <-done
				if got.n != 1 || got.err == nil {
					t.Fatalf("blocked Write = %+v", got)
				}
				synctest.Wait()
				w.mu.Lock()
				seq, buffered := w.seq, string(w.buf)
				w.mu.Unlock()
				if seq != 2 || buffered != "c" {
					t.Fatalf("expired admission assigned an upload: seq=%d buf=%q", seq, buffered)
				}
				cancel()
				w.Close()
			})
		})
	}
}

func TestCoreResume35SlotReuseCompletionAndACK(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := &core35Slots{memSlots: newMemSlots()}
		table := core35Table(s.memSlots)
		entered, release := make(chan struct{}), make(chan struct{})
		s.put = func(ctx context.Context, id, description string, body []byte) error {
			frame, _ := decodeSlot(description)
			if frame.seq == 0 && frame.mode == slotInline {
				close(entered)
				<-release
			}
			return s.memSlots.setSlot(ctx, id, description, body)
		}
		w := newSlotWriter(ctx, s, table, core35Params())
		w.first = make(chan []byte, 1)
		if n, err := w.Write([]byte("ab")); n != 2 || err != nil {
			t.Fatalf("Write = %d, %v", n, err)
		}
		<-entered
		if string(<-w.first) != "a" {
			t.Fatal("first announcement seed is wrong")
		}
		s.memSlots.setSlot(ctx, "wm", "0", nil)
		if n, err := w.Write([]byte("c")); n != 1 || err != nil {
			t.Fatalf("next lap Write = %d, %v", n, err)
		}
		synctest.Wait()
		time.Sleep(2 * slotStallTimeout)
		description, _ := s.memSlots.readDescription(ctx, "s0")
		if description != "" || w.Err() != nil {
			t.Fatalf("ACK bypassed upload completion: description=%q err=%v", description, w.Err())
		}
		close(release)
		synctest.Wait()
		description, _ = s.memSlots.readDescription(ctx, "s0")
		frame, ok := decodeSlot(description)
		if !ok || frame.seq != 2 || string(frame.data) != "c" {
			t.Fatalf("late predecessor overwrote next lap: %+v", frame)
		}
		s.memSlots.setSlot(ctx, "wm", "1", nil)
		if err := w.Close(); err != nil {
			t.Fatalf("graceful Close after ring reuse = %v", err)
		}
		description, _ = s.memSlots.readDescription(ctx, "s1")
		frame, ok = decodeSlot(description)
		if !ok || frame.seq != 3 || frame.mode != slotEnd {
			t.Fatalf("graceful EOF = %+v", frame)
		}
	})
}

func TestCoreResume35SlotReuseWaitsForACK(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		s := newMemSlots()
		w := newSlotWriter(ctx, s, core35Table(s), core35Params())
		w.Write([]byte("ab"))
		synctest.Wait()
		w.Write([]byte("c"))
		synctest.Wait()
		description, _ := s.readDescription(ctx, "s0")
		frame, _ := decodeSlot(description)
		if frame.seq != 0 {
			t.Fatalf("completion alone reused an unacknowledged slot: seq=%d", frame.seq)
		}
		s.setSlot(ctx, "wm", "0", nil)
		time.Sleep(w.watermarkTTL + w.ackRetry)
		synctest.Wait()
		description, _ = s.readDescription(ctx, "s0")
		frame, _ = decodeSlot(description)
		if frame.seq != 2 {
			t.Fatalf("acknowledged completed slot was not reused: seq=%d", frame.seq)
		}
		cancel()
		w.Close()
	})
}

func TestCoreResume35FailedPredecessorPreservesErrorAttempt(t *testing.T) {
	for _, markerFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "marker-stored", true: "marker-failed"}[markerFails], func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				s := &core35Slots{memSlots: newMemSlots()}
				table := core35Table(s.memSlots)
				dataEntered, dataRelease := make(chan struct{}), make(chan struct{})
				markerEntered, markerRelease := make(chan struct{}), make(chan struct{})
				failure := errors.New("predecessor upload failed")
				var overwrites atomic.Int32
				s.put = func(ctx context.Context, id, description string, body []byte) error {
					frame, _ := decodeSlot(description)
					if frame.seq == 0 {
						if frame.mode == slotErr {
							close(markerEntered)
							<-markerRelease
							if markerFails {
								return errors.New("marker upload failed")
							}
						} else {
							close(dataEntered)
							<-dataRelease
							return failure
						}
					} else if frame.seq == 2 {
						overwrites.Add(1)
					}
					return s.memSlots.setSlot(ctx, id, description, body)
				}
				w := newSlotWriter(ctx, s, table, core35Params())
				w.Write([]byte("ab"))
				<-dataEntered
				w.mu.Lock()
				previous := w.gates[0]
				w.mu.Unlock()
				s.memSlots.setSlot(ctx, "wm", "0", nil)
				w.Write([]byte("c"))
				synctest.Wait()
				w.mu.Lock()
				next := w.gates[0]
				w.mu.Unlock()
				close(dataRelease)
				<-markerEntered
				synctest.Wait()
				select {
				case <-next.done:
					t.Fatal("completion preceded the error-marker attempt")
				default:
				}
				close(markerRelease)
				synctest.Wait()
				<-previous.done
				<-next.done
				if !errors.Is(previous.err, failure) || !errors.Is(next.err, failure) || overwrites.Load() != 0 {
					t.Fatalf("failed predecessor did not propagate: next=%v overwrites=%d", next.err, overwrites.Load())
				}
				if !markerFails {
					description, _ := s.memSlots.readDescription(ctx, "s0")
					frame, ok := decodeSlot(description)
					if !ok || frame.seq != 0 || frame.mode != slotErr {
						t.Fatalf("error frame overwritten: %+v", frame)
					}
				}
				if err := w.Close(); !errors.Is(err, failure) {
					t.Fatalf("Close hid predecessor failure: %v", err)
				}
			})
		})
	}
}

func TestCoreResume35CanceledCloseDrainsAdmittedUploads(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		s := &core35Slots{memSlots: newMemSlots()}
		table := core35Table(s.memSlots)
		entered := make(chan struct{}, 2)
		unwind := make(chan struct{})
		var eof atomic.Int32
		s.put = func(ctx context.Context, id, description string, body []byte) error {
			frame, _ := decodeSlot(description)
			if frame.mode == slotEnd {
				eof.Add(1)
				return nil
			}
			entered <- struct{}{}
			<-ctx.Done()
			<-unwind
			return ctx.Err()
		}
		w := newSlotWriter(ctx, s, table, core35Params())
		w.Write([]byte("ab"))
		<-entered
		<-entered
		writes := make(chan error, 1)
		go func() { _, err := w.Write([]byte("c")); writes <- err }()
		synctest.Wait()
		closes := make(chan error, 1)
		go func() { closes <- w.Close() }()
		cancel()
		synctest.Wait()
		select {
		case err := <-closes:
			t.Fatalf("Close released storage before upload unwind: %v", err)
		default:
		}
		if err := <-writes; err == nil {
			t.Fatal("pending Write succeeded after shutdown")
		}
		close(unwind)
		if err := <-closes; !errors.Is(err, context.Canceled) {
			t.Fatalf("Close = %v", err)
		}
		if err := w.Close(); !errors.Is(err, context.Canceled) {
			t.Fatalf("repeated Close = %v", err)
		}
		if w.seq != 2 || eof.Load() != 0 {
			t.Fatalf("shutdown admitted more uploads/EOF: seq=%d eof=%d", w.seq, eof.Load())
		}
	})
}

func core35Reader(ctx context.Context, s slotStore, table slotTable) *slotReader {
	ctx, cancel := context.WithCancel(ctx)
	r := &slotReader{
		ctx: ctx, stop: cancel, store: s, table: table, params: core35Params(),
		ch: make(chan []byte, 4), wake: make(chan struct{}, 1), acks: make(chan struct{}, 1), grow: make(chan struct{}, 1),
		frontier: -1, hitAt: time.Now(), activeAt: time.Now(), active: make(chan struct{}),
		tailMin: time.Millisecond, tailMax: 10 * time.Millisecond, huntFloor: time.Millisecond, huntCeiling: 10 * time.Millisecond,
		ackRetry: time.Millisecond, ackDelay: 10 * time.Millisecond, stallTimeout: 100 * time.Millisecond,
	}
	return r
}

func core35RunReader(r *slotReader) {
	r.wg.Add(2)
	go func() { defer r.wg.Done(); r.run() }()
	go func() { defer r.wg.Done(); r.ackLoop() }()
}

func TestCoreResume35MissingHeadWithLaterProgress(t *testing.T) {
	for _, missing := range []string{"empty", "absent", "previous-lap", "invalid-empty", "transient-empty"} {
		t.Run(missing, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				s := newMemSlots()
				s.descs["s0"] = newSlotFrame(0, slotInline, []byte("a")).encode()
				s.descs["s1"] = ""
				s.descs["s2"] = newSlotFrame(2, slotEnd, nil).encode()
				s.descs["wm"] = "-1"
				if missing == "absent" {
					delete(s.descs, "s1")
				}
				var store slotStore = s
				if missing == "invalid-empty" || missing == "transient-empty" {
					var calls atomic.Int32
					store = &core35Slots{memSlots: s, read: func(ctx context.Context, id string) (string, error) {
						if id == "s1" && calls.Add(1)%2 == 0 {
							if missing == "invalid-empty" {
								return "invalid-but-present", nil
							}
							return "", io.ErrUnexpectedEOF
						}
						return s.readDescription(ctx, id)
					}}
				}
				r := core35Reader(ctx, store, slotTable{Slots: []string{"s0", "s1", "s2"}, Watermark: "wm"})
				if missing == "previous-lap" {
					r.head, r.started = 3, true
					s.descs["s0"] = newSlotFrame(0, slotInline, []byte("old")).encode()
					s.descs["s1"] = newSlotFrame(4, slotEnd, nil).encode()
				}
				core35RunReader(r)
				if missing != "previous-lap" {
					if first := <-r.chunks(); string(first) != "a" {
						t.Fatalf("first frame = %q", first)
					}
				}
				select {
				case _, ok := <-r.chunks():
					if ok || !strings.Contains(r.Err().Error(), "missing despite later") {
						t.Fatalf("missing head result: open=%v err=%v", ok, r.Err())
					}
				case <-time.After(10 * r.stallTimeout):
					t.Fatal("later progress failed to bound the missing head")
				}
				r.wait()
			})
		})
	}
}

func TestCoreResume35IdleEmptyHeadDoesNotTimeout(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		s := newMemSlots()
		r := core35Reader(ctx, s, core35Table(s))
		core35RunReader(r)
		time.Sleep(10 * r.stallTimeout)
		select {
		case <-r.chunks():
			t.Fatal("genuinely idle stream failed without later progress")
		default:
		}
		cancel()
		r.wait()
	})
}

func TestCoreResume35ActiveInvalidDescriptionBound(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := newMemSlots()
		table := core35Table(s)
		s.descs["s0"] = "invalid-but-present"
		r := core35Reader(ctx, s, table)
		core35RunReader(r)
		go func() {
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
		select {
		case _, ok := <-r.chunks():
			if ok || !strings.Contains(r.Err().Error(), "invalid frame") {
				t.Fatalf("invalid frame result: open=%v err=%v", ok, r.Err())
			}
		case <-time.After(5 * r.stallTimeout):
			t.Fatal("active inner description hunt bypassed the invalid-frame timeout")
		}
		r.wait()
	})
}

type core35CloseWriter struct {
	closed chan struct{}
	err    error
}

func (w *core35CloseWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *core35CloseWriter) writeUntil(p []byte, _, _ <-chan struct{}) (int, error) {
	return w.Write(p)
}
func (w *core35CloseWriter) Close() error { close(w.closed); return nil }
func (w *core35CloseWriter) Err() error   { return w.err }

type core35JoiningReader struct {
	queuedReader
	done <-chan struct{}
}

func (r core35JoiningReader) wait() { <-r.done }

func TestCoreResume35ConcurrentWriteCloseInterrupt(t *testing.T) {
	for range 20 {
		s := &core35Slots{memSlots: newMemSlots()}
		table := core35Table(s.memSlots)
		var active, released, afterRelease atomic.Int32
		s.put = func(ctx context.Context, id, description string, body []byte) error {
			active.Add(1)
			defer active.Add(-1)
			if released.Load() != 0 {
				afterRelease.Add(1)
			}
			frame, _ := decodeSlot(description)
			if frame.mode == slotEnd {
				return s.memSlots.setSlot(ctx, id, description, body)
			}
			<-ctx.Done()
			return ctx.Err()
		}
		c := newSlotConn(context.Background(), s, table, table, core35Params(), nil, func() {
			if active.Load() != 0 {
				afterRelease.Add(1)
			}
			released.Add(1)
		})
		start := make(chan struct{})
		var workers sync.WaitGroup
		for range 6 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				for range 4 {
					c.Write([]byte("abc"))
				}
			}()
		}
		workers.Add(2)
		go func() { defer workers.Done(); <-start; c.Close() }()
		go func() { defer workers.Done(); <-start; c.Interrupt() }()
		close(start)
		done := make(chan struct{})
		go func() { workers.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Write/Close/Interrupt did not drain")
		}
		if released.Load() != 1 || afterRelease.Load() != 0 {
			t.Fatalf("release=%d work-after-release=%d", released.Load(), afterRelease.Load())
		}
	}
}

func TestCoreResume35SlotWriterJoinsWatermarkPrefetch(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		s := &core35Slots{memSlots: newMemSlots()}
		table := core35Table(s.memSlots)
		entered, unwind := make(chan struct{}), make(chan struct{})
		s.read = func(ctx context.Context, id string) (string, error) {
			if id == "wm" {
				close(entered)
				<-ctx.Done()
				<-unwind
				return "", ctx.Err()
			}
			return s.memSlots.readDescription(ctx, id)
		}
		w := newSlotWriter(ctx, s, table, core35Params())
		w.Write([]byte("a"))
		<-entered
		cancel()
		done := make(chan error, 1)
		go func() { done <- w.Close() }()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("writer close omitted canceled watermark prefetch: %v", err)
		default:
		}
		close(unwind)
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Close = %v", err)
		}
	})
}

func TestCoreResume35SlotReaderJoinsDescriptionWork(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		s := &core35Slots{memSlots: newMemSlots()}
		entered, unwind := make(chan struct{}, 3), make(chan struct{})
		s.read = func(ctx context.Context, id string) (string, error) {
			entered <- struct{}{}
			<-ctx.Done()
			<-unwind
			return "", ctx.Err()
		}
		r := core35Reader(ctx, s, core35Table(s.memSlots))
		core35RunReader(r)
		<-entered
		cancel()
		done := make(chan struct{})
		go func() { r.wait(); close(done) }()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("reader wait omitted canceled description work")
		default:
		}
		close(unwind)
		<-done
	})
}

func TestCoreResume35ConnPublicationAndReaderJoin(t *testing.T) {
	for _, interrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "graceful", true: "interrupt"}[interrupt], func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				writer := &core35CloseWriter{closed: make(chan struct{})}
				publicationRelease, readerRelease := make(chan struct{}), make(chan struct{})
				publicationDone, readerDone := make(chan struct{}), make(chan struct{})
				var releases atomic.Int32
				c := &Conn{
					ctx: ctx, cancel: cancel, writer: writer, reader: core35JoiningReader{done: readerDone},
					readDeadline: newPipeDeadline(), writeDeadline: newPipeDeadline(), closing: make(chan struct{}),
					onClose: func() { releases.Add(1) },
				}
				c.publishWG.Add(1)
				failure := errors.New("announcement failed")
				go func() {
					defer c.publishWG.Done()
					defer close(publicationDone)
					if interrupt {
						<-c.ctx.Done()
					}
					<-publicationRelease
					if !interrupt {
						writer.err = failure
					}
				}()
				go func() {
					defer close(readerDone)
					<-c.ctx.Done()
					<-readerRelease
				}()
				closed := make(chan error, 1)
				go func() {
					if interrupt {
						c.Interrupt()
						closed <- c.Close()
					} else {
						closed <- c.Close()
					}
				}()
				<-writer.closed
				synctest.Wait()
				if !interrupt && c.ctx.Err() != nil {
					t.Fatal("graceful Close canceled unfinished publication")
				}
				if releases.Load() != 0 {
					t.Fatal("connection released ownership before publication completed")
				}
				close(publicationRelease)
				<-publicationDone
				synctest.Wait()
				if releases.Load() != 0 {
					t.Fatal("connection released ownership before reader work joined")
				}
				close(readerRelease)
				err := <-closed
				if !interrupt && !errors.Is(err, failure) {
					t.Fatalf("Close hid late announcement failure: %v", err)
				}
				if releases.Load() != 1 {
					t.Fatalf("ownership releases = %d", releases.Load())
				}
			})
		})
	}
}
