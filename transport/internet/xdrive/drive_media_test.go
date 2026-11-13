package xdrive

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func mediaResponse(t *testing.T, req *http.Request, payload []byte) *http.Response {
	t.Helper()
	var start, end int
	if _, err := fmt.Sscanf(req.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end < start || end >= len(payload) {
		t.Error("invalid media range request")
		return memoryResponse(http.StatusBadRequest, "")
	}
	resp := memoryResponse(http.StatusPartialContent, string(payload[start:end+1]))
	resp.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
	return resp
}

// Model the observed single-connection stall with the real session pool wiring.
// Later inline bytes must be delivered immediately after the recovered body,
// without waiting for the original connection's 60-second stall timeout.
func TestDriveMediaHeadRecovery(t *testing.T) {
	for _, stall := range []string{"headers", "body"} {
		t.Run(stall, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				payload := randomPayload(defaultSegmentBytes)
				frame := newSlotFrame(0, slotMedia, payload)
				var primary, rescues, cancelled atomic.Int32
				root := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
					t.Error("unexpected root request")
					return memoryResponse(http.StatusBadRequest, ""), nil
				}))
				root.conns = newConnPool(nil, func(context.Context) *http.Client {
					return &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
						if req.Method == http.MethodPatch {
							return memoryResponse(http.StatusOK, "{}"), nil
						}
						description := frame.encode()
						if strings.HasSuffix(req.URL.Path, "/tail") {
							description = newSlotFrame(1, slotInline, []byte("tail")).encode()
						}
						return memoryResponse(http.StatusOK, fmt.Sprintf(`{"description":%q}`, description)), nil
					})}
				})
				root.bulkConns = newConnPool(nil, func(context.Context) *http.Client {
					return &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
						rescues.Add(1)
						return mediaResponse(t, req, payload), nil
					})}
				})
				for i := range root.mediaConns {
					root.mediaConns[i] = newConnPool(nil, func(context.Context) *http.Client {
						return &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
							primary.Add(1)
							if i == 0 {
								if stall == "headers" {
									<-req.Context().Done()
									cancelled.Add(1)
									return nil, req.Context().Err()
								}
								resp := mediaResponse(t, req, payload)
								resp.Body = &doneBody{ReadCloser: driveTimeoutBody{req.Context()}, done: func() { cancelled.Add(1) }}
								return resp, nil
							}
							return mediaResponse(t, req, payload), nil
						})}
					})
				}
				s := root.session().(*driveStorage)
				defer s.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				p := paramsFromConfig(&Config{Concurrency: 2})
				r := newSlotReader(ctx, s, slotTable{Slots: []string{"head", "tail"}, Watermark: "ack"}, p, nil)
				start := time.Now()
				if body := <-r.ch; !bytes.Equal(body, payload) {
					t.Fatal("recovered body differs")
				}
				if tail := <-r.ch; string(tail) != "tail" {
					t.Fatal("later inline segment lost or reordered")
				}
				cancel()
				synctest.Wait()
				if elapsed := time.Since(start); elapsed != slotBodyHedge || primary.Load() != 4 || rescues.Load() != 1 || cancelled.Load() != 1 {
					t.Fatalf("elapsed=%v primary=%d rescues=%d cancelled=%d", elapsed, primary.Load(), rescues.Load(), cancelled.Load())
				}
			})
		})
	}
}

func TestDriveMediaRangeIntegrity(t *testing.T) {
	for _, mode := range []string{"range", "ignored", "offset", "truncated", "oversized", "checksum", "missing", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				payload := randomPayload(defaultSegmentBytes + 7)
				frame := newSlotFrame(0, slotMedia, payload)
				s := memoryDrive(roundTrip(func(req *http.Request) (*http.Response, error) {
					resp := mediaResponse(t, req, payload)
					switch mode {
					case "ignored":
						resp = memoryResponse(http.StatusOK, string(payload))
					case "offset":
						resp.Header.Set("Content-Range", "bytes 1-2/3")
					case "truncated":
						resp.Body = io.NopCloser(strings.NewReader("short"))
					case "oversized":
						resp = memoryResponse(http.StatusOK, string(payload)+"x")
					case "checksum":
						bad := bytes.Repeat([]byte{'x'}, len(payload))
						resp = mediaResponse(t, req, bad)
					case "missing":
						resp = memoryResponse(http.StatusNotFound, "")
					case "cancelled":
						<-req.Context().Done()
						return nil, req.Context().Err()
					}
					return resp, nil
				}))
				for range driveMediaLanes {
					s.mediaClients = append(s.mediaClients, s.client)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if mode == "cancelled" {
					go func() { time.Sleep(time.Second); cancel() }()
				}
				body, err := s.readFrameBody(ctx, "body", frame)
				if mode == "range" || mode == "ignored" {
					if err != nil || !bytes.Equal(body, payload) {
						t.Fatalf("valid media rejected: %v", err)
					}
				} else if err == nil || len(body) != 0 {
					t.Fatal("invalid media was returned")
				}
				if mode == "cancelled" && err != context.Canceled {
					t.Fatalf("cancellation: %v", err)
				}
				synctest.Wait()
				if len(s.inflight) != 0 {
					t.Fatal("media permits leaked")
				}
			})
		})
	}
}

func TestDriveMediaMovingAndRateLimited(t *testing.T) {
	for _, mode := range []string{"moving", "cooling", "recent"} {
		t.Run(mode, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				payload := bytes.Repeat([]byte{'x'}, 6)
				var calls, hedges atomic.Int32
				s := memoryDrive(roundTrip(func(req *http.Request) (*http.Response, error) {
					calls.Add(1)
					reader, writer := io.Pipe()
					stop := context.AfterFunc(req.Context(), func() { reader.CloseWithError(req.Context().Err()) })
					go func() {
						defer stop()
						defer writer.Close()
						if mode == "moving" {
							for range 6 {
								time.Sleep(slotBodyHedge / 2)
								if _, err := writer.Write([]byte{'x'}); err != nil {
									return
								}
							}
						} else {
							time.Sleep(3 * slotBodyHedge)
							writer.Write(payload)
						}
					}()
					resp := mediaResponse(t, req, payload)
					resp.Body = reader
					return resp, nil
				}))
				s.mediaClients = []*http.Client{s.client}
				s.readHedgeTokens = 3
				s.bulkClient = &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
					hedges.Add(1)
					return mediaResponse(t, req, payload), nil
				})}
				if mode != "moving" {
					go func() {
						time.Sleep(slotBodyHedge / 2)
						s.cool()
						if mode == "recent" {
							s.coolMu.Lock()
							s.coolUntil = time.Now()
							s.coolMu.Unlock()
						}
					}()
				}
				start := time.Now()
				body, err := s.readFrameBody(context.Background(), "body", newSlotFrame(0, slotMedia, payload))
				if err != nil || !bytes.Equal(body, payload) || calls.Load() != 1 || hedges.Load() != 0 || time.Since(start) != 3*slotBodyHedge {
					t.Fatalf("err=%v requests=%d hedges=%d elapsed=%v", err, calls.Load(), hedges.Load(), time.Since(start))
				}
			})
		})
	}
}

func TestDrivePermitHeldThroughBody(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		var calls atomic.Int32
		s := memoryDrive(roundTrip(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			resp := memoryResponse(http.StatusOK, "")
			resp.Body = driveTimeoutBody{req.Context()}
			return resp, nil
		}))
		s.inflight = make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		for range 2 {
			go s.readBody(ctx, "body")
		}
		synctest.Wait()
		if calls.Load() != 1 || len(s.inflight) != 1 {
			t.Fatal("response body did not retain its in-flight permit")
		}
		cancel()
		synctest.Wait()
		if len(s.inflight) != 0 {
			t.Fatal("permit leaked on cancellation")
		}
	})
}

func TestDriveMediaParallelTransfer(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		payload := randomPayload(defaultSegmentBytes)
		frame := newSlotFrame(0, slotMedia, payload)
		// Each connection supplies 64 KiB/s, with regular progress. Splitting
		// the frame must reduce completion time without relying on hedges.
		s := memoryDrive(roundTrip(func(req *http.Request) (*http.Response, error) {
			resp := memoryResponse(http.StatusOK, string(payload))
			if req.Header.Get("Range") != "" {
				resp = mediaResponse(t, req, payload)
			}
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			reader, writer := io.Pipe()
			stop := context.AfterFunc(req.Context(), func() { reader.CloseWithError(req.Context().Err()) })
			go func() {
				defer stop()
				defer writer.Close()
				for len(data) > 0 {
					time.Sleep(time.Second / 2)
					n := min(len(data), 32<<10)
					if _, err := writer.Write(data[:n]); err != nil {
						return
					}
					data = data[n:]
				}
			}()
			resp.Body = reader
			return resp, nil
		}))
		for range driveMediaLanes {
			s.mediaClients = append(s.mediaClients, &http.Client{Transport: s.client.Transport})
		}
		start := time.Now()
		old, err := s.readBody(context.Background(), "body")
		if err != nil || !bytes.Equal(old, payload) {
			t.Fatal("baseline download failed")
		}
		baseline := time.Since(start)
		start = time.Now()
		body, err := s.readFrameBody(context.Background(), "body", frame)
		elapsed := time.Since(start)
		if err != nil || !bytes.Equal(body, payload) || baseline != 8*time.Second || elapsed != 2*time.Second {
			t.Fatalf("baseline=%v parallel=%v err=%v", baseline, elapsed, err)
		}
		t.Logf("simulated connection-limited transfer: %v -> %v", baseline, elapsed)
	})
}

func TestDriveRequestCountExceedsTimingReservoir(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
			return memoryResponse(http.StatusOK, "body"), nil
		}))
		s.timing = newTimingAggregator()
		defer s.timing.stop()
		for range timingRing + 10 {
			if _, err := s.readBody(context.Background(), "body"); err != nil {
				t.Fatal(err)
			}
		}
		s.timing.mu.Lock()
		requests, samples := s.timing.requests[kindBodyRead], s.timing.n[kindBodyRead]
		s.timing.mu.Unlock()
		if requests != timingRing+10 || samples != timingRing {
			t.Fatalf("requests=%d samples=%d", requests, samples)
		}
	})
}

func TestDriveMediaRecoveryBypassesBusyPool(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		payload := []byte("recovered")
		s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
			t.Error("primary escaped a saturated pool")
			return memoryResponse(http.StatusBadRequest, ""), nil
		}))
		s.mediaClients = []*http.Client{s.client}
		s.readHedgeTokens = 1
		s.inflight = make(chan struct{}, 1)
		s.inflight <- struct{}{}
		s.reserved <- struct{}{}
		s.mediaRescue = make(chan struct{}, 1)
		s.bulkClient = &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
			return mediaResponse(t, req, payload), nil
		})}
		start := time.Now()
		body, err := s.readFrameBody(context.Background(), "body", newSlotFrame(0, slotMedia, payload))
		synctest.Wait()
		if err != nil || !bytes.Equal(body, payload) || time.Since(start) != slotBodyHedge || len(s.mediaRescue) != 0 || len(s.inflight) != 1 || len(s.reserved) != 1 {
			t.Fatalf("recovery did not bypass bulk pool cleanly: %v", err)
		}
	})
}

// A media hedge skipped for cooling or an empty budget must be tried again at
// the next hedge delay, not abandoned for the whole stall.
func TestDriveMediaHedgeRearmsAfterSkip(t *testing.T) {
	for _, mode := range []string{"cooling", "budget"} {
		t.Run(mode, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				payload := []byte("hedged")
				var hedges atomic.Int32
				s := memoryDrive(roundTrip(func(req *http.Request) (*http.Response, error) {
					<-req.Context().Done()
					return nil, req.Context().Err()
				}))
				s.mediaClients = []*http.Client{s.client}
				s.bulkClient = &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
					hedges.Add(1)
					return mediaResponse(t, req, payload), nil
				})}
				s.readHedgeTokens = 1
				switch mode {
				case "cooling":
					s.coolMu.Lock()
					s.coolUntil = time.Now().Add(3 * time.Second)
					s.coolMu.Unlock()
				case "budget":
					s.readHedgeTokens = 0
					time.AfterFunc(3*time.Second, func() {
						s.readHedgeMu.Lock()
						s.readHedgeTokens = 1
						s.readHedgeMu.Unlock()
					})
				}
				start := time.Now()
				body, err := s.readFrameBody(context.Background(), "body", newSlotFrame(0, slotMedia, payload))
				if err != nil || !bytes.Equal(body, payload) || hedges.Load() != 1 || time.Since(start) != 2*slotBodyHedge || s.readHedgeTokens != 0 {
					t.Fatalf("err=%v hedges=%d elapsed=%v tokens=%d", err, hedges.Load(), time.Since(start), s.readHedgeTokens)
				}
			})
		})
	}
}

// A sustained download must leave inflight to slot writes and tail reads, and
// still finish the body-admission window in the same two waves as before.
func TestDriveMediaRangesLeaveInflightFree(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		payload := randomPayload(defaultSegmentBytes)
		frame := newSlotFrame(0, slotMedia, payload)
		var started atomic.Int32
		s := memoryDrive(roundTrip(func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodGet {
				return memoryResponse(http.StatusOK, "{}"), nil
			}
			started.Add(1)
			<-time.After(time.Second)
			return mediaResponse(t, req, payload), nil
		}))
		s.mediaRanges = make(chan struct{}, driveMediaInflight)
		for range driveMediaLanes {
			s.mediaClients = append(s.mediaClients, s.client)
		}
		start := time.Now()
		var wg sync.WaitGroup
		for range slotBodyAhead {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if body, err := s.readFrameBody(context.Background(), "body", frame); err != nil || !bytes.Equal(body, payload) {
					t.Errorf("range body: %v", err)
				}
			}()
		}
		synctest.Wait()
		if started.Load() != driveMediaInflight || len(s.mediaRanges) != driveMediaInflight || len(s.inflight) != 0 {
			t.Fatalf("started=%d ranges=%d inflight=%d", started.Load(), len(s.mediaRanges), len(s.inflight))
		}
		if err := s.setSlot(context.Background(), "slot", "description", []byte("upload")); err != nil || time.Since(start) != 0 {
			t.Fatalf("upload behind the ranges: err=%v after %v", err, time.Since(start))
		}
		wg.Wait()
		if elapsed := time.Since(start); elapsed != 2*time.Second || len(s.mediaRanges) != 0 {
			t.Fatalf("elapsed=%v ranges=%d, want 2s", elapsed, len(s.mediaRanges))
		}
	})
}
