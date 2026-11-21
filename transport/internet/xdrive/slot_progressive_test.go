package xdrive

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// Model a provider that drips complete blocks over a single HTTP response.
type cadenceBody struct {
	ctx    context.Context
	reader *bytes.Reader
	delay  time.Duration
}

func (b cadenceBody) Read(p []byte) (int, error) {
	if b.reader.Len() > 0 {
		if err := batchTestWait(b.ctx, b.delay); err != nil {
			return 0, err
		}
	}
	return b.reader.Read(p)
}

func TestDriveProgressiveDeliveryCadence(t *testing.T) {
	const size = 5 * driveStreamBlockBytes
	const period = 450 * time.Millisecond
	for _, stalled := range []bool{false, true} {
		t.Run(fmt.Sprint("stalled=", stalled), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := time.Now()
				data := driveStreamTestData(size)
				frame := driveStreamTestFrame(data)
				stream := newSlotStreamBuffer(frame)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				ctx = context.WithValue(ctx, mediaHeadKey{}, true)
				ctx = context.WithValue(ctx, progressiveMediaKey{}, true)
				ctx = context.WithValue(ctx, mediaDeliveryKey{}, stream.deliveryState)
				var primary, rescue atomic.Int32
				rt := roundTrip(func(req *http.Request) (*http.Response, error) {
					m := req.Context().Value(driveMediaKey{}).(*driveMediaRequest)
					if m.rescue {
						rescue.Add(1)
						if m.start != driveStreamBlockBytes || m.end != size {
							t.Errorf("unexpected rescue range %q", req.Header.Get("Range"))
						}
						if err := batchTestWait(req.Context(), 100*time.Millisecond); err != nil {
							return nil, err
						}
					} else {
						primary.Add(1)
						if m.start != 0 || m.end != size {
							t.Errorf("progressive read split the primary request into %q", req.Header.Get("Range"))
						}
					}
					part := data[m.start:m.end]
					var body io.ReadCloser = io.NopCloser(bytes.NewReader(part))
					if !m.rescue && stalled {
						body = &driveStreamTestGatedBody{
							ctx: req.Context(), data: part, pauseAt: driveStreamBlockBytes,
							release: make(chan struct{}), closed: make(chan struct{}),
						}
					} else if !m.rescue {
						body = io.NopCloser(cadenceBody{req.Context(), bytes.NewReader(part), period})
					}
					return driveStreamTestResponse(req, http.StatusPartialContent,
						fmt.Sprintf("bytes %d-%d/%d", m.start, m.end-1, size), int64(len(part)), body), nil
				})
				s := memoryDrive(rt)
				s.mediaClients = []*http.Client{s.client, {Transport: rt}, {Transport: rt}, {Transport: rt}}
				s.bulkClient = &http.Client{Transport: rt}
				s.mediaHedgeTokens = 1
				for range rangeLatencyMin {
					s.mediaLatency[1].note(500 * time.Millisecond)
				}
				done := make(chan error, 1)
				go func() { done <- s.readFrameStream(ctx, "slot", frame, stream.offer) }()

				for i := range frame.blocks {
					var block []byte
					for block == nil {
						block, _ = stream.take()
						if block == nil {
							select {
							case <-stream.updates:
							case <-time.After(3 * time.Second):
								t.Fatalf("block %d did not arrive", i)
							}
						}
					}
					wantAt := time.Duration(i+1) * period
					if stalled {
						wantAt = 0
						if i > 0 {
							wantAt = time.Second // 900 ms recovery threshold + 100 ms alternate GET.
						}
					}
					if !bytes.Equal(block, data[i*driveStreamBlockBytes:(i+1)*driveStreamBlockBytes]) || time.Since(started) != wantAt {
						t.Fatalf("unexpected data or delivery time for block %d: got %v, want %v", i, time.Since(started), wantAt)
					}
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				wantRescues := int32(0)
				if stalled {
					wantRescues = 1
				}
				if primary.Load() != 1 || rescue.Load() != wantRescues {
					t.Fatalf("got %d primary and %d alternate downloads", primary.Load(), rescue.Load())
				}
			})
		})
	}
}

func TestDriveProgressiveRetryRetainsUnusedRescue(t *testing.T) {
	for _, stall := range []bool{false, true} {
		t.Run(fmt.Sprint("stalled-retry=", stall), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				data := driveStreamTestData(5 * driveStreamBlockBytes)
				frame := driveStreamTestFrame(data)
				stream := newSlotStreamBuffer(frame)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				ctx = context.WithValue(ctx, mediaHeadKey{}, true)
				ctx = context.WithValue(ctx, progressiveMediaKey{}, true)
				ctx = context.WithValue(ctx, mediaDeliveryKey{}, stream.deliveryState)

				var primary, rescue atomic.Int32
				retryAt := make(chan time.Time, 1)
				rescueAt := make(chan time.Time, 1)
				rt := roundTrip(func(req *http.Request) (*http.Response, error) {
					m := req.Context().Value(driveMediaKey{}).(*driveMediaRequest)
					var start, last int
					if _, err := fmt.Sscanf(req.Header.Get("Range"), "bytes=%d-%d", &start, &last); err != nil {
						return nil, err
					}
					if last != len(data)-1 {
						t.Errorf("unexpected end %d", last)
					}
					part := data[start : last+1]
					var body io.ReadCloser = io.NopCloser(bytes.NewReader(part))
					switch {
					case m.rescue:
						rescue.Add(1)
						rescueAt <- time.Now()
					case primary.Add(1) == 1:
						// Drop the body after exactly one verified block.
						body = io.NopCloser(bytes.NewReader(part[:driveStreamBlockBytes]))
					default:
						retryAt <- time.Now()
						if stall {
							body = &driveStreamTestGatedBody{
								ctx: req.Context(), data: part,
								release: make(chan struct{}), closed: make(chan struct{}),
							}
						}
					}
					if (m.rescue || primary.Load() > 1) && start != driveStreamBlockBytes {
						t.Errorf("already verified prefix was requested again: %q", req.Header.Get("Range"))
					}
					return driveStreamTestResponse(req, http.StatusPartialContent,
						fmt.Sprintf("bytes %d-%d/%d", start, last, len(data)), int64(len(part)), body), nil
				})
				s := memoryDrive(rt)
				s.backoff = 2 * time.Second // even the minimum backoff exceeds the learned recovery delay
				s.mediaClients = []*http.Client{s.client}
				s.bulkClient = &http.Client{Transport: rt}
				s.mediaHedgeTokens = 1
				for range rangeLatencyMin {
					s.mediaLatency[1].note(500 * time.Millisecond)
				}
				done := make(chan error, 1)
				go func() { done <- s.readFrameStream(ctx, "item", frame, stream.offer) }()
				for i := range frame.blocks {
					for {
						if block, _ := stream.take(); block != nil {
							if !bytes.Equal(block, data[i*driveStreamBlockBytes:(i+1)*driveStreamBlockBytes]) {
								t.Fatalf("incorrect ordered bytes at block %d", i)
							}
							break
						}
						select {
						case <-stream.updates:
						case <-time.After(4 * time.Second):
							t.Fatalf("missing block %d after retry; attempts=%d rescues=%d", i, primary.Load(), rescue.Load())
						}
					}
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				retry := <-retryAt
				wantRescues := int32(0)
				if stall {
					wantRescues = 1
				}
				if primary.Load() != 2 || rescue.Load() != wantRescues {
					t.Fatalf("got %d primary attempts and %d rescues", primary.Load(), rescue.Load())
				}
				if stall && (<-rescueAt).Sub(retry) != mediaHedgeFloor {
					t.Fatal("rescue clock did not start at actual retry dispatch")
				}
			})
		})
	}
}

type discoveryCadenceFixture struct {
	*streamReaderFixture
	publishAt time.Time
	nextSeen  chan struct{}
}

func (s *discoveryCadenceFixture) readDescription(ctx context.Context, id string) (string, error) {
	if id != "in1" {
		return s.streamReaderFixture.readDescription(ctx, id)
	}
	if time.Now().Before(s.publishAt) {
		return "", nil
	}
	select {
	case s.nextSeen <- struct{}{}:
	default:
	}
	return newSlotFrame(1, slotEnd, nil).encode(), nil
}

func (s *discoveryCadenceFixture) readFrameStream(ctx context.Context, id string, frame slotFrame, emit func(int, []byte) error) error {
	for i := range frame.blocks {
		if err := batchTestWait(ctx, 550*time.Millisecond); err != nil {
			return err
		}
		start := i * slotStreamBlockBytes
		if err := emit(i, bytes.Clone(s.data[start:start+streamBlockSize(frame.size, i)])); err != nil {
			return err
		}
	}
	// Keep the earlier frame unacknowledged until we observe next-frame discovery.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.eof:
		return nil
	}
}

func TestProgressiveDeliveryKeepsSuccessorDiscoveryActive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := bytes.Repeat([]byte("v"), 8*slotStreamBlockBytes)
		store := &discoveryCadenceFixture{
			streamReaderFixture: newStreamReaderFixture(data),
			publishAt:           time.Now().Add(4 * time.Second), nextSeen: make(chan struct{}, 1),
		}
		ctx, cancel := context.WithCancel(context.Background())
		reader := newSlotReader(ctx, store, slotTable{Slots: []string{"in0", "in1"}, Watermark: "wm", progressiveMedia: true}, streamReaderParams(), nil)
		defer func() { cancel(); reader.wait() }()
		for range store.frame.blocks {
			receiveStreamChunk(t, reader)
		}
		select {
		case <-store.nextSeen:
		case <-time.After(1200 * time.Millisecond):
			t.Fatal("successor discovery went idle while the head was still delivering blocks")
		}
		select {
		case ack := <-store.acks:
			t.Fatalf("frame acknowledged before EOF: %d", ack)
		default:
		}
		close(store.eof)
		select {
		case _, ok := <-reader.chunks():
			if ok || reader.Err() != io.EOF {
				t.Fatalf("expected ordered completion, got %v", reader.Err())
			}
		case <-time.After(2 * time.Second):
			t.Fatal("discovered successor was not delivered")
		}
	})
}

func TestDriveStreamRetriesOneMissingPublishedBody(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint("permanently-missing=", missing), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				data := driveStreamTestData(2 * driveStreamBlockBytes)
				frame := driveStreamTestFrame(data)
				var calls int
				s := memoryDrive(roundTrip(func(req *http.Request) (*http.Response, error) {
					calls++
					if calls == 1 || missing {
						return memoryResponse(http.StatusNotFound, ""), nil
					}
					return driveStreamTestResponse(req, http.StatusOK, "", int64(len(data)), io.NopCloser(bytes.NewReader(data))), nil
				}))
				var delivered []byte
				err := s.readFrameStream(context.Background(), "published-slot", frame, func(_ int, block []byte) error {
					delivered = append(delivered, block...)
					return nil
				})
				if calls != 2 {
					t.Fatalf("expected one body retry, got %d total requests", calls)
				}
				if missing {
					if err != errNotFound || len(delivered) != 0 {
						t.Fatalf("missing body did not fail closed: %v, %d bytes", err, len(delivered))
					}
				} else if err != nil || !bytes.Equal(delivered, data) {
					t.Fatalf("transient absence interrupted delivery: %v, %d bytes", err, len(delivered))
				}
			})
		})
	}
}
