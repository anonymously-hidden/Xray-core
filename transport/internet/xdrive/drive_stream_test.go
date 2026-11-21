package xdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type streamMetadataFixture struct {
	mu     sync.Mutex
	rows   map[string]map[string]interface{}
	frames []slotFrame
	bodies [][]byte
	calls  int
}

func (s *streamMetadataFixture) roundTrip(request *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	id := path.Base(request.URL.Path)
	var row map[string]interface{}
	if request.Method == http.MethodGet {
		row = s.rows[id]
	} else {
		reader := io.Reader(request.Body)
		kind, params, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
		if err != nil {
			return nil, err
		}
		if kind == "multipart/related" {
			parts := multipart.NewReader(reader, params["boundary"])
			part, err := parts.NextPart()
			if err != nil {
				return nil, err
			}
			if err := json.NewDecoder(part).Decode(&row); err != nil {
				return nil, err
			}
			body, err := parts.NextPart()
			if err != nil {
				return nil, err
			}
			data, err := io.ReadAll(body)
			if err != nil {
				return nil, err
			}
			s.bodies = append(s.bodies, data)
		} else if err := json.NewDecoder(reader).Decode(&row); err != nil {
			return nil, err
		}
		if request.Method == http.MethodPost {
			id, _ = row["id"].(string)
		}
		s.rows[id] = row
		if description, ok := row["description"].(string); ok {
			if frame, ok := decodeSlot(description); ok {
				s.frames = append(s.frames, frame)
			}
		}
	}
	if row == nil {
		row = map[string]interface{}{}
	}
	payload, err := json.Marshal(row)
	if err != nil {
		return nil, err
	}
	return driveStreamTestResponse(request, http.StatusOK, "", int64(len(payload)), io.NopCloser(bytes.NewReader(payload))), nil
}

func TestDriveStreamNegotiationMetadataNoExtraCalls(t *testing.T) {
	fixture := &streamMetadataFixture{rows: make(map[string]map[string]interface{})}
	client := driveStreamTestStorage(t, fixture.roundTrip, 0)
	server := driveStreamTestStorage(t, fixture.roundTrip, 0)
	client.configureStreaming("metadata-epoch", false)
	server.configureStreaming("metadata-epoch", true)
	ctx := context.Background()
	if err := client.createSlot(ctx, "up", "streams/metadata-epoch/c2s/s0", "", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := client.readDescription(ctx, "up"); err != nil {
		t.Fatal(err)
	}
	if client.streamingEnabled() {
		t.Fatal("client self-negotiated from outgoing metadata")
	}
	if err := server.setSlot(ctx, "wm", "0", nil); err != nil {
		t.Fatal(err)
	}
	value, err := client.readDescription(ctx, "wm")
	if err != nil || value != "0" {
		t.Fatalf("watermark changed: %q, %v", value, err)
	}
	if !client.streamingEnabled() {
		t.Fatal("server acknowledgment did not enable streaming")
	}
	if client.streamingProperties() != nil {
		t.Fatal("client advertised its own acceptance after confirmation")
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.calls != 4 {
		t.Fatalf("capability negotiation added API requests: %d", fixture.calls)
	}
	if _, ok := fixture.rows["up"]["properties"]; ok {
		t.Fatal("client outgoing marker was published")
	}
	if _, ok := fixture.rows["wm"]["properties"]; !ok {
		t.Fatal("server public acceptance marker missing")
	}
}

func TestDriveStreamWriterUsesOnlyConfirmedPeer(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		name := "legacy"
		if accepted {
			name = "streaming"
		}
		t.Run(name, func(t *testing.T) {
			fixture := &streamMetadataFixture{rows: make(map[string]map[string]interface{})}
			store := driveStreamTestStorage(t, fixture.roundTrip, 0)
			store.uploadURL = store.filesURL + "?uploadType=multipart"
			store.configureStreaming("writer-epoch", false)
			if accepted {
				store.observeStreamingProperties(map[string]string{driveStreamCapsVersionKey: "1", driveStreamCapsSessionKey: "writer-epoch"})
			}
			writer := newSlotWriter(context.Background(), store, slotTable{Slots: []string{"out0", "out1"}, Watermark: "writer-wm"}, streamReaderParams())
			payload := bytes.Repeat([]byte("w"), defaultSegmentBytes)
			if _, err := writer.Write(payload); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			writer.wait()
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			wanted := byte(slotMedia)
			if accepted {
				wanted = slotStream
			}
			found := false
			for _, frame := range fixture.frames {
				if frame.seq == 0 && frame.size == len(payload) {
					found = true
					if frame.mode != wanted {
						t.Fatalf("published mode %d, want %d", frame.mode, wanted)
					}
				}
			}
			if !found {
				t.Fatal("published media frame missing")
			}
		})
	}
}

func TestDriveWriterPublishesPrefixAndMediaInOneRequest(t *testing.T) {
	for _, tc := range []struct {
		name            string
		streams, prefix bool
	}{{"legacy", false, false}, {"older_streaming", true, false}, {"prefix_reader", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := &streamMetadataFixture{rows: make(map[string]map[string]interface{})}
			store := driveStreamTestStorage(t, fixture.roundTrip, 0)
			store.uploadURL = store.filesURL + "?uploadType=multipart"
			store.configureStreaming("writer-epoch", tc.streams)
			writer := newSlotWriter(context.Background(), store, slotTable{
				Slots: append([]string{"out0", "out1"}, make([]string, 62)...), Watermark: "writer-wm", streamPrefix: tc.prefix,
			}, streamReaderParams())
			payload := streamTestData(defaultSegmentBytes)
			if _, err := writer.Write(payload); err != nil {
				t.Fatal(err)
			}
			writer.wg.Wait()
			fixture.mu.Lock()
			calls, frames, bodies := fixture.calls, fixture.frames, fixture.bodies
			fixture.mu.Unlock()
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			writer.wait()
			if calls != 1 || len(frames) != 1 || len(bodies) != 1 {
				t.Fatalf("first frame needed more than one upload: %d calls, %d frames and %d bodies", calls, len(frames), len(bodies))
			}
			wantPrefix := 0
			if tc.prefix {
				wantPrefix = slotStreamPrefixBytes
			}
			if !bytes.Equal(frames[0].prefix, payload[:wantPrefix]) || !bytes.Equal(bodies[0], payload[wantPrefix:]) ||
				!frames[0].holds(bodies[0]) {
				t.Fatal("multipart upload lost, duplicated, or incorrectly framed data")
			}
		})
	}
}

func TestDriveDirectMediaChecksIdentityAndBody(t *testing.T) {
	payload := []byte("direct media payload")
	frame := newSlotFrame(9, slotDirectMedia, payload)
	wire := encodeDirectSlot(frame, payload)
	if got, ok := decodeDirectSlot(wire, frame.seq); !ok || !bytes.Equal(got.data, payload) {
		t.Fatal("valid direct media was rejected")
	}
	for _, body := range [][]byte{wire[:len(wire)-1], append(bytes.Clone(wire), 'x'), encodeDirectSlot(frame, bytes.Repeat([]byte{1}, len(payload)))} {
		if _, ok := decodeDirectSlot(body, frame.seq); ok {
			t.Fatal("corrupted media accepted")
		}
	}
	if _, ok := decodeDirectSlot(wire, frame.seq+1); ok {
		t.Fatal("previous ring generation accepted")
	}
}

type directTestSlots struct {
	*controlledSlots
	drive *driveStorage
}

func (s *directTestSlots) readDirectSlot(ctx context.Context, id string, want slotFrame) (slotFrame, error) {
	return s.drive.readDirectSlot(ctx, id, want)
}

func TestDriveDirectMediaReusesSpeculationOrFallsBack(t *testing.T) {
	for _, tc := range []struct {
		name     string
		desc     time.Duration
		stale    bool
		wantGets int32
	}{
		{"one GET starts delivery before metadata", 3 * time.Second, false, 1},
		{"matching metadata shares existing GET", 20 * time.Millisecond, false, 1},
		{"stale body falls back to fresh GET", 20 * time.Millisecond, true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				frame := newSlotFrame(1, slotDirectMedia, []byte("next bytes"))
				var gets atomic.Int32
				drive := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
					attempt := gets.Add(1)
					if err := batchTestWait(r.Context(), 100*time.Millisecond); err != nil {
						return nil, err
					}
					got := frame
					if tc.stale && attempt == 1 {
						got.seq--
					}
					return memoryResponse(http.StatusOK, string(encodeDirectSlot(got, []byte("next bytes")))), nil
				}))
				base := newMemSlots()
				store := &directTestSlots{drive: drive, controlledSlots: &controlledSlots{memSlots: base, desc: func(ctx context.Context, id string) (string, error) {
					if err := batchTestWait(ctx, tc.desc); err != nil {
						return "", err
					}
					return frame.encode(), nil
				}}}
				p := paramsFromConfig(&Config{Concurrency: 8})
				r := &slotReader{
					ctx: ctx, store: store, table: slotTable{Slots: []string{"zero", "one"}, directMedia: true},
					params: p, head: 1, directNext: 1, activeAt: time.Now(), hitAt: time.Now(), huntCeiling: slotHuntCeiling, stallTimeout: slotStallTimeout,
				}
				r.bodies.configure(1, p.segmentBytes, 2)
				defer func() { cancel(); r.wait() }()
				f := &slotFetch{ready: make(chan struct{}), streamReady: make(chan struct{}), promoted: make(chan struct{})}
				start := time.Now()
				r.fetch(ctx, frame.seq, 0, f)
				if f.err != nil || string(f.body) != "next bytes" || gets.Load() != tc.wantGets || time.Since(start) > 250*time.Millisecond {
					t.Fatalf("bad delivery: err=%v body=%q GETs=%d elapsed=%v", f.err, f.body, gets.Load(), time.Since(start))
				}
			})
		})
	}
}

func TestDriveDirectMediaWriterRequiresClientOffer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, offered := range []bool{false, true} {
			ctx, cancel := context.WithCancel(context.Background())
			store := &batchTestSlots{memSlots: newMemSlots(), streaming: true, warm: 4}
			p := paramsFromConfig(&Config{Concurrency: 64})
			ring, err := makeRing(ctx, store, "direct-write", p)
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			ring.Down.directMedia = offered
			w := newSlotWriter(ctx, store, ring.Down, p)
			payload := bytes.Repeat([]byte{42}, slotDirectMediaMax)
			if _, err := w.Write(payload); err != nil {
				cancel()
				w.wait()
				t.Fatal(err)
			}
			err = w.Close()
			cancel()
			w.wait()
			if err != nil {
				t.Fatal(err)
			}
			desc, _ := store.readDescription(context.Background(), ring.Down.Slots[0])
			frame, ok := decodeSlot(desc)
			body, _ := store.readBody(context.Background(), ring.Down.Slots[0])
			if offered {
				if direct, good := decodeDirectSlot(body, 0); !ok || frame.mode != slotDirectMedia || !good || !bytes.Equal(payload, direct.data) {
					t.Fatal("negotiated direct media was not written")
				}
			} else if !ok || frame.mode != slotStream || !bytes.Equal(payload, body) {
				t.Fatal("legacy reader received changed media format")
			}
		}
	})
}

func TestDriveProgressiveMediaDefaultSession(t *testing.T) {
	t.Setenv("XRAY_XDRIVE_DIRECT_MEDIA", "")
	drive := newFakeDrive(t)
	settings := drive.settings(t)
	settings.ProtocolSettings.(*Config).Concurrency = 8
	client, server, cleanup := pairWith(t, settings)
	defer cleanup()
	reader := client.(*Conn).reader.(*slotReader)
	writer := server.(*Conn).writer.(*slotWriter)
	if !reader.table.progressiveMedia || !writer.table.progressiveMedia {
		t.Fatal("default session did not negotiate progressive downstream media")
	}

	data := bytes.Repeat([]byte("d"), defaultSegmentBytes)
	written := make(chan error, 1)
	go func() { _, err := server.Write(data); written <- err }()
	if err := client.SetReadDeadline(time.Now().Add(testPatience)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(data))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil || !bytes.Equal(got, data) {
		t.Fatalf("default progressive transfer failed: %v", err)
	}
	drive.mu.Lock()
	defer drive.mu.Unlock()
	found := false
	for _, file := range drive.files {
		if frame, ok := decodeSlot(file.description); ok && frame.mode == slotStream && frame.size+len(frame.prefix) == len(data) {
			found = true
		}
	}
	if !found {
		t.Fatal("no streaming frame was published in the default session")
	}
}

func TestDriveDirectMediaCanBeDisabled(t *testing.T) {
	t.Setenv("XRAY_XDRIVE_DIRECT_MEDIA", "0")
	drive := newFakeDrive(t)
	settings := drive.settings(t)
	settings.ProtocolSettings.(*Config).Concurrency = 8
	client, server, cleanup := pairWith(t, settings)
	defer cleanup()
	reader := client.(*Conn).reader.(*slotReader)
	writer := server.(*Conn).writer.(*slotWriter)
	if reader.table.directMedia || writer.table.directMedia || reader.table.progressiveMedia || writer.table.progressiveMedia {
		t.Fatal("disabled session unexpectedly negotiated direct media")
	}
}

func TestDirectMediaConfirmedHeadRecovery(t *testing.T) {
	for _, tc := range []struct {
		name            string
		primary, rescue time.Duration
		stale           bool
		wantGets        int32
		want            time.Duration
	}{
		{"healthy", 100 * time.Millisecond, 0, false, 1, 100 * time.Millisecond},
		{"stalled", 21 * time.Second, 100 * time.Millisecond, false, 2, time.Second},
		{"stale rescue cannot cancel valid primary", 1400 * time.Millisecond, 100 * time.Millisecond, true, 2, 1400 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				data := bytes.Repeat([]byte("v"), 512<<10)
				frame := newSlotFrame(0, slotDirectMedia, data)
				var gets atomic.Int32
				drive := memoryDrive(roundTrip(func(req *http.Request) (*http.Response, error) {
					gets.Add(1)
					d := tc.primary
					reply := frame
					if m, _ := req.Context().Value(driveMediaKey{}).(*driveMediaRequest); m != nil && m.rescue {
						d = tc.rescue
						if tc.stale {
							reply.seq++
						}
					}
					if err := batchTestWait(req.Context(), d); err != nil {
						return nil, err
					}
					return memoryResponse(http.StatusOK, string(encodeDirectSlot(reply, data))), nil
				}))
				drive.mediaClients = []*http.Client{drive.client}
				drive.bulkClient = &http.Client{Transport: drive.client.Transport}
				drive.mediaHedgeTokens = 1
				for range rangeLatencyMin {
					drive.mediaLatency[1].note(500 * time.Millisecond)
				}
				store := &directTestSlots{drive: drive, controlledSlots: &controlledSlots{memSlots: newMemSlots(), desc: func(context.Context, string) (string, error) { return frame.encode(), nil }}}
				p := paramsFromConfig(&Config{Concurrency: 8})
				r := &slotReader{ctx: ctx, store: store, table: slotTable{Slots: []string{"zero", "one"}, directMedia: true}, params: p, active: make(chan struct{}), activeAt: time.Now(), huntCeiling: slotHuntCeiling, stallTimeout: slotStallTimeout}
				r.bodies.configure(0, p.segmentBytes, 2)
				defer func() { cancel(); r.wait() }()
				f := &slotFetch{ready: make(chan struct{}), streamReady: make(chan struct{}), promoted: make(chan struct{})}
				start := time.Now()
				r.fetch(ctx, 0, 0, f)
				if f.err != nil || !bytes.Equal(f.body, data) || gets.Load() != tc.wantGets || time.Since(start) != tc.want {
					t.Fatalf("err=%v valid=%v GETs=%d elapsed=%v", f.err, bytes.Equal(f.body, data), gets.Load(), time.Since(start))
				}
			})
		})
	}
}

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
	for _, tc := range []struct {
		stalled        bool
		period, median time.Duration
	}{
		{false, 450 * time.Millisecond, 500 * time.Millisecond},
		{false, 1100 * time.Millisecond, 2 * time.Second},
		{true, 0, 500 * time.Millisecond},
		{true, 0, 2 * time.Second},
	} {
		stalled, period := tc.stalled, tc.period
		t.Run(fmt.Sprintf("stalled=%v/period=%s/median=%s", stalled, period, tc.median), func(t *testing.T) {
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
						if m.start != 2*driveStreamBlockBytes || m.end != size {
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
						if err := batchTestWait(req.Context(), 100*time.Millisecond); err != nil {
							return nil, err
						}
						body = &driveStreamTestGatedBody{
							ctx: req.Context(), data: part, pauseAt: 2 * driveStreamBlockBytes,
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
					s.mediaLatency[1].note(tc.median)
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
						wantAt = 100 * time.Millisecond
						if i >= 2 {
							wantAt += time.Second // 900 ms recovery threshold + 100 ms alternate GET.
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

func TestDriveProgressiveStartupLearnsFirstVerifiedBlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := driveStreamTestData(5 * driveStreamBlockBytes)
		frame := driveStreamTestFrame(data)
		var primaries, rescues atomic.Int32
		rt := roundTrip(func(req *http.Request) (*http.Response, error) {
			m := req.Context().Value(driveMediaKey{}).(*driveMediaRequest)
			var body io.ReadCloser
			switch {
			case m.rescue:
				rescues.Add(1)
				if err := batchTestWait(req.Context(), 100*time.Millisecond); err != nil {
					return nil, err
				}
				body = io.NopCloser(bytes.NewReader(data))
			case primaries.Add(1) <= rangeLatencyMin:
				body = io.NopCloser(cadenceBody{req.Context(), bytes.NewReader(data), 450 * time.Millisecond})
			default:
				body = &driveStreamTestGatedBody{
					ctx: req.Context(), data: data,
					release: make(chan struct{}), closed: make(chan struct{}),
				}
			}
			return driveStreamTestResponse(req, http.StatusOK, "", int64(len(data)), body), nil
		})
		s := memoryDrive(rt)
		s.mediaClients = []*http.Client{s.client}
		s.bulkClient = &http.Client{Transport: rt}
		s.mediaHedgeTokens = 1
		ctx := context.WithValue(context.Background(), progressiveMediaKey{}, true)
		for range rangeLatencyMin {
			if err := s.readFrameStream(ctx, "training", frame, func(int, []byte) error { return nil }); err != nil {
				t.Fatal(err)
			}
		}
		ctx = context.WithValue(ctx, mediaHeadKey{}, true)
		ctx = context.WithValue(ctx, mediaDeliveryKey{}, func() (int, <-chan struct{}, time.Time) {
			return 0, nil, time.Now()
		})
		at := time.Now()
		var got []byte
		err := s.readFrameStream(ctx, "stalled", frame, func(_ int, p []byte) error {
			got = append(got, p...)
			return nil
		})
		if err != nil || !bytes.Equal(got, data) || time.Since(at) != time.Second || rescues.Load() != 1 {
			t.Fatalf("startup recovery took %v, got %d bytes using %d rescues: %v", time.Since(at), len(got), rescues.Load(), err)
		}
	})
}

func TestDriveStreamRetriesUnavailableSuffix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := driveStreamTestData(3 * driveStreamBlockBytes)
		var calls int
		s := memoryDrive(roundTrip(func(req *http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				// The first copy drops after one complete block.
				body := io.NopCloser(bytes.NewReader(data[:driveStreamBlockBytes]))
				return driveStreamTestResponse(req, http.StatusOK, "", int64(len(data)), body), nil
			}
			mediaTestCheckRequest(t, req, driveStreamBlockBytes, len(data))
			if calls == 2 {
				// A stale copy does not contain the requested suffix.
				return memoryResponse(http.StatusRequestedRangeNotSatisfiable, ""), nil
			}
			return driveStreamTestResponse(req, http.StatusPartialContent,
				fmt.Sprintf("bytes %d-%d/%d", driveStreamBlockBytes, len(data)-1, len(data)),
				int64(len(data)-driveStreamBlockBytes), io.NopCloser(bytes.NewReader(data[driveStreamBlockBytes:]))), nil
		}))
		var delivered []byte
		err := s.readFrameStream(context.Background(), "item", driveStreamTestFrame(data), func(_ int, p []byte) error {
			delivered = append(delivered, p...)
			return nil
		})
		if err != nil || calls != 3 || !bytes.Equal(delivered, data) {
			t.Fatalf("suffix did not recover in order: %d requests, %d bytes, err=%v", calls, len(delivered), err)
		}
	})
}
