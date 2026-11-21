package xdrive

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

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

func TestDriveDirectMediaDefaultSession(t *testing.T) {
	t.Setenv("XRAY_XDRIVE_DIRECT_MEDIA", "")
	drive := newFakeDrive(t)
	settings := drive.settings(t)
	settings.ProtocolSettings.(*Config).Concurrency = 8
	client, server, cleanup := pairWith(t, settings)
	defer cleanup()
	reader := client.(*Conn).reader.(*slotReader)
	writer := server.(*Conn).writer.(*slotWriter)
	if !reader.table.directMedia || !writer.table.directMedia {
		t.Fatal("default session did not negotiate direct media in both directions")
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
		t.Fatalf("default direct transfer failed: %v", err)
	}
	drive.mu.Lock()
	defer drive.mu.Unlock()
	found := false
	for _, file := range drive.files {
		if frame, ok := decodeSlot(file.description); ok && frame.mode == slotDirectMedia {
			found = true
		}
	}
	if !found {
		t.Fatal("no direct frame was published in the default session")
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
	if reader.table.directMedia || writer.table.directMedia {
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
