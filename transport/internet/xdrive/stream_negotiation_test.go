package xdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"path"
	"sync"
	"testing"
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
