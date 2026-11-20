package xdrive

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestParseStreamHeadRanges(t *testing.T) {
	cases := []struct {
		raw   string
		want  int
		valid bool
	}{
		{raw: "", want: 4, valid: true},
		{raw: " \t\r\n", want: 4, valid: true},
		{raw: "2", want: 2, valid: true},
		{raw: "4", want: 4, valid: true},
		{raw: " \t2\r\n", want: 2, valid: true},
		{raw: "\u20034\u00a0", want: 4, valid: true},
		{raw: "1", want: 4},
		{raw: "0", want: 4},
		{raw: "3", want: 4},
		{raw: "8", want: 4},
		{raw: "-2", want: 4},
		{raw: "+2", want: 4},
		{raw: "02", want: 4},
		{raw: "04", want: 4},
		{raw: "2.0", want: 4},
		{raw: "2e0", want: 4},
		{raw: "2 4", want: 4},
		{raw: "2\n4", want: 4},
		{raw: "two", want: 4},
		{raw: "default", want: 4},
		{raw: "auto", want: 4},
		{raw: "4\x00", want: 4},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("raw=%q", tc.raw), func(t *testing.T) {
			got, valid := parseStreamHeadRanges(tc.raw)
			if got != tc.want || valid != tc.valid {
				t.Fatalf("parseStreamHeadRanges(%q) = (%d, %t), want (%d, %t)", tc.raw, got, valid, tc.want, tc.valid)
			}
		})
	}
}

func TestStreamHeadRangeLimit(t *testing.T) {
	for _, captured := range []int{0, 1, 2, 3, 4, 8, -2} {
		t.Run(fmt.Sprintf("captured=%d", captured), func(t *testing.T) {
			s := &driveStorage{streamHeadRanges: captured}
			want := 4
			if captured == 2 {
				want = 2
			}
			if got := s.streamHeadRangeLimit(); got != want {
				t.Fatalf("streamHeadRangeLimit() = %d, want %d", got, want)
			}
		})
	}
}

func TestStreamHeadRangeLimitCopiesToIndependentSessions(t *testing.T) {
	// No constructor, connection factory, credentials or HTTP request is needed
	// to exercise session copies. These owners have no keeper or bound connection.
	owners := []struct {
		name     string
		captured int
		want     int
		storage  *driveStorage
	}{
		{name: "experiment", captured: 2, want: 2},
		{name: "control", captured: 4, want: 4},
		{name: "zero-value", captured: 0, want: 4},
	}
	t.Setenv(streamHeadRangesEnv, "4")
	var sessions []*driveStorage
	var limits []int
	for i := range owners {
		owner := &owners[i]
		owner.storage = &driveStorage{
			driveShared:      &driveShared{},
			streamHeadRanges: owner.captured,
		}
		t.Cleanup(func() {
			if err := owner.storage.Close(); err != nil {
				t.Errorf("close %s owner: %v", owner.name, err)
			}
		})
		for range 2 {
			session, ok := owner.storage.session().(*driveStorage)
			if !ok {
				t.Fatalf("%s owner did not create a driveStorage session", owner.name)
			}
			t.Cleanup(func() {
				if err := session.Close(); err != nil {
					t.Errorf("close %s session: %v", owner.name, err)
				}
			})
			if session == owner.storage || session.driveShared != owner.storage.driveShared {
				t.Fatalf("%s session did not retain its owner's shared state in a separate storage", owner.name)
			}
			if session.streamHeadRanges != owner.captured {
				t.Fatalf("%s session captured %d, want %d", owner.name, session.streamHeadRanges, owner.captured)
			}
			sessions = append(sessions, session)
			limits = append(limits, owner.want)
		}
	}
	// Changing ENV after publication must not change owners or their sessions.
	t.Setenv(streamHeadRangesEnv, "2")
	for _, owner := range owners {
		if got := owner.storage.streamHeadRangeLimit(); got != owner.want {
			t.Fatalf("%s owner limit = %d, want %d", owner.name, got, owner.want)
		}
	}
	for i, session := range sessions {
		if got := session.streamHeadRangeLimit(); got != limits[i] {
			t.Fatalf("session %d limit = %d, want %d", i, got, limits[i])
		}
		for j := 0; j < i; j++ {
			if session == sessions[j] {
				t.Fatalf("sessions %d and %d reused the same storage", i, j)
			}
		}
	}
}

type driveStreamExperimentRange struct {
	start, end int
}

type driveStreamExperimentRequests struct {
	mu     sync.Mutex
	ranges []driveStreamExperimentRange
}

// Only the approved streaming fixtures are reused. Every media GET terminates
// in this fake RoundTripper, with no network, OAuth refresh or rescue transport.
func driveStreamExperimentTestStorage(t *testing.T, data []byte, captured int) (*driveStorage, *driveStreamExperimentRequests) {
	t.Helper()
	requests := &driveStreamExperimentRequests{}
	transport := driveStreamTestTransport(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.Query().Get("alt") != "media" {
			return nil, fmt.Errorf("unexpected fake request: %s %s", request.Method, request.URL)
		}
		media, ok := request.Context().Value(driveMediaKey{}).(*driveMediaRequest)
		if !ok || media == nil || media.rescue {
			return nil, fmt.Errorf("expected a primary media range")
		}
		var start, last int
		if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &last); err != nil {
			return nil, fmt.Errorf("invalid fake Range: %w", err)
		}
		if start < 0 || last < start || last >= len(data) || request.Header.Get("Range") != fmt.Sprintf("bytes=%d-%d", start, last) {
			return nil, fmt.Errorf("out-of-bounds fake Range: %q", request.Header.Get("Range"))
		}
		requests.mu.Lock()
		requests.ranges = append(requests.ranges, driveStreamExperimentRange{start: start, end: last + 1})
		requests.mu.Unlock()
		return driveStreamTestResponse(request, http.StatusPartialContent,
			fmt.Sprintf("bytes %d-%d/%d", start, last, len(data)), int64(last-start+1),
			io.NopCloser(bytes.NewReader(data[start:last+1]))), nil
	})
	s := driveStreamTestStorage(t, transport, driveMediaLanes)
	s.streamHeadRanges = captured
	return s, requests
}

func (r *driveStreamExperimentRequests) snapshot() []driveStreamExperimentRange {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]driveStreamExperimentRange(nil), r.ranges...)
}

func TestDriveFrameStreamHeadRangeExperiment(t *testing.T) {
	cases := []struct {
		name     string
		captured int
		head     bool
		promoted bool
		size     int
		parts    int
	}{
		{name: "default-head", head: true, size: defaultSegmentBytes, parts: 4},
		{name: "control-head", captured: 4, head: true, size: defaultSegmentBytes, parts: 4},
		{name: "experiment-head", captured: 2, head: true, size: defaultSegmentBytes, parts: 2},
		{name: "control-nonhead", captured: 4, size: defaultSegmentBytes, parts: 1},
		{name: "experiment-nonhead", captured: 2, size: defaultSegmentBytes, parts: 1},
		{name: "promoted-nonhead", captured: 2, promoted: true, size: defaultSegmentBytes, parts: 1},
		{name: "large-nonhead-not-capped", captured: 2, size: 4 * defaultSegmentBytes, parts: 4},
		{name: "small-head-not-expanded", captured: 2, head: true, size: driveMediaPart, parts: 1},
		{name: "partial-final-block", captured: 2, head: true, size: defaultSegmentBytes + 17, parts: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := driveStreamTestData(tc.size)
			frame := driveStreamTestFrame(data)
			s, requests := driveStreamExperimentTestStorage(t, data, tc.captured)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if tc.head {
				ctx = context.WithValue(ctx, mediaHeadKey{}, true)
			}
			if tc.promoted {
				promoted := make(chan struct{})
				close(promoted)
				ctx = context.WithValue(ctx, mediaPromotionKey{}, (<-chan struct{})(promoted))
			}
			blocks := make([][]byte, len(frame.blocks))
			var mu sync.Mutex
			err := s.readFrameStream(ctx, "fake-stream-slot", frame, func(index int, block []byte) error {
				mu.Lock()
				defer mu.Unlock()
				if index < 0 || index >= len(blocks) {
					return fmt.Errorf("unexpected block index %d", index)
				}
				if blocks[index] != nil {
					return fmt.Errorf("primary ranges emitted block %d twice", index)
				}
				blocks[index] = block
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			ranges := requests.snapshot()
			if len(ranges) != tc.parts {
				t.Fatalf("primary GETs = %d, want %d", len(ranges), tc.parts)
			}
			sort.Slice(ranges, func(i, j int) bool { return ranges[i].start < ranges[j].start })
			for i, got := range ranges {
				want := driveStreamExperimentRange{
					start: (i * len(frame.blocks) / tc.parts) * driveStreamBlockBytes,
					end:   min(((i+1)*len(frame.blocks)/tc.parts)*driveStreamBlockBytes, len(data)),
				}
				if got != want {
					t.Fatalf("range %d = %+v, want %+v", i, got, want)
				}
			}
			if len(s.mediaClients) != 4 || s.mediaNext.Load() != uint32(tc.parts) {
				t.Fatalf("media lanes/rotation = %d/%d, want 4/%d", len(s.mediaClients), s.mediaNext.Load(), tc.parts)
			}
			for i, block := range blocks {
				start := i * driveStreamBlockBytes
				if !bytes.Equal(block, data[start:min(start+driveStreamBlockBytes, len(data))]) {
					t.Fatalf("block %d differs from the frame", i)
				}
			}
			if !bytes.Equal(bytes.Join(blocks, nil), data) {
				t.Fatal("streamed bytes differ from the frame")
			}
			// Ownership remains independent of both other emitted blocks and the
			// response source, including a short final block.
			first := data[0]
			blocks[0][0] ^= 0xff
			if data[0] != first {
				t.Fatal("emitted block aliases the response source")
			}
			for i := 1; i < len(blocks); i++ {
				start := i * driveStreamBlockBytes
				if !bytes.Equal(blocks[i], data[start:min(start+driveStreamBlockBytes, len(data))]) {
					t.Fatalf("block %d aliases another emitted block", i)
				}
			}
			second := blocks[1][0]
			data[driveStreamBlockBytes] ^= 0xff
			if blocks[1][0] != second {
				t.Fatal("response source mutation changed an emitted block")
			}
		})
	}
}

func TestDriveFrameStreamExperimentLeavesMode2Unchanged(t *testing.T) {
	for _, head := range []bool{false, true} {
		t.Run(fmt.Sprintf("head=%t", head), func(t *testing.T) {
			data := driveStreamTestData(defaultSegmentBytes)
			frame := newSlotFrame(7, slotMedia, data)
			s, requests := driveStreamExperimentTestStorage(t, data, 2)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			parts := 1
			if head {
				ctx = context.WithValue(ctx, mediaHeadKey{}, true)
				parts = 4
			}
			body, err := s.readFrameBody(ctx, "fake-mode2-slot", frame)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(requests.snapshot()); got != parts {
				t.Fatalf("MODE2 primary GETs = %d, want %d", got, parts)
			}
			if !bytes.Equal(body, data) {
				t.Fatal("MODE2 bytes differ from the frame")
			}
		})
	}
}
