package xdrive

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// captureSummaries routes the burst and download lines of the current test to
// the returned function. Only use it in tests whose goroutines have all ended
// when the test returns.
func captureSummaries(t *testing.T) func() []string {
	t.Helper()
	var mu sync.Mutex
	var lines []string
	hook := func(line string) {
		mu.Lock()
		lines = append(lines, line)
		mu.Unlock()
	}
	testSummaryLog.Store(&hook)
	t.Cleanup(func() { testSummaryLog.Store(nil) })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(lines)
	}
}

// summaryField returns the value of key= in line.
func summaryField(t *testing.T, line, key string) string {
	t.Helper()
	for _, field := range strings.Fields(line) {
		if value, ok := strings.CutPrefix(field, key+"="); ok {
			return value
		}
	}
	t.Fatalf("no %s= in %q", key, line)
	return ""
}

func summaryDuration(t *testing.T, line, key string) time.Duration {
	t.Helper()
	d, err := time.ParseDuration(summaryField(t, line, key))
	if err != nil {
		t.Fatalf("%s in %q: %v", key, line, err)
	}
	return d
}

func linesWith(lines []string, parts ...string) []string {
	var out []string
	for _, line := range lines {
		match := true
		for _, part := range parts {
			match = match && strings.Contains(line, part)
		}
		if match {
			out = append(out, line)
		}
	}
	return out
}

func TestTransferMeterReportsOverlap(t *testing.T) {
	var m transferMeter
	a := m.begin()
	b := m.begin()
	c := m.begin()
	if others, peak := m.end(b); others != 1 || peak != 2 {
		t.Fatalf("second download: overlap-start=%d overlap-max=%d, want 1 and 2", others, peak)
	}
	d := m.begin() // a and c are still in flight
	if others, peak := m.end(a); others != 0 || peak != 2 {
		t.Fatalf("first download: overlap-start=%d overlap-max=%d, want 0 and 2", others, peak)
	}
	if others, peak := m.end(c); others != 2 || peak != 2 {
		t.Fatalf("third download: overlap-start=%d overlap-max=%d, want 2 and 2", others, peak)
	}
	if others, peak := m.end(d); others != 2 || peak != 2 {
		t.Fatalf("fourth download: overlap-start=%d overlap-max=%d, want 2 and 2", others, peak)
	}
	if others, peak := m.end(m.begin()); others != 0 || peak != 0 {
		t.Fatalf("lone download: overlap-start=%d overlap-max=%d, want 0 and 0", others, peak)
	}
}

func TestPoolConnectionsHaveDistinctLogIDs(t *testing.T) {
	newClient := func(context.Context) *http.Client {
		return &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) {
			return memoryResponse(http.StatusOK, ""), nil
		})}
	}
	a, b := newConnPool(nil, newClient), newConnPool(nil, newClient)
	defer a.Close()
	defer b.Close()
	first := a.xmux.GetXmuxClient(context.Background()).XmuxConn.(*poolConn).logID()
	second := b.xmux.GetXmuxClient(context.Background()).XmuxConn.(*poolConn).logID()
	if first == 0 || second == 0 || first == second {
		t.Fatalf("pool connections were numbered %d and %d", first, second)
	}
	if id := (*poolConn)(nil).logID(); id != 0 {
		t.Fatalf("no connection was numbered %d", id)
	}
}

func TestSlotWriteBurstGroupsFramesUntilIdle(t *testing.T) {
	lines := captureSummaries(t)
	t0 := time.Unix(1000, 0)
	link := new(slotLink)
	link.noteRead(t0.Add(-300 * time.Millisecond))
	var b slotWriteBurst
	ms := time.Millisecond
	if done := b.add(0, slotInlineLimit, t0, t0.Add(10*ms), 0, link); done != nil {
		t.Fatal("the first frame closed a burst")
	}
	b.add(1, slotInlineLimit, t0, t0.Add(20*ms), 5*ms, link)
	b.add(2, 512<<10, t0, t0.Add(30*ms), 0, link)
	b.finish(slotInline, t0.Add(40*ms), 700*ms, 0, true, t0.Add(740*ms))
	b.finish(slotInline, t0.Add(40*ms), 900*ms, 0, true, t0.Add(940*ms))
	if b.idle(t0.Add(10 * time.Second)) {
		t.Fatal("a burst with an upload in flight went idle")
	}
	b.finish(slotStream, t0.Add(50*ms), 1600*ms, 20*ms, true, t0.Add(1650*ms))
	if b.idle(t0.Add(1650*ms + slotBurstGap - ms)) {
		t.Fatal("the burst went idle before slotBurstGap")
	}
	if !b.idle(t0.Add(1650*ms + slotBurstGap)) {
		t.Fatal("the burst did not go idle after slotBurstGap")
	}
	if got := link.lastWrite(); !got.Equal(t0) {
		t.Fatalf("the link saw the write burst begin at %v, want %v", got, t0)
	}

	later := t0.Add(5 * time.Second)
	done := b.add(3, 100, later, later, 0, link)
	if done == nil || done.frames != 3 || done.lastSeq != 2 {
		t.Fatalf("a frame cut after the gap did not hand back the finished burst: %+v", done)
	}
	if b.frames != 1 || b.firstSeq != 3 || !b.startAt.Equal(later) {
		t.Fatalf("the next burst did not start at the new frame: %+v", b)
	}
	done.log(context.Background(), "watermark-abcdef")
	got := linesWith(lines(), " abcdef ")
	if len(got) != 1 {
		t.Fatalf("want one summary, got %q", got)
	}
	want := "xdrive burst write abcdef seqs=0-2 frames=3 inline=2 media=1 failed=0 bytes=552960 span=1.65s first-put=40ms last-cut=30ms" +
		" inline-put-p50/max=900ms/900ms media-put-p50/max=1.6s/1.6s await-max=20ms permit-wait-max=5ms"
	if !strings.HasPrefix(got[0], want) {
		t.Fatalf("summary\n%q\nwant prefix\n%q", got[0], want)
	}
	if after := summaryField(t, got[0], "after-read"); after != "300ms" {
		t.Fatalf("after-read=%s, want 300ms", after)
	}
}

func TestSlotReadBurstSpansFirstFoundToLastDelivered(t *testing.T) {
	lines := captureSummaries(t)
	t0 := time.Unix(1000, 0)
	ms := time.Millisecond
	link := new(slotLink)
	link.noteWrite(t0)
	var b slotReadBurst
	b.add(5, slotInline, 1000, t0.Add(1000*ms), 0, 0, 0, t0.Add(1100*ms), link)
	b.add(6, slotStream, 4000, t0.Add(1050*ms), 200*ms, 750*ms, 1200*ms, t0.Add(2300*ms), link)
	if b.idle(t0.Add(2300*ms + slotBurstGap - ms)) {
		t.Fatal("the burst went idle before slotBurstGap")
	}
	if !b.idle(t0.Add(2300*ms + slotBurstGap)) {
		t.Fatal("the burst did not go idle after slotBurstGap")
	}
	if got := link.lastRead(); !got.Equal(t0.Add(2300 * ms)) {
		t.Fatalf("the link saw the last delivery at %v", got)
	}
	b.take().log(context.Background(), "watermark-fedcba")
	if b.open {
		t.Fatal("take left the burst open")
	}

	// A write that began after the read burst was found is not its request.
	late := new(slotLink)
	late.noteWrite(t0.Add(2 * time.Second))
	b.add(7, slotInline, 10, t0.Add(1000*ms), 0, 0, 0, t0.Add(1010*ms), late)
	b.take().log(context.Background(), "watermark-fedcba")

	got := linesWith(lines(), " fedcba ")
	if len(got) != 2 {
		t.Fatalf("want two summaries, got %q", got)
	}
	want := "xdrive burst read fedcba seqs=5-6 frames=2 inline=1 media=1 bytes=5000 span=1.3s first-delivery=100ms last-found=50ms" +
		" delivery-gap-max=1.2s head-wait-max=200ms first-block-wait-max=750ms body-max=1.2s mbps=0.03 after-write=2.3s"
	if got[0] != want {
		t.Fatalf("summary\n%q\nwant\n%q", got[0], want)
	}
	if after := summaryField(t, got[1], "after-write"); after != "-1ns" {
		t.Fatalf("a later write was taken as the request: after-write=%s", after)
	}
}

// One live segment fetched one at a time is one summary on each side, and the
// client's read summary times the whole request on its own clock.
func TestSlotConnSummarizesEachSegmentOnBothSides(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lines := captureSummaries(t)
		client, server := slotPair(t, newMemSlots(), 8, 512<<10)
		request := []byte("GET /segment HTTP/1.1\r\n\r\n")
		segment := randomPayload(40 << 10)
		for range 2 {
			if _, err := client.Write(request); err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadFull(server, make([]byte, len(request))); err != nil {
				t.Fatal(err)
			}
			time.Sleep(100 * time.Millisecond) // the origin fetch
			if _, err := server.Write(segment); err != nil {
				t.Fatal(err)
			}
			body := make([]byte, len(segment))
			if _, err := io.ReadFull(client, body); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(body, segment) {
				t.Fatal("the segment arrived corrupted")
			}
			time.Sleep(2 * slotBurstGap)
		}
		client.Close()
		server.Close()

		got := lines()
		segmentWrites := linesWith(got, "xdrive burst write ", " frames=3 inline=3 media=0 failed=0 bytes=40960 ")
		requestWrites := linesWith(got, "xdrive burst write ", " frames=1 inline=1 media=0 failed=0 bytes=25 ")
		segmentReads := linesWith(got, "xdrive burst read ", " frames=3 inline=3 media=0 bytes=40960 ")
		requestReads := linesWith(got, "xdrive burst read ", " frames=1 inline=1 media=0 bytes=25 ")
		if len(segmentWrites) != 2 || len(requestWrites) != 2 || len(segmentReads) != 2 || len(requestReads) != 2 {
			t.Fatalf("want one write and one read summary per request and per segment, got %q", got)
		}
		for _, line := range segmentWrites {
			if after := summaryDuration(t, line, "after-read"); after < 100*time.Millisecond {
				t.Fatalf("the server's segment began %v after the request was read, want the 100ms origin fetch: %q", after, line)
			}
		}
		for _, line := range segmentReads {
			after, span := summaryDuration(t, line, "after-write"), summaryDuration(t, line, "span")
			if after < 100*time.Millisecond || after < span {
				t.Fatalf("request-to-last-frame %v is shorter than the origin fetch or the read span %v: %q", after, span, line)
			}
		}
	})
}

// gatedReader blocks every read until gate is closed.
type gatedReader struct {
	gate <-chan struct{}
	r    io.Reader
}

func (g *gatedReader) Read(p []byte) (int, error) {
	<-g.gate
	return g.r.Read(p)
}

func TestDriveDownloadLogsConnectionAndOverlap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lines := captureSummaries(t)
		gate := make(chan struct{})
		var conns atomic.Uint64
		s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			if note, ok := r.Context().Value(poolConnKey{}).(func(*poolConn)); ok {
				note(&poolConn{id: 40 + conns.Add(1)})
			}
			resp := memoryResponse(http.StatusOK, "")
			resp.Body = io.NopCloser(&gatedReader{gate: gate, r: bytes.NewReader(make([]byte, 1000))})
			return resp, nil
		}))
		var wg sync.WaitGroup
		for _, id := range []string{"file-a", "file-b"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if body, err := s.readBody(context.Background(), id); err != nil || len(body) != 1000 {
					t.Errorf("read %d bytes: %v", len(body), err)
				}
			}()
		}
		synctest.Wait() // both bodies are being read
		close(gate)
		wg.Wait()
		s.closeWork()

		got := append(linesWith(lines(), "xdrive download slot=file-a "), linesWith(lines(), "xdrive download slot=file-b ")...)
		if len(got) != 2 {
			t.Fatalf("want one line per download, got %q", got)
		}
		var starts, ids []string
		for _, line := range got {
			for key, want := range map[string]string{
				"status": "200", "bytes": "1000", "lane": "main", "overlap-max": "1", "new-conn": "false", "result": "ok", "seq": "-1",
			} {
				if value := summaryField(t, line, key); value != want {
					t.Fatalf("%s=%s, want %s: %q", key, value, want, line)
				}
			}
			starts = append(starts, summaryField(t, line, "overlap-start"))
			ids = append(ids, summaryField(t, line, "conn"))
		}
		slices.Sort(starts)
		slices.Sort(ids)
		if !slices.Equal(starts, []string{"0", "1"}) || !slices.Equal(ids, []string{"41", "42"}) {
			t.Fatalf("overlap-start %v and conn %v, want [0 1] and [41 42]", starts, ids)
		}
	})
}
