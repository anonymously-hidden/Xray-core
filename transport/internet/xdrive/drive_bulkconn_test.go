package xdrive

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBulkBodiesUseSeparateConnection(t *testing.T) {
	var mu sync.Mutex
	descAddrs := map[string]bool{}
	mediaAddrs := map[string]bool{}
	inlineAddrs := map[string]bool{}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		addr := r.RemoteAddr
		switch {
		case strings.Contains(r.URL.RawQuery, "alt=media"):
			mediaAddrs[addr] = true
		case strings.Contains(r.URL.RawQuery, "fields=description"):
			descAddrs[addr] = true
		case r.Method == http.MethodPatch:
			if strings.Contains(r.Header.Get("Content-Type"), "multipart") {
				mediaAddrs[addr] = true
			} else {
				inlineAddrs[addr] = true
			}
		}
		mu.Unlock()
		if strings.Contains(r.URL.RawQuery, "alt=media") {
			w.Write([]byte("body-bytes"))
			return
		}
		w.Write([]byte(`{"id":"x","description":""}`))
	}))
	defer srv.Close()

	mkClient := func() *http.Client {
		return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	}
	normal, bulk := mkClient(), mkClient()
	s := &driveStorage{
		driveShared: &driveShared{token: "t", tokenExpiry: time.Now().Add(time.Hour), filesURL: srv.URL, uploadURL: srv.URL + "/upload", ids: make(map[string]string)},
		client:      normal,
		bulkClient:  bulk,
		inflight:    make(chan struct{}, 8),
		reserved:    make(chan struct{}, 8),
	}
	ctx := context.Background()
	if _, err := s.readDescription(ctx, "id"); err != nil {
		t.Fatal(err)
	}
	if err := s.setSlot(ctx, "id", "desc", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.readBody(ctx, "id"); err != nil {
		t.Fatal(err)
	}
	if err := s.setSlot(ctx, "id", "desc", []byte("media")); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(descAddrs) != 1 || len(inlineAddrs) != 1 {
		t.Fatalf("small requests used %d/%d connections, want 1 each", len(descAddrs), len(inlineAddrs))
	}
	if len(mediaAddrs) != 1 {
		t.Fatalf("media used %d connections, want 1", len(mediaAddrs))
	}
	for a := range mediaAddrs {
		if descAddrs[a] || inlineAddrs[a] {
			t.Fatal("media shared a connection with small requests")
		}
	}
}

func TestSessionBulkLazyAndClose(t *testing.T) {
	var mu sync.Mutex
	addrs := map[string]bool{}
	mediaSeen := false
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		addrs[r.RemoteAddr] = true
		if strings.Contains(r.URL.RawQuery, "alt=media") {
			mediaSeen = true
		}
		mu.Unlock()
		if strings.Contains(r.URL.RawQuery, "alt=media") {
			w.Write([]byte("body"))
			return
		}
		w.Write([]byte(`{"id":"x","description":""}`))
	}))
	defer srv.Close()
	mkClient := func() *http.Client {
		return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	}
	normal, bulk := mkClient(), mkClient()
	root := &driveStorage{driveShared: &driveShared{
		token: "t", tokenExpiry: time.Now().Add(time.Hour),
		filesURL: srv.URL, uploadURL: srv.URL + "/upload",
		conns:     newConnPool(nil, func(context.Context) *http.Client { return normal }),
		bulkConns: newConnPool(nil, func(context.Context) *http.Client { return bulk }),
	}}
	sess := root.session().(*driveStorage)
	if _, err := sess.readDescription(context.Background(), "id"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	n, media := len(addrs), mediaSeen
	mu.Unlock()
	if n != 1 || media {
		t.Fatalf("a body-less session opened %d connections (media=%v), want 1", n, media)
	}
	if _, err := sess.readBody(context.Background(), "id"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	n = len(addrs)
	mu.Unlock()
	if n != 2 {
		t.Fatalf("after a body the session used %d connections, want 2", n)
	}
	sess.Close()
	if !sess.client.Transport.(*sessionTransport).closed || !sess.bulkClient.Transport.(*sessionTransport).closed {
		t.Fatal("session close did not close both transports")
	}
}

func TestLazyCreateMediaUsesBulk(t *testing.T) {
	var mu sync.Mutex
	addrs := map[string]string{}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		switch {
		case strings.Contains(r.Header.Get("Content-Type"), "multipart"):
			addrs["media"] = r.RemoteAddr
		case strings.Contains(r.URL.RawQuery, "alt=media"):
			addrs["body"] = r.RemoteAddr
		case strings.Contains(r.URL.RawQuery, "fields=description"):
			addrs["desc"] = r.RemoteAddr
		default:
			addrs["inline"] = r.RemoteAddr
		}
		mu.Unlock()
		if strings.Contains(r.URL.RawQuery, "alt=media") {
			w.Write([]byte("body"))
			return
		}
		w.Write([]byte(`{"id":"x","description":""}`))
	}))
	defer srv.Close()
	mk := func() *http.Client {
		return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	}
	s := &driveStorage{
		driveShared: &driveShared{token: "t", tokenExpiry: time.Now().Add(time.Hour), filesURL: srv.URL, uploadURL: srv.URL + "/upload", ids: make(map[string]string)},
		client:      mk(),
		bulkClient:  mk(),
		inflight:    make(chan struct{}, 8),
		reserved:    make(chan struct{}, 8),
	}
	ctx := context.Background()
	if err := s.createSlot(ctx, "id", "name", "desc", []byte("media")); err != nil {
		t.Fatal(err)
	}
	if err := s.createSlot(ctx, "id2", "name2", "desc", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.readBody(ctx, "id"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.readDescription(ctx, "id2"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if addrs["media"] == "" || addrs["inline"] == "" || addrs["media"] == addrs["inline"] {
		t.Fatalf("lazy media create and inline create shared a connection: %v", addrs)
	}
	if addrs["media"] != addrs["body"] {
		t.Fatal("lazy media create did not use the bulk connection")
	}
	if addrs["inline"] != addrs["desc"] {
		t.Fatal("lazy inline create did not use the lane connection")
	}
}

func TestMixedInlineMediaByteExact(t *testing.T) {
	store := newMemSlots()
	client, server := slotPair(t, store, 4, 64<<10)
	payload := randomPayload(200 << 10)
	errs := make(chan error, 1)
	go func() {
		_, err := client.Write(payload)
		if err == nil {
			err = client.Close()
		}
		errs <- err
	}()
	if err := server.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(server)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatal("mixed inline/media stream not byte-exact")
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}
