package xdrive

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
)

type fakeFile struct {
	id          string
	name        string
	data        []byte
	description string
}

type fakeDrive struct {
	server *httptest.Server

	mu         sync.Mutex
	files      map[string]*fakeFile
	nextID     int
	failOnce   map[string]bool
	failStatus map[string]int
	failBody   map[string]string
	tokens     int
	hosts      map[string]bool
}

func newFakeDrive(t *testing.T) *fakeDrive {
	t.Helper()

	drive := &fakeDrive{
		files:      make(map[string]*fakeFile),
		failOnce:   make(map[string]bool),
		failStatus: make(map[string]int),
		failBody:   make(map[string]string),
		hosts:      make(map[string]bool),
	}
	mux := http.NewServeMux()
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	record := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			drive.mu.Lock()
			drive.hosts[r.Host] = true
			drive.mu.Unlock()
			next(w, r)
		}
	}
	mux.HandleFunc("/token", record(drive.handleToken))
	mux.HandleFunc("/upload", record(drive.handleUpload))
	mux.HandleFunc("/upload/", record(drive.handleUpload))
	mux.HandleFunc("/files", record(drive.handleFiles))
	mux.HandleFunc("/files/", record(drive.handleFile))
	drive.server = httptest.NewServer(mux)

	resetSharedStorage()

	t.Cleanup(func() {
		resetSharedStorage()
		drive.server.Close()
	})
	return drive
}

func (d *fakeDrive) handleToken(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	d.tokens++
	d.mu.Unlock()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"access_token": "fake-token",
		"expires_in":   3600,
	})
}

func (d *fakeDrive) handleUpload(w http.ResponseWriter, r *http.Request) {
	if d.shouldFail("upload") {
		if status, body := d.failure("upload"); status != 0 {
			w.WriteHeader(status)
			w.Write([]byte(body))
			return
		}
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	reader := multipart.NewReader(r.Body, params["boundary"])

	metaPart, err := reader.NextPart()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var metadata struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(metaPart).Decode(&metadata); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var data []byte
	if dataPart, err := reader.NextPart(); err == nil {
		data, _ = io.ReadAll(dataPart)
	}

	if id, ok := strings.CutPrefix(r.URL.Path, "/upload/"); ok {
		// an update of the body and description of a file
		d.mu.Lock()
		file := d.files[id]
		if file != nil {
			file.data, file.description = data, metadata.Description
		}
		d.mu.Unlock()
		if file == nil || r.Method != http.MethodPatch {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"id": id})
		return
	}

	d.mu.Lock()
	d.nextID++
	id := fmt.Sprintf("id-%d", d.nextID)
	d.files[id] = &fakeFile{id: id, name: metadata.Name, data: data}
	d.mu.Unlock()

	json.NewEncoder(w).Encode(map[string]string{"id": id, "name": metadata.Name})
}

func (d *fakeDrive) handleFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		d.handleCreate(w, r)
		return
	}
	d.handleList(w, r)
}

func (d *fakeDrive) handleCreate(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	d.mu.Unlock()
	if d.shouldFail("upload") {
		if status, body := d.failure("upload"); status != 0 {
			w.WriteHeader(status)
			w.Write([]byte(body))
			return
		}
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	var meta struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	d.mu.Lock()
	d.nextID++
	id := fmt.Sprintf("id-%d", d.nextID)
	d.files[id] = &fakeFile{id: id, name: meta.Name, description: meta.Description}
	d.mu.Unlock()

	json.NewEncoder(w).Encode(map[string]string{"id": id, "name": meta.Name})
}

func (d *fakeDrive) handleList(w http.ResponseWriter, r *http.Request) {
	if d.shouldFail("list") {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	query := r.URL.Query().Get("q")
	exact, prefix := parseFakeQuery(query)

	type entry struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		Description string `json:"description,omitempty"`
	}
	result := struct {
		Files []entry `json:"files"`
	}{}

	d.mu.Lock()
	for _, file := range d.files {
		match := (exact != "" && file.name == exact) || (prefix != "" && strings.HasPrefix(file.name, prefix))
		if match {
			result.Files = append(result.Files, entry{
				ID: file.id, Name: file.name, Description: file.description,
			})
		}
	}
	d.mu.Unlock()

	json.NewEncoder(w).Encode(result)
}

func (d *fakeDrive) handleFile(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/files/")

	d.mu.Lock()
	file, ok := d.files[id]
	if ok && r.Method == http.MethodDelete {
		delete(d.files, id)
	}
	var snapshot fakeFile
	if ok {
		// a file can be updated, its fields are read here
		snapshot = *file
	}
	d.mu.Unlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if r.Method == http.MethodDelete {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method == http.MethodPatch {
		var meta struct {
			Description string `json:"description"`
		}
		if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		d.mu.Lock()
		file.description = meta.Description
		d.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]string{"id": id})
		return
	}
	if strings.Contains(r.URL.RawQuery, "fields=description") {
		json.NewEncoder(w).Encode(map[string]string{"description": snapshot.description})
		return
	}
	w.Write(snapshot.data)
}

func (d *fakeDrive) shouldFail(kind string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failOnce[kind] {
		d.failOnce[kind] = false
		return true
	}
	return false
}

func (d *fakeDrive) failOnceWith(kind string, status int, body string) {
	d.mu.Lock()
	d.failOnce[kind] = true
	d.failStatus[kind] = status
	d.failBody[kind] = body
	d.mu.Unlock()
}

func (d *fakeDrive) failure(kind string) (int, string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if status, ok := d.failStatus[kind]; ok {
		return status, d.failBody[kind]
	}
	return 0, ""
}

func (d *fakeDrive) failNext(kind string) {
	d.mu.Lock()
	d.failOnce[kind] = true
	d.mu.Unlock()
}

func (d *fakeDrive) seenHosts() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, 0, len(d.hosts))
	for h := range d.hosts {
		out = append(out, h)
	}
	return out
}

func (d *fakeDrive) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.files)
}

func parseFakeQuery(query string) (exact, prefix string) {
	if value, ok := cutQuoted(query, "name = '"); ok {
		exact = value
	}
	if value, ok := cutQuoted(query, "name contains '"); ok {
		prefix = value
	}
	return exact, prefix
}

func cutQuoted(query, marker string) (string, bool) {
	start := strings.Index(query, marker)
	if start < 0 {
		return "", false
	}
	rest := query[start+len(marker):]
	end := strings.Index(rest, "'")
	if end < 0 {
		return "", false
	}
	return rest[:end], true
}

func driveSettings() *internet.MemoryStreamConfig {
	return &internet.MemoryStreamConfig{
		ProtocolName: protocolName,
		ProtocolSettings: &Config{
			RemoteFolder:      "folder-id",
			Service:           "google-drive",
			Secrets:           []string{"client", "secret", "refresh"},
			FlushIntervalMs:   5,
			PollIntervalMs:    5,
			MaxPollIntervalMs: 20,
			SessionTtlSeconds: 1,
		},
	}
}

func setDriveEndpoints(s *driveStorage, token, files, upload string) {
	s.tokenURL, s.filesURL, s.uploadURL = token, files, upload
}

// Configure the cached owner before Dial or Listener starts any shared work.
func prepareDrive(t *testing.T, settings *internet.MemoryStreamConfig, token, files, upload string) {
	t.Helper()
	s, err := newStorage(settings)
	if err != nil {
		t.Fatal(err)
	}
	setDriveEndpoints(s.(*driveStorage), token, files, upload)
}

func (d *fakeDrive) settings(t *testing.T) *internet.MemoryStreamConfig {
	t.Helper()
	s := driveSettings()
	prepareDrive(t, s, d.server.URL+"/token", d.server.URL+"/files", d.server.URL+"/upload")
	return s
}

func newDriveBackend(t *testing.T, drive *fakeDrive) *driveStorage {
	t.Helper()

	storage, err := newDriveStorage(driveSettings(), driveSettings().ProtocolSettings.(*Config))
	if err != nil {
		t.Fatalf("newDriveStorage: %v", err)
	}
	setDriveEndpoints(storage, drive.server.URL+"/token", drive.server.URL+"/files", drive.server.URL+"/upload")
	t.Cleanup(func() { storage.Close() })
	return storage
}

func TestDriveSecrets(t *testing.T) {
	if _, err := newDriveStorage(nil, &Config{RemoteFolder: "f", Secrets: []string{"a", "b"}}); err == nil {
		t.Fatal("accepted two secrets")
	}
	if _, err := newDriveStorage(nil, &Config{Secrets: []string{"a", "b", "c"}}); err == nil {
		t.Fatal("accepted an empty remoteFolder")
	}
	if _, err := newDriveStorage(nil, &Config{RemoteFolder: "f", Secrets: []string{"a", "", "c"}}); err == nil {
		t.Fatal("accepted an empty secret")
	}
}

func TestDriveRoundTrip(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t, drive)
	ctx := context.Background()

	if err := storage.Put(ctx, "streams/abc/c2s/000000000.seg", []byte("hello")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	data, err := storage.Get(ctx, "streams/abc/c2s/000000000.seg")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("Get returned %q, want %q", data, "hello")
	}

	names, err := storage.List(ctx, "streams/abc/c2s")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(names) != 1 || names[0].Name != "000000000.seg" {
		t.Fatalf("List returned %v, want 1 segment", names)
	}

	if _, err := storage.Get(ctx, "streams/abc/c2s/000000009.seg"); err != errNotFound {
		t.Fatalf("Get returned %v, want errNotFound", err)
	}

	if err := storage.Delete(ctx, "streams/abc/c2s/000000000.seg"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if drive.count() != 0 {
		t.Fatalf("fake drive still holds %d files", drive.count())
	}
}

func TestDriveListChildren(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t, drive)
	ctx := context.Background()

	for _, name := range []string{
		"streams/one/c2s/000000000.seg",
		"streams/one/s2c/000000000.seg",
		"streams/two/c2s/000000000.seg",
	} {
		if err := storage.Put(ctx, name, []byte("x")); err != nil {
			t.Fatalf("Put %s: %v", name, err)
		}
	}

	names, err := storage.List(ctx, "streams")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("List returned %v, want 2 sessions", names)
	}
}

func TestDriveDeleteSession(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t, drive)
	ctx := context.Background()

	for _, name := range []string{
		"streams/one/c2s/000000000.seg",
		"streams/one/c2s/000000001.end",
		"streams/one/s2c/000000000.seg",
		"streams/two/c2s/000000000.seg",
	} {
		if err := storage.Put(ctx, name, []byte("x")); err != nil {
			t.Fatalf("Put %s: %v", name, err)
		}
	}

	if err := storage.Delete(ctx, "streams/one"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if drive.count() != 1 {
		t.Fatalf("fake drive holds %d files, want 1", drive.count())
	}
}

func TestDriveRetry(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t, drive)
	ctx := context.Background()

	drive.failNext("upload")
	if err := storage.Put(ctx, "sessions/abc", nil); err != nil {
		t.Fatalf("Put did not survive a 429: %v", err)
	}

	drive.failNext("list")
	if _, err := storage.List(ctx, "sessions"); err != nil {
		t.Fatalf("List did not survive a 503: %v", err)
	}
}

func TestDriveTokenCache(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t, drive)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := storage.Put(ctx, fmt.Sprintf("sessions/s%d", i), nil); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if drive.tokens != 1 {
		t.Fatalf("token endpoint hit %d times, want 1", drive.tokens)
	}
}

func TestDriveTransport(t *testing.T) {
	drive := newFakeDrive(t)

	client, server, cleanup := pairWith(t, drive.settings(t))
	defer cleanup()

	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	expectRead(t, server, "ping")

	if _, err := server.Write([]byte("pong")); err != nil {
		t.Fatalf("server write: %v", err)
	}
	expectRead(t, client, "pong")
}

func TestDriveLargeTransfer(t *testing.T) {
	drive := newFakeDrive(t)

	client, server, cleanup := pairWith(t, drive.settings(t))
	defer cleanup()

	payload := make([]byte, 300000)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	go func() {
		client.Write(payload)
	}()

	if err := server.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	for i := range got {
		if got[i] != payload[i] {
			t.Fatalf("payload mismatch at byte %d", i)
		}
	}
}

func TestRateLimited(t *testing.T) {
	limited := []string{
		`{"error":{"code":403,"errors":[{"reason":"userRateLimitExceeded"}]}}`,
		`{"error":{"code":403,"errors":[{"reason":"rateLimitExceeded"}]}}`,
		`{"error":{"code":403,"errors":[{"reason":"sharingRateLimitExceeded"}]}}`,
		`{"error":{"status":"RESOURCE_EXHAUSTED"}}`,
	}
	for _, payload := range limited {
		if !rateLimited([]byte(payload)) {
			t.Fatalf("rateLimited missed %s", payload)
		}
	}

	permanent := []string{
		`{"error":{"code":403,"errors":[{"reason":"insufficientFilePermissions"}]}}`,
		`{"error":{"code":403,"errors":[{"reason":"storageQuotaExceeded"}]}}`,
		`not json at all`,
	}
	for _, payload := range permanent {
		if rateLimited([]byte(payload)) {
			t.Fatalf("rateLimited treated %s as temporary", payload)
		}
	}
}

func TestDriveRetryRateLimit(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t, drive)

	drive.failOnceWith("upload", http.StatusForbidden,
		`{"error":{"code":403,"errors":[{"reason":"userRateLimitExceeded"}]}}`)

	if err := storage.Put(context.Background(), "sessions/abc", nil); err != nil {
		t.Fatalf("Put did not survive a 403: %v", err)
	}
}

func TestDriveSharedClient(t *testing.T) {
	drive := newFakeDrive(t)
	settings := drive.settings(t)

	first, err := newStorage(settings)
	if err != nil {
		t.Fatalf("newStorage: %v", err)
	}
	second, err := newStorage(settings)
	if err != nil {
		t.Fatalf("newStorage: %v", err)
	}
	if first != second {
		t.Fatal("same settings did not share one storage")
	}
	// a session has an inflight of its own, as large
	session := sessionStorage(first).(*driveStorage)
	defer session.Close()
	if session.inflight == first.(*driveStorage).inflight || cap(session.inflight) != driveMaxInflight {
		t.Fatalf("a session shares inflight or has cap %d", cap(session.inflight))
	}
}

func TestDriveOldServiceName(t *testing.T) {
	drive := newFakeDrive(t)

	settings := driveSettings()
	settings.ProtocolSettings.(*Config).Service = "Google Drive"
	prepareDrive(t, settings, drive.server.URL+"/token", drive.server.URL+"/files", drive.server.URL+"/upload")
	if _, err := newStorage(settings); err != nil {
		t.Fatalf("newStorage: %v", err)
	}
}

func TestDriveInlineListing(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t, drive)
	ctx := context.Background()

	payload := []byte("small enough to ride along with the listing")
	if err := storage.Put(ctx, "streams/abc/c2s/000000000.seg", payload); err != nil {
		t.Fatalf("Put: %v", err)
	}

	entries, err := storage.List(ctx, "streams/abc/c2s")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("List returned %v, want 1 entry", entries)
	}
	if string(entries[0].Inline) != string(payload) {
		t.Fatalf("listing carried %q, want %q", entries[0].Inline, payload)
	}

	d := drive
	d.mu.Lock()
	var stored *fakeFile
	for _, f := range d.files {
		stored = f
	}
	d.mu.Unlock()
	if len(stored.data) != 0 {
		t.Fatal("small payload was uploaded as content")
	}
}

func TestDriveLargeContent(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t, drive)
	ctx := context.Background()

	payload := make([]byte, driveInlineLimit+1)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := storage.Put(ctx, "streams/abc/c2s/000000000.seg", payload); err != nil {
		t.Fatalf("Put: %v", err)
	}

	entries, err := storage.List(ctx, "streams/abc/c2s")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 || entries[0].Inline != nil {
		t.Fatalf("large payload was inlined, got %v", entries)
	}

	got, err := storage.Get(ctx, "streams/abc/c2s/000000000.seg")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("Get returned %d bytes, want %d", len(got), len(payload))
	}
	_ = drive
}

func TestDriveInlineGet(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t, drive)
	ctx := context.Background()

	payload := []byte("only in the description")
	if err := storage.Put(ctx, "sessions/abc", payload); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := storage.Get(ctx, "sessions/abc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("Get returned %q, want %q", got, payload)
	}
}

func TestDriveFronting(t *testing.T) {
	drive := newFakeDrive(t)

	fake, err := url.Parse(drive.server.URL)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	port, err := net.PortFromString(fake.Port())
	if err != nil {
		t.Fatalf("port: %v", err)
	}

	settings := driveSettings()
	settings.Destination = &net.Destination{
		Address: net.ParseAddress(fake.Hostname()),
		Port:    port,
		Network: net.Network_TCP,
	}

	storage, err := newDriveStorage(settings, settings.ProtocolSettings.(*Config))
	if err != nil {
		t.Fatalf("newDriveStorage: %v", err)
	}
	defer storage.Close()
	setDriveEndpoints(storage, "http://www.googleapis.com/token", "http://www.googleapis.com/files", "http://www.googleapis.com/upload")

	if err := storage.Put(context.Background(), "sessions/fronted", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if drive.count() != 1 {
		t.Fatalf("fake drive holds %d files, want 1", drive.count())
	}

	for _, host := range drive.seenHosts() {
		if host != "www.googleapis.com" {
			t.Fatalf("inner host was %q, want www.googleapis.com regardless of address", host)
		}
	}
}

func driveClock(t *testing.T, test func(*testing.T)) {
	t.Helper()
	log.Record(&log.GeneralMessage{Severity: log.Severity_Debug, Content: "xdrive local fake clock"})
	synctest.Test(t, test)
}

func memoryResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func memoryDrive(rt http.RoundTripper) *driveStorage {
	client := &http.Client{Transport: rt}
	return &driveStorage{
		driveShared: &driveShared{
			token: "fake", tokenExpiry: time.Now().Add(time.Hour),
			tokenURL: "http://drive.invalid/token", tokenClient: client,
			filesURL: "http://drive.invalid/files", uploadURL: "http://drive.invalid/upload",
			folder: "fake-folder", backoff: time.Millisecond, ids: make(map[string]string),
		},
		client: client, inflight: make(chan struct{}, 64), reserved: make(chan struct{}, 8),
	}
}

func generatedResponse(r *http.Request) *http.Response {
	n, _ := strconv.Atoi(r.URL.Query().Get("count"))
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("generated-%d", i)
	}
	body, _ := json.Marshal(map[string]any{"ids": ids})
	return memoryResponse(http.StatusOK, string(body))
}

func TestDriveIDWaiterSurvivesInitiatorCancellation(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		started := make(chan struct{})
		var requests atomic.Int32
		s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			if requests.Add(1) == 1 {
				close(started)
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
			return generatedResponse(r), nil
		}))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		leader := make(chan error, 1)
		go func() { _, err := s.takeIDs(ctx, 2, 1); leader <- err }()
		<-started
		waiter := make(chan error, 1)
		go func() {
			ids, err := s.takeIDs(context.Background(), 2, 1)
			if err == nil && len(ids) != 2 {
				err = fmt.Errorf("got %d IDs, want 2", len(ids))
			}
			waiter <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-leader; err != context.Canceled {
			t.Fatalf("initiator: %v", err)
		}
		if err := <-waiter; err != nil {
			t.Fatalf("uncancelled waiter: %v", err)
		}
	})
}

type driveTimeoutBody struct{ ctx context.Context }

func (b driveTimeoutBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}
func (driveTimeoutBody) Close() error { return nil }

func TestDriveBodyStallRetiresBulkOnly(t *testing.T) {
	for _, mode := range []string{"attempt", "body", "headers", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				var primaryConnections, bulkConnections atomic.Int32
				primary := &sessionTransport{pool: newConnPool(nil, func(context.Context) *http.Client {
					primaryConnections.Add(1)
					return &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) {
						return memoryResponse(http.StatusOK, "control"), nil
					})}
				})}
				bulk := &sessionTransport{pool: newConnPool(nil, func(context.Context) *http.Client {
					first := bulkConnections.Add(1) == 1
					next := roundTrip(func(r *http.Request) (*http.Response, error) {
						resp := memoryResponse(http.StatusOK, "media")
						if first {
							if mode == "headers" {
								<-r.Context().Done()
								return nil, r.Context().Err()
							}
							resp.Body = driveTimeoutBody{r.Context()}
						}
						return resp, nil
					})
					if mode == "attempt" {
						return &http.Client{Transport: next}
					}
					return &http.Client{Transport: &serviceTransport{plain: next, stall: driveTimeout}}
				})}
				defer primary.Close()
				defer bulk.Close()
				s := memoryDrive(primary)
				s.bulkClient = &http.Client{Transport: bulk}
				get := func(ctx context.Context, want string) {
					t.Helper()
					status, body, err := s.do(ctx, http.MethodGet, s.filesURL, "", nil)
					if err != nil || status != http.StatusOK || string(body) != want {
						t.Fatalf("GET %s: status=%d body=%q err=%v", want, status, body, err)
					}
				}
				get(context.Background(), "control")
				ctx := context.WithValue(context.Background(), bodyKey{}, true)
				if mode == "attempt" {
					ctx = context.WithValue(ctx, attemptKey{}, time.Second)
				}
				if mode == "cancelled" {
					ctx, cancel := context.WithCancel(ctx)
					cancel()
					if _, _, err := s.do(ctx, http.MethodGet, s.filesURL, "", nil); err != context.Canceled || s.throttled() != 0 {
						t.Fatalf("caller cancellation: err=%v throttles=%d", err, s.throttled())
					}
					return
				}
				// Two overlapping media stalls must retire the bulk connection; a
				// successful header response must not hide a later body stall.
				done := make(chan struct{}, 2)
				for range 2 {
					go func() {
						defer func() { done <- struct{}{} }()
						get(ctx, "media")
					}()
				}
				<-done
				<-done
				get(context.Background(), "control")
				if primaryConnections.Load() != 1 || bulkConnections.Load() != 2 || s.throttled() != 2 {
					t.Fatalf("opened connections: control=%d bulk=%d throttles=%d", primaryConnections.Load(), bulkConnections.Load(), s.throttled())
				}
			})
		})
	}
}

func TestDriveBodyHedgeWaitsForStall(t *testing.T) {
	for _, mode := range []string{"moving", "body-stall", "header-stall"} {
		t.Run(mode, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				var calls atomic.Int32
				s := memoryDrive(roundTrip(func(req *http.Request) (*http.Response, error) {
					if calls.Add(1) > 1 {
						return memoryResponse(http.StatusOK, "xxxxxx"), nil
					}
					if mode == "header-stall" {
						<-req.Context().Done()
						return nil, req.Context().Err()
					}
					reader, writer := io.Pipe()
					stop := context.AfterFunc(req.Context(), func() { reader.CloseWithError(req.Context().Err()) })
					go func() {
						defer stop()
						defer writer.Close()
						for i := range 6 {
							if mode == "body-stall" && i == 3 {
								<-req.Context().Done()
								return
							}
							<-time.NewTimer(slotBodyHedge / 2).C
							if _, err := io.WriteString(writer, "x"); err != nil {
								return
							}
						}
					}()
					resp := memoryResponse(http.StatusOK, "")
					resp.Body = reader
					return resp, nil
				}))
				r := &slotReader{store: s}
				start := time.Now()
				body, err := r.readBodyHedged(context.Background(), "body", 6)
				synctest.Wait()
				wantCalls, wantTime := int32(2), slotBodyHedge
				if mode == "moving" {
					wantCalls, wantTime = 1, 3*slotBodyHedge
				} else if mode == "body-stall" {
					wantTime = 5 * slotBodyHedge / 2
				}
				if err != nil || string(body) != "xxxxxx" || calls.Load() != wantCalls || time.Since(start) != wantTime {
					t.Fatalf("body=%q err=%v calls=%d elapsed=%v", body, err, calls.Load(), time.Since(start))
				}
			})
		})
	}
}

func TestDriveHedgesRecheckRateLimit(t *testing.T) {
	for _, body := range []bool{false, true} {
		for _, state := range []string{"healthy", "cooling", "recent"} {
			t.Run(fmt.Sprintf("body=%v/%s", body, state), func(t *testing.T) {
				driveClock(t, func(t *testing.T) {
					release := make(chan struct{})
					var calls atomic.Int32
					s := memoryDrive(roundTrip(func(req *http.Request) (*http.Response, error) {
						calls.Add(1)
						select {
						case <-release:
							return memoryResponse(http.StatusOK, "ok"), nil
						case <-req.Context().Done():
							return nil, req.Context().Err()
						}
					}))
					s.bulkClient = s.client
					s.readHedge, s.readHedgeTokens = slotReadHedge, 3
					done := make(chan error, 1)
					go func() {
						var err error
						if body {
							r := &slotReader{store: s}
							_, err = r.readBodyHedged(context.Background(), "body", 2)
						} else {
							_, _, err = s.readDescHedged(context.Background(), s.filesURL)
						}
						done <- err
					}()
					synctest.Wait()
					if calls.Load() != 1 {
						t.Fatal("primary request did not start")
					}
					if state != "healthy" {
						s.cool()
						if state == "recent" {
							s.coolMu.Lock()
							s.coolUntil = time.Now()
							s.coolMu.Unlock()
						}
					}
					<-time.NewTimer(slotBodyHedge).C
					synctest.Wait()
					want := int32(1)
					if state == "healthy" {
						want = 2
					}
					if calls.Load() != want || state != "healthy" && s.readHedgeTokens != 3 {
						t.Fatalf("requests=%d want=%d tokens=%d", calls.Load(), want, s.readHedgeTokens)
					}
					close(release)
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				})
			})
		}
	}
}

func TestDriveAuthErrorIsSticky(t *testing.T) {
	var requests atomic.Int32
	s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return memoryResponse(http.StatusBadRequest, `{"error":"invalid_grant"}`), nil
	}))
	s.token = ""
	for range 3 {
		if _, _, err := s.do(context.Background(), http.MethodGet, s.filesURL, "", nil); !isAuthError(err) {
			t.Fatalf("sticky authentication failure: %v", err)
		}
	}
	if requests.Load() != 1 {
		t.Fatal("rejected authentication kept retrying")
	}
}

func TestDriveClosedSessionFailsFast(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		transport := &sessionTransport{pool: newConnPool(nil, nil)}
		transport.Close()
		start := time.Now()
		if _, _, err := memoryDrive(transport).do(context.Background(), http.MethodGet, "http://drive.invalid/files", "", nil); err == nil || time.Since(start) != 0 {
			t.Fatalf("closed session: %v after %v of retries", err, time.Since(start))
		}
	})
}

func TestDriveIDPoolTopUp(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var counts []int
		rt := roundTrip(func(r *http.Request) (*http.Response, error) {
			n, _ := strconv.Atoi(r.URL.Query().Get("count"))
			counts = append(counts, n)
			if len(counts) == 2 {
				close(entered)
				<-release
			}
			return generatedResponse(r), nil
		})
		s := memoryDrive(rt)
		s.owner = s
		s.conns = newConnPool(nil, func(context.Context) *http.Client { return &http.Client{Transport: rt} })
		s.bulkConns = s.conns
		s.idPool = []idPoolEntry{{id: "seed-a", at: time.Now()}, {id: "seed-b", at: time.Now()}}
		defer s.Close()
		session := s.session().(*driveStorage)
		ids, err := session.takeIDs(context.Background(), 6, 3)
		if err != nil || len(ids) != 6 {
			t.Fatalf("allocation: %d IDs, %v", len(ids), err)
		}
		<-entered
		session.Close() // Closing the initiating session must not cancel shared work.
		close(release)
		synctest.Wait()
		if len(counts) != 2 || counts[0] != 4 || len(s.idPool) != 18 {
			t.Fatalf("top-up: requests=%v reserve=%d", counts, len(s.idPool))
		}
	})
}

func TestKeeperPrunesAndStopsIdle(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) { return generatedResponse(r), nil }))
		s.owner = s
		s.poolRing = 6
		s.idPool = []idPoolEntry{{id: "expired", at: time.Now().Add(-idPoolTTL)}}
		s.keepWarmWindow = 10 * time.Minute
		start := time.Now()
		s.lastTraffic, s.lastRequest = start, start.Add(-3*time.Minute)
		s.startKeeper()
		defer s.Close()
		<-time.NewTimer(s.keepWarmEvery).C
		synctest.Wait()
		s.poolMu.Lock()
		reserve := len(s.idPool)
		expired := reserve > 0 && s.idPool[0].id == "expired"
		s.poolMu.Unlock()
		if reserve != 18 || expired {
			t.Fatalf("keeper retained expired IDs or failed to refill: reserve=%d", reserve)
		}
		<-time.NewTimer(20 * time.Minute).C
		synctest.Wait()
		select {
		case <-s.keeperDone:
		default:
			t.Error("maintenance extended the idle keeper's lifetime")
		}
		s.trafficMu.Lock()
		lastTraffic := s.lastTraffic
		s.trafficMu.Unlock()
		if !lastTraffic.Equal(start) {
			t.Error("maintenance counted as user traffic")
		}
	})
}

func TestDriveRefreshInitiatorCancellation(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var requests atomic.Int32
		s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			close(entered)
			<-release
			return memoryResponse(http.StatusOK, `{"access_token":"refreshed","expires_in":3600}`), nil
		}))
		s.token = ""
		ctx, cancel := context.WithCancel(context.Background())
		leader, waiter := make(chan error, 1), make(chan string, 1)
		go func() { _, err := s.accessToken(ctx); leader <- err }()
		<-entered
		go func() { token, _ := s.accessToken(context.Background()); waiter <- token }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case err := <-leader:
			if err != context.Canceled {
				t.Error(err)
			}
		default:
			t.Error("token leader ignored its cancellation")
		}
		close(release)
		if token := <-waiter; token != "refreshed" || requests.Load() != 1 {
			t.Fatal("canceling the leader lost or duplicated the shared refresh")
		}
	})
}

func TestDriveDeleteRefreshesFailedDuplicateSet(t *testing.T) {
	var mu sync.Mutex
	left := map[string]string{"a1": "sessions~a", "a2": "sessions~a", "c1": "sessions~a~c"}
	failed := false
	s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodDelete {
			id := strings.TrimPrefix(r.URL.Path, "/files/")
			if id == "a2" && !failed {
				failed = true
				return memoryResponse(http.StatusBadRequest, "failed"), nil
			}
			delete(left, id)
			return memoryResponse(http.StatusNoContent, ""), nil
		}
		var files []map[string]string
		for id, name := range left {
			files = append(files, map[string]string{"id": id, "name": name})
		}
		body, _ := json.Marshal(map[string]any{"files": files})
		return memoryResponse(http.StatusOK, string(body)), nil
	}))
	s.ids[flatten("sessions/a")] = "a1"
	if err := s.Delete(context.Background(), "sessions/a"); err == nil {
		t.Fatal("failed duplicate deletion was not reported")
	}
	if err := s.Delete(context.Background(), "sessions/a"); err != nil || len(left) != 0 {
		t.Fatalf("duplicate cleanup retry: %v; %d objects left", err, len(left))
	}
}

func TestKeeperWarmsSessionPools(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		var made, requests [3]int
		client := func(lane int) *http.Client {
			made[lane]++
			return &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) {
				requests[lane]++
				return memoryResponse(http.StatusOK, `{"id":"folder"}`), nil
			})}
		}
		s := memoryDrive(client(0).Transport)
		s.owner = s
		s.conns = newConnPool(nil, func(context.Context) *http.Client { return client(1) })
		s.bulkConns = newConnPool(nil, func(context.Context) *http.Client { return client(2) })
		s.poolRing = 6
		for i := range 18 {
			s.idPool = append(s.idPool, idPoolEntry{id: fmt.Sprint(i), at: time.Now()})
		}
		start := time.Now()
		s.lastTraffic, s.lastRequest = start, start
		s.startKeeper()
		defer s.Close()
		synctest.Wait()
		<-time.NewTimer(s.keepWarmEvery).C
		synctest.Wait()
		if requests != [3]int{0, 1, 1} || !s.lastTraffic.Equal(start) {
			t.Fatalf("keeper requests=%v; maintenance counted as traffic=%v", requests, !s.lastTraffic.Equal(start))
		}
		session := s.session().(*driveStorage)
		defer session.Close()
		for _, ctx := range []context.Context{context.Background(), context.WithValue(context.Background(), bodyKey{}, true)} {
			if _, _, err := session.do(ctx, http.MethodGet, s.filesURL, "", nil); err != nil {
				t.Fatal(err)
			}
		}
		if made != [3]int{1, 1, 1} || requests != [3]int{0, 2, 2} {
			t.Fatalf("session did not reuse both warmed pools: connections=%v requests=%v", made, requests)
		}
	})
}
