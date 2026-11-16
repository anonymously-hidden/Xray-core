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

// Configure owners before Dial or Listener starts any shared work.
func prepareDrive(t *testing.T, settings *internet.MemoryStreamConfig, token, files, upload string) {
	t.Helper()
	if settings.FinalMask != nil {
		factory := func(config *Config) (Storage, error) {
			s, err := newDriveStorage(settings, config)
			if err != nil {
				return nil, err
			}
			setDriveEndpoints(s, token, files, upload)
			return s, nil
		}
		old := testStorageFactory.Swap(&factory)
		t.Cleanup(func() { testStorageFactory.Store(old) })
		return
	}
	s, err := newStorage(settings)
	if err != nil {
		t.Fatal(err)
	}
	setDriveEndpoints(s.(*leasedDriveStorage).driveStorage, token, files, upload)
	t.Cleanup(func() { s.Close() })
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
	defer first.Close()
	defer second.Close()
	if first == second || first.(*leasedDriveStorage).lease.owner != second.(*leasedDriveStorage).lease.owner {
		t.Fatal("same settings must share an owner, not an acquisition lease")
	}
	// a session has an inflight of its own, as large
	session := sessionStorage(first).(*leasedDriveStorage)
	defer session.Close()
	if session.inflight == first.(*leasedDriveStorage).inflight || cap(session.inflight) != driveMaxInflight {
		t.Fatalf("a session shares inflight or has cap %d", cap(session.inflight))
	}
	first.Close()
	if session.isWorkClosed() || second.(*leasedDriveStorage).isWorkClosed() {
		t.Fatal("closing one acquisition terminated its sibling or child")
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

func TestDriveReservedTokenRetryFailsFast(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) { calls++; return nil, fmt.Errorf("unexpected OAuth call") }))
		s.token = ""
		s.lastTokenErr = fmt.Errorf("token unavailable")
		s.tokenRetryAt = time.Now().Add(time.Hour)
		started := time.Now()
		if _, err := s.accessToken(context.WithValue(context.Background(), laneKey{}, true)); err != s.lastTokenErr || time.Now() != started || calls != 0 {
			t.Fatalf("reserved request waited or replaced cached error: %v", err)
		}
	})
}

func TestDriveQuotaRetryBudget(t *testing.T) {
	for _, tc := range []struct {
		elapsed      time.Duration
		refunded     int
		refund, stop bool
	}{
		{driveRateLimitBudget - time.Nanosecond, 0, true, false},
		{driveRateLimitBudget, 0, false, false},
		{driveRateLimitBudget, 1, false, true},
	} {
		if refund, stop := driveQuotaRetry(tc.elapsed, tc.refunded); refund != tc.refund || stop != tc.stop {
			t.Fatalf("quota policy at %v: refund=%v stop=%v", tc.elapsed, refund, stop)
		}
	}
	for _, status := range []int{http.StatusForbidden, http.StatusTooManyRequests} {
		s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
			return memoryResponse(status, `{"error":{"errors":[{"reason":"rateLimitExceeded"}]}}`), nil
		}))
		if _, _, err := s.do(context.WithValue(context.Background(), cleanupKey{}, true), http.MethodDelete, "http://drive.invalid/files/item", "", nil); err == nil || s.cooling() {
			t.Fatalf("cleanup quota response %d cooled or succeeded", status)
		}
	}
}

func TestDriveMediaHeadOnlySplitsRanges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		body := strings.Repeat("x", 2<<20)
		frame := newSlotFrame(1, slotMedia, []byte(body))
		for _, tc := range []struct {
			head, cool bool
			calls      int
		}{{false, false, 1}, {true, false, 4}, {true, true, 1}} {
			var calls atomic.Int32
			rt := roundTrip(func(r *http.Request) (*http.Response, error) {
				calls.Add(1)
				var start, end int
				if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
					return nil, err
				}
				resp := memoryResponse(http.StatusPartialContent, body[start:end+1])
				resp.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
				return resp, nil
			})
			s := memoryDrive(rt)
			s.mediaClients = []*http.Client{{Transport: rt}, {Transport: rt}, {Transport: rt}, {Transport: rt}}
			if tc.cool {
				s.cool()
			}
			ctx := context.Background()
			if tc.head {
				ctx = context.WithValue(ctx, mediaHeadKey{}, true)
			}
			got, err := s.readFrameBody(ctx, "item", frame)
			if err != nil || string(got) != body || int(calls.Load()) != tc.calls {
				t.Fatalf("head=%v cool=%v: ranges=%d err=%v", tc.head, tc.cool, calls.Load(), err)
			}
		}
	})
}
