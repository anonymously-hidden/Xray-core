package xdrive

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	stdnet "net"
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
	"github.com/xtls/xray-core/transport/internet/splithttp"
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

type failingDriveBody struct{}

func (failingDriveBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (failingDriveBody) Close() error             { return nil }

func TestDriveUnkeyedCreateRetriesBeforeDispatch(t *testing.T) {
	for _, media := range []bool{false, true} {
		driveClock(t, func(t *testing.T) {
			var dials atomic.Int32
			served := make(chan error, 1)
			transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
				if dials.Add(1) == 1 {
					return nil, io.ErrUnexpectedEOF
				}
				client, server := stdnet.Pipe()
				go func() {
					defer server.Close()
					req, err := http.ReadRequest(bufio.NewReader(server))
					if err == nil {
						_, err = io.Copy(io.Discard, req.Body)
						req.Body.Close()
					}
					if err == nil {
						_, err = io.WriteString(server, "HTTP/1.1 200 OK\r\nContent-Length: 16\r\nConnection: close\r\n\r\n{\"id\":\"created\"}")
					}
					served <- err
				}()
				return client, nil
			}}
			defer transport.CloseIdleConnections()
			s := memoryDrive(transport)
			data := []byte("inline")
			if media {
				data = make([]byte, driveInlineLimit+1)
			}
			if err := s.Put(context.Background(), "sessions/new", data); err != nil {
				t.Fatalf("pre-dispatch failure aborted creation: %v", err)
			}
			if err := <-served; err != nil || dials.Load() != 2 || len(s.inflight)+len(s.reserved) != 0 {
				t.Fatalf("creation dispatch or permit accounting changed: %v", err)
			}
		})
	}
}

func TestDriveUnkeyedCreateDoesNotReplayAmbiguity(t *testing.T) {
	for _, media := range []bool{false, true} {
		for _, outcome := range []string{"transport", "body", "server", "invalid", "missing", "replay", "quota"} {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
					calls++
					if r.GetBody != nil {
						t.Fatal("unkeyed creation retained a replayable body")
					}
					switch outcome {
					case "transport":
						return nil, io.ErrUnexpectedEOF
					case "body":
						resp := memoryResponse(http.StatusOK, "")
						resp.Body = failingDriveBody{}
						return resp, nil
					case "server":
						return memoryResponse(http.StatusServiceUnavailable, ""), nil
					case "invalid":
						return memoryResponse(http.StatusOK, "{"), nil
					case "missing":
						return memoryResponse(http.StatusOK, `{}`), nil
					case "replay":
						admit := r.Context().Value(dispatchKey{}).(func(context.Context) error)
						return nil, admit(r.Context())
					case "quota":
						if calls == 1 {
							resp := memoryResponse(http.StatusTooManyRequests, "")
							resp.Body = failingDriveBody{}
							return resp, nil
						}
					}
					return memoryResponse(http.StatusOK, `{"id":"created"}`), nil
				}))
				data := []byte("inline")
				if media {
					data = make([]byte, driveInlineLimit+1)
				}
				err := s.Put(context.Background(), "sessions/new", data)
				if outcome == "quota" {
					if err != nil || calls != 2 {
						t.Fatalf("known rejection was not retried: %v", err)
					}
				} else {
					var ambiguity createAmbiguityError
					if !errors.As(err, &ambiguity) || calls != 1 {
						t.Fatalf("ambiguous %s creation replayed or lost classification: %v", outcome, err)
					}
				}
				if len(s.inflight)+len(s.reserved) != 0 {
					t.Fatal("creation leaked admission permits")
				}
			})
		}
	}
}

func TestDriveGeneratedIDsRejectInvalidRingIdentities(t *testing.T) {
	for _, tc := range []struct {
		name string
		ids  []string
	}{
		{"missing", nil},
		{"short", []string{"one"}},
		{"extra", []string{"one", "two", "three"}},
		{"empty", []string{"one", ""}},
		{"duplicate", []string{"same", "same"}},
		{"oversized", []string{"one", strings.Repeat("x", maxSlotIDLen+1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := json.Marshal(map[string]any{"ids": tc.ids})
			if err != nil {
				t.Fatal(err)
			}
			s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/files/generateIds" || r.URL.Query().Get("count") != "2" {
					t.Fatal("unexpected ID allocation request")
				}
				return memoryResponse(http.StatusOK, string(payload)), nil
			}))
			defer s.Close()
			if ids, err := s.generateIDs(context.Background(), 2); err == nil || ids != nil {
				t.Fatalf("invalid generated IDs accepted: %q, %v", ids, err)
			}
			if len(s.inflight)+len(s.reserved) != 0 {
				t.Fatal("invalid allocation leaked permits")
			}
		})
	}
}

func TestDriveInvalidIDSurplusDoesNotPoisonPool(t *testing.T) {
	s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
		// The pool keeps one full generateIds batch, at least 1000 ids.
		if r.URL.Query().Get("count") != "1000" {
			t.Fatal("unexpected surplus allocation size")
		}
		ids := make([]string, 1000)
		for i := range ids {
			ids[i] = fmt.Sprint("id", i)
		}
		ids[len(ids)-1] = ""
		payload, _ := json.Marshal(map[string]any{"ids": ids})
		return memoryResponse(http.StatusOK, string(payload)), nil
	}))
	defer s.Close()
	s.idPool = []idPoolEntry{{id: "saved", at: time.Now()}}
	ids, err := s.takeIDs(context.Background(), 2, 1)
	if err == nil || ids != nil || len(s.idPool) != 1 || s.idPool[0].id != "saved" || s.poolFlight != nil {
		t.Fatalf("invalid surplus entered the pool or consumed existing IDs: %q, %v", ids, err)
	}
}

func TestDriveDuplicateCreatePreservesProbeFailure(t *testing.T) {
	for _, outcome := range []string{"auth", "cancel", "deadline", "missing"} {
		t.Run(outcome, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
				defer cancel()
				creates, reads, tokens := 0, 0, 0
				s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
					switch {
					case r.Method == http.MethodPost && r.URL.Path == "/files":
						creates++
						return memoryResponse(http.StatusConflict, "duplicate"), nil
					case r.URL.Path == "/token":
						tokens++
						return memoryResponse(http.StatusBadRequest, `{"error":"invalid_grant"}`), nil
					case r.Method == http.MethodGet && r.URL.Path == "/files/item":
						reads++
						switch outcome {
						case "auth":
							return memoryResponse(http.StatusUnauthorized, ""), nil
						case "cancel":
							cancel()
							return nil, r.Context().Err()
						case "deadline":
							<-ctx.Done()
							return nil, ctx.Err()
						default:
							return memoryResponse(http.StatusNotFound, ""), nil
						}
					default:
						t.Fatal("failed creation probe attempted an update")
						return nil, io.ErrUnexpectedEOF
					}
				}))
				defer s.Close()
				err := s.createSlot(ctx, "item", "streams/session/c2s/s0", "frame", nil)
				switch outcome {
				case "auth":
					if !isAuthError(err) || tokens != 1 {
						t.Fatalf("duplicate fallback hid terminal authentication: %v", err)
					}
				case "cancel":
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("duplicate fallback hid cancellation: %v", err)
					}
				case "deadline":
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("duplicate fallback hid deadline: %v", err)
					}
				default:
					var conflict createError
					if !errors.As(err, &conflict) || conflict.status != http.StatusConflict {
						t.Fatalf("missing duplicate target lost the original create error: %v", err)
					}
				}
				if creates != 1 || reads != 1 || len(s.inflight)+len(s.reserved) != 0 {
					t.Fatal("duplicate fallback retried or leaked permits")
				}
			})
		})
	}
}

func TestDriveKeyedSlotCreationPreservesMetadataAndMedia(t *testing.T) {
	for _, media := range []bool{false, true} {
		for _, duplicate := range []bool{false, true} {
			t.Run(fmt.Sprintf("media=%v/duplicate=%v", media, duplicate), func(t *testing.T) {
				creates, reads, updates := 0, 0, 0
				var body []byte
				if media {
					body = []byte("media payload")
				}
				name, id, description := "streams/session/c2s/s0", "item", "frame"
				s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
					if r.URL.Query().Get("supportsAllDrives") != "true" {
						t.Fatal("keyed slot request lost shared-drive support")
					}
					if r.Method == http.MethodGet {
						reads++
						if !duplicate || r.URL.Path != "/files/"+id || r.URL.Query().Get("fields") != "description" {
							t.Fatal("unexpected duplicate probe")
						}
						return memoryResponse(http.StatusOK, `{"description":"old frame"}`), nil
					}
					var metadata map[string]any
					if media {
						_, parameters, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
						if err != nil || r.URL.Query().Get("uploadType") != "multipart" {
							t.Fatalf("media slot lost multipart upload: %v", err)
						}
						multipart := multipart.NewReader(r.Body, parameters["boundary"])
						part, err := multipart.NextPart()
						if err != nil {
							t.Fatal(err)
						}
						if err := json.NewDecoder(part).Decode(&metadata); err != nil {
							t.Fatal(err)
						}
						part, err = multipart.NextPart()
						if err != nil {
							t.Fatal(err)
						}
						got, err := io.ReadAll(part)
						if err != nil || !bytes.Equal(got, body) {
							t.Fatalf("media slot payload changed: %v", err)
						}
					} else if err := json.NewDecoder(r.Body).Decode(&metadata); err != nil {
						t.Fatal(err)
					}
					if metadata["description"] != description {
						t.Fatal("slot description changed")
					}
					base := "/files"
					if media {
						base = "/upload"
					}
					if r.Method == http.MethodPost {
						creates++
						parents, ok := metadata["parents"].([]any)
						if r.URL.Path != base || metadata["id"] != id || metadata["name"] != flatten(name) || !ok || len(parents) != 1 || parents[0] != "fake-folder" {
							t.Fatal("keyed creation lost identity, name, or parent")
						}
						if duplicate {
							return memoryResponse(http.StatusConflict, "duplicate"), nil
						}
					} else if r.Method == http.MethodPatch {
						updates++
						if !duplicate || r.URL.Path != base+"/"+id || len(metadata) != 1 {
							t.Fatal("duplicate fallback changed existing identity metadata")
						}
					} else {
						t.Fatal("unexpected keyed slot method")
					}
					return memoryResponse(http.StatusOK, `{"id":"item"}`), nil
				}))
				s.uploadURL += "?uploadType=multipart&fields=id,name"
				defer s.Close()
				if err := s.createSlot(context.Background(), id, name, description, body); err != nil {
					t.Fatal(err)
				}
				wantFallback := 0
				if duplicate {
					wantFallback = 1
				}
				cached, ok := s.cachedID(flatten(name))
				if creates != 1 || reads != wantFallback || updates != wantFallback || !ok || cached != id || len(s.inflight)+len(s.reserved) != 0 {
					t.Fatal("keyed creation lost identity, duplicated work, or leaked permits")
				}
			})
		}
	}
}

func TestDriveColdIDsBatchDemandAndSurplus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		// The pool keeps one full generateIds batch, at least 1000 ids.
		const batch = 1000
		calls := 0
		s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			calls++
			if r.URL.Query().Get("count") != strconv.Itoa(batch) {
				t.Fatal("cold allocation did not include bounded surplus")
			}
			close(entered)
			<-release
			ids := make([]string, batch)
			for i := range ids {
				ids[i] = fmt.Sprint(i)
			}
			body, _ := json.Marshal(map[string]any{"ids": ids})
			return memoryResponse(http.StatusOK, string(body)), nil
		}))
		s.owner = s
		type result struct {
			ids []string
			err error
		}
		done := make(chan result, 2)
		take := func() { ids, err := s.takeIDs(context.Background(), 65, 65); done <- result{ids, err} }
		go take()
		<-entered
		go take()
		synctest.Wait()
		close(release)
		seen := make(map[string]bool)
		for range 2 {
			r := <-done
			if r.err != nil || len(r.ids) != 65 {
				t.Fatalf("allocation failed: %v", r.err)
			}
			for _, id := range r.ids {
				if seen[id] {
					t.Fatal("IDs handed to both callers")
				}
				seen[id] = true
			}
		}
		synctest.Wait()
		s.poolMu.Lock()
		pooled := len(s.idPool)
		s.poolMu.Unlock()
		if calls != 1 || pooled != batch-2*65 {
			t.Fatalf("cold allocation requested twice or lost surplus: calls=%d pooled=%d", calls, pooled)
		}
		s.Close()
	})
}

func TestDriveIDWaiterIgnoresWrappedCallerCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected allocation"); return nil, io.EOF }))
		f := &idPoolFlight{done: make(chan struct{})}
		s.poolFlight = f
		done := make(chan error, 1)
		go func() { _, err := s.takeIDs(context.Background(), 1, 1); done <- err }()
		synctest.Wait()
		s.poolMu.Lock()
		s.idPool = append(s.idPool, idPoolEntry{id: "available", at: time.Now()})
		s.coolUntil = time.Now().Add(time.Hour)
		s.finishPoolFlightLocked(f, fmt.Errorf("wrapped: %w", context.Canceled))
		s.poolMu.Unlock()
		if err := <-done; err != nil || s.poolErr != nil || !s.poolRetryAt.IsZero() {
			t.Fatalf("caller cancellation poisoned another allocation: %v", err)
		}
	})
}

// A flight can fail after it filled the pool, so the waiter must look at the
// pool before it repeats the flight's failure.
func TestDriveIDWaiterTakesPoolAfterFailedFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected allocation"); return nil, io.EOF }))
		f := &idPoolFlight{done: make(chan struct{})}
		s.poolFlight = f
		done := make(chan error, 1)
		go func() { _, err := s.takeIDs(context.Background(), 1, 1); done <- err }()
		synctest.Wait()
		s.poolMu.Lock()
		s.idPool = append(s.idPool, idPoolEntry{id: "available", at: time.Now()})
		s.coolUntil = time.Now().Add(time.Hour)
		s.finishPoolFlightLocked(f, errors.New("generateIds failed"))
		s.poolMu.Unlock()
		if err := <-done; err != nil {
			t.Fatalf("waiter repeated a flight failure although the pool was filled: %v", err)
		}
	})
}

// The death of one session must not leave the shared pool in error: the other
// sessions have to be able to allocate for themselves.
func TestDriveIDPoolSessionCloseDoesNotPoisonWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			count, err := strconv.Atoi(r.URL.Query().Get("count"))
			if err != nil || count <= 0 {
				t.Fatalf("unexpected id request %v", r.URL)
			}
			ids := make([]string, count)
			for i := range ids {
				ids[i] = fmt.Sprint("id", i)
			}
			body, _ := json.Marshal(map[string]any{"ids": ids})
			return memoryResponse(http.StatusOK, string(body)), nil
		}))
		f := &idPoolFlight{done: make(chan struct{})}
		s.poolFlight = f
		done := make(chan error, 1)
		go func() { _, err := s.takeIDs(context.Background(), 1, 1); done <- err }()
		synctest.Wait()
		s.poolMu.Lock()
		s.coolUntil = time.Now().Add(time.Minute)
		s.finishPoolFlightLocked(f, fmt.Errorf("Drive request failed: %w", errSessionClosed))
		s.poolMu.Unlock()
		if err := <-done; err != nil {
			t.Fatalf("a dying session poisoned another session: %v", err)
		}
		if s.poolErr != nil || !s.poolRetryAt.IsZero() {
			t.Fatalf("a dying session left the shared pool in error: %v", s.poolErr)
		}
	})
}

func TestDriveKeepWarmReleasesBindingsBetweenTicks(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		var made atomic.Int32
		probed := make(chan struct{}, 2)
		pool := newConnPool(&splithttp.XmuxConfig{MaxConcurrency: &splithttp.RangeConfig{From: 1, To: 1}}, func(context.Context) *http.Client {
			made.Add(1)
			return &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) {
				probed <- struct{}{}
				return memoryResponse(http.StatusOK, ""), nil
			})}
		})
		defer pool.Close()
		s := memoryDrive(nil)
		s.owner, s.conns = s, pool
		s.keepWarmEvery, s.keepWarmWindow = time.Minute, time.Hour
		s.keeperRunning = true
		s.lastTraffic = time.Now()
		ctx, cancel := context.WithCancel(context.WithValue(context.Background(), keeperKey{}, true))
		done := make(chan struct{})
		go s.keepWarm(ctx, done)
		<-probed
		synctest.Wait()
		foreground := &sessionTransport{pool: pool}
		defer foreground.Close()
		req, _ := http.NewRequest(http.MethodGet, "http://drive.invalid/files/item", nil)
		resp, err := foreground.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if made.Load() != 1 || foreground.client.Running.Load() != 1 {
			t.Fatal("keeper binding prevented foreground reuse of the warmed socket")
		}
		cancel()
		<-done
	})
}

func TestDriveKeepWarmChecksEachProbe(t *testing.T) {
	s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.Context().Value(bodyKey{}) != nil {
			return memoryResponse(http.StatusOK, ""), nil
		}
		return memoryResponse(http.StatusNotFound, ""), nil
	}))
	primary := s.keepWarmConnection(context.Background(), "http://drive.invalid/files/folder")
	bulk := s.keepWarmConnection(context.WithValue(context.Background(), bodyKey{}, true), "http://drive.invalid/files/folder")
	if primary == nil || bulk != nil {
		t.Fatalf("probe status lost: primary=%v bulk=%v", primary, bulk)
	}
}

func TestDriveTimingSnapshotOwnsSamplesAndWaits(t *testing.T) {
	a := newTimingAggregator()
	a.started = true
	a.request(kindBodyRead)
	a.record(kindBodyRead, timingSample{waitCool: time.Second, waitToken: 2 * time.Second, newConn: true})
	batch := a.snapshot()[kindBodyRead]
	a.request(kindBodyRead)
	a.record(kindBodyRead, timingSample{body: time.Second})
	next := a.snapshot()[kindBodyRead]
	if batch.requests != 1 || batch.conns != 1 || len(batch.samples) != 1 || batch.samples[0].waitCool != time.Second || batch.samples[0].waitToken != 2*time.Second || next.requests != 1 || next.conns != 0 || next.samples[0].body != time.Second {
		t.Fatal("snapshot retained mutable samples or lost wait accounting")
	}
}

func TestDriveBackgroundTokenRefreshSharesForegroundFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		calls := 0
		s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
			calls++
			close(entered)
			<-release
			return memoryResponse(http.StatusOK, `{"access_token":"new","expires_in":3600}`), nil
		}))
		s.tokenLifetime, s.tokenExpiry = time.Hour, time.Now().Add(time.Second)
		if token, err := s.accessToken(context.Background()); token != "fake" || err != nil {
			t.Fatal("usable token blocked on refresh")
		}
		<-entered
		s.invalidateToken("fake")
		done := make(chan error, 1)
		go func() {
			token, err := s.accessToken(context.Background())
			if err == nil && token != "new" {
				err = fmt.Errorf("wrong token")
			}
			done <- err
		}()
		synctest.Wait()
		close(release)
		if err := <-done; err != nil || calls != 1 {
			t.Fatalf("competing token refresh: %v calls=%d", err, calls)
		}
	})
}

func TestDriveRejectsInvalidTokenLifetime(t *testing.T) {
	for _, lifetime := range []string{"0", "-1", "9223372036854775807"} {
		s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
			return memoryResponse(http.StatusOK, `{"access_token":"new","expires_in":`+lifetime+`}`), nil
		}))
		if _, _, err := s.fetchToken(context.Background()); err == nil {
			t.Fatal("invalid lifetime accepted")
		}
	}
}

func TestDriveAdmissionTimeoutDoesNotThrottleConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := memoryDrive(nil)
		transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
			client, server := stdnet.Pipe()
			s.coolMu.Lock()
			s.coolUntil = time.Now().Add(time.Hour)
			s.coolMu.Unlock()
			go func() { defer server.Close(); io.Copy(io.Discard, server) }()
			return client, nil
		}}
		defer transport.CloseIdleConnections()
		s.client.Transport = transport
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		ctx = context.WithValue(ctx, attemptKey{}, time.Millisecond)
		if _, _, err := s.do(ctx, http.MethodGet, "http://drive.invalid/files/item", "", nil); err == nil || s.throttle.Load() != 0 {
			t.Fatal("undispatched timeout counted as connection stall")
		}
	})
}

func TestDriveTimingDoesNotDoubleCountDispatchWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := memoryDrive(nil)
		s.timing = newTimingAggregator()
		s.timing.started = true
		transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
			client, server := stdnet.Pipe()
			s.coolMu.Lock()
			s.coolUntil = time.Now().Add(time.Second)
			s.coolMu.Unlock()
			go func() {
				defer server.Close()
				if _, err := http.ReadRequest(bufio.NewReader(server)); err == nil {
					io.WriteString(server, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
				}
			}()
			return client, nil
		}}
		defer transport.CloseIdleConnections()
		s.client.Transport = transport
		if _, _, err := s.do(context.Background(), http.MethodGet, "http://drive.invalid/files/item", "", nil); err != nil {
			t.Fatal(err)
		}
		sample := s.timing.snapshot()[kindOtherRead].samples[0]
		if sample.waitCool != time.Second || sample.waitConn != 0 {
			t.Fatalf("dispatch wait counted twice: cool=%v conn=%v", sample.waitCool, sample.waitConn)
		}
	})
}

func TestDriveDescriptionHedgeCancellationDoesNotJoinRPCs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{}, 2)
		release := make(chan struct{})
		rt := roundTrip(func(r *http.Request) (*http.Response, error) {
			entered <- struct{}{}
			<-r.Context().Done()
			<-release
			return nil, r.Context().Err()
		})
		s := memoryDrive(rt)
		s.bulkClient = &http.Client{Transport: rt}
		s.readHedge, s.readHedgeTokens = time.Millisecond, 1
		ctx, cancel := context.WithCancel(context.WithValue(context.Background(), laneKey{}, true))
		done := make(chan error, 1)
		go func() { _, _, err := s.readDescHedged(ctx, "http://drive.invalid/files/item"); done <- err }()
		<-entered
		<-entered
		cancel()
		if err := <-done; err != context.Canceled {
			t.Fatalf("cancellation waited for child teardown: %v", err)
		}
		close(release)
		synctest.Wait()
	})
}

func TestDriveIdentityMergePreservesDuplicatePreference(t *testing.T) {
	s := memoryDrive(nil)
	s.rememberID("item", "primary", []string{"old", "older", "old", "", "primary"})
	s.rememberID("item", "new", []string{"old", "extra", "new", "primary", ""})
	known := s.cachedFiles("item", false)["item"]
	if known.id != "primary" || strings.Join(known.extra, ",") != "new,old,extra,older" {
		t.Fatal("duplicate merge changed primary, preference, or uniqueness")
	}
	for i := range 1000 {
		s.rememberID("many", fmt.Sprint(i), nil)
	}
	for i := range 1000 {
		s.rememberID("many", fmt.Sprint(i), nil)
	}
	known = s.cachedFiles("many", false)["many"]
	seen := map[string]bool{known.id: true}
	for _, id := range known.extra {
		if seen[id] {
			t.Fatal("repeated listing retained duplicate identities")
		}
		seen[id] = true
	}
	if known.id != "0" || len(seen) != 1000 || known.extra[0] != "999" {
		t.Fatal("repeated listing lost identities or changed preference")
	}
}

func TestDriveQueryBatchesDuplicateIdentitiesAcrossPartialPages(t *testing.T) {
	files := make([]map[string]string, 0, 1000)
	for i := range 1000 {
		files = append(files, map[string]string{"id": fmt.Sprint(i), "name": "item", "description": "first"})
	}
	payload, _ := json.Marshal(map[string]interface{}{"nextPageToken": "next", "files": files})
	s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.Query().Get("pageToken") != "" {
			return memoryResponse(http.StatusOK, "{"), nil
		}
		return memoryResponse(http.StatusOK, string(payload)), nil
	}))
	s.rememberID("item", "cached", []string{"older"})
	for range 2 {
		found, err := s.query(context.Background(), "name = 'item'")
		known := s.cachedFiles("item", false)["item"]
		if err == nil || found["item"].id != "0" || found["item"].description != "first" || len(found["item"].extra) != 999 || known.id != "cached" || len(known.extra) != 1001 || known.extra[0] != "999" || known.extra[999] != "0" || known.extra[1000] != "older" {
			t.Fatal("partial listing lost identities, ordering, metadata, or cache deduplication")
		}
	}
}

func TestDriveQueryPublishesPagesWithoutReplayingRetiredIDs(t *testing.T) {
	var s *driveStorage
	s = memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.Query().Get("pageToken") == "" {
			return memoryResponse(http.StatusOK, `{"nextPageToken":"next","files":[{"id":"a","name":"item"},{"id":"b","name":"item"},{"id":"primary","name":"item"}]}`), nil
		}
		known := s.cachedFiles("item", false)["item"]
		if known.id != "primary" || strings.Join(known.extra, ",") != "b,a,older" {
			t.Fatal("first page was not visible or changed duplicate preference")
		}
		s.retireID("item", "a")
		return memoryResponse(http.StatusOK, `{"files":[]}`), nil
	}))
	s.rememberID("item", "primary", []string{"older"})
	found, err := s.query(context.Background(), "name = 'item'")
	known := s.cachedFiles("item", false)["item"]
	if err != nil || found["item"].id != "a" || len(found["item"].extra) != 2 || known.id != "primary" || strings.Join(known.extra, ",") != "b,older" {
		t.Fatal("later page completion resurrected a retired earlier-page identity")
	}
}

func TestDriveGetBatchesExhaustedIdentitiesBeforeRefresh(t *testing.T) {
	for _, survives := range []bool{false, true} {
		flat := flatten("sessions/item")
		missing := make([]string, 1000)
		for i := range missing {
			missing[i] = fmt.Sprint(i)
		}
		var s *driveStorage
		reads, listings := 0, 0
		s = memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/files" {
				listings++
				if _, cached := s.cachedID(flat); cached {
					t.Fatal("exhausted identities remained cached before refresh")
				}
				return memoryResponse(http.StatusOK, `{"files":[{"id":"kept","name":"sessions~item"},{"id":"0","name":"sessions~item"}]}`), nil
			}
			reads++
			if strings.HasSuffix(r.URL.Path, "/kept") && survives {
				return memoryResponse(http.StatusOK, "payload"), nil
			}
			return memoryResponse(http.StatusNotFound, ""), nil
		}))
		s.rememberID(flat, missing[0], missing[1:])
		body, err := s.Get(context.Background(), "sessions/item")
		known := s.cachedFiles(flat, false)[flat]
		if listings != 1 || reads != 1001 || survives && (err != nil || string(body) != "payload" || known.id != "kept" || len(known.extra) != 0) || !survives && (err != errNotFound || known.id != "") {
			t.Fatal("batch retirement repeated stale reads, retained missing IDs, or lost a survivor")
		}
	}
}

func TestDriveMissingIdentityIsHiddenDuringSurvivorRead(t *testing.T) {
	flat := flatten("sessions/item")
	var s *driveStorage
	s = memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/gone") {
			return memoryResponse(http.StatusNotFound, ""), nil
		}
		id, cached := s.cachedID(flat)
		known := s.cachedFiles(flat, false)[flat]
		if !cached || id != "kept" || known.id != "kept" || len(known.extra) != 0 {
			t.Fatal("survivor read left a confirmed missing identity visible")
		}
		s.rememberID(flat, "gone", nil)
		return memoryResponse(http.StatusOK, "payload"), nil
	}))
	s.rememberID(flat, "gone", []string{"kept"})
	body, err := s.Get(context.Background(), "sessions/item")
	known := s.cachedFiles(flat, false)[flat]
	if err != nil || string(body) != "payload" || known.id != "kept" || strings.Join(known.extra, ",") != "gone" || len(s.missingIDs[flat]) != 0 {
		t.Fatal("completed stale read erased a later confirmed identity publication")
	}
	s.markMissingID(flat, "uncached")
	if len(s.missingIDs[flat]) != 0 {
		t.Fatal("uncached historical identity created a missing marker")
	}
	s.markMissingID(flat, "gone")
	s.rememberListedIDs(flat, []string{"fresh"})
	known = s.cachedFiles(flat, false)[flat]
	if known.id != "kept" || strings.Join(known.extra, ",") != "fresh" || len(s.knownIDs[flat]) != 2 || len(s.missingIDs[flat]) != 0 {
		t.Fatal("merge resurrected a marked old identity or retained stale marker state")
	}
	s.markMissingID(flat, "kept")
	s.forgetID(flat)
	if len(s.knownIDs[flat]) != 0 || len(s.missingIDs[flat]) != 0 {
		t.Fatal("forget retained membership or missing marker state")
	}
}

func TestDriveRetireIDsFiltersCacheInOneBatch(t *testing.T) {
	s := memoryDrive(nil)
	retired := make(map[string]struct{}, 1000)
	extra := []string{"survivor", "other"}
	for i := range 1000 {
		id := fmt.Sprint(i)
		retired[id] = struct{}{}
		extra = append(extra, id)
	}
	s.rememberID("item", "0", extra)
	s.retireIDs("item", retired)
	known := s.cachedFiles("item", false)["item"]
	if known.id != "survivor" || strings.Join(known.extra, ",") != "other" {
		t.Fatal("batch retirement lost or reordered surviving identities")
	}
	s.retireIDs("item", map[string]struct{}{"survivor": {}, "other": {}})
	if _, ok := s.cachedID("item"); ok || len(s.dupIDs["item"]) != 0 {
		t.Fatal("batch retirement retained an exhausted cache entry")
	}
}

func TestDriveListPreservesDirectInlineWithDescendants(t *testing.T) {
	s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
		return memoryResponse(http.StatusOK, `{"files":[{"id":"child","name":"sessions~one~child"},{"id":"direct","name":"sessions~one","description":"aW5saW5l"}]}`), nil
	}))
	for range 32 {
		entries, err := s.List(context.Background(), "sessions")
		if err != nil || len(entries) != 1 || string(entries[0].Inline) != "inline" {
			t.Fatalf("direct metadata lost: %v", err)
		}
	}
}

func TestDriveGetRetiresEveryPreviouslyMissingListedIdentity(t *testing.T) {
	calls := make(map[string]int)
	s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/files" {
			return memoryResponse(http.StatusOK, `{"files":[{"id":"kept","name":"sessions~item"},{"id":"gone","name":"sessions~item"}]}`), nil
		}
		id := strings.TrimPrefix(r.URL.Path, "/files/")
		calls[id]++
		if id == "gone" {
			return memoryResponse(http.StatusNotFound, ""), nil
		}
		return memoryResponse(http.StatusOK, "payload"), nil
	}))
	s.rememberID(flatten("sessions/item"), "gone", nil)
	body, err := s.Get(context.Background(), "sessions/item")
	known := s.cachedFiles(flatten("sessions/item"), false)[flatten("sessions/item")]
	if err != nil || string(body) != "payload" || calls["gone"] != 1 || calls["kept"] != 1 || known.id != "kept" || len(known.extra) != 0 {
		t.Fatalf("early survivor success retained a previously missing identity: %v", err)
	}
}

func TestDriveGetUsesIdentitiesFromPartiallyFailedListing(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, survives := range []bool{false, true} {
			calls := make(map[string]int)
			pages := 0
			s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/files" {
					pages++
					if r.URL.Query().Get("pageToken") != "" {
						return memoryResponse(http.StatusOK, "{"), nil
					}
					return memoryResponse(http.StatusOK, `{"nextPageToken":"next","files":[{"id":"gone","name":"sessions~item"},{"id":"kept","name":"sessions~item"}]}`), nil
				}
				id := strings.TrimPrefix(r.URL.Path, "/files/")
				calls[id]++
				if id == "gone" || !survives {
					return memoryResponse(http.StatusNotFound, ""), nil
				}
				return memoryResponse(http.StatusOK, "payload"), nil
			}))
			if cached {
				s.rememberID(flatten("sessions/item"), "gone", nil)
			}
			body, err := s.Get(context.Background(), "sessions/item")
			_, remembered := s.cachedID(flatten("sessions/item"))
			if calls["gone"] != 1 || calls["kept"] != 1 || survives && (err != nil || string(body) != "payload" || pages != 2) || !survives && (err == nil || err == errNotFound || remembered) {
				t.Fatalf("partial listing discarded confirmed identity or retained stale one: %v", err)
			}
		}
	}
}

func TestDriveGetRefreshesExhaustedIdentitySnapshot(t *testing.T) {
	for _, inline := range []bool{false, true} {
		for _, survives := range []bool{false, true} {
			calls := make(map[string]int)
			lists := 0
			s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/files" {
					lists++
					if !strings.Contains(r.URL.Query().Get("q"), "name = 'sessions~item'") {
						t.Fatal("identity refresh was not exact-name")
					}
					return memoryResponse(http.StatusOK, `{"files":[{"id":"gone","name":"sessions~item"},{"id":"kept","name":"sessions~item"},{"id":"kept","name":"sessions~item"}]}`), nil
				}
				id := strings.TrimPrefix(r.URL.Path, "/files/")
				calls[id]++
				if inline && r.URL.Query().Get("alt") == "media" {
					return memoryResponse(http.StatusOK, ""), nil
				}
				if id == "gone" || !survives {
					return memoryResponse(http.StatusNotFound, ""), nil
				}
				if inline {
					return memoryResponse(http.StatusOK, `{"description":"cGF5bG9hZA=="}`), nil
				}
				return memoryResponse(http.StatusOK, "payload"), nil
			}))
			s.rememberID(flatten("sessions/item"), "gone", nil)
			body, err := s.Get(context.Background(), "sessions/item")
			id, cached := s.cachedID(flatten("sessions/item"))
			reads := 1
			if inline {
				reads = 2
			}
			if lists != 1 || calls["gone"] != reads || calls["kept"] != reads || len(calls) != 2 || survives && (err != nil || string(body) != "payload" || id != "kept") || !survives && (err != errNotFound || cached) {
				t.Fatalf("identity refresh omitted survivors or repeated missing IDs: %v", err)
			}
		}
	}
}

func TestDriveGetRetriesCachedDuplicateAfterEitherStage404(t *testing.T) {
	for _, inline := range []bool{false, true} {
		s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/gone") {
				if inline && r.URL.Query().Get("alt") == "media" {
					return memoryResponse(http.StatusOK, ""), nil
				}
				return memoryResponse(http.StatusNotFound, ""), nil
			}
			return memoryResponse(http.StatusOK, "payload"), nil
		}))
		s.rememberID(flatten("sessions/item"), "gone", []string{"kept"})
		body, err := s.Get(context.Background(), "sessions/item")
		id, _ := s.cachedID(flatten("sessions/item"))
		if err != nil || string(body) != "payload" || id != "kept" {
			t.Fatalf("readable duplicate lost: %v", err)
		}
	}
}

func TestDriveGetIncludesConcurrentlyPromotedDuplicate(t *testing.T) {
	flat := flatten("sessions/item")
	var s *driveStorage
	calls := make(map[string]int)
	s = memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.Query().Get("alt") != "media" {
			if r.URL.Query().Get("pageToken") == "next" {
				s.retireID(flat, "gone")
				return memoryResponse(http.StatusOK, `{"files":[]}`), nil
			}
			return memoryResponse(http.StatusOK, `{"nextPageToken":"next","files":[{"id":"gone","name":"sessions~item"},{"id":"kept","name":"sessions~item"},{"id":"kept","name":"sessions~item"}]}`), nil
		}
		id := strings.TrimPrefix(r.URL.Path, "/files/")
		calls[id]++
		if id == "gone" {
			return memoryResponse(http.StatusNotFound, ""), nil
		}
		return memoryResponse(http.StatusOK, "payload"), nil
	}))
	body, err := s.Get(context.Background(), "sessions/item")
	known := s.cachedFiles(flat, false)[flat]
	if err != nil || string(body) != "payload" || known.id != "kept" || len(known.extra) != 0 || calls["gone"] != 1 || calls["kept"] != 1 || len(calls) != 2 {
		t.Fatalf("promoted duplicate was omitted or retried: err=%v candidates=%d", err, len(calls))
	}
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

// The token backoff exists to spare the token endpoint. A wait that cannot end
// inside the request's own budget is pure latency: report the last token error
// now instead of stalling the caller for the whole budget.
func TestDriveTokenBackoffBeyondBudgetFailsFast(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		calls := 0
		s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) { calls++; return nil, fmt.Errorf("unexpected request") }))
		s.token = ""
		s.lastTokenErr = fmt.Errorf("token unavailable")
		s.tokenRetryAt = time.Now().Add(2 * driveAuthBudget)
		started := time.Now()
		if _, err := s.accessToken(context.Background()); !errors.Is(err, s.lastTokenErr) || time.Since(started) != 0 || calls != 0 {
			t.Fatalf("token backoff stalled a request: %v after %v calls=%d", err, time.Since(started), calls)
		}
	})
}

// A refresh grant can be rejected while the cached access token is still valid;
// that must not discard the usable token before it expires.
func TestDriveCachedTokenSurvivesRefreshRejection(t *testing.T) {
	s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected request"); return nil, io.EOF }))
	s.tokenMu.Lock()
	s.authErr = authError{code: "invalid_grant"}
	s.tokenMu.Unlock()
	if token, err := s.accessToken(context.Background()); err != nil || token != "fake" {
		t.Fatalf("rejected refresh discarded a valid access token: token=%q err=%v", token, err)
	}
}

// A persistent API 401 means the answer will not change. One retry with a
// freshly fetched token may ride out a token that expired early; refetching a
// token for all eight attempts only multiplies requests against the account.
func TestDriveRejectedTokenStopsAfterOneRefresh(t *testing.T) {
	var tokens, api atomic.Int32
	s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/token" {
			tokens.Add(1)
			return memoryResponse(http.StatusOK, `{"access_token":"fresh","expires_in":3600}`), nil
		}
		api.Add(1)
		return memoryResponse(http.StatusUnauthorized, `{"error":{"code":401}}`), nil
	}))
	if _, _, err := s.do(context.Background(), http.MethodGet, "http://drive.invalid/files/item", "", nil); err == nil || !isAuthError(err) {
		t.Fatalf("persistent 401 was retried instead of reported: %v", err)
	}
	if got := api.Load(); got != 2 {
		t.Fatalf("persistent 401 spent %d API calls, want 2", got)
	}
	if got := tokens.Load(); got != 1 {
		t.Fatalf("persistent 401 fetched %d tokens, want 1", got)
	}
}

func TestDriveQuota403SurvivesCompleteErrorBodyReadFailure(t *testing.T) {
	for _, create := range []bool{false, true} {
		driveClock(t, func(t *testing.T) {
			calls := 0
			s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
				calls++
				if calls > 1 {
					return memoryResponse(http.StatusOK, "accepted"), nil
				}
				resp := memoryResponse(http.StatusForbidden, "")
				resp.Body = io.NopCloser(io.MultiReader(strings.NewReader(`{"error":{"errors":[{"reason":"rateLimitExceeded"}]}}`), failingDriveBody{}))
				return resp, nil
			}))
			ctx, method := context.Background(), http.MethodGet
			if create {
				ctx = context.WithValue(ctx, createOnceKey{}, true)
				method = http.MethodPost
			}
			status, body, err := s.do(ctx, method, "http://drive.invalid/files/item", "", nil)
			if err != nil || status != http.StatusOK || string(body) != "accepted" || calls != 2 || s.lastRateLimitAt().IsZero() {
				t.Fatalf("confirmed quota rejection bypassed retry or cooldown: %v", err)
			}
		})
	}
}

func TestDriveRejectedStatusSurvivesBodyReadFailure(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests} {
		driveClock(t, func(t *testing.T) {
			calls, tokens := 0, 0
			var s *driveStorage
			s = memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/token" {
					tokens++
					return memoryResponse(http.StatusOK, `{"access_token":"fresh","expires_in":3600}`), nil
				}
				calls++
				if status == http.StatusUnauthorized && calls > 1 {
					if r.Header.Get("Authorization") != "Bearer fresh" {
						t.Fatal("body failure retained the rejected token")
					}
					return memoryResponse(http.StatusOK, "accepted"), nil
				}
				if status == http.StatusTooManyRequests && calls > 1 && s.lastRateLimitAt().IsZero() {
					t.Fatal("body failure bypassed quota cooldown")
				}
				if calls > driveMaxAttempts {
					return memoryResponse(http.StatusOK, "accepted"), nil
				}
				resp := memoryResponse(status, "")
				resp.Body = failingDriveBody{}
				return resp, nil
			}))
			code, body, err := s.do(context.Background(), http.MethodGet, "http://drive.invalid/files/item", "", nil)
			if err != nil || code != http.StatusOK || string(body) != "accepted" || status == http.StatusUnauthorized && tokens != 1 || len(s.inflight)+len(s.reserved) != 0 {
				t.Fatalf("known rejection lost status handling: %v", err)
			}
		})
	}
}

func TestDriveQuotaRetryBudget(t *testing.T) {
	for _, tc := range []struct {
		elapsed      time.Duration
		refund, stop bool
	}{
		{driveRateLimitBudget - time.Nanosecond, true, false},
		{driveRateLimitBudget, false, true},
	} {
		if refund, stop := driveQuotaRetry(tc.elapsed); refund != tc.refund || stop != tc.stop {
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

// A request whose first rate-limited answer arrives after the budget is spent
// must stop there: the budget is the limit, not the attempt count.
func TestDriveQuotaBudgetStopsLateRateLimit(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		var calls atomic.Int32
		s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			<-time.After(driveRateLimitBudget + time.Second)
			return memoryResponse(http.StatusTooManyRequests, `{"error":{"errors":[{"reason":"rateLimitExceeded"}]}}`), nil
		}))
		if _, _, err := s.do(context.Background(), http.MethodGet, "http://drive.invalid/files/item", "", nil); err == nil {
			t.Fatal("rate limited request succeeded")
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("late rate limit spent %d Drive calls, want 1", got)
		}
	})
}

func TestDriveDescriptionRescueBudgetDoesNotRearmPendingRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var hedges atomic.Int32
		primaryDone := make(chan struct{}, 2)
		s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			<-r.Context().Done()
			primaryDone <- struct{}{}
			return nil, r.Context().Err()
		}))
		s.readHedge = time.Second
		s.bulkClient = &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) {
			hedges.Add(1)
			return memoryResponse(http.StatusOK, "rescued"), nil
		})}
		ctx, cancel := context.WithCancel(context.WithValue(context.Background(), laneKey{}, true))
		defer cancel()
		done := make(chan error, 1)
		go func() {
			status, body, err := s.readDescHedged(ctx, "http://drive.invalid/files/item")
			if err == nil && (status != http.StatusOK || string(body) != "rescued") {
				err = fmt.Errorf("unexpected rescue result")
			}
			done <- err
		}()
		synctest.Wait()
		<-time.NewTimer(2 * s.readHedge).C
		synctest.Wait()
		if hedges.Load() != 0 {
			t.Fatal("hedged without a token")
		}
		s.readHedgeMu.Lock()
		s.readHedgeTokens = 1
		s.readHedgeMu.Unlock()
		synctest.Wait()
		if hedges.Load() != 0 {
			t.Fatal("a spent description timer chased replenished tokens")
		}
		cancel()
		if err := <-done; err != context.Canceled {
			t.Fatalf("pending read did not honor cancellation: %v", err)
		}
		<-primaryDone
		synctest.Wait()
		// A subsequent head read may spend the token; the old pending read may not.
		status, body, err := s.readDescHedged(context.WithValue(context.Background(), laneKey{}, true), "http://drive.invalid/files/item")
		if err != nil || status != http.StatusOK || string(body) != "rescued" {
			t.Fatalf("next read did not use available rescue credit: status=%d body=%q err=%v", status, body, err)
		}
		<-primaryDone
		synctest.Wait()
		if hedges.Load() != 1 || len(s.inflight) != 0 || len(s.reserved) != 0 {
			t.Fatal("rescue duplicated or leaked a permit")
		}
	})
}

func TestDriveMediaHeadOnlySplitsRanges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		body := strings.Repeat("x", 2<<20)
		frame := newSlotFrame(1, slotMedia, []byte(body))
		for _, tc := range []struct {
			head, cool bool
			calls      int
		}{{false, false, 4}, {true, false, 4}, {true, true, 1}} {
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

type mediaBufferBody struct {
	data []byte
	last *byte
}

func (b *mediaBufferBody) Read(p []byte) (int, error) {
	n := copy(p, b.data)
	b.data = b.data[n:]
	if n > 0 {
		b.last = &p[n-1]
	}
	if len(b.data) == 0 {
		return n, io.EOF
	}
	return n, nil
}

func (*mediaBufferBody) Close() error { return nil }

func TestDriveSinglePartMediaReturnsValidatedRangeBuffer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tc := range []struct {
			size, status     int
			badCRC, badRange bool
		}{{384, http.StatusOK, false, false}, {384, http.StatusPartialContent, false, false}, {512 << 10, http.StatusOK, false, false}, {512 << 10, http.StatusPartialContent, false, false}, {512 << 10, http.StatusPartialContent, true, false}, {512 << 10, http.StatusPartialContent, false, true}} {
			payload := bytes.Repeat([]byte{7}, tc.size)
			reader := &mediaBufferBody{data: payload}
			calls := 0
			s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Header.Get("Range") != fmt.Sprintf("bytes=0-%d", tc.size-1) {
					t.Fatal("single-part range changed")
				}
				resp := memoryResponse(tc.status, "")
				resp.Body = reader
				resp.Header.Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", tc.size-1, tc.size))
				if tc.badRange {
					resp.Header.Set("Content-Range", fmt.Sprintf("bytes 1-%d/%d", tc.size, tc.size))
				}
				return resp, nil
			}))
			frame := newSlotFrame(1, slotMedia, payload)
			if tc.badCRC {
				frame.crc ^= 1
			}
			body, err := s.readFrameBody(context.Background(), "item", frame)
			if tc.badCRC || tc.badRange {
				if err == nil || body != nil {
					t.Fatal("invalid single-part data was returned")
				}
			} else if err != nil || !bytes.Equal(body, payload) || tc.size <= 512 && &body[len(body)-1] != reader.last {
				t.Fatalf("single-part range buffer was copied or changed: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if body, err := s.readFrameBody(ctx, "item", frame); body != nil || err != context.Canceled || calls != 1 {
				t.Fatalf("single-part cancellation changed: %v", err)
			}
		}
	})
}

type gatedCloseBody struct {
	io.Reader
	closing chan struct{}
	release <-chan struct{}
}

func (b gatedCloseBody) Close() error {
	close(b.closing)
	<-b.release
	return nil
}

func TestDrivePermitHeldUntilBodyClose(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		closing, release := make(chan struct{}), make(chan struct{})
		s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
			resp := memoryResponse(http.StatusOK, "")
			resp.Body = gatedCloseBody{Reader: strings.NewReader("ok"), closing: closing, release: release}
			return resp, nil
		}))
		done := make(chan error, 1)
		go func() { _, _, err := s.do(context.Background(), http.MethodGet, s.filesURL, "", nil); done <- err }()
		<-closing
		if len(s.inflight) != 1 {
			t.Fatal("permit released before body Close completed")
		}
		close(release)
		if err := <-done; err != nil || len(s.inflight) != 0 {
			t.Fatalf("permit after Close: %v", err)
		}
	})
}

func TestDriveMediaRangePoolLeavesSlotPermitsFree(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		started, release := make(chan struct{}), make(chan struct{})
		s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Range") != "" {
				close(started)
				<-release
				resp := memoryResponse(http.StatusPartialContent, "x")
				resp.Header.Set("Content-Range", "bytes 0-0/1")
				return resp, nil
			}
			return memoryResponse(http.StatusOK, ""), nil
		}))
		s.mediaRanges = make(chan struct{}, 1)
		ctx := context.WithValue(context.Background(), driveMediaKey{}, &driveMediaRequest{start: 0, end: 1, size: 1})
		done := make(chan error, 1)
		go func() { _, _, err := s.do(ctx, http.MethodGet, s.filesURL+"?alt=media", "", nil); done <- err }()
		<-started
		if len(s.mediaRanges) != cap(s.mediaRanges) || len(s.inflight) != 0 {
			t.Fatal("media range consumed slot permits")
		}
		_, _, err := s.do(context.Background(), http.MethodPatch, s.filesURL, "application/json", []byte(`{"description":"slot"}`))
		close(release)
		if err != nil {
			t.Fatalf("slot write behind full range pool: %v", err)
		}
		if err := <-done; err != nil || len(s.mediaRanges) != 0 {
			t.Fatalf("range completion: %v", err)
		}
	})
}
