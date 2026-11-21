package xdrive

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"mime"
	"mime/multipart"
	stdnet "net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
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

// Google Drive storage tests.

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

func TestDriveCachedTokenSurvivesRefreshRejection(t *testing.T) {
	s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected request"); return nil, io.EOF }))
	s.tokenMu.Lock()
	s.authErr = authError{code: "invalid_grant"}
	s.tokenMu.Unlock()
	if token, err := s.accessToken(context.Background()); err != nil || token != "fake" {
		t.Fatalf("rejected refresh discarded a valid access token: token=%q err=%v", token, err)
	}
}

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

// Media range tests.

type mediaTestResult struct {
	body []byte
	err  error
}

type mediaTestGatedBody struct {
	io.Reader
	ctx        context.Context
	release    <-chan struct{}
	started    chan struct{}
	canceled   chan struct{}
	closed     chan struct{}
	startOnce  sync.Once
	cancelOnce sync.Once
	closeOnce  sync.Once
	closes     atomic.Int32
}

func newMediaTestGatedBody(ctx context.Context, data []byte, release <-chan struct{}) *mediaTestGatedBody {
	return &mediaTestGatedBody{
		Reader: bytes.NewReader(data), ctx: ctx, release: release,
		started: make(chan struct{}), canceled: make(chan struct{}), closed: make(chan struct{}),
	}
}

func (b *mediaTestGatedBody) Read(p []byte) (int, error) {
	b.startOnce.Do(func() { close(b.started) })
	select {
	case <-b.release:
		return b.Reader.Read(p)
	case <-b.ctx.Done():
		b.cancelOnce.Do(func() { close(b.canceled) })
		return 0, b.ctx.Err()
	}
}

func (b *mediaTestGatedBody) Close() error {
	b.closes.Add(1)
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func mediaTestPayload(size int) []byte {
	body := make([]byte, size)
	for i := range body {
		body[i] = byte((i * 31) ^ (i >> 8) ^ (i >> 16))
	}
	return body
}

func mediaTestCheckRequest(t *testing.T, r *http.Request, start, end int) {
	t.Helper()
	if r.Method != http.MethodGet || r.URL.Path != "/files/item" || r.URL.Query().Get("alt") != "media" || r.URL.Query().Get("supportsAllDrives") != "true" {
		t.Errorf("unexpected media request: %s %s", r.Method, r.URL)
	}
	if got, want := r.Header.Get("Range"), fmt.Sprintf("bytes=%d-%d", start, end-1); got != want {
		t.Errorf("Range = %q, want %q", got, want)
	}
	if got := r.Header.Get("Accept-Encoding"); got != "identity" {
		t.Errorf("Accept-Encoding = %q, want identity", got)
	}
}

func mediaTestClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func mediaTestAssertBody(t *testing.T, body *mediaTestGatedBody, canceled bool) {
	t.Helper()
	if !mediaTestClosed(body.closed) || body.closes.Load() != 1 {
		t.Errorf("response body was not closed exactly once: closes=%d", body.closes.Load())
	}
	if got := mediaTestClosed(body.canceled); got != canceled {
		t.Errorf("response body canceled = %v, want %v", got, canceled)
	}
}

func mediaTestAssertNoPermits(t *testing.T, s *driveStorage) {
	t.Helper()
	if len(s.inflight) != 0 || len(s.reserved) != 0 || len(s.mediaRanges) != 0 || len(s.mediaRescue) != 0 {
		t.Errorf("leaked permits: inflight=%d reserved=%d media=%d rescue=%d", len(s.inflight), len(s.reserved), len(s.mediaRanges), len(s.mediaRescue))
	}
}

func mediaTestAssertPending(t *testing.T, done <-chan mediaTestResult) {
	t.Helper()
	select {
	case res := <-done:
		t.Fatalf("read returned before the remaining requests completed: bytes=%d err=%v", len(res.body), res.err)
	default:
	}
}

type mediaTestSplitFlight struct {
	storage  *driveStorage
	cancel   context.CancelFunc
	done     chan mediaTestResult
	releases []chan struct{}
	bodies   []chan *mediaTestGatedBody
	calls    []atomic.Int32
}

func startMediaTestSplit(t *testing.T, frame slotFrame, sources [][]byte, fullBody bool, invalidRange int) *mediaTestSplitFlight {
	t.Helper()
	parts := len(sources)
	width := (frame.size + parts - 1) / parts
	flight := &mediaTestSplitFlight{
		done: make(chan mediaTestResult, 1), releases: make([]chan struct{}, parts),
		bodies: make([]chan *mediaTestGatedBody, parts), calls: make([]atomic.Int32, parts),
	}
	clients := make([]*http.Client, parts)
	for i := range parts {
		flight.releases[i] = make(chan struct{})
		flight.bodies[i] = make(chan *mediaTestGatedBody, driveMaxAttempts)
		clients[i] = &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
			flight.calls[i].Add(1)
			start, end := i*width, min((i+1)*width, frame.size)
			mediaTestCheckRequest(t, r, start, end)
			status, data := http.StatusPartialContent, sources[i][start:end]
			if fullBody && i%2 == 1 {
				status, data = http.StatusOK, sources[i]
			}
			body := newMediaTestGatedBody(r.Context(), data, flight.releases[i])
			flight.bodies[i] <- body
			resp := memoryResponse(status, "")
			resp.Body = body
			if status == http.StatusPartialContent {
				offset := start
				if i == invalidRange {
					offset++
				}
				resp.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, end-1, frame.size))
			}
			return resp, nil
		})}
	}
	flight.storage = memoryDrive(clients[0].Transport)
	flight.storage.mediaClients = clients
	flight.storage.mediaRanges = make(chan struct{}, parts)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), mediaHeadKey{}, true))
	flight.cancel = cancel
	go func() {
		body, err := flight.storage.readFrameBody(ctx, "item", frame)
		flight.done <- mediaTestResult{body, err}
	}()
	return flight
}

func (f *mediaTestSplitFlight) awaitBodies(t *testing.T) []*mediaTestGatedBody {
	t.Helper()
	bodies := make([]*mediaTestGatedBody, len(f.bodies))
	for i := range bodies {
		bodies[i] = <-f.bodies[i]
		<-bodies[i].started
	}
	synctest.Wait()
	if len(f.storage.mediaRanges) != len(bodies) || len(f.storage.inflight) != 0 || len(f.storage.reserved) != 0 {
		t.Fatalf("split ranges did not acquire only their own permits: media=%d inflight=%d reserved=%d", len(f.storage.mediaRanges), len(f.storage.inflight), len(f.storage.reserved))
	}
	mediaTestAssertPending(t, f.done)
	return bodies
}

func TestDriveMediaRegressionSplitAssembly(t *testing.T) {
	for _, tc := range []struct {
		name       string
		fullBody   bool
		alteration string
	}{
		{name: "exact_206"},
		{name: "mixed_200_and_206", fullBody: true},
		{name: "crc_corruption", fullBody: true, alteration: "corruption"},
		{name: "mixed_versions", fullBody: true, alteration: "versions"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				payload := mediaTestPayload(3*driveMediaPart + 3)
				frame := newSlotFrame(1, slotMedia, payload)
				width := (len(payload) + driveMediaLanes - 1) / driveMediaLanes
				if len(payload)%driveMediaLanes == 0 {
					t.Fatal("fixture must have an uneven final range")
				}
				sources := make([][]byte, driveMediaLanes)
				for i := range sources {
					sources[i] = payload
				}
				switch tc.alteration {
				case "corruption":
					corrupt := bytes.Clone(payload)
					corrupt[2*width+11] ^= 0x80
					sources[2] = corrupt
				case "versions":
					other := bytes.Clone(payload)
					for i := range other {
						other[i] ^= 0x80
					}
					sources[1], sources[3] = other, other
				}
				assembled := make([]byte, len(payload))
				for i, source := range sources {
					start, end := i*width, min((i+1)*width, len(payload))
					copy(assembled[start:end], source[start:end])
				}
				if got, want := frame.holds(assembled), tc.alteration == ""; got != want {
					t.Fatalf("checksum fixture matches = %v, want %v", got, want)
				}

				flight := startMediaTestSplit(t, frame, sources, tc.fullBody, -1)
				defer flight.cancel()
				bodies := flight.awaitBodies(t)
				for i := len(bodies) - 1; i >= 0; i-- {
					close(flight.releases[i])
					synctest.Wait()
					mediaTestAssertBody(t, bodies[i], false)
					flight.storage.readHedgeMu.Lock()
					completed := flight.storage.mediaHedgeCount
					flight.storage.readHedgeMu.Unlock()
					if want := len(bodies) - i; completed != want {
						t.Fatalf("completed ranges = %d, want %d after releasing range %d", completed, want, i)
					}
					if i > 0 {
						mediaTestAssertPending(t, flight.done)
					}
				}
				res := <-flight.done
				if tc.alteration != "" {
					if res.body != nil || res.err == nil || !strings.Contains(res.err.Error(), "checksum") {
						t.Fatalf("invalid assembled data was not rejected: bytes=%d err=%v", len(res.body), res.err)
					}
				} else if res.err != nil || !bytes.Equal(res.body, payload) {
					t.Fatalf("out-of-order ranges changed the payload: bytes=%d err=%v", len(res.body), res.err)
				}
				synctest.Wait()
				mediaTestAssertNoPermits(t, flight.storage)
				for i := range flight.calls {
					if got := flight.calls[i].Load(); got != 1 {
						t.Errorf("range %d fetched %d times, want 1", i, got)
					}
				}
			})
		})
	}
}

func TestDriveMediaRegressionSplitCleanup(t *testing.T) {
	for _, invalidRange := range []int{-1, 2} {
		name := "caller_cancellation"
		if invalidRange >= 0 {
			name = "invalid_range_cancels_siblings"
		}
		t.Run(name, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				payload := mediaTestPayload(3*driveMediaPart + 3)
				sources := make([][]byte, driveMediaLanes)
				for i := range sources {
					sources[i] = payload
				}
				flight := startMediaTestSplit(t, newSlotFrame(1, slotMedia, payload), sources, false, invalidRange)
				defer flight.cancel()
				bodies := flight.awaitBodies(t)
				if invalidRange >= 0 {
					close(flight.releases[invalidRange])
				} else {
					flight.cancel()
				}
				res := <-flight.done
				if res.body != nil || res.err == nil {
					t.Fatalf("failed split returned data or no error: bytes=%d err=%v", len(res.body), res.err)
				}
				if invalidRange < 0 && res.err != context.Canceled {
					t.Errorf("cancellation returned %v, want context.Canceled", res.err)
				}
				if invalidRange >= 0 && !strings.Contains(res.err.Error(), "unexpected offset or length") {
					t.Errorf("invalid range returned an unrelated error: %v", res.err)
				}
				synctest.Wait()
				for i, body := range bodies {
					mediaTestAssertBody(t, body, i != invalidRange)
					if got := flight.calls[i].Load(); got != 1 {
						t.Errorf("range %d fetched %d times, want 1", i, got)
					}
				}
				mediaTestAssertNoPermits(t, flight.storage)
			})
		})
	}
}

func TestDriveMediaRegressionRangeValidation(t *testing.T) {
	payload := mediaTestPayload(29)
	start, end := 7, 19
	exact := fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(payload))
	for _, tc := range []struct {
		name         string
		status       int
		contentRange string
		body         []byte
		valid        bool
	}{
		{"exact_206", http.StatusPartialContent, exact, payload[start:end], true},
		{"full_200", http.StatusOK, "", payload, true},
		{"wrong_start", http.StatusPartialContent, "bytes 8-18/29", payload[start:end], false},
		{"wrong_end", http.StatusPartialContent, "bytes 7-19/29", payload[start:end], false},
		{"wrong_total", http.StatusPartialContent, "bytes 7-18/30", payload[start:end], false},
		{"missing_content_range", http.StatusPartialContent, "", payload[start:end], false},
		{"short_206", http.StatusPartialContent, exact, payload[start : end-1], false},
		{"long_206", http.StatusPartialContent, exact, payload[start : end+1], false},
		{"short_200", http.StatusOK, "", payload[:len(payload)-1], false},
		{"long_200", http.StatusOK, "", append(bytes.Clone(payload), 0), false},
		{"404_with_matching_range", http.StatusNotFound, exact, payload[start:end], false},
		{"416_with_matching_range", http.StatusRequestedRangeNotSatisfiable, exact, payload[start:end], false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				release := make(chan struct{})
				close(release)
				var body *mediaTestGatedBody
				var calls atomic.Int32
				s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
					calls.Add(1)
					mediaTestCheckRequest(t, r, start, end)
					body = newMediaTestGatedBody(r.Context(), tc.body, release)
					resp := memoryResponse(tc.status, "")
					resp.Body = body
					resp.Header.Set("Content-Range", tc.contentRange)
					return resp, nil
				}))
				s.mediaRanges = make(chan struct{}, 1)
				r := driveMediaRequest{start: start, end: end, size: len(payload), client: s.client}
				got, err := s.readMediaRange(context.Background(), s.filesURL+"/item?alt=media&supportsAllDrives=true", r)
				if tc.valid {
					if err != nil || !bytes.Equal(got, payload[start:end]) {
						t.Errorf("valid range rejected or changed: bytes=%d err=%v", len(got), err)
					}
				} else if err == nil || got != nil {
					t.Errorf("invalid response returned data or no error: bytes=%d err=%v", len(got), err)
				}
				synctest.Wait()
				mediaTestAssertBody(t, body, false)
				mediaTestAssertNoPermits(t, s)
				if got := calls.Load(); got != 1 {
					t.Errorf("range fetched %d times, want 1", got)
				}
			})
		})
	}
}

type mediaTestHedgeFlight struct {
	storage  *driveStorage
	payload  []byte
	cancel   context.CancelFunc
	done     chan mediaTestResult
	releases [2]chan struct{}
	bodies   [2]chan *mediaTestGatedBody
	calls    [2]atomic.Int32
}

func startMediaTestHedge(t *testing.T, statuses [2]int) *mediaTestHedgeFlight {
	t.Helper()
	flight := &mediaTestHedgeFlight{payload: mediaTestPayload(257), done: make(chan mediaTestResult, 1)}
	var clients [2]*http.Client
	for i := range clients {
		flight.releases[i] = make(chan struct{})
		flight.bodies[i] = make(chan *mediaTestGatedBody, driveMaxAttempts)
		clients[i] = &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
			flight.calls[i].Add(1)
			mediaTestCheckRequest(t, r, 0, len(flight.payload))
			body := newMediaTestGatedBody(r.Context(), flight.payload, flight.releases[i])
			flight.bodies[i] <- body
			resp := memoryResponse(statuses[i], "")
			resp.Body = body
			resp.Header.Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(flight.payload)-1, len(flight.payload)))
			return resp, nil
		})}
	}
	flight.storage = memoryDrive(clients[0].Transport)
	flight.storage.mediaClients = []*http.Client{clients[0]}
	flight.storage.bulkClient = clients[1]
	flight.storage.mediaRanges = make(chan struct{}, 1)
	flight.storage.mediaRescue = make(chan struct{}, 1)
	flight.storage.mediaHedgeTokens = 1
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), mediaHeadKey{}, true))
	flight.cancel = cancel
	go func() {
		body, err := flight.storage.readFrameBody(ctx, "item", newSlotFrame(1, slotMedia, flight.payload))
		flight.done <- mediaTestResult{body, err}
	}()
	return flight
}

func (f *mediaTestHedgeFlight) awaitBody(t *testing.T, lane int) *mediaTestGatedBody {
	t.Helper()
	body := <-f.bodies[lane]
	<-body.started
	synctest.Wait()
	if len(f.storage.mediaRanges) != 1 || len(f.storage.mediaRescue) != lane || len(f.storage.inflight) != 0 || len(f.storage.reserved) != 0 {
		t.Fatalf("unexpected active hedge permits: media=%d rescue=%d inflight=%d reserved=%d", len(f.storage.mediaRanges), len(f.storage.mediaRescue), len(f.storage.inflight), len(f.storage.reserved))
	}
	mediaTestAssertPending(t, f.done)
	return body
}

func (f *mediaTestHedgeFlight) assertAccounting(t *testing.T, hedged bool, completed int) {
	t.Helper()
	wantHedges := int32(0)
	wantTokens := 1
	if hedged {
		wantHedges, wantTokens = 1, 0
	}
	if primary, hedge := f.calls[0].Load(), f.calls[1].Load(); primary != 1 || hedge != wantHedges {
		t.Errorf("request counts: primary=%d hedge=%d, want 1/%d", primary, hedge, wantHedges)
	}
	f.storage.readHedgeMu.Lock()
	tokens, count := f.storage.mediaHedgeTokens, f.storage.mediaHedgeCount
	f.storage.readHedgeMu.Unlock()
	if tokens != wantTokens || count != completed {
		t.Errorf("hedge accounting: tokens=%d completed=%d, want %d/%d", tokens, count, wantTokens, completed)
	}
	mediaTestAssertNoPermits(t, f.storage)
}

func TestDriveMediaRegressionHedgeWinnerCleanup(t *testing.T) {
	for _, tc := range []struct {
		name   string
		hedged bool
		winner int
	}{
		{"primary_before_hedge", false, 0},
		{"primary_after_hedge", true, 0},
		{"hedge_wins", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				flight := startMediaTestHedge(t, [2]int{http.StatusPartialContent, http.StatusPartialContent})
				defer flight.cancel()
				bodies := []*mediaTestGatedBody{flight.awaitBody(t, 0)}
				if tc.hedged {
					bodies = append(bodies, flight.awaitBody(t, 1))
					if mediaTestClosed(bodies[0].canceled) {
						t.Fatal("launching a hedge canceled the primary")
					}
				}
				close(flight.releases[tc.winner])
				res := <-flight.done
				if res.err != nil || !bytes.Equal(res.body, flight.payload) {
					t.Fatalf("winner returned invalid data: bytes=%d err=%v", len(res.body), res.err)
				}
				synctest.Wait()
				for i, body := range bodies {
					mediaTestAssertBody(t, body, i != tc.winner)
				}
				flight.assertAccounting(t, tc.hedged, 1)
			})
		})
	}
}

func TestDriveMediaRegressionHedgeCancellationCleanup(t *testing.T) {
	for _, hedged := range []bool{false, true} {
		name := "before_hedge"
		if hedged {
			name = "after_hedge"
		}
		t.Run(name, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				flight := startMediaTestHedge(t, [2]int{http.StatusPartialContent, http.StatusPartialContent})
				defer flight.cancel()
				bodies := []*mediaTestGatedBody{flight.awaitBody(t, 0)}
				if hedged {
					bodies = append(bodies, flight.awaitBody(t, 1))
				}
				flight.cancel()
				res := <-flight.done
				if res.body != nil || res.err != context.Canceled {
					t.Fatalf("canceled read returned bytes=%d err=%v, want nil/context.Canceled", len(res.body), res.err)
				}
				synctest.Wait()
				for _, body := range bodies {
					mediaTestAssertBody(t, body, true)
				}
				flight.assertAccounting(t, hedged, 0)
			})
		})
	}
}

func TestDriveMediaRegressionHedgeBothErrorsCleanup(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		flight := startMediaTestHedge(t, [2]int{http.StatusNotFound, http.StatusNotFound})
		defer flight.cancel()
		primary, hedge := flight.awaitBody(t, 0), flight.awaitBody(t, 1)
		close(flight.releases[0])
		synctest.Wait()
		mediaTestAssertBody(t, primary, false)
		mediaTestAssertPending(t, flight.done)
		if mediaTestClosed(hedge.canceled) || len(flight.storage.mediaRanges) != 0 || len(flight.storage.mediaRescue) != 1 {
			t.Fatal("primary failure canceled the pending hedge or leaked its permit")
		}
		close(flight.releases[1])
		res := <-flight.done
		if res.body != nil || res.err != errNotFound {
			t.Fatalf("failed requests returned bytes=%d err=%v, want nil/errNotFound", len(res.body), res.err)
		}
		synctest.Wait()
		mediaTestAssertBody(t, hedge, false)
		flight.assertAccounting(t, true, 0)
	})
}

// Adaptive recovery tests.

func TestMediaHedgeDelayUsesSizeClassMedian(t *testing.T) {
	s := &driveStorage{driveShared: &driveShared{}}
	small := driveMediaRequest{end: driveMediaPart}
	large := driveMediaRequest{end: driveMediaPart + 1}
	for i := 0; i < rangeLatencyMin-1; i++ {
		s.mediaLatency[0].note(time.Second)
	}
	if got := s.mediaHedgeDelay(small); got != slotBodyHedge {
		t.Fatalf("cold threshold = %v, want %v", got, slotBodyHedge)
	}
	s.mediaLatency[0].note(time.Second)
	if got := s.mediaHedgeDelay(small); got != 1300*time.Millisecond {
		t.Fatalf("threshold = %v, want 1.3 times p50", got)
	}
	if got := s.mediaHedgeDelay(large); got != slotBodyHedge {
		t.Fatalf("small samples changed large-range threshold: %v", got)
	}
	for i := 0; i < rangeLatencyRing; i++ {
		s.mediaLatency[0].note(time.Millisecond)
		s.mediaLatency[1].note(10 * time.Second)
	}
	if got := s.mediaHedgeDelay(small); got != mediaHedgeFloor {
		t.Fatalf("floor = %v, want %v", got, mediaHedgeFloor)
	}
	if got := s.mediaHedgeDelay(large); got != slotBodyHedge {
		t.Fatalf("cap = %v, want %v", got, slotBodyHedge)
	}
	peer := &driveStorage{driveShared: s.driveShared}
	if got := peer.mediaHedgeDelay(small); got != mediaHedgeFloor {
		t.Fatalf("sessions did not share the rolling estimate: %v", got)
	}
}

func TestMediaLatencyAdmitsOnlyValidatedFirstPrimaryAttempts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		attempt  int
		rescue   bool
		replayed bool
		known    bool
		err      error
		want     int
	}{
		{name: "partial", status: http.StatusPartialContent, known: true, want: 1},
		{name: "full", status: http.StatusOK, known: true, want: 1},
		{name: "rescue", status: http.StatusPartialContent, known: true, rescue: true},
		{name: "retry", status: http.StatusPartialContent, known: true, attempt: 1},
		{name: "replay", status: http.StatusPartialContent, known: true, replayed: true},
		{name: "failure", status: http.StatusPartialContent, known: true, err: io.ErrUnexpectedEOF},
		{name: "rate_limit", status: http.StatusTooManyRequests, known: true},
		{name: "not_found", status: http.StatusNotFound, known: true},
		{name: "undispatched", status: http.StatusPartialContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &driveStorage{driveShared: &driveShared{}}
			r := &driveMediaRequest{end: driveMediaPart, rescue: tc.rescue}
			sample := timingSample{total: 20 * time.Second, dispatch: time.Second, hasDispatch: tc.known}
			s.observeMediaLatency(r, sample, tc.status, tc.attempt, tc.replayed, tc.err)
			if got := s.mediaLatency[0].n; got != tc.want {
				t.Fatalf("samples = %d, want %d", got, tc.want)
			}
			if tc.want == 1 && s.mediaLatency[0].ring[0] != time.Second {
				t.Fatal("local admission time contaminated the sample")
			}
		})
	}
}

func TestMediaLatencyRingConcurrentAndBounded(t *testing.T) {
	var latency rangeLatency
	latency.note(0)
	latency.note(-time.Second)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				latency.note(time.Second)
				if median, ready := latency.median(); ready && median != time.Second {
					t.Errorf("concurrent median = %v", median)
				}
			}
		})
	}
	wg.Wait()
	if latency.n != rangeLatencyRing || latency.pos < 0 || latency.pos >= rangeLatencyRing {
		t.Fatalf("ring grew or has an invalid cursor: n=%d pos=%d", latency.n, latency.pos)
	}
}

func TestMediaLatencyMeasuresActualDispatchAndRejectsInvalidRange(t *testing.T) {
	for _, valid := range []bool{false, true} {
		t.Run(fmt.Sprint(valid), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan struct{}, 1)
				s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
					entered <- struct{}{}
					time.Sleep(time.Second)
					response := memoryResponse(http.StatusPartialContent, "data")
					if valid {
						response.Header.Set("Content-Range", "bytes 0-3/4")
					}
					return response, nil
				}))
				s.mediaRanges = make(chan struct{}, 1)
				s.mediaRanges <- struct{}{}
				r := &driveMediaRequest{end: 4, size: 4, client: s.client}
				ctx := context.WithValue(context.Background(), driveMediaKey{}, r)
				done := make(chan error, 1)
				go func() { _, _, err := s.do(ctx, http.MethodGet, s.filesURL+"/item?alt=media", "", nil); done <- err }()
				synctest.Wait()
				time.Sleep(5 * time.Second)
				if len(entered) != 0 {
					t.Fatal("primary dispatched before its permit was available")
				}
				<-s.mediaRanges
				<-entered
				if err := <-done; (err == nil) != valid {
					t.Fatalf("range validation error = %v, valid = %v", err, valid)
				}
				synctest.Wait()
				want := 0
				if valid {
					want = 1
				}
				if got := s.mediaLatency[0].n; got != want {
					t.Fatalf("samples = %d, want %d", got, want)
				}
				if valid && s.mediaLatency[0].ring[0] != time.Second {
					t.Fatalf("dispatch latency = %v, want 1s, excluding 5s admission", s.mediaLatency[0].ring[0])
				}
			})
		})
	}
}

type adaptiveDelivery struct {
	mu      sync.Mutex
	next    int
	waitAt  time.Time
	changed chan struct{}
}

func (d *adaptiveDelivery) snapshot() (int, <-chan struct{}, time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.next, d.changed, d.waitAt
}

func (d *adaptiveDelivery) set(next int, waitAt time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.next, d.waitAt = next, waitAt
	close(d.changed)
	d.changed = make(chan struct{})
}

type adaptiveRangeFlight struct {
	s           *driveStorage
	delivery    *adaptiveDelivery
	promoted    chan struct{}
	primary     chan *http.Request
	rescue      chan *http.Request
	release     chan struct{}
	done        chan mediaTestResult
	cancel      context.CancelFunc
	primaryGets atomic.Int32
	rescueGets  atomic.Int32
}

func startAdaptiveRange(t *testing.T, tracked, head bool, start int, tokens int, queuedRescue, failRescue bool) *adaptiveRangeFlight {
	t.Helper()
	f := &adaptiveRangeFlight{
		delivery: &adaptiveDelivery{waitAt: time.Now(), changed: make(chan struct{})},
		promoted: make(chan struct{}), primary: make(chan *http.Request, 1), rescue: make(chan *http.Request, 1),
		release: make(chan struct{}), done: make(chan mediaTestResult, 1),
	}
	data := driveStreamTestData(2 * driveStreamBlockBytes)
	stream := &driveRangeStream{frame: driveStreamTestFrame(data), emit: func(int, []byte) error { return nil }}
	f.s = memoryDrive(roundTrip(func(request *http.Request) (*http.Response, error) {
		media := request.Context().Value(driveMediaKey{}).(*driveMediaRequest)
		body := io.NopCloser(strings.NewReader(string(data[media.start:media.end])))
		if media.rescue {
			f.rescueGets.Add(1)
			f.rescue <- request
			if failRescue {
				return nil, io.ErrUnexpectedEOF
			}
		} else {
			f.primaryGets.Add(1)
			f.primary <- request
			body = &driveStreamTestGatedBody{
				ctx: request.Context(), data: data[media.start:media.end],
				release: f.release, closed: make(chan struct{}),
			}
		}
		return driveStreamTestResponse(request, http.StatusPartialContent,
			fmt.Sprintf("bytes %d-%d/%d", media.start, media.end-1, len(data)), int64(media.end-media.start), body), nil
	}))
	f.s.bulkClient = &http.Client{Transport: f.s.client.Transport}
	f.s.mediaRanges = make(chan struct{}, 1)
	f.s.mediaRescue = make(chan struct{}, 1)
	f.s.mediaHedgeTokens = tokens
	for range rangeLatencyMin {
		f.s.mediaLatency[0].note(time.Second)
	}
	if queuedRescue {
		f.s.mediaRescue <- struct{}{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	ctx = context.WithValue(ctx, mediaPromotionKey{}, (<-chan struct{})(f.promoted))
	if head {
		close(f.promoted)
	}
	r := driveMediaRequest{start: start, end: start + driveStreamBlockBytes, size: len(data), client: f.s.client}
	if tracked {
		r.stream = stream
		ctx = context.WithValue(ctx, mediaDeliveryKey{}, f.delivery.snapshot)
	}
	go func() {
		body, err := f.s.readMediaRange(ctx, f.s.filesURL+"/item?alt=media", r)
		f.done <- mediaTestResult{body, err}
	}()
	t.Cleanup(func() { cancel(); stream.closeAndWait(); f.s.driveShared.closeWork() })
	return f
}

func TestAdaptiveHeadRangeHedgesAtDispatchDeadlineDespiteWireProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := startAdaptiveRange(t, true, true, 0, 1, false, false)
		request := <-f.primary
		synctest.Wait()
		time.Sleep(1300*time.Millisecond - time.Nanosecond)
		request.Context().Value(readProgressKey{}).(*driveReadProgress).progress()
		synctest.Wait()
		if f.rescueGets.Load() != 0 {
			t.Fatal("hedge fired before 1.3 times the live median")
		}
		time.Sleep(time.Nanosecond)
		<-f.rescue
		if result := <-f.done; result.err != nil {
			t.Fatal(result.err)
		}
		synctest.Wait()
		if f.primaryGets.Load() != 1 || f.rescueGets.Load() != 1 || f.s.mediaHedgeTokens != 0 {
			t.Fatal("adaptive recovery exceeded one primary, one rescue, or one budget token")
		}
	})
}

func TestAdaptiveRangeHedgeOnlyAfterHeadPromotion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := startAdaptiveRange(t, true, false, 0, 1, false, false)
		<-f.primary
		synctest.Wait()
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if f.rescueGets.Load() != 0 || f.s.mediaHedgeTokens != 1 {
			t.Fatal("speculative tail armed or spent head recovery")
		}
		at := time.Now()
		close(f.promoted)
		<-f.rescue
		if time.Now() != at {
			t.Fatal("promotion restarted the dispatch-age deadline")
		}
		if result := <-f.done; result.err != nil {
			t.Fatal(result.err)
		}
	})
}

func TestAdaptiveRangeHedgeWaitsForRequiredMissingBlock(t *testing.T) {
	for _, available := range []bool{false, true} {
		t.Run(fmt.Sprint(available), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := driveStreamBlockBytes
				if available {
					start = 0
				}
				f := startAdaptiveRange(t, true, true, start, 1, false, false)
				if available {
					f.delivery.set(0, time.Time{})
				}
				<-f.primary
				synctest.Wait()
				time.Sleep(3 * time.Second)
				synctest.Wait()
				if f.rescueGets.Load() != 0 || f.s.mediaHedgeTokens != 1 {
					t.Fatal("an available block or later range spent recovery credit")
				}
				at := time.Now()
				f.delivery.set(start, at)
				<-f.rescue
				if time.Now() != at {
					t.Fatal("the new missing-block wait replaced the original dispatch age")
				}
				if result := <-f.done; result.err != nil {
					t.Fatal(result.err)
				}
			})
		})
	}
}

func TestAdaptiveQueuedRescueRechecksDeliveryFrontier(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := startAdaptiveRange(t, true, true, 0, 1, true, false)
		<-f.primary
		synctest.Wait()
		time.Sleep(1300 * time.Millisecond)
		synctest.Wait()
		if f.s.mediaHedgeTokens != 0 || f.rescueGets.Load() != 0 {
			t.Fatal("rescue did not queue behind its existing permit")
		}
		f.delivery.set(0, time.Time{})
		<-f.s.mediaRescue
		synctest.Wait()
		if f.rescueGets.Load() != 0 {
			t.Fatal("queued rescue dispatched after the blocking block became available")
		}
		f.cancel()
		if result := <-f.done; !errors.Is(result.err, context.Canceled) {
			t.Fatalf("cancellation = %v", result.err)
		}
	})
}

func TestAdaptiveRangeUsesExistingBudgetReplenishment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := startAdaptiveRange(t, true, true, 0, 0, false, false)
		<-f.primary
		synctest.Wait()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if f.rescueGets.Load() != 0 {
			t.Fatal("recovery ignored an empty media budget")
		}
		f.s.readHedgeMu.Lock()
		f.s.mediaHedgeCount = 19
		f.s.readHedgeMu.Unlock()
		f.s.noteMediaRange()
		<-f.rescue
		if result := <-f.done; result.err != nil {
			t.Fatal(result.err)
		}
		synctest.Wait()
		if f.rescueGets.Load() != 1 || f.s.mediaHedgeTokens != 0 {
			t.Fatal("replenishment expanded the existing token budget")
		}
	})
}

func TestAdaptiveRangeRescueHasOneAttemptAndNoRepeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := startAdaptiveRange(t, true, true, 0, 3, false, true)
		<-f.primary
		<-f.rescue
		synctest.Wait()
		for range 5 {
			f.delivery.set(0, time.Now())
			time.Sleep(time.Second)
			synctest.Wait()
		}
		if f.rescueGets.Load() != 1 || f.s.mediaHedgeTokens != 2 {
			t.Fatal("failed rescue retried or repeated a 200-unit GET")
		}
		close(f.release)
		if result := <-f.done; result.err != nil {
			t.Fatal(result.err)
		}
	})
}

func TestAdaptiveRangeDoesNotRacePrimaryReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := startAdaptiveRange(t, true, true, 0, 1, false, false)
		request := <-f.primary
		request.Context().Value(readProgressKey{}).(*driveReadProgress).replay()
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if f.rescueGets.Load() != 0 || f.s.mediaHedgeTokens != 1 {
			t.Fatal("adaptive hedge duplicated primary transport recovery")
		}
		close(f.release)
		if result := <-f.done; result.err != nil {
			t.Fatal(result.err)
		}
	})
}

func TestUnmarkedMediaRangeNeverArmsHeadHedge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := startAdaptiveRange(t, false, false, 0, 1, false, false)
		<-f.primary
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if f.rescueGets.Load() != 0 || f.s.mediaHedgeTokens != 1 {
			t.Fatal("unmarked standalone body GET armed a head-only hedge")
		}
		close(f.release)
		if result := <-f.done; result.err != nil {
			t.Fatal(result.err)
		}
	})
}

// Verified streaming tests.

func driveStreamTestStorage(t *testing.T, transport roundTrip, lanes int) *driveStorage {
	t.Helper()
	client := &http.Client{Transport: transport}
	s := &driveStorage{
		driveShared: &driveShared{
			filesURL:    "https://drive-stream.invalid/files",
			token:       "fake-token",
			tokenExpiry: time.Now().Add(time.Hour),
		},
		client:      client,
		inflight:    make(chan struct{}, driveMaxInflight),
		reserved:    make(chan struct{}, driveReservedInflight),
		mediaRanges: make(chan struct{}, driveMediaInflight),
	}
	for range lanes {
		s.mediaClients = append(s.mediaClients, &http.Client{Transport: transport})
	}
	t.Cleanup(s.driveShared.closeWork)
	return s
}

func driveStreamTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*17 + i/driveStreamBlockBytes)
	}
	return data
}

func driveStreamTestFrame(data []byte) slotFrame {
	frame := slotFrame{seq: 7, mode: slotStream, size: len(data), crc: crc32.ChecksumIEEE(data)}
	for start := 0; start < len(data); start += driveStreamBlockBytes {
		frame.blocks = append(frame.blocks, crc32.ChecksumIEEE(data[start:min(start+driveStreamBlockBytes, len(data))]))
	}
	return frame
}

func driveStreamTestResponse(request *http.Request, status int, contentRange string, length int64, body io.ReadCloser) *http.Response {
	header := make(http.Header)
	if contentRange != "" {
		header.Set("Content-Range", contentRange)
	}
	return &http.Response{StatusCode: status, Header: header, ContentLength: length, Body: body, Request: request}
}

type driveStreamTestBlock struct {
	index int
	data  []byte
}

type driveStreamTestGatedBody struct {
	ctx       context.Context
	data      []byte
	position  int
	pauseAt   int
	release   <-chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func (b *driveStreamTestGatedBody) Read(p []byte) (int, error) {
	if b.release != nil && b.position >= b.pauseAt {
		select {
		case <-b.release:
			b.release = nil
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		}
	}
	end := len(b.data)
	if b.release != nil {
		end = min(end, b.pauseAt)
	}
	if b.position == end {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.position:end])
	b.position += n
	return n, nil
}

func (b *driveStreamTestGatedBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func TestDriveFrameStreamEmitsVerifiedBlockBeforeEOF(t *testing.T) {
	data := driveStreamTestData(2*driveStreamBlockBytes + 23)
	frame := driveStreamTestFrame(data)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	closed := make(chan struct{})
	s := driveStreamTestStorage(t, func(request *http.Request) (*http.Response, error) {
		body := &driveStreamTestGatedBody{
			ctx: request.Context(), data: data, pauseAt: driveStreamBlockBytes, release: release, closed: closed,
		}
		return driveStreamTestResponse(request, http.StatusPartialContent,
			fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)), int64(len(data)), body), nil
	}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	emitted := make(chan driveStreamTestBlock, len(frame.blocks))
	done := make(chan error, 1)
	go func() {
		done <- s.readFrameStream(ctx, "slot", frame, func(index int, block []byte) error {
			emitted <- driveStreamTestBlock{index, block}
			return nil
		})
	}()
	var first driveStreamTestBlock
	select {
	case first = <-emitted:
	case err := <-done:
		t.Fatalf("stream ended before its first block: %v", err)
	case <-ctx.Done():
		t.Fatal("first verified block was buffered until the rest of the body")
	}
	if first.index != 0 || !bytes.Equal(first.data, data[:driveStreamBlockBytes]) {
		t.Fatal("incorrect first verified block")
	}
	select {
	case err := <-done:
		t.Fatalf("stream completed while its body was blocked: %v", err)
	default:
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("stream did not finish after releasing its body")
	}

	blocks := []driveStreamTestBlock{first}
	for range len(frame.blocks) - 1 {
		select {
		case block := <-emitted:
			blocks = append(blocks, block)
		default:
			t.Fatal("successful stream omitted a block")
		}
	}
	for _, block := range blocks {
		start := block.index * driveStreamBlockBytes
		if !bytes.Equal(block.data, data[start:min(start+driveStreamBlockBytes, len(data))]) {
			t.Fatalf("emitted block %d was changed or reused", block.index)
		}
	}
	select {
	case <-closed:
	default:
		t.Fatal("response body was not closed")
	}
}

func TestDriveRangeStreamResponseValidation(t *testing.T) {
	data := driveStreamTestData(3*driveStreamBlockBytes + 19)
	frame := driveStreamTestFrame(data)
	part := data[driveStreamBlockBytes : 2*driveStreamBlockBytes]
	contentRange := fmt.Sprintf("bytes %d-%d/%d", driveStreamBlockBytes, 2*driveStreamBlockBytes-1, len(data))
	corruptPart := append([]byte(nil), part...)
	corruptPart[0] ^= 1
	corruptBefore := append([]byte(nil), data...)
	corruptBefore[0] ^= 1
	corruptAfter := append([]byte(nil), data...)
	corruptAfter[2*driveStreamBlockBytes] ^= 1
	cases := []struct {
		name         string
		status       int
		contentRange string
		length       int64
		body         []byte
		wantError    bool
		wantBlocks   []int
	}{
		{"partial known length", http.StatusPartialContent, contentRange, int64(len(part)), part, false, []int{1}},
		{"partial unknown length", http.StatusPartialContent, contentRange, -1, part, false, []int{1}},
		{"partial known zero length", http.StatusPartialContent, contentRange, 0, part, true, nil},
		{"ignored range known length", http.StatusOK, "", int64(len(data)), data, false, []int{1}},
		{"ignored range unknown length", http.StatusOK, "", -1, data, false, []int{1}},
		{"ignored range known zero length", http.StatusOK, "", 0, data, true, nil},
		{"wrong range", http.StatusPartialContent, fmt.Sprintf("bytes 0-%d/%d", len(part)-1, len(data)), -1, part, true, nil},
		{"missing range", http.StatusPartialContent, "", -1, part, true, nil},
		{"noncanonical range", http.StatusPartialContent, contentRange + " ", -1, part, true, nil},
		{"unknown total", http.StatusPartialContent, fmt.Sprintf("bytes %d-%d/*", driveStreamBlockBytes, 2*driveStreamBlockBytes-1), -1, part, true, nil},
		{"wrong partial length", http.StatusPartialContent, contentRange, int64(len(part) - 1), part, true, nil},
		{"truncated partial", http.StatusPartialContent, contentRange, -1, part[:len(part)-1], true, nil},
		{"oversized partial", http.StatusPartialContent, contentRange, -1, append(append([]byte(nil), part...), 0), true, []int{1}},
		{"corrupt assigned block", http.StatusPartialContent, contentRange, int64(len(part)), corruptPart, true, nil},
		{"ignored range wrong announced length", http.StatusOK, "", int64(len(part)), data, true, nil},
		{"ignored range truncated", http.StatusOK, "", -1, data[:2*driveStreamBlockBytes], true, []int{1}},
		{"ignored range oversized", http.StatusOK, "", -1, append(append([]byte(nil), data...), 0), true, []int{1}},
		{"ignored range corrupt unassigned prefix", http.StatusOK, "", int64(len(data)), corruptBefore, true, nil},
		{"ignored range corrupt unassigned suffix", http.StatusOK, "", int64(len(data)), corruptAfter, true, []int{1}},
		{"unexpected status", http.StatusBadGateway, "", -1, part, true, nil},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{Transport: roundTrip(func(request *http.Request) (*http.Response, error) {
				return driveStreamTestResponse(request, test.status, test.contentRange, test.length,
					io.NopCloser(bytes.NewReader(test.body))), nil
			})}
			response, err := client.Get("https://drive-stream.invalid/media")
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			var emitted []driveStreamTestBlock
			stream := &driveRangeStream{frame: frame, emit: func(index int, block []byte) error {
				emitted = append(emitted, driveStreamTestBlock{index, block})
				return nil
			}}
			request := driveMediaRequest{start: driveStreamBlockBytes, end: 2 * driveStreamBlockBytes, size: len(data), seq: frame.seq}
			payload, err := stream.read(request, response.StatusCode, response.Header.Get("Content-Range"), response.ContentLength, response.Body)
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v, want error %v", err, test.wantError)
			}
			if payload != nil {
				t.Fatal("stream returned a second buffered payload")
			}
			if len(emitted) != len(test.wantBlocks) {
				t.Fatalf("emitted %d blocks, want %v", len(emitted), test.wantBlocks)
			}
			for i, block := range emitted {
				if block.index != test.wantBlocks[i] || !bytes.Equal(block.data, part) {
					t.Fatalf("unexpected or unverified emitted block %d", block.index)
				}
			}
		})
	}
}

func TestDriveRangeStreamEmitterError(t *testing.T) {
	data := driveStreamTestData(2 * driveStreamBlockBytes)
	frame := driveStreamTestFrame(data)
	want := errors.New("collector stopped")
	calls := 0
	stream := &driveRangeStream{frame: frame, emit: func(int, []byte) error {
		calls++
		return want
	}}
	request := driveMediaRequest{end: len(data), size: len(data), seq: frame.seq}
	payload, err := stream.read(request, http.StatusPartialContent,
		fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)), int64(len(data)), bytes.NewReader(data))
	if !errors.Is(err, want) || calls != 1 || payload != nil {
		t.Fatalf("emitter error was not returned immediately: calls=%d, payload=%d, err=%v", calls, len(payload), err)
	}
}

func TestDriveRangeStreamCloseJoinsAndFencesReaders(t *testing.T) {
	data := driveStreamTestData(driveStreamBlockBytes)
	frame := driveStreamTestFrame(data)
	emitting := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var calls atomic.Int32
	stream := &driveRangeStream{frame: frame, emit: func(int, []byte) error {
		calls.Add(1)
		close(emitting)
		<-release
		return nil
	}}
	request := driveMediaRequest{end: len(data), size: len(data), seq: frame.seq}
	contentRange := fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data))
	readDone := make(chan error, 1)
	go func() {
		_, err := stream.read(request, http.StatusPartialContent, contentRange, int64(len(data)), bytes.NewReader(data))
		readDone <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	select {
	case <-emitting:
	case <-ctx.Done():
		t.Fatal("reader did not enter its emitter")
	}
	joined := make(chan struct{})
	go func() {
		stream.closeAndWait()
		close(joined)
	}()
	select {
	case <-joined:
		t.Fatal("stream shutdown did not join its active emitter")
	case <-time.After(10 * time.Millisecond):
	}
	unblock()
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal("stream shutdown did not join its reader")
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("reader did not complete")
	}
	payload, err := stream.read(request, http.StatusPartialContent, contentRange, int64(len(data)), bytes.NewReader(data))
	if !errors.Is(err, context.Canceled) || payload != nil || calls.Load() != 1 {
		t.Fatalf("a late worker entered the closed stream: calls=%d, err=%v", calls.Load(), err)
	}
}

func TestDriveFrameStreamRangePolicyAndOwnership(t *testing.T) {
	cases := []struct {
		name        string
		size        int
		lanes       int
		head        bool
		promoted    bool
		rateLimited bool
		ignored     bool
		parts       int
	}{
		{"default segment", defaultSegmentBytes, 4, false, false, false, false, 1},
		{"default boundary", defaultSegmentBytes + 1, 4, false, false, false, false, 2},
		{"default large", 2 * defaultSegmentBytes, 4, false, false, false, false, 2},
		{"default maximum lanes", 4 * defaultSegmentBytes, 4, false, false, false, false, 4},
		{"head part", driveMediaPart, 4, true, false, false, false, 1},
		{"head boundary", driveMediaPart + 1, 4, true, false, false, false, 2},
		{"head four parts", defaultSegmentBytes, 4, true, false, false, false, 4},
		{"head three lanes", defaultSegmentBytes + 17, 3, true, false, false, false, 3},
		{"four part ceiling", defaultSegmentBytes + 17, 8, true, false, false, false, 4},
		{"no media lanes", defaultSegmentBytes + 17, 0, true, false, false, false, 1},
		{"recent quota response", defaultSegmentBytes + 17, 4, true, false, true, false, 1},
		{"promotion keeps primary shape", defaultSegmentBytes, 4, false, true, false, false, 1},
		{"ignored range ownership", defaultSegmentBytes + 17, 4, true, false, false, true, 4},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			data := driveStreamTestData(test.size)
			frame := driveStreamTestFrame(data)
			type interval struct{ start, end int }
			var mu sync.Mutex
			var ranges []interval
			blocks := make(map[int][]byte)
			counts := make(map[int]int)
			s := driveStreamTestStorage(t, func(request *http.Request) (*http.Response, error) {
				var start, last int
				if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &last); err != nil {
					return nil, err
				}
				if request.Method != http.MethodGet || request.URL.Query().Get("alt") != "media" ||
					request.URL.Query().Get("supportsAllDrives") != "true" || request.Header.Get("Accept-Encoding") != "identity" {
					return nil, fmt.Errorf("unexpected media request: %s %s", request.Method, request.URL)
				}
				if start < 0 || last < start || last >= len(data) {
					return nil, fmt.Errorf("invalid range %d-%d", start, last)
				}
				mu.Lock()
				ranges = append(ranges, interval{start, last + 1})
				mu.Unlock()
				if test.ignored {
					return driveStreamTestResponse(request, http.StatusOK, "", int64(len(data)), io.NopCloser(bytes.NewReader(data))), nil
				}
				part := data[start : last+1]
				return driveStreamTestResponse(request, http.StatusPartialContent,
					fmt.Sprintf("bytes %d-%d/%d", start, last, len(data)), int64(len(part)), io.NopCloser(bytes.NewReader(part))), nil
			}, test.lanes)
			s.mediaNext.Store(7)
			if test.rateLimited {
				s.lastRateLimit = time.Now()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if test.head {
				ctx = context.WithValue(ctx, mediaHeadKey{}, true)
			}
			if test.promoted {
				promotion := make(chan struct{})
				close(promotion)
				ctx = context.WithValue(ctx, mediaPromotionKey{}, (<-chan struct{})(promotion))
			}
			if err := s.readFrameStream(ctx, "slot", frame, func(index int, block []byte) error {
				mu.Lock()
				defer mu.Unlock()
				counts[index]++
				blocks[index] = block
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(ranges) != test.parts || s.mediaNext.Load() != uint32(7+test.parts) {
				t.Fatalf("primary ranges=%d, rotation=%d; want %d ranges", len(ranges), s.mediaNext.Load(), test.parts)
			}
			sort.Slice(ranges, func(i, j int) bool { return ranges[i].start < ranges[j].start })
			end := 0
			for _, part := range ranges {
				if part.start != end || part.start%driveStreamBlockBytes != 0 ||
					(part.end != len(data) && part.end%driveStreamBlockBytes != 0) {
					t.Fatalf("ranges are not contiguous block-aligned partitions: %v", ranges)
				}
				end = part.end
			}
			if end != len(data) || len(blocks) != len(frame.blocks) {
				t.Fatal("successful ranges did not cover the whole frame")
			}
			for index := range frame.blocks {
				start := index * driveStreamBlockBytes
				if counts[index] != 1 || !bytes.Equal(blocks[index], data[start:min(start+driveStreamBlockBytes, len(data))]) {
					t.Fatalf("block %d was emitted %d times or has incorrect data", index, counts[index])
				}
			}
		})
	}
}

func TestDriveFrameStreamRetriesVerifiedPrefix(t *testing.T) {
	data := driveStreamTestData(2*driveStreamBlockBytes + 19)
	frame := driveStreamTestFrame(data)
	var attempts atomic.Int32
	s := driveStreamTestStorage(t, func(request *http.Request) (*http.Response, error) {
		body := data
		if attempts.Add(1) == 1 {
			body = data[:driveStreamBlockBytes+13]
		}
		return driveStreamTestResponse(request, http.StatusPartialContent,
			fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)), -1, io.NopCloser(bytes.NewReader(body))), nil
	}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var mu sync.Mutex
	seen := make(map[int][]byte)
	counts := make(map[int]int)
	if err := s.readFrameStream(ctx, "slot", frame, func(index int, block []byte) error {
		mu.Lock()
		defer mu.Unlock()
		counts[index]++
		if _, ok := seen[index]; !ok {
			seen[index] = block
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts.Load() != 2 || counts[0] != 2 || len(seen) != len(frame.blocks) {
		t.Fatalf("retry did not preserve/deduplicate its verified prefix: attempts=%d, counts=%v", attempts.Load(), counts)
	}
	for index, block := range seen {
		start := index * driveStreamBlockBytes
		if !bytes.Equal(block, data[start:min(start+driveStreamBlockBytes, len(data))]) {
			t.Fatalf("retry supplied an unverified block %d", index)
		}
	}
}

func TestDriveFrameStreamWaitsForEveryRangeEOF(t *testing.T) {
	data := driveStreamTestData(driveMediaPart + 1)
	frame := driveStreamTestFrame(data)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	s := driveStreamTestStorage(t, func(request *http.Request) (*http.Response, error) {
		var start, last int
		if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &last); err != nil {
			return nil, err
		}
		part := data[start : last+1]
		var body io.ReadCloser = io.NopCloser(bytes.NewReader(part))
		if last == len(data)-1 {
			body = &driveStreamTestGatedBody{
				ctx: request.Context(), data: part, pauseAt: len(part), release: release, closed: make(chan struct{}),
			}
		}
		return driveStreamTestResponse(request, http.StatusPartialContent,
			fmt.Sprintf("bytes %d-%d/%d", start, last, len(data)), int64(len(part)), body), nil
	}, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, mediaHeadKey{}, true)
	emitted := make(chan struct{}, len(frame.blocks))
	done := make(chan error, 1)
	go func() {
		done <- s.readFrameStream(ctx, "slot", frame, func(int, []byte) error {
			emitted <- struct{}{}
			return nil
		})
	}()
	for range frame.blocks {
		select {
		case <-emitted:
		case err := <-done:
			t.Fatalf("stream ended before all blocks: %v", err)
		case <-ctx.Done():
			t.Fatal("stream did not emit all blocks before its EOF check")
		}
	}
	select {
	case err := <-done:
		t.Fatalf("all blocks incorrectly counted as validated EOF: %v", err)
	default:
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("stream did not join all successful ranges")
	}
}

func TestDriveFrameStreamErrorCancelsOtherRanges(t *testing.T) {
	data := driveStreamTestData(driveMediaPart + 1)
	frame := driveStreamTestFrame(data)
	started := make(chan struct{})
	closed := make(chan struct{})
	var startedOnce sync.Once
	s := driveStreamTestStorage(t, func(request *http.Request) (*http.Response, error) {
		var start, last int
		if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &last); err != nil {
			return nil, err
		}
		if start == 0 {
			select {
			case <-started:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
			return driveStreamTestResponse(request, http.StatusPartialContent, "wrong range", -1,
				io.NopCloser(bytes.NewReader(data[start:last+1]))), nil
		}
		startedOnce.Do(func() { close(started) })
		body := &driveStreamTestGatedBody{
			ctx: request.Context(), data: data[start : last+1], pauseAt: 0, release: make(chan struct{}), closed: closed,
		}
		return driveStreamTestResponse(request, http.StatusPartialContent,
			fmt.Sprintf("bytes %d-%d/%d", start, last, len(data)), -1, body), nil
	}, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, mediaHeadKey{}, true)
	if err := s.readFrameStream(ctx, "slot", frame, func(int, []byte) error {
		t.Error("invalid or blocked ranges must not emit blocks")
		return nil
	}); err == nil {
		t.Fatal("invalid range succeeded")
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("failed frame did not cancel and close its other range")
	}
}

func TestDriveFrameStreamLeavesLegacyMediaUnchanged(t *testing.T) {
	data := driveStreamTestData(defaultSegmentBytes + 17)
	frame := newSlotFrame(7, slotMedia, data)
	var requests atomic.Int32
	s := driveStreamTestStorage(t, func(request *http.Request) (*http.Response, error) {
		requests.Add(1)
		var start, last int
		if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &last); err != nil {
			return nil, err
		}
		part := data[start : last+1]
		return driveStreamTestResponse(request, http.StatusPartialContent,
			fmt.Sprintf("bytes %d-%d/%d", start, last, len(data)), int64(len(part)), io.NopCloser(bytes.NewReader(part))), nil
	}, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	body, err := s.readFrameBody(ctx, "slot", frame)
	if err != nil || !bytes.Equal(body, data) || requests.Load() != 2 {
		t.Fatalf("legacy media changed: requests=%d, bytes=%d, err=%v", requests.Load(), len(body), err)
	}
	frame.crc ^= 1
	if _, err := s.readFrameBody(ctx, "slot", frame); err == nil {
		t.Fatal("legacy media no longer checks its whole-frame CRC")
	}
}

// Streaming negotiation tests.

func TestDriveStreamCapsLegacyFallback(t *testing.T) {
	s := &driveStorage{}
	s.observeStreamingProperties(map[string]string{"xdriveStream": "1", "xdriveSession": "session"})
	s.configureStreaming("", true)
	if s.streamCaps.Load() != nil || s.streamingEnabled() {
		t.Fatal("unconfigured or empty-nonce storage enabled streaming")
	}
	if properties := s.streamingProperties(); properties != nil {
		t.Fatalf("unconfigured properties = %v, want nil", properties)
	}
	if fields := s.streamingFields(); fields != "description" {
		t.Fatalf("unconfigured fields = %q, want description", fields)
	}

	s.configureStreaming("session", false)
	s.observeStreamingProperties(nil)
	s.observeStreamingProperties(map[string]string{})
	if s.streamingEnabled() {
		t.Fatal("legacy peer with no properties enabled streaming")
	}
	if fields := s.streamingFields(); fields != "description,properties" {
		t.Fatalf("configured fields = %q, want description,properties", fields)
	}
	if properties := s.streamingProperties(); properties != nil {
		t.Fatalf("endpoint without an accepted ring offer advertised properties: %v", properties)
	}
}

func TestDriveStreamCapsPeerOffer(t *testing.T) {
	server := &driveStorage{}
	server.configureStreaming("session", true)
	if !server.streamingEnabled() {
		t.Fatal("declared peer reader did not immediately enable the server writer")
	}
	properties := server.streamingProperties()
	if len(properties) != 2 || properties["xdriveStream"] != "1" || properties["xdriveSession"] != "session" {
		t.Fatalf("server reader acknowledgment = %v", properties)
	}

	client := &driveStorage{}
	client.configureStreaming("session", false)
	if client.streamingEnabled() {
		t.Fatal("client writer enabled before the server reader offer")
	}
	if properties := client.streamingProperties(); properties != nil {
		t.Fatalf("client advertised metadata before the server acknowledgment: %v", properties)
	}
	client.observeStreamingProperties(properties)
	if !client.streamingEnabled() {
		t.Fatal("matching server reader offer did not enable the client writer")
	}
	if properties := client.streamingProperties(); properties != nil {
		t.Fatalf("confirmed client advertised metadata: %v", properties)
	}
}

func TestDriveStreamCapsOwnOutgoingMetadataCannotBootstrap(t *testing.T) {
	client := &driveStorage{}
	client.configureStreaming("session", false)
	oldPeer := &driveStorage{}
	oldPeer.configureStreaming("session", false)
	ownProperties := client.streamingProperties()
	if ownProperties != nil {
		t.Fatalf("client outgoing slot carries a reader acknowledgment: %v", ownProperties)
	}
	client.observeStreamingProperties(ownProperties)
	client.observeStreamingProperties(oldPeer.streamingProperties())
	if client.streamingEnabled() || oldPeer.streamingEnabled() {
		t.Fatal("own-slot recovery or a legacy peer enabled a streaming writer")
	}
	if properties := oldPeer.streamingProperties(); properties != nil {
		t.Fatalf("server without an accepted ring offer advertised properties: %v", properties)
	}
	server := &driveStorage{}
	server.configureStreaming("session", true)
	client.observeStreamingProperties(server.streamingProperties())
	if !client.streamingEnabled() || client.streamingProperties() != nil {
		t.Fatal("server acknowledgment did not confirm the client without self-advertising")
	}
}

func TestDriveStreamCapsObserveProperties(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]string
		want       bool
	}{
		{name: "nil"},
		{name: "empty", properties: map[string]string{}},
		{name: "missing version", properties: map[string]string{"xdriveSession": "session"}},
		{name: "missing nonce", properties: map[string]string{"xdriveStream": "1"}},
		{name: "empty nonce", properties: map[string]string{"xdriveStream": "1", "xdriveSession": ""}},
		{name: "wrong nonce", properties: map[string]string{"xdriveStream": "1", "xdriveSession": "other"}},
		{name: "nonce whitespace", properties: map[string]string{"xdriveStream": "1", "xdriveSession": " session"}},
		{name: "unknown version", properties: map[string]string{"xdriveStream": "2", "xdriveSession": "session"}},
		{name: "zero version", properties: map[string]string{"xdriveStream": "0", "xdriveSession": "session"}},
		{name: "empty version", properties: map[string]string{"xdriveStream": "", "xdriveSession": "session"}},
		{name: "padded version", properties: map[string]string{"xdriveStream": "01", "xdriveSession": "session"}},
		{name: "version whitespace", properties: map[string]string{"xdriveStream": "1 ", "xdriveSession": "session"}},
		{name: "wrong key case", properties: map[string]string{"XdriveStream": "1", "xdriveSession": "session"}},
		{name: "matching", properties: map[string]string{"xdriveStream": "1", "xdriveSession": "session"}, want: true},
		{name: "unrelated properties", properties: map[string]string{"xdriveStream": "1", "xdriveSession": "session", "unrelated": "value"}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &driveStorage{}
			s.configureStreaming("session", false)
			s.observeStreamingProperties(tt.properties)
			if enabled := s.streamingEnabled(); enabled != tt.want {
				t.Fatalf("streamingEnabled() = %v, want %v", enabled, tt.want)
			}
			if properties := s.streamingProperties(); properties != nil {
				t.Fatalf("observing metadata caused a client advertisement: %v", properties)
			}
		})
	}
}

func TestDriveStreamCapsPropertiesAreIndependent(t *testing.T) {
	s := &driveStorage{}
	s.configureStreaming("session", true)
	properties := s.streamingProperties()
	properties["xdriveStream"] = "2"
	properties["xdriveSession"] = "other"
	properties["extra"] = "value"
	other := s.streamingProperties()
	if len(other) != 2 || other["xdriveStream"] != "1" || other["xdriveSession"] != "session" {
		t.Fatalf("caller mutation changed subsequent properties: %v", other)
	}
	client := &driveStorage{}
	client.configureStreaming("session", false)
	client.observeStreamingProperties(properties)
	if client.streamingEnabled() {
		t.Fatal("mutated server properties enabled the client writer")
	}
	client.observeStreamingProperties(other)
	if !client.streamingEnabled() {
		t.Fatal("original reader offer no longer matched after map mutation")
	}
}

func TestDriveStreamCapsMonotonicAndEpochReset(t *testing.T) {
	s := &driveStorage{}
	s.configureStreaming("first", false)
	first := s.streamCaps.Load()
	s.observeStreamingProperties(map[string]string{"xdriveStream": "1", "xdriveSession": "first"})
	s.configureStreaming("first", false)
	s.configureStreaming("", false)
	for _, properties := range []map[string]string{
		nil,
		{},
		{"xdriveStream": "0", "xdriveSession": "first"},
		{"xdriveStream": "1", "xdriveSession": "other"},
	} {
		s.observeStreamingProperties(properties)
		if !s.streamingEnabled() {
			t.Fatal("confirmed peer capability was revoked within the session")
		}
	}
	if s.streamCaps.Load() != first || first.advertise || s.streamingProperties() != nil {
		t.Fatal("same-nonce configuration replaced capability state or enabled client advertisement")
	}

	s.configureStreaming("second", false)
	second := s.streamCaps.Load()
	if second == first || second.session != "second" || s.streamingEnabled() {
		t.Fatal("different nonce did not start a fresh, unconfirmed epoch")
	}
	s.observeStreamingProperties(map[string]string{"xdriveStream": "1", "xdriveSession": "first"})
	first.peerReady.Store(true)
	if s.streamingEnabled() {
		t.Fatal("old-epoch properties or state enabled the new epoch")
	}
	s.configureStreaming("second", true)
	s.configureStreaming("second", false)
	if s.streamCaps.Load() != second || !s.streamingEnabled() || second.advertise || s.streamingProperties() != nil {
		t.Fatal("same-epoch peer offer was not retained independently of advertisement")
	}

	s.configureStreaming("third", true)
	third := s.streamCaps.Load()
	s.configureStreaming("third", false)
	properties := s.streamingProperties()
	if third == second || s.streamCaps.Load() != third || !third.advertise || !s.streamingEnabled() || len(properties) != 2 || properties["xdriveSession"] != "third" {
		t.Fatal("new accepted-offer epoch did not preserve its metadata acknowledgment")
	}
	s.configureStreaming("fourth", false)
	s.observeStreamingProperties(properties)
	if s.streamingEnabled() || s.streamCaps.Load().advertise || s.streamingProperties() != nil {
		t.Fatal("previous epoch's advertisement or readiness leaked into a fresh client epoch")
	}
}

func TestDriveStreamCapsSessionIsolation(t *testing.T) {
	root := &driveStorage{driveShared: &driveShared{}}
	root.configureStreaming("root", true)
	first := root.session().(*driveStorage)
	second := root.session().(*driveStorage)
	t.Cleanup(func() {
		first.Close()
		second.Close()
	})
	for _, child := range []*driveStorage{first, second} {
		if child.driveShared != root.driveShared {
			t.Fatal("session does not share its root's driveShared")
		}
		if child.streamCaps.Load() != nil || child.streamingEnabled() || child.streamingProperties() != nil || child.streamingFields() != "description" {
			t.Fatal("unconfigured child inherited the root's capability state")
		}
	}
	first.configureStreaming("first", false)
	second.configureStreaming("second", false)
	first.observeStreamingProperties(map[string]string{"xdriveStream": "1", "xdriveSession": "second"})
	second.observeStreamingProperties(root.streamingProperties())
	if first.streamingEnabled() || second.streamingEnabled() {
		t.Fatal("another session's reader offer enabled a child")
	}
	first.observeStreamingProperties(first.streamingProperties())
	if first.streamingEnabled() {
		t.Fatal("child's own outgoing metadata enabled its writer")
	}
	first.observeStreamingProperties(map[string]string{"xdriveStream": "1", "xdriveSession": "first"})
	if !first.streamingEnabled() || second.streamingEnabled() || !root.streamingEnabled() {
		t.Fatal("peer readiness leaked between storage instances")
	}
	if first.streamingProperties() != nil || second.streamingProperties() != nil || root.streamingProperties() == nil {
		t.Fatal("metadata advertisement leaked between storage instances")
	}
	if first.streamCaps.Load() == second.streamCaps.Load() || first.streamCaps.Load() == root.streamCaps.Load() {
		t.Fatal("configured sessions share capability pointers")
	}
	grandchild := first.session().(*driveStorage)
	t.Cleanup(func() { grandchild.Close() })
	if grandchild.streamCaps.Load() != nil || grandchild.streamingEnabled() || grandchild.streamingProperties() != nil {
		t.Fatal("nested session inherited capability state")
	}
}

func TestDriveStreamCapsLeaseMethods(t *testing.T) {
	backend := &driveStorage{}
	var storage Storage = &leasedDriveStorage{driveStorage: backend}
	caps, ok := storage.(interface {
		configureStreaming(string, bool)
		streamingEnabled() bool
		streamingProperties() map[string]string
		observeStreamingProperties(map[string]string)
		streamingFields() string
	})
	if !ok {
		t.Fatal("Drive lease does not expose the embedded capability helpers")
	}
	caps.configureStreaming("session", false)
	if caps.streamingEnabled() || caps.streamingFields() != "description,properties" {
		t.Fatal("lease did not configure the backend reader")
	}
	caps.observeStreamingProperties(caps.streamingProperties())
	if caps.streamingEnabled() || caps.streamingProperties() != nil {
		t.Fatal("lease enabled its backend from outgoing client metadata")
	}
	server := &driveStorage{}
	server.configureStreaming("session", true)
	caps.observeStreamingProperties(server.streamingProperties())
	if !caps.streamingEnabled() || !backend.streamingEnabled() {
		t.Fatal("lease did not update the backend peer capability")
	}
	if caps.streamingProperties() != nil {
		t.Fatal("confirmed client lease advertised a metadata acknowledgment")
	}
}

func TestDriveStreamCapsConcurrentConfiguration(t *testing.T) {
	s := &driveStorage{}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(peerSupports bool) {
			defer wg.Done()
			<-start
			s.configureStreaming("session", peerSupports)
		}(worker%2 == 0)
	}
	close(start)
	wg.Wait()
	caps := s.streamCaps.Load()
	if caps == nil || caps.session != "session" || !s.streamingEnabled() {
		t.Fatal("concurrent same-epoch configuration lost a peer reader offer")
	}
	advertise := caps.advertise
	s.configureStreaming("session", false)
	s.configureStreaming("session", true)
	if s.streamCaps.Load() != caps || !s.streamingEnabled() || caps.advertise != advertise || (s.streamingProperties() != nil) != advertise {
		t.Fatal("same-epoch configuration changed advertisement or revoked a confirmed offer")
	}
}

func TestDriveStreamCapsConcurrentObserveAndProperties(t *testing.T) {
	for _, tt := range []struct {
		name         string
		peerSupports bool
	}{
		{name: "client", peerSupports: false},
		{name: "accepted server", peerSupports: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := &driveStorage{}
			s.configureStreaming("session", tt.peerSupports)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for worker := 0; worker < 16; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					<-start
					for i := 0; i < 128; i++ {
						wasReady := s.streamingEnabled()
						switch worker % 4 {
						case 0:
							s.observeStreamingProperties(map[string]string{"xdriveStream": "1", "xdriveSession": "session"})
						case 1:
							s.observeStreamingProperties(map[string]string{"xdriveStream": "1", "xdriveSession": "other"})
						case 2:
							s.observeStreamingProperties(map[string]string{"xdriveStream": "2", "xdriveSession": "session"})
						case 3:
							s.configureStreaming("session", i%2 == 0)
						}
						properties := s.streamingProperties()
						if tt.peerSupports {
							if len(properties) != 2 || properties["xdriveStream"] != "1" || properties["xdriveSession"] != "session" {
								t.Errorf("concurrent server acknowledgment changed: %v", properties)
								return
							}
							properties["xdriveSession"] = "mutated"
							delete(properties, "xdriveStream")
						} else if properties != nil {
							t.Errorf("concurrent observation or reconfiguration enabled client advertisement: %v", properties)
							return
						}
						if s.streamingFields() != "description,properties" {
							t.Error("concurrent metadata fields changed")
							return
						}
						if wasReady && !s.streamingEnabled() {
							t.Error("concurrent observation revoked confirmed readiness")
							return
						}
					}
				}(worker)
			}
			close(start)
			wg.Wait()
			if !s.streamingEnabled() {
				t.Fatal("concurrent matching reader offer was lost")
			}
		})
	}
}
