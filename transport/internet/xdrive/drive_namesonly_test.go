package xdrive

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func lastFields(t *testing.T, drive *fakeDrive) []string {
	t.Helper()
	drive.mu.Lock()
	defer drive.mu.Unlock()
	return append([]string(nil), drive.listFields...)
}

func assertNoDescription(t *testing.T, fields []string) {
	t.Helper()
	if len(fields) == 0 {
		t.Fatal("no listing request was made")
	}
	for _, f := range fields {
		if strings.Contains(f, "description") {
			t.Fatalf("listing fields %q still pull descriptions", f)
		}
		if !strings.Contains(f, "files(id,name)") {
			t.Fatalf("listing fields %q are not names-only", f)
		}
	}
}

// resolveID and the children listing of Delete ask for names only.
func TestResolveAndDeleteListNamesOnly(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	if err := storage.Put(ctx, "streams/one/c2s/000000000.seg", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	flat := flatten("streams/one/c2s/000000000.seg")
	storage.forgetID(flat)
	drive.mu.Lock()
	drive.listFields = nil
	drive.mu.Unlock()
	if _, err := storage.resolveID(ctx, flat); err != nil {
		t.Fatalf("resolveID: %v", err)
	}
	assertNoDescription(t, lastFields(t, drive))

	drive.mu.Lock()
	drive.listFields = nil
	drive.mu.Unlock()
	if err := storage.Delete(ctx, "streams/one"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	assertNoDescription(t, lastFields(t, drive))
}

// collect()'s List(streamsDir) asks for names only.
func TestCollectListNamesOnly(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	if err := storage.Put(ctx, uplinkPrefix("live")+"/000000000.seg", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	l := &Listener{
		ctx:       ctx,
		storage:   storage,
		params:    paramsFromConfig(driveSettings().ProtocolSettings.(*Config)),
		active:    map[string]bool{"live": true},
		handled:   make(map[string]time.Time),
		idleSince: make(map[string]time.Time),
	}
	drive.mu.Lock()
	drive.listFields = nil
	drive.mu.Unlock()
	if err := l.collect(); err != nil {
		t.Fatalf("collect: %v", err)
	}
	// the session is active, so collect only lists streams and deletes nothing
	assertNoDescription(t, lastFields(t, drive))
}

// The accept-loop listing of sessions keeps descriptions and Entry.Inline.
func TestAcceptListingKeepsDescriptions(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	payload := []byte("the announcement rides along with the listing")
	if err := storage.Put(ctx, announceName("sess", time.Now()), payload); err != nil {
		t.Fatalf("Put: %v", err)
	}
	drive.mu.Lock()
	drive.listFields = nil
	drive.mu.Unlock()

	entries, err := storage.List(ctx, sessionsDir)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 || string(entries[0].Inline) != string(payload) {
		t.Fatalf("sessions listing lost Inline: %+v", entries)
	}
	fields := lastFields(t, drive)
	if len(fields) == 0 || !strings.Contains(fields[len(fields)-1], "description") {
		t.Fatalf("accept listing fields %v, want descriptions", fields)
	}
}

// ids are remembered in both names-only and full mode.
func TestNamesOnlyRemembersIDs(t *testing.T) {
	newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	if err := storage.Put(ctx, "streams/abc/c2s/000000000.seg", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	flat := flatten("streams/abc/c2s/000000000.seg")

	storage.forgetID(flat)
	if _, err := storage.resolveID(ctx, flat); err != nil {
		t.Fatalf("resolveID: %v", err)
	}
	if id, ok := storage.cachedID(flat); !ok || id == "" {
		t.Fatal("names-only resolve did not remember the id")
	}

	storage.forgetID(flat)
	if _, err := storage.List(ctx, "streams/abc/c2s"); err != nil {
		t.Fatalf("List: %v", err)
	}
	if id, ok := storage.cachedID(flat); !ok || id == "" {
		t.Fatal("full listing did not remember the id")
	}
}

// names-only paging follows nextPageToken and keeps ids across pages.
func TestNamesOnlyPaging(t *testing.T) {
	var mu sync.Mutex
	var fields []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			w.Write([]byte(`{"access_token":"t","expires_in":3600}`))
			return
		}
		mu.Lock()
		fields = append(fields, r.URL.Query().Get("fields"))
		mu.Unlock()
		if r.URL.Query().Get("pageToken") == "" {
			w.Write([]byte(`{"nextPageToken":"p2","files":[{"id":"id-1","name":"streams~abc~c2s~000000000.seg"}]}`))
			return
		}
		w.Write([]byte(`{"files":[{"id":"id-2","name":"streams~abc~c2s~000000001.seg"}]}`))
	}))
	defer srv.Close()

	st := &driveStorage{
		driveShared: &driveShared{
			tokenClient: srv.Client(), tokenURL: srv.URL + "/token",
			filesURL: srv.URL + "/files", uploadURL: srv.URL + "/upload",
			token: "t", tokenExpiry: time.Now().Add(time.Hour),
			folder: "f", ids: make(map[string]string),
		},
		client:   srv.Client(),
		inflight: make(chan struct{}, 8),
		reserved: make(chan struct{}, 8),
	}
	ctx := context.WithValue(context.Background(), namesOnlyKey{}, true)
	found, err := st.query(ctx, "name contains 'streams~abc~c2s~'")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(found) != 2 {
		t.Fatalf("paging returned %d files, want 2", len(found))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(fields) != 2 {
		t.Fatalf("made %d listing requests, want 2 pages", len(fields))
	}
	for _, f := range fields {
		if strings.Contains(f, "description") {
			t.Fatalf("paged fields %q carry description", f)
		}
	}
	if id, ok := st.cachedID("streams~abc~c2s~000000001.seg"); !ok || id != "id-2" {
		t.Fatalf("page 2 id not remembered: %q %v", id, ok)
	}
}
