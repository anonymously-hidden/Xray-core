package xdrive

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestDriveCreatedNamesNotCached(t *testing.T) {
	st := regressionDrive(roundTrip(func(*http.Request) (*http.Response, error) {
		return regressionResponse(http.StatusOK, `{"id":"created"}`), nil
	}))
	ctx := context.Background()
	if _, err := st.create(ctx, "streams/own/c2s/s0", "frame"); err != nil {
		t.Fatal(err)
	}
	if err := st.createSlot(ctx, "pinned", "streams/own/c2s/s1", "frame", nil); err != nil {
		t.Fatal(err)
	}
	if err := st.putMedia(ctx, "streams/own/c2s/s2", []byte("body")); err != nil {
		t.Fatal(err)
	}
	if len(st.ids) != 0 {
		t.Fatalf("self-created objects polluted the name cache: %d", len(st.ids))
	}
}

func TestDriveNameCacheBoundAndMiss(t *testing.T) {
	queries := 0
	st := regressionDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
		if !strings.Contains(r.URL.Query().Get("q"), "name = 'evicted'") {
			t.Errorf("unexpected cache-miss query: %s", r.URL.Query().Get("q"))
		}
		queries++
		return regressionResponse(http.StatusOK, `{"files":[{"id":"resolved","name":"evicted"}]}`), nil
	}))
	for i := range 10000 {
		st.rememberID(fmt.Sprintf("remote-%d", i), fmt.Sprintf("id-%d", i), nil)
	}
	if len(st.ids) > 4096 {
		t.Errorf("cache retained %d names, want <=4096", len(st.ids))
	}
	id, err := st.resolveID(context.Background(), "evicted")
	if err != nil || id != "resolved" {
		t.Fatalf("cache miss = %q %v", id, err)
	}
	id, err = st.resolveID(context.Background(), "evicted")
	if err != nil || id != "resolved" || queries != 1 {
		t.Fatalf("repeat resolution = %q %v, queries=%d", id, err, queries)
	}
	t.Logf("cache entries=%d; miss+hit queries=%d", len(st.ids), queries)
}
