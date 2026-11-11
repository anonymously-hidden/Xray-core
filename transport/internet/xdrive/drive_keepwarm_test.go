package xdrive

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func keepWarmStorage(t *testing.T, every, window time.Duration) (*driveStorage, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	var ids, tokens atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/token":
			tokens.Add(1)
			w.Write([]byte(`{"access_token":"t","expires_in":3600}`))
		default:
			ids.Add(1)
			count, _ := strconv.Atoi(r.URL.Query().Get("count"))
			out := `{"ids":[`
			for i := 0; i < count; i++ {
				if i > 0 {
					out += ","
				}
				out += `"id-` + strconv.Itoa(i) + `"`
			}
			w.Write([]byte(out + `]}`))
		}
	}))
	t.Cleanup(srv.Close)
	st := &driveStorage{
		driveShared: &driveShared{
			tokenClient: srv.Client(), tokenURL: srv.URL + "/token",
			filesURL: srv.URL + "/drive/v3/files", uploadURL: srv.URL + "/upload",
			token: "t", tokenExpiry: time.Now().Add(time.Hour),
			keepWarmEvery: every, keepWarmWindow: window,
			ids: make(map[string]string),
		},
		client:   srv.Client(),
		inflight: make(chan struct{}, 8),
		reserved: make(chan struct{}, 8),
	}
	st.driveShared.owner = st
	return st, &ids, &tokens
}

func TestKeeperKeepsPoolFresh(t *testing.T) {
	st, ids, tokens := keepWarmStorage(t, 20*time.Millisecond, time.Hour)
	st.trafficMu.Lock()
	st.lastTraffic = time.Now()
	st.lastRequest = time.Now().Add(-3 * time.Minute)
	st.trafficMu.Unlock()
	st.startKeeper()
	defer st.Close()
	deadline := time.Now().Add(2 * time.Second)
	poolFresh := false
	for time.Now().Before(deadline) {
		st.poolMu.Lock()
		n := len(st.idPool)
		poolFresh = n > 0 && time.Since(st.idPool[0].at) < idPoolTTL
		st.poolMu.Unlock()
		if poolFresh {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if ids.Load() == 0 {
		t.Fatal("keeper never refreshed the id pool")
	}
	if tokens.Load() != 0 {
		t.Fatalf("keeper fetched the token %d times despite a fresh cached token", tokens.Load())
	}
	if !poolFresh {
		t.Fatal("pool not kept fresh")
	}
}

func TestKeeperStopsOnIdleAndClose(t *testing.T) {
	st, ids, tokens := keepWarmStorage(t, 10*time.Millisecond, time.Minute)
	// Simulated long idle: no traffic within the window -> keeper stops, no requests.
	st.trafficMu.Lock()
	st.lastTraffic = time.Now().Add(-2 * time.Minute)
	st.trafficMu.Unlock()
	st.startKeeper()
	select {
	case <-st.keeperDone:
	case <-time.After(2 * time.Second):
		t.Fatal("keeper did not stop after the idle window")
	}
	if ids.Load() != 0 || tokens.Load() != 0 {
		t.Fatalf("idle keeper made requests: ids=%d tokens=%d", ids.Load(), tokens.Load())
	}
	st.Close() // must not hang or double-close
}

func TestKeeperSkipsWhileCooling(t *testing.T) {
	st, ids, _ := keepWarmStorage(t, 10*time.Millisecond, time.Hour)
	st.trafficMu.Lock()
	st.lastTraffic = time.Now()
	st.lastRequest = time.Now().Add(-3 * time.Minute)
	st.trafficMu.Unlock()
	st.coolMu.Lock()
	st.coolUntil = time.Now().Add(time.Hour)
	st.coolMu.Unlock()
	st.startKeeper()
	defer st.Close()
	time.Sleep(100 * time.Millisecond)
	if ids.Load() != 0 {
		t.Fatalf("keeper refreshed ids while cooling: %d", ids.Load())
	}
}
