package xdrive

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func regressionResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func regressionDrive(rt http.RoundTripper) *driveStorage {
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

func TestDriveRefreshInitiatorCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var requests atomic.Int32
		st := regressionDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			requests.Add(1)
			close(entered)
			<-release
			return regressionResponse(http.StatusOK, `{"access_token":"refreshed","expires_in":3600}`), nil
		}))
		st.token = ""
		ctx, cancel := context.WithCancel(context.Background())
		leader := make(chan error, 1)
		go func() { _, err := st.accessToken(ctx); leader <- err }()
		<-entered
		waiter := make(chan string, 1)
		go func() { token, _ := st.accessToken(context.Background()); waiter <- token }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case err := <-leader:
			if err != context.Canceled {
				t.Errorf("initiator error = %v, want cancellation", err)
			}
		default:
			t.Error("initiator remains blocked after its context was canceled")
		}
		close(release)
		if token := <-waiter; token != "refreshed" {
			t.Fatalf("shared refresh returned %q", token)
		}
		synctest.Wait()
		start := time.Now()
		if _, err := st.accessToken(context.Background()); err != nil {
			t.Fatal(err)
		}
		if requests.Load() != 1 || time.Since(start) != 0 {
			t.Fatalf("cached token adds work: requests=%d elapsed=%v", requests.Load(), time.Since(start))
		}
		t.Logf("refresh requests=%d; cached-call virtual latency=%v", requests.Load(), time.Since(start))
	})
}
