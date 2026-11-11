package xdrive

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

func TestDriveBulkStallRetiresBulkOnly(t *testing.T) {
	errors.LogDebug(context.Background(), "starting fake-clock stall test")
	synctest.Test(t, func(t *testing.T) {
		var primaryMade, bulkMade, requests atomic.Int32
		primary := &sessionTransport{pool: newConnPool(nil, func(context.Context) *http.Client {
			primaryMade.Add(1)
			return &http.Client{Transport: okConn{}}
		})}
		bulk := &sessionTransport{pool: newConnPool(nil, func(context.Context) *http.Client {
			generation := bulkMade.Add(1)
			return &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/prime" || generation > 1 {
					return regressionResponse(http.StatusOK, `{"id":"done"}`), nil
				}
				requests.Add(1)
				<-r.Context().Done()
				return nil, r.Context().Err()
			})}
		})}
		defer primary.Close()
		defer bulk.Close()
		for _, transport := range []*sessionTransport{primary, bulk} {
			req, _ := http.NewRequest(http.MethodGet, "http://drive.invalid/prime", nil)
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
		}
		st := regressionDrive(primary)
		st.bulkClient = &http.Client{Transport: bulk}
		ctx := context.WithValue(context.Background(), bodyKey{}, true)
		ctx = context.WithValue(ctx, attemptKey{}, 20*time.Millisecond)
		_, _, err := st.do(ctx, http.MethodGet, "http://drive.invalid/files", "", nil)
		if err != nil {
			t.Errorf("bulk request never recovered: %v", err)
		}
		if !primary.retiredAt.IsZero() || !primary.slowAt.IsZero() {
			t.Error("bulk failure was attributed to the primary connection")
		}
		if bulk.retiredAt.IsZero() || bulkMade.Load() != 2 || primaryMade.Load() != 1 {
			t.Errorf("wrong retirement: primary=%d bulk=%d bulkRetired=%v", primaryMade.Load(), bulkMade.Load(), !bulk.retiredAt.IsZero())
		}
		start := time.Now()
		if _, _, err := st.do(context.Background(), http.MethodGet, "http://drive.invalid/files", "", nil); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != 0 || primaryMade.Load() != 1 {
			t.Error("bulk recovery delayed or reconnected the primary")
		}
		t.Logf("timed-out bulk attempts=%d; primary connections=%d; primary latency=%v", requests.Load(), primaryMade.Load(), time.Since(start))
	})
}
