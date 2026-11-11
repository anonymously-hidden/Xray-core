package xdrive

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

func TestCleanupExactNameRetriesAndSpaces(t *testing.T) {
	errors.LogDebug(context.Background(), "starting fake-clock cleanup test")
	synctest.Test(t, func(t *testing.T) {
		var at []time.Time
		st := regressionDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			if r.Method == http.MethodDelete {
				at = append(at, time.Now())
				if len(at) == 1 {
					return regressionResponse(http.StatusTooManyRequests, "limited"), nil
				}
				return regressionResponse(http.StatusNoContent, ""), nil
			}
			return regressionResponse(http.StatusOK, `{"files":[]}`), nil
		}))
		ctx := context.WithValue(context.Background(), cleanupKey{}, true)
		for i := range 3 {
			name := fmt.Sprintf("sessions/entry%d", i)
			st.ids[flatten(name)] = fmt.Sprintf("id%d", i)
			if err := st.Delete(ctx, name); err != nil {
				t.Errorf("exact cleanup was not retried: %v", err)
			}
		}
		if len(at) != 4 {
			t.Errorf("DELETE attempts=%d, want 4", len(at))
		} else {
			if gap := at[1].Sub(at[0]); gap < cleanupRetryDelay {
				t.Errorf("retry gap=%v, want >=%v", gap, cleanupRetryDelay)
			}
			for i := 2; i < len(at); i++ {
				if gap := at[i].Sub(at[i-1]); gap < time.Second/5 {
					t.Errorf("exact cleanup spacing=%v, want >=200ms", gap)
				}
			}
		}
		if st.cooling() || !st.lastThrottleAt().IsZero() || !st.lastRateLimitAt().IsZero() {
			t.Error("cleanup changed live throttle state")
		}
		t.Logf("exact cleanup attempts=%d, live cooldown=%v", len(at), st.cooling())
	})
}

type dropRetryStorage struct {
	Storage
	mu sync.Mutex
	at []time.Time
}

func (s *dropRetryStorage) Delete(ctx context.Context, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.at = append(s.at, time.Now())
	if ctx.Value(cleanupKey{}) == nil {
		return errors.New("missing cleanup context")
	}
	if len(s.at) == 1 {
		return errors.New("temporary cleanup failure")
	}
	return nil
}

func TestListenerDropRetriesFailure(t *testing.T) {
	errors.LogDebug(context.Background(), "starting fake-clock drop test")
	synctest.Test(t, func(t *testing.T) {
		st := &dropRetryStorage{}
		l := &Listener{ctx: context.Background(), storage: st}
		l.drop("sessions/entry")
		if len(st.at) != 2 {
			t.Fatalf("drop attempts=%d, want retry", len(st.at))
		}
		if st.at[1].Sub(st.at[0]) < cleanupRetryDelay {
			t.Fatal("drop retry ignored cleanup backoff")
		}
	})
}

func TestCleanupChildRetryMinimum(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var at []time.Time
		st := regressionDrive(roundTrip(func(*http.Request) (*http.Response, error) {
			at = append(at, time.Now())
			if len(at) == 1 {
				return regressionResponse(http.StatusTooManyRequests, "limited"), nil
			}
			return regressionResponse(http.StatusNoContent, ""), nil
		}))
		ctx := context.WithValue(context.Background(), cleanupKey{}, true)
		if err := st.deleteChild(ctx, "child", "id"); err != nil {
			t.Fatal(err)
		}
		if gap := at[1].Sub(at[0]); gap < cleanupRetryDelay {
			t.Fatalf("retry gap=%v, below minimum %v", gap, cleanupRetryDelay)
		}
	})
}
