package xdrive

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestSharedRefillSurvivesSessionClose(t *testing.T) {
	poolRegressionTime(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		m := &poolRegressionTransport{handle: func(r *http.Request, _, call int) (*http.Response, error) {
			if call == 1 {
				close(entered)
				<-release
				return poolRegressionResponse(http.StatusServiceUnavailable, `{}`), nil
			}
			return nil, nil
		}}
		s := poolRegressionStorage(m)
		defer s.Close()
		s.conns = newConnPool(nil, func(context.Context) *http.Client { return &http.Client{Transport: m} })
		s.bulkConns = s.conns
		seedPoolRegression(s, 6, time.Now())
		session := s.session().(*driveStorage)
		if _, err := session.takeIDs(context.Background(), 6, 3); err != nil {
			t.Fatal(err)
		}
		<-entered
		s.poolMu.Lock()
		f := s.poolFlight
		s.poolMu.Unlock()
		session.Close()
		close(release)
		<-f.done
		if f.err != nil {
			t.Fatalf("session Close killed shared refill: %v", f.err)
		}
	})
}

func TestDriveCloseJoinsOwnedWork(t *testing.T) {
	for _, mode := range []string{"token", "refresh", "keeper", "refill"} {
		t.Run(mode, func(t *testing.T) {
			poolRegressionTime(t, func(t *testing.T) {
				entered, release := make(chan struct{}), make(chan struct{})
				var exited atomic.Bool
				m := &poolRegressionTransport{handle: func(r *http.Request, _, call int) (*http.Response, error) {
					if call == 1 {
						close(entered)
					}
					select {
					case <-r.Context().Done():
						exited.Store(true)
						return nil, r.Context().Err()
					case <-release:
					}
					exited.Store(true)
					return poolRegressionResponse(http.StatusBadRequest, `{}`), nil
				}}
				s := poolRegressionStorage(m)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var waiter chan struct{}
				switch mode {
				case "token":
					s.token = ""
					waiter = make(chan struct{})
					go func() { s.accessToken(ctx); close(waiter) }()
				case "refresh":
					s.tokenLifetime = time.Hour
					s.tokenExpiry = time.Now().Add(time.Minute)
					s.accessToken(ctx)
				case "keeper":
					s.token = ""
					s.lastTraffic = time.Now()
					s.startKeeper()
				case "refill":
					seedPoolRegression(s, 6, time.Now())
					s.takeIDs(ctx, 6, 3)
				}
				<-entered
				cancel()
				if waiter != nil {
					<-waiter
				}
				s.Close()
				synctest.Wait()
				active := !exited.Load()
				close(release) // join even a broken implementation, without leaking the test fake
				synctest.Wait()
				if active {
					t.Errorf("Close left owned %s HTTP work active", mode)
				}
				before, _ := m.snapshot()
				s.accessToken(context.Background())
				s.noteTraffic()
				synctest.Wait()
				after, _ := m.snapshot()
				if len(after) != len(before) {
					t.Error("root shutdown allowed new owned work")
				}
			})
		})
	}
}

func TestColdIDReserve(t *testing.T) {
	for _, depth := range []int{2, 256} {
		poolRegressionTime(t, func(t *testing.T) {
			m := new(poolRegressionTransport)
			s := poolRegressionStorage(m)
			defer s.Close()
			n := 2 * (depth + 1)
			if ids, err := s.takeIDs(context.Background(), n, depth+1); err != nil || len(ids) != n {
				t.Fatal(len(ids), err)
			}
			synctest.Wait()
			s.poolMu.Lock()
			reserve := len(s.idPool)
			s.poolMu.Unlock()
			before, _ := m.snapshot()
			if ids, err := s.takeIDs(context.Background(), n, depth+1); err != nil || len(ids) != n {
				t.Fatal(len(ids), err)
			}
			synctest.Wait()
			after, peak := m.snapshot()
			if reserve != 3*n || len(after) != len(before) || peak != 1 {
				t.Errorf("depth %d reserve=%d requests %v -> %v peak=%d", depth, reserve, before, after, peak)
			}
			for _, count := range after {
				if count > 1000 {
					t.Error("ID batch exceeds 1000")
				}
			}
			t.Logf("depth=%d reserve=%d cold/refill requests=%v; next Dial adds %d", depth, reserve, before, len(after)-len(before))
		})
	}
}
