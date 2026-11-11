package xdrive

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestKeeperRegressionMaintenanceIsNotTraffic(t *testing.T) {
	poolRegressionTime(t, func(t *testing.T) {
		m := new(poolRegressionTransport)
		s := poolRegressionStorage(m)
		s.keepWarmEvery = 4 * time.Minute
		s.keepWarmWindow = 10 * time.Minute
		start := time.Now()
		s.lastTraffic = start
		s.lastRequest = start.Add(-3 * time.Minute)
		s.startKeeper()
		defer s.Close()
		synctest.Wait()
		time.Sleep(20 * time.Minute)
		synctest.Wait()
		s.trafficMu.Lock()
		traffic, request := s.lastTraffic, s.lastRequest
		s.trafficMu.Unlock()
		counts, _ := m.snapshot()
		stopped := false
		select {
		case <-s.keeperDone:
			stopped = true
		default:
		}
		t.Logf("idle horizon=20m requests=%v lastTraffic=%s lastRequest=%s stopped=%v", counts, traffic.Sub(start), request.Sub(start), stopped)
		if !traffic.Equal(start) || !stopped || len(counts) != 1 || !request.Equal(start.Add(4*time.Minute)) {
			t.Error("maintenance must record requests without extending user activity or maintaining after idle exit")
		}
	})
}

func TestKeeperRegressionConfiguredPoolBound(t *testing.T) {
	for _, depth := range []int{2, 256} {
		t.Run(strconv.Itoa(depth), func(t *testing.T) {
			poolRegressionTime(t, func(t *testing.T) {
				s, err := newDriveStorage(nil, &Config{RemoteFolder: "fake-folder", Secrets: []string{"fake", "fake", "fake"}, Concurrency: uint32(depth)})
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				s.timing = nil
				m := new(poolRegressionTransport)
				client := &http.Client{Transport: m}
				s.tokenMu.Lock()
				s.trafficMu.Lock()
				s.client, s.bulkClient, s.tokenClient = client, client, client
				s.conns = newConnPool(nil, func(context.Context) *http.Client { return client })
				s.bulkConns = s.conns
				s.token, s.tokenExpiry = "fake-token", time.Now().Add(24*time.Hour)
				s.filesURL = "https://memory.invalid/files"
				s.lastTraffic, s.lastRequest = time.Now(), time.Now().Add(-3*time.Minute)
				s.trafficMu.Unlock()
				s.tokenMu.Unlock()
				target := 3 * 2 * (depth + 1)
				seedPoolRegression(s, target, time.Now().Add(-idPoolTTL))
				synctest.Wait()
				maxPool, expired := 0, 0
				for range 12 {
					time.Sleep(4 * time.Minute)
					synctest.Wait()
					s.poolMu.Lock()
					maxPool = max(maxPool, len(s.idPool))
					for _, e := range s.idPool {
						if time.Since(e.at) >= idPoolTTL {
							expired++
						}
					}
					s.poolMu.Unlock()
				}
				counts, _ := m.snapshot()
				t.Logf("depth=%d horizon=48m requests=%v maxPool=%d target=%d expired=%d", depth, counts, maxPool, target, expired)
				if maxPool > target || expired != 0 || len(counts) == 0 || counts[0] != min(target, 1000) {
					t.Error("keeper must prune before testing age/low, use configured rings and cap stored IDs")
				}
				for _, count := range counts {
					if count > 1000 {
						t.Errorf("oversized generate request: %d", count)
					}
				}
			})
		})
	}
}

func TestKeeperRegressionRestartAndPermanentClose(t *testing.T) {
	for _, restart := range []string{"traffic", "session"} {
		t.Run(restart, func(t *testing.T) {
			poolRegressionTime(t, func(t *testing.T) {
				m := new(poolRegressionTransport)
				s := poolRegressionStorage(m)
				s.keepWarmEvery, s.keepWarmWindow = time.Minute, 2*time.Minute
				s.lastTraffic = time.Now().Add(-3 * time.Minute)
				s.startKeeper()
				synctest.Wait()
				time.Sleep(time.Minute)
				synctest.Wait()
				oldDone := s.keeperDone
				select {
				case <-oldDone:
				default:
					t.Fatal("initial idle keeper did not exit")
				}
				if restart == "traffic" {
					s.noteTraffic()
				} else {
					session := s.session()
					session.Close()
				}
				synctest.Wait()
				restarted := s.keeperDone != oldDone
				var wg sync.WaitGroup
				for range 12 {
					wg.Go(func() { s.Close() })
					wg.Go(func() { s.noteTraffic(); s.startKeeper() })
				}
				wg.Wait()
				synctest.Wait()
				done := s.keeperDone
				s.noteTraffic()
				s.startKeeper()
				synctest.Wait()
				time.Sleep(4 * time.Minute)
				synctest.Wait()
				counts, _ := m.snapshot()
				t.Logf("restart=%s restarted=%v post-close requests=%v", restart, restarted, counts)
				if !restarted || s.keeperDone != done || len(counts) != 0 {
					t.Error("traffic/session must restart an idle keeper, but never a closed keeper")
				}
			})
		})
	}
}

func TestKeeperRegressionCloseCancelsMaintenance(t *testing.T) {
	poolRegressionTime(t, func(t *testing.T) {
		m := &poolRegressionTransport{handle: func(r *http.Request, _, _ int) (*http.Response, error) {
			<-r.Context().Done()
			return nil, r.Context().Err()
		}}
		s := poolRegressionStorage(m)
		s.lastTraffic = time.Now()
		s.lastRequest = time.Now().Add(-3 * time.Minute)
		s.startKeeper()
		synctest.Wait()
		time.Sleep(4 * time.Minute)
		synctest.Wait()
		start := time.Now()
		s.Close()
		elapsed := time.Since(start)
		t.Logf("Close during in-flight maintenance elapsed=%s", elapsed)
		if elapsed != 0 {
			t.Error("Close must cancel in-flight maintenance without waiting for its timeout")
		}
	})
}

func TestKeeperRegressionRecentRequestAndCooldown(t *testing.T) {
	poolRegressionTime(t, func(t *testing.T) {
		m := new(poolRegressionTransport)
		s := poolRegressionStorage(m)
		s.keepWarmEvery = time.Minute
		s.lastTraffic, s.lastRequest = time.Now(), time.Now()
		s.coolUntil = time.Now().Add(4 * time.Minute)
		s.startKeeper()
		defer s.Close()
		synctest.Wait()
		time.Sleep(3 * time.Minute)
		synctest.Wait()
		counts, _ := m.snapshot()
		if len(counts) != 0 {
			t.Errorf("maintenance ran while cooling: %v", counts)
		}
		if _, _, err := s.do(context.Background(), http.MethodGet, s.filesURL+"/generateIds?count=1", "", nil); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		before, _ := m.snapshot()
		time.Sleep(time.Minute)
		synctest.Wait()
		after, _ := m.snapshot()
		t.Logf("recent request horizon=1m before=%d after=%d", len(before), len(after))
		if len(after) != len(before) {
			t.Error("keeper must not maintain immediately after a user request")
		}
	})
}
