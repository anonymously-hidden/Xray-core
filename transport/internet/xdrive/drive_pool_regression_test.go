package xdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
)

func poolRegressionTime(t *testing.T, test func(*testing.T)) {
	t.Helper()
	// The global async logger's channels belong outside the fake-clock bubble.
	log.Record(&log.GeneralMessage{Severity: log.Severity_Debug, Content: t.Name()})
	synctest.Test(t, test)
}

type poolRegressionTransport struct {
	mu     sync.Mutex
	counts []int
	active int
	peak   int
	next   int
	handle func(*http.Request, int, int) (*http.Response, error)
}

func poolRegressionResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func (m *poolRegressionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	n, _ := strconv.Atoi(r.URL.Query().Get("count"))
	m.mu.Lock()
	m.counts = append(m.counts, n)
	call := len(m.counts)
	m.active++
	m.peak = max(m.peak, m.active)
	m.next += n
	first := m.next - n
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.active--
		m.mu.Unlock()
	}()
	if m.handle != nil {
		if resp, err := m.handle(r, n, call); resp != nil || err != nil {
			return resp, err
		}
	}
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("generated-%d", first+i)
	}
	b, _ := json.Marshal(map[string]any{"ids": ids})
	return poolRegressionResponse(http.StatusOK, string(b)), nil
}

func (m *poolRegressionTransport) snapshot() ([]int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int(nil), m.counts...), m.peak
}

func poolRegressionStorage(m *poolRegressionTransport) *driveStorage {
	client := &http.Client{Transport: m}
	s := &driveStorage{
		driveShared: &driveShared{
			filesURL: "https://memory.invalid/files", uploadURL: "https://memory.invalid/upload",
			tokenURL: "https://memory.invalid/token", tokenClient: client,
			token: "test-token", tokenExpiry: time.Now().Add(24 * time.Hour),
			ids: make(map[string]string), keepWarmEvery: 4 * time.Minute, keepWarmWindow: 3 * time.Hour,
		},
		client: client, bulkClient: client, inflight: make(chan struct{}, 64), reserved: make(chan struct{}, 8),
	}
	s.owner = s
	return s
}

func seedPoolRegression(s *driveStorage, n int, at time.Time) {
	s.poolMu.Lock()
	defer s.poolMu.Unlock()
	for i := 0; i < n; i++ {
		s.idPool = append(s.idPool, idPoolEntry{id: fmt.Sprintf("seed-%d", i), at: at})
	}
}

func TestPoolRegressionShortfallAndColdCounts(t *testing.T) {
	for _, depth := range []int{2, 256} {
		for _, available := range []int{0, 2} {
			t.Run(fmt.Sprintf("depth%d/available%d", depth, available), func(t *testing.T) {
				poolRegressionTime(t, func(t *testing.T) {
					m := new(poolRegressionTransport)
					s := poolRegressionStorage(m)
					defer s.Close()
					n := 2 * (depth + 1)
					seedPoolRegression(s, available, time.Now())
					start := time.Now()
					ids, err := s.takeIDs(context.Background(), n, depth+1)
					synctest.Wait()
					counts, peak := m.snapshot()
					t.Logf("depth=%d available=%d returned=%d generate=%v peak=%d elapsed=%s", depth, available, len(ids), counts, peak, time.Since(start))
					if err != nil || len(ids) != n {
						t.Errorf("all-or-nothing: got %d IDs, err=%v; want %d", len(ids), err, n)
					}
					if len(counts) < 2 || counts[0] != n-available || counts[1] != min(3*n, 1000) {
						t.Errorf("generate requests=%v; want foreground shortfall %d then background reserve", counts, n-available)
					}
				})
			})
		}
	}
}

func TestPoolRegressionRollbackAndTTL(t *testing.T) {
	for _, delay := range []time.Duration{0, idPoolTTL} {
		t.Run(delay.String(), func(t *testing.T) {
			poolRegressionTime(t, func(t *testing.T) {
				m := &poolRegressionTransport{handle: func(r *http.Request, _, _ int) (*http.Response, error) {
					time.Sleep(delay)
					return poolRegressionResponse(http.StatusBadRequest, `{}`), nil
				}}
				s := poolRegressionStorage(m)
				s.owner = nil
				defer s.Close()
				seedPoolRegression(s, 2, time.Now())
				start := time.Now()
				ids, err := s.takeIDs(context.Background(), 6, 3)
				synctest.Wait()
				time.Sleep(max(delay-time.Since(start), 0))
				synctest.Wait()
				s.poolMu.Lock()
				left := len(s.idPool)
				s.poolMu.Unlock()
				t.Logf("failure delay=%s returned=%d rollback=%d error=%v", delay, len(ids), left, err)
				want := 2
				if delay == idPoolTTL {
					want = 0
				}
				if err == nil || len(ids) != 0 || left != want {
					t.Errorf("failed allocation must return nothing and restore %d fresh IDs", want)
				}
			})
		})
	}
}

func TestPoolRegressionWithdrawalRefillSingleflight(t *testing.T) {
	for _, depth := range []int{2, 256} {
		t.Run(strconv.Itoa(depth), func(t *testing.T) {
			poolRegressionTime(t, func(t *testing.T) {
				gate := make(chan struct{})
				m := &poolRegressionTransport{handle: func(r *http.Request, _, _ int) (*http.Response, error) {
					select {
					case <-gate:
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}
					return nil, nil
				}}
				s := poolRegressionStorage(m)
				defer s.Close()
				n := 2 * (depth + 1)
				seedPoolRegression(s, 3*n, time.Now())
				start := time.Now()
				for range 3 {
					ids, err := s.takeIDs(context.Background(), n, depth+1)
					if err != nil || len(ids) != n {
						t.Errorf("warm withdrawal=%d, %v", len(ids), err)
					}
					synctest.Wait()
				}
				counts, peak := m.snapshot()
				close(gate)
				synctest.Wait()
				s.poolMu.Lock()
				left := len(s.idPool)
				s.poolMu.Unlock()
				t.Logf("depth=%d warm dials=3 elapsed=%s refill=%v peak=%d reserve=%d cap=%d", depth, time.Since(start), counts, peak, left, 3*n)
				if len(counts) != 1 || counts[0] != min(2*n, 1000) || peak != 1 || left > 3*n {
					t.Errorf("expected one post-withdrawal refill, bounded by 1000/request and three rings")
				}
			})
		})
	}
}

func TestPoolRegressionConcurrentColdDials(t *testing.T) {
	poolRegressionTime(t, func(t *testing.T) {
		gate := make(chan struct{})
		m := &poolRegressionTransport{handle: func(r *http.Request, _, _ int) (*http.Response, error) {
			<-gate
			return nil, nil
		}}
		s := poolRegressionStorage(m)
		defer s.Close()
		var wg sync.WaitGroup
		var mu sync.Mutex
		seen := make(map[string]bool)
		for range 8 {
			wg.Go(func() {
				ids, err := s.takeIDs(context.Background(), 6, 3)
				if err != nil || len(ids) != 6 {
					t.Errorf("concurrent allocation=%d, %v", len(ids), err)
				}
				mu.Lock()
				defer mu.Unlock()
				for _, id := range ids {
					if seen[id] {
						t.Errorf("duplicate allocation %q", id)
					}
					seen[id] = true
				}
			})
		}
		synctest.Wait()
		_, peak := m.snapshot()
		close(gate)
		wg.Wait()
		synctest.Wait()
		counts, _ := m.snapshot()
		t.Logf("cold dials=8 generate=%v peak=%d unique=%d", counts, peak, len(seen))
		if peak != 1 || len(counts) >= 8 || len(seen) != 48 {
			t.Errorf("single-flight reserve must amortize cold allocations without duplicate IDs")
		}
	})
}

type poolRegressionDialStore struct {
	*memStorage
	pool  *driveStorage
	eager int
}

func (s *poolRegressionDialStore) takeIDs(ctx context.Context, n, worth int) ([]string, error) {
	return s.pool.takeIDs(ctx, n, worth)
}

func (s *poolRegressionDialStore) waitCool(ctx context.Context) error { return s.pool.waitCool(ctx) }

func (s *poolRegressionDialStore) makeSlot(ctx context.Context, name string) (string, error) {
	s.memStorage.mu.Lock()
	s.eager++
	s.memStorage.mu.Unlock()
	return s.memSlots.makeSlot(ctx, name)
}

func TestPoolRegressionDialCooldownRetry(t *testing.T) {
	poolRegressionTime(t, func(t *testing.T) {
		m := new(poolRegressionTransport)
		s := poolRegressionStorage(m)
		defer s.Close()
		m.handle = func(r *http.Request, _, call int) (*http.Response, error) {
			if call == 1 {
				s.coolMu.Lock()
				s.coolUntil = time.Now().Add(5 * time.Second)
				s.coolMu.Unlock()
				return poolRegressionResponse(http.StatusBadRequest, `{}`), nil
			}
			return nil, nil
		}
		store := &poolRegressionDialStore{memStorage: &memStorage{memSlots: newMemSlots(), files: make(map[string][]byte)}, pool: s}
		factory := func(*Config) (Storage, error) { return store, nil }
		testStorageFactory.Store(&factory)
		defer testStorageFactory.Store(nil)
		config := &Config{Concurrency: 2}
		box := spareBoxFor(config)
		box.stop.Store(true)
		defer spareBoxes.Delete(config)
		start := time.Now()
		conn, err := Dial(context.Background(), net.Destination{}, &internet.MemoryStreamConfig{ProtocolSettings: config})
		elapsed := time.Since(start)
		if err != nil {
			t.Fatal(err)
		}
		conn.Close()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		counts, _ := m.snapshot()
		store.memStorage.mu.Lock()
		eager := store.eager
		store.memStorage.mu.Unlock()
		t.Logf("Dial cooldown=%s generates=%v eager creates=%d", elapsed, counts, eager)
		if elapsed != 5*time.Second || len(counts) != 3 || counts[0] != 6 || counts[1] != 6 || counts[2] != 18 || eager != 0 {
			t.Error("Dial must await shared cooldown, retry IDs once, and avoid eager fanout on recovery")
		}
	})
}

func TestPoolRegressionSlotRequestGuardrails(t *testing.T) {
	for _, size := range []int{1 << 10, 14 << 10} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			poolRegressionTime(t, func(t *testing.T) {
				data := bytes.Repeat([]byte("x"), size)
				frame := newSlotFrame(1, slotInline, data)
				var body []byte
				if size > slotInlineLimit {
					frame = newSlotFrame(1, slotMedia, data)
					body = data
				}
				m := &poolRegressionTransport{handle: func(r *http.Request, _, _ int) (*http.Response, error) {
					if r.Method == http.MethodGet {
						if r.URL.Query().Get("alt") == "media" {
							return poolRegressionResponse(http.StatusOK, string(data)), nil
						}
						b, _ := json.Marshal(map[string]string{"description": frame.encode()})
						return poolRegressionResponse(http.StatusOK, string(b)), nil
					}
					return poolRegressionResponse(http.StatusOK, `{"id":"slot"}`), nil
				}}
				s := poolRegressionStorage(m)
				defer s.Close()
				start := time.Now()
				if err := s.createSlot(context.Background(), "slot", "slot", frame.encode(), body); err != nil {
					t.Fatal(err)
				}
				desc, err := s.readDescription(context.Background(), "slot")
				if err != nil || desc != frame.encode() {
					t.Fatalf("description=%q, err=%v", desc, err)
				}
				wantCalls := 2
				if body != nil {
					got, err := s.readBody(context.Background(), "slot")
					if err != nil || !bytes.Equal(got, data) {
						t.Fatal("media exchange corrupted")
					}
					wantCalls++
				}
				counts, _ := m.snapshot()
				t.Logf("slot bytes=%d requests=%d elapsed=%s", size, len(counts), time.Since(start))
				if len(counts) != wantCalls {
					t.Fatalf("extra slot requests: %d, want %d", len(counts), wantCalls)
				}
			})
		})
	}
}
