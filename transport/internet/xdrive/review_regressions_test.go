package xdrive

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/tls"
)

type reviewWriter struct{}

func (reviewWriter) Write(p []byte) (int, error) { return len(p), nil }
func (reviewWriter) Close() error                { return nil }
func (reviewWriter) writeUntil(p []byte, _, _ <-chan struct{}) (int, error) {
	return len(p), nil
}

func TestReviewBufferedReadState(t *testing.T) {
	for _, state := range []string{"deadline", "closed"} {
		t.Run(state, func(t *testing.T) {
			c := &Conn{writer: reviewWriter{}, cancel: func() {}, readBuf: []byte("unread"),
				readDeadline: newPipeDeadline(), writeDeadline: newPipeDeadline(), closing: make(chan struct{})}
			defer c.Close()
			want := os.ErrDeadlineExceeded
			if state == "deadline" {
				c.SetReadDeadline(time.Now().Add(-time.Second))
			} else {
				c.Close()
				want = io.ErrClosedPipe
			}
			n, err := c.Read(make([]byte, 1))
			if n != 0 || !errors.Is(err, want) {
				t.Fatalf("buffered Read after %s = (%d, %v), want (0, %v)", state, n, err, want)
			}
		})
	}
}

func TestReviewDeadlineStopCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newPipeDeadline()
		entered, release := make(chan struct{}), make(chan struct{})
		panicked := make(chan any, 1)
		cancel := d.cancel
		// Model the real callback being descheduled immediately before close(cancel).
		d.timer = time.AfterFunc(time.Millisecond, func() {
			close(entered)
			<-release
			defer func() { panicked <- recover() }()
			close(cancel)
		})
		<-entered
		d.stop()
		d.set(time.Now().Add(-time.Second))
		close(release)
		if p := <-panicked; p != nil {
			t.Fatalf("callback panics after stop followed by set: %v", p)
		}
	})
}

func TestReviewStorageProfileSeparation(t *testing.T) {
	changes := map[string]func(*internet.MemoryStreamConfig){
		"tls": func(s *internet.MemoryStreamConfig) {
			s.SecuritySettings.(*tls.Config).ServerName = "different.invalid"
		},
		"socket": func(s *internet.MemoryStreamConfig) { s.SocketSettings.DialerProxy = "different-outbound" },
		"quic":   func(s *internet.MemoryStreamConfig) { s.QuicParams.MaxIdleTimeout++ },
		"xmux":   func(s *internet.MemoryStreamConfig) { s.ProtocolSettings.(*Config).Xmux.HKeepAlivePeriod++ },
		"template": func(s *internet.MemoryStreamConfig) {
			s.ProtocolSettings.(*Config).Template = `{"auth":{"type":"different"}}`
		},
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			a := keyRegressionSettings()
			b := cloneKeyRegressionSettings(a)
			change(b)
			if shareKey(a, a.ProtocolSettings.(*Config)) == shareKey(b, b.ProtocolSettings.(*Config)) {
				t.Fatalf("different %s settings reuse the same storage key", name)
			}
		})
	}
}

func TestReviewColdPoolReserve(t *testing.T) {
	poolRegressionTime(t, func(t *testing.T) {
		m := new(poolRegressionTransport)
		s := poolRegressionStorage(m)
		defer s.Close()
		ids, err := s.takeIDs(context.Background(), 6, 3)
		if err != nil || len(ids) != 6 {
			t.Fatal(len(ids), err)
		}
		synctest.Wait()
		s.poolMu.Lock()
		reserve := len(s.idPool)
		s.poolMu.Unlock()
		before, _ := m.snapshot()
		ids, err = s.takeIDs(context.Background(), 6, 3)
		if err != nil || len(ids) != 6 {
			t.Fatal(len(ids), err)
		}
		synctest.Wait()
		after, _ := m.snapshot()
		if reserve == 0 || len(after) != len(before) {
			t.Fatalf("cold take leaves reserve=%d; generate calls before/after immediate next take: %v -> %v", reserve, before, after)
		}
	})
}

func TestReviewSharedRefillSurvivesSessionClose(t *testing.T) {
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
		_, err := session.takeIDs(context.Background(), 6, 3)
		if err != nil {
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
			t.Fatalf("session Close killed the shared refill: %v", f.err)
		}
	})
}

func TestReviewDeleteStaleDuplicateCache(t *testing.T) {
	var deleted []string
	s := regressionDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodDelete {
			deleted = append(deleted, strings.TrimPrefix(r.URL.Path, "/files/"))
			return regressionResponse(http.StatusNoContent, ""), nil
		}
		if strings.Contains(r.URL.Query().Get("q"), "name = 'sessions~a'") {
			return regressionResponse(http.StatusOK, `{"files":[{"id":"a1","name":"sessions~a"},{"id":"a2","name":"sessions~a"}]}`), nil
		}
		return regressionResponse(http.StatusOK, `{"files":[]}`), nil
	}))
	s.rememberID("sessions~a", "a1", nil)
	if err := s.Delete(context.Background(), "sessions/a"); err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 2 {
		t.Fatalf("Delete reported success but deleted only %v; current listing has a1 and a2", deleted)
	}
}

func TestReviewKeeperCloseJoinsTokenFetch(t *testing.T) {
	poolRegressionTime(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var exited atomic.Bool
		m := &poolRegressionTransport{handle: func(r *http.Request, _, _ int) (*http.Response, error) {
			close(entered)
			select {
			case <-r.Context().Done():
			case <-release:
			}
			exited.Store(true)
			return nil, context.Canceled
		}}
		s := poolRegressionStorage(m)
		s.token = ""
		s.lastTraffic = time.Now()
		s.startKeeper()
		<-entered
		s.Close()
		synctest.Wait()
		activeAfterClose := !exited.Load()
		close(release)
		synctest.Wait()
		if activeAfterClose {
			t.Error("keeper closed while its detached token HTTP fetch remains active")
		}
	})
}
