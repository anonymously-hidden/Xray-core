package xdrive

import (
	"context"
	gotls "crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	"github.com/xtls/xray-core/transport/internet/tls"
)

func TestDriveResume35TerminalCloseCancelsAllLanes(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		entered := make(chan struct{}, 2+driveMediaLanes)
		var ended atomic.Int32
		rt := roundTrip(func(r *http.Request) (*http.Response, error) {
			entered <- struct{}{}
			<-r.Context().Done()
			ended.Add(1)
			return nil, r.Context().Err()
		})
		root := memoryDrive(rt)
		var contexts []context.Context
		pool := func() *connPool {
			return newConnPool(nil, func(ctx context.Context) *http.Client {
				contexts = append(contexts, ctx)
				return &http.Client{Transport: rt}
			})
		}
		root.conns, root.bulkConns = pool(), pool()
		for i := range root.mediaConns {
			root.mediaConns[i] = pool()
		}
		session := root.session().(*driveStorage)
		defer session.Close()
		done := make(chan error, 2+driveMediaLanes)
		go func() {
			_, _, err := session.do(context.Background(), http.MethodGet, root.filesURL, "", nil)
			done <- err
		}()
		<-entered
		go func() {
			_, _, err := session.do(context.WithValue(context.Background(), bodyKey{}, true), http.MethodGet, root.filesURL, "", nil)
			done <- err
		}()
		<-entered
		for i, client := range session.mediaClients {
			go func(i int, client *http.Client) {
				r := &driveMediaRequest{start: i, end: i + 1, size: driveMediaLanes, client: client}
				_, _, err := session.do(context.WithValue(context.Background(), driveMediaKey{}, r), http.MethodGet, root.filesURL, "", nil)
				done <- err
			}(i, client)
			<-entered
		}
		if err := root.Close(); err != nil {
			t.Fatal(err)
		}
		if ended.Load() != 2+driveMediaLanes || len(contexts) != 2+driveMediaLanes {
			t.Fatalf("terminal Close did not join all lanes: ended=%d pools=%d", ended.Load(), len(contexts))
		}
		for _, ctx := range contexts {
			if ctx.Err() != context.Canceled {
				t.Fatal("pooled connection lifetime survived root Close")
			}
		}
		for range 2 + driveMediaLanes {
			if err := <-done; err != context.Canceled {
				t.Fatal(err)
			}
		}
		root.Close()
		if _, err := session.accessToken(context.Background()); err != context.Canceled {
			t.Fatal("closed root allowed session token access")
		}
	})
}

func TestDriveResume35TerminalCloseJoinsBlockedQueues(t *testing.T) {
	for _, queue := range []string{"permit", "cooldown", "cleanup", "token", "id-flight"} {
		t.Run(queue, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				var calls atomic.Int32
				root := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
					calls.Add(1)
					<-r.Context().Done()
					return nil, r.Context().Err()
				}))
				defer root.Close()
				switch queue {
				case "permit":
					root.inflight = make(chan struct{}, 1)
					root.inflight <- struct{}{}
				case "cooldown":
					root.coolUntil = time.Now().Add(time.Hour)
				case "cleanup":
					root.cleanupInflight = make(chan struct{}, 1)
					root.cleanupInflight <- struct{}{}
				case "token":
					root.token = ""
				case "id-flight":
					root.poolFlight = &idPoolFlight{done: make(chan struct{})}
				}
				done := make(chan error, 1)
				go func() {
					if queue == "id-flight" {
						_, err := root.takeIDs(context.Background(), 1, 1)
						done <- err
						return
					}
					ctx := context.Background()
					if queue == "cleanup" {
						ctx = context.WithValue(ctx, cleanupKey{}, true)
					}
					_, _, err := root.do(ctx, http.MethodGet, root.filesURL, "", nil)
					done <- err
				}()
				synctest.Wait()
				if err := root.Close(); err != nil {
					t.Fatal(err)
				}
				if err := <-done; err != context.Canceled {
					t.Fatalf("%s queue retained foreground work: %v", queue, err)
				}
				wantCalls := int32(0)
				if queue == "token" {
					wantCalls = 1
				}
				if calls.Load() != wantCalls {
					t.Fatalf("queue %s sent %d requests", queue, calls.Load())
				}
			})
		})
	}
}

func TestDriveResume35PoolSessionReleaseRemainsReusable(t *testing.T) {
	var made atomic.Int32
	var connCtx context.Context
	pool := newConnPool(nil, func(ctx context.Context) *http.Client {
		made.Add(1)
		connCtx = ctx
		return &http.Client{Transport: okConn{}}
	})
	defer pool.Close()
	send := func(transport *sessionTransport) {
		req, err := http.NewRequest(http.MethodGet, "http://drive.invalid/files", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	first := &sessionTransport{pool: pool}
	send(first)
	first.Close()
	second := &sessionTransport{pool: pool}
	send(second)
	if made.Load() != 1 || connCtx.Err() != nil {
		t.Fatal("ordinary session Close destroyed reusable pooling")
	}
	second.Close()
	pool.Close()
	if connCtx.Err() != context.Canceled {
		t.Fatal("terminal pool Close retained context")
	}
	third := &sessionTransport{pool: pool}
	req, _ := http.NewRequest(http.MethodGet, "http://drive.invalid/files", nil)
	if _, err := third.RoundTrip(req); err != errSessionClosed || made.Load() != 1 {
		t.Fatalf("closed pool reopened: %v made=%d", err, made.Load())
	}
}

func TestDriveResume35PoolCloseCancelsRetiredInflight(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		entered := make(chan struct{})
		var made atomic.Int32
		pool := newConnPool(nil, func(context.Context) *http.Client {
			if made.Add(1) > 1 {
				return &http.Client{Transport: okConn{}}
			}
			return &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
				close(entered)
				<-r.Context().Done()
				return nil, r.Context().Err()
			})}
		})
		defer pool.Close()
		session := &sessionTransport{pool: pool}
		defer session.Close()
		req, _ := http.NewRequest(http.MethodGet, "http://drive.invalid/files", nil)
		done := make(chan error, 1)
		go func() { _, err := session.RoundTrip(req); done <- err }()
		<-entered
		session.mu.Lock()
		old := session.client.XmuxConn.(*poolConn)
		old.Close()
		session.mu.Unlock()
		resp, err := session.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if made.Load() != 2 {
			t.Fatal("retired connection did not leave selection")
		}
		pool.Close()
		if err := <-done; err != context.Canceled {
			t.Fatalf("retired request survived terminal shutdown: %v", err)
		}
	})
}

func TestDriveResume35RootCloseClosesRealIdleSockets(t *testing.T) {
	closed := make(chan struct{}, 3+driveMediaLanes)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"files":[]}`)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed <- struct{}{}
		}
	}
	server.Start()
	defer server.Close()
	settings := driveSettings()
	root, err := newDriveStorage(settings, settings.ProtocolSettings.(*Config))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	root.tokenMu.Lock()
	root.token, root.tokenExpiry = "fake", time.Now().Add(time.Hour)
	root.tokenMu.Unlock()
	root.filesURL = server.URL + "/files"
	if _, err := root.List(context.Background(), "sessions"); err != nil {
		t.Fatal(err)
	}
	session := root.session().(*driveStorage)
	defer session.Close()
	clients := append([]*http.Client{session.client, session.bulkClient}, session.mediaClients...)
	for _, client := range clients {
		req, _ := http.NewRequest(http.MethodGet, root.filesURL, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	session.Close()
	root.Close()
	for range 1 + len(clients) {
		select {
		case <-closed:
		case <-time.After(3 * time.Second):
			t.Fatal("root maintenance or ordinary/bulk/media idle socket survived Close")
		}
	}
}

func TestDriveResume35RootCloseCancelsBlockedDial(t *testing.T) {
	for _, protocol := range []string{"http", "https", "h3"} {
		t.Run(protocol, func(t *testing.T) {
			entered, ended := make(chan struct{}), make(chan struct{})
			settings := driveSettings()
			dialTCP := func(ctx context.Context, _ xnet.Destination) (net.Conn, error) {
				close(entered)
				<-ctx.Done()
				close(ended)
				return nil, ctx.Err()
			}
			dialUDP := func(ctx context.Context, _ xnet.Destination) (net.PacketConn, net.Addr, error) {
				close(entered)
				<-ctx.Done()
				close(ended)
				return nil, nil, ctx.Err()
			}
			settings.FinalMask = finalmask.NewFinalMask(nil, nil, dialTCP, nil, dialUDP, nil)
			scheme := protocol
			if protocol == "h3" {
				scheme = "https"
				settings.SecuritySettings = &tls.Config{NextProtocol: []string{"h3"}}
			}
			root, err := newDriveStorage(settings, settings.ProtocolSettings.(*Config))
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			root.tokenMu.Lock()
			root.token, root.tokenExpiry = "fake", time.Now().Add(time.Hour)
			root.tokenMu.Unlock()
			session := root.session().(*driveStorage)
			defer session.Close()
			done := make(chan error, 1)
			go func() {
				_, _, err := session.do(context.Background(), http.MethodGet, scheme+"://127.0.0.1:9/files", "", nil)
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("dial did not start")
			}
			root.Close()
			select {
			case <-ended:
			case <-time.After(3 * time.Second):
				t.Fatal("terminal owner shutdown left a detached dial blocked")
			}
			if err := <-done; err != context.Canceled {
				t.Fatalf("foreground request survived shutdown: %v", err)
			}
		})
	}
}

func TestDriveResume35RootCloseCancelsTLSHandshake(t *testing.T) {
	for _, fingerprint := range []string{"", "unsafe"} {
		t.Run(fingerprint, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			settings := driveSettings()
			settings.SecuritySettings = &tls.Config{Fingerprint: fingerprint}
			root, err := newDriveStorage(settings, settings.ProtocolSettings.(*Config))
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			root.tokenMu.Lock()
			root.token, root.tokenExpiry = "fake", time.Now().Add(time.Hour)
			root.tokenMu.Unlock()
			session := root.session().(*driveStorage)
			defer session.Close()
			done := make(chan error, 1)
			go func() {
				_, _, err := session.do(context.Background(), http.MethodGet, "https://"+ln.Addr().String()+"/files", "", nil)
				done <- err
			}()
			ln.(*net.TCPListener).SetDeadline(time.Now().Add(3 * time.Second))
			conn, err := ln.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			if _, err := conn.Read(make([]byte, 1)); err != nil {
				t.Fatal("TLS handshake did not start: ", err)
			}
			root.Close()
			if _, err := io.Copy(io.Discard, conn); err != nil {
				t.Fatal("terminal shutdown left the TLS socket open: ", err)
			}
			if err := <-done; err != context.Canceled {
				t.Fatal(err)
			}
		})
	}
}

func TestDriveResume35PoolCloseEndsHTTP2AndHTTP3(t *testing.T) {
	for _, protocol := range []string{"h2", "h3"} {
		t.Run(protocol, func(t *testing.T) {
			entered, canceled := make(chan struct{}), make(chan struct{})
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/idle" {
					fmt.Fprint(w, "idle")
					return
				}
				close(entered)
				<-r.Context().Done()
				close(canceled)
			})
			var settings *internet.MemoryStreamConfig
			var closed *atomic.Int32
			var target string
			if protocol == "h2" {
				server := newTLSServer(t, &gotls.Config{NextProtos: []string{"h2"}}, handler)
				settings, target, closed = server.settings(&tls.Config{}), server.URL, &server.closed
			} else {
				server := newH3Server(t, handler)
				settings, target, closed = server.settings(&tls.Config{}), "https://www.googleapis.com", &server.closed
			}
			pool := newConnPool(nil, func(ctx context.Context) *http.Client {
				return newServiceClient(ctx, new(sync.Map), settings, time.Minute, 8)
			})
			defer pool.Close()
			first := &sessionTransport{pool: pool}
			req, _ := http.NewRequest(http.MethodGet, target+"/idle", nil)
			resp, err := first.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			first.Close()
			want := 2
			if protocol == "h3" {
				want = 3
			}
			if resp.ProtoMajor != want || closed.Load() != 0 {
				t.Fatalf("session release broke reusable %s: proto=%s closed=%d", protocol, resp.Proto, closed.Load())
			}
			second := &sessionTransport{pool: pool}
			defer second.Close()
			done := make(chan error, 1)
			go func() {
				req, _ := http.NewRequest(http.MethodGet, target+"/blocked", nil)
				_, err := second.RoundTrip(req)
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("reused request did not arrive")
			}
			pool.Close()
			select {
			case <-canceled:
			case <-time.After(3 * time.Second):
				t.Fatal("terminal pool shutdown left a server stream active")
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("blocked request completed successfully")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("blocked pooled request survived shutdown")
			}
			deadline := time.Now().Add(3 * time.Second)
			for closed.Load() == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if closed.Load() != 1 {
				t.Fatalf("terminal %s pool left its connection open: closed=%d", protocol, closed.Load())
			}
		})
	}
}

func TestDriveResume35ConcurrentCloseAndPoolAdmission(t *testing.T) {
	for range 10 {
		pool := newConnPool(nil, func(context.Context) *http.Client { return &http.Client{Transport: okConn{}} })
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				session := &sessionTransport{pool: pool}
				defer session.Close()
				req, _ := http.NewRequest(http.MethodGet, "http://drive.invalid/files", nil)
				if resp, err := session.RoundTrip(req); err == nil {
					resp.Body.Close()
				}
			}()
		}
		wg.Add(1)
		go func() { defer wg.Done(); pool.Close() }()
		wg.Wait()
		pool.connMu.Lock()
		remaining := len(pool.conns)
		pool.connMu.Unlock()
		if remaining != 0 {
			t.Fatalf("%d connections survived concurrent terminal admission", remaining)
		}
	}
}
