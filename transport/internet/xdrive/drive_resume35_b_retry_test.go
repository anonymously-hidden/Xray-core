package xdrive

import (
	"bufio"
	"bytes"
	"context"
	gotls "crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
	"github.com/apernet/quic-go/quicvarint"
	"github.com/quic-go/qpack"
	xerrors "github.com/xtls/xray-core/common/errors"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/tls"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

type driveResume35RetryBWire struct {
	mu    sync.Mutex
	at    []time.Time
	conns int
}

func (w *driveResume35RetryBWire) headers() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.at = append(w.at, time.Now())
	return len(w.at)
}

func (w *driveResume35RetryBWire) snapshot() ([]time.Time, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]time.Time(nil), w.at...), w.conns
}

func driveResume35RetryBH2(t *testing.T, first func(), silent bool) (string, *internet.MemoryStreamConfig, *driveResume35RetryBWire) {
	t.Helper()
	certServer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificates, pinned := certServer.TLS.Certificates, tls.GenerateCertHash(certServer.Certificate())
	certServer.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener = gotls.NewListener(listener, &gotls.Config{Certificates: certificates, NextProtos: []string{"h2"}})
	wire := new(driveResume35RetryBWire)
	var wg sync.WaitGroup
	var sockets []net.Conn
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			wire.mu.Lock()
			wire.conns++
			sockets = append(sockets, conn)
			wire.mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				preface := make([]byte, len(http2.ClientPreface))
				if _, err := io.ReadFull(conn, preface); err != nil || string(preface) != http2.ClientPreface {
					return
				}
				framer := http2.NewFramer(conn, conn)
				if framer.WriteSettings() != nil {
					return
				}
				for {
					frame, err := framer.ReadFrame()
					if err != nil {
						return
					}
					switch frame := frame.(type) {
					case *http2.SettingsFrame:
						if !frame.IsAck() && framer.WriteSettingsAck() != nil {
							return
						}
					case *http2.PingFrame:
						if !frame.Flags.Has(http2.FlagPingAck) && framer.WritePing(true, frame.Data) != nil {
							return
						}
					case *http2.HeadersFrame:
						if wire.headers() == 1 {
							first()
							if framer.WriteRSTStream(frame.StreamID, http2.ErrCodeRefusedStream) != nil {
								return
							}
						} else if !silent {
							var block strings.Builder
							encoder := hpack.NewEncoder(&block)
							encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "204"})
							if framer.WriteHeaders(http2.HeadersFrameParam{StreamID: frame.StreamID, BlockFragment: []byte(block.String()), EndHeaders: true, EndStream: true}) != nil {
								return
							}
						}
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		wire.mu.Lock()
		for _, conn := range sockets {
			conn.Close()
		}
		wire.mu.Unlock()
		wg.Wait()
	})
	settings := driveSettings()
	settings.SecuritySettings = &tls.Config{Fingerprint: "unsafe", NextProtocol: []string{"h2"}, PinnedPeerCertSha256: [][]byte{pinned}}
	return "https://" + listener.Addr().String() + "/files", settings, wire
}

func driveResume35RetryBRoot(t *testing.T, target string, settings *internet.MemoryStreamConfig, stall time.Duration) *driveStorage {
	t.Helper()
	root, err := newDriveStorage(settings, settings.ProtocolSettings.(*Config))
	if err != nil {
		t.Fatal(err)
	}
	root.tokenMu.Lock()
	root.token, root.tokenExpiry = "fake", time.Now().Add(time.Hour)
	root.tokenMu.Unlock()
	root.filesURL, root.cleanupRate, root.backoff = target, 5, time.Millisecond
	root.conns.Close()
	root.conns = newConnPool(nil, func(ctx context.Context) *http.Client {
		return newServiceClient(ctx, new(sync.Map), settings, stall, 8)
	})
	t.Cleanup(func() { root.Close() })
	return root
}

func driveResume35RetryBCount(root *driveStorage, kind int) int {
	root.timing.mu.Lock()
	defer root.timing.mu.Unlock()
	return root.timing.requests[kind]
}

func TestDriveResume35RetryBHTTP2ReplayAdmissionAndStall(t *testing.T) {
	for _, mode := range []string{"paced", "cooldown", "replayed-body", "stalled-after-gate"} {
		t.Run(mode, func(t *testing.T) {
			var root *driveStorage
			target, settings, wire := driveResume35RetryBH2(t, func() {
				if mode != "paced" {
					root.coolMu.Lock()
					root.coolUntil = time.Now().Add(200 * time.Millisecond)
					root.coolMu.Unlock()
				}
			}, mode == "stalled-after-gate")
			root = driveResume35RetryBRoot(t, target, settings, 100*time.Millisecond)
			session := root.session().(*driveStorage)
			defer session.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if mode == "stalled-after-gate" {
				var admissions atomic.Int32
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
				req = req.WithContext(context.WithValue(ctx, dispatchKey{}, func(ctx context.Context) error {
					if err := root.waitDispatch(ctx, false); err != nil {
						return err
					}
					admissions.Add(1)
					return nil
				}))
				began := time.Now()
				_, err := session.client.Do(req)
				if xerrors.Cause(err) != errStalled || admissions.Load() != 2 {
					t.Fatalf("post-admission stall=%v admissions=%d", err, admissions.Load())
				}
				if elapsed := time.Since(began); elapsed < 270*time.Millisecond || elapsed > 700*time.Millisecond {
					t.Fatalf("policy wait was mistaken for stall, or stall never resumed: %v", elapsed)
				}
				return
			}
			method, kind := http.MethodGet, kindOtherRead
			var body []byte
			if mode == "paced" {
				ctx = context.WithValue(ctx, cleanupKey{}, true)
				method, kind = http.MethodDelete, kindDelete
			} else if mode == "replayed-body" {
				method, kind, body = http.MethodPatch, kindSlotInline, []byte("replay this body")
			}
			status, _, err := session.do(ctx, method, target, "", body)
			if err != nil || status != http.StatusNoContent {
				t.Fatalf("status=%d err=%v", status, err)
			}
			at, conns := wire.snapshot()
			if len(at) != 2 || conns != 1 || at[1].Sub(at[0]) < 170*time.Millisecond {
				t.Fatalf("internal replay admission/reuse: times=%v connections=%d", at, conns)
			}
			if count := driveResume35RetryBCount(root, kind); count != 2 {
				t.Fatalf("wire attempts=2 counted=%d", count)
			}
		})
	}
}

func TestDriveResume35RetryBHTTP2GateCancellationKeepsConnection(t *testing.T) {
	for _, mode := range []string{"caller", "attempt-deadline", "root-close"} {
		t.Run(mode, func(t *testing.T) {
			var root *driveStorage
			target, settings, wire := driveResume35RetryBH2(t, func() {
				root.coolMu.Lock()
				root.coolUntil = time.Now().Add(time.Minute)
				root.coolMu.Unlock()
			}, false)
			root = driveResume35RetryBRoot(t, target, settings, 100*time.Millisecond)
			session := root.session().(*driveStorage)
			defer session.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "attempt-deadline" {
				deadlineCtx, deadlineCancel := context.WithTimeout(ctx, 350*time.Millisecond)
				defer deadlineCancel()
				ctx = context.WithValue(deadlineCtx, attemptKey{}, 250*time.Millisecond)
			}
			gated := make(chan struct{})
			ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GetConn: func(string) {
				at, _ := wire.snapshot()
				if len(at) == 1 {
					select {
					case <-gated:
					default:
						close(gated)
					}
				}
			}})
			done := make(chan error, 1)
			go func() { _, _, err := session.do(ctx, http.MethodGet, target, "", nil); done <- err }()
			select {
			case <-gated:
			case <-time.After(time.Second):
				t.Fatal("internal retry did not reach its admission wait")
			}
			if mode == "root-close" {
				root.Close()
			} else if mode == "caller" {
				cancel()
			}
			select {
			case err := <-done:
				if err == nil || xerrors.Cause(err) == errStalled {
					t.Fatalf("gate cancellation became success/stall: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation failed to release the gate/body permits")
			}
			at, _ := wire.snapshot()
			if len(at) != 1 || driveResume35RetryBCount(root, kindOtherRead) != 1 || len(root.inflight) != 0 {
				t.Fatalf("canceled gate dispatched/counted/leaked permit: headers=%d", len(at))
			}
			if mode == "root-close" {
				return
			}
			root.coolMu.Lock()
			root.coolUntil = time.Time{}
			root.coolMu.Unlock()
			status, _, err := session.do(context.Background(), http.MethodGet, target, "", nil)
			_, conns := wire.snapshot()
			if err != nil || status != http.StatusNoContent || conns != 1 {
				t.Fatalf("canceled H2 gate destroyed shared connection: status=%d err=%v conns=%d", status, err, conns)
			}
		})
	}
}

func TestDriveResume35RetryBHTTP1InternalReplay(t *testing.T) {
	var root *driveStorage
	var seen atomic.Int32
	var mu sync.Mutex
	var at []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := seen.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		mu.Lock()
		at = append(at, time.Now())
		mu.Unlock()
		if n == 2 {
			root.coolMu.Lock()
			root.coolUntil = time.Now().Add(200 * time.Millisecond)
			root.coolMu.Unlock()
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	root = driveResume35RetryBRoot(t, server.URL, driveSettings(), 100*time.Millisecond)
	session := root.session().(*driveStorage)
	defer session.Close()
	if _, _, err := session.do(context.Background(), http.MethodGet, server.URL, "", nil); err != nil {
		t.Fatal(err)
	}
	var outer int
	previous := testDriveIO
	testDriveIO = func(string, string, []byte) { outer++ }
	defer func() { testDriveIO = previous }()
	ctx := context.WithValue(context.Background(), cleanupKey{}, true)
	if _, _, err := session.do(ctx, http.MethodGet, server.URL, "", nil); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(at) != 2 || outer != 1 || at[1].Sub(at[0]) < 170*time.Millisecond || driveResume35RetryBCount(root, kindOtherRead) != 3 {
		t.Fatalf("H1 replay bypassed admission/accounting: %v", at)
	}
}

func TestDriveResume35RetryBHTTP1CanceledAdmissionCannotWrite(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprint(secure), func(t *testing.T) {
			var seen atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen.Add(1)
				w.WriteHeader(http.StatusNoContent)
			})
			var target string
			settings := driveSettings()
			if secure {
				server := newTLSServer(t, &gotls.Config{NextProtos: []string{"http/1.1"}}, handler)
				settings, target = server.settings(&tls.Config{}), server.URL
				settings.ProtocolSettings = driveSettings().ProtocolSettings
			} else {
				server := httptest.NewServer(handler)
				defer server.Close()
				target = server.URL
			}
			client := newServiceClient(context.Background(), new(sync.Map), settings, time.Second, 8)
			defer client.CloseIdleConnections()
			gated := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx = context.WithValue(ctx, dispatchKey{}, func(ctx context.Context) error {
				close(gated)
				<-ctx.Done()
				return ctx.Err()
			})
			done := make(chan error, 1)
			go func() {
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
				_, err := client.Do(req)
				done <- err
			}()
			select {
			case <-gated:
			case <-time.After(time.Second):
				t.Fatal("H1 admission never entered")
			}
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("canceled H1 admission succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("canceled H1 admission hung")
			}
			resp, err := client.Get(target)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if seen.Load() != 1 {
				t.Fatalf("canceled gate wrote a request: server attempts=%d", seen.Load())
			}
		})
	}
}

func driveResume35RetryBH3(t *testing.T, first func()) (string, *internet.MemoryStreamConfig, *driveResume35RetryBWire) {
	t.Helper()
	ca, _ := cert.MustGenerate(nil, cert.Authority(true), cert.KeyUsage(x509.KeyUsageCertSign))
	certificate, hash := cert.MustGenerate(ca, cert.DNSNames("www.googleapis.com"))
	key, err := x509.ParsePKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", &gotls.Config{
		Certificates: []gotls.Certificate{{Certificate: [][]byte{certificate.Certificate, ca.Certificate}, PrivateKey: key}},
		NextProtos:   []string{"h3"},
	}, &quic.Config{})
	if err != nil {
		t.Fatal(err)
	}
	wire := new(driveResume35RetryBWire)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var conns []*quic.Conn
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			wire.mu.Lock()
			wire.conns++
			conns = append(conns, conn)
			wire.mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				control, err := conn.OpenUniStreamSync(ctx)
				if err != nil {
					return
				}
				control.Write([]byte{0, 4, 0})
				for {
					stream, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					reader := bufio.NewReader(stream)
					frameType, err := quicvarint.Read(reader)
					if err != nil || frameType != 1 {
						return
					}
					length, err := quicvarint.Read(reader)
					if err != nil {
						return
					}
					if _, err := io.CopyN(io.Discard, reader, int64(length)); err != nil {
						return
					}
					if wire.headers() == 1 {
						first()
						stream.CancelRead(quic.StreamErrorCode(http3.ErrCodeRequestRejected))
						stream.CancelWrite(quic.StreamErrorCode(http3.ErrCodeRequestRejected))
					} else {
						var block bytes.Buffer
						encoder := qpack.NewEncoder(&block)
						encoder.WriteField(qpack.HeaderField{Name: ":status", Value: "204"})
						encoder.Close()
						frame := quicvarint.Append(nil, 1)
						frame = quicvarint.Append(frame, uint64(block.Len()))
						stream.Write(append(frame, block.Bytes()...))
						stream.Close()
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		cancel()
		listener.Close()
		wire.mu.Lock()
		for _, conn := range conns {
			conn.CloseWithError(0, "test complete")
		}
		wire.mu.Unlock()
		wg.Wait()
	})
	port := listener.Addr().(*net.UDPAddr).Port
	settings := driveSettings()
	settings.Destination = &xnet.Destination{Address: xnet.LocalHostIP, Port: xnet.Port(port), Network: xnet.Network_TCP}
	settings.SecuritySettings = &tls.Config{NextProtocol: []string{"h3"}, PinnedPeerCertSha256: [][]byte{hash[:]}}
	return "https://www.googleapis.com/files", settings, wire
}

func TestDriveResume35RetryBHTTP3RejectedReplayAdmission(t *testing.T) {
	for _, mode := range []string{"paced", "cooldown"} {
		t.Run(mode, func(t *testing.T) {
			var root *driveStorage
			target, settings, wire := driveResume35RetryBH3(t, func() {
				if mode == "cooldown" {
					root.coolMu.Lock()
					root.coolUntil = time.Now().Add(200 * time.Millisecond)
					root.coolMu.Unlock()
				}
			})
			root = driveResume35RetryBRoot(t, target, settings, 100*time.Millisecond)
			session := root.session().(*driveStorage)
			defer session.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			method, kind := http.MethodGet, kindOtherRead
			if mode == "paced" {
				ctx = context.WithValue(ctx, cleanupKey{}, true)
				method, kind = http.MethodDelete, kindDelete
			}
			var outer int
			previous := testDriveIO
			testDriveIO = func(string, string, []byte) { outer++ }
			defer func() { testDriveIO = previous }()
			status, _, err := session.do(ctx, method, target, "", nil)
			if err != nil || status != http.StatusNoContent {
				t.Fatalf("status=%d err=%v", status, err)
			}
			at, conns := wire.snapshot()
			if len(at) != 2 || outer != 1 || conns != 2 || at[1].Sub(at[0]) < 170*time.Millisecond || driveResume35RetryBCount(root, kind) != 2 {
				t.Fatalf("H3 internal replay bypassed admission/accounting: times=%v conns=%d outer=%d", at, conns, outer)
			}
			status, _, err = session.do(context.Background(), http.MethodGet, target, "", nil)
			_, conns = wire.snapshot()
			if err != nil || status != http.StatusNoContent || conns != 2 {
				t.Fatalf("successful H3 replay was not reusable: status=%d err=%v conns=%d", status, err, conns)
			}
		})
	}
}

func TestDriveResume35RetryBHTTP3CanceledGateCannotWrite(t *testing.T) {
	var root *driveStorage
	target, settings, wire := driveResume35RetryBH3(t, func() {
		root.coolMu.Lock()
		root.coolUntil = time.Now().Add(time.Minute)
		root.coolMu.Unlock()
	})
	root = driveResume35RetryBRoot(t, target, settings, 100*time.Millisecond)
	session := root.session().(*driveStorage)
	defer session.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gated := make(chan struct{})
	var got atomic.Int32
	ctx = context.WithValue(ctx, dispatchKey{}, func(ctx context.Context) error {
		if got.Add(1) == 2 {
			close(gated)
		}
		if err := root.waitDispatch(ctx, false); err != nil {
			return err
		}
		root.timing.request(kindOtherRead)
		return nil
	})
	done := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		_, err := session.client.Do(req)
		done <- err
	}()
	select {
	case <-gated:
	case <-time.After(time.Second):
		t.Fatal("H3 internal retry did not reach admission")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || xerrors.Cause(err) == errStalled {
			t.Fatalf("H3 canceled gate=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("H3 canceled gate hung")
	}
	at, _ := wire.snapshot()
	if len(at) != 1 || driveResume35RetryBCount(root, kindOtherRead) != 1 || len(root.inflight) != 0 {
		t.Fatalf("H3 canceled gate sent/counted/leaked permit: headers=%d", len(at))
	}
	root.coolMu.Lock()
	root.coolUntil = time.Time{}
	root.coolMu.Unlock()
	status, _, err := session.do(context.Background(), http.MethodGet, target, "", nil)
	if err != nil || status != http.StatusNoContent {
		t.Fatalf("H3 unusable after cancellation: status=%d err=%v", status, err)
	}
}
