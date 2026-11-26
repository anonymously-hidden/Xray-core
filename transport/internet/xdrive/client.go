package xdrive

import (
	"cmp"
	"context"
	gotls "crypto/tls"
	"io"
	"net/http"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/net/cnc"
	"github.com/xtls/xray-core/common/utils"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion/bbr"
	"github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/tls"
)

var errStalled = errors.New("the request made no progress")

type serviceTransport struct {
	plain  *http.Transport
	secure interface {
		http.RoundTripper
		CloseIdleConnections()
	}
	stall time.Duration
}

// Cancel stalled requests, not long transfers that continue to make progress.
func (t *serviceTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	var next http.RoundTripper = t.plain
	if r.URL.Scheme == "https" {
		next = t.secure
	}
	ctx, cancel := context.WithCancelCause(r.Context())
	var mu sync.Mutex
	rescue, _ := ctx.Value(spanRescueKey{}).(func() bool)
	gap := t.stall
	if rescue != nil {
		gap = min(gap, 5*time.Second) // Includes the network wait for headers; admission and pacing precede RoundTrip.
	}
	last := time.Now()
	var timer *time.Timer
	mu.Lock()
	timer = time.AfterFunc(gap, func() {
		mu.Lock()
		defer mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		idle := time.Since(last)
		if idle >= t.stall || idle >= gap && rescue != nil && rescue() {
			cancel(errStalled)
			return
		}
		// Read activity only updates a timestamp, rather than rearming a timer per chunk.
		if idle < gap {
			timer.Reset(gap - idle)
		} else {
			timer.Reset(min(gap, t.stall-idle))
		}
	})
	mu.Unlock()
	alive := func(ordinary bool) {
		mu.Lock()
		if ordinary {
			gap = t.stall
		}
		last = time.Now()
		mu.Unlock()
	}
	end := func() {
		mu.Lock()
		defer mu.Unlock()
		timer.Stop()
		cancel(nil)
	}
	r = r.WithContext(ctx)
	// Clone before setting defaults: the caller owns the original headers.
	r.Header = r.Header.Clone()
	utils.TryDefaultHeadersWith(r.Header, "fetch")
	upload := func(body io.ReadCloser) io.ReadCloser {
		if body == nil || body == http.NoBody {
			return body
		}
		return &progressBody{ReadCloser: body, alive: alive}
	}
	r.Body = upload(r.Body)
	// Rewinds performed by the transport must also reset the stall timer.
	if getBody := r.GetBody; getBody != nil {
		r.GetBody = func() (io.ReadCloser, error) {
			body, err := getBody()
			return upload(body), err
		}
	}

	resp, err := next.RoundTrip(r)
	if err != nil {
		err = stalled(ctx, err)
		end()
		return nil, err
	}
	alive(resp.StatusCode != http.StatusPartialContent)
	resp.Body = &progressBody{ReadCloser: resp.Body, alive: alive, end: end, ctx: ctx}
	return resp, nil
}

func (t *serviceTransport) CloseIdleConnections() {
	t.plain.CloseIdleConnections()
	t.secure.CloseIdleConnections()
}

// HTTP/2 can report the stall timeout as an ordinary cancellation.
func stalled(ctx context.Context, err error) error {
	if context.Cause(ctx) == errStalled {
		return errStalled
	}
	return err
}

type progressBody struct {
	io.ReadCloser
	alive func(bool)
	end   func()
	ctx   context.Context // response request context; nil for uploads
}

func (b *progressBody) Read(p []byte) (int, error) {
	// Short upload reads keep the stall timer meaningful on slow connections.
	if b.ctx == nil {
		p = p[:min(len(p), 16<<10)]
	}
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.alive(false)
	}
	if err != nil && err != io.EOF && b.ctx != nil {
		err = stalled(b.ctx, err)
	}
	return n, err
}

func (b *progressBody) Close() error {
	err := b.ReadCloser.Close()
	if b.end != nil {
		b.end()
	}
	return err
}

// connCtx owns sockets; h1Hosts shares ALPN observations between clients.
func newServiceClient(connCtx context.Context, h1Hosts *sync.Map, streamSettings *internet.MemoryStreamConfig, stall time.Duration, maxConns int) *http.Client {
	var (
		tlsConfig *tls.Config
		sockopt   *internet.SocketConfig
		fronting  *net.Destination
		// Detect silent h2 loss unless XMUX supplies an explicit keepalive.
		h2Idle, h2PingWait = 1500 * time.Millisecond, 2500 * time.Millisecond
		h3KeepAlive        time.Duration
	)
	if streamSettings != nil {
		tlsConfig = tls.ConfigFromStreamSettings(streamSettings)
		sockopt = streamSettings.SocketSettings
		fronting = streamSettings.Destination
		config, _ := streamSettings.ProtocolSettings.(*Config)
		if config.GetService() == "google-drive" || config.GetService() == "Google Drive" {
			h2PingWait = 0 // Use net/http's 15 s ping grace on Drive's shared h2 connections.
		}
		switch period := config.GetXmux().GetHKeepAlivePeriod(); {
		case period > 0:
			h3KeepAlive = time.Duration(period) * time.Second
			h2Idle, h2PingWait = h3KeepAlive, 0
		case period < 0:
			h2Idle, h2PingWait = 0, 0
		}
	}
	if tlsConfig == nil {
		tlsConfig = &tls.Config{}
	}
	fingerprint := tls.GetFingerprint(tlsConfig.Fingerprint)

	// A configured front replaces the address, not the transport network.
	dial := func(ctx context.Context, network, addr string) (net.Conn, net.Destination, error) {
		ctx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(connCtx, cancel)
		defer stop()
		defer cancel()
		if err := connCtx.Err(); err != nil {
			return nil, net.Destination{}, err
		}
		host, err := net.ParseDestination(network + ":" + addr)
		if err != nil {
			return nil, host, errors.New("bad address: ", addr).Base(err)
		}

		target := host
		if fronting != nil {
			target.Address = fronting.Address
			if fronting.Port != 0 {
				target.Port = fronting.Port
			}
		}

		var conn net.Conn
		switch {
		case streamSettings == nil || streamSettings.FinalMask == nil:
			conn, err = internet.DialSystem(ctx, target, sockopt)
		case target.Network == net.Network_UDP:
			conn, err = streamSettings.FinalMask.DialUDP(ctx, target)
		default:
			conn, err = streamSettings.FinalMask.DialTCP(ctx, target)
		}
		if err != nil {
			return nil, host, errors.New("failed to dial to dest").Base(err)
		}
		// HTTP/3 must close QUIC itself as well as its UDP socket.
		if network == "udp" {
			return conn, host, nil
		}
		// A recently canceled h2 stream may still own the socket.
		stopConn := context.AfterFunc(connCtx, func() { conn.Close() })
		return &ctxConn{Conn: conn, stop: stopConn}, host, nil
	}

	dialPlain := func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, _, err := dial(ctx, "tcp", addr)
		return conn, err
	}

	// http.Transport picks h2 or h1 by the ALPN in the ConnectionState of the conn
	dialTLS := func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, host, err := dial(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}

		gotlsConfig := tlsConfig.GetTLSConfig(tls.WithDestination(host))
		var tlsConn net.Conn
		if fingerprint == nil {
			tlsConn = tls.Client(conn, gotlsConfig)
		} else {
			tlsConn = tls.UClient(conn, gotlsConfig, fingerprint)
		}
		// Bound the handshake independently of connection reuse.
		ctx, cancel := context.WithTimeout(ctx, stall)
		defer cancel()
		if err := tlsConn.(tls.Interface).HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, err
		}
		return tlsConn, nil
	}

	transport := func(connsPerHost int) *http.Transport {
		return &http.Transport{
			IdleConnTimeout:     net.ConnIdleTimeout,
			MaxIdleConns:        maxConns,
			MaxIdleConnsPerHost: maxConns,
			MaxConnsPerHost:     connsPerHost,
		}
	}
	secure := func(conns int) *http.Transport {
		t := transport(conns)
		t.DialTLSContext, t.ForceAttemptHTTP2 = dialTLS, true
		t.HTTP2 = &http.HTTP2Config{SendPingTimeout: h2Idle, PingTimeout: h2PingWait}
		return t
	}

	t := &serviceTransport{plain: transport(maxConns), stall: stall}
	t.plain.DialContext = dialPlain
	// no server offers h3 by ALPN over TCP, so it is used only when asked for alone
	if slices.Equal(tlsConfig.NextProtocol, []string{"h3"}) {
		quicParams := streamSettings.QuicParams
		if quicParams == nil {
			quicParams = &internet.QuicParams{BbrProfile: string(bbr.ProfileStandard)}
		}
		// QUIC idle timeout and connCtx bound unused connections.
		quicConfig := &quic.Config{
			InitialStreamReceiveWindow:     quicParams.InitStreamReceiveWindow,
			MaxStreamReceiveWindow:         quicParams.MaxStreamReceiveWindow,
			InitialConnectionReceiveWindow: quicParams.InitConnReceiveWindow,
			MaxConnectionReceiveWindow:     quicParams.MaxConnReceiveWindow,
			MaxIdleTimeout:                 time.Duration(quicParams.MaxIdleTimeout) * time.Second,
			KeepAlivePeriod:                time.Duration(quicParams.KeepAlivePeriod) * time.Second,
			MaxIncomingStreams:             quicParams.MaxIncomingStreams,
			DisablePathMTUDiscovery:        quicParams.DisablePathMtuDiscovery || (runtime.GOOS != "linux" && runtime.GOOS != "windows" && runtime.GOOS != "darwin"),
			ChromeParrot:                   !quicParams.DisableChromeParrot,
		}
		if quicConfig.KeepAlivePeriod == 0 {
			quicConfig.KeepAlivePeriod = h3KeepAlive
		}

		t.secure = &http3.Transport{
			QUICConfig: quicConfig,
			Dial: func(ctx context.Context, addr string, _ *gotls.Config, cfg *quic.Config) (*quic.Conn, error) {
				ctx, cancel := context.WithCancel(ctx)
				stop := context.AfterFunc(connCtx, cancel)
				defer stop()
				defer cancel()
				if connCtx.Err() != nil {
					cancel()
				}
				switch quicParams.Congestion {
				case "", "bbr", "reno":
				default:
					return nil, errors.New("unsupported congestion control over h3: ", quicParams.Congestion)
				}

				conn, host, err := dial(ctx, "udp", addr)
				if err != nil {
					return nil, err
				}
				var pktConn net.PacketConn
				var udpAddr net.Addr
				switch c := conn.(type) {
				case *net.PacketConnWrapper:
					pktConn = c.PacketConn
					udpAddr = c.RemoteAddr()
				case *cnc.Connection:
					pktConn = &internet.FakePacketConn{Conn: c}
					udpAddr = &net.UDPAddr{IP: []byte{0, 0, 0, 0}}
				default:
					conn.Close()
					return nil, errors.New("failed to dial to dest: unexpected ", reflect.TypeOf(c))
				}

				// Set SNI before certificate pinning and ECH lookup.
				gotlsConfig := tlsConfig.GetTLSConfig(tls.WithDestination(host))
				// it is for servers, and ChromeParrot refuses a config that has it
				gotlsConfig.GetCertificate = nil
				tr := &quic.Transport{Conn: pktConn, DisableGSO: quicParams.DisableGSO}
				if !quicParams.DisableChromeParrot {
					tr.ConnectionIDGenerator = quic.ZeroLengthConnectionIDGenerator{}
				}
				qconn, err := tr.Dial(ctx, udpAddr, gotlsConfig, cfg)
				if err != nil {
					tr.Close()
					pktConn.Close()
					return nil, err
				}
				stopConn := context.AfterFunc(connCtx, func() { qconn.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeNoError), "") })
				context.AfterFunc(qconn.Context(), func() { stopConn(); tr.Close(); pktConn.Close() })
				if quicParams.Congestion != "reno" {
					congestion.UseBBR(qconn, bbr.Profile(quicParams.BbrProfile))
				}
				return qconn, nil
			},
		}
	} else {
		t.secure = &secureTransport{h2: secure(1), h1: secure(maxConns), h1Hosts: h1Hosts}
	}
	return &http.Client{Transport: t}
}

// Closing the socket unregisters the owner-context callback.
type ctxConn struct {
	net.Conn
	stop func() bool
}

func (c *ctxConn) Close() error {
	c.stop()
	return c.Conn.Close()
}

// Use one h2 connection per host, but allow parallel h1 connections.
type secureTransport struct {
	h2, h1  *http.Transport
	h1Hosts *sync.Map
}

func (t *secureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if _, ok := t.h1Hosts.Load(r.URL.Host); ok {
		return t.h1.RoundTrip(r)
	}
	resp, err := t.h2.RoundTrip(r)
	if err == nil && resp.ProtoMajor == 1 {
		t.h1Hosts.Store(r.URL.Host, true)
	}
	return resp, err
}

func (t *secureTransport) CloseIdleConnections() {
	t.h2.CloseIdleConnections()
	t.h1.CloseIdleConnections()
}

var errSessionClosed = errors.New("the session is closed")

// XMUX selects a lease once, not repeatedly while looking for a connection.
// Running references cover both the session and any response bodies still open.
type connPool struct {
	mu     sync.Mutex
	lanes  []connLane // One pinned manager, or one per Drive media connection.
	spread bool
	next   int
	ctx    context.Context
	cancel context.CancelFunc
}

type connLane struct {
	manager *splithttp.XmuxManager
	active  atomic.Int32 // Includes open response bodies, even across XMUX lease rotation.
}

type poolConn struct {
	*http.Client
	ctx    context.Context
	cancel context.CancelFunc
	stop   func() bool
}

func (c *poolConn) IsClosed() bool { return c.ctx.Err() != nil }

func (c *poolConn) Close() error {
	c.cancel()
	c.stop()
	c.CloseIdleConnections()
	return nil
}

// driveMediaConns is the number of Drive media connections: XMUX
// maxConnections when set, else 4. One Google connection carries only a
// fraction of a link, so media requests are spread over these.
func driveMediaConns(x *splithttp.XmuxConfig) int32 {
	if n := x.GetMaxConnections().GetTo(); n > 0 {
		return n
	}
	return 4
}

// Positive conns spreads Drive media requests; other backends keep one manager.
func newConnPool(config *splithttp.XmuxConfig, conns int32, newClient func(context.Context) *http.Client) *connPool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &connPool{ctx: ctx, cancel: cancel, spread: conns > 0}
	newConn := func() splithttp.XmuxConn {
		ctx, cancel := context.WithCancel(p.ctx)
		c := &poolConn{ctx: ctx, cancel: cancel, Client: newClient(ctx)}
		c.stop = context.AfterFunc(ctx, c.CloseIdleConnections)
		return c
	}
	maxConnections := config.GetMaxConnections()
	if p.spread {
		maxConnections = &splithttp.RangeConfig{From: 1, To: 1}
	}
	for range max(1, conns) {
		manager := splithttp.NewXmuxManager(splithttp.XmuxConfig{
			MaxConnections: maxConnections, MaxConcurrency: config.GetMaxConcurrency(),
			CMaxReuseTimes: config.GetCMaxReuseTimes(), HMaxRequestTimes: config.GetHMaxRequestTimes(),
			HMaxReusableSecs: config.GetHMaxReusableSecs(), HKeepAlivePeriod: config.GetHKeepAlivePeriod(),
		}, newConn)
		p.lanes = append(p.lanes, connLane{manager: manager})
	}
	return p
}

func (p *connPool) Close() {
	if p != nil {
		p.cancel()
	}
}

type sessionTransport struct {
	pool   *connPool
	client *splithttp.XmuxClient // guarded by pool.mu
	closed bool
}

func (t *sessionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	p := t.pool
	p.mu.Lock()
	if t.closed || p.ctx.Err() != nil || r.Context().Err() != nil {
		p.mu.Unlock()
		if r.Body != nil {
			r.Body.Close()
		}
		return nil, cmp.Or[error](r.Context().Err(), errSessionClosed)
	}
	c := t.client
	lane := &p.lanes[0]
	if p.spread {
		// Rotate ties, but avoid stacking new requests on a slow connection while peers are free.
		best := p.next
		for off := 1; off < len(p.lanes) && p.lanes[best].active.Load() != 0; off++ {
			i := (p.next + off) % len(p.lanes)
			if p.lanes[i].active.Load() < p.lanes[best].active.Load() {
				best = i
			}
		}
		lane, p.next = &p.lanes[best], (best+1)%len(p.lanes)
		c = lane.manager.GetXmuxClient(r.Context())
	} else if c == nil || c.XmuxConn.IsClosed() || c.LeftRequests.Load() <= 0 || (!c.UnreusableAt.IsZero() && time.Now().After(c.UnreusableAt)) {
		if c != nil {
			c.DoneRunning()
		}
		c = lane.manager.GetXmuxClient(r.Context())
		c.AddRunning()
		t.client = c
	}
	c.LeftRequests.Add(-1)
	c.AddRunning()
	lane.active.Add(1)
	p.mu.Unlock()
	release := func() {
		lane.active.Add(-1)
		c.DoneRunning()
	}
	resp, err := c.XmuxConn.(*poolConn).Transport.RoundTrip(r)
	if err != nil {
		release()
		return nil, err
	}
	resp.Body = &leasedBody{ReadCloser: resp.Body, release: sync.OnceFunc(release)}
	return resp, nil
}

type leasedBody struct {
	io.ReadCloser
	release func()
}

func (b *leasedBody) Close() error {
	defer b.release()
	return b.ReadCloser.Close()
}

func (t *sessionTransport) Close() {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.closed = true
	if t.client != nil {
		t.client.DoneRunning()
		t.client = nil
	}
}
