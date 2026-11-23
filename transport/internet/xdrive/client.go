package xdrive

import (
	"context"
	gotls "crypto/tls"
	"io"
	"net/http"
	"net/http/httptrace"
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
	plain  http.RoundTripper
	secure http.RoundTripper
	stall  time.Duration
}

type (
	dispatchKey      struct{}
	dispatchStallKey struct{}
)

type dispatchTransport struct{ next http.RoundTripper }

func (t dispatchTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return dispatchRoundTrip(t.next, r)
}

// The installed H1/H2/H3 transports call GotConn synchronously before each
// dispatch, including internal replays. Wrappers defer admission to that hook.
func dispatchRoundTrip(next http.RoundTripper, r *http.Request) (*http.Response, error) {
	if next == nil {
		next = http.DefaultTransport
	}
	admit, ok := r.Context().Value(dispatchKey{}).(func(context.Context) error)
	if !ok {
		return next.RoundTrip(r)
	}
	switch next.(type) {
	case *serviceTransport, *secureTransport, *sessionTransport:
		return next.RoundTrip(r)
	case *http.Transport, *http3.Transport:
		ctx := r.Context()
		trace := &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) {
			resume := func() {}
			if pause, ok := ctx.Value(dispatchStallKey{}).(func() func()); ok {
				resume = pause()
			}
			err := admit(ctx)
			resume()
			if err != nil {
				// H1 can enqueue its write even after a canceled GotConn returns.
				// Its reserved socket is not shared; close it before returning.
				// H2 checks context before HEADERS, H3 before opening a stream.
				if _, h1or2 := next.(*http.Transport); h1or2 {
					state, tlsConn := i.Conn.(interface{ ConnectionState() gotls.ConnectionState })
					if !tlsConn || state.ConnectionState().NegotiatedProtocol != "h2" {
						i.Conn.Close()
					}
				}
			}
		}}
		r = r.WithContext(httptrace.WithClientTrace(ctx, trace))
	default:
		// Non-replaying in-memory transports have no connection trace.
		if err := admit(r.Context()); err != nil {
			return nil, err
		}
	}
	return next.RoundTrip(r)
}

// RoundTrip ends a request that stands still for t.stall, with no answer that
// long after its body went out or no byte of either body, but not one that just
// runs long: a large object on a slow link takes minutes and is fine as long as
// its bytes move
func (t *serviceTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	next := t.plain
	if r.URL.Scheme == "https" {
		next = t.secure
	}

	ctx, cancel := context.WithCancelCause(r.Context())
	var timerMu sync.Mutex
	paused, ended := 0, false
	deadline := time.Now().Add(t.stall)
	timer := time.AfterFunc(t.stall, func() {
		timerMu.Lock()
		defer timerMu.Unlock()
		if !ended && paused == 0 && !time.Now().Before(deadline) {
			cancel(errStalled)
		}
	})
	alive := func() {
		timerMu.Lock()
		defer timerMu.Unlock()
		if !ended && paused == 0 {
			deadline = time.Now().Add(t.stall)
			timer.Reset(t.stall)
		}
	}
	pause := func() func() {
		timerMu.Lock()
		paused++
		timer.Stop()
		timerMu.Unlock()
		return sync.OnceFunc(func() {
			timerMu.Lock()
			defer timerMu.Unlock()
			paused--
			if !ended && paused == 0 {
				deadline = time.Now().Add(t.stall)
				timer.Reset(t.stall)
			}
		})
	}
	end := func() {
		timerMu.Lock()
		ended = true
		timer.Stop()
		timerMu.Unlock()
		cancel(nil)
	}
	ctx = context.WithValue(ctx, dispatchStallKey{}, pause)
	r = r.WithContext(ctx)
	// the headers of chrome as with XHTTP and DoH, net/http and http3 would send
	// their own user agent after a hello of chrome
	r.Header = r.Header.Clone()
	utils.TryDefaultHeadersWith(r.Header, "fetch")
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = &progressBody{ReadCloser: r.Body, alive: alive, upload: true}
	}
	// a body sent again by the transport itself, after a GOAWAY or on a reused
	// connection that failed, moves just the same
	if getBody := r.GetBody; getBody != nil {
		r.GetBody = func() (io.ReadCloser, error) {
			body, err := getBody()
			if err != nil || body == http.NoBody {
				return body, err
			}
			return &progressBody{ReadCloser: body, alive: alive, upload: true}, nil
		}
	}

	resp, err := dispatchRoundTrip(next, r)
	if err != nil {
		err = stalled(ctx, err)
		end()
		return nil, err
	}
	alive()
	resp.Body = &progressBody{ReadCloser: resp.Body, alive: alive, end: end, ctx: ctx}
	return resp, nil
}

func (t *serviceTransport) CloseIdleConnections() {
	closeIdleTransport(t.plain)
	closeIdleTransport(t.secure)
}

func closeIdleTransport(t http.RoundTripper) {
	if closer, ok := t.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

// stalled returns errStalled for an error that the stall gave, which http2 reports
// as a plain cancel
func stalled(ctx context.Context, err error) error {
	if context.Cause(ctx) == errStalled {
		return errStalled
	}
	return err
}

type progressBody struct {
	io.ReadCloser
	alive  func()
	end    func()
	upload bool
	// ctx is that of the request, for the body of its response
	ctx context.Context
}

func (b *progressBody) Read(p []byte) (int, error) {
	// http2 reads a request body up to 512 KiB at a time and sends it all before
	// the next read, which on a slow link could look like no progress
	if b.upload {
		p = p[:min(len(p), 16<<10)]
	}
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.alive()
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

// newServiceClient returns a client whose requests end after stall without
// progress, see serviceTransport.RoundTrip, and whose connections close when
// connCtx is done. Clients that share h1Hosts share which hosts answer over h1
func newServiceClient(connCtx context.Context, h1Hosts *sync.Map, streamSettings *internet.MemoryStreamConfig, stall time.Duration, maxConns int) *http.Client {
	var (
		tlsConfig *tls.Config
		sockopt   *internet.SocketConfig
		fronting  *net.Destination
		// A silently dead h2 connection must be found in seconds, not after
		// the 45 s Chrome period (net.ChromeH2KeepAlivePeriod). Pings go out only after no frame at all has
		// arrived, so bulk transfers are not affected. An explicit Xmux
		// keep-alive setting overrides this.
		h2Idle, h2PingWait = 1500 * time.Millisecond, 2500 * time.Millisecond
		// h3 keeps alive only when asked to, see below
		h3KeepAlive time.Duration
	)
	if streamSettings != nil {
		tlsConfig = tls.ConfigFromStreamSettings(streamSettings)
		sockopt = streamSettings.SocketSettings
		fronting = streamSettings.Destination
		if config, ok := streamSettings.ProtocolSettings.(*Config); ok && config.Xmux != nil {
			switch period := config.Xmux.HKeepAlivePeriod; {
			case period > 0:
				h3KeepAlive = time.Duration(period) * time.Second
				h2Idle, h2PingWait = h3KeepAlive, 0
			case period < 0:
				h2Idle, h2PingWait = 0, 0
			}
		}
	}
	if tlsConfig == nil {
		tlsConfig = &tls.Config{}
	}
	fingerprint := tls.GetFingerprint(tlsConfig.Fingerprint)

	// fronting takes only the address and port, the network stays that of the
	// dial, which is UDP for h3 though the destination is built as TCP
	dial := func(ctx context.Context, network, addr string) (net.Conn, net.Destination, error) {
		ctx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(connCtx, cancel)
		defer stop()
		defer cancel()
		if connCtx.Err() != nil {
			cancel()
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
		// h3 closes its QUIC connection instead, as the server never hears of a UDP
		// socket that closes
		if network == "udp" {
			return conn, host, nil
		}
		// CloseIdleConnections leaves an h2 connection open while a stream just
		// canceled has not left it yet
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
		// the dial outlives the request that started it, and a handshake that hangs
		// would keep the one connection of the host to itself
		ctx, cancel := context.WithTimeout(ctx, stall)
		defer cancel()
		if err := tlsConn.(tls.Interface).HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, err
		}
		return tlsConn, nil
	}

	secure := func(connsPerHost int) *http.Transport {
		return &http.Transport{
			DialTLSContext:      dialTLS,
			ForceAttemptHTTP2:   true,
			IdleConnTimeout:     net.ConnIdleTimeout,
			MaxIdleConns:        maxConns,
			MaxIdleConnsPerHost: maxConns,
			MaxConnsPerHost:     connsPerHost,
			HTTP2:               &http.HTTP2Config{SendPingTimeout: h2Idle, PingTimeout: h2PingWait},
		}
	}

	var https http.RoundTripper = &secureTransport{h2: secure(1), h1: secure(maxConns), h1Hosts: h1Hosts}
	// no server offers h3 by ALPN over TCP, so it is used only when asked for alone
	if slices.Equal(tlsConfig.NextProtocol, []string{"h3"}) {
		quicParams := streamSettings.QuicParams
		if quicParams == nil {
			quicParams = &internet.QuicParams{BbrProfile: string(bbr.ProfileStandard)}
		}
		// the defaults are those of quic-go, with no keepalive unless asked for, as
		// in DoH: http3 never closes a connection that is idle or that it dropped
		// after an error, so only the idle timeout or the end of connCtx ends it
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

		https = &http3.Transport{
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
					// brutal is for Hysteria servers, not services, and a panic here, in
					// a goroutine of http3, would end the process
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

				// built for each dial with the host as SNI, as for h2: http3 sets the
				// SNI on a copy, which a pinned CA and the ECH lookup do not see
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
	}

	return &http.Client{
		Transport: &serviceTransport{
			plain: &http.Transport{
				DialContext:         dialPlain,
				IdleConnTimeout:     net.ConnIdleTimeout,
				MaxIdleConns:        maxConns,
				MaxIdleConnsPerHost: maxConns,
				MaxConnsPerHost:     maxConns,
			},
			secure: https,
			stall:  stall,
		},
	}
}

// ctxConn is a connection that connCtx no longer closes once it is closed,
// so that one that ends on its own leaves nothing behind in connCtx
type ctxConn struct {
	net.Conn
	stop func() bool
}

func (c *ctxConn) Close() error {
	c.stop()
	return c.Conn.Close()
}

// secureTransport keeps a host to one connection, all that h2 needs, as without
// a limit parallel requests would each dial their own. A host that answers over
// h1 moves to a transport with more, since h1 carries one request at a time
type secureTransport struct {
	h2, h1  http.RoundTripper
	h1Hosts *sync.Map
}

func (t *secureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if _, ok := t.h1Hosts.Load(r.URL.Host); ok {
		return dispatchRoundTrip(t.h1, r)
	}
	resp, err := dispatchRoundTrip(t.h2, r)
	if err == nil && resp.ProtoMajor == 1 {
		t.h1Hosts.Store(r.URL.Host, true)
	}
	return resp, err
}

func (t *secureTransport) CloseIdleConnections() {
	closeIdleTransport(t.h2)
	closeIdleTransport(t.h1)
}

var errSessionClosed = errors.New("the session is closed")

// errNoSpread means no spread connection was available; the request then
// takes the session's own connection
var errNoSpread = errors.New("no other connection to spread over")

// connPool hands the sessions of a storage the connections of XMUX, each a
// client of its own made by newClient
type connPool struct {
	mu     sync.Mutex
	xmux   *splithttp.XmuxManager
	ctx    context.Context
	cancel context.CancelFunc
	closed bool

	connMu sync.Mutex
	conns  map[*poolConn]struct{}

	spareAt  atomic.Int64 // unix nanoseconds of the last spare check
	maxConns int          // connections XMUX surely opens; 0 when it opens one
}

// Every pool that carries requests keeps an established idle connection, and
// a connection that goes silent under waiting requests is probed and, if it
// stays silent, closed before the HTTP/2 keepalive would find it.

const (
	// A spare is checked for at most this often per pool, and an idle
	// connection counts as ready while it answered within spareFresh; an
	// older one is probed again with a HEAD to the service host, which
	// costs no API quota.
	spareCheckEvery   = time.Second
	spareFresh        = 30 * time.Second
	spareProbeTimeout = 15 * time.Second
	// sparePoolConnections lets pools that would otherwise hold a single
	// connection keep a spare.
	sparePoolConnections = 2

	// Requests with at most deadConnSmallBody of body normally answer within a
	// second, but Drive itself sometimes takes 2-3.5 s for all of them at
	// once, so silence alone does not tell a dead connection from a slow
	// service. A connection that answered nothing deadConnSuspect after such a
	// request went out, while another connection answered meanwhile, is
	// probed with a HEAD to the service host on that same connection (one
	// round trip, no API quota, not served by Drive). Only when the probe gets
	// no answer within deadConnProbe is the connection closed, which fails
	// its waiting requests so that they are retried elsewhere; a connection
	// that answers the probe is left alone. The HTTP/2 keepalive takes up to
	// 4 s (1.5 s idle, then a 2.5 s ping timeout).
	deadConnSmallBody = 64 << 10
	deadConnSuspect   = 1500 * time.Millisecond
	deadConnProbe     = time.Second
)

// connHeardAny is when any pool connection last received a response byte, in
// unix nanoseconds.
var connHeardAny atomic.Int64

func noteHeardAny(now time.Time) {
	for at := now.UnixNano(); ; {
		last := connHeardAny.Load()
		if last >= at || connHeardAny.CompareAndSwap(last, at) {
			return
		}
	}
}

// spareXmux gives a pool without maxConnections room for a spare.
func spareXmux(c *splithttp.XmuxConfig) *splithttp.XmuxConfig {
	return xmuxWithConnections(c, sparePoolConnections)
}

// keepSpare makes sure, at most once per spareCheckEvery, that a pool carrying
// requests has an established connection that none of them uses, so that the
// requests of a connection that dies move without waiting for a dial.
func (p *connPool) keepSpare(r *http.Request) {
	if r.URL == nil || r.URL.Host == "" || r.Context().Value(keeperProbeKey{}) != nil {
		return
	}
	now := time.Now()
	last := p.spareAt.Load()
	if now.UnixNano()-last < int64(spareCheckEvery) || !p.spareAt.CompareAndSwap(last, now.UnixNano()) {
		return
	}
	var spare *poolConn
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	ready, open := false, 0
	p.connMu.Lock()
	for c := range p.conns {
		c.mu.Lock()
		if !c.closing {
			open++
		}
		usable := !c.closing && !c.warming && c.inflight == 0
		fresh := usable && !c.heardAt.IsZero() && now.Sub(c.heardAt) < spareFresh
		c.mu.Unlock()
		if fresh {
			ready = true
			break
		}
		if usable && spare == nil {
			spare = c
		}
	}
	p.connMu.Unlock()
	if !ready && spare == nil && open < p.maxConns {
		// Below that XMUX makes a new connection rather than hand out a
		// busy one; asking it at the limit would spend reuse counts.
		if c := p.xmux.GetXmuxClient(r.Context()).XmuxConn.(*poolConn); c.unused() {
			spare = c
		}
	}
	p.mu.Unlock()
	if !ready && spare != nil {
		spare.warm(r.URL.Scheme, r.URL.Host)
	}
}

func newConnPool(config *splithttp.XmuxConfig, newClient func(connCtx context.Context) *http.Client) *connPool {
	if config == nil {
		config = &splithttp.XmuxConfig{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	pool := &connPool{ctx: ctx, cancel: cancel, conns: make(map[*poolConn]struct{})}
	if config.MaxConnections != nil {
		// XMUX opens connections at least up to the lower bound.
		pool.maxConns = int(config.MaxConnections.From)
	}
	pool.xmux = splithttp.NewXmuxManager(splithttp.XmuxConfig{
		MaxConcurrency:   config.MaxConcurrency,
		MaxConnections:   config.MaxConnections,
		CMaxReuseTimes:   config.CMaxReuseTimes,
		HMaxRequestTimes: config.HMaxRequestTimes,
		HMaxReusableSecs: config.HMaxReusableSecs,
		HKeepAlivePeriod: config.HKeepAlivePeriod,
	}, func() splithttp.XmuxConn {
		ctx, cancel := context.WithCancel(pool.ctx)
		conn := &poolConn{next: newClient(ctx).Transport, ctx: ctx, id: poolConnIDs.Add(1)}
		conn.cancel = sync.OnceFunc(func() {
			cancel()
			closeIdleTransport(conn.next)
			pool.connMu.Lock()
			delete(pool.conns, conn)
			pool.connMu.Unlock()
		})
		pool.connMu.Lock()
		pool.conns[conn] = struct{}{}
		pool.connMu.Unlock()
		return conn
	})
	return pool
}

// Close is terminal; ordinary session release leaves the pool reusable.
func (p *connPool) Close() {
	if p == nil {
		return
	}
	p.cancel()
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.connMu.Lock()
	conns := make([]*poolConn, 0, len(p.conns))
	for conn := range p.conns {
		conns = append(conns, conn)
	}
	p.connMu.Unlock()
	for _, conn := range conns {
		conn.forceClose()
	}
}

// poolConnIDs numbers the connections of all pools for the logs, from 1.
var poolConnIDs atomic.Uint64

// poolConn is a connection of XMUX, which closes once XMUX drops it and the
// requests on it end
type poolConn struct {
	next   http.RoundTripper
	ctx    context.Context
	cancel context.CancelFunc
	id     uint64 // for the logs only; 0 when the connection is not from a pool

	mu       sync.Mutex
	inflight int
	closing  bool
	servedAt time.Time
	servedBy *sessionTransport

	heardAt time.Time // last response headers or body bytes
	waits   map[*connWait]struct{}
	recheck *time.Timer // one follow-up until this connection answers or its waits drain
	warming bool
	probing bool
	dead    bool
}

// connWait is a small request sent on a connection and not answered yet.
type connWait struct {
	wroteAt      time.Time
	timer        *time.Timer
	done         bool
	scheme, host string // where a liveness probe for it goes
}

// connProbeKey marks the liveness probe of a connection, which is not itself
// watched.
type connProbeKey struct{}

func (c *poolConn) heard() {
	now := time.Now()
	c.mu.Lock()
	c.heardAt = now
	c.clearRecheckLocked()
	c.mu.Unlock()
	noteHeardAny(now)
}

// unused reports a connection that never carried a request.
func (c *poolConn) unused() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closing && !c.warming && c.inflight == 0 && c.heardAt.IsZero()
}

// ready reports a connection that answered within spareFresh and takes requests.
func (c *poolConn) ready(now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closing && !c.heardAt.IsZero() && now.Sub(c.heardAt) < spareFresh
}

// warm establishes the connection with a HEAD to the service host, which needs
// no authorization and costs no API quota.
func (c *poolConn) warm(scheme, host string) {
	if !c.acquire() {
		return
	}
	c.mu.Lock()
	c.warming = true
	c.mu.Unlock()
	go func() {
		defer func() {
			c.mu.Lock()
			c.warming = false
			c.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(c.ctx, spareProbeTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, scheme+"://"+host+"/", nil)
		if err != nil {
			c.done()
			return
		}
		resp, err := c.roundTrip(req)
		if err != nil {
			errors.LogDebug(ctx, "xdrive spare connection ", c.id, ": ", err)
			return
		}
		resp.Body.Close()
		errors.LogDebug(ctx, "xdrive spare connection ", c.id, " ready, status ", resp.StatusCode)
	}()
}

// watch has a small request's answer timed once it is written; see
// deadConnSuspect.
func (c *poolConn) watch(ctx context.Context, r *http.Request) (context.Context, *connWait) {
	if r.ContentLength < 0 || r.ContentLength > deadConnSmallBody ||
		r.URL == nil || r.URL.Host == "" || ctx.Value(connProbeKey{}) != nil {
		return ctx, nil
	}
	w := &connWait{scheme: r.URL.Scheme, host: r.URL.Host}
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			c.wrote(w)
		}
	}}), w
}

func (c *poolConn) wrote(w *connWait) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w.done {
		return
	}
	w.wroteAt = time.Now()
	if w.timer != nil {
		// written again, after the transport retried it
		w.timer.Reset(deadConnSuspect)
		return
	}
	if c.waits == nil {
		c.waits = make(map[*connWait]struct{})
	}
	c.waits[w] = struct{}{}
	w.timer = time.AfterFunc(deadConnSuspect, c.checkDead)
}

func (c *poolConn) unwatch(w *connWait) {
	if w == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	w.done = true
	delete(c.waits, w)
	if w.timer != nil {
		w.timer.Stop()
	}
	if len(c.waits) == 0 {
		c.clearRecheckLocked()
	}
}

func (c *poolConn) clearRecheckLocked() {
	if c.recheck != nil {
		c.recheck.Stop()
		c.recheck = nil
	}
}

// checkDead probes a connection that answered nothing since its oldest waiting
// small request went out deadConnSuspect ago, while another connection
// answered meanwhile, so the path itself works. See deadConnProbe.
func (c *poolConn) checkDead() {
	now := time.Now()
	c.mu.Lock()
	var oldest *connWait
	for w := range c.waits {
		// A request written before the connection last answered says
		// nothing about whether it still answers; a later one does.
		if c.heardAt.After(w.wroteAt) {
			continue
		}
		if oldest == nil || w.wroteAt.Before(oldest.wroteAt) {
			oldest = w
		}
	}
	suspect := oldest != nil && now.Sub(oldest.wroteAt) >= deadConnSuspect && !c.closing && !c.probing && !c.dead
	probe := suspect && connHeardAny.Load() > oldest.wroteAt.UnixNano()
	if suspect && !probe && c.recheck == nil {
		// Another connection may answer just after this check. Keep the timer
		// recorded after it fires so persistent silence cannot rearm it.
		c.recheck = time.AfterFunc(deadConnProbe, c.checkDead)
	}
	var since time.Time
	var scheme, host string
	if probe {
		c.probing = true
		since, scheme, host = oldest.wroteAt, oldest.scheme, oldest.host
	}
	c.mu.Unlock()
	if probe {
		go c.probe(since, scheme, host)
	}
}

// probe sends a HEAD to the service host on the connection. If it gets no
// answer within deadConnProbe while requests written at or after since are
// still unanswered, the connection is closed, which fails them so that they
// are retried on another connection.
func (c *poolConn) probe(since time.Time, scheme, host string) {
	defer func() {
		c.mu.Lock()
		c.probing = false
		c.mu.Unlock()
	}()
	if !c.acquire() {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithValue(c.ctx, connProbeKey{}, true), deadConnProbe)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, scheme+"://"+host+"/", nil)
	if err != nil {
		c.done()
		return
	}
	began := time.Now()
	resp, err := c.roundTrip(req)
	if err == nil {
		resp.Body.Close()
		c.mu.Lock()
		waiting := len(c.waits)
		c.mu.Unlock()
		errors.LogDebug(context.Background(), "xdrive connection ", c.id, " slow but alive: probe answered in ",
			time.Since(began), " with ", waiting, " requests waiting for ", time.Since(since))
		return
	}
	if c.ctx.Err() != nil {
		return // closed meanwhile
	}
	c.mu.Lock()
	waiting := len(c.waits)
	kill := !c.dead && waiting > 0 && !c.heardAt.After(since)
	if kill {
		c.dead = true
	}
	c.mu.Unlock()
	if kill {
		errors.LogInfo(context.Background(), "xdrive connection ", c.id, " answered nothing for ", time.Since(since),
			" with ", waiting, " requests waiting while others answered, nor a probe in ", time.Since(began), "; closing it")
		c.forceClose()
	}
}

// logID is the connection's id in the logs, 0 when no pool connection was used.
func (c *poolConn) logID() uint64 {
	if c == nil {
		return 0
	}
	return c.id
}

func (c *poolConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closing
}

// acquire counts a request on the connection unless it is closing, which is
// decided together with the count so that Close cannot cancel it in between
func (c *poolConn) acquire() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return false
	}
	c.inflight++
	return true
}

func (c *poolConn) Close() error {
	c.mu.Lock()
	c.closing = true
	idle := c.inflight == 0
	c.mu.Unlock()
	if idle {
		c.cancel()
	}
	return nil
}

func (c *poolConn) forceClose() {
	c.mu.Lock()
	c.closing = true
	c.mu.Unlock()
	c.cancel()
}

func (c *poolConn) done() {
	c.mu.Lock()
	c.inflight--
	idle := c.closing && c.inflight == 0
	c.mu.Unlock()
	if idle {
		c.cancel()
	}
}

// roundTrip sends a request counted in inflight by the caller
func (c *poolConn) roundTrip(r *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(c.ctx, cancel)
	if c.ctx.Err() != nil {
		cancel()
	}
	ctx, wait := c.watch(ctx, r)
	finish := sync.OnceFunc(func() { stop(); cancel(); c.unwatch(wait); c.done() })
	if err := ctx.Err(); err != nil {
		finish()
		return nil, err
	}
	resp, err := dispatchRoundTrip(c.next, r.WithContext(ctx))
	c.unwatch(wait)
	if err != nil {
		finish()
		return nil, err
	}
	c.heard()
	resp.Body = &doneBody{ReadCloser: &heardBody{ReadCloser: resp.Body, conn: c}, done: finish}
	return resp, nil
}

// heardBody notes each read of response bytes on its connection.
type heardBody struct {
	io.ReadCloser
	conn *poolConn
}

func (b *heardBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.conn.heard()
	}
	return n, err
}

type doneBody struct {
	io.ReadCloser
	done func()
}

func (b *doneBody) Close() error {
	err := b.ReadCloser.Close()
	b.done()
	return err
}

// a connection that has not answered yet is given this long before its
// timeouts retire it, so that a dial or handshake is not cut short again
var stalledGrace = 12 * time.Second

type poolConnKey struct{}

// defaultPoolConnections is how many connections the normal and head-read
// pools may hold when the config sets no Xmux maxConnections, as XHTTP does.
const defaultPoolConnections = 4

// xmuxWithConnections returns c, or a copy of it with maxConnections set to n
// when the config gives none. Built field by field, not copied by value.
func xmuxWithConnections(c *splithttp.XmuxConfig, n int32) *splithttp.XmuxConfig {
	if c != nil && c.MaxConnections != nil && c.MaxConnections.To > 0 {
		return c
	}
	out := &splithttp.XmuxConfig{MaxConnections: &splithttp.RangeConfig{From: n, To: n}}
	if c != nil {
		out.MaxConcurrency, out.CMaxReuseTimes = c.MaxConcurrency, c.CMaxReuseTimes
		out.HMaxRequestTimes, out.HMaxReusableSecs = c.HMaxRequestTimes, c.HMaxReusableSecs
		out.HKeepAlivePeriod = c.HKeepAlivePeriod
	}
	return out
}

// avoidConnKey carries a *connSpread: requests that carry it are sent over an
// established connection of the pool that none of the spread's other requests
// is using, so one dead connection cannot hold all of them.
type avoidConnKey struct{}

// Per-attempt exclusion, independent of the shared head hunt.
type failedConnKey struct{}

type connSpread struct {
	mu   sync.Mutex
	used []*poolConn
}

func (s *connSpread) has(c *poolConn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Contains(s.used, c)
}

func (s *connSpread) add(c *poolConn) {
	s.mu.Lock()
	s.used = append(s.used, c)
	s.mu.Unlock()
}

func (s *connSpread) remove(c *poolConn) {
	s.mu.Lock()
	if i := slices.Index(s.used, c); i >= 0 {
		s.used = slices.Delete(s.used, i, i+1)
	}
	s.mu.Unlock()
}

// spreadTrip sends r over a connection of the pool that sp does not use,
// preferring established ones and dialing a new one only while the pool is
// below maxConnections. It does not move the session's own connection.
func (t *sessionTransport) spreadTrip(r *http.Request, sp *connSpread, failed *poolConn) (*http.Response, error) {
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed || t.pool.ctx.Err() != nil {
		return nil, errSessionClosed
	}
	var client *splithttp.XmuxClient
	var conn *poolConn
	t.pool.mu.Lock()
	if t.pool.closed {
		t.pool.mu.Unlock()
		return nil, errSessionClosed
	}
	for try := 0; try < 6 && client == nil; try++ {
		c := t.pool.xmux.GetExistingXmuxClient(r.Context())
		if c == nil {
			break
		}
		if pc := c.XmuxConn.(*poolConn); pc != failed && !sp.has(pc) && pc.acquire() {
			client, conn = c, pc
		}
	}
	for try := 0; try < 3 && client == nil; try++ {
		// At maxConnections this returns an existing connection, which may be
		// the failed one or one sp already uses: that is no spread.
		c := t.pool.xmux.GetXmuxClient(r.Context())
		if pc := c.XmuxConn.(*poolConn); pc != failed && !sp.has(pc) && pc.acquire() {
			client, conn = c, pc
		}
	}
	if client != nil {
		sp.add(conn)
		client.AddRunning()
		client.LeftRequests.Add(-1)
	}
	t.pool.mu.Unlock()
	if client == nil {
		return nil, errNoSpread
	}
	release := sync.OnceFunc(func() { sp.remove(conn); client.DoneRunning() })
	if selected, ok := r.Context().Value(poolConnKey{}).(func(*poolConn)); ok {
		selected(conn)
	}
	t.pool.keepSpare(r)
	resp, err := conn.roundTrip(r)
	if err != nil {
		release()
		return nil, err
	}
	resp.Body = &servedBody{ReadCloser: &doneBody{ReadCloser: resp.Body, done: release}, ctx: r.Context(), served: sync.OnceFunc(func() {
		conn.mu.Lock()
		conn.servedAt = maxTime(conn.servedAt, time.Now())
		conn.mu.Unlock()
	})}
	return resp, nil
}

// sessionTransport sends the requests of one session over a connection of
// XMUX, and over another once that one has no requests or time left
type sessionTransport struct {
	pool *connPool

	mu                sync.Mutex
	client            *splithttp.XmuxClient
	closed            bool
	slowAt, retiredAt time.Time
	movedAt           time.Time
	takenAt           time.Time
	servedAt          time.Time
}

func (t *sessionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	sp, _ := r.Context().Value(avoidConnKey{}).(*connSpread)
	failed, _ := r.Context().Value(failedConnKey{}).(*poolConn)
	if sp != nil || failed != nil {
		if sp == nil {
			sp = new(connSpread)
		}
		if resp, err := t.spreadTrip(r, sp, failed); err != errNoSpread {
			return resp, err
		}
	}
	t.mu.Lock()
	if t.closed || t.pool.ctx.Err() != nil {
		t.mu.Unlock()
		return nil, errSessionClosed
	}
	if err := r.Context().Err(); err != nil {
		t.mu.Unlock()
		return nil, err
	}
	client := t.client
	var conn *poolConn
	t.pool.mu.Lock()
	if client != nil && client.LeftRequests.Load() > 0 && (client.UnreusableAt.IsZero() || !time.Now().After(client.UnreusableAt)) {
		client.LeftRequests.Add(-1)
		// counted under mu and against Close, or another request of the session
		// that moves on, or another session of the pool that retires the
		// connection, could close it before this one is sent
		conn = client.XmuxConn.(*poolConn)
		if !conn.acquire() {
			conn = nil
		}
	}
	t.pool.mu.Unlock()
	for try := 0; conn == nil; try++ {
		t.pool.mu.Lock()
		if t.pool.closed || r.Context().Err() != nil {
			t.pool.mu.Unlock()
			t.mu.Unlock()
			if err := r.Context().Err(); err != nil {
				return nil, err
			}
			return nil, errSessionClosed
		}
		var next *splithttp.XmuxClient
		if client != nil {
			// Moving off a connection: an established one, such as the
			// spare, takes the requests without a dial.
			if c := t.pool.xmux.GetExistingXmuxClient(r.Context()); c != nil && c != client && c.XmuxConn.(*poolConn).ready(time.Now()) {
				next = c
			}
		}
		if next == nil {
			next = t.pool.xmux.GetXmuxClient(r.Context())
		}
		next.AddRunning()
		next.LeftRequests.Add(-1)
		t.pool.mu.Unlock()
		if client != nil {
			client.DoneRunning()
			t.movedAt = time.Now()
		}
		client, t.client = next, next
		t.takenAt, t.servedAt = time.Now(), time.Time{}
		conn = client.XmuxConn.(*poolConn)
		if !conn.acquire() {
			// XMUX drops a closed connection on the next pick; a last try
			// is sent anyway and left to the retries of the caller
			if try < 3 {
				conn = nil
				continue
			}
			conn.mu.Lock()
			conn.inflight++
			conn.mu.Unlock()
		}
	}
	t.mu.Unlock()
	if selected, ok := r.Context().Value(poolConnKey{}).(func(*poolConn)); ok {
		selected(conn)
	}
	// After the request counts on its connection, which is then not idle.
	t.pool.keepSpare(r)
	resp, err := conn.roundTrip(r)
	if err == nil {
		resp.Body = &servedBody{ReadCloser: resp.Body, ctx: r.Context(), served: sync.OnceFunc(func() {
			t.mu.Lock()
			now := time.Now()
			conn.mu.Lock()
			if !now.Before(conn.servedAt) {
				conn.servedAt, conn.servedBy = now, t
			}
			conn.mu.Unlock()
			if t.client == client {
				t.servedAt = maxTime(t.servedAt, now)
			}
			t.mu.Unlock()
		})}
	}
	return resp, err
}

type servedBody struct {
	io.ReadCloser
	ctx    context.Context
	served func()
	failed atomic.Bool
}

func (b *servedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.served()
	} else if err != nil {
		b.failed.Store(true)
	}
	return n, err
}

func (b *servedBody) Close() error {
	canceled := b.ctx != nil && b.ctx.Err() != nil
	err := b.ReadCloser.Close()
	if err == nil && !canceled && !b.failed.Load() {
		b.served()
	}
	return err
}

// stalled reports whether the connection had served nothing since the request
// began, that is, was dead rather than merely slow.
func (t *sessionTransport) stalled(began time.Time, selected ...*poolConn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(selected) > 0 {
		if selected[0] == nil {
			return false
		}
		if t.client == nil || t.client.XmuxConn != selected[0] {
			// a spread connection: dead when it served nothing since began
			c := selected[0]
			c.mu.Lock()
			dead := !c.servedAt.After(began)
			c.mu.Unlock()
			if dead {
				c.Close()
			}
			return dead
		}
	} else if began.Before(t.movedAt) {
		return false
	}
	if t.client == nil {
		return false
	}
	conn := t.client.XmuxConn.(*poolConn)
	conn.mu.Lock()
	servedAt := t.servedAt
	if conn.servedBy != t {
		servedAt = maxTime(servedAt, conn.servedAt)
	}
	conn.mu.Unlock()
	now := time.Now()
	if servedAt.IsZero() && now.Sub(t.takenAt) < stalledGrace {
		return false
	}
	// A connection that has served nothing since this request began is dead
	// already, so its first stall retires it. One that still served other
	// requests meanwhile keeps the two-strikes rule against a lone hiccup.
	if servedAt.After(began) {
		slow := now.Sub(t.slowAt) < 3*time.Second
		t.slowAt = now
		if !slow || now.Sub(t.retiredAt) < 2*time.Second {
			return false
		}
	}
	t.retiredAt = now
	t.client.XmuxConn.(*poolConn).Close()
	return !servedAt.After(began)
}

func (t *sessionTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	if t.client != nil {
		t.client.DoneRunning()
		t.client = nil
	}
}
