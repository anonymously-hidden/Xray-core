package xdrive

import (
	"cmp"
	"context"
	gotls "crypto/tls"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/net/cnc"
	"github.com/xtls/xray-core/common/utils"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion"
	"github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/tls"
)

var (
	errStalled       = errors.New("the request made no progress")
	errSessionClosed = errors.New("the session is closed")
)

type serviceTransport struct {
	ctx             context.Context // Owns the sockets opened by this transport.
	cancel          context.CancelFunc
	plain, parallel *http.Transport
	h1Hosts         *sync.Map // Hosts observed negotiating HTTP/1 on the single-connection transport.
	secure          interface {
		http.RoundTripper
		CloseIdleConnections()
	}
	stall time.Duration
}

func (t *serviceTransport) IsClosed() bool { return t.ctx.Err() != nil }

func (t *serviceTransport) Close() error {
	t.cancel()
	return nil
}

func (t *serviceTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return t.roundTrip(r, func() {})
}

func (t *serviceTransport) roundTrip(r *http.Request, release func()) (*http.Response, error) {
	next := t.secure
	if r.URL.Scheme != "https" {
		next = t.plain
	} else if _, known := t.h1Hosts.Load(r.URL.Host); t.parallel != nil && known {
		next = t.parallel
	}
	ctx, cancel := context.WithCancelCause(r.Context())
	alive := t.watch(ctx, cancel)
	r = r.WithContext(ctx)
	r.Header = r.Header.Clone()
	utils.TryDefaultHeadersWith(r.Header, "fetch")
	upload := func(body io.ReadCloser, err error) (io.ReadCloser, error) {
		if body != nil && body != http.NoBody {
			body = &progressBody{Reader: body, close: body.Close, alive: alive}
		}
		return body, err
	}
	r.Body, _ = upload(r.Body, nil)
	if getBody := r.GetBody; getBody != nil {
		r.GetBody = func() (io.ReadCloser, error) { return upload(getBody()) }
	}
	var body io.ReadCloser // The response owns the lease; a failed RoundTrip has no body.
	finish := sync.OnceValue(func() error {
		defer release()
		defer cancel(nil)
		return common.Close(body)
	})
	resp, err := next.RoundTrip(r)
	if err != nil {
		finish()
		if context.Cause(ctx) == errStalled {
			return nil, errStalled
		}
		return nil, err
	}
	if next == t.secure && t.parallel != nil && resp.ProtoMajor == 1 {
		t.h1Hosts.Store(r.URL.Host, true)
	}
	wholeGET := r.Method == http.MethodGet && r.Header.Get("Range") == "" && resp.StatusCode == http.StatusOK
	alive(resp.StatusCode != http.StatusPartialContent && !wholeGET) // Only a tagged span has a shorter watchdog to retain.
	body = resp.Body
	resp.Body = &progressBody{Reader: body, alive: alive, ctx: ctx, close: finish}
	return resp, nil
}

func (t *serviceTransport) watch(ctx context.Context, cancel context.CancelCauseFunc) func(bool) {
	rescue, _ := ctx.Value(spanRescueKey{}).(func() bool)
	gap := t.stall
	if rescue != nil {
		gap = min(gap, 5*time.Second)
	}
	last := time.Now()
	var mu sync.Mutex
	go func(next time.Time) {
		for sleepContext(ctx, time.Until(next)) == nil {
			mu.Lock()
			idle := time.Since(last)
			switch {
			case idle < gap:
				next = last.Add(gap)
			case idle >= t.stall || rescue != nil && rescue():
				cancel(errStalled)
			default:
				next = last.Add(min(t.stall, idle+gap))
			}
			mu.Unlock()
		}
	}(last.Add(gap))
	return func(ordinary bool) {
		mu.Lock()
		if ordinary {
			gap = t.stall
		}
		last = time.Now()
		mu.Unlock()
	}
}

func (t *serviceTransport) CloseIdleConnections() {
	t.plain.CloseIdleConnections()
	t.secure.CloseIdleConnections()
	if t.parallel != nil {
		t.parallel.CloseIdleConnections()
	}
}

type progressBody struct {
	io.Reader
	alive func(bool)
	close func() error
	ctx   context.Context // response request context; nil for uploads
}

func (b *progressBody) Read(p []byte) (int, error) {
	// Short upload reads keep the stall timer meaningful on slow connections.
	if b.ctx == nil {
		p = p[:min(len(p), 16<<10)]
	}
	n, err := b.Reader.Read(p)
	if n > 0 {
		b.alive(false)
	}
	if err != nil && err != io.EOF && b.ctx != nil && context.Cause(b.ctx) == errStalled {
		err = errStalled // HTTP/2 can surface a stall as ordinary cancellation.
	}
	return n, err
}

func (b *progressBody) Close() error { return b.close() }

// Either lifetime can cancel the dial and handshake. The protocol owns a successful socket.
func serviceDial[T any](owner context.Context, settings *internet.MemoryStreamConfig, handshake func(context.Context, net.Conn, net.Destination) (T, error)) func(context.Context, string, string) (T, error) {
	return func(ctx context.Context, network, addr string) (zero T, err error) {
		if err := owner.Err(); err != nil {
			return zero, err
		}
		ctx, cancel := context.WithCancel(ctx)
		defer context.AfterFunc(owner, cancel)()
		defer cancel()
		host, err := net.ParseDestination(network + ":" + addr)
		if err != nil {
			return zero, errors.New("bad address: ", addr).Base(err)
		}
		target := host // The front replaces the address, not the transport network or TLS name.
		if front := settings.Destination; front != nil {
			target.Address, target.Port = front.Address, cmp.Or(front.Port, host.Port)
		}
		var conn net.Conn
		switch mask := settings.FinalMask; {
		case mask == nil:
			conn, err = internet.DialSystem(ctx, target, settings.SocketSettings)
		case target.Network == net.Network_UDP:
			conn, err = mask.DialUDP(ctx, target)
		default:
			conn, err = mask.DialTCP(ctx, target)
		}
		if err != nil {
			return zero, errors.New("failed to dial to dest").Base(err)
		}
		return handshake(ctx, conn, host)
	}
}

func newServiceClient(connCtx context.Context, h1Hosts *sync.Map, streamSettings *internet.MemoryStreamConfig, stall time.Duration, maxConns int) *http.Client {
	connCtx, cancel := context.WithCancel(connCtx)
	streamSettings = cmp.Or(streamSettings, &internet.MemoryStreamConfig{})
	tlsConfig := cmp.Or(tls.ConfigFromStreamSettings(streamSettings), &tls.Config{})
	config, _ := streamSettings.ProtocolSettings.(*Config)
	period := config.GetXmux().GetHKeepAlivePeriod()
	h3KeepAlive := time.Duration(max(period, 0)) * time.Second
	clientTLS := tls.Client
	if fp := tls.GetFingerprint(tlsConfig.Fingerprint); fp != nil {
		clientTLS = func(c net.Conn, cfg *gotls.Config) net.Conn { return tls.UClient(c, cfg, fp) }
	}
	dialTCP := func(secure bool) func(context.Context, string, string) (net.Conn, error) {
		return serviceDial(connCtx, streamSettings, func(ctx context.Context, raw net.Conn, host net.Destination) (net.Conn, error) {
			closeSocket := sync.OnceValue(raw.Close)
			stop := context.AfterFunc(connCtx, func() { closeSocket() })
			conn := &ctxConn{Conn: raw, stop: stop, closeSocket: closeSocket}
			if !secure {
				return conn, nil
			}
			tlsConn := clientTLS(conn, tlsConfig.GetTLSConfig(tls.WithDestination(host))).(tls.Interface)
			ctx, cancel := context.WithTimeout(ctx, stall)
			defer cancel()
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, err
			}
			return tlsConn, nil
		})
	}
	plain := &http.Transport{
		DialContext: dialTCP(false), IdleConnTimeout: net.ConnIdleTimeout,
		MaxIdleConnsPerHost: maxConns, MaxIdleConns: maxConns, MaxConnsPerHost: maxConns,
	}
	t := &serviceTransport{ctx: connCtx, cancel: cancel, plain: plain, h1Hosts: h1Hosts, stall: stall}
	defer context.AfterFunc(connCtx, t.CloseIdleConnections)
	// No server offers h3 by ALPN over TCP, so use it only when asked for alone.
	if !slices.Equal(tlsConfig.NextProtocol, []string{http3.NextProtoH3}) {
		h2 := &http.HTTP2Config{SendPingTimeout: 1500 * time.Millisecond, PingTimeout: 2500 * time.Millisecond}
		if config.GetService() == "google-drive" || config.GetService() == "Google Drive" {
			h2.SendPingTimeout, h2.PingTimeout = 10*time.Second, driveReconnectGrace
		} else if period != 0 {
			h2.PingTimeout = 0
		}
		if period != 0 {
			h2.SendPingTimeout = h3KeepAlive
		}
		t.parallel = plain.Clone()
		t.parallel.DialTLSContext, t.parallel.ForceAttemptHTTP2, t.parallel.HTTP2 = dialTCP(true), true, h2
		primary := t.parallel.Clone()
		primary.MaxConnsPerHost = 1
		t.secure = primary
		return &http.Client{Transport: t}
	}
	quicParams := cmp.Or(streamSettings.QuicParams, &internet.QuicParams{}) // Empty BBR profile selects standard.
	h3 := &http3.Transport{QUICConfig: &quic.Config{
		InitialStreamReceiveWindow:     quicParams.InitStreamReceiveWindow,
		MaxStreamReceiveWindow:         quicParams.MaxStreamReceiveWindow,
		InitialConnectionReceiveWindow: quicParams.InitConnReceiveWindow,
		MaxConnectionReceiveWindow:     quicParams.MaxConnReceiveWindow,
		MaxIdleTimeout:                 time.Duration(quicParams.MaxIdleTimeout) * time.Second,
		KeepAlivePeriod:                cmp.Or(time.Duration(quicParams.KeepAlivePeriod)*time.Second, h3KeepAlive),
		MaxIncomingStreams:             quicParams.MaxIncomingStreams,
		DisablePathMTUDiscovery:        quicParams.DisablePathMtuDiscovery || (runtime.GOOS != "linux" && runtime.GOOS != "windows" && runtime.GOOS != "darwin"),
		ChromeParrot:                   !quicParams.DisableChromeParrot,
	}}

	h3.Dial = func(ctx context.Context, addr string, _ *gotls.Config, cfg *quic.Config) (*quic.Conn, error) {
		if cc := quicParams.Congestion; cc != "" && cc != congestion.TypeBBR && cc != congestion.TypeReno {
			return nil, errors.New("unsupported congestion control over h3: ", cc)
		}
		dialQUIC := serviceDial(connCtx, streamSettings, func(ctx context.Context, conn net.Conn, host net.Destination) (*quic.Conn, error) {
			if stream, ok := conn.(*cnc.Connection); ok {
				fake := &internet.FakePacketConn{Conn: stream}
				conn = &net.PacketConnWrapper{PacketConn: fake, Dest: fake.LocalAddr()}
			}
			packet, ok := conn.(*net.PacketConnWrapper)
			if !ok {
				conn.Close()
				return nil, fmt.Errorf("unexpected UDP connection type %T", conn)
			}

			// Set SNI before pinning/ECH; ChromeParrot rejects the server-only certificate callback.
			gotlsConfig := tlsConfig.GetTLSConfig(tls.WithDestination(host))
			gotlsConfig.GetCertificate = nil
			tr := &quic.Transport{Conn: packet.PacketConn, DisableGSO: quicParams.DisableGSO}
			if !quicParams.DisableChromeParrot {
				tr.ConnectionIDGenerator = quic.ZeroLengthConnectionIDGenerator{}
			}
			// Some packet connections ignore deadlines: close the socket before joining QUIC's receive loop.
			closers := common.ChainedClosable{packet.PacketConn, tr}
			qconn, err := tr.Dial(ctx, packet.RemoteAddr(), gotlsConfig, cfg)
			if err != nil {
				closers.Close()
				return nil, err
			}
			stopConn := context.AfterFunc(connCtx, func() { qconn.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeNoError), "") })
			context.AfterFunc(qconn.Context(), func() {
				stopConn()
				closers.Close()
			})
			congestion.UseConfigured(qconn, quicParams.Congestion, quicParams.BbrProfile)
			return qconn, nil
		})
		return dialQUIC(ctx, "udp", addr)
	}
	t.secure = h3
	return &http.Client{Transport: t}
}

type ctxConn struct {
	net.Conn
	stop        func() bool
	closeSocket func() error
}

func (c *ctxConn) Close() error {
	c.stop()
	return c.closeSocket()
}

// XMUX leases count both the session and response bodies that are still open.
type connPool struct {
	mu     sync.Mutex
	lanes  []connLane // One pinned manager, or one per Drive media connection.
	spread bool
	next   int
	cost   float64 // Last verified pool sample, used until a lane has a fresh one of its own.
	ctx    context.Context
	Close  context.CancelFunc
}

type connLane struct {
	manager   *splithttp.XmuxManager
	pending   atomic.Int64 // Charged until the body closes, including across lease rotation.
	delivered int64        // Tagged wire bytes consumed across overlapping GETs.
	cost      float64      // Nanoseconds per charged byte; guarded by pool.mu.
	sampled   time.Time
}

// Charge the lane until this request's response body closes, even across lease rotation.
func (p *connPool) charge(r *http.Request) (*connLane, int64) {
	if !p.spread {
		return &p.lanes[0], 0
	}
	read, _ := r.Context().Value(mediaReadKey{}).(*mediaRead)
	work := max(0, r.ContentLength) + slotTagBlock
	if read != nil {
		work = int64(read.size) + slotTagBlock
	}
	now := time.Now()
	best, bestScore := p.next, math.Inf(1)
	for off := range p.lanes {
		i := (p.next + off) % len(p.lanes)
		lane := &p.lanes[i]
		if read != nil && lane == read.stalled {
			continue // A retry uses a different connection when there is one.
		}
		cost := cmp.Or(p.cost, 1)
		if read != nil && now.Sub(lane.sampled) < driveTimeout {
			cost = lane.cost
		}
		if score := cost * float64(lane.pending.Load()+work); score < bestScore {
			best, bestScore = i, score
		}
	}
	p.next = (best + 1) % len(p.lanes)
	lane := &p.lanes[best]
	if read != nil {
		read.lane, read.stalled = lane, nil
		before := lane.delivered - slotTagBlock // Include this GET's start cost once in every estimate.
		read.progress = func(bytes int) {
			p.mu.Lock()
			defer p.mu.Unlock()
			lane.sampled = time.Now()
			lane.delivered += int64(bytes)
			p.cost = float64(lane.sampled.Sub(now)) / float64(lane.delivered-before)
			lane.cost = (p.cost + cmp.Or(lane.cost, p.cost)) / 2
		}
	}
	lane.pending.Add(work)
	return lane, work
}

// Positive conns spreads Drive media requests; other backends keep one manager.
func newConnPool(config *splithttp.XmuxConfig, conns int32, newClient func(context.Context) *http.Client) *connPool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &connPool{ctx: ctx, Close: cancel, spread: conns > 0, lanes: make([]connLane, max(1, conns))}
	newConn := func() splithttp.XmuxConn { return newClient(ctx).Transport.(*serviceTransport) }
	maxConnections := config.GetMaxConnections()
	if p.spread {
		maxConnections = &splithttp.RangeConfig{From: 1, To: 1}
	}
	for i := range p.lanes {
		p.lanes[i].manager = splithttp.NewXmuxManager(splithttp.XmuxConfig{
			MaxConnections: maxConnections, MaxConcurrency: config.GetMaxConcurrency(),
			CMaxReuseTimes: config.GetCMaxReuseTimes(), HMaxRequestTimes: config.GetHMaxRequestTimes(),
			HMaxReusableSecs: config.GetHMaxReusableSecs(), HKeepAlivePeriod: config.GetHKeepAlivePeriod(),
		}, newConn)
	}
	return p
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
		common.Close(r.Body)
		return nil, cmp.Or[error](r.Context().Err(), errSessionClosed)
	}
	lane, work := p.charge(r)
	c := t.client // Media never pins a session lease.
	if c == nil || c.XmuxConn.IsClosed() || c.LeftRequests.Load() <= 0 || (!c.UnreusableAt.IsZero() && time.Now().After(c.UnreusableAt)) {
		if c != nil {
			c.DoneRunning()
		}
		c = lane.manager.GetXmuxClient(r.Context())
		if !p.spread {
			c.AddRunning()
			t.client = c
		}
	}
	c.LeftRequests.Add(-1)
	c.AddRunning()
	p.mu.Unlock()
	return c.XmuxConn.(*serviceTransport).roundTrip(r, func() {
		lane.pending.Add(-work)
		c.DoneRunning()
	})
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
