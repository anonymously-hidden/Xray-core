package xdrive

import (
	"cmp"
	"context"
	gotls "crypto/tls"
	"fmt"
	"io"
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
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion/bbr"
	"github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/tls"
)

var errStalled = errors.New("the request made no progress")

type serviceTransport struct {
	plain, parallel *http.Transport
	h1Hosts         *sync.Map // Hosts observed negotiating HTTP/1 on the single-connection transport.
	secure          interface {
		http.RoundTripper
		CloseIdleConnections()
	}
	stall time.Duration
}

// Cancel stalled requests, not long transfers that continue to make progress.
func (t *serviceTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return t.roundTrip(r, func() {})
}

func (t *serviceTransport) roundTrip(r *http.Request, release func()) (*http.Response, error) {
	var next http.RoundTripper = t.plain
	if r.URL.Scheme == "https" {
		next = t.secure
		if _, known := t.h1Hosts.Load(r.URL.Host); t.parallel != nil && known {
			next = t.parallel
		}
	}
	ctx, cancel := context.WithCancelCause(r.Context())
	alive := t.watch(ctx, cancel)
	r = r.WithContext(ctx)
	r.Header = r.Header.Clone()
	utils.TryDefaultHeadersWith(r.Header, "fetch")
	upload := func(body io.ReadCloser) io.ReadCloser {
		if body == nil || body == http.NoBody {
			return body
		}
		return &progressBody{Reader: body, close: body.Close, alive: alive}
	}
	r.Body = upload(r.Body)
	if getBody := r.GetBody; getBody != nil {
		r.GetBody = func() (io.ReadCloser, error) {
			body, err := getBody()
			return upload(body), err
		}
	}
	resp, err := next.RoundTrip(r)
	if err != nil {
		if context.Cause(ctx) == errStalled {
			err = errStalled
		}
		cancel(nil)
		release()
		return nil, err
	}
	if next == t.secure && t.parallel != nil && resp.ProtoMajor == 1 {
		t.h1Hosts.Store(r.URL.Host, true)
	}
	alive(resp.StatusCode != http.StatusPartialContent)
	body := resp.Body
	closeBody := sync.OnceValue(func() error {
		defer release()
		defer cancel(nil)
		return body.Close()
	})
	resp.Body = &progressBody{Reader: body, alive: alive, ctx: ctx, close: closeBody}
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
		err = errStalled // HTTP/2 can report the stall timeout as an ordinary cancellation.
	}
	return n, err
}

func (b *progressBody) Close() error { return b.close() }

// connCtx owns sockets; h1Hosts shares ALPN observations between clients.
func newServiceClient(connCtx context.Context, h1Hosts *sync.Map, streamSettings *internet.MemoryStreamConfig, stall time.Duration, maxConns int) *http.Client {
	streamSettings = cmp.Or(streamSettings, &internet.MemoryStreamConfig{})
	tlsConfig := cmp.Or(tls.ConfigFromStreamSettings(streamSettings), &tls.Config{})
	config, _ := streamSettings.ProtocolSettings.(*Config)
	period := config.GetXmux().GetHKeepAlivePeriod()
	h3KeepAlive := time.Duration(max(period, 0)) * time.Second
	h2 := &http.HTTP2Config{SendPingTimeout: 1500 * time.Millisecond, PingTimeout: 2500 * time.Millisecond}
	if config.GetService() == "google-drive" || config.GetService() == "Google Drive" {
		// A missed PING tears down every in-flight read and write on this connection.
		h2.SendPingTimeout, h2.PingTimeout = 10*time.Second, driveReconnectGrace
	} else if period != 0 {
		h2.PingTimeout = 0
	}
	if period != 0 {
		h2.SendPingTimeout = h3KeepAlive
	}
	clientTLS := tls.Client
	if fp := tls.GetFingerprint(tlsConfig.Fingerprint); fp != nil {
		clientTLS = func(c net.Conn, cfg *gotls.Config) net.Conn { return tls.UClient(c, cfg, fp) }
	}

	// A configured front replaces the address, not the transport network.
	dial := func(ctx context.Context, network, addr string) (net.Conn, net.Destination, error) {
		if err := connCtx.Err(); err != nil {
			return nil, net.Destination{}, err
		}
		host, err := net.ParseDestination(network + ":" + addr)
		if err != nil {
			return nil, host, errors.New("bad address: ", addr).Base(err)
		}

		target := host
		if fronting := streamSettings.Destination; fronting != nil {
			target.Address = fronting.Address
			target.Port = cmp.Or(fronting.Port, target.Port)
		}

		var conn net.Conn
		switch mask := streamSettings.FinalMask; {
		case mask == nil:
			conn, err = internet.DialSystem(ctx, target, streamSettings.SocketSettings)
		case target.Network == net.Network_UDP:
			conn, err = mask.DialUDP(ctx, target)
		default:
			conn, err = mask.DialTCP(ctx, target)
		}
		if err != nil {
			return nil, host, errors.New("failed to dial to dest").Base(err)
		}
		return conn, host, nil
	}

	dialTCP := func(secure bool) func(context.Context, string, string) (net.Conn, error) {
		return func(ctx context.Context, _, addr string) (net.Conn, error) {
			ctx, cancel := context.WithCancel(ctx)
			defer context.AfterFunc(connCtx, cancel)()
			defer cancel()
			raw, host, err := dial(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			conn := &ctxConn{Conn: raw, stop: context.AfterFunc(connCtx, func() { raw.Close() })}
			if !secure {
				return conn, nil
			}
			tlsConn := clientTLS(conn, tlsConfig.GetTLSConfig(tls.WithDestination(host))).(tls.Interface)
			// Bound the handshake independently of connection reuse.
			handshake, finish := context.WithTimeout(ctx, stall)
			defer finish()
			if err := tlsConn.HandshakeContext(handshake); err != nil {
				conn.Close()
				return nil, err
			}
			return tlsConn, nil
		}
	}
	plain := &http.Transport{DialContext: dialTCP(false), IdleConnTimeout: net.ConnIdleTimeout,
		MaxIdleConnsPerHost: maxConns, MaxIdleConns: maxConns, MaxConnsPerHost: maxConns}
	t := &serviceTransport{plain: plain, h1Hosts: h1Hosts, stall: stall}
	// No server offers h3 by ALPN over TCP, so use it only when asked for alone.
	if !slices.Equal(tlsConfig.NextProtocol, []string{http3.NextProtoH3}) {
		t.parallel = plain.Clone()
		t.parallel.DialTLSContext, t.parallel.ForceAttemptHTTP2, t.parallel.HTTP2 = dialTCP(true), true, h2
		primary := t.parallel.Clone()
		primary.MaxConnsPerHost = 1
		t.secure = primary
		return &http.Client{Transport: t}
	}
	quicParams := cmp.Or(streamSettings.QuicParams, &internet.QuicParams{BbrProfile: string(bbr.ProfileStandard)})
	// QUIC idle timeout and connCtx bound unused connections.
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
		ctx, cancel := context.WithCancel(ctx)
		defer context.AfterFunc(connCtx, cancel)()
		defer cancel()
		cc := quicParams.Congestion
		if cc != "" && cc != congestion.TypeBBR && cc != congestion.TypeReno {
			return nil, errors.New("unsupported congestion control over h3: ", cc)
		}

		conn, host, err := dial(ctx, "udp", addr)
		if err != nil {
			return nil, err
		}
		var pktConn net.PacketConn
		var udpAddr net.Addr
		switch c := conn.(type) {
		case *net.PacketConnWrapper:
			pktConn, udpAddr = c.PacketConn, c.RemoteAddr()
		case *cnc.Connection:
			pktConn = &internet.FakePacketConn{Conn: c}
			udpAddr = &net.UDPAddr{IP: []byte{0, 0, 0, 0}}
		default:
			conn.Close()
			return nil, fmt.Errorf("unexpected UDP connection type %T", c)
		}

		// Set SNI before pinning/ECH; ChromeParrot rejects the server-only certificate callback.
		gotlsConfig := tlsConfig.GetTLSConfig(tls.WithDestination(host))
		gotlsConfig.GetCertificate = nil
		tr := &quic.Transport{Conn: pktConn, DisableGSO: quicParams.DisableGSO}
		if !quicParams.DisableChromeParrot {
			tr.ConnectionIDGenerator = quic.ZeroLengthConnectionIDGenerator{}
		}
		// Some packet conns ignore read deadlines. Close the socket before
		// joining QUIC's receive loop on both dial failure and connection exit.
		closers := common.ChainedClosable{pktConn, tr}
		qconn, err := tr.Dial(ctx, udpAddr, gotlsConfig, cfg)
		if err != nil {
			closers.Close()
			return nil, err
		}
		stopConn := context.AfterFunc(connCtx, func() { qconn.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeNoError), "") })
		context.AfterFunc(qconn.Context(), func() {
			stopConn()
			closers.Close()
		})
		congestion.UseConfigured(qconn, cc, quicParams.BbrProfile)
		return qconn, nil
	}
	t.secure = h3
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

var errSessionClosed = errors.New("the session is closed")

// XMUX selects a lease once, not repeatedly while looking for a connection.
// Running references cover both the session and any response bodies still open.
type connPool struct {
	mu     sync.Mutex
	lanes  []connLane // One pinned manager, or one per Drive media connection.
	spread bool
	next   int
	cost   float64 // Last verified pool sample, used until a lane has a fresh one of its own.
	ctx    context.Context
	cancel context.CancelFunc
}

type connLane struct {
	manager   *splithttp.XmuxManager
	pending   atomic.Int64 // Charged until the body closes, including across lease rotation.
	delivered int64        // Tagged wire bytes consumed across overlapping GETs.
	cost      float64      // Nanoseconds per charged byte; guarded by pool.mu.
	sampled   time.Time
}

// Estimate completion time from outstanding work and recent verified downloads.
func (p *connPool) pickMedia(work int64, read *mediaRead) *connLane {
	now := time.Now()
	score := func(l *connLane) float64 {
		cost := cmp.Or(p.cost, 1)
		if read != nil && now.Sub(l.sampled) < driveTimeout {
			cost = l.cost
		}
		return cost * float64(l.pending.Load()+work)
	}
	best := p.next
	for off := 1; off < len(p.lanes); off++ {
		i := (p.next + off) % len(p.lanes)
		if score(&p.lanes[i]) < score(&p.lanes[best]) {
			best = i
		}
	}
	p.next = (best + 1) % len(p.lanes)
	lane := &p.lanes[best]
	if read != nil {
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
	return lane
}

type poolConn struct {
	*serviceTransport
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *poolConn) IsClosed() bool { return c.ctx.Err() != nil }

func (c *poolConn) Close() error {
	c.cancel()
	return nil
}

// Positive conns spreads Drive media requests; other backends keep one manager.
func newConnPool(config *splithttp.XmuxConfig, conns int32, newClient func(context.Context) *http.Client) *connPool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &connPool{ctx: ctx, cancel: cancel, spread: conns > 0, lanes: make([]connLane, max(1, conns))}
	newConn := func() splithttp.XmuxConn {
		ctx, cancel := context.WithCancel(p.ctx)
		c := &poolConn{ctx: ctx, cancel: cancel, serviceTransport: newClient(ctx).Transport.(*serviceTransport)}
		context.AfterFunc(ctx, c.CloseIdleConnections)
		return c
	}
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

func (p *connPool) Close() { p.cancel() }

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
	c := t.client
	lane := &p.lanes[0]
	var work int64
	if p.spread {
		read, _ := r.Context().Value(mediaReadKey{}).(*mediaRead)
		work = slotTagBlock + max(0, r.ContentLength)
		if read != nil {
			work = int64(slotTagBlock + read.size)
		}
		lane = p.pickMedia(work, read)
		lane.pending.Add(work)
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
	p.mu.Unlock()
	return c.XmuxConn.(*poolConn).roundTrip(r, func() {
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
