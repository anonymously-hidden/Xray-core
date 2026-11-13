package xdrive

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/dice"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/transport/internet"
)

// testDriveIO is a nil-by-default test hook observing every Drive request
var testDriveIO func(method, target string, body []byte)

const (
	flatSeparator = "~"

	driveBoundary         = "xdrive-boundary"
	drivePageSize         = 1000
	driveMaxAttempts      = 8
	driveMaxInflight      = 64
	driveReservedInflight = 8
	driveInlineLimit      = 12000
	// driveTimeout ends a request that makes no progress for that long
	driveTimeout = 60 * time.Second
)

var (
	driveMaxBackoff     = 8 * time.Second
	driveTokenURL       = "https://oauth2.googleapis.com/token"
	driveFilesURL       = "https://www.googleapis.com/drive/v3/files"
	driveUploadURL      = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name"
	driveInitialBackoff = 200 * time.Millisecond
	driveCooldown       = 2 * time.Second
	driveSlotRead       = 18 * time.Second
	driveSlotWrite      = 20 * time.Second
	slotReadHedge       = 900 * time.Millisecond
	tokenRefreshBackoff = 30 * time.Second
	maxTokenBackoff     = 5 * time.Minute
	driveMaxResponse    = 32 << 20
	driveMaxAnnounce    = 1 << 20
)

// maxBodyKey lets a caller tighten the response-body cap (announcements).
type maxBodyKey struct{}

// tokenFlight is one shared foreground token fetch; waiters read its result
// after done closes instead of starting their own fetch.
type tokenFlight struct {
	done     chan struct{}
	token    string
	lifetime time.Duration
	err      error
}

type laneKey struct{}

// bodyKey marks media-body requests so they ride the bulk connection, keeping
// small reads/acks off the connection saturated by large transfers.
type bodyKey struct{}

// bodyProgressKey carries the callback for a media read's hedge timer.
type bodyProgressKey struct{}

type attemptKey struct{}

// cleanupKey marks session-cleanup requests (the child deletions of Delete and
// the Listener's drop/collect) so a rate limit they hit does not cool live
// traffic.
type cleanupKey struct{}

// namesOnlyKey asks query() for ids and names without descriptions, for the
// listings that never read Entry.Inline (resolveID, Delete children, collect).
type namesOnlyKey struct{}

// Cleanup is deliberately gentler than live traffic: few parallel deletes, a
// low sustained rate, and retries that never cool the live path. The values are
// copied into the storage at construction so tests tune their own instance.
const (
	cleanupParallel     = 2
	cleanupRatePerSec   = 5.0
	cleanupRetryDelay   = 2 * time.Second
	cleanupMaxRetries   = 5
	cleanupSessionPause = 1 * time.Second
)

// cleanupRateLimited is returned by do() when a cleanup request is rate limited.
// The cleanup caller retries it later; live traffic is not cooled.
type cleanupRateLimited struct{ detail string }

func (e cleanupRateLimited) Error() string {
	return "Drive rate limited a cleanup request: " + e.detail
}

func isCleanupRateLimited(err error) bool {
	_, ok := err.(cleanupRateLimited)
	return ok
}

type driveStorage struct {
	*driveShared
	// a session has a client and inflight of its own, see session
	client       *http.Client
	bulkClient   *http.Client
	mediaClients []*http.Client
	inflight     chan struct{}
	reserved     chan struct{}
	mediaRescue  chan struct{}
	release      func()
	// readHedge/budget are copied per instance so tests never mutate globals
	// that other goroutines read.
	readHedge       time.Duration
	readHedgeMu     sync.Mutex
	readHedgeTokens int
	readHedgeCount  int
}

// driveShared is what the sessions of a storage have in common
type driveShared struct {
	conns        *connPool
	bulkConns    *connPool
	mediaConns   [driveMediaLanes]*connPool
	folder       string
	clientID     string
	clientSecret string
	refreshToken string
	tokenClient  *http.Client
	tokenURL     string
	filesURL     string
	uploadURL    string
	backoff      time.Duration

	tokenMu       sync.Mutex
	token         string
	tokenExpiry   time.Time
	tokenLifetime time.Duration
	refreshing    bool
	authErr       error
	flight        *tokenFlight
	tokenRetryAt  time.Time
	tokenBackoff  time.Duration
	lastTokenErr  error

	idMu   sync.Mutex
	ids    map[string]string
	dupIDs map[string][]string

	coolMu        sync.Mutex
	coolUntil     time.Time
	lastThrottle  time.Time
	lastRateLimit time.Time
	throttle      atomic.Int64
	timing        *timingAggregator
	idPoolState

	// cleanup token bucket and retry tuning, shared by the sessions of a
	// storage; instance fields so tests never mutate package-level values.
	cleanupMu         sync.Mutex
	cleanupTokens     float64
	cleanupAt         time.Time
	cleanupRate       float64
	cleanupParallel   int
	cleanupRetryDelay time.Duration
	cleanupMaxRetries int

	// keep-warm keeper; intervals are instance fields so tests never mutate
	// package-level values other goroutines read.
	owner          *driveStorage
	keepWarmEvery  time.Duration
	keepWarmWindow time.Duration
	trafficMu      sync.Mutex
	lastTraffic    time.Time
	lastRequest    time.Time
	keeperMu       sync.Mutex
	keeperRunning  bool
	keeperClosed   bool
	keeperCancel   context.CancelFunc
	keeperDone     chan struct{}

	workMu     sync.Mutex
	workCtx    context.Context
	workCancel context.CancelFunc
	workWG     sync.WaitGroup
	workClosed bool
}

func newDriveStorage(streamSettings *internet.MemoryStreamConfig, config *Config) (*driveStorage, error) {
	if config.RemoteFolder == "" {
		return nil, errors.New(`empty "remoteFolder", it must be a Google Drive folder id`)
	}
	if len(config.Secrets) != 3 {
		return nil, errors.New("Google Drive needs 3 secrets in order of ClientID, ClientSecret, RefreshToken")
	}
	for i, secret := range config.Secrets {
		if secret == "" {
			return nil, errors.New("Google Drive secret ", i, " is empty")
		}
	}

	h1Hosts := new(sync.Map)
	client := newServiceClient(context.Background(), h1Hosts, streamSettings, driveTimeout, driveMaxInflight)
	st := &driveStorage{
		driveShared: &driveShared{
			conns: newConnPool(config.Xmux, func(connCtx context.Context) *http.Client {
				return newServiceClient(connCtx, h1Hosts, streamSettings, driveTimeout, driveMaxInflight)
			}),
			bulkConns: newConnPool(config.Xmux, func(connCtx context.Context) *http.Client {
				return newServiceClient(connCtx, h1Hosts, streamSettings, driveTimeout, driveMaxInflight)
			}),
			folder:       config.RemoteFolder,
			clientID:     config.Secrets[0],
			clientSecret: config.Secrets[1],
			refreshToken: config.Secrets[2],
			tokenClient:  client,
			tokenURL:     driveTokenURL,
			filesURL:     driveFilesURL,
			uploadURL:    driveUploadURL,
			backoff:      driveInitialBackoff,
			ids:          make(map[string]string),
			timing:       newTimingAggregator(),

			cleanupRate:       cleanupRatePerSec,
			cleanupParallel:   cleanupParallel,
			cleanupRetryDelay: cleanupRetryDelay,
			cleanupMaxRetries: cleanupMaxRetries,
		},
		client:          client,
		inflight:        make(chan struct{}, driveMaxInflight),
		reserved:        make(chan struct{}, driveReservedInflight),
		mediaRescue:     make(chan struct{}, driveMediaLanes),
		readHedge:       slotReadHedge,
		readHedgeTokens: 3,
	}
	st.driveShared.owner = st
	for i := range st.mediaConns {
		st.mediaConns[i] = newConnPool(config.Xmux, func(connCtx context.Context) *http.Client {
			return newServiceClient(connCtx, h1Hosts, streamSettings, driveTimeout, driveMaxInflight)
		})
	}
	st.driveShared.keepWarmEvery = 4 * time.Minute
	st.driveShared.keepWarmWindow = 3 * time.Hour
	st.driveShared.poolRing = 2 * (max(paramsForStorage(config, st).concurrency, 2) + 1)
	st.noteTraffic()
	return st, nil
}

// session returns a storage for one session, over the connections of XMUX
// until Close, with the token and ids of s
func (s *driveStorage) session() Storage {
	s.startKeeper()
	transport := &sessionTransport{pool: s.conns}
	bulkTransport := &sessionTransport{pool: s.bulkConns}
	st := &driveStorage{
		driveShared:     s.driveShared,
		client:          &http.Client{Transport: transport},
		bulkClient:      &http.Client{Transport: bulkTransport},
		inflight:        make(chan struct{}, driveMaxInflight),
		reserved:        make(chan struct{}, driveReservedInflight),
		mediaRescue:     make(chan struct{}, driveMediaLanes),
		readHedge:       s.readHedge,
		readHedgeTokens: 3,
	}
	transports := []*sessionTransport{transport, bulkTransport}
	for _, pool := range s.mediaConns {
		if pool == nil {
			continue
		}
		t := &sessionTransport{pool: pool}
		transports = append(transports, t)
		st.mediaClients = append(st.mediaClients, &http.Client{Transport: t})
	}
	st.release = func() {
		for _, t := range transports {
			t.Close()
		}
	}
	return st
}

func flatten(name string) string {
	return strings.ReplaceAll(name, "/", flatSeparator)
}

func quoteDriveValue(value string) string {
	return strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value)
}

type authError struct{ code string }

func (e authError) Error() string {
	return "Google rejected the OAuth credentials (" + e.code + "); create a new refresh token, see the XDRIVE setup guide"
}

func isAuthError(err error) bool {
	_, ok := err.(authError)
	return ok
}

func (s *driveStorage) accessToken(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.tokenMu.Lock()
	if s.isWorkClosed() {
		s.tokenMu.Unlock()
		return "", context.Canceled
	}
	if s.authErr != nil {
		err := s.authErr
		s.tokenMu.Unlock()
		return "", err
	}
	if s.token != "" && time.Now().Before(s.tokenExpiry) {
		if s.tokenLifetime > 0 && time.Until(s.tokenExpiry) < s.tokenLifetime/5 && !s.refreshing && time.Now().After(s.tokenRetryAt) {
			if refreshCtx, finish, ok := s.beginWork(30 * time.Second); ok {
				s.refreshing = true
				go func() { defer finish(); s.refreshTokenBackground(refreshCtx) }()
			}
		}
		token := s.token
		s.tokenMu.Unlock()
		return token, nil
	}
	if f := s.flight; f != nil {
		s.tokenMu.Unlock()
		select {
		case <-f.done:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		if f.err != nil {
			return "", f.err
		}
		return f.token, nil
	}
	if time.Now().Before(s.tokenRetryAt) && s.lastTokenErr != nil {
		err := s.lastTokenErr
		s.tokenMu.Unlock()
		return "", err
	}
	f := &tokenFlight{done: make(chan struct{})}
	fetchCtx, finish, ok := s.beginWork(30 * time.Second)
	if !ok {
		s.tokenMu.Unlock()
		return "", context.Canceled
	}
	s.flight = f
	s.tokenMu.Unlock()

	go func() {
		defer finish()
		token, lifetime, err := s.fetchToken(fetchCtx)

		s.tokenMu.Lock()
		s.flight = nil
		f.token, f.lifetime, f.err = token, lifetime, err
		if err != nil {
			s.lastTokenErr = err
			if isAuthError(err) {
				s.authErr = err
				errors.LogError(ctx, err)
			}
			s.noteTokenFailure()
		} else {
			s.storeToken(token, lifetime)
			s.tokenBackoff = 0
			s.tokenRetryAt = time.Time{}
			s.lastTokenErr = nil
		}
		close(f.done)
		s.tokenMu.Unlock()
	}()
	select {
	case <-f.done:
		return f.token, f.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// noteTokenFailure backs off further token fetches so a failing endpoint is
// not hit once per API call; called with tokenMu held.
func (s *driveStorage) noteTokenFailure() {
	if s.tokenBackoff == 0 {
		s.tokenBackoff = tokenRefreshBackoff
	} else {
		s.tokenBackoff = min(2*s.tokenBackoff, maxTokenBackoff)
	}
	s.tokenRetryAt = time.Now().Add(s.tokenBackoff)
}

func (s *driveStorage) storeToken(token string, lifetime time.Duration) {
	s.token = token
	s.tokenLifetime = lifetime
	expiry := lifetime
	if expiry > time.Minute {
		expiry -= time.Minute
	}
	s.tokenExpiry = time.Now().Add(expiry)
}

func (s *driveStorage) refreshTokenBackground(ctx context.Context) {
	token, lifetime, err := s.fetchToken(ctx)

	s.tokenMu.Lock()
	defer s.tokenMu.Unlock()
	s.refreshing = false
	if err != nil {
		if isAuthError(err) && s.authErr == nil {
			s.authErr = err
			errors.LogError(ctx, err)
		}
		s.lastTokenErr = err
		s.noteTokenFailure()
		return
	}
	if s.authErr == nil {
		s.storeToken(token, lifetime)
		s.tokenBackoff = 0
		s.tokenRetryAt = time.Time{}
		s.lastTokenErr = nil
	}
}

func (s *driveStorage) fetchToken(ctx context.Context) (string, time.Duration, error) {
	start := time.Now()
	defer func() {
		if s.timing != nil {
			s.timing.record(kindToken, timingSample{total: time.Since(start)})
		}
	}()
	form := url.Values{
		"client_id":     {s.clientID},
		"client_secret": {s.clientSecret},
		"refresh_token": {s.refreshToken},
		"grant_type":    {"refresh_token"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, errors.New("failed to build the token request").Base(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.tokenClient.Do(req)
	if err != nil {
		return "", 0, errors.New("failed to refresh the access token").Base(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", 0, errors.New("failed to read the token response").Base(err)
	}
	if resp.StatusCode != http.StatusOK {
		var rejected struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(body, &rejected) == nil {
			switch rejected.Error {
			case "invalid_grant", "invalid_client", "unauthorized_client":
				return "", 0, authError{rejected.Error}
			}
		}
		return "", 0, errors.New("the token endpoint answered ", resp.StatusCode, ": ", string(body))
	}

	var parsed struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", 0, errors.New("failed to parse the token response").Base(err)
	}
	if parsed.AccessToken == "" {
		return "", 0, errors.New("the token endpoint returned no access token")
	}
	return parsed.AccessToken, time.Duration(parsed.ExpiresIn) * time.Second, nil
}

func jitter(backoff time.Duration) time.Duration {
	half := backoff / 2
	if half <= 0 {
		return backoff
	}
	return half + time.Duration(dice.RollInt63n(int64(half)))
}

func rateLimited(payload []byte) bool {
	var parsed struct {
		Error struct {
			Status string `json:"status"`
			Errors []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"error"`
	}
	if json.Unmarshal(payload, &parsed) != nil {
		return false
	}
	for _, item := range parsed.Error.Errors {
		switch item.Reason {
		case "rateLimitExceeded", "userRateLimitExceeded", "sharingRateLimitExceeded":
			return true
		}
	}
	return parsed.Error.Status == "RESOURCE_EXHAUSTED"
}

func retryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

func (s *driveStorage) do(ctx context.Context, method, target, contentType string, body []byte) (int, []byte, error) {
	defer s.noteRequest(ctx)
	backoff := s.backoff
	var lastErr error

	for attempt := 0; attempt < driveMaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			case <-time.After(jitter(backoff)):
			}
			backoff *= 2
			if backoff > driveMaxBackoff {
				backoff = driveMaxBackoff
			}
		}
		coolStart := time.Now()
		if err := s.waitCool(ctx); err != nil {
			return 0, nil, err
		}
		waitCoolDur := time.Since(coolStart)
		if testDriveIO != nil {
			testDriveIO(method, target, body)
		}

		tokenStart := time.Now()
		token, err := s.accessToken(ctx)
		waitTokenDur := time.Since(tokenStart)
		if err != nil {
			if isAuthError(err) {
				return 0, nil, err
			}
			lastErr = err
			continue
		}

		media, _ := ctx.Value(driveMediaKey{}).(*driveMediaRequest)
		pool := s.inflight
		if ctx.Value(laneKey{}) != nil {
			pool = s.reserved
		}
		if media != nil && media.rescue && s.mediaRescue != nil {
			// A stalled bulk window must not queue its own recovery, nor take
			// the permits reserved for head descriptions and acknowledgements.
			pool = s.mediaRescue
		}
		poolStart := time.Now()
		select {
		case pool <- struct{}{}:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
		waitPoolDur := time.Since(poolStart)

		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		attemptCtx, cancel := ctx, context.CancelFunc(func() {})
		if d, ok := ctx.Value(attemptKey{}).(time.Duration); ok {
			attemptCtx, cancel = context.WithTimeout(ctx, d)
		}
		req, err := http.NewRequestWithContext(attemptCtx, method, target, reader)
		if err != nil {
			cancel()
			<-pool
			return 0, nil, errors.New("failed to build a Drive request").Base(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		if media != nil {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", media.start, media.end-1))
			req.Header.Set("Accept-Encoding", "identity")
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}

		var tr struct {
			mu                                                 sync.Mutex
			connAt, connStart, connDone, tlsDone, wrote, first time.Time
			reused, gotConn                                    bool
		}
		trace := &httptrace.ClientTrace{
			GotConn: func(i httptrace.GotConnInfo) {
				tr.mu.Lock()
				tr.connAt, tr.reused, tr.gotConn = time.Now(), i.Reused, true
				tr.mu.Unlock()
			},
			ConnectStart: func(_, _ string) {
				tr.mu.Lock()
				tr.connStart = time.Now()
				tr.mu.Unlock()
			},
			ConnectDone: func(_, _ string, _ error) {
				tr.mu.Lock()
				tr.connDone = time.Now()
				tr.mu.Unlock()
			},
			TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
				tr.mu.Lock()
				tr.tlsDone = time.Now()
				tr.mu.Unlock()
			},
			WroteRequest: func(httptrace.WroteRequestInfo) {
				tr.mu.Lock()
				tr.wrote = time.Now()
				tr.mu.Unlock()
			},
			GotFirstResponseByte: func() {
				tr.mu.Lock()
				tr.first = time.Now()
				tr.mu.Unlock()
			},
		}
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

		httpc := s.client
		if ctx.Value(bodyKey{}) != nil && s.bulkClient != nil {
			httpc = s.bulkClient
		}
		if media != nil && media.client != nil {
			httpc = media.client
		}
		began := time.Now()
		if s.timing != nil {
			s.timing.request(classifyDriveRequest(ctx, method, target, contentType, body))
		}
		resp, err := httpc.Do(req)
		respAt := time.Now()
		noteTimeout := func() {
			if ctx.Err() == nil && (attemptCtx.Err() == context.DeadlineExceeded || errors.Cause(err) == errStalled) {
				s.throttle.Add(1)
				s.coolMu.Lock()
				s.lastThrottle = time.Now()
				s.coolMu.Unlock()
				if t, ok := httpc.Transport.(*sessionTransport); ok {
					t.stalled(began)
				}
			}
		}
		if err != nil {
			<-pool
			noteTimeout()
			cancel()
			if ctx.Err() != nil {
				return 0, nil, ctx.Err()
			}
			lastErr = errors.New("Drive request failed").Base(err)
			if errors.Cause(err) == errSessionClosed {
				return 0, nil, lastErr
			}
			errors.LogWarningInner(ctx, err, "retrying a failed Drive request, attempt ",
				attempt+1, " of ", driveMaxAttempts)
			continue
		}
		limit := int64(driveMaxResponse)
		if v, ok := ctx.Value(maxBodyKey{}).(int64); ok && v > 0 {
			limit = v
		}
		if progress, ok := ctx.Value(bodyProgressKey{}).(func()); ok {
			resp.Body = &progressBody{ReadCloser: resp.Body, alive: progress}
		}
		payload, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
		if err != nil {
			noteTimeout()
		}
		resp.Body.Close()
		cancel()
		// Keep the permit until EOF/Close, including media response bodies.
		<-pool
		bodyEnd := time.Now()
		if err != nil {
			if ctx.Err() != nil {
				return 0, nil, ctx.Err()
			}
			lastErr = errors.New("failed to read the Drive response").Base(err)
			continue
		}
		if int64(len(payload)) > limit {
			return 0, nil, errors.New("Drive response exceeds the ", limit, " byte cap")
		}
		tr.mu.Lock()
		sample := timingSample{
			total:     bodyEnd.Sub(poolStart) + waitCoolDur + waitTokenDur,
			waitCool:  waitCoolDur,
			waitPool:  waitPoolDur,
			waitToken: waitTokenDur,
			waitConn:  tr.connAt.Sub(began),
			dialTLS:   dialEnd(tr.tlsDone, tr.connDone).Sub(tr.connStart),
			server:    tr.first.Sub(tr.wrote),
			body:      bodyEnd.Sub(maxTime(tr.first, respAt)),
			reused:    tr.reused,
			newConn:   tr.gotConn && !tr.reused,
			hasConn:   tr.gotConn,
			hasDial:   !tr.connStart.IsZero(),
			hasServer: !tr.wrote.IsZero() && !tr.first.IsZero(),
		}
		tr.mu.Unlock()
		s.recordTiming(ctx, method, target, contentType, body, sample)

		if resp.StatusCode == http.StatusUnauthorized {
			s.invalidateToken(token)
			lastErr = errors.New("Drive rejected the access token")
			continue
		}
		if resp.StatusCode == http.StatusForbidden && rateLimited(payload) {
			if ctx.Value(cleanupKey{}) != nil {
				return 0, nil, cleanupRateLimited{detail: string(payload)}
			}
			lastErr = errors.New("Drive is rate limiting: ", string(payload))
			errors.LogWarning(ctx, "rate limited by Drive, attempt ",
				attempt+1, " of ", driveMaxAttempts, ", ", method, " ", len(body),
				" bytes, body read ", strings.Contains(target, "alt=media"),
				", lane ", ctx.Value(laneKey{}) != nil, ": ", string(payload[:min(len(payload), 300)]))
			s.cool()
			continue
		}
		if retryableStatus(resp.StatusCode) {
			if resp.StatusCode == http.StatusTooManyRequests && ctx.Value(cleanupKey{}) != nil {
				return 0, nil, cleanupRateLimited{detail: string(payload)}
			}
			lastErr = errors.New("Drive answered ", resp.StatusCode, ": ", string(payload))
			errors.LogWarning(ctx, "retrying after Drive answered ", resp.StatusCode,
				", attempt ", attempt+1, " of ", driveMaxAttempts)
			if resp.StatusCode == http.StatusTooManyRequests {
				s.cool()
			}
			continue
		}
		if media != nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent) {
			payload, err = media.extract(resp.StatusCode, resp.Header.Get("Content-Range"), payload)
		}
		return resp.StatusCode, payload, err
	}

	return 0, nil, lastErr
}

func (s *driveStorage) waitCool(ctx context.Context) error {
	s.coolMu.Lock()
	until := s.coolUntil
	s.coolMu.Unlock()
	if d := time.Until(until); d > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
		}
	}
	return nil
}

func (s *driveStorage) cool() {
	s.throttle.Add(1)
	s.coolMu.Lock()
	s.coolUntil = time.Now().Add(jitter(driveCooldown))
	s.lastThrottle = time.Now()
	s.lastRateLimit = time.Now()
	s.coolMu.Unlock()
}

func (s *driveStorage) cooling() bool {
	s.coolMu.Lock()
	defer s.coolMu.Unlock()
	return time.Now().Before(s.coolUntil)
}

func (s *driveStorage) lastThrottleAt() time.Time {
	s.coolMu.Lock()
	defer s.coolMu.Unlock()
	return s.lastThrottle
}

// lastRateLimitAt is set only by cool() (403 rate limit / 429), not by attempt
// timeouts, so a stall does not disable the read hedge.
func (s *driveStorage) lastRateLimitAt() time.Time {
	s.coolMu.Lock()
	defer s.coolMu.Unlock()
	return s.lastRateLimit
}

func (s *driveStorage) throttled() int64 {
	return s.throttle.Load()
}

func (s *driveStorage) invalidateToken(used string) {
	s.tokenMu.Lock()
	if s.token == used {
		s.token = ""
	}
	s.tokenMu.Unlock()
}

const idCacheMax = 4096

// rememberID caches the id of name, and those of any further objects that
// share it, so that Delete can remove them all
func (s *driveStorage) rememberID(name, id string, extra []string) {
	s.idMu.Lock()
	if _, ok := s.ids[name]; !ok && len(s.ids) >= idCacheMax {
		for victim := range s.ids {
			delete(s.ids, victim)
			delete(s.dupIDs, victim)
			if len(s.ids) < idCacheMax {
				break
			}
		}
	}
	s.ids[name] = id
	if len(extra) > 0 {
		if s.dupIDs == nil {
			s.dupIDs = make(map[string][]string)
		}
		s.dupIDs[name] = extra
	} else {
		delete(s.dupIDs, name)
	}
	s.idMu.Unlock()
}

func (s *driveStorage) forgetID(name string) {
	s.idMu.Lock()
	delete(s.ids, name)
	delete(s.dupIDs, name)
	s.idMu.Unlock()
}

func (s *driveStorage) cachedID(name string) (string, bool) {
	s.idMu.Lock()
	defer s.idMu.Unlock()
	id, ok := s.ids[name]
	return id, ok
}

type driveFile struct {
	id          string
	description string
	extra       []string // ids of other objects under the same name
}

func (s *driveStorage) query(ctx context.Context, condition string) (map[string]driveFile, error) {
	found := make(map[string]driveFile)
	pageToken := ""
	fields := "nextPageToken,files(id,name,description)"
	if ctx.Value(namesOnlyKey{}) != nil {
		fields = "nextPageToken,files(id,name)"
	}

	for {
		params := url.Values{
			"q":                         {"'" + quoteDriveValue(s.folder) + "' in parents and trashed = false and " + condition},
			"fields":                    {fields},
			"pageSize":                  {fmt.Sprint(drivePageSize)},
			"supportsAllDrives":         {"true"},
			"includeItemsFromAllDrives": {"true"},
		}
		if pageToken != "" {
			params.Set("pageToken", pageToken)
		}

		status, payload, err := s.do(ctx, http.MethodGet, s.filesURL+"?"+params.Encode(), "", nil)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, errors.New("Drive listing answered ", status, ": ", string(payload))
		}

		var parsed struct {
			NextPageToken string `json:"nextPageToken"`
			Files         []struct {
				ID          string `json:"id"`
				Name        string `json:"name"`
				Description string `json:"description"`
			} `json:"files"`
		}
		if err := json.Unmarshal(payload, &parsed); err != nil {
			return nil, errors.New("failed to parse the Drive listing").Base(err)
		}

		for _, file := range parsed.Files {
			if prev, ok := found[file.Name]; ok {
				prev.extra = append(prev.extra, file.ID)
				found[file.Name] = prev
				continue
			}
			found[file.Name] = driveFile{id: file.ID, description: file.Description}
		}

		pageToken = parsed.NextPageToken
		if pageToken == "" {
			for name, file := range found {
				s.rememberID(name, file.id, file.extra)
			}
			return found, nil
		}
	}
}

// resolveIDs is every id under the exact name
func (s *driveStorage) resolveIDs(ctx context.Context, flat string) ([]string, error) {
	found, err := s.query(context.WithValue(ctx, namesOnlyKey{}, true), "name = '"+quoteDriveValue(flat)+"'")
	if err != nil {
		return nil, err
	}
	if file, ok := found[flat]; ok {
		return append([]string{file.id}, file.extra...), nil
	}
	return nil, errNotFound
}

func (s *driveStorage) resolveID(ctx context.Context, flat string) (string, error) {
	if id, ok := s.cachedID(flat); ok {
		return id, nil
	}
	found, err := s.query(context.WithValue(ctx, namesOnlyKey{}, true), "name = '"+quoteDriveValue(flat)+"'")
	if err != nil {
		return "", err
	}
	if file, ok := found[flat]; ok {
		return file.id, nil
	}
	return "", errNotFound
}

func (s *driveStorage) Put(ctx context.Context, name string, data []byte) error {
	if len(data) <= driveInlineLimit {
		return s.putInline(ctx, name, data)
	}
	return s.putMedia(ctx, name, data)
}

func (s *driveStorage) putInline(ctx context.Context, name string, data []byte) error {
	_, err := s.create(ctx, name, base64.StdEncoding.EncodeToString(data))
	return err
}

// create makes an object with description and no body and returns its id
func (s *driveStorage) create(ctx context.Context, name, description string) (string, error) {
	flat := flatten(name)

	body, err := json.Marshal(map[string]interface{}{
		"name":        flat,
		"parents":     []string{s.folder},
		"description": description,
	})
	if err != nil {
		return "", errors.New("failed to build the inline metadata").Base(err)
	}

	status, payload, err := s.do(ctx, http.MethodPost, s.filesURL+"?fields=id",
		"application/json; charset=UTF-8", body)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", errors.New("Drive rejected the inline upload of ", name,
			" with ", status, ": ", string(payload))
	}

	var parsed struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return "", errors.New("failed to parse the Drive upload response").Base(err)
	}
	return parsed.ID, nil
}

// multipartBody is a body of an upload with metadata and data
func multipartBody(metadata, data []byte) []byte {
	var body bytes.Buffer
	fmt.Fprintf(&body, "--%s\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n", driveBoundary)
	body.Write(metadata)
	fmt.Fprintf(&body, "\r\n--%s\r\nContent-Type: application/octet-stream\r\n\r\n", driveBoundary)
	body.Write(data)
	fmt.Fprintf(&body, "\r\n--%s--\r\n", driveBoundary)
	return body.Bytes()
}

func (s *driveStorage) putMedia(ctx context.Context, name string, data []byte) error {
	ctx = context.WithValue(ctx, bodyKey{}, true)
	flat := flatten(name)

	metadata, err := json.Marshal(map[string]interface{}{
		"name":    flat,
		"parents": []string{s.folder},
	})
	if err != nil {
		return errors.New("failed to build the upload metadata").Base(err)
	}

	status, payload, err := s.do(ctx, http.MethodPost, s.uploadURL,
		"multipart/related; boundary="+driveBoundary, multipartBody(metadata, data))
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return errors.New("Drive upload of ", name, " answered ", status, ": ", string(payload))
	}

	var parsed struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return errors.New("failed to parse the Drive upload response").Base(err)
	}
	return nil
}

func (s *driveStorage) Get(ctx context.Context, name string) ([]byte, error) {
	flat := flatten(name)
	id, err := s.resolveID(ctx, flat)
	if err != nil {
		return nil, err
	}

	payload, err := s.readBody(ctx, id)
	if err == errNotFound {
		s.forgetID(flat)
	}
	if err != nil || len(payload) > 0 {
		return payload, err
	}
	return s.getInline(ctx, id)
}

func (s *driveStorage) getInline(ctx context.Context, id string) ([]byte, error) {
	description, err := s.readDescription(ctx, id)
	if err != nil || description == "" {
		return nil, err
	}
	data, err := base64.StdEncoding.DecodeString(description)
	if err != nil {
		return nil, errors.New("the inline data is not valid base64").Base(err)
	}
	return data, nil
}

func (s *driveStorage) deleteID(ctx context.Context, flat, id string) error {
	status, payload, err := s.do(ctx, http.MethodDelete,
		s.filesURL+"/"+url.PathEscape(id)+"?supportsAllDrives=true", "", nil)
	if err != nil {
		return err
	}
	s.forgetID(flat)
	switch status {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		return errors.New("Drive deletion of ", flat, " answered ", status, ": ", string(payload))
	}
}

// waitCleanupSlot is a token bucket capping cleanup deletes at cleanupRate per
// second per storage. It holds at most one token, so deletes are spaced rather
// than bursty, and it honours ctx cancellation while waiting.
func (s *driveStorage) waitCleanupSlot(ctx context.Context) error {
	rate := s.cleanupRate
	if rate <= 0 {
		rate = cleanupRatePerSec
	}
	for {
		s.cleanupMu.Lock()
		now := time.Now()
		if s.cleanupAt.IsZero() {
			s.cleanupAt = now
			s.cleanupTokens = 1
		}
		s.cleanupTokens = min(1, s.cleanupTokens+now.Sub(s.cleanupAt).Seconds()*rate)
		s.cleanupAt = now
		if s.cleanupTokens >= 1 {
			s.cleanupTokens--
			s.cleanupMu.Unlock()
			return nil
		}
		wait := time.Duration((1 - s.cleanupTokens) / rate * float64(time.Second))
		s.cleanupMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

// deleteChild removes one child of a deleted prefix. A rate-limited attempt is
// retried after a jittered delay, up to cleanupMaxRetries tries in total, then
// reported as an ordinary error. Non-rate-limit errors are returned at once.
func (s *driveStorage) deleteChild(ctx context.Context, name, id string) error {
	delay := s.cleanupRetryDelay
	if delay <= 0 {
		delay = cleanupRetryDelay
	}
	tries := s.cleanupMaxRetries
	if tries <= 0 {
		tries = cleanupMaxRetries
	}
	var last error
	for attempt := 0; attempt < tries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay + jitter(delay)):
			}
		}
		if err := s.waitCleanupSlot(ctx); err != nil {
			return err
		}
		err := s.deleteID(ctx, name, id)
		if err == nil {
			return nil
		}
		if !isCleanupRateLimited(err) {
			return err
		}
		last = err
	}
	return errors.New("Drive cleanup deletion of ", name, " failed after ", tries, " tries").Base(last)
}

func (s *driveStorage) Delete(ctx context.Context, name string) error {
	flat := flatten(name)
	remove := s.deleteID
	if ctx.Value(cleanupKey{}) != nil {
		remove = s.deleteChild
	}

	cleanupCtx := context.WithValue(ctx, cleanupKey{}, true)
	cleanupCtx = context.WithValue(cleanupCtx, namesOnlyKey{}, true)
	// A cached name is not an exhaustive listing: retries can create duplicates
	// after it was cached. Discover exact and descendant IDs in one paginated
	// listing, retaining cached IDs as well for objects not yet list-visible.
	s.idMu.Lock()
	var exact []string
	if id, ok := s.ids[flat]; ok {
		exact = append([]string{id}, s.dupIDs[flat]...)
	}
	s.idMu.Unlock()
	children, err := s.query(cleanupCtx, "(name = '"+quoteDriveValue(flat)+"' or name contains '"+quoteDriveValue(flat+flatSeparator)+"')")
	if err != nil {
		return err
	}
	if file, ok := children[flat]; ok {
		for _, id := range append([]string{file.id}, file.extra...) {
			if !slices.Contains(exact, id) {
				exact = append(exact, id)
			}
		}
		delete(children, flat)
	}
	for _, id := range exact {
		if err := remove(ctx, flat, id); err != nil {
			return err
		}
	}
	parallel := s.cleanupParallel
	if parallel <= 0 {
		parallel = cleanupParallel
	}
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	for childName, file := range children {
		wg.Add(1)
		sem <- struct{}{}
		go func(name string, ids []string) {
			defer wg.Done()
			defer func() { <-sem }()
			for _, id := range ids {
				if err := s.deleteChild(cleanupCtx, name, id); err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
				}
			}
		}(childName, append([]string{file.id}, file.extra...))
	}
	wg.Wait()
	return first
}

func (s *driveStorage) List(ctx context.Context, prefix string) ([]Entry, error) {
	flat := flatten(prefix) + flatSeparator

	found, err := s.query(ctx, "name contains '"+quoteDriveValue(flat)+"'")
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool, len(found))
	entries := make([]Entry, 0, len(found))
	for name, file := range found {
		rest := strings.TrimPrefix(name, flat)
		if rest == "" {
			continue
		}
		direct := true
		if cut := strings.Index(rest, flatSeparator); cut >= 0 {
			rest = rest[:cut]
			direct = false
		}
		if seen[rest] {
			continue
		}
		seen[rest] = true

		entry := Entry{Name: rest}
		if direct && file.description != "" {
			if data, err := base64.StdEncoding.DecodeString(file.description); err == nil {
				entry.Inline = data
			}
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (s *driveStorage) Close() error {
	if s.release != nil {
		s.release()
	} else {
		s.closeWork()
		s.stopKeeper()
		if s.timing != nil {
			s.timing.stop()
		}
	}
	return nil
}

// makeSlot implements slotStore
func (s *driveStorage) makeSlot(ctx context.Context, name string) (string, error) {
	id, err := s.create(ctx, name, "")
	if err == nil && id == "" {
		err = errors.New("Drive returned no id for ", name)
	}
	return id, err
}

// setSlot implements slotStore, a description and a body go in one request
func (s *driveStorage) setSlot(ctx context.Context, id, description string, body []byte) error {
	if body == nil {
		ctx = context.WithValue(ctx, attemptKey{}, driveSlotWrite)
	} else {
		ctx = context.WithValue(ctx, bodyKey{}, true)
	}
	metadata, err := json.Marshal(map[string]string{"description": description})
	if err != nil {
		return errors.New("failed to build the slot metadata").Base(err)
	}
	target := s.filesURL + "/" + url.PathEscape(id) + "?fields=id&supportsAllDrives=true"
	contentType := "application/json; charset=UTF-8"
	if body != nil {
		upload, _, _ := strings.Cut(s.uploadURL, "?")
		target = upload + "/" + url.PathEscape(id) + "?uploadType=multipart&fields=id&supportsAllDrives=true"
		contentType = "multipart/related; boundary=" + driveBoundary
		metadata = multipartBody(metadata, body)
	}

	status, payload, err := s.do(ctx, http.MethodPatch, target, contentType, metadata)
	switch {
	case err != nil:
		return err
	case status == http.StatusNotFound:
		return errNotFound
	case status != http.StatusOK:
		return errors.New("Drive answered ", status, " for the update of ", id, ": ", string(payload))
	}
	return nil
}

// noteDescriptionRead accrues one hedge token per twenty description reads,
// capped at six. Tokens are spent when a hedge timer fires, not when a read
// starts, so a stalled read among fast ones can still be hedged.
func (s *driveStorage) noteDescriptionRead() {
	s.readHedgeMu.Lock()
	defer s.readHedgeMu.Unlock()
	s.readHedgeCount++
	if s.readHedgeCount >= 20 {
		s.readHedgeCount = 0
		if s.readHedgeTokens < 6 {
			s.readHedgeTokens++
		}
	}
}

// takeHedgeToken spends one token when the hedge timer fires.
func (s *driveStorage) takeHedgeToken() bool {
	s.readHedgeMu.Lock()
	defer s.readHedgeMu.Unlock()
	if s.readHedgeTokens > 0 {
		s.readHedgeTokens--
		return true
	}
	return false
}

// readDescHedged issues a description GET; if it is still pending after
// readHedge and a budget token is available, one identical GET goes over the
// bulk connection. Reads are idempotent, so a duplicate GET carries no
// stale-write hazard. The first definitive answer (200 or 404) wins and the
// loser's context is cancelled; if both fail the last error is returned. A
// primary that finishes (error included) before the timer fires is returned as
// is with no hedge.
func (s *driveStorage) readDescHedged(ctx context.Context, target string) (int, []byte, error) {
	s.noteDescriptionRead()
	if s.readHedge <= 0 || s.bulkClient == nil ||
		s.cooling() || time.Since(s.lastRateLimitAt()) <= 5*time.Second {
		return s.do(ctx, http.MethodGet, target, "", nil)
	}
	type res struct {
		status  int
		payload []byte
		err     error
		hedge   bool
	}
	ch := make(chan res, 2)
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	go func() {
		st, pl, e := s.do(ctx1, http.MethodGet, target, "", nil)
		ch <- res{st, pl, e, false}
	}()
	timer := time.NewTimer(s.readHedge)
	defer timer.Stop()
	select {
	case r := <-ch:
		return r.status, r.payload, r.err
	case <-timer.C:
	}
	if s.cooling() || time.Since(s.lastRateLimitAt()) <= 5*time.Second || !s.takeHedgeToken() {
		r := <-ch
		return r.status, r.payload, r.err
	}
	kind := kindOtherRead
	if ctx.Value(laneKey{}) != nil {
		kind = kindHeadRead
	}
	ctx2, cancel2 := context.WithCancel(context.WithValue(ctx, bodyKey{}, true))
	defer cancel2()
	go func() {
		st, pl, e := s.do(ctx2, http.MethodGet, target, "", nil)
		ch <- res{st, pl, e, true}
	}()
	var last res
	won := false
	for i := 0; i < 2; i++ {
		r := <-ch
		if r.err == nil && (r.status == http.StatusOK || r.status == http.StatusNotFound) {
			won = r.hedge
			last = r
			break
		}
		last = r
	}
	if s.timing != nil {
		s.timing.noteHedge(kind, won)
	}
	return last.status, last.payload, last.err
}

// readDescription implements slotStore
func (s *driveStorage) readDescription(ctx context.Context, id string) (string, error) {
	ctx = context.WithValue(ctx, attemptKey{}, driveSlotRead)
	status, payload, err := s.readDescHedged(ctx, s.filesURL+"/"+url.PathEscape(id)+"?fields=description&supportsAllDrives=true")
	switch {
	case err != nil:
		return "", err
	case status == http.StatusNotFound:
		return "", errNotFound
	case status != http.StatusOK:
		return "", errors.New("Drive answered ", status, " for the description of ", id, ": ", string(payload))
	}

	var parsed struct {
		Description string `json:"description"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return "", errors.New("failed to parse the description of ", id).Base(err)
	}
	return parsed.Description, nil
}

// readBody implements slotStore
func (s *driveStorage) readBody(ctx context.Context, id string) ([]byte, error) {
	ctx = context.WithValue(ctx, bodyKey{}, true)
	status, payload, err := s.do(ctx, http.MethodGet,
		s.filesURL+"/"+url.PathEscape(id)+"?alt=media&supportsAllDrives=true", "", nil)
	switch {
	case err != nil:
		return nil, err
	case status == http.StatusNotFound:
		return nil, errNotFound
	case status != http.StatusOK:
		return nil, errors.New("Drive answered ", status, " for the body of ", id, ": ", string(payload))
	}
	return payload, nil
}
