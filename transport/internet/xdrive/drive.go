package xdrive

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/dice"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/transport/internet"
)

// Google Drive storage.

var testDriveIO func(method, target string, body []byte)

const (
	flatSeparator = "~"

	drivePageSize         = 1000
	driveMaxAttempts      = 8
	driveMaxInflight      = 64
	driveReservedInflight = 8
	driveInlineLimit      = 12000

	driveDescriptionHedgeQuiet = time.Minute

	driveTimeout = 60 * time.Second
)

var (
	driveMaxBackoff     = 8 * time.Second
	driveTokenURL       = "https://oauth2.googleapis.com/token"
	driveFilesURL       = "https://www.googleapis.com/drive/v3/files"
	driveUploadURL      = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name"
	driveInitialBackoff = 200 * time.Millisecond
	driveCooldown       = 2 * time.Second

	driveRateLimitBudget = 45 * time.Second
	driveSlotRead        = 18 * time.Second
	driveSlotWrite       = 20 * time.Second
	slotReadHedge        = 900 * time.Millisecond
	tokenRefreshBackoff  = 30 * time.Second
	maxTokenBackoff      = 5 * time.Minute
	driveMaxResponse     = 32 << 20
	driveMaxAnnounce     = 1 << 20
)

const (
	driveAuthMaxAttempts = 3
	driveAuthBudget      = 2 * time.Minute
)

type maxBodyKey struct{}

type tokenFlight struct {
	done     chan struct{}
	token    string
	lifetime time.Duration
	err      error
}

type laneKey struct{}

type bodyKey struct{}

type bodyProgressKey struct{}

type attemptKey struct{}

// Hedge only the first dispatch; admission waits and retries are excluded.

type (
	readProgressKey     struct{}
	descriptionHedgeKey struct{}
	readHedgeGateKey    struct{}
)

type readHedgeGate struct {
	primary   *driveReadProgress
	delay     time.Duration
	remaining func() (time.Duration, bool)
}

type driveReadProgress struct {
	mu         sync.Mutex
	dispatched chan struct{}
	started    bool
	pending    bool
	dispatchAt time.Time
	progressAt time.Time
}

func (p *driveReadProgress) dispatch() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dispatchAt = time.Now()
	if !p.started {
		p.started, p.pending = true, true
		p.progressAt = p.dispatchAt
		close(p.dispatched)
	}
}

func (p *driveReadProgress) replay() {
	p.mu.Lock()
	p.dispatchAt = time.Now()
	p.pending = false
	p.mu.Unlock()
}

func (p *driveReadProgress) progress() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending {
		p.progressAt = time.Now()
	}
}

func (p *driveReadProgress) finish() {
	p.mu.Lock()
	p.pending = false
	p.mu.Unlock()
}

func (p *driveReadProgress) remaining(delay time.Duration) (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return delay - time.Since(p.progressAt), p.pending
}

// Media deadlines use dispatch age, not wire progress.

func (p *driveReadProgress) remainingSinceDispatch(delay time.Duration) (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.started || p.dispatchAt.IsZero() {
		return delay, false
	}
	return delay - time.Since(p.dispatchAt), p.pending
}

var errReadHedgeSkipped = errors.New("Drive read hedge is no longer admitted")

type cleanupKey struct{}

type namesOnlyKey struct{}

const (
	cleanupParallel     = 2
	cleanupRatePerSec   = 5.0
	cleanupRetryDelay   = 2 * time.Second
	cleanupMaxRetries   = 5
	cleanupSessionPause = 1 * time.Second
)

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
	streamCaps       atomic.Pointer[driveStreamCaps]
	streamHeadRanges int
	// Clients and permits are session-local.
	client         *http.Client
	priorityClient *http.Client
	bulkClient     *http.Client
	mediaClients   []*http.Client
	inflight       chan struct{}
	reserved       chan struct{}
	readRescue     chan struct{}
	mediaRescue    chan struct{}
	mediaRanges    chan struct{}
	release        func()

	readHedge       time.Duration
	readHedgeMu     sync.Mutex
	readHedgeTokens int
	readHedgeCount  int
	readHedgeActive atomic.Bool

	mediaHedgeTokens  int
	mediaHedgeCount   int
	mediaHedgeChanged chan struct{} // guarded by readHedgeMu

	mediaNext atomic.Uint32
}

type driveShared struct {
	conns *connPool

	priorityConns *connPool
	bulkConns     *connPool
	mediaConns    [driveMediaLanes]*connPool
	folder        string
	clientID      string
	clientSecret  string
	refreshToken  string
	tokenClient   *http.Client
	tokenURL      string
	filesURL      string
	uploadURL     string
	backoff       time.Duration

	tokenMu       sync.Mutex
	token         string
	tokenExpiry   time.Time
	tokenLifetime time.Duration
	authErr       error
	flight        *tokenFlight
	tokenRetryAt  time.Time
	tokenBackoff  time.Duration
	lastTokenErr  error

	idMu       sync.Mutex
	ids        map[string]string
	dupIDs     map[string][]string
	knownIDs   map[string]map[string]struct{}
	missingIDs map[string]map[string]struct{}

	coolMu        sync.Mutex
	coolUntil     time.Time
	lastThrottle  time.Time
	lastRateLimit time.Time
	throttle      atomic.Int64
	mediaLatency  [2]rangeLatency // shared, per requested-size class
	sessionGuard  driveSessionGuard
	timing        *timingAggregator
	idPoolState

	cleanupMu         sync.Mutex
	cleanupTokens     float64
	cleanupAt         time.Time
	cleanupRate       float64
	cleanupParallel   int
	cleanupRetryDelay time.Duration
	cleanupMaxRetries int
	cleanupInflight   chan struct{}

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
	closeOnce  sync.Once
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
	headRanges, validHeadRanges := parseStreamHeadRanges(os.Getenv(streamHeadRangesEnv))

	h1Hosts := new(sync.Map)
	workCtx, workCancel := context.WithCancel(context.Background())
	if !validHeadRanges {
		errors.LogWarning(workCtx, "invalid ", streamHeadRangesEnv, "; retaining default head range limit 4")
	}
	errors.LogInfo(workCtx, "xdrive streamed head range limit=", headRanges, " (2 is experimental; default 4)")
	client := newServiceClient(workCtx, h1Hosts, streamSettings, driveTimeout, driveMaxInflight)
	st := &driveStorage{
		driveShared: &driveShared{
			conns: newConnPool(xmuxWithConnections(config.Xmux, defaultPoolConnections), func(connCtx context.Context) *http.Client {
				return newServiceClient(connCtx, h1Hosts, streamSettings, driveTimeout, driveMaxInflight)
			}),
			priorityConns: newConnPool(xmuxWithConnections(config.Xmux, defaultPoolConnections), func(connCtx context.Context) *http.Client {
				return newServiceClient(connCtx, h1Hosts, streamSettings, driveTimeout, driveReservedInflight)
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
			workCtx:      workCtx,
			workCancel:   workCancel,
			ids:          make(map[string]string),
			timing:       newTimingAggregator(),

			cleanupRate:       cleanupRatePerSec,
			cleanupParallel:   cleanupParallel,
			cleanupRetryDelay: cleanupRetryDelay,
			cleanupMaxRetries: cleanupMaxRetries,
		},
		client:           client,
		streamHeadRanges: headRanges,
		priorityClient:   newServiceClient(workCtx, h1Hosts, streamSettings, driveTimeout, driveReservedInflight),
		inflight:         make(chan struct{}, driveMaxInflight),
		reserved:         make(chan struct{}, driveReservedInflight),
		readRescue:       make(chan struct{}, 1),
		mediaRescue:      make(chan struct{}, driveMediaLanes),
		mediaRanges:      make(chan struct{}, driveMediaInflight),
		readHedge:        slotReadHedge,
		readHedgeTokens:  3,
		mediaHedgeTokens: 3,
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

func (s *driveStorage) session() Storage {
	s.startKeeper()
	transport := &sessionTransport{pool: s.conns}
	bulkTransport := &sessionTransport{pool: s.bulkConns}
	st := &driveStorage{
		driveShared:      s.driveShared,
		client:           &http.Client{Transport: transport},
		streamHeadRanges: s.streamHeadRanges,
		bulkClient:       &http.Client{Transport: bulkTransport},
		inflight:         make(chan struct{}, driveMaxInflight),
		reserved:         make(chan struct{}, driveReservedInflight),
		readRescue:       make(chan struct{}, 1),
		mediaRescue:      make(chan struct{}, driveMediaLanes),
		mediaRanges:      make(chan struct{}, driveMediaInflight),
		readHedge:        s.readHedge,
		readHedgeTokens:  3,
		mediaHedgeTokens: 3,
	}
	transports := []*sessionTransport{transport, bulkTransport}
	if s.priorityConns != nil {
		t := &sessionTransport{pool: s.priorityConns}
		transports = append(transports, t)
		st.priorityClient = &http.Client{Transport: t}
	}
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

var errSessionInUse = errors.New("session is live or already being collected")

type driveSessionGuard struct {
	mu       sync.Mutex
	sessions map[string]*driveSessionUse
}

type driveSessionUse struct {
	active   int
	cleaning bool
}

func (g *driveSessionGuard) acquire(prefix string, cleanup bool) (func(), error) {
	g.mu.Lock()
	if g.sessions == nil {
		g.sessions = make(map[string]*driveSessionUse)
	}
	use := g.sessions[prefix]
	if use == nil {
		use = &driveSessionUse{}
		g.sessions[prefix] = use
	}
	if use.cleaning || cleanup && use.active > 0 {
		g.mu.Unlock()
		return nil, errSessionInUse
	}
	if cleanup {
		use.cleaning = true
	} else {
		use.active++
	}
	g.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			if cleanup {
				use.cleaning = false
			} else {
				use.active--
			}
			if use.active == 0 && !use.cleaning {
				delete(g.sessions, prefix)
			}
		})
	}, nil
}

func (s *driveStorage) holdSession(session string) (func(), error) {
	return s.sessionGuard.acquire(sessionPrefix(session), false)
}

func (s *driveStorage) guardSessionCleanup(ctx context.Context, prefix string) (func(), error) {
	if ctx.Value(cleanupKey{}) != nil && strings.HasPrefix(prefix, streamsDir+"/") &&
		safeSessionID(strings.TrimPrefix(prefix, streamsDir+"/")) {
		return s.sessionGuard.acquire(prefix, true)
	}
	return func() {}, nil
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

func (s *driveStorage) accessToken(parent context.Context) (string, error) {
	ctx, finish, ok := s.beginRequest(parent)
	if !ok {
		return "", context.Canceled
	}
	defer finish()
	ctx, cancel := context.WithTimeout(ctx, driveAuthBudget)
	defer cancel()
	var last error
	for attempt := 0; attempt < driveAuthMaxAttempts; attempt++ {
		token, err := s.accessTokenOnce(ctx)
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if s.isWorkClosed() {
			return "", context.Canceled
		}
		if err == nil || isAuthError(err) || ctx.Value(laneKey{}) != nil {
			return token, err
		}
		last = err
	}
	return "", errors.New("Drive authentication failed after ", driveAuthMaxAttempts, " tries").Base(last)
}

func (s *driveStorage) accessTokenOnce(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.tokenMu.Lock()
	if s.isWorkClosed() {
		s.tokenMu.Unlock()
		return "", context.Canceled
	}
	if s.token != "" && time.Now().Before(s.tokenExpiry) {
		if s.tokenLifetime > 0 && time.Until(s.tokenExpiry) < s.tokenLifetime/5 && s.flight == nil && time.Now().After(s.tokenRetryAt) {
			if refreshCtx, finish, ok := s.beginWork(30 * time.Second); ok {
				f := &tokenFlight{done: make(chan struct{})}
				s.flight = f
				go func() { defer finish(); s.refreshTokenBackground(refreshCtx, f) }()
			}
		}

		token := s.token
		s.tokenMu.Unlock()
		return token, nil
	}
	if s.authErr != nil {
		err := s.authErr
		s.tokenMu.Unlock()
		return "", err
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
		wait := time.Until(s.tokenRetryAt)

		if ctx.Value(laneKey{}) != nil {
			s.tokenMu.Unlock()
			return "", err
		}
		if deadline, ok := ctx.Deadline(); ok && wait >= time.Until(deadline) {
			s.tokenMu.Unlock()
			return "", err
		}
		s.tokenMu.Unlock()
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-timer.C:
			return s.accessTokenOnce(ctx)
		}
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

func (s *driveStorage) refreshTokenBackground(ctx context.Context, f *tokenFlight) {
	token, lifetime, err := s.fetchToken(ctx)

	s.tokenMu.Lock()
	defer s.tokenMu.Unlock()
	defer close(f.done)
	s.flight = nil
	f.token, f.lifetime, f.err = token, lifetime, err
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
	if parsed.ExpiresIn <= 0 || parsed.ExpiresIn > int64(time.Duration(1<<63-1)/time.Second) {
		return "", 0, errors.New("the token endpoint returned an invalid lifetime")
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

func driveQuotaRetry(elapsed time.Duration) (refund, stop bool) {
	return elapsed < driveRateLimitBudget, elapsed >= driveRateLimitBudget
}

type createOnceKey struct{}

type createAmbiguityError struct{ err error }

func (e createAmbiguityError) Error() string {
	return "Drive creation outcome is unknown; refusing to replay: " + e.err.Error()
}

func (e createAmbiguityError) Unwrap() error { return e.err }

func (s *driveStorage) do(parent context.Context, method, target, contentType string, body []byte) (int, []byte, error) {
	ctx, finish, ok := s.beginRequest(parent)
	if !ok {
		return 0, nil, context.Canceled
	}
	defer finish()
	defer s.noteRequest(ctx)
	createOnce := method == http.MethodPost && ctx.Value(createOnceKey{}) != nil
	descriptionHedge := method == http.MethodGet && ctx.Value(descriptionHedgeKey{}) != nil
	media, _ := ctx.Value(driveMediaKey{}).(*driveMediaRequest)
	mediaTiming, _ := ctx.Value(bodyReadTimingKey{}).(*bodyReadTiming)
	readHedgeAttempt := descriptionHedge || method == http.MethodGet &&
		(media != nil && media.rescue || ctx.Value(directProbeKey{}) != nil)
	singleAttempt := readHedgeAttempt || method == http.MethodGet && ctx.Value(keeperProbeKey{}) != nil
	if readHedgeAttempt && !s.readHedgeAllowed(ctx) {
		return 0, nil, errReadHedgeSkipped
	}
	primary, _ := ctx.Value(readProgressKey{}).(*driveReadProgress)
	if primary != nil {
		defer primary.finish()
	}
	maxAttempts := driveMaxAttempts
	if singleAttempt {
		maxAttempts = 1
	}
	backoff := s.backoff
	var lastErr error

	var failedConn *poolConn
	kind := -1
	if s.timing != nil {
		kind = classifyDriveRequest(ctx, method, target, contentType, body)
	}

	firstTry := time.Now()
	refunded := 0
	rejected := 0
	for attempt := 0; attempt-refunded < maxAttempts; attempt++ {
		if attempt > 0 {
			mediaTiming.reject()
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
		attemptStart := time.Now()
		coolStart := attemptStart
		if err := s.waitCool(ctx); err != nil {
			return 0, nil, err
		}
		waitCoolDur := time.Since(coolStart)
		tokenStart := time.Now()
		token, err := s.accessToken(ctx)
		waitTokenDur := time.Since(tokenStart)
		if err != nil {
			return 0, nil, err
		}

		pool := s.inflight
		if ctx.Value(laneKey{}) != nil {
			pool = s.reserved
		}
		if descriptionHedge && s.readRescue != nil {
			pool = s.readRescue
		}
		if media != nil && !media.rescue && s.mediaRanges != nil {
			pool = s.mediaRanges
		}
		if media != nil && media.rescue && s.mediaRescue != nil {
			pool = s.mediaRescue
		}
		poolStart := time.Now()
		var cleanupPool chan struct{}
		if ctx.Value(cleanupKey{}) != nil {
			cleanupPool = s.cleanupPool()
			select {
			case cleanupPool <- struct{}{}:
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			}
		}
		select {
		case pool <- struct{}{}:
		case <-ctx.Done():
			if cleanupPool != nil {
				<-cleanupPool
			}
			return 0, nil, ctx.Err()
		}
		releasePermit := func() {
			<-pool
			if cleanupPool != nil {
				<-cleanupPool
			}
		}
		waitPoolDur := time.Since(poolStart)
		coolStart = time.Now()
		if err := s.waitCool(ctx); err != nil {
			releasePermit()
			return 0, nil, err
		}
		waitCoolDur += time.Since(coolStart)

		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		attemptCtx, cancel := ctx, context.CancelFunc(func() {})
		if d, ok := ctx.Value(attemptKey{}).(time.Duration); ok {
			attemptCtx, cancel = context.WithTimeout(ctx, d)
		} else if createOnce || readHedgeAttempt {
			attemptCtx, cancel = context.WithCancel(ctx)
		}
		req, err := http.NewRequestWithContext(attemptCtx, method, target, reader)
		if err != nil {
			cancel()
			releasePermit()
			return 0, nil, errors.New("failed to build a Drive request").Base(err)
		}
		if createOnce {
			req.GetBody = nil
		}
		req.Header.Set("Authorization", "Bearer "+token)
		requestMedia := media
		if media != nil && media.progress != nil && attempt > 0 {
			resumed := media.progress.resume(*media)
			requestMedia = &resumed
		}
		if requestMedia != nil {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", requestMedia.start, requestMedia.end-1))
			req.Header.Set("Accept-Encoding", "identity")
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}

		var tr struct {
			mu                                                 sync.Mutex
			connAt, connStart, connDone, tlsDone, wrote, first time.Time
			admitted                                           time.Time
			wroteOK                                            bool
			reused, gotConn                                    bool
			dispatchWait                                       time.Duration
		}
		var dispatched, admissionFailed, replayed atomic.Bool
		var headAtDispatch atomic.Bool
		admit := func(dispatchCtx context.Context) error {
			start := time.Now()
			err := s.waitDispatch(dispatchCtx, cleanupPool != nil && method == http.MethodDelete)
			tr.mu.Lock()
			tr.dispatchWait += time.Since(start)
			if err == nil {
				tr.admitted = time.Now()
			}
			tr.mu.Unlock()
			if err != nil {
				admissionFailed.Store(true)
				if readHedgeAttempt {
					cancel()
				}
			}
			if err == nil {
				if media != nil {
					headAtDispatch.Store(mediaHead(ctx))
				}
				previous := dispatched.Swap(true)
				if previous {
					replayed.Store(true)
				}
				if previous || media != nil && media.rescue {
					mediaTiming.reject()
				}
				if createOnce && previous {
					cancel()
					return createAmbiguityError{err: errors.New("transport attempted a second creation dispatch")}
				}
				if readHedgeAttempt && previous {
					cancel()
					return errReadHedgeSkipped
				}
				if primary != nil {
					if previous {
						primary.replay()
					} else {
						primary.dispatch()
					}
				}
			}
			if err == nil && s.timing != nil {
				s.timing.request(kind)
				if readHedgeAttempt {
					s.timing.noteHedge(kind, false)
				}
			}
			return err
		}
		req = req.WithContext(context.WithValue(req.Context(), dispatchKey{}, admit))
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
			WroteRequest: func(info httptrace.WroteRequestInfo) {
				tr.mu.Lock()
				tr.wrote = time.Now()
				tr.wroteOK = info.Err == nil
				tr.mu.Unlock()
			},
			GotFirstResponseByte: func() {
				tr.mu.Lock()
				tr.first = time.Now()
				tr.mu.Unlock()
				if primary != nil {
					primary.progress()
				}
			},
		}
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

		httpc := s.client
		if ctx.Value(laneKey{}) != nil && !descriptionHedge && s.priorityClient != nil {
			httpc = s.priorityClient
		}
		if ctx.Value(bodyKey{}) != nil && s.bulkClient != nil {
			httpc = s.bulkClient
		}
		if media != nil && media.client != nil {
			httpc = media.client
		}
		if err := ctx.Err(); err != nil {
			cancel()
			releasePermit()
			return 0, nil, err
		}
		if testDriveIO != nil {
			testDriveIO(method, target, body)
		}
		var selected atomic.Pointer[poolConn]
		req = req.WithContext(context.WithValue(req.Context(), poolConnKey{}, selected.Store))
		if failedConn != nil {
			req = req.WithContext(context.WithValue(req.Context(), failedConnKey{}, failedConn))
		}
		began := time.Now()
		dispatchClient := *httpc
		dispatchClient.Transport = dispatchTransport{next: httpc.Transport}
		resp, err := dispatchClient.Do(req)
		respAt := time.Now()
		noteTimeout := func() {
			if ctx.Err() == nil && dispatched.Load() && !admissionFailed.Load() && (attemptCtx.Err() == context.DeadlineExceeded || errors.Cause(err) == errStalled) {

				dead := false
				if t, ok := httpc.Transport.(*sessionTransport); ok {
					dead = t.stalled(began, selected.Load())
				}
				if !dead {
					s.throttle.Add(1)
					s.coolMu.Lock()
					s.lastThrottle = time.Now()
					s.coolMu.Unlock()
				}
			}
		}
		if err != nil {
			mediaTiming.reject()
			if primary != nil {
				primary.finish()
			}
			if dispatched.Load() && s.timing != nil {
				s.timing.noteFailure(kind, ctx.Err() != nil)
			}
			releasePermit()
			noteTimeout()
			failedConn = selected.Load()
			cancel()
			if ctx.Err() != nil {
				return 0, nil, ctx.Err()
			}
			lastErr = errors.New("Drive request failed").Base(err)
			if errors.Cause(err) == errSessionClosed {
				return 0, nil, lastErr
			}
			if createOnce && dispatched.Load() {
				return 0, nil, createAmbiguityError{err: lastErr}
			}
			if singleAttempt {
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
		var payload []byte
		if requestMedia != nil && requestMedia.stream != nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent) {
			payload, err = requestMedia.stream.read(*requestMedia, resp.StatusCode, resp.Header.Get("Content-Range"), resp.ContentLength, io.LimitReader(resp.Body, limit+1))
		} else {
			payload, err = readResponse(resp.Body, limit, resp.ContentLength)
		}
		if err != nil {
			mediaTiming.reject()
			noteTimeout()
			if dispatched.Load() && s.timing != nil {
				s.timing.noteFailure(kind, ctx.Err() != nil)
			}
		}
		resp.Body.Close()
		cancel()

		releasePermit()
		bodyEnd := time.Now()
		headAtCompletion := media != nil && mediaHead(ctx)
		if primary != nil {
			primary.finish()
		}
		if err != nil {
			if ctx.Err() != nil {
				return 0, nil, ctx.Err()
			}
			failedConn = selected.Load()
			lastErr = errors.New("failed to read the Drive response").Base(err)
			if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusTooManyRequests && !(resp.StatusCode == http.StatusForbidden && rateLimited(payload)) {
				if createOnce {
					return 0, nil, createAmbiguityError{err: lastErr}
				}
				if singleAttempt {
					return 0, nil, lastErr
				}
				continue
			}
		}
		if int64(len(payload)) > limit {
			err := errors.New("Drive response exceeds the ", limit, " byte cap")
			if createOnce {
				return 0, nil, createAmbiguityError{err: err}
			}
			return 0, nil, err
		}
		tr.mu.Lock()
		sample := timingSample{
			total:       bodyEnd.Sub(attemptStart),
			waitCool:    waitCoolDur + tr.dispatchWait,
			waitPool:    waitPoolDur,
			waitToken:   waitTokenDur,
			waitConn:    max(0, tr.connAt.Sub(began)-tr.dispatchWait),
			dialTLS:     dialEnd(tr.tlsDone, tr.connDone).Sub(tr.connStart),
			send:        max(0, tr.wrote.Sub(tr.admitted)),
			server:      tr.first.Sub(tr.wrote),
			body:        bodyEnd.Sub(maxTime(tr.first, respAt)),
			reused:      tr.reused,
			newConn:     tr.gotConn && !tr.reused,
			hasConn:     tr.gotConn,
			hasDial:     !tr.connStart.IsZero(),
			hasSend:     tr.wroteOK && !tr.admitted.IsZero() && !tr.wrote.IsZero() && !tr.wrote.Before(tr.admitted),
			hasServer:   !tr.wrote.IsZero() && !tr.first.IsZero(),
			hasDispatch: !tr.admitted.IsZero(),
		}
		if sample.hasDispatch {
			sample.dispatch = bodyEnd.Sub(tr.admitted)
		}
		tr.mu.Unlock()
		s.recordTiming(kind, sample)
		s.logSlowRequest(ctx, kind, req.URL.Path, sample, requestMedia, headAtDispatch.Load(), headAtCompletion, resp.StatusCode, attempt+1)

		if resp.StatusCode == http.StatusUnauthorized {
			s.invalidateToken(token)
			lastErr = errors.New("Drive rejected the access token")
			if singleAttempt {
				return 0, nil, lastErr
			}

			if rejected++; rejected > 1 {
				return 0, nil, authError{code: "401"}
			}
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusForbidden && rateLimited(payload) {
			if s.timing != nil {
				s.timing.noteRateLimit(kind)
			}
			if ctx.Value(cleanupKey{}) != nil {
				return 0, nil, cleanupRateLimited{detail: string(payload)}
			}
			lastErr = errors.New("Drive is rate limiting: ", string(payload))
			errors.LogWarning(ctx, "rate limited by Drive, attempt ",
				attempt+1, " of ", driveMaxAttempts, ", ", method, " ", len(body),
				" bytes, body read ", strings.Contains(target, "alt=media"),
				", lane ", ctx.Value(laneKey{}) != nil, ": ", string(payload[:min(len(payload), 300)]))
			s.cool()
			if singleAttempt {
				return 0, nil, lastErr
			}
			if refund, stop := driveQuotaRetry(time.Since(firstTry)); refund {
				refunded++
			} else if stop {
				return 0, nil, lastErr
			}
			continue
		}
		if retryableStatus(resp.StatusCode) {
			lastErr = errors.New("Drive answered ", resp.StatusCode, ": ", string(payload))
			if createOnce {
				return 0, nil, createAmbiguityError{err: lastErr}
			}
			if singleAttempt {
				return 0, nil, lastErr
			}
			errors.LogWarning(ctx, "retrying after Drive answered ", resp.StatusCode,
				", attempt ", attempt+1, " of ", driveMaxAttempts)
			continue
		}
		if media != nil && media.stream == nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent) {
			payload, err = media.extract(resp.StatusCode, resp.Header.Get("Content-Range"), payload)
			if want := media.direct; want != nil && err == nil {
				// Validate each candidate before it can win and cancel its sibling.
				got, ok := decodeDirectSlot(payload, want.seq)
				if !ok || got.size != want.size || got.crc != want.crc {
					err = errors.New("direct media frame does not match its description")
				}
			}
		}
		if err != nil {
			mediaTiming.reject()
		}
		if ctx.Err() == nil {
			s.observeMediaLatency(media, sample, resp.StatusCode, attempt, replayed.Load(), err)
			if media != nil && !media.rescue && err == nil && sample.hasDispatch &&
				(resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent) {
				mediaTiming.observe(sample.dispatch)
			}
		}
		return resp.StatusCode, payload, err
	}

	return 0, nil, lastErr
}

func readResponse(r io.Reader, limit, announced int64) ([]byte, error) {
	if announced <= 0 || announced > limit || announced > maxSegmentBytes {
		return io.ReadAll(io.LimitReader(r, limit+1))
	}
	buf := bytes.NewBuffer(make([]byte, 0, announced+bytes.MinRead+1))
	_, err := buf.ReadFrom(io.LimitReader(r, limit+1))
	return buf.Bytes(), err
}

func (s *driveStorage) waitDispatch(ctx context.Context, cleanupDelete bool) error {
	for {
		if !s.readHedgeAllowed(ctx) {
			return errReadHedgeSkipped
		}
		if err := s.waitCool(ctx); err != nil {
			return err
		}
		if cleanupDelete {
			if err := s.waitCleanupSlot(ctx); err != nil {
				return err
			}
		}
		if !s.readHedgeAllowed(ctx) {
			return errReadHedgeSkipped
		}
		if !s.cooling() {
			return ctx.Err()
		}
	}
}

func (s *driveStorage) waitCool(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.coolMu.Lock()
		until := s.coolUntil
		s.coolMu.Unlock()
		d := time.Until(until)
		if d <= 0 {
			return nil
		}
		timing, _ := ctx.Value(bodyReadTimingKey{}).(*bodyReadTiming)
		timing.reject()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
		}
	}
}

func (s *driveStorage) cool() {
	s.throttle.Add(1)
	now := time.Now()
	until := now.Add(jitter(driveCooldown))
	s.coolMu.Lock()

	if until.After(s.coolUntil) {
		s.coolUntil = until
	}
	s.lastThrottle = now
	s.lastRateLimit = now
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

func (s *driveStorage) rememberID(name, id string, extra []string) {
	if id == "" {
		return
	}
	s.idMu.Lock()
	defer s.idMu.Unlock()
	s.retireIDsLocked(name, s.missingIDs[name])
	s.rememberIDLocked(name, id, extra)
}

func (s *driveStorage) rememberListedIDs(name string, listed []string) {
	if len(listed) == 0 {
		return
	}
	s.idMu.Lock()
	defer s.idMu.Unlock()

	if len(listed) == 1 && s.ids[name] == listed[0] && len(s.missingIDs[name]) == 0 {
		return
	}
	s.retireIDsLocked(name, s.missingIDs[name])
	id := s.ids[name]
	if id == "" {
		id = listed[0]
	}
	extra := append([]string(nil), listed...)
	slices.Reverse(extra)
	s.rememberIDLocked(name, id, extra)
}

func (s *driveStorage) rememberIDLocked(name, id string, extra []string) {
	if s.ids == nil {
		s.ids = make(map[string]string)
	}
	if _, ok := s.ids[name]; !ok && len(s.ids) >= idCacheMax {
		for victim := range s.ids {
			delete(s.ids, victim)
			delete(s.dupIDs, victim)
			delete(s.knownIDs, victim)
			delete(s.missingIDs, victim)
			if len(s.ids) < idCacheMax {
				break
			}
		}
	}
	if primary := s.ids[name]; primary != "" {
		extra = append(append([]string{id}, extra...), s.dupIDs[name]...)
		id = primary
	}
	s.ids[name] = id
	var duplicates []string
	seen := make(map[string]struct{}, len(extra))
	for _, candidate := range extra {
		if candidate == "" || candidate == id {
			continue
		}
		if _, ok := seen[candidate]; !ok {
			seen[candidate] = struct{}{}
			duplicates = append(duplicates, candidate)
		}
	}
	if len(duplicates) > 0 {
		if s.dupIDs == nil {
			s.dupIDs = make(map[string][]string)
		}
		s.dupIDs[name] = duplicates
	} else {
		delete(s.dupIDs, name)
	}
	seen[id] = struct{}{}
	if s.knownIDs == nil {
		s.knownIDs = make(map[string]map[string]struct{})
	}
	s.knownIDs[name] = seen
}

func (s *driveStorage) forgetID(name string) {
	s.idMu.Lock()
	delete(s.ids, name)
	delete(s.dupIDs, name)
	delete(s.knownIDs, name)
	delete(s.missingIDs, name)
	s.idMu.Unlock()
}

func (s *driveStorage) markMissingID(name, id string) {
	s.idMu.Lock()
	defer s.idMu.Unlock()
	if _, known := s.knownIDs[name][id]; !known {
		return
	}
	if s.missingIDs == nil {
		s.missingIDs = make(map[string]map[string]struct{})
	}
	if s.missingIDs[name] == nil {
		s.missingIDs[name] = make(map[string]struct{})
	}
	s.missingIDs[name][id] = struct{}{}
}

func (s *driveStorage) retireID(name, id string) {
	s.retireIDs(name, map[string]struct{}{id: {}})
}

func (s *driveStorage) retireIDs(name string, retired map[string]struct{}) {
	s.idMu.Lock()
	defer s.idMu.Unlock()
	s.retireIDsLocked(name, retired)
}

func (s *driveStorage) retireMissingIDs(name string) {
	s.idMu.Lock()
	defer s.idMu.Unlock()
	s.retireIDsLocked(name, s.missingIDs[name])
}

func (s *driveStorage) retireIDsLocked(name string, retired map[string]struct{}) {
	if len(retired) == 0 {
		return
	}
	known := append([]string{s.ids[name]}, s.dupIDs[name]...)
	kept := known[:0]
	for _, candidate := range known {
		if _, gone := retired[candidate]; candidate != "" && !gone {
			kept = append(kept, candidate)
		}
	}
	for id := range retired {
		delete(s.missingIDs[name], id)
		delete(s.knownIDs[name], id)
	}
	if len(s.missingIDs[name]) == 0 {
		delete(s.missingIDs, name)
	}
	if len(kept) == 0 {
		delete(s.ids, name)
		delete(s.dupIDs, name)
		delete(s.knownIDs, name)
		delete(s.missingIDs, name)
		return
	}
	s.ids[name] = kept[0]
	if len(kept) > 1 {
		s.dupIDs[name] = append([]string(nil), kept[1:]...)
	} else {
		delete(s.dupIDs, name)
	}
}

func (s *driveStorage) cachedFiles(flat string, descendants bool) map[string]driveFile {
	s.idMu.Lock()
	defer s.idMu.Unlock()
	found := make(map[string]driveFile)
	if !descendants {
		if file := s.cachedFileLocked(flat); file.id != "" {
			found[flat] = file
		}
		return found
	}
	prefix := flat + flatSeparator
	for name := range s.ids {
		if name == flat || strings.HasPrefix(name, prefix) {
			file := s.cachedFileLocked(name)
			if file.id != "" {
				found[name] = file
			}
		}
	}
	return found
}

func (s *driveStorage) cachedFileLocked(name string) driveFile {
	id := s.ids[name]
	if id == "" {
		return driveFile{}
	}
	if len(s.missingIDs[name]) == 0 {
		return driveFile{id: id, extra: append([]string(nil), s.dupIDs[name]...)}
	}
	file := driveFile{}
	for _, candidate := range append([]string{id}, s.dupIDs[name]...) {
		if _, missing := s.missingIDs[name][candidate]; missing {
			continue
		}
		if file.id == "" {
			file.id = candidate
		} else {
			file.extra = append(file.extra, candidate)
		}
	}
	return file
}

func mergeDriveFile(first, next driveFile) driveFile {
	if first.id == "" {
		first = next
		first.extra = append([]string(nil), next.extra...)
		return first
	}
	seen := make(map[string]struct{}, len(first.extra)+len(next.extra)+2)
	seen[first.id] = struct{}{}
	for _, id := range first.extra {
		seen[id] = struct{}{}
	}
	for _, id := range append([]string{next.id}, next.extra...) {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			first.extra = append(first.extra, id)
		}
	}
	return first
}

func (s *driveStorage) cachedID(name string) (string, bool) {
	s.idMu.Lock()
	defer s.idMu.Unlock()
	if id := s.ids[name]; id != "" {
		if _, missing := s.missingIDs[name][id]; !missing {
			return id, true
		}
		for _, candidate := range s.dupIDs[name] {
			if _, missing := s.missingIDs[name][candidate]; !missing {
				return candidate, true
			}
		}
	}
	return "", false
}

type driveFile struct {
	id          string
	description string
	extra       []string
}

func (s *driveStorage) query(ctx context.Context, condition string) (map[string]driveFile, error) {
	found := make(map[string]driveFile)
	seen := make(map[string]map[string]struct{})
	pageToken := ""
	seenTokens := make(map[string]struct{})
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
			return found, err
		}
		if status != http.StatusOK {
			return found, errors.New("Drive listing answered ", status, ": ", string(payload))
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
			return found, errors.New("failed to parse the Drive listing").Base(err)
		}

		listed := make(map[string][]string)
		for _, file := range parsed.Files {
			if file.ID == "" {
				continue
			}
			listed[file.Name] = append(listed[file.Name], file.ID)
			ids := seen[file.Name]
			if ids == nil {
				ids = make(map[string]struct{})
				seen[file.Name] = ids
			}
			if _, duplicate := ids[file.ID]; duplicate {
				continue
			}
			ids[file.ID] = struct{}{}
			known := found[file.Name]
			if known.id == "" {
				known.id, known.description = file.ID, file.Description
			} else {
				known.extra = append(known.extra, file.ID)
			}
			found[file.Name] = known
		}
		for name, ids := range listed {
			s.rememberListedIDs(name, ids)
		}

		pageToken = parsed.NextPageToken
		if pageToken == "" {
			return found, nil
		}
		if _, repeated := seenTokens[pageToken]; repeated {
			return found, errors.New("Drive listing repeated a page token")
		}
		seenTokens[pageToken] = struct{}{}
	}
}

func (s *driveStorage) resolveIDs(ctx context.Context, flat string) ([]string, error) {
	found, err := s.query(context.WithValue(ctx, namesOnlyKey{}, true), "name = '"+quoteDriveValue(flat)+"'")
	file := mergeDriveFile(s.cachedFiles(flat, false)[flat], found[flat])
	if file.id != "" {
		return append([]string{file.id}, file.extra...), err
	}
	if err != nil {
		return nil, err
	}
	return nil, errNotFound
}

func (s *driveStorage) resolveID(ctx context.Context, flat string) (string, error) {
	if id, ok := s.cachedID(flat); ok {
		return id, nil
	}
	found, err := s.query(context.WithValue(ctx, namesOnlyKey{}, true), "name = '"+quoteDriveValue(flat)+"'")
	if file, ok := found[flat]; ok {
		return file.id, nil
	}
	if err != nil {
		return "", err
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

	ctx = context.WithValue(ctx, createOnceKey{}, true)
	status, payload, err := s.do(ctx, http.MethodPost, s.filesURL+"?fields=id&supportsAllDrives=true",
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
		return "", createAmbiguityError{err: errors.New("failed to parse the Drive upload response").Base(err)}
	}
	if parsed.ID == "" {
		return "", createAmbiguityError{err: errors.New("Drive returned no id for ", name)}
	}
	s.rememberID(flat, parsed.ID, nil)
	return parsed.ID, nil
}

func multipartBody(metadata, data []byte) ([]byte, string) {
	var boundary string
	for {
		var entropy [24]byte
		rand.Read(entropy[:])
		boundary = "xdrive-" + hex.EncodeToString(entropy[:])
		if !bytes.Contains(metadata, []byte(boundary)) && !bytes.Contains(data, []byte(boundary)) {
			break
		}
	}
	var body bytes.Buffer
	body.Grow(len(metadata) + len(data) + 3*len(boundary) + 256)
	fmt.Fprintf(&body, "--%s\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n", boundary)
	body.Write(metadata)
	fmt.Fprintf(&body, "\r\n--%s\r\nContent-Type: application/octet-stream\r\n\r\n", boundary)
	body.Write(data)
	fmt.Fprintf(&body, "\r\n--%s--\r\n", boundary)
	return body.Bytes(), "multipart/related; boundary=" + boundary
}

func withAllDrives(target string) string {
	parsed, err := url.Parse(target)
	if err != nil {
		return target
	}
	params := parsed.Query()
	params.Set("supportsAllDrives", "true")
	parsed.RawQuery = params.Encode()
	return parsed.String()
}

func (s *driveStorage) putMedia(ctx context.Context, name string, data []byte) error {
	ctx = context.WithValue(ctx, bodyKey{}, true)
	ctx = context.WithValue(ctx, createOnceKey{}, true)
	flat := flatten(name)

	metadata, err := json.Marshal(map[string]interface{}{
		"name":    flat,
		"parents": []string{s.folder},
	})
	if err != nil {
		return errors.New("failed to build the upload metadata").Base(err)
	}

	body, contentType := multipartBody(metadata, data)
	status, payload, err := s.do(ctx, http.MethodPost, withAllDrives(s.uploadURL), contentType, body)
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
		return createAmbiguityError{err: errors.New("failed to parse the Drive upload response").Base(err)}
	}
	if parsed.ID == "" {
		return createAmbiguityError{err: errors.New("Drive returned no id for ", name)}
	}
	s.rememberID(flat, parsed.ID, nil)
	return nil
}

func (s *driveStorage) Get(ctx context.Context, name string) ([]byte, error) {
	flat := flatten(name)
	id, err := s.resolveID(ctx, flat)
	if err != nil {
		return nil, err
	}

	known := mergeDriveFile(driveFile{id: id}, s.cachedFiles(flat, false)[flat])
	candidates := append([]string{known.id}, known.extra...)
	tried := make(map[string]struct{}, len(candidates))
	missing := make(map[string]struct{}, len(candidates))
	defer s.retireMissingIDs(flat)
	var refreshErr error
	for round := range 2 {
		if round > 0 {
			s.retireMissingIDs(flat)
			candidates, refreshErr = s.resolveIDs(ctx, flat)
			s.retireIDs(flat, missing)
		}
		for _, candidate := range candidates {
			if _, ok := tried[candidate]; ok {
				continue
			}
			tried[candidate] = struct{}{}
			payload, err := s.readBody(ctx, candidate)
			if err == nil && len(payload) == 0 {
				payload, err = s.getInline(ctx, candidate)
			}
			if err != errNotFound {
				return payload, err
			}
			missing[candidate] = struct{}{}
			s.markMissingID(flat, candidate)
		}
	}
	if refreshErr != nil {
		return nil, refreshErr
	}
	return nil, errNotFound
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
	switch status {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		s.retireID(flat, id)
		return nil
	default:
		return errors.New("Drive deletion of ", flat, " answered ", status, ": ", string(payload))
	}
}

func (s *driveShared) cleanupPool() chan struct{} {
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()
	if s.cleanupInflight == nil {
		parallel := s.cleanupParallel
		if parallel <= 0 {
			parallel = cleanupParallel
		}
		s.cleanupInflight = make(chan struct{}, parallel)
	}
	return s.cleanupInflight
}

func (s *driveStorage) waitCleanupSlot(ctx context.Context) error {
	rate := s.cleanupRate
	if rate <= 0 {
		rate = cleanupRatePerSec
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
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

func (s *driveStorage) deleteChild(ctx context.Context, name, id string) error {
	ctx = context.WithValue(ctx, cleanupKey{}, true)
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

func (s *driveStorage) Delete(parent context.Context, name string) error {
	ctx, finish, ok := s.beginRequest(parent)
	if !ok {
		return context.Canceled
	}
	defer finish()
	release, err := s.guardSessionCleanup(ctx, name)
	if err != nil {
		return err
	}
	defer release()
	flat := flatten(name)
	remove := s.deleteID
	if ctx.Value(cleanupKey{}) != nil {
		remove = s.deleteChild
	}

	cleanupCtx := context.WithValue(ctx, cleanupKey{}, true)
	cleanupCtx = context.WithValue(cleanupCtx, namesOnlyKey{}, true)
	children := s.cachedFiles(flat, true)
	listed, listErr := s.query(cleanupCtx, "(name = '"+quoteDriveValue(flat)+"' or name contains '"+quoteDriveValue(flat+flatSeparator)+"')")
	for childName, file := range listed {
		if childName == flat || strings.HasPrefix(childName, flat+flatSeparator) {
			children[childName] = mergeDriveFile(children[childName], file)
		}
	}
	for childName, file := range s.cachedFiles(flat, true) {
		children[childName] = mergeDriveFile(children[childName], file)
	}
	if file, ok := children[flat]; ok {
		for _, id := range append([]string{file.id}, file.extra...) {
			if err := remove(ctx, flat, id); err != nil {
				return err
			}
		}
		delete(children, flat)
	}
	parallel := s.cleanupParallel
	if parallel <= 0 {
		parallel = cleanupParallel
	}
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	var mu sync.Mutex
	first := listErr
	launching := true
	for childName, file := range children {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			launching = false
		}
		if !launching {
			break
		}
		wg.Add(1)
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
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return first
}

func (s *driveStorage) List(ctx context.Context, prefix string) ([]Entry, error) {
	flat := flatten(prefix) + flatSeparator

	found, err := s.query(ctx, "name contains '"+quoteDriveValue(flat)+"'")

	seen := make(map[string]int, len(found))
	entries := make([]Entry, 0, len(found))
	for name, file := range found {
		if !strings.HasPrefix(name, flat) {
			continue
		}
		rest := strings.TrimPrefix(name, flat)
		if rest == "" {
			continue
		}
		direct := true
		if cut := strings.Index(rest, flatSeparator); cut >= 0 {
			rest = rest[:cut]
			direct = false
		}
		index, exists := seen[rest]
		if !exists {
			index = len(entries)
			seen[rest] = index
			entries = append(entries, Entry{Name: rest})
		}
		entry := &entries[index]
		if direct && file.description != "" {
			if data, err := base64.StdEncoding.DecodeString(file.description); err == nil {
				entry.Inline = data
			}
		}
	}
	return entries, err
}

func (s *driveStorage) Close() error {
	if s.release != nil {
		s.release()
		return nil
	}
	s.closeOnce.Do(func() {
		s.cancelWork()
		s.conns.Close()
		s.priorityConns.Close()
		s.bulkConns.Close()
		for _, pool := range s.mediaConns {
			pool.Close()
		}
		for _, client := range append([]*http.Client{s.client, s.priorityClient, s.bulkClient, s.tokenClient}, s.mediaClients...) {
			if client != nil {
				client.CloseIdleConnections()
			}
		}
		s.closeWork()
		s.stopKeeper()
		if s.timing != nil {
			s.timing.stop()
		}
	})
	return nil
}

func (s *driveStorage) makeSlot(ctx context.Context, name string) (string, error) {
	id, err := s.create(ctx, name, "")
	if err == nil && id == "" {
		err = errors.New("Drive returned no id for ", name)
	}
	return id, err
}

func (s *driveStorage) setSlot(ctx context.Context, id, description string, body []byte) error {
	if body == nil {
		ctx = context.WithValue(ctx, attemptKey{}, driveSlotWrite)
	} else {
		ctx = context.WithValue(ctx, bodyKey{}, true)
	}
	fields := map[string]interface{}{"description": description}
	if properties := s.streamingProperties(); properties != nil {
		fields["properties"] = properties
	}
	metadata, err := json.Marshal(fields)
	if err != nil {
		return errors.New("failed to build the slot metadata").Base(err)
	}
	target := s.filesURL + "/" + url.PathEscape(id) + "?fields=id&supportsAllDrives=true"
	contentType := "application/json; charset=UTF-8"
	if body != nil {
		upload, _, _ := strings.Cut(s.uploadURL, "?")
		target = upload + "/" + url.PathEscape(id) + "?uploadType=multipart&fields=id&supportsAllDrives=true"
		metadata, contentType = multipartBody(metadata, body)
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

func (s *driveStorage) noteDescriptionRead() {
	s.readHedgeMu.Lock()
	defer s.readHedgeMu.Unlock()
	accrueHedgeToken(&s.readHedgeCount, &s.readHedgeTokens)
}

func (s *driveStorage) takeHedgeToken() bool {
	s.readHedgeMu.Lock()
	defer s.readHedgeMu.Unlock()
	return spendHedgeToken(&s.readHedgeTokens)
}

func (s *driveStorage) noteMediaRange() {
	s.readHedgeMu.Lock()
	defer s.readHedgeMu.Unlock()
	before := s.mediaHedgeTokens
	accrueHedgeToken(&s.mediaHedgeCount, &s.mediaHedgeTokens)
	if s.mediaHedgeTokens > before && s.mediaHedgeChanged != nil {
		close(s.mediaHedgeChanged)
		s.mediaHedgeChanged = nil
	}
}

func (s *driveStorage) takeMediaHedgeToken() bool {
	s.readHedgeMu.Lock()
	defer s.readHedgeMu.Unlock()
	return spendHedgeToken(&s.mediaHedgeTokens)
}

func (s *driveStorage) takeMediaHedgeTokenOrWait() (bool, <-chan struct{}) {
	s.readHedgeMu.Lock()
	defer s.readHedgeMu.Unlock()
	if spendHedgeToken(&s.mediaHedgeTokens) {
		return true, nil
	}
	if s.mediaHedgeChanged == nil {
		s.mediaHedgeChanged = make(chan struct{})
	}
	return false, s.mediaHedgeChanged
}

func accrueHedgeToken(count, tokens *int) {
	*count++
	if *count >= 20 {
		*count = 0
		if *tokens < 6 {
			*tokens++
		}
	}
}

func spendHedgeToken(tokens *int) bool {
	if *tokens > 0 {
		*tokens--
		return true
	}
	return false
}

func (s *driveStorage) descriptionHedgeAllowed() bool {
	s.coolMu.Lock()
	defer s.coolMu.Unlock()
	now := time.Now()
	return !now.Before(s.coolUntil) && (s.lastRateLimit.IsZero() || now.Sub(s.lastRateLimit) > driveDescriptionHedgeQuiet)
}

func (s *driveStorage) readHedgeAllowed(ctx context.Context) bool {
	if gate, ok := ctx.Value(readHedgeGateKey{}).(readHedgeGate); ok {
		var remaining time.Duration
		var pending bool
		if gate.remaining != nil {
			remaining, pending = gate.remaining()
		} else {
			remaining, pending = gate.primary.remaining(gate.delay)
		}
		if !pending || remaining > 0 {
			return false
		}
	}
	if ctx.Value(descriptionHedgeKey{}) != nil || ctx.Value(directProbeKey{}) != nil {
		return s.descriptionHedgeAllowed()
	}
	if media, ok := ctx.Value(driveMediaKey{}).(*driveMediaRequest); ok && media.rescue {
		if !mediaHead(ctx) {
			return false
		}
		s.coolMu.Lock()
		defer s.coolMu.Unlock()
		now := time.Now()
		return !now.Before(s.coolUntil) && (s.lastRateLimit.IsZero() || now.Sub(s.lastRateLimit) > 5*time.Second)
	}
	return true
}

func (s *driveStorage) readDescHedged(ctx context.Context, target string) (status int, payload []byte, err error) {
	if ctx.Value(laneKey{}) == nil {
		return s.do(ctx, http.MethodGet, target, "", nil)
	}
	defer func() {
		if err == nil && (status == http.StatusOK || status == http.StatusNotFound) {
			s.noteDescriptionRead()
		}
	}()
	if s.readHedge <= 0 || (s.priorityClient == nil && s.bulkClient == nil) || !s.descriptionHedgeAllowed() {
		return s.do(ctx, http.MethodGet, target, "", nil)
	}
	type res struct {
		status  int
		payload []byte
		err     error
		hedge   bool
	}
	ch := make(chan res, 2)
	primary := &driveReadProgress{dispatched: make(chan struct{})}
	ctx1, cancel1 := context.WithCancel(context.WithValue(ctx, readProgressKey{}, primary))
	defer cancel1()
	ctx1 = context.WithValue(ctx1, bodyProgressKey{}, primary.progress)
	go func() {
		st, pl, e := s.do(ctx1, http.MethodGet, target, "", nil)
		ch <- res{st, pl, e, false}
	}()
	select {
	case r := <-ch:
		return r.status, r.payload, r.err
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case <-primary.dispatched:
	}
	delay, _ := primary.remaining(s.readHedge)
	timer := time.NewTimer(max(0, delay))
	defer timer.Stop()
	for {
		select {
		case r := <-ch:
			return r.status, r.payload, r.err
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-timer.C:
		}
		remaining, pending := primary.remaining(s.readHedge)
		if pending && remaining > 0 {
			timer.Reset(remaining)
			continue
		}
		if pending && s.descriptionHedgeAllowed() && s.readHedgeActive.CompareAndSwap(false, true) {
			if s.takeHedgeToken() {
				break
			}
			s.readHedgeActive.Store(false)
		}

		select {
		case r := <-ch:
			return r.status, r.payload, r.err
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}
	hedgeCtx := context.WithValue(ctx, descriptionHedgeKey{}, true)
	hedgeCtx = context.WithValue(hedgeCtx, readHedgeGateKey{}, readHedgeGate{primary: primary, delay: s.readHedge})
	if s.priorityClient == nil {
		hedgeCtx = context.WithValue(hedgeCtx, bodyKey{}, true)
	}
	ctx2, cancel2 := context.WithCancel(hedgeCtx)
	defer cancel2()
	go func() {
		defer s.readHedgeActive.Store(false)
		st, pl, e := s.do(ctx2, http.MethodGet, target, "", nil)
		ch <- res{st, pl, e, true}
	}()
	expected, slotRead := ctx.Value(slotExpectedSeqKey{}).(int64)
	match := func(r res) bool {
		if r.status != http.StatusOK || r.err != nil {
			return false
		}
		if !slotRead {
			return true
		}
		var payload struct{ Description string }
		if json.Unmarshal(r.payload, &payload) != nil {
			return false
		}
		frame, ok := decodeSlot(payload.Description)
		return ok && frame.seq == expected
	}
	pick := func(r res) (int, []byte, error) {
		if r.hedge && match(r) && s.timing != nil {
			s.timing.noteHedgeWin(kindHeadRead)
		}
		return r.status, r.payload, r.err
	}
	var first res
	select {
	case first = <-ch:
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
	if match(first) || isAuthError(first.err) {
		return pick(first)
	}
	// A fresh frame can race a missing or previous-lap snapshot. Give the
	// already dispatched sibling a short chance to answer without making
	// each genuine miss wait through the full HTTP timeout.
	grace := time.NewTimer(slotDescriptionMissGrace)
	defer grace.Stop()
	select {
	case second := <-ch:
		if match(second) || isAuthError(second.err) ||
			second.err == nil && (second.status == http.StatusOK ||
				second.status == http.StatusNotFound && (first.err != nil || first.status != http.StatusOK)) {
			return pick(second)
		}
		return pick(first)
	case <-grace.C:
		return pick(first)
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (s *driveStorage) readDescription(ctx context.Context, id string) (string, error) {
	ctx = context.WithValue(ctx, attemptKey{}, driveSlotRead)
	status, payload, err := s.readDescHedged(ctx, s.filesURL+"/"+url.PathEscape(id)+"?fields="+s.streamingFields()+"&supportsAllDrives=true")
	switch {
	case err != nil:
		return "", err
	case status == http.StatusNotFound:
		return "", errNotFound
	case status != http.StatusOK:
		return "", errors.New("Drive answered ", status, " for the description of ", id, ": ", string(payload))
	}

	var parsed struct {
		Description string            `json:"description"`
		Properties  map[string]string `json:"properties"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return "", errors.New("failed to parse the description of ", id).Base(err)
	}
	s.observeStreamingProperties(parsed.Properties)
	return parsed.Description, nil
}

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

// Owner lifecycle.

// Register before launch; owner shutdown cancels and joins shared work.

func (s *driveShared) beginWork(timeout time.Duration) (context.Context, func(), bool) {
	s.workMu.Lock()
	defer s.workMu.Unlock()
	if s.workClosed {
		return nil, nil, false
	}
	if s.workCtx == nil {
		s.workCtx, s.workCancel = context.WithCancel(context.Background())
	}
	ctx := context.WithValue(s.workCtx, keeperKey{}, true)
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	s.workWG.Add(1)
	return ctx, func() { cancel(); s.workWG.Done() }, true
}

func (s *driveShared) beginRequest(parent context.Context) (context.Context, func(), bool) {
	s.workMu.Lock()
	defer s.workMu.Unlock()
	if s.workClosed {
		return nil, nil, false
	}
	if s.workCtx == nil {
		s.workCtx, s.workCancel = context.WithCancel(context.Background())
	}
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(s.workCtx, cancel)
	s.workWG.Add(1)
	return ctx, func() { stop(); cancel(); s.workWG.Done() }, true
}

func (s *driveShared) cancelWork() {
	s.workMu.Lock()
	s.workClosed = true
	if s.workCancel != nil {
		s.workCancel()
	}
	s.workMu.Unlock()
}

func (s *driveShared) closeWork() {
	s.cancelWork()
	s.workWG.Wait()
}

func (s *driveShared) isWorkClosed() bool {
	s.workMu.Lock()
	defer s.workMu.Unlock()
	return s.workClosed
}

// Generated IDs and slot creation.

type idPoolEntry struct {
	id string
	at time.Time
}

const idPoolTTL = 10 * time.Minute

func (s *driveShared) idPoolCap() int {
	return max(3*s.poolRing, 1000)
}

func (s *driveStorage) generateIDs(ctx context.Context, n int) ([]string, error) {
	if n > 1000 {
		n = 1000
	}
	status, payload, err := s.do(ctx, http.MethodGet,
		s.filesURL+"/generateIds?count="+strconv.Itoa(n)+"&space=drive", "", nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, errors.New("Drive generateIds answered ", status, ": ", string(payload))
	}
	var parsed struct {
		IDs []string `json:"ids"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return nil, errors.New("failed to parse Drive generateIds").Base(err)
	}
	if len(parsed.IDs) != n {
		return nil, errors.New("Drive returned ", len(parsed.IDs), " ids, want ", n)
	}
	seen := make(map[string]bool, len(parsed.IDs))
	for _, id := range parsed.IDs {
		if len(id) == 0 || len(id) > maxSlotIDLen || seen[id] {
			return nil, errors.New("Drive returned invalid or repeated generated ids")
		}
		seen[id] = true
	}
	return parsed.IDs, nil
}

func (s *driveStorage) takeIDs(parent context.Context, n, ringWorth int) ([]string, error) {
	ctx, finish, ok := s.beginRequest(parent)
	if !ok {
		return nil, context.Canceled
	}
	defer finish()
	if n <= 0 {
		return nil, nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if s.isWorkClosed() {
			return nil, context.Canceled
		}
		s.poolMu.Lock()
		s.poolRing = max(s.poolRing, 2*ringWorth)
		s.prunePoolLocked()
		if len(s.idPool) < n {
			if f := s.poolFlight; f != nil {
				s.poolMu.Unlock()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-f.done:

					s.poolMu.Lock()
					filled := len(s.idPool) >= n
					s.poolMu.Unlock()
					if filled {
						continue
					}
					if err := flightFailure(f.err); err != nil {
						return nil, err
					}
				}
				continue
			}
			if time.Now().Before(s.poolRetryAt) && s.poolErr != nil {
				err := s.poolErr
				s.poolMu.Unlock()
				return nil, err
			}
		}
		take := min(n, len(s.idPool))
		taken := append([]idPoolEntry(nil), s.idPool[:take]...)
		s.idPool = s.idPool[take:]
		out := make([]string, 0, n)
		for _, e := range taken {
			out = append(out, e.id)
		}
		if take == n {
			s.refillPoolLocked()
			s.poolMu.Unlock()
			return out, nil
		}
		shortfall := n - take
		batch := max(shortfall, min(s.idPoolCap(), 1000))
		f := &idPoolFlight{done: make(chan struct{})}
		s.poolFlight = f
		s.poolMu.Unlock()

		var ids []string
		var err error
		if shortfall > 1000 {
			err = errors.New("Drive id shortfall exceeds 1000")
		} else {
			ids, err = s.generateIDs(ctx, batch)
			if err == nil && len(ids) != batch {
				err = errors.New("Drive returned ", len(ids), " ids, want ", batch)
			}
		}
		s.poolMu.Lock()
		if err != nil {
			s.idPool = append(s.idPool, taken...)
		} else {
			now := time.Now()
			for _, id := range ids[shortfall:] {
				s.idPool = append(s.idPool, idPoolEntry{id: id, at: now})
			}
		}
		s.prunePoolLocked()
		s.finishPoolFlightLocked(f, err)
		if err == nil {
			s.refillPoolLocked()
		}
		s.poolMu.Unlock()
		if err != nil {
			return nil, err
		}
		return append(out, ids[:shortfall]...), nil
	}
}

func (s *driveShared) refillPoolLocked() {
	if s.owner == nil || len(s.idPool) >= 2*s.poolRing {
		return
	}
	f, want := s.beginRefillLocked(false)
	if f == nil {
		return
	}
	ctx, finish, ok := s.beginWork(30 * time.Second)
	if !ok {
		s.finishPoolFlightLocked(f, context.Canceled)
		return
	}
	go func() { defer finish(); s.owner.refillIDs(ctx, f, want) }()
}

func (s *driveShared) prunePoolLocked() {
	if s.poolRing <= 0 {
		s.poolRing = 2 * (64 + 1)
	}
	now := time.Now()
	kept := s.idPool[:0]
	for _, e := range s.idPool {
		if now.Sub(e.at) < idPoolTTL {
			kept = append(kept, e)
		}
	}
	s.idPool = kept[max(0, len(kept)-s.idPoolCap()):]
}

func (s *driveShared) beginRefillLocked(refreshOld bool) (*idPoolFlight, int) {
	if s.poolFlight != nil || time.Now().Before(s.poolRetryAt) {
		return nil, 0
	}
	want := s.idPoolCap() - len(s.idPool)
	if refreshOld {
		for _, e := range s.idPool {
			if time.Since(e.at) > 5*time.Minute {
				want++
			}
		}
	}
	if want <= 0 {
		return nil, 0
	}
	f := &idPoolFlight{done: make(chan struct{})}
	s.poolFlight = f
	return f, want
}

func (s *driveShared) finishPoolFlightLocked(f *idPoolFlight, err error) {
	f.err = err
	s.poolErr = err
	if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) || stderrors.Is(err, errSessionClosed) {
		s.poolErr = nil
		s.poolRetryAt = time.Time{}
	} else if err != nil {
		s.coolMu.Lock()
		s.poolRetryAt = s.coolUntil
		s.coolMu.Unlock()
	} else {
		s.poolRetryAt = time.Time{}
	}
	s.poolFlight = nil
	close(f.done)
}

func flightFailure(err error) error {
	if err == nil || stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) || stderrors.Is(err, errSessionClosed) {
		return nil
	}
	return err
}

func (s *driveStorage) refillIDs(ctx context.Context, f *idPoolFlight, want int) {
	var err error
	for want > 0 {
		batch := min(want, 1000)
		var ids []string
		ids, err = s.generateIDs(ctx, batch)
		if err == nil && len(ids) != batch {
			err = errors.New("Drive returned ", len(ids), " ids, want ", batch)
		}
		if err != nil {
			break
		}
		s.poolMu.Lock()
		now := time.Now()
		for _, id := range ids {
			s.idPool = append(s.idPool, idPoolEntry{id: id, at: now})
		}
		s.prunePoolLocked()
		s.poolMu.Unlock()
		want -= batch
	}
	s.poolMu.Lock()
	defer s.poolMu.Unlock()
	s.prunePoolLocked()
	s.finishPoolFlightLocked(f, err)
}

type createError struct {
	status  int
	payload []byte
}

func (e createError) Error() string {
	return "Drive create answered " + strconv.Itoa(e.status) + ": " + string(e.payload)
}

func (s *driveStorage) createWithID(ctx context.Context, metadata map[string]interface{}, body []byte) error {
	md, err := json.Marshal(metadata)
	if err != nil {
		return errors.New("failed to build the create metadata").Base(err)
	}
	target := s.filesURL + "?fields=id&supportsAllDrives=true"
	contentType := "application/json; charset=UTF-8"
	payload := md
	if body != nil {
		ctx = context.WithValue(ctx, bodyKey{}, true)
		target = withAllDrives(s.uploadURL)
		payload, contentType = multipartBody(md, body)
	}
	status, reply, err := s.do(ctx, http.MethodPost, target, contentType, payload)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return createError{status: status, payload: reply}
	}
	name, _ := metadata["name"].(string)
	id, _ := metadata["id"].(string)
	s.rememberID(name, id, nil)
	var parsed struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(reply, &parsed) == nil {
		s.rememberID(name, parsed.ID, nil)
	}
	return nil
}

func (s *driveStorage) createSlot(ctx context.Context, id, name, description string, body []byte) error {
	flat := flatten(name)
	metadata := map[string]interface{}{
		"id":          id,
		"name":        flat,
		"parents":     []string{s.folder},
		"description": description,
	}
	if properties := s.streamingProperties(); properties != nil {
		metadata["properties"] = properties
	}
	err := s.createWithID(ctx, metadata, body)
	if err == nil {
		return nil
	}
	ce, ok := err.(createError)
	if ok && ce.status >= 400 && ce.status < 500 && ce.status != http.StatusUnauthorized &&
		ce.status != http.StatusTooManyRequests && !(ce.status == http.StatusForbidden && rateLimited(ce.payload)) {
		if existing, derr := s.readDescription(ctx, id); derr == nil {

			if body != nil || existing != description {
				s.fallbackLogOnce.Do(func() {
					errors.LogInfo(ctx, "xdrive: create with pre-generated id fell back to update")
				})
				if err := s.setSlot(ctx, id, description, body); err != nil {
					return err
				}
			}
			s.rememberID(flat, id, nil)
			return nil
		} else if isAuthError(derr) || stderrors.Is(derr, context.Canceled) || stderrors.Is(derr, context.DeadlineExceeded) {
			return derr
		}
	}
	return err
}

type idPoolFlight struct {
	done chan struct{}
	err  error
}

type idPoolState struct {
	poolMu          sync.Mutex
	idPool          []idPoolEntry
	poolRing        int
	poolFlight      *idPoolFlight
	poolRetryAt     time.Time
	poolErr         error
	fallbackLogOnce sync.Once
}

// Connection maintenance.

type keeperKey struct{}

// Maintenance probes do not retry.

type keeperProbeKey struct{}

func (s *driveShared) noteTraffic() {
	s.trafficMu.Lock()
	s.lastTraffic = time.Now()
	s.trafficMu.Unlock()
	s.startKeeper()
}

func (s *driveShared) noteRequest(ctx context.Context) {
	s.trafficMu.Lock()
	s.lastRequest = time.Now()
	traffic := ctx.Value(keeperKey{}) == nil
	if traffic {
		s.lastTraffic = s.lastRequest
	}
	s.trafficMu.Unlock()
	if traffic {
		s.startKeeper()
	}
}

func (s *driveShared) startKeeper() {
	s.keeperMu.Lock()
	defer s.keeperMu.Unlock()
	if s.keeperClosed || s.keeperRunning || s.owner == nil {
		return
	}
	if s.keepWarmEvery <= 0 {
		s.keepWarmEvery = 4 * time.Minute
	}
	if s.keepWarmWindow <= 0 {
		s.keepWarmWindow = 3 * time.Hour
	}
	workCtx, finish, ok := s.beginWork(0)
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(workCtx)
	s.keeperCancel = cancel
	s.keeperDone = make(chan struct{})
	s.keeperRunning = true
	done := s.keeperDone
	go func() { defer finish(); defer cancel(); s.keepWarm(ctx, done) }()
}

func (s *driveShared) stopKeeper() {
	s.keeperMu.Lock()
	s.keeperClosed = true
	done := s.keeperDone
	cancel := s.keeperCancel
	s.keeperMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (s *driveShared) keeperStorage() *driveStorage {
	warm := &driveStorage{
		driveShared: s,
		client:      s.owner.client,
		inflight:    s.owner.inflight,
		reserved:    s.owner.reserved,
	}
	var transports []*sessionTransport
	if s.conns != nil {
		t := &sessionTransport{pool: s.conns}
		warm.client = &http.Client{Transport: t}
		transports = append(transports, t)
	}
	if s.bulkConns != nil {
		t := &sessionTransport{pool: s.bulkConns}
		warm.bulkClient = &http.Client{Transport: t}
		transports = append(transports, t)
	}
	warm.release = func() {
		for _, t := range transports {
			t.Close()
		}
	}
	return warm
}

func (s *driveStorage) keepWarmConnection(ctx context.Context, target string) error {
	ctx = context.WithValue(ctx, keeperProbeKey{}, true)
	status, _, err := s.do(ctx, http.MethodGet, target, "", nil)
	if err == nil && status != http.StatusOK {
		err = errors.New("Drive keep-warm answered ", status)
	}
	return err
}

func (s *driveShared) keepWarm(runCtx context.Context, done chan struct{}) {
	t := time.NewTicker(s.keepWarmEvery)
	defer t.Stop()
	var warm *driveStorage
	defer func() {
		if warm != nil {
			warm.Close()
		}
	}()
	for {
		select {
		case <-runCtx.Done():
		case <-t.C:
		}
		s.keeperMu.Lock()
		s.trafficMu.Lock()
		lastTraffic, lastRequest := s.lastTraffic, s.lastRequest
		s.trafficMu.Unlock()
		if runCtx.Err() != nil || time.Since(lastTraffic) > s.keepWarmWindow {
			s.keeperRunning = false
			close(done)
			s.keeperMu.Unlock()
			return
		}
		s.keeperMu.Unlock()
		o := s.owner
		if o.cooling() || time.Since(o.lastRateLimitAt()) <= 5*time.Second {
			continue
		}
		ctx, cancel := context.WithTimeout(runCtx, 30*time.Second)
		o.tokenMu.Lock()
		tokenBefore := o.token
		needsToken := tokenBefore == "" || !time.Now().Before(o.tokenExpiry)
		o.tokenMu.Unlock()
		tok, err := o.accessToken(ctx)
		if err != nil {
			errors.LogDebug(ctx, "xdrive keep-warm token: ", err)
		}
		tokenFetched := needsToken || tok != tokenBefore
		if tokenFetched {
			s.noteRequest(ctx)
		}
		s.poolMu.Lock()
		s.prunePoolLocked()
		var flight *idPoolFlight
		var want int
		if err == nil && !tokenFetched && time.Since(lastRequest) > 2*time.Minute {
			low, old := len(s.idPool) < 2*s.poolRing, false
			for _, e := range s.idPool {
				old = old || time.Since(e.at) > 5*time.Minute
			}
			if low || old {
				flight, want = s.beginRefillLocked(old)
			}
		}
		s.poolMu.Unlock()
		idle := err == nil && time.Since(lastRequest) > 2*time.Minute
		if (idle || flight != nil) && warm == nil {
			warm = s.keeperStorage()
		}
		if flight != nil {
			warm.refillIDs(ctx, flight, want)
			if flight.err != nil {
				errors.LogDebug(ctx, "xdrive keep-warm ids: ", flight.err)
			}
		}
		if idle && ctx.Err() == nil && (flight == nil || flight.err == nil) {
			target := s.filesURL + "/" + url.PathEscape(s.folder) + "?fields=id&supportsAllDrives=true"
			if s.conns != nil && flight == nil {
				if err := warm.keepWarmConnection(ctx, target); err != nil {
					errors.LogDebug(ctx, "xdrive keep-warm connection: ", err)
				}
			}
			if s.bulkConns != nil && ctx.Err() == nil {
				if err := warm.keepWarmConnection(context.WithValue(ctx, bodyKey{}, true), target); err != nil {
					errors.LogDebug(ctx, "xdrive keep-warm connection: ", err)
				}
			}
		}
		cancel()
		if warm != nil {
			warm.Close()
			warm = nil
		}
	}
}

// Media ranges and adaptive recovery.

const (
	// Media GETs cost 200 quota units; recovery does not expand the range budget.

	driveMediaLanes = 4
	driveMediaPart  = 128 << 10

	driveMediaInflight = driveMaxInflight - 2*driveReservedInflight
)

type driveMediaKey struct{}

type (
	mediaHeadKey      struct{}
	mediaPromotionKey struct{}
)

type mediaDeliveryKey struct{}

func mediaHead(ctx context.Context) bool {
	if ctx.Value(mediaHeadKey{}) != nil {
		return true
	}
	promoted, _ := ctx.Value(mediaPromotionKey{}).(<-chan struct{})
	select {
	case <-promoted:
		return true
	default:
		return false
	}
}

type driveMediaRequest struct {
	start, end, size int
	seq              int64
	client           *http.Client
	rescue           bool
	direct           *slotFrame // expected envelope for a full direct-media object
	stream           *driveRangeStream
	progress         *driveRangeProgress
}

type driveRangeProgress struct {
	mu   sync.Mutex
	next int
}

func (p *driveRangeProgress) verified(start, end int) {
	p.mu.Lock()
	if start == p.next {
		p.next = end
	}
	p.mu.Unlock()
}

func (p *driveRangeProgress) resume(r driveMediaRequest) driveMediaRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Even a fully delivered range must revalidate a nonempty suffix after EOF failure.
	last := (r.end - 1) / driveStreamBlockBytes * driveStreamBlockBytes
	r.start = max(r.start, min(p.next, last))
	return r
}

func (r *driveMediaRequest) extract(status int, contentRange string, body []byte) ([]byte, error) {
	if status == http.StatusOK {
		if len(body) == r.size {
			return body[r.start:r.end], nil
		}
	} else if contentRange == fmt.Sprintf("bytes %d-%d/%d", r.start, r.end-1, r.size) && len(body) == r.end-r.start {
		return body, nil
	}
	return nil, errors.New("Drive media range has an unexpected offset or length")
}

func (s *driveStorage) readFrameBody(ctx context.Context, id string, frame slotFrame) ([]byte, error) {
	if frame.size <= 0 || frame.size > maxSegmentBytes {
		return nil, errors.New("invalid Drive media frame size")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx = context.WithValue(ctx, bodyKey{}, true)
	ctx = context.WithValue(ctx, maxBodyKey{}, int64(max(frame.size, 64<<10)))
	target := s.filesURL + "/" + url.PathEscape(id) + "?alt=media&supportsAllDrives=true"
	lanes := max(1, len(s.mediaClients))

	partSize := int(defaultSegmentBytes)
	if ctx.Value(mediaHeadKey{}) != nil {
		partSize = driveMediaPart
	}
	parts := max(1, min((frame.size+partSize-1)/partSize, lanes))
	if s.cooling() || time.Since(s.lastRateLimitAt()) <= 5*time.Second {
		parts = 1
	}
	first := s.mediaNext.Add(uint32(parts)) - uint32(parts)
	if parts == 1 {
		r := driveMediaRequest{end: frame.size, size: frame.size, seq: frame.seq}
		if len(s.mediaClients) > 0 {
			r.client = s.mediaClients[int(first%uint32(len(s.mediaClients)))]
		}
		body, err := s.readMediaRange(ctx, target, r)
		if err != nil {
			return nil, err
		}
		if !frame.holds(body) {
			return nil, errors.New("Drive media ranges do not match the segment checksum")
		}
		return body, nil
	}
	width := (frame.size + parts - 1) / parts
	type result struct {
		start int
		body  []byte
		err   error
	}
	results := make(chan result, parts)
	for i := range parts {
		r := driveMediaRequest{start: i * width, end: min((i+1)*width, frame.size), size: frame.size, seq: frame.seq}
		if len(s.mediaClients) > 0 {
			r.client = s.mediaClients[int((first+uint32(i))%uint32(len(s.mediaClients)))]
		}
		go func() {
			body, err := s.readMediaRange(ctx, target, r)
			results <- result{r.start, body, err}
		}()
	}
	body := make([]byte, frame.size)
	for range parts {
		select {
		case res := <-results:
			if res.err != nil {
				return nil, res.err
			}
			copy(body[res.start:], res.body)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if !frame.holds(body) {
		return nil, errors.New("Drive media ranges do not match the segment checksum")
	}
	return body, nil
}

func mediaDeliveryRemaining(ctx context.Context, primary *driveReadProgress, r driveMediaRequest, delay time.Duration, delivery func() (int, <-chan struct{}, time.Time)) (time.Duration, bool, <-chan struct{}) {
	next, changed, waitAt := delivery()
	if !mediaHead(ctx) {
		return 0, false, changed
	}
	if next == r.size {
		remaining, active := primary.remainingSinceDispatch(delay)
		return remaining, active, changed
	}
	if next < r.start || next >= r.end || waitAt.IsZero() {
		return 0, false, changed
	}
	remaining, active := primary.remainingSinceDispatch(delay)
	return remaining, active, changed
}

// One rescue per required head range, under the existing media budget.

func (s *driveStorage) readMediaRange(ctx context.Context, target string, r driveMediaRequest) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if r.stream != nil {
		r.progress = &driveRangeProgress{next: r.start}
	}
	primary := &driveReadProgress{dispatched: make(chan struct{})}
	delivery, tracked := ctx.Value(mediaDeliveryKey{}).(func() (int, <-chan struct{}, time.Time))
	tracked = tracked && delivery != nil && r.stream != nil && r.stream.frame.mode == slotStream
	hedgeDelay := slotBodyHedge
	gate := readHedgeGate{primary: primary, delay: hedgeDelay}
	if tracked {
		gate.remaining = func() (time.Duration, bool) {
			remaining, eligible, _ := mediaDeliveryRemaining(ctx, primary, r, hedgeDelay, delivery)
			return remaining, eligible
		}
	} else {
		gate.remaining = func() (time.Duration, bool) {
			remaining, active := primary.remainingSinceDispatch(hedgeDelay)
			return remaining, active && mediaHead(ctx)
		}
	}
	type result struct {
		body  []byte
		err   error
		hedge bool
	}
	results := make(chan result, 2)
	launch := func(hedge bool) {
		request := r
		requestCtx := ctx
		if hedge {
			request.client = s.bulkClient
			request.rescue = true
			request.progress = nil
			requestCtx = context.WithValue(requestCtx, readHedgeGateKey{}, gate)
		} else {
			requestCtx = context.WithValue(requestCtx, readProgressKey{}, primary)
			requestCtx = context.WithValue(requestCtx, bodyProgressKey{}, primary.progress)
		}
		go func() {
			status, body, err := s.do(context.WithValue(requestCtx, driveMediaKey{}, &request), http.MethodGet, target, "", nil)
			if err == nil {
				switch status {
				case http.StatusOK, http.StatusPartialContent:
				case http.StatusNotFound:
					err = errNotFound
				default:
					err = errors.New("Drive media range answered ", status)
				}
			}
			results <- result{body, err, hedge}
		}()
	}
	launch(false)
	pending := 1

	var timer *time.Timer
	var hedgeAt <-chan time.Time
	var dispatched <-chan struct{}
	var promoted <-chan struct{}
	var frontier, budgetChanged <-chan struct{}
	var recoveryStopped, hedged bool
	promotion, _ := ctx.Value(mediaPromotionKey{}).(<-chan struct{})
	if hedgeDelay > 0 {
		dispatched = primary.dispatched
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	armTracked := func() {
		if timer != nil {
			timer.Stop()
		}
		hedgeAt, frontier, budgetChanged, promoted = nil, nil, nil, nil
		if recoveryStopped || hedged {
			return
		}
		if _, active := primary.remainingSinceDispatch(hedgeDelay); !active {
			recoveryStopped = true
			return
		}
		if !mediaHead(ctx) {
			promoted = promotion
			return
		}
		remaining, eligible, changed := mediaDeliveryRemaining(ctx, primary, r, hedgeDelay, delivery)
		frontier = changed
		if !eligible {
			return
		}
		if timer == nil {
			timer = time.NewTimer(max(0, remaining))
		} else {
			timer.Reset(max(0, remaining))
		}
		hedgeAt = timer.C
	}
	for {
		select {
		case res := <-results:
			pending--
			if res.err == nil {
				s.noteMediaRange()
				if res.hedge && s.timing != nil {
					s.timing.noteHedgeWin(kindBodyRead)
				}
				return res.body, nil
			}
			if pending == 0 || isAuthError(res.err) {
				return nil, res.err
			}
		case <-dispatched:
			dispatched = nil
			hedgeDelay = s.mediaHedgeDelay(r)
			gate.delay = hedgeDelay
			if tracked {
				armTracked()
				continue
			}
			if !mediaHead(ctx) {
				promoted = promotion
				continue
			}
			remaining, _ := primary.remainingSinceDispatch(hedgeDelay)
			timer = time.NewTimer(max(0, remaining))
			hedgeAt = timer.C
		case <-promoted:
			promoted = nil
			if tracked {
				armTracked()
				continue
			}
			remaining, active := primary.remainingSinceDispatch(hedgeDelay)
			if active {

				if timer == nil {
					timer = time.NewTimer(max(0, remaining))
				} else {
					timer.Reset(max(0, remaining))
				}
				hedgeAt = timer.C
			}
		case <-frontier:
			armTracked()
		case <-budgetChanged:
			armTracked()
		case <-hedgeAt:
			helper := r
			helper.rescue = true

			hedgeAt = nil
			if s.bulkClient == nil || r.client == nil || r.client == s.bulkClient {
				if tracked {
					recoveryStopped = true
					armTracked()
				}
				continue
			}
			if tracked {
				remaining, eligible, changed := mediaDeliveryRemaining(ctx, primary, r, hedgeDelay, delivery)
				if !eligible || remaining > 0 {
					armTracked()
					continue
				}
				if !s.readHedgeAllowed(context.WithValue(ctx, driveMediaKey{}, &helper)) {

					recoveryStopped = true
					armTracked()
					continue
				}
				taken, changedBudget := s.takeMediaHedgeTokenOrWait()
				if !taken {
					frontier, budgetChanged = changed, changedBudget
					continue
				}
				hedged = true
				armTracked()
				pending++
				launch(true)
				continue
			}
			remaining, active := primary.remainingSinceDispatch(hedgeDelay)
			if !active {
				continue
			}
			if remaining > 0 {
				timer.Reset(remaining)
				hedgeAt = timer.C
				continue
			}
			if !mediaHead(ctx) {

				promoted = promotion
				if s.timing != nil {
					s.timing.noteDeferredHedge(kindBodyRead)
				}
				continue
			}
			if !s.readHedgeAllowed(context.WithValue(ctx, driveMediaKey{}, &helper)) || !s.takeMediaHedgeToken() {
				continue
			}
			hedged = true
			pending++
			launch(true)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Verified streaming.

const driveStreamBlockBytes = slotStreamBlockBytes

type driveRangeStream struct {
	frame slotFrame
	emit  func(int, []byte) error
	mu    sync.Mutex
	wg    sync.WaitGroup
	done  bool
}

func (s *driveRangeStream) closeAndWait() {
	s.mu.Lock()
	s.done = true
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *driveRangeStream) read(request driveMediaRequest, status int, contentRange string, contentLength int64, body io.Reader) ([]byte, error) {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return nil, context.Canceled
	}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()
	frame := s.frame
	if frame.size <= 0 || frame.size > maxSegmentBytes || request.size != frame.size ||
		request.start < 0 || request.start >= request.end || request.end > frame.size ||
		request.start%driveStreamBlockBytes != 0 ||
		(request.end != frame.size && request.end%driveStreamBlockBytes != 0) || s.emit == nil {
		return nil, errors.New("invalid Drive media stream range")
	}
	start, end := request.start, request.end
	switch status {
	case http.StatusPartialContent:
		if contentRange != fmt.Sprintf("bytes %d-%d/%d", start, end-1, frame.size) {
			return nil, errors.New("Drive media stream has an unexpected Content-Range")
		}
	case http.StatusOK:

		start, end = 0, frame.size
	default:
		return nil, errors.New("Drive media stream answered ", status)
	}

	if contentLength >= 0 && contentLength != int64(end-start) {
		return nil, errors.New("Drive media stream has an unexpected Content-Length")
	}
	for offset := start; offset < end; {
		index := offset / driveStreamBlockBytes
		size := streamBlockSize(frame.size, index)
		if size <= 0 || size > end-offset {
			return nil, errors.New("invalid Drive media stream block boundary")
		}
		block := make([]byte, size)
		if _, err := io.ReadFull(body, block); err != nil {
			return nil, errors.New("failed to read a complete Drive media stream block").Base(err)
		}
		if !validStreamBlock(frame, index, block) {
			return nil, errors.New("Drive media stream block does not match its checksum")
		}
		if offset >= request.start && offset < request.end {
			if err := s.emit(index, block); err != nil {
				return nil, err
			}
			if request.progress != nil {
				request.progress.verified(offset, offset+size)
			}
		}
		offset += size
	}
	var extra [1]byte
	n, err := io.ReadFull(body, extra[:])
	if n != 0 {
		return nil, errors.New("Drive media stream exceeds the expected response length")
	}
	if err != io.EOF {
		return nil, errors.New("failed to verify Drive media stream EOF").Base(err)
	}
	return nil, nil
}

func (s *driveStorage) readFrameStream(ctx context.Context, id string, frame slotFrame, emit func(int, []byte) error) error {
	if frame.size <= 0 || frame.size > maxSegmentBytes || frame.mode != slotStream ||
		len(frame.blocks) != (frame.size+driveStreamBlockBytes-1)/driveStreamBlockBytes || emit == nil {
		return errors.New("invalid Drive media stream frame")
	}
	ctx, cancel := context.WithCancel(ctx)
	ctx = context.WithValue(ctx, bodyKey{}, true)
	ctx = context.WithValue(ctx, maxBodyKey{}, int64(max(frame.size, 64<<10)))
	target := s.filesURL + "/" + url.PathEscape(id) + "?alt=media&supportsAllDrives=true"
	lanes := min(driveMediaLanes, max(1, len(s.mediaClients)))
	partSize := int(defaultSegmentBytes)
	if ctx.Value(mediaHeadKey{}) != nil {
		partSize = driveMediaPart
	}
	parts := max(1, min((frame.size+partSize-1)/partSize, lanes))
	if ctx.Value(mediaHeadKey{}) != nil {
		parts = min(parts, s.streamHeadRangeLimit())
	}
	if s.cooling() || time.Since(s.lastRateLimitAt()) <= 5*time.Second {
		parts = 1
	}
	if parts > 1 {
		errors.LogDebug(ctx, "xdrive stream plan slot=", slotTag(id), " seq=", frame.seq,
			" bytes=", frame.size, " parts=", parts, " head=", ctx.Value(mediaHeadKey{}) != nil,
			" head-limit=", s.streamHeadRangeLimit())
	}
	first := s.mediaNext.Add(uint32(parts)) - uint32(parts)
	stream := &driveRangeStream{frame: frame, emit: emit}
	defer func() {
		cancel()
		stream.closeAndWait()
	}()
	if parts == 1 {
		r := driveMediaRequest{end: frame.size, size: frame.size, seq: frame.seq, stream: stream}
		if len(s.mediaClients) > 0 {
			r.client = s.mediaClients[int(first%uint32(len(s.mediaClients)))]
		}
		_, err := s.readMediaRange(ctx, target, r)
		return err
	}
	blocks := len(frame.blocks)
	results := make(chan error, parts)
	for i := range parts {
		r := driveMediaRequest{
			start:  (i * blocks / parts) * driveStreamBlockBytes,
			end:    min(((i+1)*blocks/parts)*driveStreamBlockBytes, frame.size),
			size:   frame.size,
			seq:    frame.seq,
			stream: stream,
		}
		if len(s.mediaClients) > 0 {
			r.client = s.mediaClients[int((first+uint32(i))%uint32(len(s.mediaClients)))]
		}

		go func() {
			_, err := s.readMediaRange(ctx, target, r)
			results <- err
		}()
	}
	var firstErr error
	canceled := ctx.Done()
	for pending := parts; pending > 0; {
		select {
		case err := <-results:
			pending--
			if err != nil && firstErr == nil {
				firstErr = err
				cancel()
				canceled = nil
			}
		case <-canceled:
			firstErr = ctx.Err()
			cancel()
			canceled = nil
		}
	}
	return firstErr
}

// Streaming negotiation.

const (
	driveStreamCapsVersionKey = "xdriveStream"
	driveStreamCapsSessionKey = "xdriveSession"
	driveStreamCapsVersion    = "1"
)

// Session-local negotiation, not authentication or integrity protection.

type driveStreamCaps struct {
	session   string
	advertise bool
	peerReady atomic.Bool
}

// Configure before traffic; a new nonce starts a fresh capability epoch.

func (s *driveStorage) configureStreaming(session string, peerSupports bool) {
	if session == "" {
		return
	}
	for {
		current := s.streamCaps.Load()
		if current != nil && current.session == session {
			if peerSupports {
				current.peerReady.Store(true)
			}
			return
		}
		next := &driveStreamCaps{session: session, advertise: peerSupports}
		next.peerReady.Store(peerSupports)
		if s.streamCaps.CompareAndSwap(current, next) {
			return
		}
	}
}

func (s *driveStorage) streamingEnabled() bool {
	caps := s.streamCaps.Load()
	return caps != nil && caps.peerReady.Load()
}

// Only servers acknowledge accepted offers; client metadata must not self-enable.

func (s *driveStorage) streamingProperties() map[string]string {
	caps := s.streamCaps.Load()
	if caps == nil || !caps.advertise {
		return nil
	}
	return map[string]string{
		driveStreamCapsVersionKey: driveStreamCapsVersion,
		driveStreamCapsSessionKey: caps.session,
	}
}

func (s *driveStorage) observeStreamingProperties(properties map[string]string) {
	caps := s.streamCaps.Load()
	if caps != nil && properties[driveStreamCapsVersionKey] == driveStreamCapsVersion && properties[driveStreamCapsSessionKey] == caps.session {
		caps.peerReady.Store(true)
	}
}

func (s *driveStorage) streamingFields() string {
	if s.streamCaps.Load() != nil {
		return "description,properties"
	}
	return "description"
}

// Head-range experiment.

const streamHeadRangesEnv = "XRAY_XDRIVE_STREAM_HEAD_RANGES"

func parseStreamHeadRanges(raw string) (int, bool) {
	switch strings.TrimSpace(raw) {
	case "", "4":
		return 4, true
	case "2":
		return 2, true
	default:
		return 4, false
	}
}

func (s *driveStorage) streamHeadRangeLimit() int {
	if s.streamHeadRanges == 2 {
		return 2
	}
	return 4
}

// Latency and request telemetry.

const (
	kindCreate = iota
	kindSlotInline
	kindSlotMedia
	kindWatermark
	kindHeadRead
	kindOtherRead
	kindBodyRead
	kindDelete
	kindList
	kindToken
	kindCount
)

var kindNames = [kindCount]string{
	"create", "slot-write-inline", "slot-write-media", "watermark-write",
	"head-read", "other-read", "body-read", "delete", "list", "token-fetch",
}

type watermarkWriteKey struct{}

type timingSample struct {
	total       time.Duration
	waitCool    time.Duration
	waitPool    time.Duration
	waitToken   time.Duration
	waitConn    time.Duration
	dialTLS     time.Duration
	send        time.Duration
	server      time.Duration
	body        time.Duration
	reused      bool
	newConn     bool
	hedge       bool
	won         bool
	hasConn     bool
	hasDial     bool
	hasSend     bool
	hasServer   bool
	dispatch    time.Duration
	hasDispatch bool
}

const (
	timingWindow      = 60 * time.Second
	timingRing        = 512
	slowRequestTiming = 2 * time.Second
	rangeLatencyRing  = 32
	rangeLatencyMin   = 12
	mediaHedgeFloor   = 900 * time.Millisecond
)

// Only validated first-primary samples train size-class dispatch-to-EOF p50.

type rangeLatency struct {
	mu   sync.Mutex
	n    int
	pos  int
	ring [rangeLatencyRing]time.Duration
}

func (l *rangeLatency) note(d time.Duration) {
	if d <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ring[l.pos] = d
	l.pos = (l.pos + 1) % rangeLatencyRing
	l.n = min(l.n+1, rangeLatencyRing)
}

func (l *rangeLatency) median() (time.Duration, bool) {
	l.mu.Lock()
	if l.n < rangeLatencyMin {
		l.mu.Unlock()
		return 0, false
	}
	v := append([]time.Duration(nil), l.ring[:l.n]...)
	l.mu.Unlock()
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	return percentile(v, .5), true
}

func rangeClass(r driveMediaRequest) int {
	if r.end-r.start <= driveMediaPart {
		return 0
	}
	return 1
}

// Hedge at 1.3x p50, clamped to 900ms–2s; cold classes keep 2s.

func (s *driveStorage) mediaHedgeDelay(r driveMediaRequest) time.Duration {
	median, ok := s.mediaLatency[rangeClass(r)].median()
	if !ok {
		return slotBodyHedge
	}
	if median >= slotBodyHedge {
		return slotBodyHedge
	}
	return min(max(median*13/10, mediaHedgeFloor), slotBodyHedge)
}

func (s *driveStorage) observeMediaLatency(r *driveMediaRequest, sample timingSample, status, attempt int, replayed bool, err error) {
	if r == nil || r.rescue || attempt != 0 || replayed || err != nil || !sample.hasDispatch ||
		(status != http.StatusOK && status != http.StatusPartialContent) {
		return
	}
	s.mediaLatency[rangeClass(*r)].note(sample.dispatch)
}

type timingAggregator struct {
	mu         sync.Mutex
	started    bool
	stopOnce   sync.Once
	done       chan struct{}
	n          [kindCount]int
	pos        [kindCount]int
	ring       [kindCount][timingRing]timingSample
	conns      [kindCount]int
	hedges     [kindCount]int
	won        [kindCount]int
	requests   [kindCount]int
	failed     [kindCount]int
	canceled   [kindCount]int
	rateLimits [kindCount]int
	deferred   [kindCount]int
}

func (a *timingAggregator) noteFailure(kind int, canceled bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if canceled {
		a.canceled[kind]++
	} else {
		a.failed[kind]++
	}
}

func (a *timingAggregator) noteRateLimit(kind int) {
	a.mu.Lock()
	a.rateLimits[kind]++
	a.mu.Unlock()
}

func (a *timingAggregator) request(kind int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started {
		a.started = true
		go a.loop()
	}
	a.requests[kind]++
}

func driveQuotaCost(kind int) int {
	switch kind {
	case kindToken:
		return 0
	case kindCreate, kindSlotInline, kindSlotMedia, kindWatermark, kindDelete:
		return 50
	case kindBodyRead:
		return 200
	case kindList:
		return 100
	default:
		return 5
	}
}

func newTimingAggregator() *timingAggregator {
	return &timingAggregator{done: make(chan struct{})}
}

func (a *timingAggregator) record(kind int, s timingSample) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started {
		a.started = true
		go a.loop()
	}
	i := a.pos[kind]
	a.ring[kind][i] = s
	a.pos[kind] = (i + 1) % timingRing
	if a.n[kind] < timingRing {
		a.n[kind]++
	}
	if s.newConn {
		a.conns[kind]++
	}
	if s.hedge {
		a.hedges[kind]++
	}
	if s.won {
		a.won[kind]++
	}
}

func (a *timingAggregator) stop() {
	a.stopOnce.Do(func() { close(a.done) })
}

func (a *timingAggregator) noteHedge(kind int, won bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hedges[kind]++
	if won {
		a.won[kind]++
	}
}

func (a *timingAggregator) noteHedgeWin(kind int) {
	a.mu.Lock()
	a.won[kind]++
	a.mu.Unlock()
}

func (a *timingAggregator) noteDeferredHedge(kind int) {
	a.mu.Lock()
	a.deferred[kind]++
	a.mu.Unlock()
}

func (a *timingAggregator) loop() {
	t := time.NewTicker(timingWindow)
	defer t.Stop()
	for {
		select {
		case <-a.done:
			return
		case <-t.C:
			a.flush()
		}
	}
}

func percentile(v []time.Duration, p float64) time.Duration {
	if len(v) == 0 {
		return -1
	}
	return v[min(int(p*float64(len(v))), len(v)-1)]
}

func dialEnd(tlsDone, connDone time.Time) time.Time {
	if !tlsDone.IsZero() {
		return tlsDone
	}
	return connDone
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (s *driveStorage) recordTiming(kind int, sample timingSample) {
	if s.timing == nil {
		return
	}
	s.timing.record(kind, sample)
}

func (s *driveStorage) logSlowRequest(ctx context.Context, kind int, path string, sample timingSample, media *driveMediaRequest, headAtDispatch, headAtCompletion bool, status, attempt int) {
	if kind < 0 || kind >= kindCount || sample.total < slowRequestTiming {
		return
	}
	send, server, connection, dispatch := time.Duration(-1), time.Duration(-1), time.Duration(-1), time.Duration(-1)
	if sample.hasSend {
		send = sample.send
	}
	if sample.hasServer {
		server = sample.server
	}
	if sample.hasConn {
		connection = sample.waitConn
	}
	if sample.hasDispatch {
		dispatch = sample.dispatch
	}
	if media != nil {
		errors.LogDebug(ctx, "xdrive slow timing kind=", kindNames[kind], " slot=", slotTag(path),
			" status=", status, " attempt=", attempt,
			" seq=", media.seq, " range=", media.start, "-", media.end,
			" frame-bytes=", media.size, " requested-bytes=", media.end-media.start,
			" total=", sample.total, " pool=", sample.waitPool, " cool=", sample.waitCool,
			" dispatch-to-eof=", dispatch,
			" token=", sample.waitToken, " connection=", connection, " send=", send,
			" server=", server, " body=", sample.body, " rescue=", media.rescue,
			" head-at-dispatch=", headAtDispatch, " head-at-completion=", headAtCompletion)
		return
	}
	errors.LogDebug(ctx, "xdrive slow timing kind=", kindNames[kind], " slot=", slotTag(path),
		" status=", status, " attempt=", attempt,
		" total=", sample.total, " pool=", sample.waitPool, " cool=", sample.waitCool,
		" token=", sample.waitToken, " connection=", connection, " send=", send,
		" server=", server, " body=", sample.body)
}

type timingBatch struct {
	samples                      []timingSample
	requests, conns, hedges, won int
	failed, canceled, rateLimits int
	deferred                     int
}

func (a *timingAggregator) snapshot() [kindCount]timingBatch {
	a.mu.Lock()
	defer a.mu.Unlock()
	var batches [kindCount]timingBatch
	for k := range batches {
		batches[k] = timingBatch{
			samples:  append([]timingSample(nil), a.ring[k][:a.n[k]]...),
			requests: a.requests[k], conns: a.conns[k], hedges: a.hedges[k], won: a.won[k],
			failed: a.failed[k], canceled: a.canceled[k], rateLimits: a.rateLimits[k],
			deferred: a.deferred[k],
		}
		a.n[k], a.pos[k], a.conns[k], a.hedges[k], a.won[k], a.requests[k] = 0, 0, 0, 0, 0, 0
		a.failed[k], a.canceled[k], a.rateLimits[k] = 0, 0, 0
		a.deferred[k] = 0
	}
	return batches
}

func (a *timingAggregator) flush() {
	for k, batch := range a.snapshot() {
		n := len(batch.samples)
		if n == 0 && batch.requests == 0 && batch.hedges == 0 && batch.won == 0 && batch.failed == 0 && batch.canceled == 0 && batch.rateLimits == 0 && batch.deferred == 0 {
			continue
		}
		total := make([]time.Duration, 0, n)
		pool := make([]time.Duration, 0, n)
		cool := make([]time.Duration, 0, n)
		token := make([]time.Duration, 0, n)
		conn := make([]time.Duration, 0, n)
		dial := make([]time.Duration, 0, n)
		send := make([]time.Duration, 0, n)
		server := make([]time.Duration, 0, n)
		body := make([]time.Duration, 0, n)
		dispatch := make([]time.Duration, 0, n)
		for _, s := range batch.samples {
			total = append(total, s.total)
			pool = append(pool, s.waitPool)
			cool = append(cool, s.waitCool)
			token = append(token, s.waitToken)
			if s.hasConn {
				conn = append(conn, s.waitConn)
			}
			if s.hasDial {
				dial = append(dial, s.dialTLS)
			}
			if s.hasSend {
				send = append(send, s.send)
			}
			if s.hasServer {
				server = append(server, s.server)
			}
			body = append(body, s.body)
			if s.hasDispatch {
				dispatch = append(dispatch, s.dispatch)
			}
		}
		sort.Slice(total, func(i, j int) bool { return total[i] < total[j] })
		sort.Slice(pool, func(i, j int) bool { return pool[i] < pool[j] })
		sort.Slice(cool, func(i, j int) bool { return cool[i] < cool[j] })
		sort.Slice(token, func(i, j int) bool { return token[i] < token[j] })
		sort.Slice(conn, func(i, j int) bool { return conn[i] < conn[j] })
		sort.Slice(dial, func(i, j int) bool { return dial[i] < dial[j] })
		sort.Slice(send, func(i, j int) bool { return send[i] < send[j] })
		sort.Slice(server, func(i, j int) bool { return server[i] < server[j] })
		sort.Slice(body, func(i, j int) bool { return body[i] < body[j] })
		sort.Slice(dispatch, func(i, j int) bool { return dispatch[i] < dispatch[j] })
		errors.LogInfo(context.Background(), "xdrive timing ", kindNames[k],
			" n=", n,
			" requests=", batch.requests, " units=", batch.requests*driveQuotaCost(k),
			" failed=", batch.failed, " canceled=", batch.canceled, " rate-limits=", batch.rateLimits,
			" total p50/p95/max ", percentile(total, .5), "/", percentile(total, .95), "/", percentile(total, 1),
			" wait-pool p50 ", percentile(pool, .5),
			" wait-cool p50 ", percentile(cool, .5),
			" wait-token p50 ", percentile(token, .5),
			" wait-conn p50 ", percentile(conn, .5),
			" dial+tls p50 ", percentile(dial, .5),
			" server p50 ", percentile(server, .5),
			" body p50 ", percentile(body, .5),
			" newconns=", batch.conns, " hedges=", batch.hedges, " won=", batch.won,
			" hedge-deferrals=", batch.deferred,
			" send-p50/p95/max=", percentile(send, .5), "/", percentile(send, .95), "/", percentile(send, 1),
			" pool-p95/max=", percentile(pool, .95), "/", percentile(pool, 1),
			" connection-p95/max=", percentile(conn, .95), "/", percentile(conn, 1),
			" server-p95/max=", percentile(server, .95), "/", percentile(server, 1),
			" body-p95/max=", percentile(body, .95), "/", percentile(body, 1))
		if k == kindBodyRead {
			errors.LogInfo(context.Background(), "xdrive timing body-read dispatch-to-eof p50/p90/max ",
				percentile(dispatch, .5), "/", percentile(dispatch, .9), "/", percentile(dispatch, 1))
		}
	}
}

func classifyDriveRequest(ctx context.Context, method, target, contentType string, body []byte) int {
	switch method {
	case "DELETE":
		return kindDelete
	case "POST":
		return kindCreate
	case "PATCH":
		if ctx.Value(watermarkWriteKey{}) != nil {
			return kindWatermark
		}
		mediaType, _, err := mime.ParseMediaType(contentType)
		if err == nil && mediaType == "multipart/related" {
			return kindSlotMedia
		}
		if isWatermarkBody(body) {
			return kindWatermark
		}
		return kindSlotInline
	case "GET":
		switch {
		case strings.Contains(target, "alt=media"):
			return kindBodyRead
		case strings.Contains(target, "q="):
			return kindList
		case ctx.Value(laneKey{}) != nil:
			return kindHeadRead
		default:
			return kindOtherRead
		}
	}
	return kindOtherRead
}

func isWatermarkBody(body []byte) bool {
	if len(body) > 128 {
		return false
	}
	var doc struct {
		Description string `json:"description"`
	}
	if json.Unmarshal(body, &doc) != nil || doc.Description == "" {
		return false
	}
	for _, c := range doc.Description {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
