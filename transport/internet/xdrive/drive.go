package xdrive

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
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

	drivePageSize         = 1000
	driveMaxAttempts      = 8
	driveMaxInflight      = 64
	driveReservedInflight = 8
	driveInlineLimit      = 12000
	// A quota window must be quiet before spending requests on description
	// recovery. The ordinary request retry/cooldown policy remains unchanged.
	driveDescriptionHedgeQuiet = time.Minute
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
	// rate-limit answers do not use up attempts while a request is younger than
	// this: the limit is per minute and a cooldown is only 1-2 s, so eight
	// attempts could end inside a single window and fail a slot write.
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

// bodyProgressKey carries the callback for a read's hedge timer.
type bodyProgressKey struct{}

type attemptKey struct{}

// Read hedges recover only a stalled first dispatch. Token,
// connection and permit waits, and the primary's retries, are not hedgeable.
type (
	readProgressKey     struct{}
	descriptionHedgeKey struct{}
	readHedgeGateKey    struct{}
)

type readHedgeGate struct {
	primary   *driveReadProgress
	delay     time.Duration
	remaining func() (time.Duration, bool) // optional tracked-stream eligibility/clock
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
	p.pending = false // transport recovery is already running; do not also hedge it
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

// A verified-block wait may recover a trickling body, but never count time spent
// locally queued before this dispatch. Headers/body bytes do not reset dispatchAt.
func (p *driveReadProgress) remainingForBlock(delay time.Duration, waitAt time.Time) (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	remaining := delay - now.Sub(p.progressAt)
	if !waitAt.IsZero() && !p.dispatchAt.IsZero() {
		remaining = min(remaining, delay-now.Sub(maxTime(waitAt, p.dispatchAt)))
	}
	return remaining, p.pending
}

var errReadHedgeSkipped = errors.New("Drive read hedge is no longer admitted")

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
	streamCaps       atomic.Pointer[driveStreamCaps]
	streamHeadRanges int // immutable owner snapshot; zero means the unchanged default
	// a session has a client and inflight of its own, see session
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
	// readHedge/budget are copied per instance so tests never mutate globals
	// that other goroutines read.
	readHedge       time.Duration
	readHedgeMu     sync.Mutex
	readHedgeTokens int
	readHedgeCount  int
	readHedgeActive atomic.Bool
	// Media range hedges have a budget of their own: description hedges win
	// far less often and must not leave the head frame's ranges without one.
	mediaHedgeTokens  int
	mediaHedgeCount   int
	mediaHedgeChanged chan struct{} // replenishment broadcast, guarded by readHedgeMu
	// mediaNext rotates frames over mediaClients so a stalled connection does
	// not carry every single-range frame.
	mediaNext atomic.Uint32
}

// driveShared is what the sessions of a storage have in common
type driveShared struct {
	conns *connPool
	// Heads and watermarks have a connection as well as reserved permits of
	// their own, so speculative descriptions and uploads cannot queue them.
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
	cleanupInflight   chan struct{}

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

// session returns a storage for one session, over the connections of XMUX
// until Close, with the token and ids of s
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
		// A rejected refresh grant must not discard a token that Drive still
		// accepts: serve it until it expires, then report the grant error.
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
		// A reserved lane never waits, and a wait that cannot finish inside
		// this request's own budget would only burn that budget and then hide
		// the token error: report the error now.
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

// driveQuotaRetry tells the request loop whether a rate-limited attempt may be
// refunded (the budget is not spent yet) or whether the budget is spent and
// the request must stop. The budget is the limit: a request that is answered
// with a rate limit only after the budget is gone must not spend another
// attempt on an account that is already refusing it.
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
	readHedgeAttempt := descriptionHedge || method == http.MethodGet && media != nil && media.rescue
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
		// Recovery must not start another retry/refund loop beside the primary;
		// an idle socket probe also has no payload worth retrying for.
		maxAttempts = 1
	}
	backoff := s.backoff
	var lastErr error
	// the connection of the last failed attempt: the retry goes over another one
	var failedConn *poolConn
	kind := -1
	if s.timing != nil {
		// the same for every attempt, and it may decode the body
		kind = classifyDriveRequest(ctx, method, target, contentType, body)
	}

	firstTry := time.Now()
	refunded := 0
	rejected := 0
	for attempt := 0; attempt-refunded < maxAttempts; attempt++ {
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
			// A stalled priority window must not queue its own recovery.
			pool = s.readRescue
		}
		if media != nil && !media.rescue && s.mediaRanges != nil {
			// Ranges of a long download must not queue slot writes or tail
			// reads behind them in inflight.
			pool = s.mediaRanges
		}
		if media != nil && media.rescue && s.mediaRescue != nil {
			// A stalled bulk window must not queue its own recovery, nor take
			// the permits reserved for head descriptions and acknowledgements.
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
			admitted                                           time.Time
			wroteOK                                            bool
			reused, gotConn                                    bool
			dispatchWait                                       time.Duration
		}
		var dispatched, admissionFailed atomic.Bool
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
						// A transport replay is recovery already, not a stalled
						// first dispatch that should also acquire a hedge.
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
		if _, spread := req.Context().Value(avoidConnKey{}).(*connSpread); !spread && failedConn != nil {
			req = req.WithContext(context.WithValue(req.Context(), avoidConnKey{}, &connSpread{used: []*poolConn{failedConn}}))
		}
		began := time.Now()
		dispatchClient := *httpc
		dispatchClient.Transport = dispatchTransport{next: httpc.Transport}
		resp, err := dispatchClient.Do(req)
		respAt := time.Now()
		noteTimeout := func() {
			if ctx.Err() == nil && dispatched.Load() && !admissionFailed.Load() && (attemptCtx.Err() == context.DeadlineExceeded || errors.Cause(err) == errStalled) {
				// A connection that served nothing since the request began is
				// dead, not Drive throttling: replace it without shrinking the
				// write window or cooling the readers.
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
		if media != nil && media.stream != nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent) {
			payload, err = media.stream.read(*media, resp.StatusCode, resp.Header.Get("Content-Range"), resp.ContentLength, io.LimitReader(resp.Body, limit+1))
		} else {
			payload, err = readResponse(resp.Body, limit, resp.ContentLength)
		}
		if err != nil {
			noteTimeout()
			if dispatched.Load() && s.timing != nil {
				s.timing.noteFailure(kind, ctx.Err() != nil)
			}
		}
		resp.Body.Close()
		cancel()
		// Keep the permits until EOF/Close, including media response bodies.
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
			total:     bodyEnd.Sub(attemptStart),
			waitCool:  waitCoolDur + tr.dispatchWait,
			waitPool:  waitPoolDur,
			waitToken: waitTokenDur,
			waitConn:  max(0, tr.connAt.Sub(began)-tr.dispatchWait),
			dialTLS:   dialEnd(tr.tlsDone, tr.connDone).Sub(tr.connStart),
			send:      max(0, tr.wrote.Sub(tr.admitted)),
			server:    tr.first.Sub(tr.wrote),
			body:      bodyEnd.Sub(maxTime(tr.first, respAt)),
			reused:    tr.reused,
			newConn:   tr.gotConn && !tr.reused,
			hasConn:   tr.gotConn,
			hasDial:   !tr.connStart.IsZero(),
			hasSend:   tr.wroteOK && !tr.admitted.IsZero() && !tr.wrote.IsZero() && !tr.wrote.Before(tr.admitted),
			hasServer: !tr.wrote.IsZero() && !tr.first.IsZero(),
		}
		tr.mu.Unlock()
		s.recordTiming(kind, sample)
		s.logSlowRequest(ctx, kind, req.URL.Path, sample, media, headAtDispatch.Load(), headAtCompletion, resp.StatusCode, attempt+1)

		if resp.StatusCode == http.StatusUnauthorized {
			s.invalidateToken(token)
			lastErr = errors.New("Drive rejected the access token")
			if singleAttempt {
				return 0, nil, lastErr
			}
			// One retry with a freshly fetched token can ride out a token that
			// expired early. A second rejection means the answer will not
			// change: stop instead of fetching a token for every attempt.
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
				// the budget is spent: give up now, not after eight more attempts
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
		}
		return resp.StatusCode, payload, err
	}

	return 0, nil, lastErr
}

// readResponse reads at most limit+1 bytes. When the server announced a length
// that fits the cap, the buffer is allocated once; io.ReadAll would regrow and
// copy a 512 KiB media range about twenty times. The extra bytes.MinRead keeps
// bytes.Buffer.ReadFrom from growing the buffer again just to see EOF.
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

// waitCool re-reads the deadline after every sleep: a cooldown extended while a
// request was waiting must not release it into the limit again.
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
	// a later, shorter draw must not end a cooldown that is still running
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

// rememberID merges confirmed identities; a partial or lagging listing must
// not erase an object that a create or an earlier page already confirmed.
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
	// Re-listing a name whose single id is already the cached primary changes
	// nothing; skip the per-name map and slice rebuilds on every poll.
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

// cachedFileLocked snapshots one name with idMu held, excluding missing ids.
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
	extra       []string // ids of other objects under the same name
}

func (s *driveStorage) query(ctx context.Context, condition string) (map[string]driveFile, error) {
	found := make(map[string]driveFile)
	seen := make(map[string]map[string]struct{})
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
	}
}

// resolveIDs is every id under the exact name
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

// multipartBody chooses a fresh boundary absent from both parts, including
// arbitrary binary payloads, and returns its matching Content-Type.
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

// waitCleanupSlot is a token bucket capping cleanup deletes at cleanupRate per
// second per storage. It holds at most one token, so deletes are spaced rather
// than bursty, and it honours ctx cancellation while waiting.
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

// deleteChild removes one child of a deleted prefix. A rate-limited attempt is
// retried after a jittered delay, up to cleanupMaxRetries tries in total, then
// reported as an ordinary error. Non-rate-limit errors are returned at once.
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

// noteDescriptionRead accrues one hedge token per twenty completed priority
// description reads, capped at six. Speculative tails cannot fund or spend the
// head's recovery budget, and cancelled/failed reads do not refill it.
func (s *driveStorage) noteDescriptionRead() {
	s.readHedgeMu.Lock()
	defer s.readHedgeMu.Unlock()
	accrueHedgeToken(&s.readHedgeCount, &s.readHedgeTokens)
}

// takeHedgeToken spends one token when the hedge timer fires.
func (s *driveStorage) takeHedgeToken() bool {
	s.readHedgeMu.Lock()
	defer s.readHedgeMu.Unlock()
	return spendHedgeToken(&s.readHedgeTokens)
}

// noteMediaRange and takeMediaHedgeToken keep the same budget rule for media
// ranges: one token per twenty completed ranges, capped at six.
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

// Only a tracked, currently required head range waits for replenishment. Taking
// or subscribing under the same lock avoids missing a completion's broadcast.
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

// Recheck recovery admission at the dispatch boundary as well as when its
// timer fires. Ordinary reads keep their existing cooldown/retry behavior.
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
			// Recheck queued duplicates against their actual recovery target.
			// Tracked streams can remain blocked despite unverified wire bytes;
			// every gate still excludes primary retry/transport recovery.
			return false
		}
	}
	if ctx.Value(descriptionHedgeKey{}) != nil {
		return s.descriptionHedgeAllowed()
	}
	if media, ok := ctx.Value(driveMediaKey{}).(*driveMediaRequest); ok && media.rescue {
		if promotion, _ := ctx.Value(mediaPromotionKey{}).(<-chan struct{}); promotion != nil && !mediaHead(ctx) {
			return false
		}
		s.coolMu.Lock()
		defer s.coolMu.Unlock()
		now := time.Now()
		return !now.Before(s.coolUntil) && (s.lastRateLimit.IsZero() || now.Sub(s.lastRateLimit) > 5*time.Second)
	}
	return true
}

// readDescHedged recovers only head/watermark reads, with at most one concurrent
// hedge per session and one dispatch per hedge. Its timer starts at dispatch and
// follows response progress, never token/permit waits or retry backoff. A spent
// timer does not wait for future budget tokens or the end of a quota window.
// The first 200/404 still wins; the slot reader validates sequence/checksum, so
// mutable descriptions are neither cached nor coalesced here.
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
		// No repeated timer/budget chasing beside a queued or retrying primary.
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
		// Fallback for storages without a separate priority connection.
		hedgeCtx = context.WithValue(hedgeCtx, bodyKey{}, true)
	}
	ctx2, cancel2 := context.WithCancel(hedgeCtx)
	defer cancel2()
	go func() {
		defer s.readHedgeActive.Store(false)
		st, pl, e := s.do(ctx2, http.MethodGet, target, "", nil)
		ch <- res{st, pl, e, true}
	}()
	var last res
	for i := 0; i < 2; i++ {
		var r res
		select {
		case r = <-ch:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
		if r.err == nil && (r.status == http.StatusOK || r.status == http.StatusNotFound) {
			if r.hedge && s.timing != nil {
				s.timing.noteHedgeWin(kindHeadRead)
			}
			last = r
			break
		}
		last = r
	}
	return last.status, last.payload, last.err
}

// readDescription implements slotStore
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
