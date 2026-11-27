package xdrive

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/dice"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/transport/internet"
)

const (
	flatSeparator       = "~"
	driveInlineLimit    = 12000
	driveMaxAnnounce    = 1 << 20
	driveMaxInflight    = 64
	driveMaxAttempts    = 8
	driveTimeout        = time.Minute
	driveReconnectGrace = 45 * time.Second
	driveRequestSpacing = time.Second / 80
	driveInitialBackoff = 200 * time.Millisecond
	driveMaxBackoff     = 8 * time.Second
	cleanupMaxRetries   = 5
	cleanupRetryDelay   = 2 * time.Second
	cleanupSessionPause = time.Second
	filesURL            = "https://www.googleapis.com/drive/v3/files"
	uploadURL           = "https://www.googleapis.com/upload/drive/v3/files"
)

type (
	maxBodyKey    struct{}
	headKey       struct{}
	mediaHeadKey  struct{} // <-chan struct{}, closed on promotion
	mediaSeqKey   struct{} // int64, for request retry diagnostics
	mediaReadKey  struct{}
	spanRescueKey struct{} // func() bool for the ordered span, cleared on resume
)

type mediaRead struct {
	size      int
	byteRange string
	progress  func(int) // Timing only: receipt of a tagged block does not release ring credit.
	consume   func(*http.Response) error
}

type driveLane int

const (
	metadataLane driveLane = iota
	priorityLane
	mediaLane
	laneCount
)

type driveStorage struct {
	*driveShared
	clients     [laneCount]*http.Client
	child       bool
	boundFolder string
	unchecked   sync.Map // peer-selected ID -> expected name, until first verified access
}

type driveShared struct {
	folder, space, tokenForm           string
	tokenClient                        *http.Client
	pools                              [laneCount]*connPool
	permits                            [laneCount]chan struct{}
	headMedia                          chan struct{} // reserved HTTP permits, same media pool
	mu                                 sync.Mutex
	oauth                              atomic.Pointer[driveToken]
	coolUntil, nextRequest, nextDelete time.Time
	coolDelay                          time.Duration
	tokenLock, idLock                  chan struct{}
	ids                                []string
	idsExpire                          time.Time
}

func newDriveStorage(settings *internet.MemoryStreamConfig, config *Config) (*driveStorage, error) {
	if config.RemoteFolder == "" || len(config.Secrets) != 3 || slices.Contains(config.Secrets, "") {
		return nil, errors.New("Drive requires a folder ID, client ID, client secret and refresh token")
	}
	hosts := new(sync.Map)
	client := func(ctx context.Context) *http.Client {
		return newServiceClient(ctx, hosts, settings, driveTimeout, driveMaxInflight)
	}
	mediaConns := cmp.Or(max(config.Xmux.GetMaxConnections().GetTo(), 0), 4)
	s := &driveStorage{driveShared: &driveShared{
		folder: config.RemoteFolder, space: "drive",
		tokenForm: url.Values{
			"client_id": {config.Secrets[0]}, "client_secret": {config.Secrets[1]},
			"refresh_token": {config.Secrets[2]}, "grant_type": {"refresh_token"},
		}.Encode(),
		idLock: make(chan struct{}, 1), tokenLock: make(chan struct{}, 1),
		headMedia: make(chan struct{}, min(4, mediaConns)),
	}}
	if s.folder == "appDataFolder" {
		s.space = s.folder
	}
	// Four ordinary media requests per connection hide request latency;
	// the byte budget and separate head permits bound speculative load.
	for i, limit := range [laneCount]int{48, 8, min(driveMaxInflight, 4*int(mediaConns))} {
		s.pools[i] = newConnPool(config.Xmux, [laneCount]int32{0, 0, mediaConns}[i], client)
		s.permits[i] = make(chan struct{}, limit)
	}
	// The token client shares the root pool lifetime, not a session lease.
	s.tokenClient = client(s.pools[metadataLane].ctx)
	s.tokenClient.Timeout = 30 * time.Second
	s.tokenClient.CheckRedirect = driveRedirect
	return s.openClients(), nil
}

// Redirect reads only: never convert a mutation to GET or forward a refresh form.
func driveRedirect(req *http.Request, via []*http.Request) error {
	if via[0].Method != http.MethodGet || req.URL.Scheme != "https" || len(via) >= 10 {
		return http.ErrUseLastResponse
	}
	return nil
}

func (s *driveStorage) openClients() *driveStorage {
	for i, pool := range s.pools {
		s.clients[i] = &http.Client{Transport: &sessionTransport{pool: pool}, CheckRedirect: driveRedirect}
	}
	return s
}

func (s *driveStorage) session() backend {
	return (&driveStorage{driveShared: s.driveShared, child: true}).openClients()
}

func (s *driveStorage) Close() error {
	for i, c := range s.clients {
		c.Transport.(*sessionTransport).Close()
		if !s.child {
			s.pools[i].Close()
		}
	}
	if !s.child {
		s.tokenClient.CloseIdleConnections()
	}
	return nil
}

var errSessionInUse = errors.New("session is live or already being collected")

func flatten(name string) string { return strings.ReplaceAll(name, "/", flatSeparator) }

func (s *driveStorage) getMedia(ctx context.Context, id string, limit int64) ([]byte, error) {
	return s.request(ctx, mediaLane, http.MethodGet, filesURL+"/"+url.PathEscape(id)+"?alt=media&supportsAllDrives=true", "", nil, limit, nil)
}

var driveQueryEscapes = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

type tokenRetry struct{ error }

func (e tokenRetry) Unwrap() error { return e.error }

type authError string

func (e authError) Error() string {
	return "Google rejected the OAuth credentials (" + string(e) + "); create a new refresh token, see the XDRIVE setup guide"
}

func isAuthError(err error) bool {
	_, rejected := err.(authError)
	return rejected
}

// The endpoint response and its cache entry share the validated bearer and lifetime.
type driveToken struct {
	AccessToken string    `json:"access_token"`
	ExpiresIn   int64     `json:"expires_in"`
	ErrorCode   authError `json:"error"`
	err         error
	expires     time.Time
}

func (s *driveStorage) refreshToken(ctx context.Context, token *driveToken) error {
	ctx = context.WithValue(ctx, spanRescueKey{}, (func() bool)(nil)) // A stalled media span cannot shorten an OAuth POST.
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://oauth2.googleapis.com/token", strings.NewReader(s.tokenForm))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.tokenClient.Do(req)
	if err != nil {
		return tokenRetry{errors.New("failed to refresh the access token").Base(err)}
	}
	defer resp.Body.Close()
	status := resp.StatusCode
	if retryableStatus(status) || status == http.StatusRequestTimeout {
		s.delayRequests(resp.Header) // Honor Retry-After even when the error body is truncated.
		return tokenRetry{errors.New("the token endpoint answered ", status)}
	}

	body, err := readLimited(resp.Body, 1<<20)
	if err != nil {
		if err == errDriveSize {
			return err
		}
		return tokenRetry{errors.New("failed to read the token response").Base(err)}
	}
	credentialStatus := status == 400 || status == 401 || status == 403
	switch err := json.Unmarshal(body, token); {
	case credentialStatus && err == nil && slices.Contains([]authError{"invalid_grant", "invalid_client", "unauthorized_client"}, token.ErrorCode):
		return token.ErrorCode
	case status != http.StatusOK:
		return errors.New("the token endpoint answered ", status)
	case err != nil:
		return errors.New("failed to parse the token response").Base(err)
	case token.AccessToken == "":
		return errors.New("the token endpoint returned no access token")
	case token.ExpiresIn <= 0 || token.ExpiresIn > math.MaxInt64/int64(time.Second):
		return errors.New("the token endpoint returned an invalid lifetime")
	}
	life := time.Duration(token.ExpiresIn) * time.Second
	token.expires = time.Now().Add(life - min(time.Minute, life/5))
	return nil
}

func jitter(backoff time.Duration) time.Duration {
	half := backoff / 2 // All callers use positive millisecond-scale delays.
	return half + time.Duration(dice.RollInt63n(int64(half)))
}

func rateLimited(payload []byte) bool {
	type quotaReason struct{ Reason string }
	var parsed struct {
		Error struct {
			Status string
			Errors []quotaReason
		}
	}
	if json.Unmarshal(payload, &parsed) != nil {
		return false
	}
	return parsed.Error.Status == "RESOURCE_EXHAUSTED" || slices.ContainsFunc(parsed.Error.Errors, func(item quotaReason) bool {
		return slices.Contains([]string{"rateLimitExceeded", "userRateLimitExceeded", "sharingRateLimitExceeded"}, item.Reason)
	})
}

func retryableStatus(status int) bool {
	return status == 429 || status == 500 || status == 502 || status == 503 || status == 504
}

// Convert Retry-After to a local delay. A response Date avoids local clock skew.
func retryAfter(h http.Header, now time.Time) time.Duration {
	raw := h.Get("Retry-After")
	if raw != "" && strings.Trim(raw, "0123456789") == "" {
		n, _ := strconv.ParseUint(raw, 10, 64) // overflow saturates before the duration clamp
		return time.Duration(min(n, math.MaxInt64/uint64(time.Second))) * time.Second
	}
	if until, err := http.ParseTime(raw); err == nil {
		if server, err := http.ParseTime(h.Get("Date")); err == nil {
			now = server
		}
		return max(0, until.Sub(now))
	}
	return 0
}

func (s *driveStorage) delayRequests(h http.Header) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !now.Before(s.coolUntil) {
		s.coolDelay = min(30*time.Second, max(2*time.Second, 2*s.coolDelay))
		if now.Sub(s.coolUntil) > time.Minute {
			s.coolDelay = 2 * time.Second
		}
		s.coolUntil = now.Add(jitter(s.coolDelay))
	}
	if until := now.Add(retryAfter(h, now)); until.After(s.coolUntil) {
		s.coolUntil = until
	}
	errors.LogDebug(context.Background(), "xdrive cooldown ", time.Until(s.coolUntil))
}

// File attempts and uncached token refreshes reserve turns on the shared clock.
// A new cooldown can postpone a reservation; deletion spacing is independent.
func (s *driveStorage) pace(ctx context.Context, next *time.Time, interval time.Duration) error {
	for {
		s.mu.Lock()
		now := time.Now()
		delay := max(0, next.Sub(now))
		if next == &s.nextRequest {
			delay = max(delay, s.coolUntil.Sub(now))
		}
		*next = now.Add(delay).Add(interval)
		s.mu.Unlock()
		if err := sleepContext(ctx, delay); err != nil || next != &s.nextRequest || !s.cooling() {
			return err
		}
	}
}

// Write tags directly into the upload buffer. A nil tag leaves an announcement body unchanged.
func multipartBody(metadata, data, tag []byte) ([]byte, string) {
	var body bytes.Buffer
	for {
		boundary := rand.Text()
		body.Reset()
		body.Grow(len(metadata) + len(data) + len(tag)*slotBlocks(len(data)) + 3*len(boundary) + 256)
		fmt.Fprintf(&body, "--%s\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n", boundary)
		body.Write(metadata)
		fmt.Fprintf(&body, "\r\n--%s\r\nContent-Type: application/octet-stream\r\n\r\n", boundary)
		start := body.Len()
		for block := range slices.Chunk(data, slotTagBlock) {
			body.Write(tag)
			body.Write(block)
		}
		if bytes.Contains(metadata, []byte(boundary)) || bytes.Contains(body.Bytes()[start:], []byte(boundary)) {
			continue // A boundary could cross a block tag.
		}
		fmt.Fprintf(&body, "\r\n--%s--\r\n", boundary)
		return body.Bytes(), "multipart/related; boundary=" + boundary
	}
}

// Serialize refreshes with a cancelable permit. A 401 invalidates the cached
// entry independently, without waiting behind a network call.
func (s *driveStorage) accessToken(ctx context.Context) (string, error) {
	if err := sendUntil(ctx, s.tokenLock, struct{}{}, nil, nil); err != nil {
		return "", err
	}
	defer func() { <-s.tokenLock }()
	if cached := s.oauth.Load(); cached != nil && (time.Now().Before(cached.expires) || isAuthError(cached.err)) {
		if _, transient := cached.err.(tokenRetry); !transient {
			return cached.AccessToken, cached.err
		}
		// Wait out the negative cache rather than spending another attempt on the same failure.
		if err := sleepContext(ctx, time.Until(cached.expires)); err != nil {
			return "", err
		}
	}
	if err := s.pace(ctx, &s.nextRequest, driveRequestSpacing); err != nil {
		return "", err
	}
	token := new(driveToken)
	if err := s.refreshToken(ctx, token); err != nil {
		token = &driveToken{err: err, expires: time.Now().Add(time.Second)}
	}
	if token.err == nil || ctx.Err() == nil {
		s.oauth.Store(token)
	}
	return token.AccessToken, token.err
}

func sleepContext(ctx context.Context, d time.Duration) error {
	return waitFor(ctx, time.After(d), nil)
}

func waitFor[T any](ctx context.Context, event <-chan T, wake <-chan struct{}) error {
	select {
	case <-ctx.Done():
	case <-event:
	case <-wake:
	}
	return ctx.Err()
}

func (s *driveStorage) cooling() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Now().Before(s.coolUntil)
}

var (
	errDriveSize  = errors.New("Drive response exceeds size limit")
	errDriveScope = errors.New("Drive file does not belong to the announced session")
	errDriveRange = errors.New("Drive range not yet available")
)

func readLimited(r io.Reader, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if int64(len(b)) > limit {
		return nil, errDriveSize
	}
	return b, err
}

// A listing also needs the service clock at header arrival (before reading its body).
type driveClock struct{ served, observed time.Time }

// Hold the permit through the body read; only an explicit rejection permits write replay.
func (s *driveStorage) request(ctx context.Context, lane driveLane, method, target, contentType string, body []byte, limit int64, clock *driveClock) (data []byte, err error) {
	unauthorized := 0
	upload := method == http.MethodPost || method == http.MethodPatch
	read := method == http.MethodGet
	stream, _ := ctx.Value(mediaReadKey{}).(*mediaRead)
	if lane == metadataLane && ctx.Value(headKey{}) != nil && method != http.MethodDelete {
		lane = priorityLane
	}
	once := func() (bool, error) {
		permits := s.permits[lane]
		head, _ := ctx.Value(mediaHeadKey{}).(<-chan struct{})
		// Promotion skips the ordinary queue, including while waiting for a permit.
		for isClosedChan(head) || sendUntil(ctx, permits, struct{}{}, head, nil) != nil {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			permits, head = s.headMedia, nil
		}
		defer func() { <-permits }()
		token, err := s.accessToken(ctx)
		if err != nil {
			_, retry := err.(tokenRetry) // No file request has been sent yet.
			return retry, err
		}
		if err := s.pace(ctx, &s.nextRequest, driveRequestSpacing); err != nil {
			return false, err
		}
		req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
		if err != nil {
			return false, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		if stream != nil && stream.byteRange != "" {
			req.Header.Set("Range", stream.byteRange)
			req.Header.Set("Accept-Encoding", "identity")
		} else if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		req.GetBody = nil // Reads have no body; never let the transport rewind an upload.
		resp, err := s.clients[lane].Do(req)
		if err != nil {
			cause := errors.Cause(err)
			if cause == errStalled {
				ctx = context.WithValue(ctx, spanRescueKey{}, (func() bool)(nil)) // Ordinary timeout on subsequent HTTP attempts.
			}
			return read && cause != errSessionClosed, err
		}
		defer resp.Body.Close()
		status := resp.StatusCode
		switch success := status >= 200 && status < 300; {
		case success && stream != nil:
			return false, stream.consume(resp)
		case success && read && lane == mediaLane && status != http.StatusOK:
			return false, fmt.Errorf("expected HTTP 200 for Drive media announcement, got %d", status)
		case success:
			if clock != nil {
				clock.observed = time.Now()
				clock.served, _ = http.ParseTime(resp.Header.Get("Date"))
			}
			data, err = readLimited(resp.Body, limit)
			return read && err != errDriveSize, err // Only a buffered GET can discard a partial read.
		case status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable && resp.Header.Get("Retry-After") != "":
			s.delayRequests(resp.Header) // Other lanes must not wait for this error body to finish.
			return status == http.StatusTooManyRequests || read, fmt.Errorf("Drive %s: HTTP %d", method, status)
		}
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		err = fmt.Errorf("Drive %s: HTTP %d: %s", method, status, detail)
		switch {
		case status == http.StatusUnauthorized:
			if cached := s.oauth.Load(); cached != nil && cached.AccessToken == token {
				s.oauth.CompareAndSwap(cached, nil)
			}
			unauthorized++
			return unauthorized == 1, authError("401")
		case status == http.StatusForbidden && rateLimited(detail):
			s.delayRequests(resp.Header)
			return true, fmt.Errorf("Drive %s: quota rate limit (HTTP 403)", method)
		case status == http.StatusRequestedRangeNotSatisfiable:
			return false, errDriveRange
		case upload && (status == http.StatusBadRequest || status == http.StatusRequestEntityTooLarge):
			return false, refusedPayload{err}
		case status == http.StatusNotFound:
			return false, errNotFound
		}
		return read && retryableStatus(status), err
	}
	for attempt := 1; ; attempt++ {
		retry, err := once()
		if err == nil || !retry || attempt == driveMaxAttempts || ctx.Err() != nil {
			return data, err
		}
		errors.LogDebug(ctx, "xdrive request retry seq ", ctx.Value(mediaSeqKey{}), " method ", method, " attempt ", attempt+1, " err ", err)
		if err := sleepContext(ctx, jitter(min(driveMaxBackoff, driveInitialBackoff<<(attempt-1)))); err != nil {
			return data, err
		}
	}
}

type driveFile struct {
	MimeType    string    `json:"mimeType,omitempty"`
	Trashed     bool      `json:"trashed,omitempty"`
	CreatedTime string    `json:"createdTime,omitempty"`
	created     time.Time // estimated local instant from Drive's Date and createdTime
	ID          string    `json:"id,omitempty"`
	Name        string    `json:"name,omitempty"`
	Description string    `json:"description,omitempty"`
	Parents     []string  `json:"parents,omitempty"`
}

func (f driveFile) regular() bool {
	return f.MimeType != "" && !strings.HasPrefix(f.MimeType, "application/vnd.google-apps.")
}

// A name creates the file; subsequent publications patch its reserved ID.
func (s *driveStorage) writeFile(ctx context.Context, id, name, description string, payload, tag []byte) error {
	f := driveFile{Description: description}
	method, path := http.MethodPatch, "/"+url.PathEscape(id)
	if name != "" {
		method, path = http.MethodPost, ""
		f.ID, f.Name = id, flatten(name)
		f.MimeType, f.Parents = "application/octet-stream", []string{s.folder}
	} else if _, pending := s.unchecked.Load(id); pending {
		if _, err := s.readDescription(ctx, id); err != nil {
			return err
		}
	}
	md, _ := json.Marshal(f) // driveFile's wire fields are strings, a bool and a string slice.
	lane := metadataLane
	target, query, typ := filesURL, "?fields=id&supportsAllDrives=true", "application/json"
	if payload != nil {
		lane, target, query = mediaLane, uploadURL, query+"&uploadType=multipart"
		md, typ = multipartBody(md, payload, tag)
	}
	data, err := s.request(ctx, lane, method, target+path+query, typ, md, 4096, nil)
	var reply driveFile
	if err == nil && json.Unmarshal(data, &reply) == nil && reply.ID == id {
		s.unchecked.Delete(id)
		return nil
	}
	err = cmp.Or[error](err, errors.New("unexpected Drive file ID in write response"))
	if _, refused := err.(refusedPayload); refused || isAuthError(err) || ctx.Err() != nil {
		return err
	}
	// One writer per file: confirm the exact frame or ACK without replay.
	verify, cancel := context.WithTimeout(context.WithValue(ctx, headKey{}, true), driveReconnectGrace)
	defer cancel()
	for delay := driveInitialBackoff; verify.Err() == nil; delay = min(2*delay, 2*time.Second) {
		d, readErr := s.readDescription(verify, id)
		matches := readErr == nil && d == description
		if matches && description == "" && payload != nil { // A media announcement needs a full body comparison.
			var media []byte
			media, readErr = s.getMedia(verify, id, int64(len(payload)))
			matches = readErr == nil && bytes.Equal(media, payload)
		}
		if matches {
			return nil
		}
		if readErr == errDriveScope || isAuthError(readErr) || sleepContext(verify, delay) != nil {
			break
		}
	}
	return err
}

type refusedPayload struct{ error }

func (s *driveStorage) getJSON(ctx context.Context, target string, limit int64, v any, clock *driveClock) error {
	data, err := s.request(ctx, metadataLane, http.MethodGet, target, "", nil, limit, clock)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func (s *driveStorage) readDescription(ctx context.Context, id string) (string, error) {
	name, pending := s.unchecked.Load(id)
	fields := "description"
	if pending {
		fields += ",name,parents,trashed,mimeType"
	}
	var f driveFile
	if err := s.getJSON(ctx, filesURL+"/"+url.PathEscape(id)+"?fields="+fields+"&supportsAllDrives=true", 64<<10, &f, nil); err != nil {
		return "", err
	}
	if pending {
		if !f.regular() || f.Trashed || f.Name != name || !slices.Contains(f.Parents, s.boundFolder) {
			return "", errDriveScope
		}
		s.unchecked.Delete(id)
	}
	return f.Description, nil
}

// readSpan verifies tagged blocks [lo, hi), emitting each payload byte once.
// On a flaky connection, allow an interrupted resume to reconnect once without new bytes.
func (s *driveStorage) readSpan(ctx context.Context, id string, f slotFrame, lo, hi int, out chan<- []byte, whole, prefixReady func()) error {
	const stride = slotBodyHeader + slotTagBlock
	total := f.size + slotBodyHeader*slotBlocks(f.size)
	pos := lo * slotTagBlock // Payload offset of the next undelivered chunk.
	for attempt, stalled := 1, false; ; attempt++ {
		before, started := pos, time.Now()
		first := min(pos/slotTagBlock, hi-1)
		start, end := first*stride, min(hi*stride, total)
		read := &mediaRead{size: end - start}
		if read.size != total {
			read.byteRange = fmt.Sprintf("bytes=%d-%d", start, end-1)
		}
		var headers time.Time
		var readErr error // Only interrupted reads qualify for further resumes.
		read.consume = func(resp *http.Response) error {
			headers = time.Now()
			from, to, length := first, hi, end-start
			switch {
			case resp.StatusCode == http.StatusOK:
				from, to, length = 0, slotBlocks(f.size), total
				if lo == 0 && whole != nil {
					hi = to
					whole() // Keep this response; cancel redundant full-body downloads.
				}
			case resp.StatusCode != http.StatusPartialContent:
				return fmt.Errorf("unexpected Drive media status %d", resp.StatusCode)
			case resp.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", start, end-1, total):
				return errors.New("Drive returned a different media range")
			}
			if resp.ContentLength >= 0 && resp.ContentLength != int64(length) {
				return errors.New("unexpected Drive media length")
			}
			var tag [slotBodyHeader]byte
			at := from * slotTagBlock
			for i := from; i < to; i++ {
				_, readErr = io.ReadFull(resp.Body, tag[:])
				if readErr != nil || int64(binary.BigEndian.Uint64(tag[:])) != f.seq {
					return cmp.Or[error](readErr, errDriveRange) // A different tag belongs to an older lap of this slot.
				}
				for end := min(at+slotTagBlock, f.size); at < end; {
					size := min(slotReadChunk, end-at)
					if at == 0 { // Expose headers before a slow link fills the first 32 KiB.
						size = min(size, 4<<10)
					}
					b := make([]byte, size)
					if _, readErr = io.ReadFull(resp.Body, b); readErr != nil {
						return readErr
					}
					if at == pos && i < hi { // Replays start at a tag and use the same chunk boundaries.
						out <- b // Queue fits the entire verified span.
						pos += len(b)
					}
					at += len(b)
				}
				read.progress(slotBodyHeader + min(slotTagBlock, f.size-i*slotTagBlock))
				if prefixReady != nil && i == from {
					prefixReady()
				}
			}
			if _, readErr = io.ReadFull(resp.Body, tag[:1]); readErr != io.EOF {
				return errors.New("Drive media did not end at the declared length").Base(readErr)
			}
			errors.LogDebug(ctx, "xdrive span seq ", f.seq, " blocks ", lo, "-", hi, " status ", resp.StatusCode, " bytes ", length, " headers ", headers.Sub(started), " body ", time.Since(headers))
			return nil
		}
		_, err := s.getMedia(context.WithValue(ctx, mediaReadKey{}, read), id, 0)
		firstRetry := attempt == 1 && (!headers.IsZero() || err == errNotFound || err == errDriveRange)
		progressRetry := attempt < driveMaxAttempts && readErr != nil && (!stalled || pos > before)
		stalled = pos == before
		if err == nil || ctx.Err() != nil || !firstRetry && !progressRetry {
			return err
		}
		errors.LogDebug(ctx, "xdrive span retry seq ", f.seq, " blocks ", lo, "-", hi, " emitted ", pos-lo*slotTagBlock, " elapsed ", time.Since(started), " err ", err)
		ctx = context.WithValue(ctx, spanRescueKey{}, (func() bool)(nil)) // Never shorten the resume.
		if err := sleepContext(ctx, driveInitialBackoff); err != nil {
			return err
		}
	}
}

// Speculative frames start with the first two ranges. The ordered head may use all lanes.
func (s *driveStorage) readFrameStream(ctx context.Context, id string, f slotFrame, out chan<- []byte, verifiedFrame *atomic.Int64) error {
	ctx = context.WithValue(ctx, mediaSeqKey{}, f.seq)
	count := min(cap(s.headMedia), f.size/(2*slotTagBlock))
	if f.size <= slotProgressiveMax || s.cooling() {
		count = 1
	}
	boundary := func(i int) int { return i * slotBlocks(f.size) / count }
	var draining, verified atomic.Int64 // Ordered range and highest range verified through EOF.
	head := ctx.Value(mediaHeadKey{}).(<-chan struct{})
	read := func(ctx context.Context, i int, chunks chan<- []byte, whole, prefixReady func()) error {
		ctx = context.WithValue(ctx, spanRescueKey{}, func() bool {
			later := verifiedFrame.Load() > f.seq || verified.Load() > int64(i)
			return later && isClosedChan(head) && draining.Load() == int64(i) && !s.cooling()
		})
		return s.readSpan(ctx, id, f, boundary(i), boundary(i+1), chunks, whole, prefixReady)
	}
	if count == 1 {
		return read(ctx, 0, out, nil, nil)
	}
	tailCtx, stopTails := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer stopTails()
	firstBlock := make(chan struct{})
	queues := make([]chan []byte, count) // Range zero streams directly to out.
	results := make([]error, count)      // Published by closing the corresponding queue.
	for i := 1; i < count; i++ {
		queues[i] = make(chan []byte, (boundary(i+1)-boundary(i))*(slotTagBlock/slotReadChunk))
		wg.Go(func() {
			if i == 1 || waitFor(tailCtx, firstBlock, head) == nil {
				if results[i] = read(tailCtx, i, queues[i], nil, nil); results[i] == nil {
					advance(&verified, int64(i))
				}
			}
			close(queues[i])
		})
	}
	err := read(ctx, 0, out, stopTails, sync.OnceFunc(func() { close(firstBlock) }))
	for i := 1; i < count && err == nil && tailCtx.Err() == nil; i++ {
		draining.Store(int64(i))
		for b := range queues[i] {
			out <- b
		}
		err = results[i]
	}
	return cmp.Or(err, ctx.Err()) // Full HTTP 200 cancels only the tails.
}

// One ID for the announcement and two tables, each with a watermark.
func (s *driveStorage) takeIDs(ctx context.Context, depth int) ([]string, error) {
	n := 2*(depth+1) + 1 // depth is capped at maxConcurrency (256).
	if err := sendUntil(ctx, s.idLock, struct{}{}, nil, nil); err != nil {
		return nil, err
	}
	defer func() { <-s.idLock }()
	if len(s.ids) < n || time.Now().After(s.idsExpire) {
		count := min(1000, 3*n)
		target := fmt.Sprintf("%s/generateIds?count=%d&space=%s", filesURL, count, s.space)
		var batch struct{ IDs []string }
		if err := s.getJSON(ctx, target, 256<<10, &batch, nil); err != nil {
			return nil, err
		}
		seen := make(map[string]bool, count)
		for _, id := range batch.IDs {
			if slotIDPattern.MatchString(id) {
				seen[id] = true
			}
		}
		if len(batch.IDs) != count || len(seen) != count {
			return nil, errors.New("invalid Drive ID batch")
		}
		s.ids, s.idsExpire = batch.IDs, time.Now().Add(10*time.Minute)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ids := s.ids[:n:n]
	s.ids = s.ids[n:]
	return ids, nil
}

// Drive's name query narrows the search; apply the same flat-path bounds locally
// before exposing a file or handing its ID to Delete.
func (s *driveStorage) scan(ctx context.Context, name string, deleting bool, visit func(driveFile)) error {
	flat := flatten(name)
	prefix := flat + flatSeparator
	children := !strings.HasPrefix(name, sessionsDir+"/") || !deleting
	var names []string
	if deleting {
		names = append(names, "name = '"+driveQueryEscapes.Replace(flat)+"'")
	}
	if children {
		names = append(names, "name contains '"+driveQueryEscapes.Replace(prefix)+"'")
	}
	fields := "id,name,mimeType"
	var clock *driveClock
	if !deleting && name == sessionsDir {
		fields += ",description,createdTime"
		clock = new(driveClock)
	}
	q := url.Values{
		"q":      {"'" + driveQueryEscapes.Replace(s.folder) + "' in parents and trashed = false and mimeType != 'application/vnd.google-apps.folder' and (" + strings.Join(names, " or ") + ")"},
		"fields": {"nextPageToken,incompleteSearch,files(" + fields + ")"}, "pageSize": {"1000"}, "spaces": {s.space},
		"supportsAllDrives": {"true"}, "includeItemsFromAllDrives": {"true"},
	}
	seen := make(map[string]bool)
	for {
		var page struct {
			Files            []driveFile
			NextPageToken    string
			IncompleteSearch bool
		}
		if err := s.getJSON(ctx, filesURL+"?"+q.Encode(), 32<<20, &page, clock); err != nil {
			return err
		}
		if page.IncompleteSearch {
			return errors.New("Drive returned an incomplete file listing")
		}
		for _, f := range page.Files {
			match := deleting && f.Name == flat || children && strings.HasPrefix(f.Name, prefix)
			if !match || !f.regular() {
				continue // Never read or delete native Drive objects through the flat namespace.
			}
			if clock != nil {
				if clock.served.IsZero() {
					return errors.New("missing Drive listing date")
				}
				if created, err := time.Parse(time.RFC3339Nano, f.CreatedTime); err == nil {
					f.created = clock.observed.Add(-max(0, clock.served.Sub(created)))
				}
			}
			visit(f)
		}
		if page.NextPageToken == "" {
			return nil
		}
		if seen[page.NextPageToken] {
			return errors.New("repeated Drive page token")
		}
		seen[page.NextPageToken] = true
		q.Set("pageToken", page.NextPageToken)
	}
}

func (s *driveStorage) List(ctx context.Context, prefix string) ([]Entry, error) {
	flat := flatten(prefix) + flatSeparator
	announcements := prefix == sessionsDir
	entries := make(map[string]Entry)
	err := s.scan(ctx, prefix, false, func(f driveFile) {
		name, _, child := strings.Cut(f.Name[len(flat):], flatSeparator)
		_, seen := entries[name]
		switch {
		case name == "" || announcements && child:
		case announcements && (seen || f.created.IsZero() || f.ID == ""):
			entries[name] = Entry{} // A duplicate or unknown age remains ineligible across pages.
		case announcements:
			entries[name] = Entry{Name: name, created: f.created, driveID: f.ID, driveDescription: f.Description}
		case f.ID != "":
			entries[name] = Entry{Name: name}
		}
	})
	if err != nil {
		return nil, err
	}
	maps.DeleteFunc(entries, func(_ string, e Entry) bool { return e.Name == "" })
	return slices.Collect(maps.Values(entries)), nil
}

// readRing returns nil without an error for malformed announcements. Only a valid
// ring needs the canonical parent ID for its first file identity checks.
func (s *driveStorage) readRing(ctx context.Context, session string, e Entry) (*ring, error) {
	data, err := base64.StdEncoding.DecodeString(e.driveDescription)
	if err != nil {
		return nil, nil
	}
	if e.driveDescription == "" {
		if data, err = s.getMedia(ctx, e.driveID, driveMaxAnnounce); err != nil {
			return nil, err
		}
	}
	r := decodeRing(data, session, &s.unchecked)
	s.boundFolder = s.folder
	if r == nil || s.folder != "root" && s.folder != "appDataFolder" {
		return r, nil
	}
	var folder driveFile
	err = s.getJSON(ctx, filesURL+"/"+s.folder+"?fields=id", 4096, &folder, nil)
	if err != nil || folder.ID == "" {
		return nil, cmp.Or[error](err, errDriveScope)
	}
	s.boundFolder = folder.ID
	return r, nil
}

func (s *driveStorage) Delete(ctx context.Context, name string) error {
	// Finish pagination before deleting: partial results cannot prove uniqueness.
	var ids []string
	if err := s.scan(ctx, name, true, func(f driveFile) { ids = append(ids, f.ID) }); err != nil {
		return err
	}
	if strings.HasPrefix(name, sessionsDir+"/") && len(ids) > 1 {
		return errors.New("multiple Drive files have the requested name")
	}
	for _, id := range slices.DeleteFunc(ids, func(id string) bool { return id == "" }) {
		// Share cleanup pacing across sessions instead of competing with uploads.
		if err := s.pace(ctx, &s.nextDelete, 200*time.Millisecond); err != nil {
			return err
		}
		_, err := s.request(ctx, metadataLane, http.MethodDelete, filesURL+"/"+url.PathEscape(id)+"?supportsAllDrives=true", "", nil, 4096, nil)
		if err != nil && err != errNotFound {
			return err
		}
	}
	return nil
}
