package xdrive

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
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
	driveMaxAnnounce    = 1 << 20
	driveMaxInflight    = 64
	driveMaxAttempts    = 8
	driveTimeout        = time.Minute
	driveReconnectGrace = 45 * time.Second
	driveRequestSpacing = time.Second / 80
	driveInitialBackoff = 200 * time.Millisecond
	driveMaxBackoff     = 8 * time.Second
	cleanupRetryDelay   = 2 * time.Second
	cleanupSessionPause = time.Second
	filesURL            = "https://www.googleapis.com/drive/v3/files"
	uploadURL           = "https://www.googleapis.com/upload/drive/v3/files"
)

type (
	maxBodyKey    struct{}
	headKey       struct{}
	mediaHeadKey  struct{} // <-chan struct{}, closed on promotion
	mediaReadKey  struct{}
	spanRescueKey struct{} // func() bool for the ordered span, cleared on resume
)

type mediaRead struct {
	lane, stalled *connLane // This span's last GET and the lane to skip on its next attempt.
	size          int
	byteRange     string
	consume       func(*http.Response) error
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
	clients [laneCount]*http.Client
	child   bool
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
	tokenLock                          chan struct{}
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
		tokenLock: make(chan struct{}, 1),
		headMedia: make(chan struct{}, min(4, mediaConns)),
	}}
	if s.folder == "appDataFolder" {
		s.space = s.folder
	}
	for i, limit := range [laneCount]int{48, 8, min(driveMaxInflight, 4*int(mediaConns))} {
		s.pools[i] = newConnPool(config.Xmux, [laneCount]int32{0, 0, mediaConns}[i], client)
		s.permits[i] = make(chan struct{}, limit)
	}
	s.tokenClient = client(s.pools[metadataLane].ctx) // Root-owned, independent of session leases.
	s.tokenClient.Timeout, s.tokenClient.CheckRedirect = 30*time.Second, driveRedirect
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
	switch {
	case err == errDriveSize:
		return err
	case err != nil:
		return tokenRetry{errors.New("failed to read the token response").Base(err)}
	case status == http.StatusOK:
		if err := json.Unmarshal(body, token); err != nil {
			return errors.New("failed to parse the token response").Base(err)
		}
		if token.AccessToken == "" || token.ExpiresIn <= 0 || token.ExpiresIn > math.MaxInt64/int64(time.Second) {
			return errors.New("the token endpoint returned an invalid access token or lifetime")
		}
		life := time.Duration(token.ExpiresIn) * time.Second
		token.expires = time.Now().Add(life - min(time.Minute, life/5))
		return nil
	case status == 400 || status == 401 || status == 403:
		if json.Unmarshal(body, token) == nil && slices.Contains([]authError{"invalid_grant", "invalid_client", "unauthorized_client"}, token.ErrorCode) {
			return token.ErrorCode
		}
	}
	return errors.New("the token endpoint answered ", status)
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
	quota := func(item quotaReason) bool {
		return slices.Contains([]string{"rateLimitExceeded", "userRateLimitExceeded", "sharingRateLimitExceeded"}, item.Reason)
	}
	return json.Unmarshal(payload, &parsed) == nil && (parsed.Error.Status == "RESOURCE_EXHAUSTED" || slices.ContainsFunc(parsed.Error.Errors, quota))
}

func retryableStatus(status int) bool {
	return slices.Contains([]int{429, 500, 502, 503, 504}, status)
}

func (s *driveStorage) delayRequests(h http.Header) {
	now := time.Now()
	var retry time.Duration
	if raw := h.Get("Retry-After"); raw != "" && strings.Trim(raw, "0123456789") == "" {
		n, _ := strconv.ParseUint(raw, 10, 64) // overflow saturates before the duration clamp
		retry = time.Duration(min(n, math.MaxInt64/uint64(time.Second))) * time.Second
	} else if until, err := http.ParseTime(raw); err == nil {
		server, _ := http.ParseTime(h.Get("Date"))
		retry = until.Sub(cmp.Or(server, now)) // Prefer Drive's clock to avoid local skew.
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delay := s.coolUntil.Sub(now)
	if delay <= 0 {
		if delay < -time.Minute {
			s.coolDelay = 0
		}
		s.coolDelay = min(30*time.Second, max(2*time.Second, 2*s.coolDelay))
		delay = jitter(s.coolDelay)
	}
	s.coolUntil = now.Add(max(delay, retry))
	if s.nextRequest.Before(s.coolUntil) {
		s.nextRequest = s.coolUntil
	}
	errors.LogDebug(context.Background(), "xdrive cooldown ", time.Until(s.coolUntil))
}

// Attempts share a pacing clock and recheck cooldown; deletions have their own clock.
func (s *driveStorage) pace(ctx context.Context, next *time.Time, interval time.Duration) error {
	for {
		s.mu.Lock()
		now := time.Now()
		delay := max(0, next.Sub(now))
		*next = now.Add(delay).Add(interval)
		s.mu.Unlock()
		if err := sleepContext(ctx, delay); err != nil || next != &s.nextRequest || !s.cooling() {
			return err
		}
	}
}

func multipartBody(metadata, data []byte) ([]byte, string) {
	for {
		boundary := rand.Text()
		body := slices.Concat(
			[]byte("--"+boundary+"\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"), metadata,
			[]byte("\r\n--"+boundary+"\r\nContent-Type: application/octet-stream\r\n\r\n"),
			data, []byte("\r\n--"+boundary+"--\r\n"),
		)
		if bytes.Count(body, []byte(boundary)) == 3 { // No collision in metadata, media, or across a tag.
			return body, "multipart/related; boundary=" + boundary
		}
	}
}

// Serialize refreshes; a 401 can invalidate the cache while a refresh is in flight.
func (s *driveStorage) accessToken(ctx context.Context) (string, error) {
	if err := sendUntil(ctx, s.tokenLock, struct{}{}, nil, nil); err != nil {
		return "", err
	}
	defer func() { <-s.tokenLock }()
	if cached := s.oauth.Load(); cached != nil && (isAuthError(cached.err) || time.Now().Before(cached.expires)) {
		if _, transient := cached.err.(tokenRetry); !transient {
			return cached.AccessToken, cached.err
		}
		if err := sleepContext(ctx, time.Until(cached.expires)); err != nil {
			return "", err
		}
	}
	if err := s.pace(ctx, &s.nextRequest, driveRequestSpacing); err != nil {
		return "", err
	}
	var token driveToken
	if err := s.refreshToken(ctx, &token); err != nil {
		token = driveToken{err: err, expires: time.Now().Add(time.Second)}
	}
	if token.err == nil || ctx.Err() == nil {
		s.oauth.Store(&token)
	}
	return token.AccessToken, token.err
}

func sleepContext(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
	return ctx.Err()
}

func (s *driveStorage) cooling() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Now().Before(s.coolUntil)
}

var (
	errSessionInUse = errors.New("session is live or already being collected")
	errDriveSize    = errors.New("Drive response exceeds size limit")
	errDriveScope   = errors.New("Drive file does not belong to the announced session")
	errDriveRange   = errors.New("Drive range not yet available")
	errCredit       = errors.New("invalid receiver watermark")
	driveStatusErrs = map[int]error{http.StatusNotFound: errNotFound, http.StatusRequestedRangeNotSatisfiable: errDriveRange}
)

func readLimited(r io.Reader, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if int64(len(b)) > limit {
		return nil, errDriveSize
	}
	return b, err
}

type driveClock struct{ served, observed time.Time } // Drive and local times at header arrival.

func (s *driveStorage) request(ctx context.Context, lane driveLane, method, target, contentType string, body []byte, limit int64, clock *driveClock) (data []byte, err error) {
	read := method == http.MethodGet
	stream, _ := ctx.Value(mediaReadKey{}).(*mediaRead)
	if lane == metadataLane && ctx.Value(headKey{}) != nil && method != http.MethodDelete {
		lane = priorityLane
	}
	head, _ := ctx.Value(mediaHeadKey{}).(<-chan struct{})
	permits := s.permits[lane]
	unauthorized := 0
	send := func() (bool, error) {
		defer func() { <-permits }()
		token, err := s.accessToken(ctx)
		if err != nil {
			_, retry := err.(tokenRetry) // No file request has been sent yet.
			return retry, err
		}
		req, buildErr := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
		if err := cmp.Or(s.pace(ctx, &s.nextRequest, driveRequestSpacing), buildErr); err != nil {
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
			return read && errors.Cause(err) != errSessionClosed, err
		}
		defer resp.Body.Close()
		status := resp.StatusCode
		switch success := status/100 == 2; {
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
		case status == 429 || status == 503 && resp.Header.Get("Retry-After") != "":
			s.delayRequests(resp.Header) // Other lanes must not wait for this error body to finish.
			return read || status == 429, fmt.Errorf("Drive %s: HTTP %d", method, status)
		}
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		switch {
		case status == http.StatusForbidden && rateLimited(detail):
			s.delayRequests(resp.Header)
			return true, fmt.Errorf("Drive %s: quota rate limit (HTTP 403)", method)
		case status == http.StatusUnauthorized:
			if cached := s.oauth.Load(); cached != nil && cached.AccessToken == token {
				s.oauth.CompareAndSwap(cached, nil)
			}
			unauthorized++
			return unauthorized == 1, authError("401")
		}
		err = fmt.Errorf("Drive %s: HTTP %d: %s", method, status, detail)
		if (method == http.MethodPost || method == http.MethodPatch) && (status == 400 || status == 413) {
			return false, refusedPayload{err}
		}
		return read && retryableStatus(status), cmp.Or(driveStatusErrs[status], err)
	}
	for attempt := 1; ; attempt++ {
		for isClosedChan(head) || sendUntil(ctx, permits, struct{}{}, head, nil) != nil {
			if err := ctx.Err(); err != nil {
				return data, err
			}
			permits, head = s.headMedia, nil // Promotion also moves waiters out of the ordinary queue.
		}
		retry, err := send()
		if stream != nil && errors.Cause(err) == errStalled {
			stream.stalled = stream.lane
			return nil, err // Resume the verified prefix on a different connection.
		}
		if err == nil || !retry || attempt == driveMaxAttempts || ctx.Err() != nil {
			return data, err
		}
		errors.LogDebug(ctx, "xdrive request retry ", method, " attempt ", attempt+1, " err ", err)
		if err := sleepContext(ctx, jitter(min(driveMaxBackoff, driveInitialBackoff<<(attempt-1)))); err != nil {
			return data, err
		}
	}
}

type driveFile struct {
	MimeType    string   `json:"mimeType,omitempty"`
	Trashed     bool     `json:"trashed,omitempty"`
	CreatedTime string   `json:"createdTime,omitempty"`
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	Parents     []string `json:"parents,omitempty"`
}

func (s *driveStorage) writeFile(ctx context.Context, id, name, description string, payload []byte, covered func() bool) error {
	f := driveFile{Description: description}
	method, path := http.MethodPatch, "/"+url.PathEscape(id)
	if name != "" {
		method, path = http.MethodPost, ""
		f.ID, f.Name = id, flatten(name)
		f.MimeType, f.Parents = "application/octet-stream", []string{s.folder}
	}
	md, _ := json.Marshal(f)
	path += "?fields=id&supportsAllDrives=true"
	lane, target, typ := metadataLane, filesURL+path, "application/json"
	if payload != nil {
		target = uploadURL + path + "&uploadType=multipart"
		if len(payload) > slotHeader {
			lane = mediaLane
		} // Tiny clears do not queue behind bulk uploads.
		md, typ = multipartBody(md, payload)
	}
	data, err := s.request(ctx, lane, method, target, typ, md, 4096, nil)
	var reply driveFile
	if err == nil && json.Unmarshal(data, &reply) == nil && reply.ID == id {
		return nil
	}
	err = cmp.Or[error](err, errors.New("unexpected Drive file ID in write response"))
	if _, refused := err.(refusedPayload); refused || isAuthError(err) || ctx.Err() != nil {
		return err
	}
	verify, cancel := context.WithTimeout(ctx, driveReconnectGrace) // GETs only; never replay an ambiguous mutation.
	var watcher sync.WaitGroup
	defer watcher.Wait()
	defer cancel()
	if covered != nil {
		watcher.Go(func() {
			for !covered() && sleepContext(verify, driveInitialBackoff) == nil {
			}
			cancel() // A covering receipt also interrupts a stuck exact readback.
		})
	}
	for delay := driveInitialBackoff; verify.Err() == nil; delay = min(2*delay, 2*time.Second) {
		stored, e := s.readFile(context.WithValue(verify, headKey{}, true), id, "name,description")
		if e == nil && stored.Description == description && (name == "" || stored.Name == f.Name) {
			return nil
		}
		if isAuthError(e) || sleepContext(verify, delay) != nil {
			break
		}
	}
	if covered != nil && covered() {
		return nil
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

func (s *driveStorage) readFile(ctx context.Context, id, fields string) (driveFile, error) {
	var f driveFile
	target := filesURL + "/" + url.PathEscape(id) + "?fields=" + fields + "&supportsAllDrives=true"
	err := s.getJSON(ctx, target, 64<<10, &f, nil)
	return f, err
}

// The listener lists two directories and collects one announcement or stream namespace.
func (s *driveStorage) lookup(ctx context.Context, path string, cleanup bool) (map[string]Entry, error) {
	dir, leaf, hasLeaf := strings.Cut(path, "/")
	validDir := dir == sessionsDir || dir == streamsDir
	if !validDir || hasLeaf != cleanup || cleanup && (leaf == "" || strings.ContainsAny(leaf, "/"+flatSeparator)) {
		return nil, errDriveScope
	}
	flat, quote := flatten(path), driveQueryEscapes.Replace
	prefix := flat + flatSeparator
	query, children := "name = '"+quote(flat)+"'", "name contains '"+quote(prefix)+"'"
	switch {
	case !cleanup:
		query = children
	case dir == streamsDir:
		query += " or " + children
	}
	fields := "id,name,mimeType"
	if path == sessionsDir {
		fields += ",description,createdTime"
	}
	q := url.Values{
		"q":      {"'" + quote(s.folder) + "' in parents and trashed = false and mimeType != 'application/vnd.google-apps.folder' and (" + query + ")"},
		"fields": {"nextPageToken,incompleteSearch,files(" + fields + ")"}, "pageSize": {"1000"}, "spaces": {s.space},
		"supportsAllDrives": {"true"}, "includeItemsFromAllDrives": {"true"},
	}
	entries, seen := make(map[string]Entry), make(map[string]bool)
	for {
		var clock driveClock
		var page struct {
			Files            []driveFile
			NextPageToken    string
			IncompleteSearch bool
		}
		switch err := s.getJSON(ctx, filesURL+"?"+q.Encode(), 32<<20, &page, &clock); {
		case err != nil:
			return nil, err
		case page.IncompleteSearch || seen[page.NextPageToken]:
			return nil, errors.New("incomplete or repeated Drive listing page")
		case path == sessionsDir && len(page.Files) != 0 && clock.served.IsZero():
			return nil, errors.New("missing Drive listing date")
		}
		for _, f := range page.Files {
			rest, under := strings.CutPrefix(f.Name, prefix)
			switch {
			case f.Trashed || f.MimeType == "" || strings.HasPrefix(f.MimeType, "application/vnd.google-apps."):
			case cleanup && (f.Name == flat || dir == streamsDir && under):
				if dir == sessionsDir && len(entries) != 0 {
					return nil, errors.New("multiple Drive files have the requested name")
				}
				entries[f.ID] = Entry{Name: f.Name} // Keep physical IDs, including duplicate stream names.
			case path == streamsDir && under && f.ID != "":
				name, _, _ := strings.Cut(rest, flatSeparator)
				entries[name] = Entry{Name: name}
			case path == sessionsDir && under && !strings.Contains(rest, flatSeparator):
				created, err := time.Parse(time.RFC3339Nano, f.CreatedTime)
				if _, duplicate := entries[rest]; duplicate || err != nil || f.ID == "" {
					entries[rest] = Entry{} // Keep the name ineligible on subsequent pages.
					continue
				}
				created = clock.observed.Add(-max(0, clock.served.Sub(created)))
				entries[rest] = Entry{Name: rest, created: created, driveID: f.ID, driveDescription: f.Description}
			}
		}
		if page.NextPageToken == "" {
			return entries, nil
		}
		seen[page.NextPageToken] = true
		q.Set("pageToken", page.NextPageToken)
	}
}

func (s *driveStorage) List(ctx context.Context, dir string) ([]Entry, error) {
	entries, err := s.lookup(ctx, dir, false)
	maps.DeleteFunc(entries, func(_ string, e Entry) bool { return e.Name == "" })
	return slices.Collect(maps.Values(entries)), err
}

func (s *driveStorage) readRing(ctx context.Context, session string, e Entry) (*ring, error) {
	if e.driveDescription != slotMagic {
		return nil, nil
	}
	data, err := s.getMedia(ctx, e.driveID, driveMaxAnnounce)
	if err != nil {
		return nil, err
	}
	var r ring
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	depth := len(r.Up.Slots)
	if depth < 2 || depth > maxConcurrency || len(r.Down.Slots) != depth || r.Tick < 25 || r.Tick > 1000 {
		return nil, errDriveScope
	}
	r.Up.name, r.Down.name = uplinkPrefix(session), downlinkPrefix(session)
	// Bind every peer-selected ID before allowing either media reads or clears.
	files, err := s.lookup(ctx, sessionPrefix(session), true)
	if err != nil {
		return nil, err
	}
	for _, t := range []slotTable{r.Up, r.Down} {
		for i, id := range t.ids() {
			if files[id].Name != flatten(t.filename(i)) {
				return nil, errDriveScope
			}
		}
	}
	return &r, nil
}

func (s *driveStorage) Delete(ctx context.Context, name string) error {
	ids, err := s.lookup(ctx, name, true)
	if err != nil {
		return err
	}
	delete(ids, "")
	for id := range ids {
		if err := s.pace(ctx, &s.nextDelete, 200*time.Millisecond); err != nil { // Cleanup has its own pacing clock.
			return err
		}
		target := filesURL + "/" + url.PathEscape(id) + "?supportsAllDrives=true"
		if _, err := s.request(ctx, metadataLane, http.MethodDelete, target, "", nil, 4096, nil); err != nil && err != errNotFound {
			return err
		}
	}
	return nil
}
