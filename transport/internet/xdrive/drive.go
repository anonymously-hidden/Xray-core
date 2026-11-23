package xdrive

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
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
	driveInitialBackoff = 200 * time.Millisecond
	driveMaxBackoff     = 8 * time.Second
	cleanupMaxRetries   = 5
	cleanupRetryDelay   = 2 * time.Second
	cleanupSessionPause = time.Second
	filesURL            = "https://www.googleapis.com/drive/v3/files"
	uploadURL           = "https://www.googleapis.com/upload/drive/v3/files"
)

type (
	maxBodyKey   struct{}
	cleanupKey   struct{}
	namesOnlyKey struct{}
)

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
	folder, clientID, clientSecret, refreshToken, tokenURL string
	tokenClient                                            *http.Client
	pools                                                  [laneCount]*connPool
	permits                                                [laneCount]chan struct{}
	cancel                                                 context.CancelFunc
	sessionGuard                                           driveSessionGuard
	mu                                                     sync.Mutex
	token                                                  string
	expiry, coolUntil, tokenRetryAt                        time.Time
	tokenErr                                               error
	refreshing                                             chan struct{}
	nextDelete                                             time.Time
	idLock                                                 chan struct{}
	ids                                                    []string
	idsExpire                                              time.Time
}

func newDriveStorage(settings *internet.MemoryStreamConfig, config *Config) (*driveStorage, error) {
	if config.RemoteFolder == "" || len(config.Secrets) != 3 || config.Secrets[0] == "" || config.Secrets[1] == "" || config.Secrets[2] == "" {
		return nil, errors.New("Drive requires a folder ID, client ID, client secret and refresh token")
	}
	ctx, cancel := context.WithCancel(context.Background())
	hosts := new(sync.Map)
	client := func(ctx context.Context) *http.Client {
		return newServiceClient(ctx, hosts, settings, driveTimeout, driveMaxInflight)
	}
	s := &driveStorage{driveShared: &driveShared{
		folder: config.RemoteFolder, clientID: config.Secrets[0], clientSecret: config.Secrets[1], refreshToken: config.Secrets[2],
		tokenURL: "https://oauth2.googleapis.com/token", tokenClient: client(ctx), cancel: cancel, idLock: make(chan struct{}, 1),
	}}
	for i, limit := range [laneCount]int{48, 8, 24} {
		s.pools[i] = newConnPool(config.Xmux, client)
		s.permits[i] = make(chan struct{}, limit)
	}
	s.openClients()
	return s, nil
}

func (s *driveStorage) openClients() {
	for i, pool := range s.pools {
		s.clients[i] = &http.Client{Transport: &sessionTransport{pool: pool}}
	}
}

func (s *driveStorage) session() Storage {
	c := &driveStorage{driveShared: s.driveShared, child: true}
	c.openClients()
	return c
}

func (s *driveStorage) Close() error {
	for _, c := range s.clients {
		c.Transport.(*sessionTransport).Close()
	}
	if !s.child {
		s.cancel()
		for _, pool := range s.pools {
			pool.Close()
		}
		s.tokenClient.CloseIdleConnections()
	}
	return nil
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

func (s *driveStorage) fetchToken(ctx context.Context) (string, time.Duration, error) {
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

// A canceled caller does not strand other waiters or poison the token cache.
func (s *driveStorage) accessToken(ctx context.Context) (string, error) {
	for {
		s.mu.Lock()
		switch {
		case s.token != "" && time.Now().Before(s.expiry):
			t := s.token
			s.mu.Unlock()
			return t, nil
		case s.tokenErr != nil && (isAuthError(s.tokenErr) || time.Now().Before(s.tokenRetryAt)):
			err := s.tokenErr
			s.mu.Unlock()
			return "", err
		case s.refreshing != nil:
			done := s.refreshing
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-done:
				continue
			}
		}
		s.refreshing = make(chan struct{})
		s.mu.Unlock()
		fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		token, life, err := s.fetchToken(fetchCtx)
		cancel()
		s.mu.Lock()
		if err == nil {
			s.token, s.expiry = token, time.Now().Add(life-min(time.Minute, life/5))
			s.tokenErr = nil
		} else if ctx.Err() == nil {
			s.tokenErr, s.tokenRetryAt = err, time.Now().Add(time.Second)
		}
		close(s.refreshing)
		s.refreshing = nil
		s.mu.Unlock()
		return token, err
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (s *driveStorage) cooling() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Now().Before(s.coolUntil)
}

func readLimited(r io.Reader, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if int64(len(b)) > limit {
		return nil, errors.New("Drive response exceeds size limit")
	}
	return b, err
}

// request owns the response through consume, including the HTTP permit. Only
// an explicit rejection permits replaying a mutation: a lost reply is ambiguous.
func (s *driveStorage) request(ctx context.Context, lane driveLane, method, target, contentType string, body []byte, byteRange string, consume func(*http.Response) error) error {
	var last error
	refreshed := false
	for attempt := 0; attempt < driveMaxAttempts; attempt++ {
		if attempt > 0 {
			if err := sleepContext(ctx, jitter(min(driveMaxBackoff, driveInitialBackoff<<uint(attempt-1)))); err != nil {
				return err
			}
		}
		select {
		case s.permits[lane] <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		retry, err := func() (bool, error) {
			defer func() { <-s.permits[lane] }()
			for {
				s.mu.Lock()
				delay := time.Until(s.coolUntil)
				s.mu.Unlock()
				if delay <= 0 {
					break
				}
				if err := sleepContext(ctx, delay); err != nil {
					return false, err
				}
			}
			token, err := s.accessToken(ctx)
			if err != nil {
				return false, err
			}
			req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
			if err != nil {
				return false, err
			}
			req.Header.Set("Authorization", "Bearer "+token)
			if byteRange != "" {
				req.Header.Set("Range", byteRange)
				req.Header.Set("Accept-Encoding", "identity")
			}
			if contentType != "" {
				req.Header.Set("Content-Type", contentType)
			}
			if method == http.MethodPost || method == http.MethodPatch {
				req.GetBody = nil
			}
			resp, err := s.clients[lane].Do(req)
			if err != nil {
				return method == http.MethodGet, err
			}
			defer resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return false, consume(resp)
			}
			detail, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
			err = fmt.Errorf("Drive %s: HTTP %d: %s", method, resp.StatusCode, detail)
			switch {
			case resp.StatusCode == http.StatusUnauthorized:
				s.mu.Lock()
				if s.token == token {
					s.token = ""
				}
				s.mu.Unlock()
				retry := !refreshed
				refreshed = true
				return retry, authError{"401"}
			case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusForbidden && rateLimited(detail):
				s.mu.Lock()
				until := time.Now().Add(jitter(2 * time.Second))
				if until.After(s.coolUntil) {
					s.coolUntil = until
				}
				s.mu.Unlock()
				return true, err
			case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable:
				return false, errDriveRange
			case resp.StatusCode == http.StatusConflict:
				return false, errDriveConflict
			case resp.StatusCode == http.StatusNotFound:
				return false, errNotFound
			default:
				return method == http.MethodGet && retryableStatus(resp.StatusCode), err
			}
		}()
		if !retry || err == nil || ctx.Err() != nil {
			return err
		}
		last = err
	}
	return last
}

func (s *driveStorage) bytes(ctx context.Context, lane driveLane, method, target, contentType string, payload []byte, limit int64) ([]byte, error) {
	var data []byte
	err := s.request(ctx, lane, method, target, contentType, payload, "", func(resp *http.Response) error {
		var err error
		data, err = readLimited(resp.Body, limit)
		return err
	})
	return data, err
}

type driveFile struct {
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	Parents     []string `json:"parents,omitempty"`
}

func (s *driveStorage) writeFile(ctx context.Context, lane driveLane, method, id string, f driveFile, payload []byte) error {
	md, err := json.Marshal(f)
	if err != nil {
		return err
	}
	target, query, typ := filesURL, "?fields=id&supportsAllDrives=true", "application/json"
	if payload != nil {
		target, query = uploadURL, query+"&uploadType=multipart"
		md, typ = multipartBody(md, payload)
	}
	if id != "" {
		target += "/" + url.PathEscape(id)
	}
	data, err := s.bytes(ctx, lane, method, target+query, typ, md, 4096)
	if err != nil {
		return err
	}
	var reply driveFile
	if json.Unmarshal(data, &reply) != nil || reply.ID == "" || (f.ID != "" && reply.ID != f.ID) || (id != "" && reply.ID != id) {
		return errors.New("unexpected Drive file ID in write response")
	}
	return nil
}

var (
	errDriveRange    = errors.New("Drive range not yet available")
	errDriveConflict = errors.New("Drive file ID already exists")
)

func (s *driveStorage) createSlot(ctx context.Context, id, name, description string, body []byte) error {
	f := driveFile{ID: id, Name: flatten(name), Parents: []string{s.folder}, Description: description}
	err := s.writeFile(ctx, writeLane(ctx, body), http.MethodPost, "", f, body)
	if err == errDriveConflict {
		// Reconcile only the exact generation, never overwrite a conflicting ID.
		if existing, readErr := s.readDescription(ctx, id); readErr == nil && existing == description {
			return nil
		}
	}
	return err
}

type headKey struct{}

func writeLane(ctx context.Context, body []byte) driveLane {
	if body != nil {
		return mediaLane
	}
	if ctx.Value(headKey{}) != nil {
		return priorityLane
	}
	return metadataLane
}

func (s *driveStorage) setSlot(ctx context.Context, id, description string, body []byte) error {
	return s.writeFile(ctx, writeLane(ctx, body), http.MethodPatch, id, driveFile{Description: description}, body)
}

func (s *driveStorage) readDescription(ctx context.Context, id string) (string, error) {
	data, err := s.bytes(ctx, writeLane(ctx, nil), http.MethodGet, filesURL+"/"+url.PathEscape(id)+"?fields=description&supportsAllDrives=true", "", nil, 64<<10)
	if err != nil {
		return "", err
	}
	var f driveFile
	err = json.Unmarshal(data, &f)
	return f.Description, err
}

// A range owns [lo, hi) block indices. On retry next never moves back: even
// a server that ignores Range cannot replay a block. After the last block a
// retry still has to verify a nonempty suffix and its exact EOF.
func (s *driveStorage) readSpan(ctx context.Context, id string, f slotFrame, lo, hi int, emit func([]byte) error) error {
	next := lo
	for attempt := 0; ; attempt++ {
		first := min(next, hi-1)
		start, end := first*slotStreamBlockBytes, min(hi*slotStreamBlockBytes, f.size)
		var header string
		if start > 0 || end < f.size {
			header = fmt.Sprintf("bytes=%d-%d", start, end-1)
		}
		bodyFailed := false
		err := s.request(ctx, mediaLane, http.MethodGet, filesURL+"/"+url.PathEscape(id)+"?alt=media&supportsAllDrives=true", "", nil, header, func(resp *http.Response) error {
			bodyFailed = true
			from, to, length := first, hi, end-start
			switch resp.StatusCode {
			case http.StatusOK:
				from, to, length = 0, len(f.blocks), f.size
			case http.StatusPartialContent:
				if resp.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", start, end-1, f.size) {
					return errors.New("Drive returned a different media range")
				}
			default:
				return fmt.Errorf("unexpected Drive media status %d", resp.StatusCode)
			}
			if resp.ContentLength >= 0 && resp.ContentLength != int64(length) {
				return errors.New("unexpected Drive media length")
			}
			for i := from; i < to; i++ {
				b := make([]byte, streamBlockSize(f.size, i))
				if _, err := io.ReadFull(resp.Body, b); err != nil {
					return err
				}
				if !validStreamBlock(f, i, b) {
					return errors.New("Drive media block checksum mismatch")
				}
				if i == next && i < hi {
					if err := emit(b); err != nil {
						bodyFailed = false
						return err
					}
					next++
				}
			}
			var extra [1]byte
			if n, err := io.ReadFull(resp.Body, extra[:]); n != 0 || err != io.EOF {
				return errors.New("Drive media did not end at the declared length").Base(err)
			}
			return nil
		})
		if err == nil || attempt != 0 || (!bodyFailed && err != errNotFound && err != errDriveRange) || ctx.Err() != nil {
			return err
		}
		if err := sleepContext(ctx, driveInitialBackoff); err != nil {
			return err
		}
	}
}

func (s *driveStorage) readFrameStream(ctx context.Context, id string, f slotFrame, emit func([]byte) error) error {
	n := len(f.blocks)
	if n == 0 || f.mode != slotStream {
		return errors.New("invalid Drive stream frame")
	}
	if f.size+len(f.prefix) <= slotProgressiveMax || s.cooling() {
		return s.readSpan(ctx, id, f, 0, n, emit)
	}
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()
	type part struct {
		blocks chan []byte
		err    error // published by close(blocks)
	}
	parts := make([]part, min(4, n))
	for i := range parts {
		lo, hi := i*n/len(parts), (i+1)*n/len(parts)
		p := &parts[i]
		p.blocks = make(chan []byte, hi-lo)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(p.blocks)
			p.err = s.readSpan(ctx, id, f, lo, hi, func(b []byte) error {
				p.blocks <- b // capacity is the range size, each block is emitted once
				return nil
			})
		}()
	}
	for i := range parts {
		p := &parts[i]
		for b := range p.blocks {
			if err := emit(b); err != nil {
				return err
			}
		}
		if p.err != nil {
			return p.err
		}
	}
	return nil
}

func (s *driveStorage) takeIDs(ctx context.Context, n int) ([]string, error) {
	select {
	case s.idLock <- struct{}{}:
		defer func() { <-s.idLock }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if len(s.ids) < n || time.Now().After(s.idsExpire) {
		count := min(1000, 3*n)
		target := fmt.Sprintf("%s/generateIds?count=%d&space=drive", filesURL, count)
		data, err := s.bytes(ctx, metadataLane, http.MethodGet, target, "", nil, 256<<10)
		if err != nil {
			return nil, err
		}
		var batch struct {
			IDs []string `json:"ids"`
		}
		if json.Unmarshal(data, &batch) != nil || len(batch.IDs) != count {
			return nil, errors.New("invalid Drive ID batch")
		}
		seen := make(map[string]bool, count)
		for _, id := range batch.IDs {
			if id == "" || len(id) > maxSlotIDLen || seen[id] {
				return nil, errors.New("invalid or repeated Drive file ID")
			}
			seen[id] = true
		}
		s.ids, s.idsExpire = batch.IDs, time.Now().Add(10*time.Minute)
	}
	ids := s.ids[:n:n]
	s.ids = s.ids[n:]
	return ids, nil
}

func (s *driveStorage) Put(ctx context.Context, name string, payload []byte) error {
	f := driveFile{Name: flatten(name), Parents: []string{s.folder}}
	if len(payload) <= driveInlineLimit {
		f.Description = base64.StdEncoding.EncodeToString(payload)
		payload = nil
	}
	return s.writeFile(ctx, writeLane(ctx, payload), http.MethodPost, "", f, payload)
}

func (s *driveStorage) find(ctx context.Context, query string, describe bool) ([]driveFile, error) {
	fields := "id,name"
	if describe {
		fields += ",description"
	}
	q := url.Values{
		"q":      {"'" + quoteDriveValue(s.folder) + "' in parents and trashed = false and (" + query + ")"},
		"fields": {"nextPageToken,files(" + fields + ")"}, "pageSize": {"1000"},
		"supportsAllDrives": {"true"}, "includeItemsFromAllDrives": {"true"},
	}
	var found []driveFile
	seen := make(map[string]bool)
	for {
		data, err := s.bytes(ctx, metadataLane, http.MethodGet, filesURL+"?"+q.Encode(), "", nil, 32<<20)
		if err != nil {
			return nil, err
		}
		var page struct {
			Files         []driveFile
			NextPageToken string
		}
		if err := json.Unmarshal(data, &page); err != nil {
			return nil, err
		}
		found = append(found, page.Files...)
		if page.NextPageToken == "" {
			return found, nil
		}
		if seen[page.NextPageToken] {
			return nil, errors.New("repeated Drive page token")
		}
		seen[page.NextPageToken] = true
		q.Set("pageToken", page.NextPageToken)
	}
}

func (s *driveStorage) List(ctx context.Context, prefix string) ([]Entry, error) {
	flat := flatten(prefix) + flatSeparator
	files, err := s.find(ctx, "name contains '"+quoteDriveValue(flat)+"'", ctx.Value(namesOnlyKey{}) == nil)
	if err != nil {
		return nil, err
	}
	entries := make(map[string]Entry)
	for _, f := range files {
		suffix, ok := strings.CutPrefix(f.Name, flat)
		if !ok || suffix == "" || f.ID == "" {
			continue
		}
		name, _, child := strings.Cut(suffix, flatSeparator)
		e := entries[name]
		e.Name = name
		if !child && f.Description != "" {
			e.Inline, err = base64.StdEncoding.DecodeString(f.Description)
			if err != nil {
				continue
			}
		}
		entries[name] = e
	}
	out := make([]Entry, 0, len(entries))
	for _, e := range entries {
		out = append(out, e)
	}
	return out, nil
}

func (s *driveStorage) Get(ctx context.Context, name string) ([]byte, error) {
	flat := flatten(name)
	files, err := s.find(ctx, "name = '"+quoteDriveValue(flat)+"'", true)
	if err != nil {
		return nil, err
	}
	limit, _ := ctx.Value(maxBodyKey{}).(int64)
	if limit <= 0 {
		limit = maxSegmentBytes
	}
	for _, f := range files {
		if f.Name != flat || f.ID == "" {
			continue
		}
		if f.Description != "" {
			b, err := base64.StdEncoding.DecodeString(f.Description)
			if int64(len(b)) > limit {
				return nil, errors.New("oversized Drive inline object")
			}
			return b, err
		}
		b, err := s.bytes(ctx, mediaLane, http.MethodGet, filesURL+"/"+url.PathEscape(f.ID)+"?alt=media&supportsAllDrives=true", "", nil, limit)
		if err != errNotFound {
			return b, err
		}
	}
	return nil, errNotFound
}

func (s *driveStorage) Delete(ctx context.Context, name string) error {
	release, err := s.guardSessionCleanup(ctx, name)
	if err != nil {
		return err
	}
	defer release()
	flat := flatten(name)
	prefix := flat + flatSeparator
	files, err := s.find(ctx, "name = '"+quoteDriveValue(flat)+"' or name contains '"+quoteDriveValue(prefix)+"'", false)
	if err != nil {
		return err
	}
	for _, f := range files {
		if f.ID == "" || (f.Name != flat && !strings.HasPrefix(f.Name, prefix)) {
			continue
		}
		// Share cleanup pacing across sessions instead of competing with uploads.
		s.mu.Lock()
		when := s.nextDelete
		if now := time.Now(); now.After(when) {
			when = now
		}
		s.nextDelete = when.Add(200 * time.Millisecond)
		s.mu.Unlock()
		if err := sleepContext(ctx, time.Until(when)); err != nil {
			return err
		}
		_, err := s.bytes(ctx, metadataLane, http.MethodDelete, filesURL+"/"+url.PathEscape(f.ID)+"?supportsAllDrives=true", "", nil, 4096)
		if err != nil && err != errNotFound {
			return err
		}
	}
	return nil
}
