package xdrive

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// idPoolEntry is a pre-generated Drive id with the time it was fetched.
type idPoolEntry struct {
	id string
	at time.Time
}

const idPoolTTL = 10 * time.Minute

// generateIDs asks Drive for n pre-generated ids (n <= 1000).
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
	return parsed.IDs, nil
}

func (s *driveStorage) takeIDs(ctx context.Context, n, ringWorth int) ([]string, error) {
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
					if f.err != nil {
						return nil, f.err
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
		f := &idPoolFlight{done: make(chan struct{})}
		s.poolFlight = f
		s.poolMu.Unlock()

		var ids []string
		var err error
		shortfall := n - take
		if shortfall > 1000 {
			err = errors.New("Drive id shortfall exceeds 1000")
		} else {
			ids, err = s.generateIDs(ctx, shortfall)
			if err == nil && len(ids) != shortfall {
				err = errors.New("Drive returned ", len(ids), " ids, want ", shortfall)
			}
		}
		s.poolMu.Lock()
		if err != nil {
			s.idPool = append(s.idPool, taken...)
			s.prunePoolLocked()
		}
		s.finishPoolFlightLocked(f, err)
		if err == nil {
			s.refillPoolLocked()
		}
		s.poolMu.Unlock()
		if err != nil {
			return nil, err
		}
		return append(out, ids...), nil
	}
}

// Called with poolMu held; registration precedes publishing the goroutine so
// root Close joins it. Maintenance uses the root transport, never a session's.
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
	s.idPool = kept[max(0, len(kept)-3*s.poolRing):]
}

func (s *driveShared) beginRefillLocked(refreshOld bool) (*idPoolFlight, int) {
	if s.poolFlight != nil || time.Now().Before(s.poolRetryAt) {
		return nil, 0
	}
	want := 3*s.poolRing - len(s.idPool)
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
	if err != nil {
		s.coolMu.Lock()
		s.poolRetryAt = s.coolUntil
		s.coolMu.Unlock()
	} else {
		s.poolRetryAt = time.Time{}
	}
	s.poolFlight = nil
	close(f.done)
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

// createError carries a non-200 create status so createSlot can decide whether
// a duplicate-id fallback applies.
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
	target := s.filesURL + "?fields=id"
	contentType := "application/json; charset=UTF-8"
	payload := md
	if body != nil {
		ctx = context.WithValue(ctx, bodyKey{}, true)
		target = s.uploadURL
		contentType = "multipart/related; boundary=" + driveBoundary
		payload = multipartBody(md, body)
	}
	status, reply, err := s.do(ctx, http.MethodPost, target, contentType, payload)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return createError{status: status, payload: reply}
	}
	return nil
}

// createSlot creates a slot with a pre-generated id; on a duplicate-id 4xx it
// falls back to updating the existing object.
func (s *driveStorage) createSlot(ctx context.Context, id, name, description string, body []byte) error {
	flat := flatten(name)
	metadata := map[string]interface{}{
		"id":          id,
		"name":        flat,
		"parents":     []string{s.folder},
		"description": description,
	}
	err := s.createWithID(ctx, metadata, body)
	if err == nil {
		return nil
	}
	ce, ok := err.(createError)
	if ok && ce.status >= 400 && ce.status < 500 && ce.status != http.StatusUnauthorized &&
		ce.status != http.StatusTooManyRequests && !(ce.status == http.StatusForbidden && rateLimited(ce.payload)) {
		if _, derr := s.readDescription(ctx, id); derr == nil {
			s.fallbackLogOnce.Do(func() {
				errors.LogInfo(ctx, "xdrive: create with pre-generated id fell back to update")
			})
			return s.setSlot(ctx, id, description, body)
		}
	}
	return err
}

// pool state lives on driveShared; declared here to keep it beside its users.
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
