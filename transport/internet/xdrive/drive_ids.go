package xdrive

import (
	"context"
	"encoding/json"
	stderrors "errors"
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

// idPoolCap is how many ids the pool keeps: three rings at least, and one full
// generateIds call so that a burst of new sessions (a page opening many
// connections) is served from the pool instead of queueing behind one
// allocation after another. Unused ids cost nothing and expire with idPoolTTL.
func (s *driveShared) idPoolCap() int {
	return max(3*s.poolRing, 1000)
}

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
					// A cold allocation belongs to its caller. Its cancellation
					// must not cancel other sessions waiting for IDs.
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

// flightFailure reports the error a waiter must see for a finished flight. A
// cancelled flight, or the death of the session that owned it, says nothing
// about the pool: the waiter has to look at the pool and, when it is still
// short, allocate for itself.
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
			if err := s.setSlot(ctx, id, description, body); err != nil {
				return err
			}
			s.rememberID(flat, id, nil)
			return nil
		} else if isAuthError(derr) || stderrors.Is(derr, context.Canceled) || stderrors.Is(derr, context.DeadlineExceeded) {
			return derr
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
