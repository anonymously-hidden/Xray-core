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

// takeIDs drops expired ids, refills in one background request when below one
// ring's worth, and returns up to n ids (generating inline only if empty).
func (s *driveStorage) takeIDs(ctx context.Context, n, ringWorth int) ([]string, error) {
	s.poolMu.Lock()
	now := time.Now()
	kept := s.idPool[:0]
	for _, e := range s.idPool {
		if now.Sub(e.at) < idPoolTTL {
			kept = append(kept, e)
		}
	}
	s.idPool = kept
	if len(s.idPool) < ringWorth && !s.poolRefill {
		s.poolRefill = true
		go s.refillIDs(2 * ringWorth)
	}
	take := min(n, len(s.idPool))
	out := make([]string, 0, take)
	for i := 0; i < take; i++ {
		out = append(out, s.idPool[i].id)
	}
	s.idPool = s.idPool[take:]
	s.poolMu.Unlock()
	if len(out) > 0 {
		return out, nil
	}
	return s.generateIDs(ctx, n)
}

func (s *driveStorage) refillIDs(want int) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ids, err := s.generateIDs(ctx, want)
	s.poolMu.Lock()
	defer s.poolMu.Unlock()
	s.poolRefill = false
	if err != nil {
		return
	}
	now := time.Now()
	for _, id := range ids {
		s.idPool = append(s.idPool, idPoolEntry{id: id, at: now})
	}
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
		s.rememberID(flat, id)
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
type idPoolState struct {
	poolMu          sync.Mutex
	idPool          []idPoolEntry
	poolRefill      bool
	fallbackLogOnce sync.Once
}
