package xdrive

import (
	"context"
	"strings"
	"sync"
)

// Under sharedMu, the empty ID counts roots; session IDs count connections or mark exclusive cleanup (-1).
type storageOwner struct {
	backend
	key      string
	sessions map[string]int
}

func (o *storageOwner) hold(id string, delta int) bool {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if n := o.sessions[id]; n >= 0 && (n == 0 || delta >= 0) {
		o.sessions[id] = n + delta
		return true
	}
	return false
}

func (o *storageOwner) release(id string) error {
	sharedMu.Lock()
	if o.sessions[id]--; o.sessions[id] <= 0 {
		delete(o.sessions, id) // Drop the last holder, including an exclusive cleanup (-1).
	}
	if len(o.sessions) == 0 {
		delete(shared, o.key)
		sharedMu.Unlock()
		return o.backend.Close()
	}
	sharedMu.Unlock()
	return nil
}

// Dial and the listener retain a root until session admission finishes.
func (o *storageOwner) session(id string) (backend, func(), error) {
	if !o.hold(id, 1) {
		return nil, nil, errSessionInUse
	}
	child := o.backend.session()
	return child, sync.OnceFunc(func() {
		child.Close()
		o.release(id)
	}), nil
}

// Keep active sessions on this profile out of another listener's collection.
func (o *storageOwner) Delete(ctx context.Context, name string) error {
	if id, stream := strings.CutPrefix(name, streamsDir+"/"); stream {
		if !o.hold(id, -1) {
			return errSessionInUse
		}
		defer o.release(id)
	}
	return o.backend.Delete(ctx, name)
}
