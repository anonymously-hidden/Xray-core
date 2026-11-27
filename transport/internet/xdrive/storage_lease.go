package xdrive

import (
	"context"
	"strings"
	"sync"
)

// sharedMu protects the owners and their holders. The empty ID counts root
// handles; session IDs count open connections or mark exclusive cleanup (-1).
type storageOwner struct {
	backend
	key      string
	sessions map[string]int
}

func (o *storageOwner) hold(id string, delta int) bool {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	n := o.sessions[id]
	if n < 0 || delta < 0 && n != 0 {
		return false
	}
	o.sessions[id] = n + delta
	return true
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

// Called with a live root reference: Dial still owns it, and Listener.Close joins
// the accept loop before releasing it. Only remote backends need child clients.
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
