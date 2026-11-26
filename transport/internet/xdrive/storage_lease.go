package xdrive

import "sync"

// sharedMu protects the owner's ref count and the shared-profile map. The last
// lease removes the owner from the map before closing its transports.
type storageOwner struct {
	root Storage
	key  string
	refs int
}

type storageLease struct {
	Storage
	owner *storageOwner
	close func() error
}

func ownedStorage(root Storage) *storageLease {
	return (&storageOwner{root: root, refs: 1}).lease(root)
}

func (o *storageOwner) lease(backend Storage) *storageLease {
	return &storageLease{Storage: backend, owner: o, close: sync.OnceValue(func() error {
		var err error
		if backend != o.root {
			err = backend.Close()
		}
		sharedMu.Lock()
		o.refs--
		last := o.refs == 0
		if last && o.key != "" {
			delete(shared, o.key)
		}
		sharedMu.Unlock()
		if last {
			if rootErr := o.root.Close(); err == nil {
				err = rootErr
			}
		}
		return err
	})}
}

func (l *storageLease) Close() error { return l.close() }

// Called with a live root lease: Dial still owns it, and Listener.Close joins
// the accept loop before releasing it. Only remote backends need child clients.
func (l *storageLease) session() *storageLease {
	sharedMu.Lock()
	l.owner.refs++
	sharedMu.Unlock()
	backend := l.owner.root
	if root, ok := backend.(interface{ session() Storage }); ok {
		backend = root.session()
	}
	return l.owner.lease(backend)
}
