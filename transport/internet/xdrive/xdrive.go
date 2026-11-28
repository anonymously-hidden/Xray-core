package xdrive

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

const (
	protocolName = "xdrive"
	sessionsDir  = "sessions"
	streamsDir   = "streams"
	uplinkDir    = "c2s"
	downlinkDir  = "s2c"
)

func init() {
	common.Must(internet.RegisterProtocolConfigCreator(protocolName, func() interface{} {
		return new(Config)
	}))
	common.Must(internet.RegisterTransportDialer(protocolName, Dial))
	common.Must(internet.RegisterTransportListener(protocolName, Serve))
}

func newSessionID() string {
	var id [16]byte
	rand.Read(id[:]) // crypto/rand.Read fills the buffer or terminates the process.
	return hex.EncodeToString(id[:])
}

func announceName(session string, at time.Time) string {
	return fmt.Sprintf("%s/%d-%s", sessionsDir, at.UnixNano(), session)
}

// A valid timestamp does not imply a safe session name: validate it before use.
func announceParts(entry string) (string, bool) {
	stamp, session, ok := strings.Cut(entry, "-")
	_, err := strconv.ParseInt(stamp, 10, 64)
	return session, ok && session != "" && err == nil
}

// A session id reaches object names, so it must not traverse, must not
// collide with another name once a separator is flattened, and must not name
// a reserved file.
func safeSessionID(session string) bool {
	return safeNameChars(session) && !reservedName(session)
}

func safeNameChars(name string) bool {
	if name == "" || len(name) > 255 {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return false
		}
	}
	return name[0] != '.' && name[len(name)-1] != '.'
}

func reservedName(name string) bool {
	base, _, _ := strings.Cut(name, ".")
	switch strings.ToUpper(base) {
	case "CON", "PRN", "AUX", "NUL":
		return true
	}
	if len(base) == 4 && base[3] >= '1' && base[3] <= '9' {
		return strings.EqualFold(base[:3], "COM") || strings.EqualFold(base[:3], "LPT")
	}
	return false
}

func sessionPrefix(session string) string {
	return streamsDir + "/" + session
}

func uplinkPrefix(session string) string {
	return sessionPrefix(session) + "/" + uplinkDir
}

func downlinkPrefix(session string) string {
	return sessionPrefix(session) + "/" + downlinkDir
}

func streamConfig(streamSettings *internet.MemoryStreamConfig) (*Config, error) {
	if streamSettings == nil {
		return nil, errors.New("missing stream settings")
	}
	config, ok := streamSettings.ProtocolSettings.(*Config)
	if !ok || config == nil {
		return nil, errors.New("invalid protocol settings")
	}
	return config, nil
}

func Dial(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (stat.Connection, error) {
	config, err := streamConfig(streamSettings)
	if err = cmp.Or(ctx.Err(), err); err != nil {
		return nil, err
	}

	if streamSettings.Destination == nil && dest.Address != nil {
		streamSettings = new(*streamSettings)
		streamSettings.Destination = new(net.TCPDestination(dest.Address, dest.Port))
	}

	root, err := newStorage(streamSettings, config)
	if err != nil {
		return nil, err
	}
	session := newSessionID()
	storage, release, err := root.session(session)
	root.release("")
	if err != nil {
		return nil, err
	}
	p := paramsForStorage(config, storage)
	if slots, ok := storage.(*driveStorage); ok {
		r, id, err := slots.createRing(ctx, session, p)
		if err != nil {
			release()
			return nil, err
		}
		conn := newSlotConn(context.Background(), slots, r.Up, r.Down, p, release)
		announce, _ := json.Marshal(r)
		// The listener may delete the announcement before its POST reply arrives.
		if err := slots.writeFile(ctx, id, announceName(session, time.Now()), slotMagic, announce, func() bool { return conn.writer.(*slotWriter).acked.Load() >= 0 }); err != nil {
			conn.fail(err)
			conn.Close()
			return nil, err
		}
		errors.LogInfo(ctx, "opened session ", session, " in ", len(r.Up.Slots), " slots")
		return conn, nil
	}

	wal := storage.(Storage)
	if err := wal.Put(ctx, announceName(session, time.Now()), nil); err != nil {
		release()
		return nil, errors.New("failed to announce session ", session).Base(err)
	}
	errors.LogInfo(ctx, "opened session ", session)
	return newConn(context.Background(), wal, uplinkPrefix(session), downlinkPrefix(session), p, false, release), nil
}

type Listener struct {
	ctx     context.Context
	cancel  context.CancelFunc
	storage *storageOwner
	addConn internet.ConnHandler
	params

	handled       map[string]time.Time
	announcements map[string]seenAnnouncement
	nextPrune     time.Time
	cleaning      sync.Mutex // One announcement list can be draining at a time.
	idle          sync.Map   // Session ID to last stream sweep; a claim invalidates the mark.
	loopWG        sync.WaitGroup
	closeOnce     sync.Once
	closeErr      error
}

type seenAnnouncement struct {
	first, last time.Time
	warned      bool
}

func Serve(ctx context.Context, address net.Address, port net.Port, streamSettings *internet.MemoryStreamConfig, addConn internet.ConnHandler) (internet.Listener, error) {
	config, err := streamConfig(streamSettings)
	if err != nil {
		return nil, err
	}

	storage, err := newStorage(streamSettings, config)
	if err != nil {
		return nil, err
	}

	listenerCtx, cancel := context.WithCancel(context.Background())
	listener := &Listener{
		ctx:           listenerCtx,
		cancel:        cancel,
		storage:       storage,
		addConn:       addConn,
		params:        paramsForStorage(config, storage.backend),
		handled:       make(map[string]time.Time),
		announcements: make(map[string]seenAnnouncement),
	}

	listener.loopWG.Go(func() { listener.acceptLoop(ctx) })
	listener.loopWG.Go(func() { listener.collectLoop(ctx) })

	return listener, nil
}

func (l *Listener) acceptLoop(logCtx context.Context) {
	delay := l.minPollInterval
	active := time.Now()
	for {
		accepted, err := l.acceptPending(logCtx)
		if err != nil && !isAuthError(err) {
			errors.LogWarningInner(logCtx, err, "failed to list sessions")
		}
		if accepted {
			active = time.Now()
		}
		if time.Since(active) < l.eagerWindow {
			delay = l.minPollInterval
		} else {
			delay = min(2*delay, l.maxPollInterval)
		}

		if sleepContext(l.ctx, delay) != nil {
			return
		}
	}
}

func (l *Listener) acceptPending(logCtx context.Context) (bool, error) {
	sessions, err := l.storage.List(l.ctx, sessionsDir)
	if err != nil {
		return false, err
	}
	now := time.Now()
	accepted := false
	var discard []string
	for _, listed := range sessions {
		name := sessionsDir + "/" + listed.Name
		id, ok := announceParts(listed.Name)
		if !ok || !safeSessionID(id) {
			if !ok && safeSessionID(listed.Name) {
				discard = append(discard, name)
			}
			continue
		}
		a := l.announcements[listed.Name]
		a.first, a.last = cmp.Or(a.first, listed.created, now), now
		expires := a.first.Add(l.sessionTTL)
		_, handled := l.handled[id]
		if !handled && time.Now().Before(expires) {
			conn, err := l.accept(logCtx, id, listed, expires)
			if err != nil && !a.warned {
				errors.LogWarningInner(logCtx, err, "failed to read the announcement of session ", id)
				a.warned = true
			}
			l.announcements[listed.Name] = a
			if conn == nil {
				continue // Retry this announcement on the next successful poll.
			}
			accepted, handled = true, true
			l.addConn(conn)
		}
		l.announcements[listed.Name] = a
		if handled {
			l.handled[id] = now
		}
		discard = append(discard, name)
	}
	l.drop(logCtx, discard)
	if !now.Before(l.nextPrune) {
		l.nextPrune = now.Add(l.sessionTTL)
		cutoff := now.Add(-2 * l.sessionTTL)
		maps.DeleteFunc(l.announcements, func(_ string, a seenAnnouncement) bool { return a.last.Before(cutoff) })
		sharedMu.Lock()
		maps.DeleteFunc(l.handled, func(id string, last time.Time) bool {
			return last.Before(cutoff) && l.storage.sessions[id] == 0
		})
		sharedMu.Unlock()
	}
	return accepted, nil
}

func (l *Listener) accept(logCtx context.Context, id string, listed Entry, expires time.Time) (*Conn, error) {
	storage, release, err := l.storage.session(id)
	if err != nil {
		return nil, nil // Another listener owns the cleanup lease.
	}
	l.idle.Delete(id)
	admit, cancel := context.WithDeadline(l.ctx, expires)
	defer cancel()
	if slots, drive := storage.(*driveStorage); drive {
		var r *ring
		r, err = slots.readRing(admit, id, listed)
		if r != nil && admit.Err() == nil && time.Now().Before(expires) {
			errors.LogInfo(logCtx, "accepted session ", id, " in ", len(r.Down.Slots), " slots")
			p := l.params
			p.flushInterval = time.Duration(r.Tick) * time.Millisecond
			return newSlotConn(l.ctx, slots, r.Down, r.Up, p, release), nil
		}
	} else if admit.Err() == nil && time.Now().Before(expires) {
		errors.LogInfo(logCtx, "accepted session ", id)
		return newConn(l.ctx, storage.(Storage), downlinkPrefix(id), uplinkPrefix(id), l.params, true, release), nil
	}
	release()
	return nil, err
}

func (l *Listener) drop(logCtx context.Context, names []string) {
	if len(names) == 0 || !l.cleaning.TryLock() {
		return // A later complete list will rediscover anything still present.
	}
	l.loopWG.Go(func() {
		defer l.cleaning.Unlock()
		var backoff time.Duration
		for _, name := range names {
			if l.ctx.Err() != nil {
				return
			}
			if err := l.storage.Delete(l.ctx, name); err != nil {
				errors.LogDebug(logCtx, "failed to drop ", name, ": ", err)
				backoff = cleanupRetryDelay + jitter(cleanupRetryDelay)
			}
		}
		sleepContext(l.ctx, backoff) // Retry remaining names from a fresh listing.
	})
}

func (l *Listener) collectLoop(logCtx context.Context) {
	for sleepContext(l.ctx, l.sessionTTL) == nil {
		if drive, ok := l.storage.backend.(*driveStorage); ok && drive.cooling() {
			continue
		}
		sessions, err := l.storage.List(l.ctx, streamsDir)
		if err != nil {
			errors.LogWarningInner(logCtx, err, "failed to collect abandoned sessions")
			continue
		}
		sweep := time.Now()
		for _, listed := range sessions {
			id := listed.Name
			if !safeSessionID(id) {
				continue
			}
			if !l.storage.hold(id, -1) {
				l.idle.Delete(id)
				continue
			}
			last, seen := l.idle.Swap(id, sweep)
			expired := seen && last != sweep // Idle on two consecutive, TTL-spaced sweeps.
			if expired {
				if err := l.storage.backend.Delete(l.ctx, sessionPrefix(id)); err != nil {
					errors.LogWarningInner(logCtx, err, "failed to collect ", id)
				} else {
					l.idle.Delete(id)
				}
			}
			l.storage.release(id)
			if expired && sleepContext(l.ctx, cleanupSessionPause) != nil {
				return
			}
		}
		l.idle.Range(func(id, last any) bool {
			if last != sweep {
				l.idle.Delete(id)
			}
			return true
		})
	}
}

func (l *Listener) Addr() net.Addr {
	return placeholderAddr
}

func (l *Listener) Close() error {
	l.closeOnce.Do(func() {
		l.cancel()
		l.loopWG.Wait()
		l.closeErr = l.storage.release("")
	})
	return l.closeErr
}
