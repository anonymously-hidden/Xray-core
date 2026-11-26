package xdrive

import (
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config, err := streamConfig(streamSettings)
	if err != nil {
		return nil, err
	}

	if streamSettings.Destination == nil && dest.Address != nil {
		front := dest
		front.Network = net.Network_TCP
		patched := *streamSettings
		patched.Destination = &front
		streamSettings = &patched
	}

	root, err := newStorage(streamSettings, config)
	if err != nil {
		return nil, err
	}
	storage := root.session()
	root.Close()
	session := newSessionID()
	p := paramsForStorage(config, storage.Storage)
	if slots, ok := storage.Storage.(*driveStorage); ok {
		depth := max(2, p.concurrency)
		ids, err := slots.takeIDs(ctx, 2*(depth+1)+1)
		if err != nil {
			storage.Close()
			return nil, err
		}
		r := lazyRing(session, depth, ids[:len(ids)-1])
		errors.LogInfo(ctx, "opened session ", session, " in ", len(r.Up.Slots), " slots")
		releaseSession, err := slots.holdSession(session)
		if err != nil {
			storage.Close()
			return nil, err
		}
		conn := newSlotConn(context.Background(), slots, r.Up, r.Down, p, nil, func() {
			storage.Close()
			releaseSession()
		})
		writer := conn.writer.(*slotWriter)

		conn.publishWG.Go(func() {
			select {
			case <-conn.ctx.Done():
				return
			case r.First = <-writer.first:
			case <-time.After(25 * time.Millisecond):
			}
			if conn.ctx.Err() != nil {
				return
			}
			announce, _ := json.Marshal(r)
			if len(r.First) != 0 && len(announce) > driveInlineLimit {
				r.First = nil
				announce, _ = json.Marshal(r)
			}
			// An announce failure surfaces as a connection error on the next Write/Read.
			if err := slots.putObject(conn.ctx, ids[len(ids)-1], announceName(session, time.Now()), announce); err != nil && conn.ctx.Err() == nil {
				err = errors.New("failed to announce session ", session).Base(err)
				writer.fail(err)
			}
		})
		return conn, nil
	}

	if err := storage.Put(ctx, announceName(session, time.Now()), nil); err != nil {
		storage.Close()
		return nil, errors.New("failed to announce session ", session).Base(err)
	}
	errors.LogInfo(ctx, "opened session ", session)
	return newConn(context.Background(), storage.Storage,
		uplinkPrefix(session), downlinkPrefix(session), p, false, func() {
			storage.Close()
		}), nil
}

type Listener struct {
	ctx     context.Context
	cancel  context.CancelFunc
	storage *storageLease
	addConn internet.ConnHandler
	params

	mu             sync.Mutex
	active         map[string]bool
	handled        map[string]time.Time
	idleSince      map[string]time.Time
	collecting     map[string]bool
	announceWarned map[string]bool
	announcements  map[string]seenAnnouncement
	stopping       bool
	cleanupPending map[string]bool
	cleanupQueue   []string
	cleanupRunning int
	loopWG         sync.WaitGroup
	cleanupWG      sync.WaitGroup
	closeOnce      sync.Once
	closeErr       error
}

type seenAnnouncement struct{ first, last time.Time }

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
		ctx:            listenerCtx,
		cancel:         cancel,
		storage:        storage,
		addConn:        addConn,
		params:         paramsForStorage(config, storage.Storage),
		active:         make(map[string]bool),
		handled:        make(map[string]time.Time),
		idleSince:      make(map[string]time.Time),
		announceWarned: make(map[string]bool),
		announcements:  make(map[string]seenAnnouncement),
		collecting:     make(map[string]bool),
		cleanupPending: make(map[string]bool),
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
		if accepted || time.Since(active) < l.eagerWindow {
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

	accepted := false
	l.mu.Lock()
	now := time.Now()
	for _, listed := range sessions {
		seen := l.announcements[listed.Name]
		if seen.first.IsZero() {
			seen.first = listed.created
			if seen.first.IsZero() || seen.first.After(now) {
				seen.first = now
			}
		}
		seen.last = now
		l.announcements[listed.Name] = seen
	}
	// Keep first-seen times across brief listing omissions.
	maps.DeleteFunc(l.announcements, func(_ string, a seenAnnouncement) bool { return now.Sub(a.last) >= 2*l.sessionTTL })
	l.mu.Unlock()

	for _, listed := range sessions {
		entry := listed.Name
		full := sessionsDir + "/" + entry

		session, ok := announceParts(entry)
		if !ok {
			// Never construct a deletion path from an unsafe name.
			if safeSessionID(entry) {
				l.queueDrop(full)
			}
			continue
		}
		if !safeSessionID(session) {
			continue
		}
		l.mu.Lock()
		expires := l.announcements[entry].first.Add(l.sessionTTL)
		l.mu.Unlock()
		if !time.Now().Before(expires) {
			errors.LogInfo(logCtx, "dropping the stale announcement of session ", session)
			l.queueDrop(full)
			l.queueSessionDrop(session)
			continue
		}
		if !l.claim(session) {
			l.mu.Lock()
			if !l.handled[session].IsZero() {
				l.queueDropLocked(full)
			}
			l.mu.Unlock()
			continue
		}
		admit, cancel := context.WithDeadline(l.ctx, expires)
		announce := listed.Inline
		var conn *Conn
		var loadErr error
		if _, slots := l.storage.Storage.(*driveStorage); slots && len(announce) == 0 {
			announce, loadErr = l.storage.Get(context.WithValue(admit, maxBodyKey{}, int64(driveMaxAnnounce)), full)
		}
		if loadErr == nil && admit.Err() == nil {
			conn = l.newSessionConn(admit, session, announce)
		}
		cancel()
		if conn == nil {
			l.mu.Lock()
			delete(l.active, session)
			delete(l.handled, session)
			warn := loadErr != nil && !l.announceWarned[session]
			if warn {
				l.announceWarned[session] = true
			}
			l.mu.Unlock()
			if warn {
				errors.LogWarningInner(logCtx, loadErr, "failed to read the announcement of session ", session)
			}
			continue
		}
		l.queueDrop(full)
		accepted = true
		l.addConn(conn)
	}
	return accepted, nil
}

const (
	listenerCleanupWorkers = 2
	listenerCleanupQueue   = 128
)

func (l *Listener) queueDrop(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.queueDropLocked(name)
}

func (l *Listener) queueSessionDrop(session string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active[session] || !l.handled[session].IsZero() || l.collecting[session] {
		return
	}
	if since := l.idleSince[session]; !since.IsZero() && time.Since(since) < l.sessionTTL {
		return
	}
	l.collecting[session] = true
	if !l.queueDropLocked(sessionPrefix(session)) {
		delete(l.collecting, session)
	}
}

func (l *Listener) queueDropLocked(name string) bool {
	if l.stopping || l.ctx.Err() != nil || len(l.cleanupQueue) >= listenerCleanupQueue || l.cleanupPending[name] {
		return false
	}
	l.cleanupPending[name] = true
	l.cleanupQueue = append(l.cleanupQueue, name)
	if l.cleanupRunning < listenerCleanupWorkers {
		l.cleanupRunning++
		l.cleanupWG.Go(l.cleanupWorker)
	}
	return true
}

func (l *Listener) cleanupWorker() {
	for {
		l.mu.Lock()
		if len(l.cleanupQueue) == 0 {
			l.cleanupRunning--
			l.mu.Unlock()
			return
		}
		name := l.cleanupQueue[0]
		l.cleanupQueue[0] = ""
		l.cleanupQueue = l.cleanupQueue[1:]
		l.mu.Unlock()
		err := l.drop(name)
		l.finishDrop(name, err)
	}
}

func (l *Listener) finishDrop(name string, err error) {
	l.mu.Lock()
	delete(l.cleanupPending, name)
	if strings.HasPrefix(name, streamsDir+"/") {
		session := strings.TrimPrefix(name, streamsDir+"/")
		delete(l.collecting, session)
		if err == nil || err == errSessionInUse || l.idleSince[session].IsZero() {
			l.idleSince[session] = time.Now()
		}
	}
	l.mu.Unlock()
}

func (l *Listener) drop(name string) error {
	ctx := context.WithValue(l.ctx, cleanupKey{}, true)
	var last error
	for attempt := 0; attempt < cleanupMaxRetries && ctx.Err() == nil; attempt++ {
		last = l.storage.Delete(ctx, name)
		if last == nil || last == errSessionInUse {
			return last
		}
		errors.LogDebug(ctx, "failed to drop ", name, ": ", last)
		if attempt+1 < cleanupMaxRetries && sleepContext(ctx, cleanupRetryDelay+jitter(cleanupRetryDelay)) != nil {
			return ctx.Err()
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return last
}

func (l *Listener) claim(session string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopping || l.ctx.Err() != nil || l.active[session] || !l.handled[session].IsZero() || l.collecting[session] {
		return false
	}
	l.active[session] = true
	l.handled[session] = time.Now()
	// The session was accepted, so the inactivity it was listed with is over:
	// a claim must not hand the session the age it had before it was taken.
	delete(l.idleSince, session)
	return true
}

// Drive requires a ring announcement; other backends use the WAL.
func (l *Listener) newSessionConn(admit context.Context, session string, announce []byte) *Conn {
	storage := l.storage.session()
	onClose := func() {
		storage.Close()
		l.mu.Lock()
		delete(l.active, session)
		l.mu.Unlock()
	}
	if slots, ok := storage.Storage.(*driveStorage); ok {
		r, valid := decodeRing(announce)
		if !valid {
			onClose()
			return nil
		}
		release, err := slots.holdSession(session)
		if err != nil {
			onClose()
			return nil
		}
		r.Up.lazy, r.Down.lazy = r.Lazy, r.Lazy
		r.Up.name, r.Down.name = uplinkPrefix(session), downlinkPrefix(session)
		deadline, _ := admit.Deadline()
		if slots.bindRing(admit, r) != nil || admit.Err() != nil || !time.Now().Before(deadline) {
			release()
			onClose()
			return nil
		}
		errors.LogInfo(admit, "accepted session ", session, " in ", len(r.Down.Slots), " slots")
		return newSlotConn(l.ctx, slots, r.Down, r.Up, l.params, r.First, func() { release(); onClose() })
	}
	errors.LogInfo(admit, "accepted session ", session)
	return newConn(l.ctx, storage.Storage, downlinkPrefix(session), uplinkPrefix(session), l.params, true, onClose)
}

func (l *Listener) collectLoop(logCtx context.Context) {
	for sleepContext(l.ctx, l.sessionTTL/2) == nil {
		if err := l.collect(); err != nil {
			errors.LogWarningInner(logCtx, err, "failed to collect abandoned sessions")
		}
	}
}

func (l *Listener) collect() error {
	if drive, ok := l.storage.Storage.(*driveStorage); ok && drive.cooling() {
		return nil
	}
	ctx := context.WithValue(l.ctx, cleanupKey{}, true)
	ctx = context.WithValue(ctx, namesOnlyKey{}, true)
	sessions, err := l.storage.List(ctx, streamsDir)
	if err != nil {
		return err
	}

	now := time.Now()
	var expired []string

	l.mu.Lock()
	present := make(map[string]bool, len(sessions))
	for _, listed := range sessions {
		session := listed.Name
		if !safeSessionID(session) {
			// A listed stream name must never reach an object name.
			continue
		}
		present[session] = true
		if l.active[session] {
			delete(l.idleSince, session)
			continue
		}
		if l.collecting[session] || l.stopping {
			continue
		}
		since, seen := l.idleSince[session]
		if !seen {
			l.idleSince[session] = now
			continue
		}
		if now.Sub(since) >= l.sessionTTL {
			l.collecting[session] = true
			expired = append(expired, session)
		}
	}
	maps.DeleteFunc(l.idleSince, func(id string, _ time.Time) bool { return !present[id] })
	announced := make(map[string]bool)
	for entry, seen := range l.announcements {
		if session, ok := announceParts(entry); ok && safeSessionID(session) && now.Sub(seen.last) < 2*l.sessionTTL {
			announced[session] = true
		}
	}
	unused := func(id string) bool { return !l.active[id] && !l.collecting[id] && !announced[id] }
	maps.DeleteFunc(l.handled, func(id string, at time.Time) bool { return unused(id) && now.Sub(at) >= 2*l.sessionTTL })
	maps.DeleteFunc(l.announceWarned, func(id string, _ bool) bool { return unused(id) && l.handled[id].IsZero() })
	l.mu.Unlock()

	defer func() {
		l.mu.Lock()
		for _, session := range expired {
			delete(l.collecting, session)
		}
		l.mu.Unlock()
	}()
	var first error
	for i, session := range expired {
		if i > 0 && sleepContext(l.ctx, cleanupSessionPause) != nil {
			return l.ctx.Err()
		}
		err := l.storage.Delete(ctx, sessionPrefix(session))
		if err != nil && err != errSessionInUse {
			if first == nil {
				first = err
			}
			continue
		}
		l.mu.Lock()
		if err == nil {
			delete(l.idleSince, session)
		} else {
			l.idleSince[session] = time.Now()
		}
		l.mu.Unlock()
	}
	return first
}

func (l *Listener) Addr() net.Addr {
	return placeholderAddr
}

func (l *Listener) Close() error {
	l.closeOnce.Do(func() {
		l.mu.Lock()
		l.stopping = true
		l.mu.Unlock()
		if l.cancel != nil {
			l.cancel()
		}
		l.loopWG.Wait()
		l.cleanupWG.Wait()
		l.closeErr = l.storage.Close()
	})
	return l.closeErr
}
