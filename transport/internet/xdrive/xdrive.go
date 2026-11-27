package xdrive

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/base64"
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
		depth := max(2, p.concurrency)
		ids, err := slots.takeIDs(ctx, depth)
		if err != nil {
			release()
			return nil, err
		}
		r := lazyRing(session, depth, ids)
		errors.LogInfo(ctx, "opened session ", session, " in ", len(r.Up.Slots), " slots")
		conn := newSlotConn(context.Background(), slots, r.Up, r.Down, p, nil, release)
		conn.publishWG.Go(func() {
			select {
			case r.First = <-conn.writer.(*slotWriter).first:
			case <-time.After(25 * time.Millisecond):
			case <-conn.ctx.Done():
				return
			}
			announce, _ := json.Marshal(r)
			if len(r.First) != 0 && len(announce) > driveInlineLimit {
				r.First = nil
				announce, _ = json.Marshal(r)
			}
			description := "" // Only oversized announcements need media.
			if len(announce) <= driveInlineLimit {
				description, announce = base64.StdEncoding.EncodeToString(announce), nil
			}
			if err := slots.writeFile(conn.ctx, ids[len(ids)-1], announceName(session, time.Now()), driveFile{Description: description}, announce, nil, nil); err != nil {
				conn.fail(errors.New("failed to announce session ", session).Base(err))
			}
		})
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

	mu             sync.Mutex
	sessions       map[string]*sessionState
	announcements  map[string]seenAnnouncement
	cleanupPending map[string]bool
	cleanupQueue   chan string
	loopWG         sync.WaitGroup
	closeOnce      sync.Once
	closeErr       error
}

type seenAnnouncement struct{ first, last time.Time }

type sessionState struct {
	handled, warned, present bool
	keepUntil, idle          time.Time
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
		ctx:            listenerCtx,
		cancel:         cancel,
		storage:        storage,
		addConn:        addConn,
		params:         paramsForStorage(config, storage.backend),
		sessions:       make(map[string]*sessionState),
		announcements:  make(map[string]seenAnnouncement),
		cleanupPending: make(map[string]bool),
		cleanupQueue:   make(chan string, listenerCleanupQueue),
	}

	listener.loopWG.Go(func() { listener.acceptLoop(ctx) })
	listener.loopWG.Go(func() { listener.collectLoop(ctx) })
	for range listenerCleanupWorkers {
		listener.loopWG.Go(func() { listener.cleanupWorker(ctx) })
	}

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
	// Record every age and retain handled IDs before an admission blocks on I/O.
	l.mu.Lock()
	now := time.Now()
	for i, listed := range sessions {
		// Drive translates future creation dates to now; WAL supplies no date.
		sessions[i].created = cmp.Or(l.announcements[listed.Name].first, listed.created, now)
		l.announcements[listed.Name] = seenAnnouncement{first: sessions[i].created, last: now}
		if id, ok := announceParts(listed.Name); ok && l.sessions[id] != nil {
			l.sessions[id].keepUntil = now.Add(2 * l.sessionTTL)
		}
	}
	l.mu.Unlock()
	accepted := false
	for _, listed := range sessions {
		if conn := l.accept(logCtx, listed); conn != nil {
			l.addConn(conn)
			accepted = true
		}
	}
	return accepted, nil
}

func (l *Listener) accept(logCtx context.Context, listed Entry) *Conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	full := sessionsDir + "/" + listed.Name
	session, ok := announceParts(listed.Name)
	if !ok && safeSessionID(listed.Name) {
		l.queueDrop(full)
	}
	if !ok || !safeSessionID(session) {
		return nil
	}
	s := l.state(session)
	now := time.Now()
	expires := listed.created.Add(l.sessionTTL)
	s.keepUntil = now.Add(2 * l.sessionTTL)
	if !now.Before(expires) || s.handled {
		l.queueDrop(full)
		if !s.handled && now.Sub(s.idle) >= l.sessionTTL {
			errors.LogInfo(logCtx, "dropping the stale announcement of session ", session)
			l.queueDrop(sessionPrefix(session))
		}
		return nil
	}
	if l.ctx.Err() != nil || l.cleanupPending[sessionPrefix(session)] {
		return nil
	}
	storage, release, err := l.storage.session(session)
	if err != nil {
		return nil
	}
	s.handled, s.idle = true, time.Time{} // The owner lease also protects this tentative claim.
	admit, cancel := context.WithDeadline(l.ctx, expires)
	defer cancel()
	slots, drive := storage.(*driveStorage)
	var r *ring
	ready := admit.Err() == nil
	if drive && ready {
		l.mu.Unlock()
		r, err = slots.readRing(admit, session, listed)
		ready = r != nil && time.Now().Before(expires) && admit.Err() == nil
		l.mu.Lock()
	}
	if !ready {
		release()
		s.handled = false
		if err != nil && !s.warned {
			errors.LogWarningInner(logCtx, err, "failed to read the announcement of session ", session)
			s.warned = true
		}
		return nil
	}
	defer l.queueDrop(full)
	if drive {
		errors.LogInfo(admit, "accepted session ", session, " in ", len(r.Down.Slots), " slots")
		return newSlotConn(l.ctx, slots, r.Down, r.Up, l.params, r.First, release)
	}
	errors.LogInfo(admit, "accepted session ", session)
	return newConn(l.ctx, storage.(Storage), downlinkPrefix(session), uplinkPrefix(session), l.params, true, release)
}

const (
	listenerCleanupWorkers = 2
	listenerCleanupQueue   = 128
)

// Called under mu. A state stays in the map while a connection or a cleanup uses it.
func (l *Listener) state(id string) *sessionState {
	if l.sessions[id] == nil {
		l.sessions[id] = new(sessionState)
	}
	return l.sessions[id]
}

func (l *Listener) queueDrop(name string) {
	if !l.cleanupPending[name] && len(l.cleanupQueue) < cap(l.cleanupQueue) && l.ctx.Err() == nil {
		l.cleanupPending[name] = true
		l.cleanupQueue <- name // Producers hold mu; a worker can only free space.
	}
}

func (l *Listener) cleanupWorker(logCtx context.Context) {
	for {
		select {
		case <-l.ctx.Done():
			return
		case name := <-l.cleanupQueue:
			l.drop(logCtx, name, false)
		}
	}
}

func (l *Listener) drop(logCtx context.Context, name string, periodic bool) {
	var err error
	for attempt := 1; l.ctx.Err() == nil; attempt++ {
		if err = l.storage.Delete(l.ctx, name); err == nil || err == errSessionInUse {
			break
		}
		if periodic {
			errors.LogWarningInner(logCtx, err, "failed to collect ", name)
			break
		}
		errors.LogDebug(logCtx, "failed to drop ", name, ": ", err)
		if attempt == cleanupMaxRetries || sleepContext(l.ctx, cleanupRetryDelay+jitter(cleanupRetryDelay)) != nil {
			break
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.cleanupPending, name)
	if session, ok := strings.CutPrefix(name, streamsDir+"/"); ok {
		s := l.sessions[session]
		if err == nil && periodic {
			s.idle = time.Time{} // A new listing starts a new inactivity period.
		} else if err == nil || err == errSessionInUse || s.idle.IsZero() {
			s.idle = time.Now()
		}
	}
}

func (l *Listener) collectLoop(logCtx context.Context) {
	var nextDrop time.Time
	for sleepContext(l.ctx, l.sessionTTL/2) == nil {
		if drive, ok := l.storage.backend.(*driveStorage); ok && drive.cooling() {
			continue
		}
		sessions, err := l.storage.List(l.ctx, streamsDir)
		if err != nil {
			errors.LogWarningInner(logCtx, err, "failed to collect abandoned sessions")
			continue
		}
		for _, name := range l.collect(sessions) {
			if sleepContext(l.ctx, time.Until(nextDrop)) != nil {
				return
			}
			l.drop(logCtx, name, true)
			nextDrop = time.Now().Add(cleanupSessionPause)
		}
	}
}

func (l *Listener) collect(sessions []Entry) []string {
	now := time.Now()
	var expired []string
	l.mu.Lock()
	defer l.mu.Unlock()
	maps.DeleteFunc(l.announcements, func(_ string, a seenAnnouncement) bool { return now.Sub(a.last) >= 2*l.sessionTTL })
	for _, listed := range sessions {
		if safeSessionID(listed.Name) {
			l.state(listed.Name).present = true
		}
	}
	sharedMu.Lock() // Take one snapshot of the owner's active holders for this sweep.
	defer sharedMu.Unlock()
	for id, s := range l.sessions {
		name := sessionPrefix(id)
		active := l.storage.sessions[id] > 0
		pending := l.cleanupPending[name]
		retained := active || pending || now.Before(s.keepUntil)
		if !retained {
			s.handled, s.warned = false, false
		}
		switch {
		case !s.present && !retained:
			delete(l.sessions, id)
		case !s.present || active:
			s.idle = time.Time{}
		case pending: // A cleanup already owns this session.
		case s.idle.IsZero():
			s.idle = now
		case now.Sub(s.idle) >= l.sessionTTL:
			l.cleanupPending[name] = true
			expired = append(expired, name)
		}
		s.present = false
	}
	return expired
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
