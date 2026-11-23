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

func newSessionID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", errors.New("failed to generate session id").Base(err)
	}
	return hex.EncodeToString(buf), nil
}

func announceName(session string, at time.Time) string {
	return fmt.Sprintf("%s/%d-%s", sessionsDir, at.UnixNano(), session)
}

func parseAnnounce(entry string) (string, time.Time, bool) {
	session, at, ok := announceParts(entry)
	if !ok || !safeSessionID(session) {
		return "", time.Time{}, false
	}
	return session, at, true
}

// announceParts splits an announcement entry into its session and its time.
// The session is not checked: a caller that turns it into an object name has
// to ask safeSessionID first.
func announceParts(entry string) (string, time.Time, bool) {
	dash := strings.IndexByte(entry, '-')
	if dash <= 0 || dash == len(entry)-1 {
		return "", time.Time{}, false
	}
	nanos, err := strconv.ParseInt(entry[:dash], 10, 64)
	if err != nil {
		return "", time.Time{}, false
	}
	return entry[dash+1:], time.Unix(0, nanos), true
}

// A session id reaches object names, so it must not traverse, must not
// collide with another name once a separator is flattened, and must not name
// a reserved file.
func safeSessionID(session string) bool {
	return safeNameChars(session) && !reservedName(session)
}

// A listed name becomes a delete target, so it must be safe to build one
// from.
func safeListedName(name string) bool {
	return safeNameChars(name) && !reservedName(name)
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
	base := name
	if i := strings.IndexByte(base, '.'); i >= 0 {
		base = base[:i]
	}
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

	storage, err := newStorage(streamSettings)
	if err != nil {
		return nil, err
	}
	storage, err = transferSessionStorage(storage)
	if err != nil {
		return nil, err
	}
	session, err := newSessionID()
	if err != nil {
		storage.Close()
		return nil, err
	}
	p := paramsForStorage(config, storage)
	if slots, ok := storage.(slotStore); ok {
		depth := max(2, p.concurrency)
		ids, err := slots.takeIDs(ctx, 2*(depth+1)+1)
		if err != nil {
			storage.Close()
			return nil, err
		}
		a := ringAnnouncement{Version: ringVersion, IDs: ids[:len(ids)-1]}
		r := sessionRing(session, a.IDs)
		releaseSession, err := holdStorageSession(storage, session)
		if err != nil {
			storage.Close()
			return nil, err
		}
		onClose := func() {
			storage.Close()
			releaseSession()
		}
		announce, err := json.Marshal(a)
		if err == nil {
			err = putDriveObject(ctx, slots, ids[len(ids)-1], announceName(session, time.Now()), announce)
		}
		if err != nil {
			onClose()
			return nil, errors.New("failed to announce session ", session).Base(err)
		}
		errors.LogInfo(ctx, "opened session ", session, " in ", len(r.Up.Slots), " slots")
		return newSlotConn(context.Background(), slots, r.Up, r.Down, p, onClose), nil
	}

	if err := storage.Put(ctx, announceName(session, time.Now()), nil); err != nil {
		storage.Close()
		return nil, errors.New("failed to announce session ", session).Base(err)
	}
	errors.LogInfo(ctx, "opened session ", session)
	return newConn(context.Background(), storage,
		uplinkPrefix(session), downlinkPrefix(session), p, false, func() {
			storage.Close()
		}), nil
}

type noticeTime struct {
	id          string
	first, last time.Time
}

type Listener struct {
	ctx     context.Context
	cancel  context.CancelFunc
	storage Storage
	addConn internet.ConnHandler
	params

	mu             sync.Mutex
	active         map[string]bool
	handled        map[string]time.Time
	idleSince      map[string]time.Time
	collecting     string // the one stream currently being deleted
	announceWarned map[string]bool
	notices        map[string]noticeTime
	stopping       bool
	cleanupPending map[string]string // announcement name -> listed Drive ID (empty for WAL)
	cleanupQueue   []string
	cleanupRunning int
	loopWG         sync.WaitGroup
	cleanupWG      sync.WaitGroup
	closeOnce      sync.Once
	closeErr       error

	// collectPause spaces the deletion of two collected sessions.
	collectPause time.Duration
}

func Serve(ctx context.Context, address net.Address, port net.Port, streamSettings *internet.MemoryStreamConfig, addConn internet.ConnHandler) (internet.Listener, error) {
	config, err := streamConfig(streamSettings)
	if err != nil {
		return nil, err
	}

	storage, err := newStorage(streamSettings)
	if err != nil {
		return nil, err
	}

	listenerCtx, cancel := context.WithCancel(context.Background())
	listener := &Listener{
		ctx:            listenerCtx,
		cancel:         cancel,
		storage:        storage,
		addConn:        addConn,
		params:         paramsForStorage(config, storage),
		active:         make(map[string]bool),
		handled:        make(map[string]time.Time),
		idleSince:      make(map[string]time.Time),
		announceWarned: make(map[string]bool),
		notices:        make(map[string]noticeTime),
		cleanupPending: make(map[string]string),
		collectPause:   cleanupSessionPause,
	}

	listener.loopWG.Add(2)
	go func() {
		defer listener.loopWG.Done()
		listener.acceptLoop(ctx)
	}()
	go func() {
		defer listener.loopWG.Done()
		listener.collectLoop(ctx)
	}()

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

		switch {
		case accepted:
			active = time.Now()
			delay = l.minPollInterval
		case time.Since(active) < l.eagerWindow:
			delay = l.minPollInterval
		default:
			delay *= 2
			if delay > l.maxPollInterval {
				delay = l.maxPollInterval
			}
		}

		select {
		case <-l.ctx.Done():
			return
		case <-time.After(delay):
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
		n := l.notices[listed.Name]
		if n.first.IsZero() || n.id != listed.driveID {
			n.id, n.first = listed.driveID, listed.created
			if n.first.IsZero() || n.first.After(now) {
				n.first = now
			}
		}
		n.last = now
		l.notices[listed.Name] = n
		if session, _, ok := parseAnnounce(listed.Name); ok && now.Before(n.first.Add(l.sessionTTL)) {
			delete(l.idleSince, session) // invalidate an outstanding collection candidate
		}
	}
	maps.DeleteFunc(l.notices, func(_ string, n noticeTime) bool { return now.Sub(n.last) >= 2*l.sessionTTL })
	l.mu.Unlock()

	for _, listed := range sessions {
		entry := listed.Name
		full := sessionsDir + "/" + entry

		session, _, ok := announceParts(entry)
		if !ok {
			// Never construct a deletion path from an unsafe name.
			if safeListedName(entry) {
				l.queueDrop(full, listed.driveID)
			}
			continue
		}
		if !safeSessionID(session) {
			continue
		}
		l.mu.Lock()
		expires := l.notices[entry].first.Add(l.sessionTTL)
		l.mu.Unlock()
		if !time.Now().Before(expires) {
			errors.LogInfo(logCtx, "dropping the stale announcement of session ", session)
			l.queueDrop(full, listed.driveID)
			continue
		}
		if !l.claim(session) {
			l.mu.Lock()
			handled := !l.handled[session].IsZero()
			l.mu.Unlock()
			if handled {
				l.queueDrop(full, listed.driveID)
			}
			continue
		}
		admit, cancel := context.WithDeadline(l.ctx, expires)
		announce := listed.Inline
		var conn *Conn
		var loadErr error
		if _, slots := l.storage.(slotStore); slots && len(announce) == 0 {
			ctx := context.WithValue(admit, listedDriveIDKey{}, listed.driveID)
			announce, loadErr = l.storage.Get(context.WithValue(ctx, maxBodyKey{}, int64(driveMaxAnnounce)), full)
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
		l.queueDrop(full, listed.driveID)
		accepted = true
		l.addConn(conn)
	}
	return accepted, nil
}

const (
	listenerCleanupWorkers = 2
	listenerCleanupQueue   = 128
)

// Only announcements go through this queue. Their expiry is not proof that
// the stream files are orphaned; collect applies a separate inactivity TTL.
func (l *Listener) queueDrop(name, id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, pending := l.cleanupPending[name]
	if pending || l.stopping || l.ctx.Err() != nil || len(l.cleanupQueue) >= listenerCleanupQueue {
		return
	}
	l.cleanupPending[name] = id
	l.cleanupQueue = append(l.cleanupQueue, name)
	if l.cleanupRunning < listenerCleanupWorkers {
		l.cleanupRunning++
		l.cleanupWG.Add(1)
		go l.cleanupWorker()
	}
}

func (l *Listener) cleanupWorker() {
	defer l.cleanupWG.Done()
	l.mu.Lock()
	for len(l.cleanupQueue) > 0 {
		name := l.cleanupQueue[0]
		id := l.cleanupPending[name]
		l.cleanupQueue[0] = ""
		l.cleanupQueue = l.cleanupQueue[1:]
		l.mu.Unlock()
		if err := l.drop(name, id); err != nil && l.ctx.Err() == nil {
			errors.LogWarningInner(l.ctx, err, "failed to remove announcement ", name)
		}
		l.mu.Lock()
		delete(l.cleanupPending, name)
	}
	l.cleanupRunning--
	l.mu.Unlock()
}

func (l *Listener) drop(name, id string) error {
	ctx := context.WithValue(l.ctx, cleanupKey{}, id)
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := l.storage.Delete(ctx, name)
		if err == nil || err == errSessionInUse || attempt == cleanupMaxRetries {
			return err
		}
		errors.LogDebug(ctx, "failed to drop ", name, ": ", err)
		if err := sleepContext(ctx, cleanupRetryDelay+jitter(cleanupRetryDelay)); err != nil {
			return err
		}
	}
}

func (l *Listener) claim(session string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !safeSessionID(session) {
		return false
	}
	if l.stopping || l.ctx.Err() != nil || l.active[session] || !l.handled[session].IsZero() || l.collecting == session {
		return false
	}
	if l.active == nil {
		l.active = make(map[string]bool)
	}
	if l.handled == nil {
		l.handled = make(map[string]time.Time)
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
	storage, err := acquireSessionStorage(l.storage)
	if err != nil {
		return nil
	}
	releaseSession, err := holdStorageSession(storage, session)
	if err != nil {
		storage.Close()
		return nil
	}
	onClose := func() {
		storage.Close()
		releaseSession()
		l.mu.Lock()
		delete(l.active, session)
		l.mu.Unlock()
	}
	if slots, ok := storage.(slotStore); ok {
		if r, ok := decodeRing(session, announce); ok {
			deadline, _ := admit.Deadline() // acceptPending always supplies the announcement deadline
			if scoped, ok := slots.(interface {
				bindRing(context.Context, ring) error
			}); (ok && scoped.bindRing(admit, r) != nil) || admit.Err() != nil || !time.Now().Before(deadline) {
				onClose()
				return nil
			}
			errors.LogInfo(admit, "accepted session ", session, " in ", len(r.Down.Slots), " slots")
			return newSlotConn(l.ctx, slots, r.Down, r.Up, l.params, onClose)
		}
		onClose()
		return nil
	}
	errors.LogInfo(admit, "accepted session ", session)
	return newConn(l.ctx, storage, downlinkPrefix(session), uplinkPrefix(session), l.params, true, onClose)
}

func (l *Listener) collectLoop(logCtx context.Context) {
	interval := l.sessionTTL / 2
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-time.After(interval):
		}
		if err := l.collect(); err != nil {
			errors.LogWarningInner(logCtx, err, "failed to collect abandoned sessions")
		}
	}
}

func (l *Listener) collect() error {
	if c, ok := l.storage.(interface{ cooling() bool }); ok && c.cooling() {
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
	// Multiple announcement names can refer to one session. Use the latest
	// admission deadline, independent of list or map iteration order.
	announced := make(map[string]time.Time)
	for entry, n := range l.notices {
		session, _, ok := parseAnnounce(entry)
		if expires := n.first.Add(l.sessionTTL); ok && now.Sub(n.last) < 2*l.sessionTTL && expires.After(announced[session]) {
			announced[session] = expires
		}
	}
	present := make(map[string]bool, len(sessions))
	for _, listed := range sessions {
		session := listed.Name
		if !safeSessionID(session) {
			// A listed stream name must never reach an object name.
			continue
		}
		present[session] = true
		if l.active[session] || now.Before(announced[session]) {
			delete(l.idleSince, session)
			continue
		}
		since, seen := l.idleSince[session]
		if !seen {
			l.idleSince[session] = now
			continue
		}
		if now.Sub(since) >= l.sessionTTL {
			expired = append(expired, session)
		}
	}
	maps.DeleteFunc(l.idleSince, func(id string, _ time.Time) bool { return !present[id] })
	unused := func(id string) bool { return !l.active[id] && announced[id].IsZero() }
	maps.DeleteFunc(l.handled, func(id string, at time.Time) bool { return unused(id) && now.Sub(at) >= 2*l.sessionTTL })
	maps.DeleteFunc(l.announceWarned, func(id string, _ bool) bool { return unused(id) && l.handled[id].IsZero() })
	l.mu.Unlock()

	var first error
	for i, session := range expired {
		if i > 0 && l.collectPause > 0 && sleepContext(l.ctx, l.collectPause) != nil {
			return l.ctx.Err()
		}
		// An announcement or accepted connection may have reset the idle timer
		// while an earlier deletion was in flight. Reserve only the next item.
		l.mu.Lock()
		if l.stopping || l.active[session] || l.idleSince[session].IsZero() {
			l.mu.Unlock()
			continue
		}
		l.collecting = session
		l.mu.Unlock()
		err := l.storage.Delete(ctx, sessionPrefix(session))
		l.mu.Lock()
		l.collecting = ""
		switch err {
		case nil:
			delete(l.idleSince, session)
		case errSessionInUse:
			l.idleSince[session] = time.Now()
		default:
			if first == nil {
				first = err
			}
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
