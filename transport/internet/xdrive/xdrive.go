package xdrive

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

const (
	spareCount    = 2
	spareIdleStop = 10 * time.Minute
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
	config, ok := streamSettings.ProtocolSettings.(*Config)
	if !ok || config == nil {
		return nil, errors.New("invalid protocol settings")
	}
	return config, nil
}

type spareRing struct {
	storage Storage
	session string
	ring    ring
	at      time.Time
}

type spareBox struct {
	ch     chan spareRing
	wake   chan struct{}
	done   chan struct{}
	busy   atomic.Bool
	last   atomic.Int64
	ttl    atomic.Int64
	stop   atomic.Bool
	closed atomic.Bool
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	timer  *time.Timer
	key    spareKey
}

type spareKey struct {
	config  *Config
	profile string
}

var spareBoxes sync.Map

func spareBoxFor(config *Config, settings ...*internet.MemoryStreamConfig) *spareBox {
	var streamSettings *internet.MemoryStreamConfig
	if len(settings) > 0 {
		streamSettings = settings[0]
	}
	key := spareKey{config: config, profile: shareKey(streamSettings, config)}
	for {
		if b, ok := spareBoxes.Load(key); ok {
			box := b.(*spareBox)
			if !box.closed.Load() {
				return box
			}
			spareBoxes.CompareAndDelete(key, box)
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		box := &spareBox{
			ch: make(chan spareRing, spareCount), wake: make(chan struct{}, 1), done: make(chan struct{}),
			ctx: ctx, cancel: cancel, key: key,
		}
		box.last.Store(time.Now().UnixNano())
		box.ttl.Store(int64(defaultSessionTTL))
		if _, loaded := spareBoxes.LoadOrStore(key, box); loaded {
			cancel()
			continue
		}
		go box.reap()
		return box
	}
}

func (box *spareBox) reap() {
	defer close(box.done)
	for {
		if box.stop.Load() {
			box.cancel()
			box.mu.Lock()
			if box.timer != nil {
				box.timer.Stop()
				box.timer = nil
			}
			if !box.busy.Load() {
				for len(box.ch) > 0 {
					dropSpare(box.ctx, <-box.ch)
				}
				box.mu.Unlock()
				return
			}
			box.mu.Unlock()
			<-box.wake
			continue
		}
		box.mu.Lock()
		now := time.Now()
		next := spareIdleStop - now.Sub(time.Unix(0, box.last.Load()))
		for n := len(box.ch); n > 0; n-- {
			s := <-box.ch
			remaining := time.Duration(box.ttl.Load())*2/3 - now.Sub(s.at)
			if remaining <= 0 {
				dropSpare(box.ctx, s)
			} else {
				box.ch <- s
				next = min(next, remaining)
			}
		}
		box.mu.Unlock()
		if next <= 0 {
			box.close()
			continue
		}
		timer := time.NewTimer(next)
		select {
		case <-box.ctx.Done():
		case <-box.wake:
		case <-timer.C:
		}
		timer.Stop()
	}
}

func (box *spareBox) signal() {
	select {
	case box.wake <- struct{}{}:
	default:
	}
}

func dropSpare(_ context.Context, s spareRing) {
	s.storage.Close()
}

// Wake the reaper to cancel refill and drain after admitted work exits.
func (box *spareBox) close() {
	if !box.closed.CompareAndSwap(false, true) {
		return
	}
	box.stop.Store(true)
	spareBoxes.CompareAndDelete(box.key, box)
	box.signal()
}

func stopSpares(config *Config) {
	spareBoxes.Range(func(key, value any) bool {
		if key.(spareKey).config == config && spareBoxes.CompareAndDelete(key, value) {
			value.(*spareBox).close()
		}
		return true
	})
}

func stopAllSpares() {
	spareBoxes.Range(func(key, value any) bool {
		if spareBoxes.CompareAndDelete(key, value) {
			value.(*spareBox).close()
		}
		return true
	})
}

func takeSpare(config *Config, p params, settings ...*internet.MemoryStreamConfig) (spareRing, bool) {
	var streamSettings *internet.MemoryStreamConfig
	if len(settings) > 0 {
		streamSettings = settings[0]
	}
	value, found := spareBoxes.Load(spareKey{config: config, profile: shareKey(streamSettings, config)})
	if !found {
		return spareRing{}, false
	}
	box := value.(*spareBox)
	box.mu.Lock()
	defer box.mu.Unlock()
	if box.stop.Load() {
		return spareRing{}, false
	}
	box.last.Store(time.Now().UnixNano())
	box.ttl.Store(int64(p.sessionTTL))
	box.signal()
	for {
		select {
		case s := <-box.ch:
			if time.Since(s.at) < p.sessionTTL*2/3 {
				return s, true
			}
			dropSpare(box.ctx, s)
		default:
			return spareRing{}, false
		}
	}
}

func warmSpares(streamSettings *internet.MemoryStreamConfig, config *Config, p params) {
	box := spareBoxFor(config, streamSettings)
	box.last.Store(time.Now().UnixNano())
	box.ttl.Store(int64(p.sessionTTL))
	box.signal()
	fillSpares(streamSettings, config, p)
}

func fillSpares(streamSettings *internet.MemoryStreamConfig, config *Config, p params) {
	fillSparesBox(spareBoxFor(config, streamSettings), streamSettings, config, p)
}

func fillSparesBox(box *spareBox, streamSettings *internet.MemoryStreamConfig, config *Config, p params) {
	box.mu.Lock()
	if box.stop.Load() || !box.busy.CompareAndSwap(false, true) {
		box.mu.Unlock()
		return
	}
	if box.timer != nil {
		box.timer.Stop()
	}
	box.timer = nil
	box.mu.Unlock()
	go func() {
		defer func() {
			box.busy.Store(false)
			box.signal()
		}()
		box.mu.Lock()
		for n := len(box.ch); n > 0; n-- {
			s := <-box.ch
			if time.Since(s.at) < p.sessionTTL/3 {
				box.ch <- s
			} else {
				dropSpare(box.ctx, s)
			}
		}
		box.mu.Unlock()
		for len(box.ch) < cap(box.ch) {
			select {
			case <-box.ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			box.mu.Lock()
			stopped, full := box.stop.Load(), len(box.ch) == cap(box.ch)
			box.mu.Unlock()
			if stopped {
				return
			}
			if full {
				break
			}
			storage, err := newStorage(streamSettings)
			if err != nil {
				return
			}
			storage, err = transferSessionStorage(storage)
			if err != nil {
				return
			}
			slots, ok := storage.(slotStore)
			if !ok {
				storage.Close()
				return
			}
			session, err := newSessionID()
			if err != nil {
				storage.Close()
				return
			}
			ctx, cancel := context.WithTimeout(box.ctx, 30*time.Second)
			r, err := makeRing(ctx, slots, session, p)
			cancel()
			if err != nil {
				dropSpare(box.ctx, spareRing{storage: storage, session: session})
				return
			}
			box.mu.Lock()
			if box.stop.Load() {
				box.mu.Unlock()
				storage.Close()
				return
			}
			select {
			case box.ch <- spareRing{storage: storage, session: session, ring: r, at: time.Now()}:
				box.signal()
				box.mu.Unlock()
			default:
				box.mu.Unlock()
				storage.Close()
				return
			}
		}
		box.mu.Lock()
		defer box.mu.Unlock()
		if box.stop.Load() {
			return
		}
		box.timer = time.AfterFunc(p.sessionTTL/4, func() {
			if box.stop.Load() {
				return
			}
			if time.Since(time.Unix(0, box.last.Load())) >= spareIdleStop {
				box.close()
				return
			}
			fillSparesBox(box, streamSettings, config, p)
		})
	}()
}

func terminalSessionError(err error) bool {
	return isAuthError(err) || stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded)
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

	p := paramsFromConfig(config)

	var spare spareRing
	var warm bool
	if streamSettings.FinalMask == nil {
		spare, warm = takeSpare(config, p, streamSettings)
	}
	storage, session, r := spare.storage, spare.session, spare.ring
	if !warm {
		storage, err = newStorage(streamSettings)
		if err != nil {
			return nil, err
		}
		storage, err = transferSessionStorage(storage)
		if err != nil {
			return nil, err
		}

		session, err = newSessionID()
		if err != nil {
			storage.Close()
			return nil, err
		}
	}

	p = paramsForStorage(config, storage)

	slots, ok := storage.(slotStore)
	capable, streams := storage.(interface{ configureStreaming(string, bool) })
	if ok && streams {
		capable.configureStreaming(session, false)
	}
	ls, canAllocate := storage.(interface {
		takeIDs(context.Context, int, int) ([]string, error)
	})
	lazy := false
	if ok && !warm {
		if canAllocate {
			depth := max(p.concurrency, 2)
			worth := depth + 1
			ids, e := ls.takeIDs(ctx, 2*worth, worth)
			if e != nil && !terminalSessionError(e) {
				if cooled, ok := storage.(interface{ waitCool(context.Context) error }); ok {
					if e = cooled.waitCool(ctx); e == nil {
						ids, e = ls.takeIDs(ctx, 2*worth, worth)
					}
				}
			}
			if ctx.Err() != nil {
				e = ctx.Err()
			}
			if terminalSessionError(e) {
				storage.Close()
				return nil, e
			}
			if e == nil && len(ids) >= 2*worth {
				r = lazyRing(session, depth, ids)
				lazy = true
			}
		}
		if !lazy {
			if r, err = makeRing(ctx, slots, session, p); err != nil {
				if terminalSessionError(err) {
					storage.Close()
					return nil, err
				}
				ok = false
				errors.LogWarningInner(ctx, err, "failed to make the slots of session ", session, ", it goes in segments")
			}
		}
	}

	if err := ctx.Err(); err != nil {
		storage.Close()
		return nil, err
	}
	if ok {
		if streams {
			r.StreamVersion = 1
			r.StreamPrefix, r.Down.streamPrefix = true, true
		}
		if !canAllocate && !lazy && streamSettings.FinalMask == nil {
			warmSpares(streamSettings, config, p)
		}
		errors.LogInfo(ctx, "opened session ", session, " in ", len(r.Up.Slots), " slots")
		releaseSession, err := holdStorageSession(storage, session)
		if err != nil {
			storage.Close()
			return nil, err
		}
		conn := newSlotConn(context.Background(), slots, r.Up, r.Down, p, nil, func() {
			storage.Close()
			releaseSession()
		})
		writer := conn.writer.(*slotWriter)
		writer.first = make(chan []byte, 1)
		conn.publishWG.Add(1)
		go func() {
			defer conn.publishWG.Done()
			var first []byte
			select {
			case <-conn.ctx.Done():
				return
			case first = <-writer.first:
			case <-time.After(25 * time.Millisecond):
			}
			if conn.ctx.Err() != nil {
				return
			}
			announce, _ := json.Marshal(r)
			if len(first) > 0 && len(first) <= slotInlineLimit {
				withFirst := r
				withFirst.First = first
				if encoded, err := json.Marshal(withFirst); err == nil && len(encoded) <= driveInlineLimit {
					announce = encoded
				}
			}
			// An announce failure surfaces as a connection error on the next Write/Read.
			if err := storage.Put(conn.ctx, announceName(session, time.Now()), announce); err != nil && conn.ctx.Err() == nil {
				err = errors.New("failed to announce session ", session).Base(err)
				writer.fail(err)
				conn.reader.(*slotReader).setErr(err)
			}
		}()
		return conn, nil
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
	collecting     map[string]bool
	announceWarned map[string]bool
	firstSeen      map[string]time.Time
	lastSeen       map[string]time.Time
	stopping       bool
	cleanupPending map[string]bool
	cleanupQueue   []string
	cleanupRunning int
	loopWG         sync.WaitGroup
	cleanupWG      sync.WaitGroup
	closeOnce      sync.Once
	closeErr       error

	// collectPause spaces the deletion of two collected sessions; set in Serve,
	// zero for listeners built directly by tests.
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
		firstSeen:      make(map[string]time.Time),
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
	if l.firstSeen == nil {
		l.firstSeen = make(map[string]time.Time)
	}
	if l.lastSeen == nil {
		l.lastSeen = make(map[string]time.Time)
	}
	now := time.Now()
	listedEntries := make(map[string]bool, len(sessions))
	for _, listed := range sessions {
		listedEntries[listed.Name] = true
		l.lastSeen[listed.Name] = now
		if _, seen := l.firstSeen[listed.Name]; !seen {
			l.firstSeen[listed.Name] = now
		}
	}
	// Keep local age across transient listing omissions; absent entries age
	// out after a bounded grace period rather than an unbounded tombstone.
	for entry := range l.firstSeen {
		if !listedEntries[entry] && now.Sub(l.lastSeen[entry]) >= 2*l.sessionTTL {
			delete(l.firstSeen, entry)
			delete(l.lastSeen, entry)
		}
	}
	l.mu.Unlock()

	for _, listed := range sessions {
		entry := listed.Name
		full := sessionsDir + "/" + entry

		session, _, ok := announceParts(entry)
		if !ok {
			// A name that is not an announcement is stale litter, unless it is
			// unsafe to turn into a delete target.
			if safeListedName(entry) {
				l.queueDrop(full)
			}
			continue
		}
		if !safeSessionID(session) {
			// An unsafe session must never reach an object name; leave the
			// entry alone rather than deleting through it.
			continue
		}
		l.mu.Lock()
		age := now.Sub(l.firstSeen[entry])
		l.mu.Unlock()
		if age > l.sessionTTL {
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
		announce := listed.Inline
		if len(announce) == 0 {
			if _, ok := l.storage.(slotStore); ok {
				announce, err = l.storage.Get(context.WithValue(l.ctx, maxBodyKey{}, int64(driveMaxAnnounce)), full)
				if err != nil {
					l.mu.Lock()
					delete(l.active, session)
					delete(l.handled, session)
					warn := !l.announceWarned[session]
					l.announceWarned[session] = true
					l.mu.Unlock()
					if warn {
						errors.LogWarningInner(logCtx, err, "failed to read the announcement of session ", session)
					}
					continue
				}
			}
		}
		conn := l.newSessionConn(logCtx, session, announce)
		if conn == nil {
			l.mu.Lock()
			delete(l.active, session)
			delete(l.handled, session)
			l.mu.Unlock()
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
	if l.collecting == nil {
		l.collecting = make(map[string]bool)
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
	if l.cleanupPending == nil {
		l.cleanupPending = make(map[string]bool)
	}
	l.cleanupPending[name] = true
	l.cleanupQueue = append(l.cleanupQueue, name)
	if l.cleanupRunning < listenerCleanupWorkers {
		l.cleanupRunning++
		l.cleanupWG.Add(1)
		go l.cleanupWorker()
	}
	return true
}

func (l *Listener) cleanupWorker() {
	defer l.cleanupWG.Done()
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
		if l.idleSince == nil {
			l.idleSince = make(map[string]time.Time)
		}
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
		if err := l.storage.Delete(ctx, name); err == nil {
			return nil
		} else if err == errSessionInUse {
			return err
		} else {
			last = err
			errors.LogDebug(ctx, "failed to drop ", name, ": ", err)
		}
		if attempt+1 < cleanupMaxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cleanupRetryDelay + jitter(cleanupRetryDelay)):
			}
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
	if !safeSessionID(session) {
		return false
	}
	if l.stopping || l.ctx.Err() != nil || l.active[session] || !l.handled[session].IsZero() || l.collecting[session] {
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

// newSessionConn takes a session in the slots that its announcement has, or
// in segments if it has none
func (l *Listener) newSessionConn(logCtx context.Context, session string, announce []byte) *Conn {
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
		if r, ok := decodeRing(announce); ok {
			if capable, ok := storage.(interface{ configureStreaming(string, bool) }); ok {
				capable.configureStreaming(session, r.StreamVersion == 1)
				r.Down.streamPrefix = r.StreamVersion == 1 && r.StreamPrefix
			}
			r.Up.lazy, r.Down.lazy = r.Lazy, r.Lazy
			r.Up.name, r.Down.name = uplinkPrefix(session), downlinkPrefix(session)
			errors.LogInfo(logCtx, "accepted session ", session, " in ", len(r.Down.Slots), " slots")
			return newSlotConn(l.ctx, slots, r.Down, r.Up, l.params, r.First, onClose)
		}
		if len(announce) > 0 {
			storage.Close()
			releaseSession()
			return nil
		}
	}
	errors.LogInfo(logCtx, "accepted session ", session)
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
	// Cleanup yields while live traffic is cooling, so it never piles on.
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
	if l.collecting == nil {
		l.collecting = make(map[string]bool)
	}
	if l.idleSince == nil {
		l.idleSince = make(map[string]time.Time)
	}
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
	for session := range l.idleSince {
		if !present[session] {
			delete(l.idleSince, session)
		}
	}
	announced := make(map[string]bool)
	for entry, seen := range l.lastSeen {
		if session, _, ok := parseAnnounce(entry); ok && now.Sub(seen) < 2*l.sessionTTL {
			announced[session] = true
		}
	}
	for session, at := range l.handled {
		if !l.active[session] && !l.collecting[session] && !announced[session] && now.Sub(at) >= 2*l.sessionTTL {
			delete(l.handled, session)
			delete(l.announceWarned, session)
		}
	}
	for session := range l.announceWarned {
		if !l.active[session] && !l.collecting[session] && !announced[session] && l.handled[session].IsZero() {
			delete(l.announceWarned, session)
		}
	}
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
		if i > 0 && l.collectPause > 0 {
			select {
			case <-l.ctx.Done():
				return l.ctx.Err()
			case <-time.After(l.collectPause):
			}
		}
		if err := l.storage.Delete(ctx, sessionPrefix(session)); err != nil && err != errSessionInUse {
			if first == nil {
				first = err
			}
		} else if err == nil {
			l.mu.Lock()
			delete(l.idleSince, session)
			l.mu.Unlock()
		} else {
			l.mu.Lock()
			l.idleSince[session] = time.Now()
			l.mu.Unlock()
		}
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
