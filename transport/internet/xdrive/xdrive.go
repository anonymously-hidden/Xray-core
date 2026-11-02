package xdrive

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
		return nil, errors.New("invalid protocol settings")
	}
	config, ok := streamSettings.ProtocolSettings.(*Config)
	if !ok || config == nil {
		return nil, errors.New("invalid protocol settings")
	}
	return config, nil
}

type spareRing struct {
	storage Storage
	session string
	table   [2]slotTable
	at      time.Time
}

type spareBox struct {
	ch   chan spareRing
	busy atomic.Bool
	last atomic.Int64
}

var spareBoxes sync.Map

func spareBoxFor(config *Config) *spareBox {
	if b, ok := spareBoxes.Load(config); ok {
		return b.(*spareBox)
	}
	b, _ := spareBoxes.LoadOrStore(config, &spareBox{ch: make(chan spareRing, spareCount)})
	return b.(*spareBox)
}

func takeSpare(config *Config, p params) (spareRing, bool) {
	box := spareBoxFor(config)
	for {
		select {
		case s := <-box.ch:
			if time.Since(s.at) < p.sessionTTL*2/3 {
				return s, true
			}
			s.storage.Close()
		default:
			return spareRing{}, false
		}
	}
}

func warmSpares(streamSettings *internet.MemoryStreamConfig, config *Config, p params) {
	spareBoxFor(config).last.Store(time.Now().UnixNano())
	fillSpares(streamSettings, config, p)
}

func fillSpares(streamSettings *internet.MemoryStreamConfig, config *Config, p params) {
	box := spareBoxFor(config)
	if !box.busy.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer box.busy.Store(false)
		for n := len(box.ch); n > 0; n-- {
			select {
			case s := <-box.ch:
				if time.Since(s.at) < p.sessionTTL/3 {
					box.ch <- s
				} else {
					s.storage.Close()
				}
			default:
			}
		}
		for len(box.ch) < cap(box.ch) {
			time.Sleep(2 * time.Second)
			storage, err := newStorage(streamSettings)
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
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			table, err := buildSlotTable(ctx, slots, session, p)
			cancel()
			if err != nil {
				storage.Close()
				return
			}
			select {
			case box.ch <- spareRing{storage: storage, session: session, table: table, at: time.Now()}:
			default:
				storage.Close()
				return
			}
		}
		time.AfterFunc(p.sessionTTL/4, func() {
			if time.Since(time.Unix(0, box.last.Load())) < spareIdleStop {
				fillSpares(streamSettings, config, p)
			}
		})
	}()
}

func Dial(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (stat.Connection, error) {
	config, err := streamConfig(streamSettings)
	if err != nil {
		return nil, err
	}

	p := paramsFromConfig(config)

	spare, warm := takeSpare(config, p)
	storage, session := spare.storage, spare.session
	if !warm {
		storage, err = newStorage(streamSettings)
		if err != nil {
			return nil, err
		}
		session, err = newSessionID()
		if err != nil {
			storage.Close()
			return nil, err
		}
	}

	if slots, ok := storage.(slotStore); ok {
		table := spare.table
		var built error
		if !warm {
			table, built = buildSlotTable(ctx, slots, session, p)
		}
		if built == nil {
			announce := encodeAnnounce(slotAnnounce{Uplink: table[0], Downlink: table[1]})
			if err := storage.Put(ctx, announceName(session, time.Now()), []byte(announce)); err == nil {
				warmSpares(streamSettings, config, p)
				errors.LogInfo(ctx, "opened session ", session, " on a slot ring of depth ",
					len(table[0].Slots))
				return newSlotConn(context.Background(), slots, slotAnnounce{
					Uplink: table[0], Downlink: table[1],
				}, p, func() { storage.Close() }), nil
			} else {
				errors.LogWarning(ctx, "failed to announce the session on slots, ",
					"falling back to the segment protocol: ", err)
			}
		} else {
			errors.LogWarning(ctx, "failed to build the slot ring, falling back to the ",
				"segment protocol: ", built)
		}
	}

	if err := storage.Put(ctx, announceName(session, time.Now()), nil); err != nil {
		storage.Close()
		return nil, errors.New("failed to announce session ", session).Base(err)
	}

	errors.LogInfo(ctx, "opened session ", session)

	return newConn(context.Background(), storage,
		uplinkPrefix(session), downlinkPrefix(session), p, func() {
			storage.Close()
		}), nil
}

func buildSlotTable(ctx context.Context, store slotStore, session string, p params) ([2]slotTable, error) {
	depth := p.concurrency
	if depth > slotDepthLimit {
		depth = slotDepthLimit
	}
	if depth < 2 {
		depth = 2
	}

	var out [2]slotTable
	for i := range out {
		out[i] = slotTable{Slots: make([]string, depth)}
	}
	dirs := [2]string{uplinkDir, downlinkDir}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		fail error
	)
	create := func(name string, set func(id string)) {
		defer wg.Done()
		id, err := store.makeSlot(ctx, name)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			if fail == nil {
				fail = err
			}
			return
		}
		set(id)
	}
	for d := range dirs {
		for i := 0; i < depth; i++ {
			d, i := d, i
			wg.Add(1)
			go create(sessionPrefix(session)+"/"+dirs[d]+"/s"+strconv.Itoa(i), func(id string) {
				out[d].Slots[i] = id
			})
		}
		d := d
		wg.Add(1)
		go create(sessionPrefix(session)+"/"+dirs[d]+"/w", func(id string) {
			out[d].Watermark = id
		})
	}
	wg.Wait()
	if fail != nil {
		return out, fail
	}
	return out, nil
}

type Listener struct {
	ctx            context.Context
	cancel         context.CancelFunc
	storage        Storage
	streamSettings *internet.MemoryStreamConfig
	addConn        internet.ConnHandler
	params

	mu        sync.Mutex
	active    map[string]bool
	handled   map[string]time.Time
	idleSince map[string]time.Time
	warm      Storage
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
		streamSettings: streamSettings,
		addConn:        addConn,
		params:         paramsFromConfig(config),
		active:         make(map[string]bool),
		handled:        make(map[string]time.Time),
		idleSince:      make(map[string]time.Time),
	}

	go listener.acceptLoop(ctx)
	go listener.collectLoop(ctx)
	go listener.refillWarm()

	return listener, nil
}

func (l *Listener) acceptLoop(logCtx context.Context) {
	delay := l.minPollInterval
	active := time.Now()
	for {
		accepted, err := l.acceptPending(logCtx)
		if err != nil {
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
	for _, listed := range sessions {
		entry := listed.Name
		full := sessionsDir + "/" + entry

		session, at, ok := parseAnnounce(entry)
		if !ok {
			go l.drop(full)
			continue
		}
		if time.Since(at) > l.sessionTTL {
			errors.LogInfo(logCtx, "dropping the stale announcement of session ", session)
			go l.drop(full)
			go l.drop(sessionPrefix(session))
			continue
		}
		if !l.claim(session) {
			continue
		}
		go l.drop(full)

		if listed.Inline != nil {
			if table, ok := decodeAnnounce(string(listed.Inline)); ok {
				errors.LogInfo(logCtx, "accepted session ", session, " on a slot ring")
				l.addConn(l.newSlotSessionConn(session, table))
				accepted = true
				continue
			}
		}

		errors.LogInfo(logCtx, "accepted session ", session)
		accepted = true
		l.addConn(l.newSessionConn(session))
	}
	return accepted, nil
}

func (l *Listener) drop(name string) {
	l.storage.Delete(l.ctx, name)
}

func (l *Listener) claim(session string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active[session] || !l.handled[session].IsZero() {
		return false
	}
	l.active[session] = true
	l.handled[session] = time.Now()
	return true
}

func (l *Listener) newSessionConn(session string) *Conn {
	storage := l.storage
	if s, err := newStorage(l.streamSettings); err == nil {
		storage = s
	}
	return newConn(l.ctx, storage,
		downlinkPrefix(session), uplinkPrefix(session), l.params, func() {
			if storage != l.storage {
				storage.Close()
			}
			l.release(session)
		})
}

func (l *Listener) newSlotSessionConn(session string, table slotAnnounce) *slotConn {
	store, _ := l.storage.(slotStore)
	shared := true
	if s := l.takeWarm(); s != nil {
		if slots, ok := s.(slotStore); ok {
			store, shared = slots, false
		} else {
			s.Close()
		}
	}
	return newSlotConn(l.ctx, store, slotAnnounce{
		Uplink: table.Downlink, Downlink: table.Uplink,
	}, l.params, func() {
		if !shared && store != nil {
			store.Close()
		}
		l.release(session)
	})
}

func (l *Listener) release(session string) {
	l.mu.Lock()
	delete(l.active, session)
	l.mu.Unlock()
}

func (l *Listener) takeWarm() Storage {
	l.mu.Lock()
	s := l.warm
	l.warm = nil
	l.mu.Unlock()
	go l.refillWarm()
	if s != nil {
		return s
	}
	fresh, err := newStorage(l.streamSettings)
	if err != nil {
		return nil
	}
	return fresh
}

func (l *Listener) refillWarm() {
	s, err := newStorage(l.streamSettings)
	if err != nil {
		return
	}
	l.mu.Lock()
	keep := l.warm == nil && l.ctx.Err() == nil
	if keep {
		l.warm = s
	}
	l.mu.Unlock()
	if !keep {
		s.Close()
	}
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
	sessions, err := l.storage.List(l.ctx, streamsDir)
	if err != nil {
		return err
	}

	now := time.Now()
	var expired []string

	l.mu.Lock()
	present := make(map[string]bool, len(sessions))
	for _, listed := range sessions {
		session := listed.Name
		present[session] = true
		if l.active[session] {
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
			delete(l.idleSince, session)
		}
	}
	for session := range l.idleSince {
		if !present[session] {
			delete(l.idleSince, session)
		}
	}
	for session, at := range l.handled {
		if !l.active[session] && now.Sub(at) >= l.sessionTTL {
			delete(l.handled, session)
		}
	}
	l.mu.Unlock()

	for _, session := range expired {
		if err := l.storage.Delete(l.ctx, sessionPrefix(session)); err != nil {
			return err
		}
	}
	return nil
}

func (l *Listener) Addr() net.Addr {
	return placeholderAddr
}

func (l *Listener) Close() error {
	l.cancel()
	l.mu.Lock()
	warm := l.warm
	l.warm = nil
	l.mu.Unlock()
	if warm != nil {
		warm.Close()
	}
	return l.storage.Close()
}
