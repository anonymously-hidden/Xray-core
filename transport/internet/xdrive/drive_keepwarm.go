package xdrive

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

type keeperKey struct{}

// Socket probes are disposable maintenance, unlike token and ID refills.
// A failed probe can wait for the next tick rather than spending API retries.
type keeperProbeKey struct{}

func (s *driveShared) noteTraffic() {
	s.trafficMu.Lock()
	s.lastTraffic = time.Now()
	s.trafficMu.Unlock()
	s.startKeeper()
}

func (s *driveShared) noteRequest(ctx context.Context) {
	s.trafficMu.Lock()
	s.lastRequest = time.Now()
	traffic := ctx.Value(keeperKey{}) == nil
	if traffic {
		s.lastTraffic = s.lastRequest
	}
	s.trafficMu.Unlock()
	if traffic {
		s.startKeeper()
	}
}

func (s *driveShared) startKeeper() {
	s.keeperMu.Lock()
	defer s.keeperMu.Unlock()
	if s.keeperClosed || s.keeperRunning || s.owner == nil {
		return
	}
	if s.keepWarmEvery <= 0 {
		s.keepWarmEvery = 4 * time.Minute
	}
	if s.keepWarmWindow <= 0 {
		s.keepWarmWindow = 3 * time.Hour
	}
	workCtx, finish, ok := s.beginWork(0)
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(workCtx)
	s.keeperCancel = cancel
	s.keeperDone = make(chan struct{})
	s.keeperRunning = true
	done := s.keeperDone
	go func() { defer finish(); defer cancel(); s.keepWarm(ctx, done) }()
}

func (s *driveShared) stopKeeper() {
	s.keeperMu.Lock()
	s.keeperClosed = true
	done := s.keeperDone
	cancel := s.keeperCancel
	s.keeperMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// Warm pooled sockets rather than the owner's separate maintenance connection;
// release the session bindings after each tick so foreground sessions can reuse them.
func (s *driveShared) keeperStorage() *driveStorage {
	warm := &driveStorage{
		driveShared: s,
		client:      s.owner.client,
		inflight:    s.owner.inflight,
		reserved:    s.owner.reserved,
	}
	var transports []*sessionTransport
	if s.conns != nil {
		t := &sessionTransport{pool: s.conns}
		warm.client = &http.Client{Transport: t}
		transports = append(transports, t)
	}
	if s.bulkConns != nil {
		t := &sessionTransport{pool: s.bulkConns}
		warm.bulkClient = &http.Client{Transport: t}
		transports = append(transports, t)
	}
	warm.release = func() {
		for _, t := range transports {
			t.Close()
		}
	}
	return warm
}

func (s *driveStorage) keepWarmConnection(ctx context.Context, target string) error {
	ctx = context.WithValue(ctx, keeperProbeKey{}, true)
	status, _, err := s.do(ctx, http.MethodGet, target, "", nil)
	if err == nil && status != http.StatusOK {
		err = errors.New("Drive keep-warm answered ", status)
	}
	return err
}

func (s *driveShared) keepWarm(runCtx context.Context, done chan struct{}) {
	t := time.NewTicker(s.keepWarmEvery)
	defer t.Stop()
	var warm *driveStorage
	defer func() {
		if warm != nil {
			warm.Close()
		}
	}()
	for {
		select {
		case <-runCtx.Done():
		case <-t.C:
		}
		s.keeperMu.Lock()
		s.trafficMu.Lock()
		lastTraffic, lastRequest := s.lastTraffic, s.lastRequest
		s.trafficMu.Unlock()
		if runCtx.Err() != nil || time.Since(lastTraffic) > s.keepWarmWindow {
			s.keeperRunning = false
			close(done)
			s.keeperMu.Unlock()
			return
		}
		s.keeperMu.Unlock()
		o := s.owner
		if o.cooling() || time.Since(o.lastRateLimitAt()) <= 5*time.Second {
			continue
		}
		ctx, cancel := context.WithTimeout(runCtx, 30*time.Second)
		o.tokenMu.Lock()
		tokenBefore := o.token
		needsToken := tokenBefore == "" || !time.Now().Before(o.tokenExpiry)
		o.tokenMu.Unlock()
		tok, err := o.accessToken(ctx)
		if err != nil {
			errors.LogDebug(ctx, "xdrive keep-warm token: ", err)
		}
		tokenFetched := needsToken || tok != tokenBefore
		if tokenFetched {
			s.noteRequest(ctx)
		}
		s.poolMu.Lock()
		s.prunePoolLocked()
		var flight *idPoolFlight
		var want int
		if err == nil && !tokenFetched && time.Since(lastRequest) > 2*time.Minute {
			low, old := len(s.idPool) < 2*s.poolRing, false
			for _, e := range s.idPool {
				old = old || time.Since(e.at) > 5*time.Minute
			}
			if low || old {
				flight, want = s.beginRefillLocked(old)
			}
		}
		s.poolMu.Unlock()
		idle := err == nil && time.Since(lastRequest) > 2*time.Minute
		if (idle || flight != nil) && warm == nil {
			warm = s.keeperStorage()
		}
		if flight != nil {
			warm.refillIDs(ctx, flight, want)
			if flight.err != nil {
				errors.LogDebug(ctx, "xdrive keep-warm ids: ", flight.err)
			}
		}
		if idle && ctx.Err() == nil && (flight == nil || flight.err == nil) {
			target := s.filesURL + "/" + url.PathEscape(s.folder) + "?fields=id&supportsAllDrives=true"
			if s.conns != nil && flight == nil {
				if err := warm.keepWarmConnection(ctx, target); err != nil {
					errors.LogDebug(ctx, "xdrive keep-warm connection: ", err)
				}
			}
			if s.bulkConns != nil && ctx.Err() == nil {
				if err := warm.keepWarmConnection(context.WithValue(ctx, bodyKey{}, true), target); err != nil {
					errors.LogDebug(ctx, "xdrive keep-warm connection: ", err)
				}
			}
		}
		cancel()
		if warm != nil {
			warm.Close()
			warm = nil
		}
	}
}
