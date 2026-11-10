package xdrive

import (
	"context"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// noteTraffic records Drive activity so the keeper knows the tunnel is in use.
func (s *driveShared) noteTraffic() {
	s.trafficMu.Lock()
	now := time.Now()
	s.lastTraffic = now
	s.lastRequest = now
	s.trafficMu.Unlock()
}

// startKeeper launches the keep-warm goroutine once, on the first session/Dial.
func (s *driveShared) startKeeper() {
	s.keeperOnce.Do(func() {
		if s.keepWarmEvery <= 0 {
			s.keepWarmEvery = 4 * time.Minute
		}
		if s.keepWarmWindow <= 0 {
			s.keepWarmWindow = 3 * time.Hour
		}
		s.keeperStop = make(chan struct{})
		s.keeperDone = make(chan struct{})
		go s.keepWarm()
	})
}

// stopKeeper stops the keeper and waits for it to exit (no leak).
func (s *driveShared) stopKeeper() {
	s.keeperStopOnce.Do(func() {
		if s.keeperStop != nil {
			close(s.keeperStop)
			<-s.keeperDone
		}
	})
}

// keepWarm keeps the token, the id pool and the lane connection warm while the
// tunnel is in use. At most one request per tick; failures are Debug-only and
// never affect sessions. idPoolTTL is unchanged; the keeper just keeps fresh
// ids in the pool so takeIDs finds them.
func (s *driveShared) keepWarm() {
	defer close(s.keeperDone)
	t := time.NewTicker(s.keepWarmEvery)
	defer t.Stop()
	for {
		select {
		case <-s.keeperStop:
			return
		case <-t.C:
		}
		s.trafficMu.Lock()
		lastTraffic, lastRequest := s.lastTraffic, s.lastRequest
		s.trafficMu.Unlock()
		if time.Since(lastTraffic) > s.keepWarmWindow {
			return
		}
		o := s.owner
		if o == nil || o.cooling() || time.Since(o.lastRateLimitAt()) <= 5*time.Second {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		o.tokenMu.Lock()
		tokenBefore := o.token
		o.tokenMu.Unlock()
		tok, err := o.accessToken(ctx)
		if err != nil {
			errors.LogDebug(ctx, "xdrive keep-warm token: ", err)
		}
		tokenFetched := tok != tokenBefore
		if !tokenFetched && time.Since(lastRequest) > 2*time.Minute {
			s.poolMu.Lock()
			low := len(s.idPool) < 2*(64+1)
			old := len(s.idPool) > 0 && time.Since(s.idPool[0].at) > 5*time.Minute
			s.poolMu.Unlock()
			if low || old {
				if ids, err := o.generateIDs(ctx, 2*(64+1)); err != nil {
					errors.LogDebug(ctx, "xdrive keep-warm ids: ", err)
				} else {
					now := time.Now()
					s.poolMu.Lock()
					for _, id := range ids {
						s.idPool = append(s.idPool, idPoolEntry{id: id, at: now})
					}
					s.poolMu.Unlock()
				}
			}
		}
		cancel()
	}
}
