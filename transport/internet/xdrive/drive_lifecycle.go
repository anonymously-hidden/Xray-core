package xdrive

import (
	"context"
	"time"
)

// beginWork registers shared work before it is launched. Shutdown excludes new
// registrations under workMu, then cancels and joins without holding any lock.
// A session/caller cancellation must not cancel work shared by other sessions.
func (s *driveShared) beginWork(timeout time.Duration) (context.Context, func(), bool) {
	s.workMu.Lock()
	defer s.workMu.Unlock()
	if s.workClosed {
		return nil, nil, false
	}
	if s.workCtx == nil {
		s.workCtx, s.workCancel = context.WithCancel(context.Background())
	}
	ctx := context.WithValue(s.workCtx, keeperKey{}, true)
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	s.workWG.Add(1)
	return ctx, func() { cancel(); s.workWG.Done() }, true
}

// beginRequest keeps caller values/deadlines while binding foreground work to
// the terminal owner lifetime. Queued token, cooldown and permit waits count too.
func (s *driveShared) beginRequest(parent context.Context) (context.Context, func(), bool) {
	s.workMu.Lock()
	defer s.workMu.Unlock()
	if s.workClosed {
		return nil, nil, false
	}
	if s.workCtx == nil {
		s.workCtx, s.workCancel = context.WithCancel(context.Background())
	}
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(s.workCtx, cancel)
	s.workWG.Add(1)
	return ctx, func() { stop(); cancel(); s.workWG.Done() }, true
}

func (s *driveShared) cancelWork() {
	s.workMu.Lock()
	s.workClosed = true
	if s.workCancel != nil {
		s.workCancel()
	}
	s.workMu.Unlock()
}

func (s *driveShared) closeWork() {
	s.cancelWork()
	s.workWG.Wait()
}

func (s *driveShared) isWorkClosed() bool {
	s.workMu.Lock()
	defer s.workMu.Unlock()
	return s.workClosed
}
