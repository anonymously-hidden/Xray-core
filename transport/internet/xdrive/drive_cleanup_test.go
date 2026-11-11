package xdrive

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// A rate limit answered to a cleanup delete must not cool live traffic: a live
// request issued at the same time stays fast and cooling() remains false.
func TestCleanupRateLimitDoesNotCoolLive(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	storage.cleanupRetryDelay = 300 * time.Millisecond
	ctx := context.Background()

	if err := storage.Put(ctx, "streams/one/c2s/000000000.seg", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	drive.mu.Lock()
	drive.deleteLimit = 1
	drive.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- storage.Delete(ctx, "streams/one") }()

	waitFor(t, "the rate-limited cleanup delete", func() bool {
		drive.mu.Lock()
		defer drive.mu.Unlock()
		return drive.deleteLimited >= 1
	})

	start := time.Now()
	if err := storage.Put(ctx, "sessions/live", nil); err != nil {
		t.Fatalf("live Put: %v", err)
	}
	if took := time.Since(start); took > 200*time.Millisecond {
		t.Fatalf("live Put took %v while cleanup was rate limited; the cooldown leaked", took)
	}
	if storage.cooling() {
		t.Fatal("a cleanup rate limit cooled live traffic")
	}
	if !storage.lastRateLimitAt().IsZero() {
		t.Fatal("a cleanup rate limit set lastRateLimit")
	}
	if !storage.lastThrottleAt().IsZero() {
		t.Fatal("a cleanup rate limit set lastThrottle")
	}
	if err := <-done; err != nil {
		t.Fatalf("cleanup Delete: %v", err)
	}
}

// A rate-limited cleanup delete is retried after a delay and then finishes.
func TestCleanupRetriesRateLimitedDelete(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	storage.cleanupRetryDelay = 60 * time.Millisecond
	ctx := context.Background()

	if err := storage.Put(ctx, "streams/one/c2s/000000000.seg", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	drive.mu.Lock()
	drive.deleteLimit = 1
	drive.mu.Unlock()

	if err := storage.Delete(ctx, "streams/one"); err != nil {
		t.Fatalf("Delete did not survive a cleanup rate limit: %v", err)
	}
	if drive.count() != 0 {
		t.Fatalf("fake drive still holds %d files", drive.count())
	}
	drive.mu.Lock()
	times := append([]time.Time(nil), drive.deleteAt...)
	drive.mu.Unlock()
	if len(times) != 2 {
		t.Fatalf("saw %d delete attempts, want a retry (2)", len(times))
	}
	if gap := times[1].Sub(times[0]); gap < 25*time.Millisecond {
		t.Fatalf("the retry arrived after %v, want a jittered delay", gap)
	}
	if storage.cooling() {
		t.Fatal("the cleanup retry cooled live traffic")
	}
}

// With the rate bucket relaxed, the semaphore still caps deletes in flight at 2.
func TestCleanupDeleteParallelism(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	storage.cleanupRate = 100000
	ctx := context.Background()

	const n = 10
	for i := 0; i < n; i++ {
		if err := storage.Put(ctx, fmt.Sprintf("streams/one/c2s/%09d.seg", i), []byte("x")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	drive.mu.Lock()
	drive.deleteDelay = 20 * time.Millisecond
	drive.deleteAt = nil
	drive.deleteMax = 0
	drive.mu.Unlock()

	if err := storage.Delete(ctx, "streams/one"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	drive.mu.Lock()
	max := drive.deleteMax
	drive.mu.Unlock()
	if max > 2 {
		t.Fatalf("up to %d deletes in flight, want at most 2", max)
	}
	if max < 2 {
		t.Fatalf("deletes never ran two at a time (max %d); the cap is not exercised", max)
	}
	if drive.count() != 0 {
		t.Fatalf("fake drive still holds %d files", drive.count())
	}
}

// At the default rate, deletes are spaced to at most 5 per second.
func TestCleanupDeleteRate(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	const n = 12
	for i := 0; i < n; i++ {
		if err := storage.Put(ctx, fmt.Sprintf("streams/one/c2s/%09d.seg", i), []byte("x")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	drive.mu.Lock()
	drive.deleteAt = nil
	drive.deleteMax = 0
	drive.mu.Unlock()

	start := time.Now()
	if err := storage.Delete(ctx, "streams/one"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	elapsed := time.Since(start)

	drive.mu.Lock()
	times := append([]time.Time(nil), drive.deleteAt...)
	max := drive.deleteMax
	drive.mu.Unlock()
	if len(times) != n {
		t.Fatalf("saw %d deletes, want %d", len(times), n)
	}
	if max > 2 {
		t.Fatalf("up to %d deletes in flight, want at most 2", max)
	}
	if floor := time.Duration(n-1) * (time.Second / 5); elapsed < floor-30*time.Millisecond {
		t.Fatalf("%d deletes took %v, faster than the 5/s floor %v", n, elapsed, floor)
	}
	// any six consecutive deletes span at least ~1s, i.e. no more than 5/s
	for i := 5; i < len(times); i++ {
		if gap := times[i].Sub(times[i-5]); gap < 900*time.Millisecond {
			t.Fatalf("six deletes spanned %v, more than 5 per second", gap)
		}
	}
}

// Cleanup honours an active cooldown caused by live traffic before it is sent.
func TestCleanupWaitsWhileCooling(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	if err := storage.Put(ctx, "streams/one/c2s/000000000.seg", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	storage.coolMu.Lock()
	storage.coolUntil = time.Now().Add(150 * time.Millisecond)
	storage.coolMu.Unlock()

	start := time.Now()
	if err := storage.Delete(ctx, "streams/one"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if took := time.Since(start); took < 100*time.Millisecond {
		t.Fatalf("cleanup ran after %v despite an active cooldown", took)
	}
	if drive.count() != 0 {
		t.Fatalf("fake drive still holds %d files", drive.count())
	}
}

// A delete that keeps being rate limited is reported after 5 tries, not forever.
func TestCleanupDeleteGivesUpAfterFiveTries(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	storage.cleanupRetryDelay = 10 * time.Millisecond
	ctx := context.Background()

	if err := storage.Put(ctx, "streams/one/c2s/000000000.seg", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	drive.mu.Lock()
	drive.deleteLimit = 1000
	drive.deleteAt = nil
	drive.mu.Unlock()

	err := storage.Delete(ctx, "streams/one")
	if err == nil {
		t.Fatal("Delete succeeded despite a permanent cleanup rate limit")
	}
	drive.mu.Lock()
	n := len(drive.deleteAt)
	drive.mu.Unlock()
	if n != 5 {
		t.Fatalf("made %d delete attempts, want 5", n)
	}
	if storage.cooling() {
		t.Fatal("a permanent cleanup rate limit cooled live traffic")
	}
}
