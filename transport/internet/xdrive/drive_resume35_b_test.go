package xdrive

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestDriveResume35MultipartAndSharedCreates(t *testing.T) {
	data := append(bytes.Repeat([]byte{'x'}, driveInlineLimit+1), []byte("\r\n--xdrive-boundary--\r\nretained-suffix")...)
	var boundaries []string
	s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.Query().Get("supportsAllDrives") != "true" {
			t.Errorf("shared-drive support omitted: %s", r.URL)
		}
		if r.Method == http.MethodPost || r.Method == http.MethodPatch {
			mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil {
				t.Fatal(err)
			}
			if mediaType == "multipart/related" {
				boundary := params["boundary"]
				if len(boundary) < 48 || len(boundary) > 70 || bytes.Contains(data, []byte(boundary)) {
					t.Fatalf("unsafe boundary %q", boundary)
				}
				for _, previous := range boundaries {
					if previous == boundary {
						t.Fatal("multipart boundary reused")
					}
				}
				boundaries = append(boundaries, boundary)
				reader := multipart.NewReader(r.Body, boundary)
				part, err := reader.NextPart()
				if err != nil {
					t.Fatal(err)
				}
				metadata, err := io.ReadAll(part)
				if err != nil || !json.Valid(metadata) {
					t.Fatalf("metadata=%q err=%v", metadata, err)
				}
				part, err = reader.NextPart()
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := io.ReadAll(part)
				if err != nil || !bytes.Equal(decoded, data) {
					t.Fatalf("multipart truncated: %d/%d, %v", len(decoded), len(data), err)
				}
				if _, err := reader.NextPart(); err != io.EOF {
					t.Fatalf("unexpected third part: %v", err)
				}
				if r.Method == http.MethodPatch && classifyDriveRequest(r.Context(), r.Method, r.URL.String(), r.Header.Get("Content-Type"), metadata) != kindSlotMedia {
					t.Fatal("dynamic-boundary media PATCH misclassified")
				}
			}
		}
		return memoryResponse(http.StatusOK, `{"id":"returned"}`), nil
	}))
	defer s.Close()
	for _, operation := range []func() error{
		func() error { return s.Put(context.Background(), "inline", []byte("small")) },
		func() error { return s.Put(context.Background(), "media", data) },
		func() error { return s.createSlot(context.Background(), "slot-inline", "slots/inline", "frame", nil) },
		func() error { return s.createSlot(context.Background(), "slot-media", "slots/media", "frame", data) },
		func() error { return s.setSlot(context.Background(), "slot-media", "frame", data) },
	} {
		if err := operation(); err != nil {
			t.Fatal(err)
		}
	}
	if len(boundaries) != 3 {
		t.Fatalf("tested %d multipart callers, want 3", len(boundaries))
	}
	if classifyDriveRequest(context.Background(), http.MethodPatch, "", `Multipart/Related; boundary="any-boundary"`, nil) != kindSlotMedia {
		t.Fatal("quoted MIME boundary misclassified")
	}
}

func TestDriveResume35OAuthRecoveryKeepsAPIBudget(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		var tokens, api atomic.Int32
		s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/token" {
				if tokens.Add(1) == 1 {
					return memoryResponse(http.StatusServiceUnavailable, `{"error":"temporarily_unavailable"}`), nil
				}
				return memoryResponse(http.StatusOK, `{"access_token":"recovered","expires_in":3600}`), nil
			}
			if api.Add(1) < driveMaxAttempts {
				return memoryResponse(http.StatusServiceUnavailable, "retry"), nil
			}
			return memoryResponse(http.StatusOK, "ok"), nil
		}))
		s.token = ""
		defer s.Close()
		start := time.Now()
		status, body, err := s.do(context.Background(), http.MethodGet, s.filesURL, "", nil)
		if err != nil || status != http.StatusOK || string(body) != "ok" || tokens.Load() != 2 || api.Load() != driveMaxAttempts {
			t.Fatalf("status=%d body=%q tokens=%d API=%d err=%v", status, body, tokens.Load(), api.Load(), err)
		}
		if time.Since(start) < tokenRefreshBackoff {
			t.Fatal("transient OAuth prohibition bypassed")
		}
	})
}

func TestDriveResume35OAuthWaitCancellationAndBound(t *testing.T) {
	for _, mode := range []string{"cancel", "root-close", "bounded", "cached-recovery", "eligibility-deadline"} {
		t.Run(mode, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				var tokens, api atomic.Int32
				s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path != "/token" {
						api.Add(1)
						return memoryResponse(http.StatusOK, "ok"), nil
					}
					tokens.Add(1)
					if mode == "cached-recovery" {
						return memoryResponse(http.StatusOK, `{"access_token":"recovered","expires_in":3600}`), nil
					}
					return memoryResponse(http.StatusServiceUnavailable, `{"error":"temporarily_unavailable"}`), nil
				}))
				s.token = ""
				defer s.Close()
				if mode == "cached-recovery" || mode == "eligibility-deadline" {
					s.lastTokenErr = fmt.Errorf("cached transient failure")
					delay := tokenRefreshBackoff
					if mode == "eligibility-deadline" {
						delay = time.Hour
					}
					s.tokenRetryAt = time.Now().Add(delay)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				start := time.Now()
				done := make(chan error, 1)
				go func() { _, _, err := s.do(ctx, http.MethodGet, s.filesURL, "", nil); done <- err }()
				synctest.Wait()
				if mode == "cancel" {
					cancel()
				} else if mode == "root-close" {
					s.Close()
				}
				err := <-done
				switch mode {
				case "cancel", "root-close":
					if err != context.Canceled || time.Since(start) != 0 || tokens.Load() != 1 || api.Load() != 0 {
						t.Fatalf("err=%v elapsed=%v tokens=%d API=%d", err, time.Since(start), tokens.Load(), api.Load())
					}
				case "bounded":
					if err == nil || tokens.Load() != driveAuthMaxAttempts || api.Load() != 0 || time.Since(start) > driveAuthBudget {
						t.Fatalf("unbounded authentication: err=%v elapsed=%v tokens=%d API=%d", err, time.Since(start), tokens.Load(), api.Load())
					}
				case "cached-recovery":
					if err != nil || tokens.Load() != 1 || api.Load() != 1 || time.Since(start) != tokenRefreshBackoff {
						t.Fatalf("cached eligibility: err=%v elapsed=%v tokens=%d API=%d", err, time.Since(start), tokens.Load(), api.Load())
					}
				case "eligibility-deadline":
					if err != context.DeadlineExceeded || tokens.Load() != 0 || api.Load() != 0 || time.Since(start) != driveAuthBudget {
						t.Fatalf("eligibility exceeded authentication bound: err=%v elapsed=%v tokens=%d API=%d", err, time.Since(start), tokens.Load(), api.Load())
					}
				}
			})
		})
	}
}

func TestDriveResume35CooldownAfterQueuedWaits(t *testing.T) {
	for _, wait := range []string{"permit", "token"} {
		t.Run(wait, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				var api atomic.Int32
				releaseToken := make(chan struct{})
				s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == "/token" {
						select {
						case <-releaseToken:
							return memoryResponse(http.StatusOK, `{"access_token":"fresh","expires_in":3600}`), nil
						case <-r.Context().Done():
							return nil, r.Context().Err()
						}
					}
					api.Add(1)
					return memoryResponse(http.StatusOK, "ok"), nil
				}))
				defer s.Close()
				s.inflight = make(chan struct{}, 1)
				if wait == "permit" {
					s.inflight <- struct{}{}
				} else {
					s.token = ""
				}
				done := make(chan error, 1)
				go func() { _, _, err := s.do(context.Background(), http.MethodGet, s.filesURL, "", nil); done <- err }()
				synctest.Wait()
				s.coolMu.Lock()
				s.coolUntil = time.Now().Add(time.Minute)
				s.coolMu.Unlock()
				start := time.Now()
				if wait == "permit" {
					<-s.inflight
				} else {
					close(releaseToken)
				}
				synctest.Wait()
				if api.Load() != 0 {
					t.Fatal("queued request bypassed newly established cooldown")
				}
				if err := <-done; err != nil || time.Since(start) != time.Minute || api.Load() != 1 {
					t.Fatalf("err=%v elapsed=%v API=%d", err, time.Since(start), api.Load())
				}
			})
		})
	}
}

func TestDriveResume35CleanupRetryPacing(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		var at []time.Time
		s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			at = append(at, time.Now())
			if len(at) < driveMaxAttempts {
				return memoryResponse(http.StatusServiceUnavailable, "retry"), nil
			}
			return memoryResponse(http.StatusNoContent, ""), nil
		}))
		s.cleanupRate = 1
		defer s.Close()
		if err := s.deleteChild(context.Background(), "child", "id"); err != nil {
			t.Fatal(err)
		}
		if len(at) != driveMaxAttempts {
			t.Fatalf("attempts=%d", len(at))
		}
		for i := 1; i < len(at); i++ {
			if at[i].Sub(at[i-1]) < time.Second {
				t.Fatalf("retry %d bypassed cleanup pacing: %v", i, at[i].Sub(at[i-1]))
			}
		}
	})
}

func TestDriveResume35CleanupGloballyBoundsListingsAndDeletes(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		var active, peak atomic.Int32
		release := make(chan struct{})
		s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			n := active.Add(1)
			defer active.Add(-1)
			for previous := peak.Load(); n > previous && !peak.CompareAndSwap(previous, n); previous = peak.Load() {
			}
			select {
			case <-release:
				if r.Method == http.MethodGet {
					return memoryResponse(http.StatusOK, `{"files":[]}`), nil
				}
				return memoryResponse(http.StatusNoContent, ""), nil
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}))
		s.cleanupParallel, s.cleanupRate = 2, 1000000
		s.conns = newConnPool(nil, func(context.Context) *http.Client { return s.client })
		s.bulkConns = s.conns
		defer s.Close()
		var wg sync.WaitGroup
		errors := make(chan error, 10)
		for i := range 10 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				session := s.session().(*driveStorage)
				defer session.Close()
				ctx := context.WithValue(context.Background(), cleanupKey{}, true)
				if i%2 == 0 {
					_, err := session.query(ctx, "name contains 'sessions~a~'")
					errors <- err
				} else {
					errors <- session.deleteChild(ctx, "sessions~a~child", fmt.Sprint(i))
				}
			}(i)
		}
		synctest.Wait()
		time.Sleep(time.Millisecond)
		synctest.Wait()
		if active.Load() != 2 || peak.Load() != 2 {
			t.Fatalf("cleanup concurrency active=%d peak=%d", active.Load(), peak.Load())
		}
		close(release)
		wg.Wait()
		close(errors)
		for err := range errors {
			if err != nil {
				t.Fatal(err)
			}
		}
		if peak.Load() != 2 {
			t.Fatalf("cleanup concurrency exceeded global limit: %d", peak.Load())
		}
	})
}

func TestDriveResume35KnownCreatedIDsSurviveListingLag(t *testing.T) {
	driveClock(t, func(t *testing.T) {
		var mu sync.Mutex
		deleted := make(map[string]int)
		var creates int
		s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
			mu.Lock()
			defer mu.Unlock()
			switch r.Method {
			case http.MethodPost:
				creates++
				return memoryResponse(http.StatusOK, fmt.Sprintf(`{"id":"returned-%d"}`, creates)), nil
			case http.MethodDelete:
				deleted[strings.TrimPrefix(r.URL.Path, "/files/")]++
				return memoryResponse(http.StatusNoContent, ""), nil
			case http.MethodGet:
				return memoryResponse(http.StatusOK, `{"files":[]}`), nil
			}
			return memoryResponse(http.StatusOK, `{}`), nil
		}))
		defer s.Close()
		ctx := context.Background()
		if err := s.Put(ctx, "sessions/a", []byte("inline")); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(ctx, "sessions/a", make([]byte, driveInlineLimit+1)); err != nil {
			t.Fatal(err)
		}
		if err := s.createSlot(ctx, "generated", "sessions/a/up/s0", "frame", nil); err != nil {
			t.Fatal(err)
		}
		if err := s.createSlot(ctx, "generated", "sessions/a/up/s0", "frame", nil); err != nil {
			t.Fatal(err)
		}
		s.rememberID("sessions~ab~untouched", "unrelated", nil)
		ids, err := s.resolveIDs(ctx, "sessions~a")
		if err != nil || len(ids) != 2 {
			t.Fatalf("lagging exact listing erased creates: ids=%v err=%v", ids, err)
		}
		if err := s.Delete(ctx, "sessions/a"); err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"returned-1", "returned-2", "returned-3", "returned-4", "generated"} {
			if deleted[id] != 1 {
				t.Fatalf("identity %s deleted %d times", id, deleted[id])
			}
		}
		if deleted["unrelated"] != 0 || len(s.cachedFiles("sessions~a", true)) != 0 {
			t.Fatal("wrong prefix deleted or confirmed identities not retired")
		}
	})
}

func TestDriveResume35PartialListingsRemainUsable(t *testing.T) {
	for _, operation := range []string{"query", "list", "delete"} {
		t.Run(operation, func(t *testing.T) {
			driveClock(t, func(t *testing.T) {
				var mu sync.Mutex
				var deleted []string
				s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
					mu.Lock()
					defer mu.Unlock()
					if r.Method == http.MethodDelete {
						deleted = append(deleted, strings.TrimPrefix(r.URL.Path, "/files/"))
						return memoryResponse(http.StatusNoContent, ""), nil
					}
					if r.URL.Query().Get("pageToken") != "" {
						return memoryResponse(http.StatusBadRequest, "page failed"), nil
					}
					return memoryResponse(http.StatusOK, `{"nextPageToken":"second","files":[{"id":"listed","name":"sessions~a~listed","description":"`+base64.StdEncoding.EncodeToString([]byte("inline"))+`"},{"id":"listed","name":"sessions~a~listed"},{"id":"unrelated","name":"other~sessions~a~suffix"}]}`), nil
				}))
				defer s.Close()
				s.rememberID("sessions~a~cached", "cached", nil)
				switch operation {
				case "query":
					found, err := s.query(context.Background(), "name contains 'sessions~a~'")
					if err == nil || found["sessions~a~listed"].id != "listed" || len(found["sessions~a~listed"].extra) != 0 {
						t.Fatalf("partial snapshot lost or duplicated: %v %v", found, err)
					}
					if id, ok := s.cachedID("sessions~a~listed"); !ok || id != "listed" {
						t.Fatal("first page not cached before later failure")
					}
				case "list":
					entries, err := s.List(context.Background(), "sessions/a")
					if err == nil || len(entries) != 1 || entries[0].Name != "listed" || string(entries[0].Inline) != "inline" {
						t.Fatalf("partial List lost: %v %v", entries, err)
					}
				case "delete":
					if err := s.Delete(context.Background(), "sessions/a"); err == nil {
						t.Fatal("partial listing failure hidden")
					}
					if len(deleted) != 2 || len(s.cachedFiles("sessions~a", true)) != 0 {
						t.Fatalf("known prefix snapshot not cleaned: %v", deleted)
					}
					for _, id := range deleted {
						if id == "unrelated" {
							t.Fatal("contains-match outside exact descendant boundary deleted")
						}
					}
				}
			})
		})
	}
}

func TestDriveResume35NotFoundRetiresOnlyConfirmedID(t *testing.T) {
	for _, operation := range []string{"delete", "get"} {
		t.Run(operation, func(t *testing.T) {
			s := memoryDrive(roundTrip(func(*http.Request) (*http.Response, error) {
				return memoryResponse(http.StatusNotFound, "missing"), nil
			}))
			defer s.Close()
			s.rememberID("name", "missing", []string{"retained"})
			if operation == "delete" {
				if err := s.deleteID(context.Background(), "name", "missing"); err != nil {
					t.Fatal(err)
				}
			} else if _, err := s.Get(context.Background(), "name"); err != errNotFound {
				t.Fatal(err)
			}
			if id, ok := s.cachedID("name"); !ok || id != "retained" || len(s.dupIDs["name"]) != 0 {
				t.Fatalf("404 retired an unconfirmed identity: %v", s.cachedFiles("name", false))
			}
		})
	}
}

func TestDriveResume35FailedDeletionKeepsOtherIDs(t *testing.T) {
	s := memoryDrive(roundTrip(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/second") {
			return memoryResponse(http.StatusForbidden, "no permission"), nil
		}
		return memoryResponse(http.StatusNoContent, ""), nil
	}))
	defer s.Close()
	s.rememberID("name", "first", []string{"second", "second", "first"})
	if err := s.deleteID(context.Background(), "name", "first"); err != nil {
		t.Fatal(err)
	}
	if err := s.deleteID(context.Background(), "name", "second"); err == nil {
		t.Fatal("failed delete not returned")
	}
	if id, ok := s.cachedID("name"); !ok || id != "second" || len(s.dupIDs["name"]) != 0 {
		t.Fatalf("failed identity forgotten or confirmed identity retained: %v", s.cachedFiles("name", false))
	}
}
