package xdrive

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type adapterResume35Transport func(*http.Request) (*http.Response, error)

func (f adapterResume35Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func adapterResume35Response(status int, body io.ReadCloser) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: body}
}

func adapterResume35Template(transport http.RoundTripper) *templateStorage {
	return &templateStorage{
		templateShared: &templateShared{tmpl: &storageTemplate{
			Flatten: true,
			Put:     opTemplate{Method: http.MethodPut, URL: "https://adapter.invalid/objects/{name}"},
			Get:     opTemplate{URL: "https://adapter.invalid/objects/{name}"},
			Delete:  opTemplate{Method: http.MethodDelete, URL: "https://adapter.invalid/objects/{name}"},
			List:    opTemplate{URL: "https://adapter.invalid/list?prefix={prefix}"},
			names:   regexp.MustCompile(`"name":"([^"]+)"`),
		}},
		client: &http.Client{Transport: transport}, inflight: make(chan struct{}, 1),
	}
}

func adapterResume35S3(transport http.RoundTripper) *s3Storage {
	base, _ := url.Parse("https://adapter.invalid/bucket/")
	return &s3Storage{
		s3Shared: &s3Shared{base: *base, prefix: "root/", region: "us-east-1", accessKey: "offline", secretKey: "offline-secret"},
		client:   &http.Client{Transport: transport}, inflight: make(chan struct{}, 1),
	}
}

func TestAdapterResume35PaginationConfiguration(t *testing.T) {
	config := &Config{Template: `{"put":{"url":"https://adapter.invalid"},"get":{"url":"https://adapter.invalid"},"delete":{"url":"https://adapter.invalid"},"list":{"url":"https://adapter.invalid/list","namesRegex":"(.*)","nextPagePath":"next"}}`}
	if _, err := newTemplateStorage(nil, config); err == nil || !strings.Contains(err.Error(), "needs {pageToken}") {
		t.Fatalf("invalid pagination configuration error=%v", err)
	}
}

func TestAdapterResume35TemplateCommitObject(t *testing.T) {
	store := newFakeStore(t)
	s := newTemplateBackend(t, store, map[string]interface{}{"type": "none"}, nil)
	commit := make([]byte, 48)
	for i := range commit {
		commit[i] = byte(i)
	}
	ctx := context.WithValue(context.Background(), maxBodyKey{}, int64(48))
	name := "streams/abc/c2s/0.seg.commit"
	if err := s.Put(ctx, name, commit); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, name)
	if err != nil || !bytes.Equal(got, commit) {
		t.Fatalf("binary commitment round trip: payload=%x err=%v", got, err)
	}
	if err := s.Delete(ctx, "streams/abc"); err != nil {
		t.Fatal(err)
	}
	if store.count() != 0 {
		t.Fatalf("commitment survived session deletion: %d objects", store.count())
	}
}

func TestAdapterResume35FlatDeletePaginationBoundary(t *testing.T) {
	objects := map[string]bool{
		"streams~abc": true, "streams~abc~c2s~0.seg": true, "streams~abc~c2s~0.seg.commit": true,
		"streams~abc~s2c~0.seg": true, "streams~abcd~c2s~0.seg": true, "streams~abc-other": true,
		"streams~other~c2s~0.seg": true,
	}
	var events []string
	token := "page&+雪"
	transport := adapterResume35Transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/list" {
			if got := r.URL.Query().Get("prefix"); got != "streams~abc~" {
				t.Fatalf("descendant prefix = %q", got)
			}
			page := r.URL.Query().Get("pageToken")
			events = append(events, "list:"+page)
			var names []string
			var next string
			switch page {
			case "":
				names = []string{"streams~abc", "streams~abc~c2s~0.seg", "streams~abcd~c2s~0.seg", "streams~abc-other"}
				next = token
			case token:
				names = []string{"streams~abc~c2s~0.seg", "streams~abc~c2s~0.seg.commit", "streams~abc~s2c~0.seg", "streams~other~c2s~0.seg"}
			default:
				t.Fatalf("unexpected page token %q", page)
			}
			items := make([]map[string]string, 0, len(names))
			for _, name := range names {
				items = append(items, map[string]string{"name": name})
			}
			payload, _ := json.Marshal(map[string]interface{}{"items": items, "next": next})
			return adapterResume35Response(http.StatusOK, io.NopCloser(bytes.NewReader(payload))), nil
		}
		if r.Method != http.MethodDelete {
			t.Fatalf("unexpected method %s", r.Method)
		}
		name := strings.TrimPrefix(r.URL.Path, "/objects/")
		events = append(events, "delete:"+name)
		delete(objects, name)
		return adapterResume35Response(http.StatusNoContent, io.NopCloser(strings.NewReader(""))), nil
	})
	s := adapterResume35Template(transport)
	s.tmpl.List.URL += "&pageToken={pageToken}"
	s.tmpl.List.NextPagePath = "next"
	if err := s.Delete(context.Background(), "streams/abc"); err != nil {
		t.Fatal(err)
	}
	wantEvents := []string{"list:", "list:" + token, "delete:streams~abc~c2s~0.seg", "delete:streams~abc~c2s~0.seg.commit", "delete:streams~abc~s2c~0.seg", "delete:streams~abc"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
	wantObjects := map[string]bool{"streams~abcd~c2s~0.seg": true, "streams~abc-other": true, "streams~other~c2s~0.seg": true}
	if !reflect.DeepEqual(objects, wantObjects) {
		t.Fatalf("remaining objects = %v, want %v", objects, wantObjects)
	}
}

func TestAdapterResume35FlatDeletePaginationCycle(t *testing.T) {
	lists, deletes := 0, 0
	s := adapterResume35Template(adapterResume35Transport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodDelete {
			deletes++
		}
		lists++
		return adapterResume35Response(http.StatusOK, io.NopCloser(strings.NewReader(`{"items":[{"name":"streams~abc~0.seg"}],"next":"same"}`))), nil
	}))
	s.tmpl.List.NextPagePath = "next"
	s.tmpl.List.URL += "&pageToken={pageToken}"
	if err := s.Delete(context.Background(), "streams/abc"); err == nil || !strings.Contains(err.Error(), "pagination did not advance") {
		t.Fatalf("cycle error = %v", err)
	}
	if lists != 2 || deletes != 0 {
		t.Fatalf("lists = %d, deletes = %d", lists, deletes)
	}
}

func TestAdapterResume35FlatDeleteInvalidPagination(t *testing.T) {
	for _, payload := range []string{`{"items":[{"name":"streams~abc~0.seg"}],"next":123}`, `{"items":[{"name":"streams~abc~0.seg"}],`} {
		t.Run(payload, func(t *testing.T) {
			deletes := 0
			s := adapterResume35Template(adapterResume35Transport(func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodDelete {
					deletes++
				}
				return adapterResume35Response(http.StatusOK, io.NopCloser(strings.NewReader(payload))), nil
			}))
			s.tmpl.List.NextPagePath = "next"
			s.tmpl.List.URL += "&pageToken={pageToken}"
			if err := s.Delete(context.Background(), "streams/abc"); err == nil || !isPermanentAdapterError(err) || deletes != 0 {
				t.Fatalf("err=%v deletes=%d", err, deletes)
			}
		})
	}
}

func TestAdapterResume35Pagination404(t *testing.T) {
	for _, operation := range []string{"list", "delete"} {
		for _, mode := range []string{"continuation", "initial-paginated", "initial-unpaginated"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				lists, deletes, closed := 0, 0, 0
				var s *templateStorage
				s = adapterResume35Template(adapterResume35Transport(func(r *http.Request) (*http.Response, error) {
					status, payload := http.StatusNotFound, `{"error":"continuation not found"}`
					if r.Method == http.MethodDelete {
						deletes++
						status, payload = http.StatusNoContent, ""
						if r.URL.Path != "/objects/streams~abc" {
							t.Errorf("unexpected object deletion: %s", r.URL.Path)
						}
					} else {
						lists++
						if mode == "continuation" && lists == 1 {
							status, payload = http.StatusOK, `{"items":[{"name":"streams~abc~c2s~0.seg"}],"next":"remaining-page"}`
						} else if mode == "continuation" && r.URL.Query().Get("pageToken") != "remaining-page" {
							t.Error("continuation request lost its token")
						}
					}
					return adapterResume35Response(status, &adapterResume35CloseReader{Reader: strings.NewReader(payload), close: func() {
						closed++
						if len(s.inflight) != 1 {
							t.Error("404 response Close did not hold admission")
						}
					}}), nil
				}))
				if mode != "initial-unpaginated" {
					s.tmpl.List.NextPagePath = "next"
					s.tmpl.List.URL += "&pageToken={pageToken}"
				}
				var err error
				if operation == "delete" {
					err = s.Delete(context.Background(), "streams/abc")
				} else {
					var entries []Entry
					entries, err = s.List(context.Background(), "streams")
					if mode != "continuation" && len(entries) != 0 {
						t.Errorf("initial 404 returned entries: %v", entries)
					}
				}
				wantLists, wantDeletes := 1, 0
				if mode == "continuation" {
					wantLists = 2
					if err == nil || !strings.Contains(err.Error(), "answered 404") {
						t.Fatalf("continuation 404 was successful exhaustion: %v", err)
					}
				} else {
					if err != nil {
						t.Fatalf("initial 404 compatibility: %v", err)
					}
					if operation == "delete" {
						wantDeletes = 1
					}
				}
				if lists != wantLists || deletes != wantDeletes || closed != lists+deletes || len(s.inflight) != 0 {
					t.Fatalf("lists=%d deletes=%d closed=%d inflight=%d", lists, deletes, closed, len(s.inflight))
				}
			})
		}
	}
}

func TestAdapterResume35ListPaginationDirectChildren(t *testing.T) {
	calls := 0
	s := adapterResume35Template(adapterResume35Transport(func(r *http.Request) (*http.Response, error) {
		calls++
		payload := `{"items":[{"name":"streams~one~c2s~0.seg"}],"next":"two"}`
		if r.URL.Query().Get("pageToken") == "two" {
			payload = `{"items":[{"name":"streams~one~s2c~0.seg"},{"name":"streams~two~c2s~0.seg"},{"name":"streamsx~foreign"}]}`
		}
		return adapterResume35Response(http.StatusOK, io.NopCloser(strings.NewReader(payload))), nil
	}))
	s.tmpl.List.NextPagePath = "next"
	s.tmpl.List.URL += "&pageToken={pageToken}"
	entries, err := s.List(context.Background(), "streams")
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || !reflect.DeepEqual(entries, []Entry{{Name: "one"}, {Name: "two"}}) {
		t.Fatalf("calls = %d, entries = %v", calls, entries)
	}
}

type adapterResume35Body struct {
	remaining int64
	read      int64
	close     func()
}

func (b *adapterResume35Body) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), b.remaining))
	clear(p[:n])
	b.remaining -= int64(n)
	b.read += int64(n)
	return n, nil
}

func (b *adapterResume35Body) Close() error {
	if b.close != nil {
		b.close()
	}
	return nil
}

func TestAdapterResume35ResponseCaps(t *testing.T) {
	for _, backend := range []string{"template", "s3"} {
		t.Run(backend, func(t *testing.T) {
			cases := []struct {
				name    string
				op      string
				status  int
				cap     int64
				body    int64
				context int64
			}{
				{"commit-exact", "get", 200, 48, 48, 48},
				{"commit-oversize", "get", 200, 48, 63, 48},
				{"object-oversize", "get", 200, maxSegmentBytes, maxSegmentBytes + 17, 0},
				{"context-cannot-expand", "get", 200, maxSegmentBytes, maxSegmentBytes + 17, 2 * maxSegmentBytes},
				{"list-larger-than-object", "list", 200, adapterMaxListResponse, maxSegmentBytes + 1, 48},
				{"list-oversize", "list", 200, adapterMaxListResponse, adapterMaxListResponse + 17, 0},
				{"error-oversize", "get", 503, adapterMaxErrorBody, adapterMaxErrorBody + 17, 48},
				{"put-response-oversize", "put", 200, adapterMaxMetadataBody, adapterMaxMetadataBody + 17, 0},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					ctx := context.Background()
					if tc.context > 0 {
						ctx = context.WithValue(ctx, maxBodyKey{}, tc.context)
					}
					calls, closed := 0, 0
					var inflight chan struct{}
					body := &adapterResume35Body{remaining: tc.body, close: func() {
						closed++
						if len(inflight) != 1 {
							t.Error("response Close did not hold admission")
						}
					}}
					transport := adapterResume35Transport(func(*http.Request) (*http.Response, error) {
						calls++
						return adapterResume35Response(tc.status, body), nil
					})
					var payload []byte
					var err error
					if backend == "template" {
						s := adapterResume35Template(transport)
						inflight = s.inflight
						op := &s.tmpl.Get
						if tc.op == "list" {
							op = &s.tmpl.List
						} else if tc.op == "put" {
							op = &s.tmpl.Put
						}
						_, payload, err = s.run(ctx, op, s.baseVars(), nil)
					} else {
						s := adapterResume35S3(transport)
						inflight = s.inflight
						method, key, query := http.MethodGet, "root/0.seg.commit", url.Values(nil)
						if tc.op == "list" {
							key, query = "", url.Values{"list-type": {"2"}}
						} else if tc.op == "put" {
							method = http.MethodPut
						}
						_, payload, err = s.do(ctx, method, key, query, nil)
					}
					oversize := tc.body > tc.cap
					if oversize {
						if err == nil || !isPermanentAdapterError(err) || payload != nil {
							t.Fatalf("oversize returned len = %d, err = %v", len(payload), err)
						}
					} else if err != nil || int64(len(payload)) != tc.body {
						t.Fatalf("bounded success len = %d, err = %v", len(payload), err)
					}
					if calls != 1 || closed != 1 || len(inflight) != 0 || body.read != min(tc.body, tc.cap+1) {
						t.Fatalf("calls=%d closed=%d inflight=%d read=%d", calls, closed, len(inflight), body.read)
					}
				})
			}
		})
	}
}

func TestAdapterResume35HrefObjectCap(t *testing.T) {
	calls, closed := 0, 0
	var s *templateStorage
	s = adapterResume35Template(adapterResume35Transport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Path != "/blob" {
			return adapterResume35Response(http.StatusOK, io.NopCloser(strings.NewReader(`{"href":"https://adapter.invalid/blob"}`))), nil
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("href received API credentials")
		}
		return adapterResume35Response(http.StatusOK, &adapterResume35Body{remaining: 49, close: func() {
			closed++
			if len(s.inflight) != 1 {
				t.Error("href body Close did not hold admission")
			}
		}}), nil
	}))
	s.tmpl.Get.HrefPath = "href"
	s.tmpl.Auth = authTemplate{Type: "static", Header: map[string]string{"Authorization": "secret"}}
	ctx := context.WithValue(context.Background(), maxBodyKey{}, int64(48))
	if _, err := s.Get(ctx, "0.seg.commit"); err == nil || !isPermanentAdapterError(err) {
		t.Fatalf("href oversize error = %v", err)
	}
	if calls != 2 || closed != 1 || len(s.inflight) != 0 {
		t.Fatalf("calls=%d closed=%d inflight=%d", calls, closed, len(s.inflight))
	}
}

type adapterResume35GateBody struct {
	ctx       context.Context
	reading   chan struct{}
	readGate  chan struct{}
	closing   chan struct{}
	closeGate chan struct{}
	err       error
}

func (b *adapterResume35GateBody) Read([]byte) (int, error) {
	close(b.reading)
	select {
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	case <-b.readGate:
		return 0, b.err
	}
}

func (b *adapterResume35GateBody) Close() error {
	close(b.closing)
	<-b.closeGate
	return nil
}

func TestAdapterResume35AdmissionUntilClose(t *testing.T) {
	for _, backend := range []string{"template", "s3"} {
		for _, outcome := range []string{"eof", "read-error", "canceled"} {
			t.Run(backend+"/"+outcome, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					firstBody := &adapterResume35GateBody{ctx: ctx, reading: make(chan struct{}), readGate: make(chan struct{}), closing: make(chan struct{}), closeGate: make(chan struct{}), err: io.EOF}
					if outcome == "read-error" {
						firstBody.err = stderrors.New("offline read failure")
					}
					var calls atomic.Int32
					transport := adapterResume35Transport(func(r *http.Request) (*http.Response, error) {
						if calls.Add(1) == 1 {
							return adapterResume35Response(http.StatusOK, firstBody), nil
						}
						return adapterResume35Response(http.StatusOK, io.NopCloser(strings.NewReader("ok"))), nil
					})
					var get func(context.Context) error
					if backend == "template" {
						s := adapterResume35Template(transport)
						get = func(ctx context.Context) error { _, err := s.Get(ctx, "0.seg"); return err }
					} else {
						s := adapterResume35S3(transport)
						get = func(ctx context.Context) error { _, err := s.Get(ctx, "0.seg"); return err }
					}
					first, second := make(chan error, 1), make(chan error, 1)
					go func() { first <- get(ctx) }()
					<-firstBody.reading
					go func() { second <- get(context.Background()) }()
					synctest.Wait()
					if calls.Load() != 1 {
						t.Fatalf("second request entered during body Read: calls=%d", calls.Load())
					}
					if outcome == "canceled" {
						cancel()
					} else {
						close(firstBody.readGate)
					}
					<-firstBody.closing
					synctest.Wait()
					if calls.Load() != 1 {
						t.Fatalf("second request entered during body Close: calls=%d", calls.Load())
					}
					if outcome == "read-error" {
						cancel()
					}
					close(firstBody.closeGate)
					if err := <-second; err != nil {
						t.Fatalf("second request: %v", err)
					}
					err := <-first
					if outcome == "eof" && err != nil || outcome != "eof" && !stderrors.Is(err, context.Canceled) {
						t.Fatalf("first request: %v", err)
					}
				})
			})
		}
	}
}

func TestAdapterResume35AdmissionTransportFailure(t *testing.T) {
	for _, backend := range []string{"template", "s3"} {
		t.Run(backend, func(t *testing.T) {
			calls := 0
			transport := adapterResume35Transport(func(*http.Request) (*http.Response, error) {
				calls++
				return nil, errSessionClosed
			})
			var err error
			var inflight chan struct{}
			if backend == "template" {
				s := adapterResume35Template(transport)
				inflight = s.inflight
				_, err = s.Get(context.Background(), "0.seg")
			} else {
				s := adapterResume35S3(transport)
				inflight = s.inflight
				_, err = s.Get(context.Background(), "0.seg")
			}
			if !stderrors.Is(err, errSessionClosed) || calls != 1 || len(inflight) != 0 {
				t.Fatalf("err=%v calls=%d inflight=%d", err, calls, len(inflight))
			}
		})
	}
}

func TestAdapterResume35AdmissionRetryClosesBody(t *testing.T) {
	for _, backend := range []string{"template", "s3"} {
		t.Run(backend, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls, closed := 0, 0
				var inflight chan struct{}
				transport := adapterResume35Transport(func(*http.Request) (*http.Response, error) {
					calls++
					if closed != calls-1 {
						t.Fatal("next attempt started before prior response Close")
					}
					status := http.StatusOK
					if calls == 1 {
						status = http.StatusServiceUnavailable
					}
					return adapterResume35Response(status, &adapterResume35CloseReader{Reader: strings.NewReader("ok"), close: func() {
						if len(inflight) != 1 {
							t.Error("retry response Close did not hold admission")
						}
						closed++
					}}), nil
				})
				var payload []byte
				var err error
				if backend == "template" {
					s := adapterResume35Template(transport)
					inflight = s.inflight
					s.tmpl.Retry.Status = []int{http.StatusServiceUnavailable}
					payload, err = s.Get(context.Background(), "0.seg")
				} else {
					s := adapterResume35S3(transport)
					inflight = s.inflight
					payload, err = s.Get(context.Background(), "0.seg")
				}
				if err != nil || string(payload) != "ok" || calls != 2 || closed != 2 || len(inflight) != 0 {
					t.Fatalf("payload=%q err=%v calls=%d closed=%d inflight=%d", payload, err, calls, closed, len(inflight))
				}
			})
		})
	}
}

func TestAdapterResume35OAuthWaiterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started, release := make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		s := adapterResume35Template(nil)
		s.tmpl.Auth = authTemplate{Type: "oauth2", TokenURL: "https://oauth.invalid/token"}
		s.tokenClient = &http.Client{Transport: adapterResume35Transport(func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			close(started)
			select {
			case <-release:
				return adapterResume35Response(http.StatusOK, io.NopCloser(strings.NewReader(`{"access_token":"shared"}`))), nil
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		})}
		leaderCtx, cancelLeader := context.WithCancel(context.Background())
		waitCtx, cancelWaiter := context.WithCancel(context.Background())
		leader, canceled, survivor := make(chan error, 1), make(chan error, 1), make(chan string, 1)
		go func() { _, err := s.accessToken(leaderCtx); leader <- err }()
		<-started
		go func() { _, err := s.accessToken(waitCtx); canceled <- err }()
		go func() { token, err := s.accessToken(context.Background()); survivor <- fmt.Sprint(token, ":", err) }()
		synctest.Wait()
		cancelWaiter()
		synctest.Wait()
		select {
		case err := <-canceled:
			if !stderrors.Is(err, context.Canceled) {
				t.Fatalf("waiter error = %v", err)
			}
		default:
			t.Fatal("canceled waiter blocked behind token fetch")
		}
		cancelLeader()
		if err := <-leader; !stderrors.Is(err, context.Canceled) {
			t.Fatalf("leader error = %v", err)
		}
		close(release)
		if got := <-survivor; got != "shared:<nil>" {
			t.Fatalf("surviving waiter result = %q", got)
		}
		if token, err := s.accessToken(context.Background()); token != "shared" || err != nil || calls.Load() != 1 {
			t.Fatalf("cached token=%q err=%v calls=%d", token, err, calls.Load())
		}
	})
}

func TestAdapterResume35OAuthAbandonedFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started, canceled, releaseOld := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		s := adapterResume35Template(nil)
		s.tmpl.Auth = authTemplate{Type: "oauth2", TokenURL: "https://oauth.invalid/token"}
		s.tokenClient = &http.Client{Transport: adapterResume35Transport(func(r *http.Request) (*http.Response, error) {
			if calls.Add(1) == 1 {
				close(started)
				<-r.Context().Done()
				close(canceled)
				<-releaseOld
				return adapterResume35Response(http.StatusOK, io.NopCloser(strings.NewReader(`{"access_token":"abandoned"}`))), nil
			}
			return adapterResume35Response(http.StatusOK, io.NopCloser(strings.NewReader(`{"access_token":"new"}`))), nil
		})}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := s.accessToken(ctx); done <- err }()
		<-started
		cancel()
		if err := <-done; !stderrors.Is(err, context.Canceled) {
			t.Fatalf("abandoned caller error = %v", err)
		}
		<-canceled
		if token, err := s.accessToken(context.Background()); token != "new" || err != nil {
			t.Fatalf("new flight token=%q err=%v", token, err)
		}
		close(releaseOld)
		synctest.Wait()
		if token, err := s.accessToken(context.Background()); token != "new" || err != nil || calls.Load() != 2 {
			t.Fatalf("late old result replaced token: token=%q err=%v calls=%d", token, err, calls.Load())
		}
	})
}

func TestAdapterResume35OAuthInvalidConfiguration(t *testing.T) {
	for _, auth := range []authTemplate{
		{Type: "oauth2", TokenURL: "://invalid"},
		{Type: "oauth2", TokenURL: "file:///token"},
		{Type: "unsupported"},
	} {
		t.Run(auth.Type+auth.TokenURL, func(t *testing.T) {
			s := adapterResume35Template(adapterResume35Transport(func(*http.Request) (*http.Response, error) {
				t.Fatal("API sent with invalid auth configuration")
				return nil, nil
			}))
			s.tmpl.Auth = auth
			s.tokenClient = &http.Client{Transport: adapterResume35Transport(func(*http.Request) (*http.Response, error) {
				t.Fatal("token endpoint called with invalid URL")
				return nil, nil
			})}
			if _, err := s.Get(context.Background(), "0.seg"); err == nil || !isPermanentAdapterError(err) {
				t.Fatalf("invalid auth configuration error=%v", err)
			}
		})
	}
}

func TestAdapterResume35OAuthTransientRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		s := adapterResume35Template(adapterResume35Transport(func(*http.Request) (*http.Response, error) {
			return adapterResume35Response(http.StatusOK, io.NopCloser(strings.NewReader("ok"))), nil
		}))
		s.tmpl.Auth = authTemplate{Type: "oauth2", TokenURL: "https://oauth.invalid/token"}
		s.tokenClient = &http.Client{Transport: adapterResume35Transport(func(*http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				return adapterResume35Response(http.StatusServiceUnavailable, io.NopCloser(strings.NewReader("transient"))), nil
			}
			return adapterResume35Response(http.StatusOK, io.NopCloser(strings.NewReader(`{"access_token":"recovered"}`))), nil
		})}
		payload, err := s.Get(context.Background(), "0.seg")
		if err != nil || string(payload) != "ok" || calls != 2 {
			t.Fatalf("payload=%q err=%v token calls=%d", payload, err, calls)
		}
	})
}

func TestAdapterResume35OAuthPermanentErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"invalid-grant", 400, `{"error":"invalid_grant","secret":"must-not-echo"}`},
		{"missing-token", 200, `{}`},
		{"oversized-token", 200, strings.Repeat("x", adapterMaxMetadataBody+1)},
		{"oversized-error", 400, strings.Repeat("x", adapterMaxErrorBody+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, closed := 0, 0
			s := adapterResume35Template(adapterResume35Transport(func(*http.Request) (*http.Response, error) {
				t.Fatal("API sent despite failed authentication")
				return nil, nil
			}))
			s.tmpl.Auth = authTemplate{Type: "oauth2", TokenURL: "https://oauth.invalid/token"}
			s.tokenClient = &http.Client{Transport: adapterResume35Transport(func(*http.Request) (*http.Response, error) {
				calls++
				return adapterResume35Response(tc.status, &adapterResume35CloseReader{Reader: strings.NewReader(tc.body), close: func() { closed++ }}), nil
			})}
			if _, err := s.Get(context.Background(), "0.seg"); err == nil || !isPermanentAdapterError(err) || strings.Contains(err.Error(), "must-not-echo") {
				t.Fatalf("permanent auth error = %v", err)
			}
			if calls != 1 || closed != 1 {
				t.Fatalf("calls=%d closed=%d", calls, closed)
			}
		})
	}
}

func TestAdapterResume35OAuthStatusReadFailure(t *testing.T) {
	for _, status := range []int{200, 400, 401, 403, 408, 429, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls, closed, apiCalls atomic.Int32
				s := adapterResume35Template(adapterResume35Transport(func(*http.Request) (*http.Response, error) {
					apiCalls.Add(1)
					return adapterResume35Response(http.StatusOK, io.NopCloser(strings.NewReader("ok"))), nil
				}))
				s.tmpl.Auth = authTemplate{Type: "oauth2", TokenURL: "https://oauth.invalid/token"}
				s.tokenClient = &http.Client{Transport: adapterResume35Transport(func(*http.Request) (*http.Response, error) {
					if calls.Add(1) == 1 {
						return adapterResume35Response(status, &adapterResume35CloseReader{Reader: adapterResume35UnexpectedEOF{}, close: func() { closed.Add(1) }}), nil
					}
					return adapterResume35Response(http.StatusOK, &adapterResume35CloseReader{Reader: strings.NewReader(`{"access_token":"recovered"}`), close: func() { closed.Add(1) }}), nil
				})}
				payload, err := s.Get(context.Background(), "0.seg")
				terminal := status == 400 || status == 401 || status == 403
				if terminal {
					if err == nil || !isPermanentAdapterError(err) || !stderrors.Is(err, io.ErrUnexpectedEOF) || payload != nil || calls.Load() != 1 || closed.Load() != 1 || apiCalls.Load() != 0 {
						t.Fatalf("terminal status changed eligibility: payload=%q err=%v calls=%d closed=%d api=%d", payload, err, calls.Load(), closed.Load(), apiCalls.Load())
					}
				} else if err != nil || string(payload) != "ok" || calls.Load() != 2 || closed.Load() != 2 || apiCalls.Load() != 1 {
					t.Fatalf("transient read failure did not recover: payload=%q err=%v calls=%d closed=%d api=%d", payload, err, calls.Load(), closed.Load(), apiCalls.Load())
				}
			})
		})
	}
}

type adapterResume35UnexpectedEOF struct{}

func (adapterResume35UnexpectedEOF) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestAdapterResume35OAuthWireTerminalTruncatedBody(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Length", "64")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":"invalid_grant","secret":"must-not-echo"}`)
	}))
	defer server.Close()
	s := adapterResume35Template(adapterResume35Transport(func(*http.Request) (*http.Response, error) {
		t.Error("API called after terminal OAuth rejection")
		return adapterResume35Response(http.StatusOK, io.NopCloser(strings.NewReader(""))), nil
	}))
	s.tmpl.Auth = authTemplate{Type: "oauth2", TokenURL: server.URL + "/token"}
	s.tokenClient = server.Client()
	defer s.tokenClient.CloseIdleConnections()
	_, err := s.Get(context.Background(), "0.seg")
	if err == nil || !isPermanentAdapterError(err) || !stderrors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "must-not-echo") || calls.Load() != 1 {
		t.Fatalf("truncated terminal OAuth body retried: err=%v calls=%d", err, calls.Load())
	}
}

type adapterResume35CloseReader struct {
	io.Reader
	close func()
}

func (b *adapterResume35CloseReader) Close() error { b.close(); return nil }

func adapterResume35WireSignature(req *http.Request, secret, region, access string) string {
	hash := func(value string) string {
		sum := sha256.Sum256([]byte(value))
		return hex.EncodeToString(sum[:])
	}
	mac := func(key []byte, value string) []byte {
		h := hmac.New(sha256.New, key)
		h.Write([]byte(value))
		return h.Sum(nil)
	}
	stamp := req.Header.Get("X-Amz-Date")
	date := stamp[:8]
	fields := strings.Split(strings.Split(req.Header.Get("Authorization"), "SignedHeaders=")[1], ",")[0]
	var canonicalHeaders strings.Builder
	for _, field := range strings.Split(fields, ";") {
		value := req.Header.Get(field)
		if field == "host" {
			value = req.Host
		}
		fmt.Fprintf(&canonicalHeaders, "%s:%s\n", field, strings.Join(strings.Fields(value), " "))
	}
	query := req.URL.Query()
	keys := make([]string, 0, len(query))
	for key := range query {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var pairs []string
	for _, key := range keys {
		sort.Strings(query[key])
		for _, value := range query[key] {
			pairs = append(pairs, strings.ReplaceAll(url.QueryEscape(key), "+", "%20")+"="+strings.ReplaceAll(url.QueryEscape(value), "+", "%20"))
		}
	}
	canonical := strings.Join([]string{req.Method, req.URL.EscapedPath(), strings.Join(pairs, "&"), canonicalHeaders.String(), fields, req.Header.Get("X-Amz-Content-Sha256")}, "\n")
	scope := date + "/" + region + "/s3/aws4_request"
	key := mac([]byte("AWS4"+secret), date)
	for _, part := range []string{region, "s3", "aws4_request"} {
		key = mac(key, part)
	}
	signature := hex.EncodeToString(mac(key, "AWS4-HMAC-SHA256\n"+stamp+"\n"+scope+"\n"+hash(canonical)))
	return "AWS4-HMAC-SHA256 Credential=" + access + "/" + scope + ", SignedHeaders=" + fields + ", Signature=" + signature
}

func TestAdapterResume35S3UnicodeAuthorityWireSignature(t *testing.T) {
	for _, tc := range []struct {
		endpoint  string
		pathStyle bool
		host      string
	}{
		{"https://bücher.example:9443/api", true, "xn--bcher-kva.example:9443"},
		{"https://bücher.example:9443/api", false, "bucket.xn--bcher-kva.example:9443"},
		{"https://例え.テスト/api", true, "xn--r8jz45g.xn--zckzah"},
		{"https://example.invalid:443/api", false, "bucket.example.invalid:443"},
		{"http://[::1]:9000/api", true, "[::1]:9000"},
	} {
		t.Run(tc.host, func(t *testing.T) {
			config := &Config{RemoteFolder: "root", Secrets: []string{"offline", "offline-secret", "offline-session"}, S3: &S3Config{Endpoint: tc.endpoint, Bucket: "bucket", PathStyle: tc.pathStyle}}
			s, err := newS3Storage(nil, config)
			if err != nil {
				t.Fatal(err)
			}
			if s.base.Host != tc.host {
				t.Fatalf("base authority = %q, want %q", s.base.Host, tc.host)
			}
			target := s.target("root/snow 雪.seg", url.Values{"prefix": {"root/a b"}})
			req, err := http.NewRequest(http.MethodGet, target, nil)
			if err != nil {
				t.Fatal(err)
			}
			s.sign(req, s3Hash(nil), time.Date(2026, 10, 31, 12, 34, 56, 0, time.UTC))
			var wire bytes.Buffer
			if err := req.Write(&wire); err != nil {
				t.Fatal(err)
			}
			transmitted, err := http.ReadRequest(bufio.NewReader(&wire))
			if err != nil {
				t.Fatal(err)
			}
			defer transmitted.Body.Close()
			if transmitted.Host != tc.host || transmitted.Host != req.URL.Host {
				t.Fatalf("wire Host=%q, signed authority=%q, want %q", transmitted.Host, req.URL.Host, tc.host)
			}
			want := adapterResume35WireSignature(transmitted, "offline-secret", "us-east-1", "offline")
			if got := transmitted.Header.Get("Authorization"); got != want {
				t.Fatalf("SigV4 does not verify against transmitted authority: got %q, want %q", got, want)
			}
		})
	}
}
