package xdrive

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type oauthFormRoundTripper func(*http.Request) (*http.Response, error)

func (f oauthFormRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestTemplateOAuthFormRegression(t *testing.T) {
	for _, tc := range []struct {
		name    string
		secrets []string
		folder  string
		form    map[string]string
		want    url.Values
	}{
		{
			name:    "plain",
			secrets: []string{"dummy-client", "dummy-refresh"},
			folder:  "dummy-folder",
			form: map[string]string{
				"grant_type": "refresh_token", "client_id": "{secret0}",
				"refresh_token": "{secret1}",
			},
			want: url.Values{
				"grant_type": {"refresh_token"}, "client_id": {"dummy-client"},
				"refresh_token": {"dummy-refresh"},
			},
		},
		{
			name:    "reserved_unicode_repeated_placeholders",
			secrets: []string{"dummy&+ = café東京", "refresh=one&injected=two+ trois雪"},
			folder:  "folder &+= 日本",
			// The schema is map[string]string: each key has one value. Repeated
			// placeholders must not split that value or create extra form fields.
			form: map[string]string{
				"grant_type": "refresh_token", "client_id": "{secret0}",
				"refresh_token": "{secret1}", "repeat": "{secret0}|{secret0}|{secret1}",
				"field&+ = 日本": "{folder}", "{secret0}": "literal-key",
				"empty": "", "": "empty-key",
			},
			want: url.Values{
				"grant_type": {"refresh_token"}, "client_id": {"dummy&+ = café東京"},
				"refresh_token": {"refresh=one&injected=two+ trois雪"},
				"repeat":        {"dummy&+ = café東京|dummy&+ = café東京|refresh=one&injected=two+ trois雪"},
				"field&+ = 日本":  {"folder &+= 日本"}, "{secret0}": {"literal-key"},
				"empty": {""}, "": {"empty-key"},
			},
		},
		{name: "empty_form", form: map[string]string{}, want: url.Values{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const tokenLatency = 17 * time.Millisecond
				const callers = 16
				var requests atomic.Int32
				client := &http.Client{Transport: oauthFormRoundTripper(func(r *http.Request) (*http.Response, error) {
					requests.Add(1)
					if r.Method != http.MethodPost || r.URL.String() != "https://oauth.invalid/token" {
						t.Error("token request method or target changed")
					}
					if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
						t.Error("token request must remain form encoded")
					}
					if err := r.ParseForm(); err != nil {
						t.Errorf("decode token form: %v", err)
					} else if !reflect.DeepEqual(r.PostForm, tc.want) {
						// Do not print form contents, even though these credentials are dummies.
						t.Errorf("decoded form differs: got %d fields, want %d exact single-valued fields", len(r.PostForm), len(tc.want))
					}
					time.Sleep(tokenLatency)
					response := httptest.NewRecorder()
					response.Header().Set("Content-Type", "application/json")
					io.WriteString(response, `{"access_token":"dummy-access-token","expires_in":3600}`)
					return response.Result(), nil
				})}
				storage := &templateStorage{templateShared: &templateShared{
					tmpl: &storageTemplate{Auth: authTemplate{
						Type: "oauth2", TokenURL: "https://oauth.invalid/token", Form: tc.form,
						Header: map[string]string{"Authorization": "Bearer {token}"}, ExpiryPath: "expires_in",
					}},
					tokenClient: client, folder: tc.folder, secrets: tc.secrets,
				}}
				checkToken := func() {
					headers, err := storage.authHeaders(context.Background(), storage.baseVars())
					if err != nil {
						t.Errorf("authHeaders: %v", err)
					} else if headers["Authorization"] != "Bearer dummy-access-token" {
						t.Error("OAuth header did not retain the decoded token")
					}
				}

				// Exercise the timed cold request without mutex waiters: Mutex.Lock
				// is not a durable block for synctest's fake clock.
				start := time.Now()
				checkToken()
				coldElapsed := time.Since(start)
				if coldElapsed != tokenLatency {
					t.Errorf("cold auth latency = %v, want exactly %v", coldElapsed, tokenLatency)
				}
				start = time.Now()
				var wg sync.WaitGroup
				for range callers {
					wg.Go(checkToken)
				}
				wg.Wait()
				for range 4 {
					checkToken()
				}
				cachedElapsed := time.Since(start)
				if cachedElapsed != 0 {
					t.Errorf("cached auth latency = %v, want 0", cachedElapsed)
				}
				if got := requests.Load(); got != 1 {
					t.Errorf("token requests = %d, want exactly 1", got)
				}
				t.Logf("cold calls=1 cached calls=%d token requests=%d fake cold=%v cached=%v", callers+4, requests.Load(), coldElapsed, cachedElapsed)
			})
		})
	}
}
