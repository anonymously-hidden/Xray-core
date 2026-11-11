package xdrive

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/tls"
	"google.golang.org/protobuf/proto"
)

func keyRegressionSettings() *internet.MemoryStreamConfig {
	return &internet.MemoryStreamConfig{
		ProtocolName: protocolName,
		ProtocolSettings: &Config{
			Service: "s3", RemoteFolder: "dummy-folder", Secrets: []string{"dummy-access", "dummy-secret"},
			S3:       &S3Config{Endpoint: "https://s3.invalid", Bucket: "dummy-bucket", Region: "us-east-1", PathStyle: true},
			Template: `{"auth":{"type":"none"}}`,
			Xmux: &splithttp.XmuxConfig{
				MaxConcurrency:   &splithttp.RangeConfig{From: 2, To: 2},
				MaxConnections:   &splithttp.RangeConfig{From: 3, To: 3},
				CMaxReuseTimes:   &splithttp.RangeConfig{From: 4, To: 4},
				HMaxRequestTimes: &splithttp.RangeConfig{From: 5, To: 5},
				HMaxReusableSecs: &splithttp.RangeConfig{From: 6, To: 6}, HKeepAlivePeriod: 7,
			},
		},
		SecurityType: "tls",
		SecuritySettings: &tls.Config{
			ServerName: "front.invalid", Fingerprint: "chrome", NextProtocol: []string{"h3", "h2"},
		},
		Destination:    &net.Destination{Network: net.Network_TCP, Address: net.DomainAddress("front.invalid"), Port: 443},
		SocketSettings: &internet.SocketConfig{Mark: 42, DialerProxy: "dummy-outbound"},
		QuicParams:     &internet.QuicParams{Congestion: "bbr", MaxIdleTimeout: 30},
	}
}

func cloneKeyRegressionSettings(s *internet.MemoryStreamConfig) *internet.MemoryStreamConfig {
	return &internet.MemoryStreamConfig{
		ProtocolName: s.ProtocolName, ProtocolSettings: proto.Clone(s.ProtocolSettings.(*Config)),
		SecurityType: s.SecurityType, SecuritySettings: proto.Clone(s.SecuritySettings.(*tls.Config)),
		Destination: &net.Destination{
			Network: s.Destination.Network, Address: net.DomainAddress(s.Destination.Address.Domain()), Port: s.Destination.Port,
		},
		SocketSettings: proto.Clone(s.SocketSettings).(*internet.SocketConfig),
		QuicParams:     proto.Clone(s.QuicParams).(*internet.QuicParams),
	}
}

// FinalMask still exposes runtime wrappers rather than stable configuration.
// The representable configuration below must nevertheless be isolated.
func TestStorageProfileSeparation(t *testing.T) {
	changes := map[string]func(*internet.MemoryStreamConfig){
		"tls":    func(s *internet.MemoryStreamConfig) { s.SecuritySettings.(*tls.Config).ServerName = "other.invalid" },
		"alpn":   func(s *internet.MemoryStreamConfig) { s.SecuritySettings.(*tls.Config).NextProtocol = []string{"h3"} },
		"socket": func(s *internet.MemoryStreamConfig) { s.SocketSettings.DialerProxy = "other-outbound" },
		"quic":   func(s *internet.MemoryStreamConfig) { s.QuicParams.MaxIdleTimeout++ },
		"xmux":   func(s *internet.MemoryStreamConfig) { s.ProtocolSettings.(*Config).Xmux.HKeepAlivePeriod++ },
		"template": func(s *internet.MemoryStreamConfig) {
			s.ProtocolSettings.(*Config).Template = `{"auth":{"type":"other"}}`
		},
		"s3":     func(s *internet.MemoryStreamConfig) { s.ProtocolSettings.(*Config).S3.Bucket = "other-bucket" },
		"secret": func(s *internet.MemoryStreamConfig) { s.ProtocolSettings.(*Config).Secrets[0] = "other-dummy" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			a := keyRegressionSettings()
			b := cloneKeyRegressionSettings(a)
			change(b)
			if shareKey(a, a.ProtocolSettings.(*Config)) == shareKey(b, b.ProtocolSettings.(*Config)) {
				t.Errorf("different %s profiles share", name)
			}
		})
	}
	a := keyRegressionSettings()
	b := cloneKeyRegressionSettings(a)
	a.ProtocolSettings.(*Config).Secrets = []string{"a\x00b", "c"}
	b.ProtocolSettings.(*Config).Secrets = []string{"a", "b\x00c"}
	if shareKey(a, a.ProtocolSettings.(*Config)) == shareKey(b, b.ProtocolSettings.(*Config)) {
		t.Error("ambiguous secret framing")
	}
}

func TestStorageKeyCloneStable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		settings := keyRegressionSettings()
		config := settings.ProtocolSettings.(*Config)
		before := proto.Clone(config)
		want := shareKey(settings, config)
		var clients atomic.Int32
		newClient := func(context.Context) *http.Client {
			clients.Add(1)
			return &http.Client{}
		}
		start := time.Now()
		var wg sync.WaitGroup
		for i := range 64 {
			wg.Go(func() {
				s := settings
				if i%2 == 0 {
					s = cloneKeyRegressionSettings(settings)
				}
				c := s.ProtocolSettings.(*Config)
				if got := shareKey(s, c); got != want {
					t.Error("repeated or cloned settings changed the key")
				}
				paramsForStorage(c, &driveStorage{})
				newConnPool(c.Xmux, newClient)
				if got := shareKey(s, c); got != want {
					t.Error("parameter or pool construction changed the key")
				}
			})
		}
		wg.Wait()
		newConnPool(nil, newClient)
		if !proto.Equal(config, before) {
			t.Error("parameter or pool construction mutated the input config")
		}
		if got := clients.Load(); got != 0 {
			t.Errorf("client constructions = %d, want 0 before any request", got)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("non-I/O key/pool checks advanced fake time by %v", elapsed)
		}
		t.Logf("64 concurrent repeated/clone checks: client constructions=%d, fake elapsed=%v", clients.Load(), time.Since(start))
	})
}

func TestStorageParamsCopylockDefaults(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config *Config
		want   params
	}{
		{
			name: "defaults", config: &Config{},
			want: params{segmentBytes: 512 << 10, concurrency: 64, flushInterval: 10 * time.Millisecond,
				minPollInterval: 50 * time.Millisecond, maxPollInterval: 200 * time.Millisecond,
				eagerWindow: 2 * time.Second, holeTimeout: 30 * time.Second, sessionTTL: 5 * time.Minute},
		},
		{
			name: "explicit", config: &Config{SegmentBytes: 8192, Concurrency: 4, FlushIntervalMs: 30,
				PollIntervalMs: 7, MaxPollIntervalMs: 100, EagerWindowMs: 11, HoleTimeoutMs: 13, SessionTtlSeconds: 17},
			want: params{segmentBytes: 8192, concurrency: 4, flushInterval: 30 * time.Millisecond,
				minPollInterval: 7 * time.Millisecond, maxPollInterval: 100 * time.Millisecond,
				eagerWindow: 11 * time.Millisecond, holeTimeout: 13 * time.Millisecond, sessionTTL: 17 * time.Second},
		},
		{
			name: "default_max_clamped_to_min", config: &Config{PollIntervalMs: 750},
			want: params{segmentBytes: 512 << 10, concurrency: 64, flushInterval: 10 * time.Millisecond,
				minPollInterval: 750 * time.Millisecond, maxPollInterval: 750 * time.Millisecond,
				eagerWindow: 2 * time.Second, holeTimeout: 30 * time.Second, sessionTTL: 5 * time.Minute},
		},
		{
			name: "default_max_below_wal_default", config: &Config{PollIntervalMs: 300},
			want: params{segmentBytes: 512 << 10, concurrency: 64, flushInterval: 10 * time.Millisecond,
				minPollInterval: 300 * time.Millisecond, maxPollInterval: 300 * time.Millisecond,
				eagerWindow: 2 * time.Second, holeTimeout: 30 * time.Second, sessionTTL: 5 * time.Minute},
		},
		{
			name: "explicit_max_clamped_and_caps", config: &Config{SegmentBytes: 32 << 20, Concurrency: 999,
				PollIntervalMs: 80, MaxPollIntervalMs: 10},
			want: params{segmentBytes: 16 << 20, concurrency: 256, flushInterval: 10 * time.Millisecond,
				minPollInterval: 80 * time.Millisecond, maxPollInterval: 80 * time.Millisecond,
				eagerWindow: 2 * time.Second, holeTimeout: 30 * time.Second, sessionTTL: 5 * time.Minute},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := proto.Clone(tc.config)
			if got := paramsForStorage(tc.config, &driveStorage{}); got != tc.want {
				t.Errorf("slot params = %+v, want %+v", got, tc.want)
			}
			if got, want := paramsForStorage(tc.config, &localStorage{}), paramsFromConfig(tc.config); got != want {
				t.Errorf("WAL params changed: %+v, want %+v", got, want)
			}
			if !proto.Equal(tc.config, before) {
				t.Error("paramsForStorage mutated its protobuf input")
			}
		})
	}
}
