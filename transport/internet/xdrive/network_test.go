package xdrive

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"

	xnet "github.com/xtls/xray-core/common/net"
)

// These guards live for the whole process, including teardown goroutines. Check
// literal loopback addresses before DNS, while allowing local fronting tests.
func TestMain(m *testing.M) {
	for _, name := range []string{"XRAY_XDRIVE_DRIVE_SECRETS", "XDRIVE_YANDEX_USER", "XDRIVE_YANDEX_PASS"} {
		os.Unsetenv(name) // no credential file or live service in this suite
	}
	testServiceDial = func(dest xnet.Destination) error {
		return localTestTarget(dest.Address.String())
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		if err := localTestTarget(host); err != nil {
			return nil, err
		}
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
	http.DefaultTransport = t
	os.Exit(m.Run())
}

func localTestTarget(host string) error {
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("xdrive test blocked non-loopback target %q before DNS/dial", host)
}

func TestNetworkGuard(t *testing.T) {
	for _, host := range []string{"oauth2.googleapis.com", "www.googleapis.com", "drive.invalid", "8.8.8.8"} {
		if localTestTarget(host) == nil {
			t.Errorf("external target allowed: %s", host)
		}
	}
	for _, host := range []string{"127.0.0.1", "::1"} {
		if err := localTestTarget(host); err != nil {
			t.Error(err)
		}
	}
}
