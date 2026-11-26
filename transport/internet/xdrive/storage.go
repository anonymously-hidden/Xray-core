package xdrive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/tls"
)

var errNotFound = errors.New("object not found")

type Entry struct {
	Name                      string
	Inline                    []byte
	created                   time.Time // optional backend-supplied creation time on the local clock
	driveID, driveDescription string    // from the same scoped announcement listing
}

// Config.Secrets for clients need only create/update/read/list permissions;
// only server credentials need delete permission.
type backend interface {
	session() backend
	Delete(ctx context.Context, name string) error
	List(ctx context.Context, prefix string) ([]Entry, error)
	Close() error
}

// Storage is the named-object interface used by the WAL.
type Storage interface {
	Delete(ctx context.Context, name string) error
	List(ctx context.Context, prefix string) ([]Entry, error)
	Close() error
	Put(ctx context.Context, name string, data []byte) error
	Get(ctx context.Context, name string) ([]byte, error)
}

func newStorage(streamSettings *internet.MemoryStreamConfig, config *Config) (*storageOwner, error) {
	owner := &storageOwner{sessions: map[string]int{"": 1}}
	// Local stores and opaque masks do not share a transport profile.
	if config.Service != "local" && (streamSettings == nil || streamSettings.FinalMask == nil) {
		owner.key = shareKey(streamSettings, config)
		sharedMu.Lock()
		defer sharedMu.Unlock()
		if cached := shared[owner.key]; cached != nil {
			cached.sessions[""]++
			return cached, nil
		}
	}
	var err error
	switch config.Service {
	case "google-drive", "Google Drive":
		owner.backend, err = newDriveStorage(streamSettings, config)
	case "local":
		owner.backend, err = newLocalStorage(config.RemoteFolder)
	case "template":
		owner.backend, err = newTemplateStorage(streamSettings, config)
	case "s3":
		owner.backend, err = newS3Storage(streamSettings, config)
	default:
		return nil, errors.New("unsupported service: ", config.Service)
	}
	if err != nil {
		return nil, err
	}
	if owner.key != "" {
		shared[owner.key] = owner
	}
	return owner, nil
}

var (
	sharedMu sync.Mutex
	shared   = make(map[string]*storageOwner)
)

func shareKey(streamSettings *internet.MemoryStreamConfig, config *Config) string {
	// Encode only configuration, not protobuf runtime state or transport caches.
	// JSON provides field/element boundaries (unlike NUL-joined strings), with
	// deterministic map ordering. These concrete config types contain no funcs.
	profile := struct {
		Service, Folder, Template, SecurityType, Destination string
		Secrets                                              []string
		S3                                                   *S3Config
		TLS                                                  *tls.Config
		Socket                                               *internet.SocketConfig
		QUIC                                                 *internet.QuicParams
		XMUX                                                 *splithttp.XmuxConfig
	}{
		Service: config.Service, Folder: config.RemoteFolder, Secrets: config.Secrets,
		Template: config.Template, XMUX: config.Xmux,
	}
	if config.S3 != nil {
		profile.S3 = &S3Config{
			Endpoint: config.S3.Endpoint, Region: s3Region(config.S3.Region),
			Bucket: config.S3.Bucket, PathStyle: config.S3.PathStyle,
		}
	}
	if streamSettings != nil {
		profile.SecurityType = streamSettings.SecurityType
		profile.TLS = tls.ConfigFromStreamSettings(streamSettings)
		profile.Socket, profile.QUIC = streamSettings.SocketSettings, streamSettings.QuicParams
		if streamSettings.Destination != nil {
			profile.Destination = streamSettings.Destination.String()
		}
	}
	h := sha256.New()
	common.Must(json.NewEncoder(h).Encode(profile))
	return hex.EncodeToString(h.Sum(nil))
}
