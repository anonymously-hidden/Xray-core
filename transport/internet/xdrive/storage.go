package xdrive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"sync/atomic"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/tls"
)

var errNotFound = errors.New("object not found")

// errDuplicateID is returned by test stores that reject a repeated create.
var errDuplicateID = errors.New("duplicate id")

type Entry struct {
	Name   string
	Inline []byte
}

type Storage interface {
	Put(ctx context.Context, name string, data []byte) error
	Get(ctx context.Context, name string) ([]byte, error)
	Delete(ctx context.Context, name string) error
	List(ctx context.Context, prefix string) ([]Entry, error)
	Close() error
}

// testStorageFactory lets tests substitute a backend for newStorage; nil in
// production so behaviour is unchanged.
var testStorageFactory atomic.Pointer[func(config *Config) (Storage, error)]

func newStorage(streamSettings *internet.MemoryStreamConfig) (Storage, error) {
	config, err := streamConfig(streamSettings)
	if err != nil {
		return nil, err
	}
	if f := testStorageFactory.Load(); f != nil {
		return (*f)(config)
	}

	switch config.Service {
	case "local":
		return newLocalStorage(config.RemoteFolder)
	case "google-drive", "Google Drive":
		return sharedStorage(streamSettings, config, func() (Storage, error) {
			return newDriveStorage(streamSettings, config)
		})
	case "template":
		return sharedStorage(streamSettings, config, func() (Storage, error) {
			return newTemplateStorage(streamSettings, config)
		})
	case "s3":
		return sharedStorage(streamSettings, config, func() (Storage, error) {
			return newS3Storage(streamSettings, config)
		})
	default:
		return nil, errors.New("unsupported service: ", config.Service)
	}
}

// sessionStorage returns the storage of one session, which takes connections
// of XMUX until Close, with the token and cache of storage. Local storage has
// no connections
func sessionStorage(storage Storage) Storage {
	switch s := storage.(type) {
	case *driveStorage:
		return s.session()
	case *templateStorage:
		return s.session()
	case *s3Storage:
		return s.session()
	}
	return storage
}

var (
	sharedMu sync.Mutex
	shared   = make(map[string]Storage)
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
		HasFinalMask                                         bool
	}{Service: config.Service, Folder: config.RemoteFolder, Secrets: config.Secrets,
		Template: config.Template, XMUX: config.Xmux}
	if config.S3 != nil {
		profile.S3 = &S3Config{Endpoint: config.S3.Endpoint, Region: s3Region(config.S3.Region),
			Bucket: config.S3.Bucket, PathStyle: config.S3.PathStyle}
	}
	if streamSettings != nil {
		profile.SecurityType = streamSettings.SecurityType
		profile.TLS = tls.ConfigFromStreamSettings(streamSettings)
		profile.Socket, profile.QUIC = streamSettings.SocketSettings, streamSettings.QuicParams
		// Different non-nil masks remain unresolved: FinalMask exposes neither
		// stable configuration nor its wrappers. Never hash runtime pointers.
		profile.HasFinalMask = streamSettings.FinalMask != nil
		if streamSettings.Destination != nil {
			profile.Destination = streamSettings.Destination.String()
		}
	}
	h := sha256.New()
	common.Must(json.NewEncoder(h).Encode(profile))
	return hex.EncodeToString(h.Sum(nil))
}

func resetSharedStorage() {
	stopAllSpares()
	sharedMu.Lock()
	old := shared
	shared = make(map[string]Storage)
	sharedMu.Unlock()
	for _, storage := range old {
		storage.Close()
	}
}

func sharedStorage(streamSettings *internet.MemoryStreamConfig, config *Config, build func() (Storage, error)) (Storage, error) {
	key := shareKey(streamSettings, config)

	sharedMu.Lock()
	defer sharedMu.Unlock()

	if storage, ok := shared[key]; ok {
		return storage, nil
	}
	storage, err := build()
	if err != nil {
		return nil, err
	}
	shared[key] = storage
	return storage, nil
}
