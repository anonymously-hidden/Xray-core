package xdrive

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/transport/internet"
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
	var key string
	// Local stores and opaque masks do not share a transport profile.
	if config.Service != "local" && (streamSettings == nil || streamSettings.FinalMask == nil) {
		key = shareKey(streamSettings, config)
		sharedMu.Lock()
		defer sharedMu.Unlock()
		if cached := shared[key]; cached != nil {
			cached.sessions[""]++
			return cached, nil
		}
	}
	owner := &storageOwner{key: key, sessions: map[string]int{"": 1}}
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
	if key != "" {
		shared[key] = owner
	}
	return owner, nil
}

var (
	sharedMu sync.Mutex
	shared   = make(map[string]*storageOwner)
)

func shareKey(streamSettings *internet.MemoryStreamConfig, config *Config) string {
	stream := cmp.Or(streamSettings, &internet.MemoryStreamConfig{})
	// JSON preserves field and element boundaries and sorts map keys.
	profile := []any{
		config.Service, config.RemoteFolder, config.Template, config.Secrets, config.Xmux,
		stream.SecurityType, tls.ConfigFromStreamSettings(stream), stream.SocketSettings, stream.QuicParams,
	}
	if stream.Destination != nil {
		profile = append(profile, stream.Destination.String())
	}
	if s := config.S3; s != nil {
		profile = append(profile, s.Endpoint, s3Region(s.Region), s.Bucket, s.PathStyle)
	}
	sum := sha256.Sum256(common.Must2(json.Marshal(profile)))
	return hex.EncodeToString(sum[:])
}
