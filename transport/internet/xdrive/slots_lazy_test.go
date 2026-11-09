package xdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/transport/internet"
)

// memStorage adapts memSlots to Storage for Dial/Serve in tests.
type memStorage struct {
	*memSlots
	mu    sync.Mutex
	files map[string][]byte
}

func (m *memStorage) Put(ctx context.Context, name string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[name] = bytes.Clone(data)
	return nil
}

func (m *memStorage) Get(ctx context.Context, name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.files[name]
	if !ok {
		return nil, errNotFound
	}
	return d, nil
}

func (m *memStorage) List(ctx context.Context, prefix string) ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Entry
	for name, d := range m.files {
		if !strings.HasPrefix(name, prefix+"/") {
			continue
		}
		rest := strings.TrimPrefix(name, prefix+"/")
		if strings.Contains(rest, "/") {
			continue
		}
		out = append(out, Entry{Name: rest, Inline: d})
	}
	return out, nil
}

func (m *memStorage) Delete(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, name)
	for k := range m.files {
		if strings.HasPrefix(k, name+"/") {
			delete(m.files, k)
		}
	}
	return nil
}

func (m *memStorage) Close() error { return nil }

func TestLazyRingEndToEnd(t *testing.T) {
	store := newMemSlots()
	storage := &memStorage{memSlots: store, files: make(map[string][]byte)}
	factory := func(*Config) (Storage, error) { return storage, nil }
	testStorageFactory.Store(&factory)
	t.Cleanup(func() { testStorageFactory.Store(nil) })

	settings := &internet.MemoryStreamConfig{
		ProtocolName: protocolName,
		ProtocolSettings: &Config{
			Service:           "sim",
			RemoteFolder:      "x",
			Secrets:           []string{"a", "b", "c"},
			Concurrency:       2,
			FlushIntervalMs:   1,
			PollIntervalMs:    1,
			MaxPollIntervalMs: 5,
			SessionTtlSeconds: 60,
		},
	}
	client, server, cleanup := pairWith(t, settings)
	defer cleanup()
	if err := server.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	// More segments than the depth-2 ring so slots wrap and later laps update.
	payload := randomPayload(20000)
	errs := make(chan error, 1)
	go func() {
		_, err := client.Write(payload)
		if err == nil {
			err = client.Close()
		}
		errs <- err
	}()
	got, err := io.ReadAll(server)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("lazy ring stream not byte-exact")
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	creates, updates := store.creates, store.updates
	store.mu.Unlock()
	if creates == 0 {
		t.Fatal("lazy ring made no createSlot calls")
	}
	if updates == 0 {
		t.Fatal("wrapped lazy slots never updated")
	}
}

func TestDecodeRingAcceptsLazy(t *testing.T) {
	ids := make([]string, 6)
	for i := range ids {
		ids[i] = "id-" + string(rune('a'+i))
	}
	r := lazyRing("sess", 2, ids)
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	dec, ok := decodeRing(b)
	if !ok || !dec.Lazy {
		t.Fatal("decodeRing rejected a lazy ring")
	}
	// An eager ring (no Lazy) still decodes and is not lazy.
	r.Lazy = false
	b, _ = json.Marshal(r)
	dec, ok = decodeRing(b)
	if !ok || dec.Lazy {
		t.Fatal("eager ring did not decode as eager")
	}
}
