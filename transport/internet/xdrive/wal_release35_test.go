package xdrive

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type releaseWALStorage struct {
	mu       sync.Mutex
	objects  map[string][]byte
	failures map[string]error
	puts     []string
	commit   bool
}

func (s *releaseWALStorage) requiresWALCommit() bool { return s.commit }

func (s *releaseWALStorage) Put(ctx context.Context, name string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts = append(s.puts, name)
	if err := s.failures[name]; err != nil {
		return err
	}
	if s.objects == nil {
		s.objects = make(map[string][]byte)
	}
	s.objects[name] = bytes.Clone(data)
	return nil
}

func (s *releaseWALStorage) Get(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.failures[name]; err != nil {
		return nil, err
	}
	data, ok := s.objects[name]
	if !ok {
		return nil, errNotFound
	}
	return bytes.Clone(data), nil
}

func (s *releaseWALStorage) Delete(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, name)
	return ctx.Err()
}

func (s *releaseWALStorage) List(ctx context.Context, prefix string) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var entries []Entry
	for name := range s.objects {
		if strings.HasPrefix(name, prefix+"/") {
			entries = append(entries, Entry{Name: strings.TrimPrefix(name, prefix+"/")})
		}
	}
	return entries, ctx.Err()
}

func (*releaseWALStorage) Close() error { return nil }

func TestReleaseWALHardErrorBehindPending(t *testing.T) {
	hard := errors.New("hard storage failure")
	for _, first := range []error{nil, errNotFound} {
		s := &releaseWALStorage{objects: map[string][]byte{"p/000000000.seg": {}}, failures: map[string]error{
			"p/000000000.seg": first, "p/000000001.seg": hard,
		}}
		r := &walReader{ctx: context.Background(), storage: s, prefix: "p"}
		_, err := r.fetch([]Entry{{Name: "000000000.seg"}, {Name: "000000001.seg"}})
		if err != hard {
			t.Fatalf("first error %v: got %v, want hard error", first, err)
		}
	}
}

func TestReleaseWALPermanentValidationBehindPending(t *testing.T) {
	for _, empty := range []bool{false, true} {
		s := &releaseWALStorage{objects: map[string][]byte{
			"p/000000001.seg": bytes.Repeat([]byte("x"), maxSegmentBytes+1),
		}}
		if empty {
			s.objects["p/000000000.seg"] = []byte{}
		}
		r := &walReader{ctx: context.Background(), storage: s, prefix: "p"}
		_, err := r.fetch([]Entry{{Name: "000000000.seg"}, {Name: "000000001.seg"}})
		if err == nil || err == errNotFound {
			t.Fatalf("empty=%v: pending body masked oversized body: %v", empty, err)
		}
	}
	hard := errors.New("hard commitment failure")
	s := &releaseWALStorage{commit: true, objects: map[string][]byte{
		"p/000000000.seg": {}, "p/000000001.seg": []byte("full body"),
	}, failures: map[string]error{"p/000000001.commit": hard}}
	r := &walReader{ctx: context.Background(), storage: s, prefix: "p"}
	if _, err := r.fetch([]Entry{{Name: "000000000.seg"}, {Name: "000000001.seg"}}); err != hard {
		t.Fatalf("pending body masked hard commitment failure: %v", err)
	}
}

func TestReleaseWALCommitOnlyAndAdvancedHoleTimeout(t *testing.T) {
	for _, commitOnly := range []bool{false, true} {
		s := &releaseWALStorage{commit: commitOnly, objects: make(map[string][]byte)}
		if commitOnly {
			s.objects["p/000000000.commit"] = encodeWALCommit([]byte("missing body"))
		} else {
			s.objects["p/000000000.seg"] = []byte("first body")
			s.objects["p/000000002.seg"] = []byte("later body")
		}
		p := paramsFromConfig(&Config{})
		r := &walReader{ctx: context.Background(), storage: s, prefix: "p", params: p, ch: make(chan []byte, p.concurrency)}
		advanced, _, err := r.poll()
		if err != nil || advanced == commitOnly || r.holeSince.IsZero() {
			t.Fatalf("commitOnly=%v: advanced=%v err=%v hole=%v", commitOnly, advanced, err, r.holeSince)
		}
		s.mu.Lock()
		s.objects = nil
		s.mu.Unlock()
		r.holeSince = time.Now().Add(-2 * p.holeTimeout)
		if _, _, err := r.poll(); err == nil {
			t.Fatalf("commitOnly=%v: listing omission erased observed hole", commitOnly)
		}
	}
}

func TestReleaseWALCommitRejectsPartialPublication(t *testing.T) {
	ctx := context.Background()
	s := &releaseWALStorage{commit: true}
	full := []byte("complete binary segment")
	name := objectName("p", 0, segSuffix)
	commitName := objectName("p", 0, commitSuffix)
	r := &walReader{ctx: ctx, storage: s, prefix: "p", params: paramsFromConfig(&Config{}), ch: make(chan []byte, 1)}
	if err := s.Put(ctx, name, full[:5]); err != nil {
		t.Fatal(err)
	}
	for _, commitment := range [][]byte{nil, encodeWALCommit(full)[:12], encodeWALCommit(full), encodeWALCommit([]byte("other segment"))} {
		if commitment != nil {
			if err := s.Put(ctx, commitName, commitment); err != nil {
				t.Fatal(err)
			}
		}
		advanced, _, err := r.poll()
		if advanced || err != nil || r.seq != 0 || len(r.ch) != 0 {
			t.Fatalf("partial delivered: advanced=%v err=%v seq=%d", advanced, err, r.seq)
		}
	}
	if err := s.Put(ctx, name, full); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, commitName, encodeWALCommit(full)); err != nil {
		t.Fatal(err)
	}
	advanced, _, err := r.poll()
	if !advanced || err != nil || r.seq != 1 || !bytes.Equal(<-r.ch, full) {
		t.Fatalf("complete segment not delivered: advanced=%v err=%v seq=%d", advanced, err, r.seq)
	}
}

func TestReleaseWALPendingPublicationTimeout(t *testing.T) {
	for _, commit := range []bool{false, true} {
		s := &releaseWALStorage{commit: commit}
		data := []byte(nil)
		if commit {
			data = []byte("partial")
		}
		if err := s.Put(context.Background(), objectName("p", 0, segSuffix), data); err != nil {
			t.Fatal(err)
		}
		r := &walReader{ctx: context.Background(), storage: s, prefix: "p", params: paramsFromConfig(&Config{}), ch: make(chan []byte, 1)}
		if _, _, err := r.poll(); err != nil || r.holeSince.IsZero() {
			t.Fatalf("did not start timeout: %v", err)
		}
		r.holeSince = time.Now().Add(-2 * r.holeTimeout)
		if _, _, err := r.poll(); err == nil {
			t.Fatal("listed incomplete upload reset its timeout")
		}
	}
}

func TestReleaseWALWriterCommitOrdering(t *testing.T) {
	for _, failCommit := range []bool{false, true} {
		s := &releaseWALStorage{commit: true, failures: make(map[string]error)}
		if failCommit {
			s.failures[objectName("p", 0, commitSuffix)] = errors.New("commit failed")
		}
		ctx, cancel := context.WithCancel(context.Background())
		w := newWALWriter(ctx, s, "p", paramsFromConfig(&Config{FlushIntervalMs: 1000}))
		data := []byte("full payload")
		if _, err := w.Write(data); err != nil {
			cancel()
			t.Fatal(err)
		}
		err := w.Close()
		cancel()
		if (err != nil) != failCommit {
			t.Fatalf("failCommit=%v: Close returned %v", failCommit, err)
		}
		s.mu.Lock()
		wantLast := objectName("p", 1, endSuffix)
		if failCommit {
			wantLast = objectName("p", 0, errSuffix)
		}
		if len(s.puts) != 3 || s.puts[0] != objectName("p", 0, segSuffix) || s.puts[1] != objectName("p", 0, commitSuffix) || s.puts[2] != wantLast {
			t.Errorf("publication order: %v", s.puts)
		}
		if !failCommit && !validWALCommit(s.objects[s.puts[1]], data) {
			t.Error("invalid writer commitment")
		}
		s.mu.Unlock()
	}
}

func TestReleaseWALAdmissionAfterDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newWALWriter(ctx, &releaseWALStorage{}, "p", paramsFromConfig(&Config{}))
	deadline := make(chan struct{})
	close(deadline)
	if n, err := w.writeUntil([]byte("x"), deadline, nil); n != 0 || err != errWriteStopped {
		t.Fatalf("admitted expired write: %d %v", n, err)
	}
	cancel()
	w.Close()
}

func TestReleaseWALConcurrentWriteClose(t *testing.T) {
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		w := newWALWriter(ctx, &releaseWALStorage{}, "p", paramsFromConfig(&Config{SegmentBytes: 16, Concurrency: 1, FlushIntervalMs: 1}))
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := w.Write(bytes.Repeat([]byte("x"), 256))
			if err != nil && err != io.ErrClosedPipe {
				t.Errorf("Write: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := w.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
		wg.Wait()
		cancel()
	}
}
