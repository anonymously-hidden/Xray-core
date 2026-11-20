package xdrive

import (
	"sync"
	"testing"
)

func TestDriveStreamCapsLegacyFallback(t *testing.T) {
	s := &driveStorage{}
	s.observeStreamingProperties(map[string]string{"xdriveStream": "1", "xdriveSession": "session"})
	s.configureStreaming("", true)
	if s.streamCaps.Load() != nil || s.streamingEnabled() {
		t.Fatal("unconfigured or empty-nonce storage enabled streaming")
	}
	if properties := s.streamingProperties(); properties != nil {
		t.Fatalf("unconfigured properties = %v, want nil", properties)
	}
	if fields := s.streamingFields(); fields != "description" {
		t.Fatalf("unconfigured fields = %q, want description", fields)
	}

	s.configureStreaming("session", false)
	for i := 0; i < 8; i++ {
		s.observeStreamingProperties(nil)
		s.observeStreamingProperties(map[string]string{})
	}
	if s.streamingEnabled() {
		t.Fatal("legacy peer with no properties enabled streaming")
	}
	if fields := s.streamingFields(); fields != "description,properties" {
		t.Fatalf("configured fields = %q, want description,properties", fields)
	}
	if properties := s.streamingProperties(); properties != nil {
		t.Fatalf("endpoint without an accepted ring offer advertised properties: %v", properties)
	}
}

func TestDriveStreamCapsPeerOffer(t *testing.T) {
	server := &driveStorage{}
	server.configureStreaming("session", true)
	if !server.streamingEnabled() {
		t.Fatal("declared peer reader did not immediately enable the server writer")
	}
	properties := server.streamingProperties()
	if len(properties) != 2 || properties["xdriveStream"] != "1" || properties["xdriveSession"] != "session" {
		t.Fatalf("server reader acknowledgment = %v", properties)
	}

	client := &driveStorage{}
	client.configureStreaming("session", false)
	if client.streamingEnabled() {
		t.Fatal("client writer enabled before the server reader offer")
	}
	if properties := client.streamingProperties(); properties != nil {
		t.Fatalf("client advertised metadata before the server acknowledgment: %v", properties)
	}
	client.observeStreamingProperties(properties)
	if !client.streamingEnabled() {
		t.Fatal("matching server reader offer did not enable the client writer")
	}
	if properties := client.streamingProperties(); properties != nil {
		t.Fatalf("confirmed client advertised metadata: %v", properties)
	}
}

func TestDriveStreamCapsOwnOutgoingMetadataCannotBootstrap(t *testing.T) {
	client := &driveStorage{}
	client.configureStreaming("session", false)
	oldPeer := &driveStorage{}
	oldPeer.configureStreaming("session", false)
	for i := 0; i < 8; i++ {
		// Duplicate-create recovery reads the client's own outgoing slot, not
		// necessarily an incoming slot owned by the server.
		ownProperties := client.streamingProperties()
		if ownProperties != nil {
			t.Fatalf("client outgoing slot carries a reader acknowledgment: %v", ownProperties)
		}
		client.observeStreamingProperties(ownProperties)
		client.observeStreamingProperties(oldPeer.streamingProperties())
		if client.streamingEnabled() || oldPeer.streamingEnabled() {
			t.Fatal("own-slot recovery or a legacy peer enabled a streaming writer")
		}
	}
	if properties := oldPeer.streamingProperties(); properties != nil {
		t.Fatalf("server without an accepted ring offer advertised properties: %v", properties)
	}
	server := &driveStorage{}
	server.configureStreaming("session", true)
	client.observeStreamingProperties(server.streamingProperties())
	if !client.streamingEnabled() || client.streamingProperties() != nil {
		t.Fatal("server acknowledgment did not confirm the client without self-advertising")
	}
}

func TestDriveStreamCapsObserveProperties(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]string
		want       bool
	}{
		{name: "nil"},
		{name: "empty", properties: map[string]string{}},
		{name: "missing version", properties: map[string]string{"xdriveSession": "session"}},
		{name: "missing nonce", properties: map[string]string{"xdriveStream": "1"}},
		{name: "empty nonce", properties: map[string]string{"xdriveStream": "1", "xdriveSession": ""}},
		{name: "wrong nonce", properties: map[string]string{"xdriveStream": "1", "xdriveSession": "other"}},
		{name: "nonce whitespace", properties: map[string]string{"xdriveStream": "1", "xdriveSession": " session"}},
		{name: "unknown version", properties: map[string]string{"xdriveStream": "2", "xdriveSession": "session"}},
		{name: "zero version", properties: map[string]string{"xdriveStream": "0", "xdriveSession": "session"}},
		{name: "empty version", properties: map[string]string{"xdriveStream": "", "xdriveSession": "session"}},
		{name: "padded version", properties: map[string]string{"xdriveStream": "01", "xdriveSession": "session"}},
		{name: "version whitespace", properties: map[string]string{"xdriveStream": "1 ", "xdriveSession": "session"}},
		{name: "wrong key case", properties: map[string]string{"XdriveStream": "1", "xdriveSession": "session"}},
		{name: "matching", properties: map[string]string{"xdriveStream": "1", "xdriveSession": "session"}, want: true},
		{name: "unrelated properties", properties: map[string]string{"xdriveStream": "1", "xdriveSession": "session", "unrelated": "value"}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &driveStorage{}
			s.configureStreaming("session", false)
			s.observeStreamingProperties(tt.properties)
			if enabled := s.streamingEnabled(); enabled != tt.want {
				t.Fatalf("streamingEnabled() = %v, want %v", enabled, tt.want)
			}
			if properties := s.streamingProperties(); properties != nil {
				t.Fatalf("observing metadata caused a client advertisement: %v", properties)
			}
		})
	}
}

func TestDriveStreamCapsPropertiesAreIndependent(t *testing.T) {
	s := &driveStorage{}
	s.configureStreaming("session", true)
	properties := s.streamingProperties()
	properties["xdriveStream"] = "2"
	properties["xdriveSession"] = "other"
	properties["extra"] = "value"
	other := s.streamingProperties()
	if len(other) != 2 || other["xdriveStream"] != "1" || other["xdriveSession"] != "session" {
		t.Fatalf("caller mutation changed subsequent properties: %v", other)
	}
	client := &driveStorage{}
	client.configureStreaming("session", false)
	client.observeStreamingProperties(properties)
	if client.streamingEnabled() {
		t.Fatal("mutated server properties enabled the client writer")
	}
	client.observeStreamingProperties(other)
	if !client.streamingEnabled() {
		t.Fatal("original reader offer no longer matched after map mutation")
	}
}

func TestDriveStreamCapsMonotonicAndEpochReset(t *testing.T) {
	s := &driveStorage{}
	s.configureStreaming("first", false)
	first := s.streamCaps.Load()
	s.observeStreamingProperties(map[string]string{"xdriveStream": "1", "xdriveSession": "first"})
	s.configureStreaming("first", false)
	s.configureStreaming("", false)
	for _, properties := range []map[string]string{
		nil,
		{},
		{"xdriveStream": "0", "xdriveSession": "first"},
		{"xdriveStream": "1", "xdriveSession": "other"},
	} {
		s.observeStreamingProperties(properties)
		if !s.streamingEnabled() {
			t.Fatal("confirmed peer capability was revoked within the session")
		}
	}
	if s.streamCaps.Load() != first || first.advertise || s.streamingProperties() != nil {
		t.Fatal("same-nonce configuration replaced capability state or enabled client advertisement")
	}

	s.configureStreaming("second", false)
	second := s.streamCaps.Load()
	if second == first || second.session != "second" || s.streamingEnabled() {
		t.Fatal("different nonce did not start a fresh, unconfirmed epoch")
	}
	s.observeStreamingProperties(map[string]string{"xdriveStream": "1", "xdriveSession": "first"})
	first.peerReady.Store(true)
	if s.streamingEnabled() {
		t.Fatal("old-epoch properties or state enabled the new epoch")
	}
	s.configureStreaming("second", true)
	s.configureStreaming("second", false)
	if s.streamCaps.Load() != second || !s.streamingEnabled() || second.advertise || s.streamingProperties() != nil {
		t.Fatal("same-epoch peer offer was not retained independently of advertisement")
	}

	s.configureStreaming("third", true)
	third := s.streamCaps.Load()
	s.configureStreaming("third", false)
	properties := s.streamingProperties()
	if third == second || s.streamCaps.Load() != third || !third.advertise || !s.streamingEnabled() || len(properties) != 2 || properties["xdriveSession"] != "third" {
		t.Fatal("new accepted-offer epoch did not preserve its metadata acknowledgment")
	}
	s.configureStreaming("fourth", false)
	s.observeStreamingProperties(properties)
	if s.streamingEnabled() || s.streamCaps.Load().advertise || s.streamingProperties() != nil {
		t.Fatal("previous epoch's advertisement or readiness leaked into a fresh client epoch")
	}
}

func TestDriveStreamCapsSessionIsolation(t *testing.T) {
	// A nil owner prevents the shared keeper from starting. These sessions need
	// no network requests or credentials to exercise capability inheritance.
	root := &driveStorage{driveShared: &driveShared{}}
	root.configureStreaming("root", true)
	first := root.session().(*driveStorage)
	second := root.session().(*driveStorage)
	t.Cleanup(func() {
		first.Close()
		second.Close()
	})
	for _, child := range []*driveStorage{first, second} {
		if child.driveShared != root.driveShared {
			t.Fatal("session does not share its root's driveShared")
		}
		if child.streamCaps.Load() != nil || child.streamingEnabled() || child.streamingProperties() != nil || child.streamingFields() != "description" {
			t.Fatal("unconfigured child inherited the root's capability state")
		}
	}
	first.configureStreaming("first", false)
	second.configureStreaming("second", false)
	first.observeStreamingProperties(map[string]string{"xdriveStream": "1", "xdriveSession": "second"})
	second.observeStreamingProperties(root.streamingProperties())
	if first.streamingEnabled() || second.streamingEnabled() {
		t.Fatal("another session's reader offer enabled a child")
	}
	first.observeStreamingProperties(first.streamingProperties())
	if first.streamingEnabled() {
		t.Fatal("child's own outgoing metadata enabled its writer")
	}
	first.observeStreamingProperties(map[string]string{"xdriveStream": "1", "xdriveSession": "first"})
	if !first.streamingEnabled() || second.streamingEnabled() || !root.streamingEnabled() {
		t.Fatal("peer readiness leaked between storage instances")
	}
	if first.streamingProperties() != nil || second.streamingProperties() != nil || root.streamingProperties() == nil {
		t.Fatal("metadata advertisement leaked between storage instances")
	}
	if first.streamCaps.Load() == second.streamCaps.Load() || first.streamCaps.Load() == root.streamCaps.Load() {
		t.Fatal("configured sessions share capability pointers")
	}
	grandchild := first.session().(*driveStorage)
	t.Cleanup(func() { grandchild.Close() })
	if grandchild.streamCaps.Load() != nil || grandchild.streamingEnabled() || grandchild.streamingProperties() != nil {
		t.Fatal("nested session inherited capability state")
	}
}

func TestDriveStreamCapsLeaseMethods(t *testing.T) {
	backend := &driveStorage{}
	var storage Storage = &leasedDriveStorage{driveStorage: backend}
	caps, ok := storage.(interface {
		configureStreaming(string, bool)
		streamingEnabled() bool
		streamingProperties() map[string]string
		observeStreamingProperties(map[string]string)
		streamingFields() string
	})
	if !ok {
		t.Fatal("Drive lease does not expose the embedded capability helpers")
	}
	caps.configureStreaming("session", false)
	if caps.streamingEnabled() || caps.streamingFields() != "description,properties" {
		t.Fatal("lease did not configure the backend reader")
	}
	caps.observeStreamingProperties(caps.streamingProperties())
	if caps.streamingEnabled() || caps.streamingProperties() != nil {
		t.Fatal("lease enabled its backend from outgoing client metadata")
	}
	server := &driveStorage{}
	server.configureStreaming("session", true)
	caps.observeStreamingProperties(server.streamingProperties())
	if !caps.streamingEnabled() || !backend.streamingEnabled() {
		t.Fatal("lease did not update the backend peer capability")
	}
	if caps.streamingProperties() != nil {
		t.Fatal("confirmed client lease advertised a metadata acknowledgment")
	}
}

func TestDriveStreamCapsConcurrentConfiguration(t *testing.T) {
	s := &driveStorage{}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(peerSupports bool) {
			defer wg.Done()
			<-start
			s.configureStreaming("session", peerSupports)
		}(worker%2 == 0)
	}
	close(start)
	wg.Wait()
	caps := s.streamCaps.Load()
	if caps == nil || caps.session != "session" || !s.streamingEnabled() {
		t.Fatal("concurrent same-epoch configuration lost a peer reader offer")
	}
	advertise := caps.advertise
	s.configureStreaming("session", false)
	s.configureStreaming("session", true)
	if s.streamCaps.Load() != caps || !s.streamingEnabled() || caps.advertise != advertise || (s.streamingProperties() != nil) != advertise {
		t.Fatal("same-epoch configuration changed advertisement or revoked a confirmed offer")
	}
}

func TestDriveStreamCapsConcurrentObserveAndProperties(t *testing.T) {
	for _, tt := range []struct {
		name         string
		peerSupports bool
	}{
		{name: "client", peerSupports: false},
		{name: "accepted server", peerSupports: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := &driveStorage{}
			s.configureStreaming("session", tt.peerSupports)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for worker := 0; worker < 16; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					<-start
					for i := 0; i < 128; i++ {
						wasReady := s.streamingEnabled()
						switch worker % 4 {
						case 0:
							s.observeStreamingProperties(map[string]string{"xdriveStream": "1", "xdriveSession": "session"})
						case 1:
							s.observeStreamingProperties(map[string]string{"xdriveStream": "1", "xdriveSession": "other"})
						case 2:
							s.observeStreamingProperties(map[string]string{"xdriveStream": "2", "xdriveSession": "session"})
						case 3:
							s.configureStreaming("session", i%2 == 0)
						}
						properties := s.streamingProperties()
						if tt.peerSupports {
							if len(properties) != 2 || properties["xdriveStream"] != "1" || properties["xdriveSession"] != "session" {
								t.Errorf("concurrent server acknowledgment changed: %v", properties)
								return
							}
							properties["xdriveSession"] = "mutated"
							delete(properties, "xdriveStream")
						} else if properties != nil {
							t.Errorf("concurrent observation or reconfiguration enabled client advertisement: %v", properties)
							return
						}
						if s.streamingFields() != "description,properties" {
							t.Error("concurrent metadata fields changed")
							return
						}
						if wasReady && !s.streamingEnabled() {
							t.Error("concurrent observation revoked confirmed readiness")
							return
						}
					}
				}(worker)
			}
			close(start)
			wg.Wait()
			if !s.streamingEnabled() {
				t.Fatal("concurrent matching reader offer was lost")
			}
		})
	}
}
