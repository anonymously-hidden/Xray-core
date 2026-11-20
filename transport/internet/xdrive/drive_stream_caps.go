package xdrive

import "sync/atomic"

const (
	driveStreamCapsVersionKey = "xdriveStream"
	driveStreamCapsSessionKey = "xdriveSession"
	driveStreamCapsVersion    = "1"
)

// driveStreamCaps belongs to one driveStorage, never to driveShared. The nonce
// and advertisement decision are immutable after publication; peerReady only
// advances within that session.
// These properties negotiate a decoder, not authentication or payload integrity.
type driveStreamCaps struct {
	session   string
	advertise bool
	peerReady atomic.Bool
}

// configureStreaming records an optional peer reader offer. Only an initially
// accepted offer enables our metadata acknowledgment: clients offer their reader
// in the ring instead. Call this before starting session traffic. Repeating the
// same nonce preserves the advertisement decision and any confirmed offer; a
// different nonce starts a fresh capability epoch.
func (s *driveStorage) configureStreaming(session string, peerSupports bool) {
	if session == "" {
		return
	}
	for {
		current := s.streamCaps.Load()
		if current != nil && current.session == session {
			if peerSupports {
				current.peerReady.Store(true)
			}
			return
		}
		next := &driveStreamCaps{session: session, advertise: peerSupports}
		next.peerReady.Store(peerSupports)
		if s.streamCaps.CompareAndSwap(current, next) {
			return
		}
	}
}

// streamingEnabled gates only the writer, independently of whether this endpoint
// acknowledges an accepted ring offer in its metadata.
func (s *driveStorage) streamingEnabled() bool {
	caps := s.streamCaps.Load()
	return caps != nil && caps.peerReady.Load()
}

// streamingProperties acknowledges an initially accepted ring offer in public
// Files.properties on existing slot/watermark metadata requests, so peers using
// different OAuth applications can see it. Clients never write this marker, even
// after observing the server's acknowledgment: duplicate-create recovery also
// reads outgoing slots, so advertising there would let clients bootstrap themselves.
// Callers may merge or modify the fresh map without changing capability state.
func (s *driveStorage) streamingProperties() map[string]string {
	caps := s.streamCaps.Load()
	if caps == nil || !caps.advertise {
		return nil
	}
	return map[string]string{
		driveStreamCapsVersionKey: driveStreamCapsVersion,
		driveStreamCapsSessionKey: caps.session,
	}
}

// observeStreamingProperties accepts only this session's exact reader version.
// Missing, stale, or unknown properties leave the writer's capability unchanged.
func (s *driveStorage) observeStreamingProperties(properties map[string]string) {
	caps := s.streamCaps.Load()
	if caps != nil && properties[driveStreamCapsVersionKey] == driveStreamCapsVersion && properties[driveStreamCapsSessionKey] == caps.session {
		caps.peerReady.Store(true)
	}
}

// streamingFields adds public Files.properties to the existing description read,
// without another GET.
func (s *driveStorage) streamingFields() string {
	if s.streamCaps.Load() != nil {
		return "description,properties"
	}
	return "description"
}
