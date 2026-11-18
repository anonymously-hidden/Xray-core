package xdrive

import (
	"context"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xtls/xray-core/transport/internet/stat"
)

type auditListingStorage struct {
	Storage
	announcements []Entry
	streams       []Entry
	mu            sync.Mutex
	deletes       []string
}

func (s *auditListingStorage) List(ctx context.Context, prefix string) ([]Entry, error) {
	if prefix == sessionsDir {
		return s.announcements, nil
	}
	if prefix == streamsDir && s.streams != nil {
		return s.streams, nil
	}
	return s.Storage.List(ctx, prefix)
}

func (s *auditListingStorage) Delete(ctx context.Context, name string) error {
	s.mu.Lock()
	s.deletes = append(s.deletes, name)
	s.mu.Unlock()
	return s.Storage.Delete(ctx, name)
}

func (s *auditListingStorage) deletedNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deletes...)
}

func TestXdriveAuditAnnounceComponents(t *testing.T) {
	at := time.Unix(1757000000, 123)
	for _, session := range []string{"abc123", "dead", "session-1", "fixture_1", "fixture.v1", "ABC-xyz_123"} {
		entry := strings.TrimPrefix(announceName(session, at), sessionsDir+"/")
		got, parsed, ok := parseAnnounce(entry)
		if !ok || got != session || !parsed.Equal(at) {
			t.Errorf("safe non-hex session %q was rejected or changed: %q, %v, %v", session, got, parsed, ok)
		}
	}
	for _, session := range []string{
		".", "..", "../live", "a/../live", "a/b", `/live`, `a\b`, `..\live`,
		"a~b", "a:b", "a\x00b", "a\nb", "a b", "session.", "session ",
		"NUL", "con", "CON.txt", "COM1", "lpt9.log", tempPrefix + "session",
	} {
		entry := strings.TrimPrefix(announceName(session, at), sessionsDir+"/")
		if _, _, ok := parseAnnounce(entry); ok {
			t.Errorf("unsafe session component %q was accepted", session)
		}
	}
}

func TestXdriveAuditUnsafeAnnouncementPreservesLiveStreams(t *testing.T) {
	for _, entry := range []string{"1-.", "1-..", "../streams", `..\streams`, "1-a~b"} {
		t.Run(entry, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := newTestListener(t, t.TempDir())
				store := &auditListingStorage{Storage: l.storage, announcements: []Entry{{Name: entry}}}
				l.storage = store
				l.active["live"] = true
				l.firstSeen = map[string]time.Time{entry: time.Now().Add(-2 * l.sessionTTL)}
				name := downlinkPrefix("live") + "/000000000.seg"
				if err := store.Put(l.ctx, name, []byte("live payload")); err != nil {
					t.Fatal(err)
				}
				accepted, err := l.acceptPending(l.ctx)
				l.cleanupWG.Wait()
				if err != nil || accepted {
					t.Fatalf("unsafe announcement was accepted: accepted=%v err=%v", accepted, err)
				}
				if got, err := store.Get(l.ctx, name); err != nil || string(got) != "live payload" {
					t.Errorf("unsafe listing %q deleted active stream data: %q, %v", entry, got, err)
				}
				if names := store.deletedNames(); len(names) != 0 {
					t.Errorf("unsafe listing %q reached deletion: %q", entry, names)
				}
			})
		})
	}
}

func TestXdriveAuditUnsafeCollectedSessionPreservesLiveStreams(t *testing.T) {
	for _, session := range []string{".", "..", "../streams", `..\streams`, "a~b"} {
		t.Run(session, func(t *testing.T) {
			l := newTestListener(t, t.TempDir())
			store := &auditListingStorage{Storage: l.storage, streams: []Entry{{Name: session}}}
			l.storage = store
			l.active["live"] = true
			l.idleSince[session] = time.Now().Add(-2 * l.sessionTTL)
			name := downlinkPrefix("live") + "/000000000.seg"
			if err := store.Put(l.ctx, name, []byte("live payload")); err != nil {
				t.Fatal(err)
			}
			if err := l.collect(); err != nil {
				t.Errorf("unsafe stream listing reached cleanup: %v", err)
			}
			if got, err := store.Get(l.ctx, name); err != nil || string(got) != "live payload" {
				t.Errorf("unsafe session %q deleted active stream data: %q, %v", session, got, err)
			}
			if names := store.deletedNames(); len(names) != 0 {
				t.Errorf("unsafe session %q reached deletion: %q", session, names)
			}
		})
	}
}

func TestXdriveAuditSafeMalformedAnnouncementStillDeleted(t *testing.T) {
	l := newTestListener(t, t.TempDir())
	store := &auditListingStorage{Storage: l.storage, announcements: []Entry{{Name: "not-an-announcement"}}}
	l.storage = store
	accepted, err := l.acceptPending(l.ctx)
	l.cleanupWG.Wait()
	if err != nil || accepted {
		t.Fatalf("malformed announcement was accepted: accepted=%v err=%v", accepted, err)
	}
	if names := store.deletedNames(); len(names) != 1 || names[0] != "sessions/not-an-announcement" {
		t.Fatalf("safe malformed announcement cleanup changed: %q", names)
	}
}

func TestXdriveAuditClaimResetsIdleSince(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newTestListener(t, t.TempDir())
		store := &auditListingStorage{Storage: l.storage, announcements: []Entry{{Name: "1-session"}}}
		l.storage = store
		l.idleSince["session"] = time.Now().Add(-2 * l.sessionTTL)
		name := downlinkPrefix("session") + "/000000000.seg"
		if err := store.Put(l.ctx, name, []byte("unread downlink")); err != nil {
			t.Fatal(err)
		}
		l.addConn = func(conn stat.Connection) { conn.(*Conn).Interrupt() }
		accepted, err := l.acceptPending(l.ctx)
		l.cleanupWG.Wait()
		if err != nil || !accepted {
			t.Fatalf("fresh non-hex session was not accepted: accepted=%v err=%v", accepted, err)
		}
		if l.active["session"] || l.handled["session"].IsZero() {
			t.Fatal("accepted session did not finish closing")
		}
		if since := l.idleSince["session"]; !since.IsZero() {
			t.Errorf("claim retained pre-accept idleSince: %v", since)
		}
		if err := l.collect(); err != nil {
			t.Fatal(err)
		}
		if got, err := store.Get(l.ctx, name); err != nil || string(got) != "unread downlink" {
			t.Errorf("collection deleted downlink immediately after close: %q, %v", got, err)
		}
		if names := store.deletedNames(); len(names) != 1 || names[0] != "sessions/1-session" {
			t.Errorf("post-close cleanup used pre-accept inactivity: %q", names)
		}
	})
}

func TestXdriveAuditRejectedClaimPreservesIdleSince(t *testing.T) {
	for _, guard := range []string{"active", "handled", "collecting", "stopping", "cancelled"} {
		t.Run(guard, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			old := time.Now().Add(-time.Hour)
			l := &Listener{ctx: ctx, idleSince: map[string]time.Time{"session": old}}
			switch guard {
			case "active":
				l.active = map[string]bool{"session": true}
			case "handled":
				l.handled = map[string]time.Time{"session": old}
			case "collecting":
				l.collecting = map[string]bool{"session": true}
			case "stopping":
				l.stopping = true
			case "cancelled":
				cancel()
			}
			if l.claim("session") || l.idleSince["session"] != old {
				t.Fatal("rejected claim changed inactivity eligibility")
			}
		})
	}
}
