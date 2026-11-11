package xdrive

import (
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type followupWriter struct{}

func (followupWriter) Write(p []byte) (int, error) { return len(p), nil }
func (followupWriter) Close() error                { return nil }
func (followupWriter) writeUntil(p []byte, _, _ <-chan struct{}) (int, error) {
	return len(p), nil
}

func followupConn() *Conn {
	return &Conn{writer: followupWriter{}, cancel: func() {}, readBuf: []byte("unread"),
		readDeadline: newPipeDeadline(), writeDeadline: newPipeDeadline(), closing: make(chan struct{})}
}

func TestConnBufferedReadState(t *testing.T) {
	c := followupConn()
	defer c.Close()
	b := make([]byte, 1)
	if n, err := c.Read(b); n != 1 || err != nil || b[0] != 'u' {
		t.Fatal(n, err, b)
	}
	c.SetReadDeadline(time.Now().Add(-time.Second))
	if n, err := c.Read(b); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("buffered read after deadline: (%d, %v)", n, err)
	}
	c.SetReadDeadline(time.Time{})
	if n, err := c.Read(b); n != 1 || err != nil || b[0] != 'n' {
		t.Fatalf("clearing deadline lost buffered bytes: (%d, %v), %q", n, err, b)
	}
	c.Close()
	if n, err := c.Read(b); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("buffered read after Close: (%d, %v)", n, err)
	}
}

// stop must not wait for a callback that has already started: that callback can
// be delayed arbitrarily, and waiting for it would stall Close. It also must not
// leave a channel that the late callback could close twice.
func TestDeadlineStopDoesNotJoinCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newPipeDeadline()
		entered, release := make(chan struct{}), make(chan struct{})
		panicked := make(chan any, 1)
		cancel := d.cancel
		d.timer = time.AfterFunc(time.Millisecond, func() {
			close(entered)
			<-release // callback already started, but has not closed cancel
			defer func() { panicked <- recover() }()
			close(cancel)
		})
		<-entered
		stopped := make(chan struct{})
		go func() { d.stop(); close(stopped) }()
		synctest.Wait()
		select {
		case <-stopped:
		default:
			t.Fatal("stop waited for a callback that had not completed")
		}
		d.set(time.Now().Add(-time.Second))
		close(release)
		if p := <-panicked; p != nil {
			t.Fatalf("callback panicked after stop: %v", p)
		}
	})
}

func TestConnCloseConcurrentDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := followupConn()
		var wg sync.WaitGroup
		for range 20 {
			wg.Go(func() {
				c.SetDeadline(time.Now().Add(time.Millisecond))
				time.Sleep(time.Millisecond)
				c.SetReadDeadline(time.Now().Add(-time.Millisecond))
				c.SetWriteDeadline(time.Time{})
			})
			wg.Go(func() { c.Close() })
		}
		wg.Wait()
	})
}
