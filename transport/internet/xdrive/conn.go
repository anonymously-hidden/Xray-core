package xdrive

import (
	"context"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/net"
)

var placeholderAddr = &net.TCPAddr{IP: net.IP{127, 0, 0, 1}, Port: 0}

// Conn is a session in segments, walWriter and walReader, or in slots,
// slotWriter and slotReader
type Conn struct {
	ctx     context.Context
	fail    context.CancelCauseFunc // first transport error wins; nil means ordinary cancellation
	writer  connWriter
	reader  chunkReader
	onClose func()

	readMu  sync.Mutex
	readBuf []byte

	readDeadline  *pipeDeadline
	writeDeadline *pipeDeadline
	closing       chan struct{}
	progress      atomic.Pointer[time.Time] // Last outbound or graceful drain progress.

	closeOnce sync.Once
	closeErr  error
}

type connWriter interface {
	io.Closer
	// writeUntil returns errWriteStopped, with the bytes it took, once either
	// channel is closed
	writeUntil(p []byte, deadline, closing <-chan struct{}) (int, error)
}

type chunkReader interface {
	// chunks is closed when there are no more, then Err says why
	chunks() <-chan []byte
	Err() error
	Wake()
	wait()
}

func newConn(ctx context.Context, storage Storage, writePrefix, readPrefix string, p params, discard bool, onClose func()) *Conn {
	c := baseConn(ctx, onClose)
	c.writer = newWALWriter(c.ctx, storage, writePrefix, p)
	c.reader = newWALReader(c.ctx, storage, readPrefix, p, discard)
	return c
}

func newSlotConn(ctx context.Context, store *driveStorage, write, read slotTable, p params, onClose func()) *Conn {
	c := baseConn(ctx, onClose)
	r := &slotReader{slotDirection: slotDirection{c, store, read, p}, ch: make(chan []byte, 2), done: make(chan struct{})}
	c.reader, c.writer = r, newSlotWriter(slotDirection{c, store, write, p})
	go r.run()
	return c
}

func baseConn(ctx context.Context, onClose func()) *Conn {
	ctx, fail := context.WithCancelCause(ctx)
	return &Conn{
		ctx: ctx, fail: fail, onClose: onClose, closing: make(chan struct{}),
		readDeadline: newPipeDeadline(), writeDeadline: newPipeDeadline(),
	}
}

func (c *Conn) Read(b []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	deadline := c.readDeadline.wait()
	if isClosedChan(c.closing) {
		return 0, io.ErrClosedPipe
	}
	if isClosedChan(deadline) {
		return 0, os.ErrDeadlineExceeded
	}
	if len(b) == 0 {
		return 0, nil
	}
	if len(c.readBuf) == 0 {
		select {
		case data, ok := <-c.reader.chunks():
			if !ok {
				return 0, c.reader.Err()
			}
			c.readBuf = data
		case <-c.closing:
			return 0, io.ErrClosedPipe
		case <-deadline:
			return 0, os.ErrDeadlineExceeded
		}
	}
	n := copy(b, c.readBuf)
	c.readBuf = c.readBuf[n:]
	if len(c.readBuf) == 0 {
		c.readBuf = nil
	}
	return n, nil
}

func (c *Conn) Write(b []byte) (int, error) {
	if isClosedChan(c.closing) {
		return 0, io.ErrClosedPipe
	}
	deadline := c.writeDeadline.wait()
	if isClosedChan(deadline) {
		return 0, os.ErrDeadlineExceeded
	}

	n, err := c.writer.writeUntil(b, deadline, c.closing)
	if err == errWriteStopped {
		select {
		case <-c.closing:
			err = io.ErrClosedPipe
		default:
			err = os.ErrDeadlineExceeded
		}
	}
	if n > 0 {
		c.reader.Wake()
	}
	return n, err
}

func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closing)
		c.readDeadline.stop()
		c.writeDeadline.stop()
		result := make(chan error, 2) // The ordered close and its watchdog can each report once.
		c.markProgress()
		go func() { result <- c.closeOrdered() }()
		for wait := slotStallTimeout; wait > 0; {
			select {
			case c.closeErr = <-result:
				return
			case <-time.After(wait):
			}
			last := *c.progress.Load()
			wait = slotStallTimeout - time.Since(last)
			if w, ok := c.writer.(*slotWriter); ok {
				wait = w.remaining(last)
			}
		}
		result <- context.DeadlineExceeded
		c.closeErr = <-result // A completion already in the queue wins.
		c.fail(nil)
	})
	return c.closeErr
}

func (c *Conn) closeOrdered() error {
	defer c.onClose()
	err := c.writer.Close()
	if _, slots := c.writer.(*slotWriter); slots {
		c.reader.wait() // Drain through END, including the final watermark.
		err = context.Cause(c.ctx)
	} else {
		defer c.reader.wait() // WAL polling stops on cancellation.
	}
	c.fail(nil)
	return err
}

func (c *Conn) Interrupt() {
	c.fail(nil)
	c.Close()
}

func (c *Conn) LocalAddr() net.Addr {
	return placeholderAddr
}

func (c *Conn) RemoteAddr() net.Addr {
	return placeholderAddr
}

func (c *Conn) SetDeadline(t time.Time) error {
	c.readDeadline.set(t)
	c.writeDeadline.set(t)
	return nil
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.readDeadline.set(t)
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline.set(t)
	return nil
}

// pipeDeadline is a deadline whose wait channel closes when it passes. Setting
// a new time re-arms it, so a call already waiting on the channel sees the
// change.
type pipeDeadline struct {
	mu      sync.Mutex
	timer   *time.Timer
	cancel  chan struct{}
	gen     uint64
	stopped bool
}

func newPipeDeadline() *pipeDeadline {
	return &pipeDeadline{cancel: make(chan struct{})}
}

func (d *pipeDeadline) set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		return
	}

	if d.timer != nil {
		// Stop does not join a callback that has already started; gen makes
		// that callback skip its close, so a channel is closed at most once.
		d.timer.Stop()
		d.timer = nil
		d.gen++
	}

	closed := isClosedChan(d.cancel)
	if t.IsZero() {
		if closed {
			d.cancel = make(chan struct{})
		}
		return
	}
	if dur := time.Until(t); dur > 0 {
		if closed {
			d.cancel = make(chan struct{})
		}
		cancel, gen := d.cancel, d.gen
		d.timer = time.AfterFunc(dur, func() {
			d.mu.Lock()
			if d.gen == gen {
				close(cancel)
			}
			d.mu.Unlock()
		})
		return
	}
	if !closed {
		close(d.cancel)
		d.gen++
	}
}

func (d *pipeDeadline) stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	d.gen++ // an in-flight callback must not close a channel reused later
	d.stopped = true
}

func (d *pipeDeadline) wait() chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cancel
}

func isClosedChan(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}
