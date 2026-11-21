package xdrive

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"testing"
	"testing/synctest"
	"time"
)

type streamReaderFixture struct {
	data       []byte
	frame      slotFrame
	tail       chan struct{}
	eof        chan struct{}
	acks       chan int64
	legacy     bool
	firstDelay time.Duration
}

func newStreamReaderFixture(data []byte) *streamReaderFixture {
	return &streamReaderFixture{
		data: data, frame: newSlotFrame(0, slotStream, data),
		tail: make(chan struct{}), eof: make(chan struct{}), acks: make(chan int64, 8),
	}
}

func (s *streamReaderFixture) makeSlot(context.Context, string) (string, error) { return "unused", nil }

func (s *streamReaderFixture) createSlot(ctx context.Context, id, name, description string, body []byte) error {
	return s.setSlot(ctx, id, description, body)
}

func (s *streamReaderFixture) setSlot(ctx context.Context, id, description string, body []byte) error {
	if id == "wm" {
		value, err := strconv.ParseInt(description, 10, 64)
		if err != nil {
			return err
		}
		select {
		case s.acks <- value:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *streamReaderFixture) readDescription(ctx context.Context, id string) (string, error) {
	switch id {
	case "in0":
		if s.legacy {
			return newSlotFrame(0, slotMedia, s.data).encode(), nil
		}
		return s.frame.encode(), nil
	case "in1":
		return newSlotFrame(1, slotEnd, nil).encode(), nil
	default:
		return "", nil
	}
}

func (s *streamReaderFixture) readBody(context.Context, string) ([]byte, error) {
	return bytes.Clone(s.data), nil
}

func (s *streamReaderFixture) readFrameStream(ctx context.Context, id string, frame slotFrame, emit func(int, []byte) error) error {
	if err := batchTestWait(ctx, s.firstDelay); err != nil {
		return err
	}
	if err := emit(0, bytes.Clone(s.data[:streamBlockSize(frame.size, 0)])); err != nil {
		return err
	}
	select {
	case <-s.tail:
	case <-ctx.Done():
		return ctx.Err()
	}
	for index := 1; index < len(frame.blocks); index++ {
		start := index * slotStreamBlockBytes
		if err := emit(index, bytes.Clone(s.data[start:start+streamBlockSize(frame.size, index)])); err != nil {
			return err
		}
	}
	select {
	case <-s.eof:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func streamReaderParams() params {
	return params{
		concurrency: 2, segmentBytes: 512 << 10, flushInterval: 10 * time.Millisecond,
		minPollInterval: 50 * time.Millisecond, maxPollInterval: 200 * time.Millisecond,
		eagerWindow: 2 * time.Second, holeTimeout: 30 * time.Second, sessionTTL: time.Minute,
	}
}

func receiveStreamChunk(t *testing.T, reader *slotReader) []byte {
	t.Helper()
	select {
	case value, ok := <-reader.chunks():
		if !ok {
			t.Fatalf("reader closed early: %v", reader.Err())
		}
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for verified block")
	}
	return nil
}

func TestStreamReaderPrefixBeforeEOFAndAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := bytes.Repeat([]byte("s"), 2*slotStreamBlockBytes+73)
		store := newStreamReaderFixture(data)
		ctx, cancel := context.WithCancel(context.Background())
		reader := newSlotReader(ctx, store, slotTable{Slots: []string{"in0", "in1"}, Watermark: "wm"}, streamReaderParams(), nil)
		defer func() { cancel(); reader.wait() }()
		first := receiveStreamChunk(t, reader)
		if !bytes.Equal(first, data[:slotStreamBlockBytes]) {
			t.Fatal("first block differs")
		}
		synctest.Wait()
		_, active, outstanding, buffered, _ := reader.bodies.snapshot()
		if active != 1 || outstanding != len(data)-len(first) || buffered != 0 {
			t.Fatal("verified prefix did not return only its own ordered byte credit")
		}
		select {
		case ack := <-store.acks:
			t.Fatalf("premature ACK before body tail: %d", ack)
		default:
		}
		close(store.tail)
		second, third := receiveStreamChunk(t, reader), receiveStreamChunk(t, reader)
		if !bytes.Equal(second, data[slotStreamBlockBytes:2*slotStreamBlockBytes]) || !bytes.Equal(third, data[2*slotStreamBlockBytes:]) {
			t.Fatal("remaining blocks differ")
		}
		synctest.Wait()
		_, active, outstanding, buffered, _ = reader.bodies.snapshot()
		if active != 1 || outstanding != 0 || buffered != 0 {
			t.Fatal("last handoff returned HTTP/EOF credit or retained byte credit")
		}
		select {
		case ack := <-store.acks:
			t.Fatalf("premature ACK before validated EOF: %d", ack)
		case <-time.After(25 * time.Millisecond):
		}
		close(store.eof)
		select {
		case ack := <-store.acks:
			if ack != 0 {
				t.Fatalf("ACK=%d, want 0", ack)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("no completion ACK")
		}
		select {
		case _, ok := <-reader.chunks():
			if ok {
				t.Fatal("unexpected extra chunk")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("END not delivered")
		}
		if reader.Err() != io.EOF {
			t.Fatalf("reader error: %v", reader.Err())
		}
		cancel()
		reader.wait()
		_, active, outstanding, buffered, _ = reader.bodies.snapshot()
		if active != 0 || outstanding != 0 || buffered != 0 {
			t.Fatal("completed reader retained body credit")
		}
	})
}

func TestStreamReaderCancelWhilePartial(t *testing.T) {
	store := newStreamReaderFixture(bytes.Repeat([]byte("c"), 2*slotStreamBlockBytes))
	ctx, cancel := context.WithCancel(context.Background())
	reader := newSlotReader(ctx, store, slotTable{Slots: []string{"in0", "in1"}, Watermark: "wm"}, streamReaderParams(), nil)
	_ = receiveStreamChunk(t, reader)
	cancel()
	done := make(chan struct{})
	go func() { reader.wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("canceled streaming reader did not join")
	}
	select {
	case ack := <-store.acks:
		t.Fatalf("partial canceled frame ACKed: %d", ack)
	default:
	}
}

func TestStreamBufferOutOfOrderAndDuplicate(t *testing.T) {
	data := bytes.Repeat([]byte("d"), 2*slotStreamBlockBytes+91)
	frame := newSlotFrame(0, slotStream, data)
	buffer := newSlotStreamBuffer(frame)
	block := func(index int) []byte {
		start := index * slotStreamBlockBytes
		return bytes.Clone(data[start : start+streamBlockSize(frame.size, index)])
	}
	if err := buffer.offer(2, block(2)); err != nil {
		t.Fatal(err)
	}
	if value, _ := buffer.take(); value != nil {
		t.Fatal("out-of-order block escaped")
	}
	if err := buffer.offer(0, block(0)); err != nil {
		t.Fatal(err)
	}
	if value, _ := buffer.take(); !bytes.Equal(value, block(0)) {
		t.Fatal("first block differs")
	}
	if err := buffer.offer(0, block(0)); err != nil {
		t.Fatal(err)
	}
	if value, _ := buffer.take(); value != nil {
		t.Fatal("consumed duplicate replayed")
	}
	bad := block(1)
	bad[0] ^= 1
	if err := buffer.offer(1, bad); err == nil {
		t.Fatal("corrupt block accepted")
	}
	if err := buffer.offer(1, block(1)); err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{1, 2} {
		if value, _ := buffer.take(); !bytes.Equal(value, block(index)) {
			t.Fatalf("block %d differs", index)
		}
	}
	if value, all := buffer.take(); value != nil || !all || !buffer.complete() {
		t.Fatal("incomplete block accounting")
	}
}

func TestStreamReaderLegacyMediaUnchanged(t *testing.T) {
	store := newStreamReaderFixture(bytes.Repeat([]byte("l"), 2*slotStreamBlockBytes+17))
	store.legacy = true
	ctx, cancel := context.WithCancel(context.Background())
	reader := newSlotReader(ctx, store, slotTable{Slots: []string{"in0", "in1"}, Watermark: "wm"}, streamReaderParams(), nil)
	defer func() { cancel(); reader.wait() }()
	if value := receiveStreamChunk(t, reader); !bytes.Equal(value, store.data) {
		t.Fatal("legacy media delivery changed")
	}
}

func TestStreamReaderMetadataPrefixAheadOfMedia(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		t.Run(strconv.FormatBool(prefix), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				table := slotTable{Slots: []string{"in0", "in1"}, Watermark: "wm", streamPrefix: prefix}
				body := streamTestData(slotStreamBlockBytes + 17)
				store := newStreamReaderFixture(body)
				store.firstDelay = 900 * time.Millisecond
				if prefix {
					store.frame.prefix = bytes.Repeat([]byte("p"), slotStreamPrefixBytes)
				}
				began := time.Now()
				reader := newSlotReader(ctx, store, table, streamReaderParams(), nil)
				defer func() { cancel(); reader.wait() }()
				first := receiveStreamChunk(t, reader)
				elapsed := time.Since(began)
				t.Logf("first handoff: %v", elapsed)
				if prefix {
					if !bytes.Equal(first, store.frame.prefix) || elapsed >= store.firstDelay {
						t.Fatal("the metadata bytes waited for the media request")
					}
					synctest.Wait()
					_, active, outstanding, buffered, _ := reader.bodies.snapshot()
					if active != 1 || outstanding != len(body) || buffered != 0 {
						t.Fatalf("prefix did not return just its own credit: %d/%d/%d", active, outstanding, buffered)
					}
					first = receiveStreamChunk(t, reader)
				} else if elapsed < store.firstDelay {
					t.Fatal("legacy reader did not wait for media")
				}
				if !bytes.Equal(first, body[:slotStreamBlockBytes]) {
					t.Fatal("first media block differs")
				}
				close(store.tail)
				if last := receiveStreamChunk(t, reader); !bytes.Equal(last, body[slotStreamBlockBytes:]) {
					t.Fatal("last media block differs")
				}
				select {
				case ack := <-store.acks:
					t.Fatalf("premature ACK before media EOF: %d", ack)
				case <-time.After(25 * time.Millisecond):
				}
				close(store.eof)
				select {
				case ack := <-store.acks:
					if ack != 0 {
						t.Fatalf("ACK=%d, want 0", ack)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("no completion ACK")
				}
				if _, ok := <-reader.chunks(); ok || reader.Err() != io.EOF {
					t.Fatal("reader failed to stop at the ordered END")
				}
			})
		})
	}
}

func TestStreamReaderRejectsUnnegotiatedMetadataPrefix(t *testing.T) {
	store := newStreamReaderFixture(streamTestData(slotStreamBlockBytes))
	store.frame.prefix = make([]byte, slotStreamPrefixBytes)
	ctx, cancel := context.WithCancel(context.Background())
	reader := newSlotReader(ctx, store, slotTable{Slots: []string{"in0", "in1"}, Watermark: "wm"}, streamReaderParams(), nil)
	defer func() { cancel(); reader.wait() }()
	select {
	case _, ok := <-reader.chunks():
		if ok || reader.Err() == nil || reader.Err() == io.EOF {
			t.Fatal("unnegotiated frame was accepted")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reader did not reject unnegotiated frame")
	}
}
