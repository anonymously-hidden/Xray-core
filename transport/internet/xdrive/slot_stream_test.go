package xdrive

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math/rand"
	"strconv"
	"testing"
	"testing/synctest"
	"time"
)

func streamTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*31 + i/251)
	}
	return data
}

func TestStreamBlockChecksums(t *testing.T) {
	for _, size := range []int{0, 1, slotStreamBlockBytes - 1, slotStreamBlockBytes, slotStreamBlockBytes + 1, 2*slotStreamBlockBytes + 17, maxSegmentBytes} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			data := streamTestData(size)
			checks := streamBlockChecksums(data)
			count := 0
			if size > 0 {
				count = (size-1)/slotStreamBlockBytes + 1
			}
			if len(checks) != count {
				t.Fatalf("got %d checksums, want %d", len(checks), count)
			}
			for i, check := range checks {
				start := i * slotStreamBlockBytes
				end := min(start+slotStreamBlockBytes, size)
				if want := crc32.ChecksumIEEE(data[start:end]); check != want {
					t.Fatalf("block %d: got %08x, want %08x", i, check, want)
				}
			}
			decoded, ok := decodeStreamChecksums(size, crc32.ChecksumIEEE(data), appendStreamChecksums(nil, checks))
			if size == 0 {
				if checks != nil || ok || decoded != nil {
					t.Fatal("empty data must have no table and cannot form a stream frame")
				}
				return
			}
			if !ok || len(decoded) != len(checks) {
				t.Fatal("valid checksum table did not round-trip")
			}
			for i := range checks {
				if decoded[i] != checks[i] {
					t.Fatalf("decoded block %d changed", i)
				}
			}
		})
	}
}

func TestAppendStreamChecksums(t *testing.T) {
	header := []byte("XDRVSL01-header-prefix")
	checks := []uint32{0x01234567, 0x89abcdef, 0, 0xffffffff}
	encoded := appendStreamChecksums(bytes.Clone(header), checks)
	if len(encoded) != len(header)+4*len(checks) || !bytes.Equal(encoded[:len(header)], header) {
		t.Fatal("appending a table changed the header or encoded length")
	}
	want := []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff}
	if !bytes.Equal(encoded[len(header):], want) {
		t.Fatalf("table is not big-endian: %x", encoded[len(header):])
	}
	if got := appendStreamChecksums(bytes.Clone(header), nil); !bytes.Equal(got, header) {
		t.Fatal("an empty table changed the header")
	}
}

func TestDecodeStreamChecksumsRejectsInvalidMetadata(t *testing.T) {
	data := streamTestData(2*slotStreamBlockBytes + 19)
	whole := crc32.ChecksumIEEE(data)
	trailer := appendStreamChecksums(nil, streamBlockChecksums(data))
	corrupt := bytes.Clone(trailer)
	corrupt[0] ^= 1
	cases := []struct {
		name    string
		size    int
		whole   uint32
		trailer []byte
	}{
		{"negative_size", -1, whole, trailer},
		{"empty_size", 0, whole, trailer},
		{"oversized_header", maxSegmentBytes + 1, whole, trailer},
		{"missing_table", len(data), whole, nil},
		{"short_table", len(data), whole, trailer[:len(trailer)-1]},
		{"extra_byte", len(data), whole, append(bytes.Clone(trailer), 0)},
		{"extra_checksum", len(data), whole, append(bytes.Clone(trailer), 0, 0, 0, 0)},
		{"corrupt_header_crc", len(data), whole ^ 1, trailer},
		{"corrupt_table", len(data), whole, corrupt},
		{"wrong_last_block_length", len(data) + 1, whole, trailer},
		{"wrong_header_block_count", 2 * slotStreamBlockBytes, whole, trailer},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if checks, ok := decodeStreamChecksums(tc.size, tc.whole, tc.trailer); ok || checks != nil {
				t.Fatal("invalid stream metadata was accepted")
			}
		})
	}
}

func TestStreamChecksumsHeaderFields(t *testing.T) {
	data := streamTestData(slotStreamBlockBytes + 7)
	header := make([]byte, 25)
	copy(header, "XDRVSL01")
	header[16] = slotStream
	binary.BigEndian.PutUint32(header[17:21], uint32(len(data)))
	binary.BigEndian.PutUint32(header[21:25], crc32.ChecksumIEEE(data))
	encoded := appendStreamChecksums(header, streamBlockChecksums(data))
	decode := func(raw []byte) bool {
		_, ok := decodeStreamChecksums(int(binary.BigEndian.Uint32(raw[17:21])), binary.BigEndian.Uint32(raw[21:25]), raw[25:])
		return ok
	}
	if !decode(encoded) {
		t.Fatal("table rejected matching fields from the unchanged 25-byte header")
	}
	for _, offset := range []int{20, 24, 25} {
		corrupt := bytes.Clone(encoded)
		corrupt[offset] ^= 1
		if decode(corrupt) {
			t.Fatalf("accepted corruption at header/table byte %d", offset)
		}
	}
}

func TestStreamBlockSize(t *testing.T) {
	cases := []struct {
		size, index, want int
	}{
		{0, 0, 0},
		{-1, 0, 0},
		{1, -1, 0},
		{1, 0, 1},
		{1, 1, 0},
		{slotStreamBlockBytes, 0, slotStreamBlockBytes},
		{slotStreamBlockBytes, 1, 0},
		{slotStreamBlockBytes + 7, 0, slotStreamBlockBytes},
		{slotStreamBlockBytes + 7, 1, 7},
		{slotStreamBlockBytes + 7, 2, 0},
		{maxSegmentBytes, 511, slotStreamBlockBytes},
		{maxSegmentBytes, 512, 0},
		{maxSegmentBytes + 1, 0, 0},
	}
	for _, tc := range cases {
		if got := streamBlockSize(tc.size, tc.index); got != tc.want {
			t.Fatalf("size=%d index=%d: got %d, want %d", tc.size, tc.index, got, tc.want)
		}
	}
}

func TestValidStreamBlock(t *testing.T) {
	data := streamTestData(2*slotStreamBlockBytes + 19)
	whole := crc32.ChecksumIEEE(data)
	checks, ok := decodeStreamChecksums(len(data), whole, appendStreamChecksums(nil, streamBlockChecksums(data)))
	if !ok {
		t.Fatal("valid table was rejected")
	}
	frame := slotFrame{mode: slotStream, size: len(data), crc: whole, blocks: checks}
	for i := range checks {
		start := i * slotStreamBlockBytes
		block := data[start:min(start+slotStreamBlockBytes, len(data))]
		if !validStreamBlock(frame, i, block) {
			t.Fatalf("valid block %d was rejected", i)
		}
		if validStreamBlock(frame, i, block[:len(block)-1]) || validStreamBlock(frame, i, append(bytes.Clone(block), 0)) {
			t.Fatalf("block %d accepted the wrong length", i)
		}
		for _, offset := range []int{0, len(block) / 2, len(block) - 1} {
			corrupt := bytes.Clone(block)
			corrupt[offset] ^= 1
			if validStreamBlock(frame, i, corrupt) {
				t.Fatalf("block %d accepted corruption at byte %d", i, offset)
			}
		}
	}
	first := data[:slotStreamBlockBytes]
	if validStreamBlock(frame, -1, first) || validStreamBlock(frame, len(checks), first) {
		t.Fatal("accepted an invalid block index")
	}
	bad := frame
	bad.mode = slotMedia
	if validStreamBlock(bad, 0, first) {
		t.Fatal("accepted a legacy media frame as a stream frame")
	}
	bad = frame
	bad.blocks = checks[:len(checks)-1]
	if validStreamBlock(bad, 0, first) {
		t.Fatal("accepted an incomplete table")
	}
	bad.blocks = append(append([]uint32(nil), checks...), 0)
	if validStreamBlock(bad, 0, first) {
		t.Fatal("accepted an oversized table")
	}
	bad = frame
	bad.blocks = append([]uint32(nil), checks...)
	bad.blocks[0] ^= 1
	if validStreamBlock(bad, 0, first) {
		t.Fatal("accepted a block with the wrong checksum")
	}
	for _, size := range []int{-1, 0, maxSegmentBytes + 1} {
		bad = frame
		bad.size = size
		if validStreamBlock(bad, 0, first) {
			t.Fatalf("accepted invalid frame size %d", size)
		}
	}
}

func TestCRC32Combine(t *testing.T) {
	for _, size := range []int{0, 1, 2, 31, 257, slotStreamBlockBytes, slotStreamBlockBytes + 1, 2*slotStreamBlockBytes + 17} {
		data := streamTestData(size)
		want := crc32.ChecksumIEEE(data)
		for _, split := range []int{0, min(1, size), size / 2, max(0, size-1), size} {
			first := crc32.ChecksumIEEE(data[:split])
			second := crc32.ChecksumIEEE(data[split:])
			if got := crc32Combine(first, second, size-split); got != want {
				t.Fatalf("size=%d split=%d: got %08x, want %08x", size, split, got, want)
			}
		}
	}
	if got := crc32Combine(0x12345678, 0xabcdef01, 0); got != 0x12345678 {
		t.Fatal("zero-length concatenation changed the first CRC")
	}
}

func TestCRC32CombineRandomSplits(t *testing.T) {
	rng := rand.New(rand.NewSource(0x58445256))
	for iteration := 0; iteration < 64; iteration++ {
		data := make([]byte, rng.Intn(4*slotStreamBlockBytes+1))
		if _, err := rng.Read(data); err != nil {
			t.Fatal(err)
		}
		var combined uint32
		for offset := 0; offset < len(data); {
			n := 1 + rng.Intn(min(len(data)-offset, 2*slotStreamBlockBytes))
			combined = crc32Combine(combined, crc32.ChecksumIEEE(data[offset:offset+n]), n)
			offset += n
			if want := crc32.ChecksumIEEE(data[:offset]); combined != want {
				t.Fatalf("iteration=%d prefix=%d: got %08x, want %08x", iteration, offset, combined, want)
			}
		}
		if combined != crc32.ChecksumIEEE(data) {
			t.Fatalf("iteration %d: concatenated CRC differs from whole bytes", iteration)
		}
		checks, ok := decodeStreamChecksums(len(data), combined, appendStreamChecksums(nil, streamBlockChecksums(data)))
		if len(data) == 0 {
			if ok || checks != nil {
				t.Fatal("accepted an empty random frame")
			}
			continue
		}
		if !ok {
			t.Fatalf("iteration %d: random block table was rejected", iteration)
		}
		frame := slotFrame{mode: slotStream, size: len(data), crc: combined, blocks: checks}
		for i := range checks {
			start := i * slotStreamBlockBytes
			if !validStreamBlock(frame, i, data[start:min(start+slotStreamBlockBytes, len(data))]) {
				t.Fatalf("iteration=%d block=%d: valid random block was rejected", iteration, i)
			}
		}
	}
}

func TestStreamMetadataPrefixBoundsAndChecksums(t *testing.T) {
	payload := streamTestData(maxSegmentBytes)
	frame := newSlotFrame(31, slotStream, payload[slotStreamPrefixBytes:])
	frame.prefix = payload[:slotStreamPrefixBytes]
	encoded := frame.encode()
	// The largest metadata prefix fits within the size already used for inline descriptions.
	if len(encoded) > base64.StdEncoding.EncodedLen(slotHeaderLen+slotInlineLimit) {
		t.Fatal("description exceeded the existing inline envelope")
	}
	got, ok := decodeSlot(encoded)
	if !ok || got.seq != frame.seq || got.mode != slotStream || !bytes.Equal(got.prefix, frame.prefix) || !got.holds(payload[slotStreamPrefixBytes:]) {
		t.Fatal("prefix, media size, or checksum changed on the wire")
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if raw[16] != slotStreamPrefix {
		t.Fatal("the new format was marked as ordinary streaming")
	}
	for _, offset := range []int{slotHeaderLen, len(raw) - slotStreamPrefixBytes - 1, len(raw) - 1} {
		corrupt := bytes.Clone(raw)
		corrupt[offset] ^= 1
		if _, ok := decodeSlot(base64.StdEncoding.EncodeToString(corrupt)); ok {
			t.Fatalf("accepted broken metadata at byte %d", offset)
		}
	}
	if _, ok := decodeSlot(base64.StdEncoding.EncodeToString(raw[:len(raw)-1])); ok {
		t.Fatal("accepted a truncated prefix")
	}
}

func BenchmarkDecodeStreamChecksums(b *testing.B) {
	data := streamTestData(defaultSegmentBytes - 1)
	check := crc32.ChecksumIEEE(data)
	trailer := appendStreamChecksums(nil, streamBlockChecksums(data))
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := decodeStreamChecksums(len(data), check, trailer); !ok {
			b.Fatal("failed to verify checksum table")
		}
	}
}

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
