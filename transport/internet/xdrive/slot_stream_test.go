package xdrive

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math/rand"
	"strconv"
	"testing"
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
