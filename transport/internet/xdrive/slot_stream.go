package xdrive

import (
	"encoding/binary"
	"hash/crc32"
	"math/bits"
)

const (
	slotStream       byte = 5
	slotStreamPrefix byte = 6
	// Leave room for checksums and headers under the existing inline description size.
	slotStreamPrefixBytes = 11 << 10
	slotStreamBlockBytes  = 32 << 10
	// slotProgressiveMax bounds a frame read progressively by one GET.
	slotProgressiveMax = defaultSegmentBytes
)

// streamBlockChecksums covers each fixed-size block, including the final short
// block. An empty payload has no checksums and is not a valid stream frame.
func streamBlockChecksums(data []byte) []uint32 {
	if len(data) == 0 {
		return nil
	}
	checks := make([]uint32, (len(data)-1)/slotStreamBlockBytes+1)
	for i := range checks {
		start := i * slotStreamBlockBytes
		checks[i] = crc32.ChecksumIEEE(data[start:min(start+slotStreamBlockBytes, len(data))])
	}
	return checks
}

// Validate the whole CRC table before allowing progressive delivery.
func decodeStreamChecksums(size int, wholeCRC uint32, trailer []byte) ([]uint32, bool) {
	if size <= 0 || size > maxSegmentBytes {
		return nil, false
	}
	count := (size-1)/slotStreamBlockBytes + 1
	if len(trailer) != 4*count {
		return nil, false
	}
	checks := make([]uint32, count)
	var combined uint32
	for i := range checks {
		checks[i] = binary.BigEndian.Uint32(trailer[4*i:])
		if i == 0 {
			combined = checks[i]
		} else {
			combined = crc32Combine(combined, checks[i], streamBlockSize(size, i))
		}
	}
	if combined != wholeCRC {
		return nil, false
	}
	return checks, true
}

// streamBlockSize returns zero for a size or index outside a stream frame.
func streamBlockSize(size, index int) int {
	if size <= 0 || size > maxSegmentBytes || index < 0 || index >= (size-1)/slotStreamBlockBytes+1 {
		return 0
	}
	return min(slotStreamBlockBytes, size-index*slotStreamBlockBytes)
}

// The caller has already validated the checksum table root.
func validStreamBlock(frame slotFrame, index int, data []byte) bool {
	size := streamBlockSize(frame.size, index)
	if frame.mode != slotStream || size == 0 || len(data) != size ||
		len(frame.blocks) != (frame.size-1)/slotStreamBlockBytes+1 {
		return false
	}
	return crc32.ChecksumIEEE(data) == frame.blocks[index]
}

// Combine two finalized IEEE CRCs by advancing the first by secondLen bytes.
func crc32Combine(first, second uint32, secondLen int) uint32 {
	if secondLen <= 0 {
		return first
	}
	for n := uint(secondLen); n != 0; n &= n - 1 {
		first = crc32MatrixTimes(&crc32BytePowers[bits.TrailingZeros(n)], first)
	}
	return first ^ second
}

// Cache the operators for advancing by powers of two bytes.
var crc32BytePowers = func() [bits.UintSize - 1][32]uint32 {
	var powers [bits.UintSize - 1][32]uint32
	var bit, two, four [32]uint32
	bit[0] = crc32.IEEE
	for i := 1; i < len(bit); i++ {
		bit[i] = 1 << (i - 1)
	}
	crc32MatrixSquare(&two, &bit)
	crc32MatrixSquare(&four, &two)
	crc32MatrixSquare(&powers[0], &four)
	for i := 1; i < len(powers); i++ {
		crc32MatrixSquare(&powers[i], &powers[i-1])
	}
	return powers
}()

func crc32MatrixTimes(matrix *[32]uint32, vector uint32) uint32 {
	var sum uint32
	for i := 0; vector != 0; i++ {
		if vector&1 != 0 {
			sum ^= matrix[i]
		}
		vector >>= 1
	}
	return sum
}

func crc32MatrixSquare(square, matrix *[32]uint32) {
	for i := range square {
		square[i] = crc32MatrixTimes(matrix, matrix[i])
	}
}
