package xdrive

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"strconv"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

const (
	slotMagic        = "XDRVSL01"
	slotHeaderLen    = len(slotMagic) + 17
	slotInlineLimit  = 14 << 10
	slotInline       = 1
	slotEnd          = 3
	slotErr          = 4
	maxSlotIDLen     = 128
	slotStallTimeout = time.Minute
)

var errWriteStopped = errors.New("write stopped")

type slotStore interface {
	takeIDs(context.Context, int) ([]string, error)
	createSlot(context.Context, string, string, string, []byte) error
	setSlot(context.Context, string, string, []byte) error
	readDescription(context.Context, string) (string, error)
	readFrameStream(context.Context, string, slotFrame, func([]byte) error) error
}

// slotFrame is what the description of a slot says: which segment it holds,
// and either the segment itself or the size and checksum of the body that does
type slotFrame struct {
	seq    int64
	mode   byte
	size   int
	crc    uint32
	data   []byte
	blocks []uint32 // per-block CRCs; rooted against crc before streamed delivery
	prefix []byte   // verified bytes before the streamed suffix; excluded from size and crc
}

func newSlotFrame(seq int64, mode byte, data []byte) slotFrame {
	f := slotFrame{seq: seq, mode: mode, size: len(data), crc: crc32.ChecksumIEEE(data)}
	if mode == slotInline {
		f.data = data
	} else if mode == slotStream {
		f.blocks = streamBlockChecksums(data)
	}
	return f
}

func (f slotFrame) encode() string {
	b := make([]byte, slotHeaderLen, slotHeaderLen+len(f.data))
	copy(b, slotMagic)
	binary.BigEndian.PutUint64(b[8:], uint64(f.seq))
	b[16] = f.mode
	binary.BigEndian.PutUint32(b[17:], uint32(f.size))
	binary.BigEndian.PutUint32(b[21:], f.crc)
	if f.mode == slotStream {
		b = appendStreamChecksums(b, f.blocks)
		if len(f.prefix) != 0 {
			b[16] = slotStreamPrefix
			b = binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(f.prefix))
			b = append(b, f.prefix...)
		}
	} else {
		b = append(b, f.data...)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func decodeSlot(description string) (slotFrame, bool) {
	b, err := base64.StdEncoding.DecodeString(description)
	if err != nil || len(b) < slotHeaderLen || string(b[:len(slotMagic)]) != slotMagic {
		return slotFrame{}, false
	}
	f := slotFrame{
		seq:  int64(binary.BigEndian.Uint64(b[8:])),
		mode: b[16],
		size: int(binary.BigEndian.Uint32(b[17:])),
		crc:  binary.BigEndian.Uint32(b[21:]),
	}
	if f.seq < 0 || f.size < 0 || f.size > maxSegmentBytes {
		return slotFrame{}, false
	}
	switch f.mode {
	case slotInline:
		f.data = b[slotHeaderLen:]
		if f.size == 0 || f.size > slotInlineLimit || len(f.data) != f.size || crc32.ChecksumIEEE(f.data) != f.crc {
			return slotFrame{}, false
		}
	case slotStream, slotStreamPrefix:
		trailer := b[slotHeaderLen:]
		if f.mode == slotStreamPrefix {
			end := len(trailer) - slotStreamPrefixBytes - 4
			if end < 0 || f.size > maxSegmentBytes-slotStreamPrefixBytes {
				return slotFrame{}, false
			}
			f.prefix = trailer[end+4:]
			if crc32.ChecksumIEEE(f.prefix) != binary.BigEndian.Uint32(trailer[end:]) {
				return slotFrame{}, false
			}
			trailer, f.mode = trailer[:end], slotStream
		}
		var ok bool
		f.blocks, ok = decodeStreamChecksums(f.size, f.crc, trailer)
		if !ok {
			return slotFrame{}, false
		}
	case slotEnd, slotErr:
		if f.size != 0 || f.crc != 0 || len(b) != slotHeaderLen {
			return slotFrame{}, false
		}
	default:
		return slotFrame{}, false
	}
	return f, true
}

// slotTable is the ring of one direction: the ids of its slots and of the
// watermark of its reader. name and lazy are local, not serialized.
type slotTable struct {
	Slots     []string `json:"s"`
	Watermark string   `json:"w"`
	name      string
	lazy      bool
}

// ring is what a client announces a session with, the tables are named from
// its side. Lazy rings create their slots on first write instead of up front.
type ring struct {
	Up    slotTable `json:"u"`
	Down  slotTable `json:"d"`
	First []byte    `json:"first,omitempty"`
	Lazy  bool      `json:"lazy,omitempty"`
}

func decodeRing(b []byte) (ring, bool) {
	var r ring
	if json.Unmarshal(b, &r) != nil || len(r.Up.Slots) == 0 || len(r.Down.Slots) == 0 ||
		r.Up.Watermark == "" || r.Down.Watermark == "" {
		return ring{}, false
	}
	if len(r.First) > slotInlineLimit || len(r.Up.Slots) > maxConcurrency || len(r.Down.Slots) > maxConcurrency {
		return ring{}, false
	}
	seen := make(map[string]bool, len(r.Up.Slots)+len(r.Down.Slots)+2)
	for _, t := range []slotTable{r.Up, r.Down} {
		if len(t.Watermark) > maxSlotIDLen || seen[t.Watermark] {
			return ring{}, false
		}
		seen[t.Watermark] = true
		for _, id := range t.Slots {
			if len(id) == 0 || len(id) > maxSlotIDLen || seen[id] {
				return ring{}, false
			}
			seen[id] = true
		}
	}
	return r, true
}

// lazyRing assembles a ring from pre-generated ids without any create call.
// ids must hold 2*(depth+1) entries: up slots, up watermark, down slots, down
// watermark.
func lazyRing(session string, depth int, ids []string) ring {
	r := ring{
		Lazy: true,
		Up:   slotTable{Slots: append([]string(nil), ids[:depth]...), Watermark: ids[depth], name: uplinkPrefix(session), lazy: true},
		Down: slotTable{Slots: append([]string(nil), ids[depth+1:2*depth+1]...), Watermark: ids[2*depth+1], name: downlinkPrefix(session), lazy: true},
	}
	return r
}

// Each object has one writer. Only its first publication in a lazy ring is a create.
func (t slotTable) publish(ctx context.Context, store slotStore, seq int64, mode byte, data []byte) error {
	idx := int(seq % int64(len(t.Slots)))
	var prefix, body []byte
	if mode == slotStream {
		if len(data) >= 2*slotStreamBlockBytes {
			prefix, data = data[:slotStreamPrefixBytes], data[slotStreamPrefixBytes:]
		}
		body = data
	}
	f := newSlotFrame(seq, mode, data)
	f.prefix = prefix
	if t.lazy && seq < int64(len(t.Slots)) {
		return store.createSlot(ctx, t.Slots[idx], t.name+"/s"+strconv.Itoa(idx), f.encode(), body)
	}
	return store.setSlot(ctx, t.Slots[idx], f.encode(), body)
}

func isCooling(s slotStore) bool {
	c, ok := s.(interface{ cooling() bool })
	return ok && c.cooling()
}
