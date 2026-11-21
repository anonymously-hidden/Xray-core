package xdrive

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

const (
	slotDirectMedia        byte = 7
	slotDirectMediaMax          = defaultSegmentBytes
	slotDirectProbeTimeout      = 1250 * time.Millisecond
)

type directProbeKey struct{}

type directSlotStore interface {
	readDirectSlot(context.Context, string, slotFrame) (slotFrame, error)
}

type directSlotProbe struct {
	stop   context.CancelFunc
	result <-chan slotFrame
}

func encodeDirectSlot(frame slotFrame, data []byte) []byte {
	return append(frame.header(), data...)
}

func decodeDirectSlot(payload []byte, seq int64) (slotFrame, bool) {
	if len(payload) <= slotHeaderLen || len(payload) > slotHeaderLen+slotDirectMediaMax {
		return slotFrame{}, false
	}
	frame, ok := decodeSlotBytes(payload[:slotHeaderLen])
	if !ok || frame.mode != slotDirectMedia || frame.seq != seq || !frame.holds(payload[slotHeaderLen:]) {
		return slotFrame{}, false
	}
	frame.data = payload[slotHeaderLen:]
	return frame, true
}

func (s *driveStorage) readDirectSlot(ctx context.Context, id string, want slotFrame) (slotFrame, error) {
	ctx = context.WithValue(ctx, bodyKey{}, true)
	ctx = context.WithValue(ctx, maxBodyKey{}, int64(slotHeaderLen+slotDirectMediaMax))
	target := s.filesURL + "/" + url.PathEscape(id) + "?alt=media&supportsAllDrives=true"
	// Discovery has no trusted size yet. Once metadata identifies the frame,
	// schedule a single full-object range on the existing media lanes so a
	// stalled delivery head is eligible for the normal bounded rescue.
	if want.size != 0 {
		width := slotHeaderLen + want.size
		request := driveMediaRequest{end: width, size: width, seq: want.seq, direct: &want}
		if n := uint32(len(s.mediaClients)); n != 0 {
			request.client = s.mediaClients[(s.mediaNext.Add(1)-1)%n]
		}
		wire, err := s.readMediaRange(ctx, target, request)
		if err != nil {
			return slotFrame{}, err
		}
		want.data = wire[slotHeaderLen:]
		return want, nil
	}
	status, payload, err := s.do(ctx, http.MethodGet, target, "", nil)
	if err != nil {
		return slotFrame{}, err
	}
	if status == http.StatusOK {
		if frame, ok := decodeDirectSlot(payload, want.seq); ok {
			return frame, nil
		}
	}
	return slotFrame{}, errors.New("Drive did not return a complete direct media frame for sequence ", want.seq)
}

func (r *slotReader) startDirectProbe(ctx context.Context, seq int64, id string) *directSlotProbe {
	store, ok := r.store.(directSlotStore)
	if !ok || !r.table.directMedia || r.descriptionBudget() == 1 {
		return nil
	}
	r.mu.Lock()
	followsMedia := seq > 0 && r.head == seq && r.directNext == seq && time.Since(r.hitAt) <= slotReadAheadBurst
	r.mu.Unlock()
	if !followsMedia {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithValue(ctx, directProbeKey{}, true), slotDirectProbeTimeout)
	done := make(chan slotFrame, 1)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer cancel()
		frame, _ := store.readDirectSlot(ctx, id, slotFrame{seq: seq})
		done <- frame
	}()
	return &directSlotProbe{stop: cancel, result: done}
}
