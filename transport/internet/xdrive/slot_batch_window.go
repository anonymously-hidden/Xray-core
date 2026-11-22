package xdrive

import "os"

// slotInlineSingleWave lets a committed inline plan (at most
// slotInlineBurstFrames frames) publish in one wave instead of waiting for a
// second slotInlineWriteInflight wave. It is on by default and read once at
// process start; XRAY_XDRIVE_INLINE_SINGLE_WAVE=0 restores the two-wave window.
var slotInlineSingleWave = os.Getenv("XRAY_XDRIVE_INLINE_SINGLE_WAVE") != "0"

// slotLiveInline keeps a live-sized burst (at most one inline wave, 336 KiB at
// the default payload size) inline even while unrelated uploads are still
// outstanding; the writer withholds it during sustained transfers and quota
// recovery. On by default; XRAY_XDRIVE_LIVE_INLINE=0 restores the old rule.
var slotLiveInline = os.Getenv("XRAY_XDRIVE_LIVE_INLINE") != "0"

// slotPermitCap sizes the writer's permit channel. It must cover every window
// writeWindowFor can return, or a flusher holding mu would block on the send.
func slotPermitCap(singleWave bool) int {
	n := max(slotWriteInflight, slotBulkInflight)
	if singleWave {
		n = max(n, slotInlineBurstFrames)
	}
	return n
}

const (
	slotInlineBurstWaves = 2
	// Large plans multiply Drive writes and ring occupancy. Keep up to 32
	// inline frames (448 KiB at the default payload size); use media above it.
	slotInlineBurstFrames = 32
)

// The writer locks this controller. Inline plans reserve a complete buffered burst.
type slotBatchWindow struct {
	unit, window, budget int
	burstBudget          int
	planned              int
	live                 bool // refreshed by the writer under its lock before each admission
}

func newSlotBatchWindow(size, depth int) *slotBatchWindow {
	unit, window := min(size, slotInlineLimit), min(depth, slotInlineWriteInflight)
	return &slotBatchWindow{
		unit: unit, window: window, budget: unit * window,
		burstBudget: unit * min(depth, slotInlineBurstWaves*window, slotInlineBurstFrames),
	}
}

func (b *slotBatchWindow) fits(buffered, outstanding, active int) bool {
	// A segment-sized burst is cheaper inline than as a media frame behind
	// other traffic: the reader skips a media GET and the PUT queue.
	if b.live && buffered <= b.budget {
		return true
	}
	budget := b.budget
	frames := b.window + b.window/2
	// Spare upload capacity can cover a bounded second wave without a media GET.
	if outstanding <= b.budget/2 && active <= b.window/2 {
		budget, frames = b.burstBudget, b.burstBudget/b.unit
	}
	return buffered <= budget-outstanding && (buffered+b.unit-1)/b.unit <= frames-active
}

func (b *slotBatchWindow) inlineNext(buffered, outstanding, active int) bool {
	return buffered > 0 && (buffered <= 4*b.unit || b.planned > 0 || b.fits(buffered, outstanding, active))
}

func (b *slotBatchWindow) frameSize(buffered, outstanding, active, maximum int) int {
	if buffered <= b.unit {
		b.planned = max(0, b.planned-buffered)
		return buffered
	}
	if b.planned == 0 && b.fits(buffered, outstanding, active) {
		b.planned = buffered
	}
	if b.planned > 0 {
		n := min(buffered, b.planned, b.unit)
		b.planned -= n
		return n
	}
	if buffered <= 4*b.unit {
		return b.unit
	}
	return min(buffered, maximum)
}
