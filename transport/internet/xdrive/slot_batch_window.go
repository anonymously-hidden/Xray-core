package xdrive

// A committed inline plan (at most slotInlineBurstFrames frames) publishes in
// one wave. A live-sized burst (at most one inline wave, 336 KiB at the
// default payload size) stays inline beside unrelated uploads, except during
// sustained transfers and quota recovery. A live burst larger than one wave
// sends that wave inline and the rest as media alongside it.

// slotPermitCap sizes the writer's permit channel. It must cover every window
// writeWindowFor can return, or a flusher holding mu would block on the send:
// a single-wave plan, and a split burst's inline wave beside the media window.
func slotPermitCap() int {
	return max(slotWriteInflight, slotBulkInflight, slotInlineBurstFrames, slotInlineWriteInflight+slotWriteInflight)
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

	// planCap is refreshed with live: how many uploads a committed inline
	// plan may have in flight. hybrid is set when a split burst's inline wave
	// is planned and kept until the writer has no inline upload in flight and
	// nothing buffered, so the rest of that burst goes to media.
	planCap int
	hybrid  bool
}

func newSlotBatchWindow(size, depth int) *slotBatchWindow {
	unit, window := min(size, slotInlineLimit), min(depth, slotInlineWriteInflight)
	return &slotBatchWindow{
		unit: unit, window: window, budget: unit * window,
		burstBudget: unit * min(depth, slotInlineBurstWaves*window, slotInlineBurstFrames),
	}
}

func (b *slotBatchWindow) fits(buffered, outstanding, active int) bool {
	if b.hybrid {
		// The rest of a split burst follows its inline wave as media.
		return false
	}
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

// splits reports whether a live burst too large for one inline wave should
// send a full wave inline now, with the rest as media. The wave must be able
// to go out at once beside what is already in flight.
func (b *slotBatchWindow) splits(buffered, active int) bool {
	return b.live && !b.hybrid && b.planned == 0 && b.window >= slotInlineWriteInflight &&
		buffered > b.budget && active+b.window <= b.planCap
}

func (b *slotBatchWindow) inlineNext(buffered, outstanding, active int) bool {
	return buffered > 0 && (buffered <= 4*b.unit || b.planned > 0 || b.fits(buffered, outstanding, active) || b.splits(buffered, active))
}

func (b *slotBatchWindow) frameSize(buffered, outstanding, active, maximum int) int {
	if buffered <= b.unit {
		b.planned = max(0, b.planned-buffered)
		return buffered
	}
	if b.planned == 0 && b.fits(buffered, outstanding, active) {
		b.planned = buffered
	} else if b.splits(buffered, active) {
		b.planned, b.hybrid = b.budget, true
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
