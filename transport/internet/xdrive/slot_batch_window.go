package xdrive

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
}

func newSlotBatchWindow(size, depth int) *slotBatchWindow {
	unit, window := min(size, slotInlineLimit), min(depth, slotInlineWriteInflight)
	return &slotBatchWindow{
		unit: unit, window: window, budget: unit * window,
		burstBudget: unit * min(depth, slotInlineBurstWaves*window, slotInlineBurstFrames),
	}
}

func (b *slotBatchWindow) fits(buffered, outstanding, active int) bool {
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
