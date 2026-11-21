package xdrive

// The writer locks this controller. Inline plans reserve a complete buffered burst.
type slotBatchWindow struct {
	unit, window, budget int
	planned              int
}

func newSlotBatchWindow(size, depth int) *slotBatchWindow {
	unit, window := min(size, slotInlineLimit), min(depth, slotWriteInflight)
	return &slotBatchWindow{unit: unit, window: window, budget: unit * window}
}

func (b *slotBatchWindow) frameSize(buffered, outstanding, active, maximum int) int {
	if buffered <= b.unit {
		b.planned = max(0, b.planned-buffered)
		return buffered
	}
	if b.planned == 0 {
		budget := b.budget
		if outstanding == 0 && active == 0 {
			budget = (b.window + b.window/2) * b.unit
		}
		requests := (buffered + b.unit - 1) / b.unit
		if buffered <= budget-outstanding && requests <= b.window+b.window/2-active {
			b.planned = buffered
		}
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
