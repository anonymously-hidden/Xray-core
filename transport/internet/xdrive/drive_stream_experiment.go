package xdrive

import "strings"

// streamHeadRangesEnv opts into a 2-versus-4 MODE5 head-primary experiment.
// Fewer ranges can save quota while worsening later gaps or connection diversity.
// The owner captures the setting once at construction and copies it to sessions.
const streamHeadRangesEnv = "XRAY_XDRIVE_STREAM_HEAD_RANGES"

func parseStreamHeadRanges(raw string) (int, bool) {
	switch strings.TrimSpace(raw) {
	case "", "4":
		return 4, true
	case "2":
		return 2, true
	default:
		return 4, false
	}
}

func (s *driveStorage) streamHeadRangeLimit() int {
	if s.streamHeadRanges == 2 {
		return 2
	}
	return 4
}
