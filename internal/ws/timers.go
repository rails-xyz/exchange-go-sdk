package ws

import "time"

// newTicker returns a ticker, or one that never fires when the interval is zero, so the owner
// can select on it either way.
func newTicker(interval time.Duration) *time.Ticker {
	if interval <= 0 {
		t := time.NewTicker(time.Hour)
		t.Stop()
		return t
	}
	return time.NewTicker(interval)
}

// stoppedTimer returns a timer that is not running. Since Go 1.23 a timer never delivers a
// stale value after Stop or Reset, so callers need no drain.
func stoppedTimer() *time.Timer {
	t := time.NewTimer(time.Hour)
	t.Stop()
	return t
}
