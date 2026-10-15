package ws

import (
	"math/rand/v2"
	"time"
)

// Reconnect pacing: full jitter, floored at the connect budget the exchange allows a retail
// account, so even the first retry waits and a fleet of clients cannot dial in step.
const (
	backoffBase   = time.Second
	backoffCap    = 30 * time.Second
	connectBudget = 3 * time.Second // 20 connects per minute
)

// backoff is a random point in [0, min(cap, base*2^attempt)), never below the connect budget.
func backoff(attempt int) time.Duration {
	window := backoffCap
	if attempt < 16 {
		if scaled := backoffBase << attempt; scaled < window {
			window = scaled
		}
	}
	return max(connectBudget, rand.N(window))
}
