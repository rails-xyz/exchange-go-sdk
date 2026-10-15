package transport

import (
	"context"
	"math/rand/v2"
	"net/http"
	"time"
)

// Retry policy for the reads the SDK issues. Mutating calls are never retried: a repeated
// clientRequestId is either rejected as a duplicate or silently dropped.
const (
	maxAttempts = 3
	backoffBase = 250 * time.Millisecond
	backoffCap  = 5 * time.Second
)

// retryable reports whether a response is worth a second attempt. A nil response means the
// request never completed, which is the clearest case of all.
func retryable(resp *http.Response) bool {
	if resp == nil {
		return true
	}
	switch {
	case resp.StatusCode == http.StatusRequestTimeout,
		resp.StatusCode == http.StatusTooManyRequests,
		resp.StatusCode >= http.StatusInternalServerError:
		return true
	}
	return false
}

// backoff is full jitter: a random wait in [0, min(cap, base*2^attempt)). Picking a point in
// the range rather than the range itself is what stops a fleet of clients retrying in step.
func backoff(attempt int) time.Duration {
	window := min(backoffCap, backoffBase<<attempt)
	return rand.N(window)
}

// wait sleeps for d, or returns the context's error if that comes first.
func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
