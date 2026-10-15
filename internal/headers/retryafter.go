// Package headers reads the response headers the HTTP calls and the WebSocket upgrade share,
// so the rules for reading them cannot drift apart.
package headers

import (
	"net/http"
	"strconv"
	"time"
)

// RetryAfter reads how long the server asked us to wait, in either spelling RFC 9110 allows:
// a delay in seconds, or the date to come back at. The exchange sends seconds, but a 429 or a
// 503 raised in front of it may not.
//
// Zero means it did not say, or the moment it named has passed.
func RetryAfter(h http.Header, now time.Time) time.Duration {
	// Already trimmed: net/http strips the whitespace around a header value as it parses.
	value := h.Get("Retry-After")
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		return max(0, time.Duration(seconds)*time.Second)
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(0, at.Sub(now))
	}
	return 0
}
