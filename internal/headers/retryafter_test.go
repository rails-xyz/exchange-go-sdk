package headers

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 21, 7, 28, 0, 0, time.UTC)

	cases := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{"seconds, which is what the exchange sends", "5", 5 * time.Second},
		// An edge in front of the exchange answers a 429 or a 503 itself, and RFC 9110 lets it
		// name a moment rather than a delay.
		{"the date to come back at", "Wed, 21 Oct 2026 07:29:00 GMT", time.Minute},
		{"a date that has already passed", "Wed, 21 Oct 2026 07:27:00 GMT", 0},
		{"a delay that has already elapsed", "-5", 0},
		{"nothing at all", "", 0},
		{"something that is neither", "soon", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			header := http.Header{}
			if tc.value != "" {
				header.Set("Retry-After", tc.value)
			}
			assert.Equal(t, tc.want, RetryAfter(header, now))
		})
	}
}
