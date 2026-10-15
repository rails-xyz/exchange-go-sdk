package rails

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorIsMapsStatusToSentinel(t *testing.T) {
	cases := []struct {
		status   int
		sentinel error
	}{
		{http.StatusUnauthorized, ErrUnauthorized},
		{http.StatusForbidden, ErrForbidden},
		{http.StatusNotFound, ErrNotFound},
		{http.StatusConflict, ErrConflict},
		{http.StatusTooManyRequests, ErrRateLimited},
	}
	for _, tc := range cases {
		err := fmt.Errorf("wrapped: %w", &Error{StatusCode: tc.status})
		assert.ErrorIs(t, err, tc.sentinel, "status %d", tc.status)

		for _, other := range cases {
			if other.sentinel != tc.sentinel {
				assert.NotErrorIs(t, err, other.sentinel, "status %d should not also match", tc.status)
			}
		}
	}
}

// A soft ban is a 403 that also matches its own sentinel; an ordinary 403 is not a soft ban.
func TestSoftBanIsStillAForbidden(t *testing.T) {
	banned := fmt.Errorf("%w: %w", ErrSoftBanned, &Error{StatusCode: http.StatusForbidden, ClientRequestID: "a-crid"})
	assert.ErrorIs(t, banned, ErrSoftBanned)
	assert.ErrorIs(t, banned, ErrForbidden)
	assert.NotErrorIs(t, banned, ErrTimeout)

	var detail *Error
	require.ErrorAs(t, banned, &detail)
	assert.Equal(t, "a-crid", detail.ClientRequestID)

	assert.NotErrorIs(t, &Error{StatusCode: http.StatusForbidden}, ErrSoftBanned)
}

func TestErrorMessage(t *testing.T) {
	err := &Error{
		Op:              "createOrder",
		StatusCode:      429,
		Slug:            "TOO_MANY_REQUESTS",
		Code:            "SYS-0003",
		Message:         "api call rate limit exceeded",
		RetryAfter:      5 * time.Second,
		ClientRequestID: "2f6635e5-bdab-4fbc-8e2e-21252d2219dc",
	}
	const want = "createOrder: 429 api call rate limit exceeded (TOO_MANY_REQUESTS/SYS-0003) " +
		"[clientRequestId=2f6635e5-bdab-4fbc-8e2e-21252d2219dc]"

	assert.Equal(t, want, err.Error())
	assert.Equal(t, "GET /api/v1/order: request failed", (&Error{Op: "GET /api/v1/order"}).Error())
}
