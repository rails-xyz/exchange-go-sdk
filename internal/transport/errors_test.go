package transport

import (
	"net/http"
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
)

// The exchange returns three incompatible error envelopes depending on which service built the
// response, plus routes that return no JSON at all. All four have to land in one rails.Error.
func TestParseErrorEnvelopes(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		headers  map[string]string
		body     string
		want     rails.Error
		sentinel error
	}{
		{
			name:     "message envelope",
			status:   http.StatusUnauthorized,
			body:     `{"message":"Unauthorized"}`,
			want:     rails.Error{StatusCode: 401, Message: "Unauthorized"},
			sentinel: rails.ErrUnauthorized,
		},
		{
			name:     "body and statusCode envelope",
			status:   http.StatusTooManyRequests,
			headers:  map[string]string{"Retry-After": "5"},
			body:     `{"body":"api call rate limit exceeded","statusCode":429}`,
			want:     rails.Error{StatusCode: 429, Message: "api call rate limit exceeded", RetryAfter: 5 * time.Second},
			sentinel: rails.ErrRateLimited,
		},
		{
			name:    "slug and code envelope",
			status:  http.StatusNotFound,
			headers: map[string]string{"X-Api-Quota-Used": "13", "X-Api-Quota-Limit": "250"},
			body:    `{"error":{"slug":"NOT_FOUND","code":"SYS-0007"},"message":"order not found"}`,
			want: rails.Error{
				StatusCode: 404, Slug: "NOT_FOUND", Code: "SYS-0007", Message: "order not found",
				APIQuotaUsed: 13, APIQuotaLimit: 250,
			},
			sentinel: rails.ErrNotFound,
		},
		{
			name:   "no JSON body at all",
			status: http.StatusInternalServerError,
			body:   "",
			want:   rails.Error{StatusCode: 500, Message: "Internal Server Error"},
		},
		{
			name:     "plain text body",
			status:   http.StatusForbidden,
			body:     "user soft banned till 1756000000",
			want:     rails.Error{StatusCode: 403, Message: "user soft banned till 1756000000"},
			sentinel: rails.ErrForbidden,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{StatusCode: tc.status, Header: http.Header{}}
			for k, v := range tc.headers {
				resp.Header.Set(k, v)
			}

			got := ParseError("GET /api/v1/order", resp, []byte(tc.body))

			want := tc.want
			want.Op = "GET /api/v1/order"
			assert.Equal(t, want, *got)
			if tc.sentinel != nil {
				assert.ErrorIs(t, got, tc.sentinel)
			}
		})
	}
}

// A body far larger than any real error must not become the message verbatim.
func TestFallbackMessageIsBounded(t *testing.T) {
	huge := make([]byte, 4096)
	for i := range huge {
		huge[i] = 'x'
	}
	resp := &http.Response{StatusCode: 502, Header: http.Header{}}
	assert.Len(t, ParseError("op", resp, huge).Message, maxErrorBody)
}
