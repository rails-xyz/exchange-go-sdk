package ws

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Only 202 is not an answer. A status the exchange does not send today, a 201 say, still ends
// the request rather than leaving the caller to wait out its deadline.
func TestTerminal(t *testing.T) {
	for code, want := range map[int]bool{
		0: false, 202: false,
		200: true, 201: true, 204: true, 400: true, 429: true, 500: true,
	} {
		assert.Equal(t, want, terminal(code), "status %d", code)
	}
}
