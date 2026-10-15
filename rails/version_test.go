package rails

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The User-Agent is how Rails attributes traffic to an SDK version in its access logs, so its
// shape is a wire contract, not a formatting preference. Under `go test` the SDK is the main
// module, so the version reads as devel; a consumer's build resolves it to the real tag.
func TestUserAgentShape(t *testing.T) {
	want := regexp.MustCompile(`^exchange-go-sdk/\S+ \(go[\d.]+; \w+/\w+\)$`)
	assert.Regexp(t, want, UserAgent())
	assert.Equal(t, devVersion, Version(), "the SDK is the main module under its own tests")
}
