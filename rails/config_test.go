package rails

import (
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConfigDefaults(t *testing.T) {
	t.Setenv("RAILS_SDK_TELEMETRY", "")
	cfg := NewConfig()

	assert.Equal(t, Production(), cfg.Environment, "a client built without an environment trades for real")
	assert.NotSame(t, http.DefaultClient, cfg.HTTPClient, "the shared client's settings must not reach the SDK")
	assert.NotNil(t, cfg.HTTPClient)
	require.NotNil(t, cfg.Logger)
	assert.False(t, cfg.Logger.Enabled(t.Context(), slog.LevelError), "the default logger should discard")
	assert.Zero(t, cfg.Hooks, "no hooks unless asked for")
	assert.Nil(t, cfg.TokenSource, "a nil token source is the signal for the product client to install its own")
	assert.True(t, cfg.Telemetry)
}

// Whoever runs the process outranks whoever compiled it.
func TestEnvironmentOptOutBeatsTheOption(t *testing.T) {
	t.Setenv("RAILS_SDK_TELEMETRY", "off")
	assert.False(t, NewConfig(WithTelemetry(true)).Telemetry)
}
