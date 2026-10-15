package perps_test

import (
	"testing"

	"github.com/rails-xyz/exchange-go-sdk/perps"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A custom environment cannot lose telemetry by omission: with reporting on it has to say
// where reports go, and the only other way through is to turn reporting off.
func TestNewClientRefusesATelemetryHostLeftEmpty(t *testing.T) {
	// The switch in the process environment beats every option, so it must be off here.
	t.Setenv("RAILS_SDK_TELEMETRY", "")
	env := rails.Environment{
		AuthURL: "https://auth.invalid", OrderURL: "https://order.invalid",
		AccountURL: "https://account.invalid", MarketDataURL: "https://market-data.invalid",
		WSURL: "wss://ws.invalid",
	}
	creds := rails.Credentials{APIKey: "key", APISecret: "secret"}

	_, err := perps.NewClient(creds, rails.WithEnvironment(env))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TelemetryURL")
	assert.Contains(t, err.Error(), "WithTelemetry(false)")

	c, err := perps.NewClient(creds, rails.WithEnvironment(env), rails.WithTelemetry(false))
	require.NoError(t, err)
	c.Close()

	env.TelemetryURL = "https://telemetry.invalid"
	c, err = perps.NewClient(creds, rails.WithEnvironment(env))
	require.NoError(t, err)
	c.Close()
}
