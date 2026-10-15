package options_test

import (
	"testing"

	"github.com/rails-xyz/exchange-go-sdk/options"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClientHosts(t *testing.T) {
	creds := rails.Credentials{APIKey: "key", APISecret: "secret"}
	env := rails.Environment{
		AuthURL: "https://auth.invalid", TelemetryURL: "https://telemetry.invalid",
	}

	_, err := options.NewClient(creds, rails.WithEnvironment(env))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OptionsURL")
	assert.Contains(t, err.Error(), "WSURL")

	env.OptionsURL, env.WSURL = "https://options.invalid", "wss://ws.invalid"
	c, err := options.NewClient(creds, rails.WithEnvironment(env))
	require.NoError(t, err)
	c.Close()
}
