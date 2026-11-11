package rails

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

// A supplied client with no redirect policy of its own would follow Go's, which forwards the
// credential headers to any host.
func TestClientsFollowRedirectsOnlyWithinTheOrigin(t *testing.T) {
	elsewhere := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(elsewhere.Close)
	mux := http.NewServeMux()
	mux.HandleFunc("/moved", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/token", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("/away", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+"/token", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("/token", func(http.ResponseWriter, *http.Request) {})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	for name, client := range map[string]*http.Client{
		"default":  NewConfig().HTTPClient,
		"supplied": NewConfig(WithHTTPClient(&http.Client{Timeout: time.Minute})).HTTPClient,
	} {
		for path, want := range map[string]int{
			"/moved": http.StatusOK,
			"/away":  http.StatusTemporaryRedirect,
		} {
			resp, err := client.Get(srv.URL + path)
			require.NoError(t, err)
			_ = resp.Body.Close()
			assert.Equal(t, want, resp.StatusCode, name+" "+path)
		}
	}

	own := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return nil }}
	assert.Same(t, own, NewConfig(WithHTTPClient(own)).HTTPClient, "a client's own policy was replaced")
}

// Whoever runs the process outranks whoever compiled it.
func TestEnvironmentOptOutBeatsTheOption(t *testing.T) {
	t.Setenv("RAILS_SDK_TELEMETRY", "off")
	assert.False(t, NewConfig(WithTelemetry(true)).Telemetry)
}
