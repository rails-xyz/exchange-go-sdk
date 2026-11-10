package rails

import "log/slog"

// Credentials are the API key and secret minted in the Rails app. They are exchanged for a
// short-lived bearer token, and never authenticate a request or a WebSocket directly.
//
// Both values are redacted when a Credentials value is formatted with fmt or logged with
// log/slog. A struct holding it that is encoded field by field, as JSON for instance, still
// carries both.
type Credentials struct {
	APIKey    string
	APISecret string
}

const redactedCredentials = "rails.Credentials{APIKey:<redacted> APISecret:<redacted>}"

func (Credentials) String() string { return redactedCredentials }

func (Credentials) GoString() string { return redactedCredentials }

func (Credentials) LogValue() slog.Value { return slog.StringValue(redactedCredentials) }

// Environment is the full set of hosts a client talks to. The WebSocket host does not follow
// the pattern of the HTTP hosts, so every URL is named rather than composed from a base domain.
//
// TelemetryURL is where the SDK reports the latency it observes. A client with telemetry on
// refuses an Environment that leaves it empty, so a custom one cannot silently stop reporting;
// switch reporting off with [WithTelemetry] instead.
//
// OptionsURL is the options HTTP API host; options WebSocket connections use WSURL.
type Environment struct {
	AuthURL       string
	OrderURL      string
	AccountURL    string
	MarketDataURL string
	OptionsURL    string
	WSURL         string
	TelemetryURL  string
}

// Production returns the live environment, and is what a client uses when it is built without
// [WithEnvironment]. Orders placed here are real.
func Production() Environment {
	return Environment{
		AuthURL:       "https://api-auth.rails.xyz",
		OrderURL:      "https://order.rails.xyz",
		AccountURL:    "https://user-account.rails.xyz",
		MarketDataURL: "https://market-data.rails.xyz",
		OptionsURL:    "https://api-options.rails.xyz",
		WSURL:         "wss://ws.rails.xyz",
		TelemetryURL:  "https://telemetry.rails.xyz",
	}
}

// Sandbox returns the public test environment, isolated from the live market and needing no
// real funds.
func Sandbox() Environment {
	return Environment{
		AuthURL:       "https://api-auth.sandbox.rails.xyz",
		OrderURL:      "https://order.sandbox.rails.xyz",
		AccountURL:    "https://user-account.sandbox.rails.xyz",
		MarketDataURL: "https://market-data.sandbox.rails.xyz",
		OptionsURL:    "https://api-options.sandbox.rails.xyz",
		WSURL:         "wss://ws.sandbox.rails.xyz",
		TelemetryURL:  "https://telemetry.sandbox.rails.xyz",
	}
}
