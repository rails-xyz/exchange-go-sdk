package rails

import (
	"log/slog"
	"net/http"
	"os"
	"strings"
)

// Config is the assembled result of applying options. Build one with [NewConfig]: the [Option]
// functions are the supported surface, and fields may be added here in any release.
type Config struct {
	Environment Environment
	HTTPClient  *http.Client
	Logger      *slog.Logger
	Hooks       Hooks
	TokenSource TokenSource
	Telemetry   bool
	Pacer       Pacer
}

// Option configures a client. Options are applied in order, so a later one wins.
type Option func(*Config)

// NewConfig applies opts over the defaults. TokenSource is left nil, which tells a product
// client to install the credential-backed source it builds itself.
//
// RAILS_SDK_TELEMETRY=off is applied last and beats [WithTelemetry]: whoever runs the process
// gets the final say on what leaves it, not whoever compiled it.
func NewConfig(opts ...Option) *Config {
	cfg := &Config{
		Environment: Production(),
		// Not http.DefaultClient: an application that sets a Timeout on that shared instance
		// would break every WebSocket dial in the SDK.
		HTTPClient: &http.Client{},
		Logger:     slog.New(slog.DiscardHandler),
		Telemetry:  true,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if strings.EqualFold(os.Getenv("RAILS_SDK_TELEMETRY"), "off") {
		cfg.Telemetry = false
	}
	return cfg
}

// WithEnvironment selects the hosts to talk to. Defaults to [Production].
func WithEnvironment(env Environment) Option {
	return func(c *Config) { c.Environment = env }
}

// WithHTTPClient supplies the client used for every HTTP call and for the WebSocket upgrade.
// Wrap its transport to add tracing of your own. nil keeps the default.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Config) {
		if hc != nil {
			c.HTTPClient = hc
		}
	}
}

// WithLogger supplies the logger for the SDK's own diagnostics. Defaults to discarding them;
// nil keeps that default.
func WithLogger(l *slog.Logger) Option {
	return func(c *Config) {
		if l != nil {
			c.Logger = l
		}
	}
}

// WithHooks registers instrumentation callbacks; set only the funcs you want.
func WithHooks(h Hooks) Option {
	return func(c *Config) { c.Hooks = h }
}

// WithTelemetry enables or disables the reports of client-observed latency the SDK sends back
// to Rails. They are sent with your token, so Rails knows which account they describe; they
// carry latencies, counts, failure classes, markets and the clientRequestIds of slow or failed
// commands, never prices, quantities, balances or order ids.
func WithTelemetry(enabled bool) Option {
	return func(c *Config) { c.Telemetry = enabled }
}

// WithPacer installs client-side pacing of the metered calls; each product package ships one,
// such as perps.NewPacer. Defaults to none.
func WithPacer(p Pacer) Option {
	return func(c *Config) { c.Pacer = p }
}

// WithTokenSource replaces the built-in token cache, for pre-minted tokens or a cache shared
// across processes.
func WithTokenSource(ts TokenSource) Option {
	return func(c *Config) { c.TokenSource = ts }
}
