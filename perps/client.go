package perps

import (
	"github.com/rails-xyz/exchange-go-sdk/internal/client"
	"github.com/rails-xyz/exchange-go-sdk/internal/pace"
	"github.com/rails-xyz/exchange-go-sdk/internal/telemetry"
	"github.com/rails-xyz/exchange-go-sdk/internal/transport"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// Client talks to the perpetuals API. It is safe for concurrent use.
type Client struct {
	cfg  *rails.Config
	rest *transport.Client
	tel  *telemetry.Collector
}

// NewClient builds a client for the perpetuals API. Credentials are required; everything else
// is an option. Without [rails.WithEnvironment] the client trades on production, and without
// [rails.WithTelemetry] it reports the latency it observes back to Rails.
func NewClient(creds rails.Credentials, opts ...rails.Option) (*Client, error) {
	parts, err := client.New(client.Product{Name: "perps", Hosts: requiredHosts, Pacer: pacerEngine}, creds, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{cfg: parts.Config, rest: parts.HTTP, tel: parts.Telemetry}, nil
}

func requiredHosts(env rails.Environment) []client.Host {
	return []client.Host{
		{Name: "AuthURL", URL: env.AuthURL},
		{Name: "OrderURL", URL: env.OrderURL},
		{Name: "AccountURL", URL: env.AccountURL},
		{Name: "MarketDataURL", URL: env.MarketDataURL},
		{Name: "WSURL", URL: env.WSURL},
	}
}

func pacerEngine(p rails.Pacer) *pace.Pacer {
	if pacer, ok := p.(*Pacer); ok && pacer != nil {
		return pacer.engine
	}
	return nil
}

// Close sends what the client has measured since its last report and stops reporting. Open
// connections are unaffected; close those with [Conn.Close].
func (c *Client) Close() { c.tel.Close() }
