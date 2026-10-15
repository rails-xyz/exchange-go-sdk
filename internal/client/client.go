// Package client builds the parts every product client shares: its configuration, token
// source, telemetry collector and HTTP transport.
package client

import (
	"errors"
	"fmt"
	"strings"

	"github.com/rails-xyz/exchange-go-sdk/internal/auth"
	"github.com/rails-xyz/exchange-go-sdk/internal/pace"
	"github.com/rails-xyz/exchange-go-sdk/internal/telemetry"
	"github.com/rails-xyz/exchange-go-sdk/internal/transport"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// Host is a [rails.Environment] field a product needs, by its Go name.
type Host struct {
	Name string
	URL  string
}

// Parts are a product client's shared parts.
type Parts struct {
	Config    *rails.Config
	HTTP      *transport.Client
	Telemetry *telemetry.Collector
}

// Product describes what differs between product clients at construction.
type Product struct {
	// Name prefixes every error and names the product in telemetry reports.
	Name string
	// Hosts lists the environment fields the product cannot work without.
	Hosts func(rails.Environment) []Host
	// Pacer finds the product's own pacer in the configured one, or returns nil. Nil for a
	// product that has no pacer of its own.
	Pacer func(rails.Pacer) *pace.Pacer
}

// New checks the credentials and hosts, then builds the parts from opts.
func New(product Product, creds rails.Credentials, opts ...rails.Option) (Parts, error) {
	if creds.APIKey == "" || creds.APISecret == "" {
		return Parts{}, errors.New(product.Name + ": both APIKey and APISecret are required")
	}
	cfg := rails.NewConfig(opts...)
	if err := requireHosts(product.Name, product.Hosts(cfg.Environment)); err != nil {
		return Parts{}, err
	}
	if cfg.TokenSource == nil {
		cfg.TokenSource = auth.New(creds, cfg)
	}
	var pacer *pace.Pacer
	if product.Pacer != nil {
		pacer = product.Pacer(cfg.Pacer)
	}
	if pacer != nil {
		cfg.TokenSource = pacer.Tokens(cfg.TokenSource)
	}
	// Measurements reach the caller's hooks through the collector, so both sides see the same
	// numbers.
	tel, err := telemetry.New(cfg, cfg.TokenSource, product.Name)
	if err != nil {
		return Parts{}, fmt.Errorf("%s: %w", product.Name, err)
	}
	cfg.Hooks = tel.Hooks()
	if pacer != nil {
		pacer.Install(cfg.Hooks)
	}
	return Parts{Config: cfg, HTTP: transport.New(cfg, cfg.TokenSource), Telemetry: tel}, nil
}

func requireHosts(product string, hosts []Host) error {
	names := make([]string, len(hosts))
	missing := false
	for i, h := range hosts {
		names[i] = h.Name
		missing = missing || h.URL == ""
	}
	if !missing {
		return nil
	}
	list := names[len(names)-1]
	if len(names) > 1 {
		list = strings.Join(names[:len(names)-1], ", ") + " and " + list
	}
	return errors.New(product + ": the environment needs " + list)
}
