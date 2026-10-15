package perps

import (
	"context"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/pace"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// The budgets the exchange meters separately. ClassAll caps every metered call together.
const (
	ClassAll          rails.OpClass = "*"
	ClassOrderCreate  rails.OpClass = "order-create"
	ClassOrderModify  rails.OpClass = "order-modify"
	ClassOrderCancel  rails.OpClass = "order-cancel"
	ClassAccountWrite rails.OpClass = "account-write" // leverage and margin mode
	ClassReadOrders   rails.OpClass = "read-orders"   // open, completed and single orders
	ClassReadAccount  rails.OpClass = "read-account"  // balances, positions, fundings
	ClassReadLedger   rails.OpClass = "read-ledger"   // deposits and withdrawals
)

const (
	// defaultMaxCooldown is the length of an order-creation ban.
	defaultMaxCooldown = 5 * time.Minute
	// accountTypeMarketMaker is the accountType claim of a market-maker token.
	accountTypeMarketMaker = "market_maker"
)

// Limit is a ceiling per window. Zero means none for that window.
type Limit struct {
	PerSecond int
	PerMinute int
}

func (l Limit) windows() pace.Limit {
	return pace.Limit{time.Second: l.PerSecond, time.Minute: l.PerMinute}
}

// Limits is a ceiling per class. A class absent from it is unlimited.
type Limits map[rails.OpClass]Limit

func (l Limits) windows() pace.Limits {
	out := pace.Limits{}
	for class, limit := range l {
		out[class] = limit.windows()
	}
	return out
}

// DefaultLimits is the documented budget of a retail account.
func DefaultLimits() Limits {
	return Limits{
		ClassOrderCreate: {PerMinute: 250},
		ClassOrderModify: {PerMinute: 250},
		ClassOrderCancel: {PerMinute: 500},
	}
}

// MarketMakerLimits is the documented budget of a market-maker account.
func MarketMakerLimits() Limits {
	return Limits{
		ClassOrderCreate: {PerMinute: 10000},
		ClassOrderModify: {PerMinute: 10000},
		ClassOrderCancel: {PerMinute: 20000},
	}
}

func limitsForAccountType(accountType string) pace.Limits {
	if accountType == accountTypeMarketMaker {
		return MarketMakerLimits().windows()
	}
	return DefaultLimits().windows()
}

// throttled recognises a rate limit or a creation ban, and how long it asked for.
func throttled(err *rails.Error) (time.Duration, bool) {
	if err.RetryAfter <= 0 || (err.Code != rails.CodeTooManyRequests && err.Code != CodeCreateOrderBanned) {
		return 0, false
	}
	return err.RetryAfter, true
}

// Pacer keeps the metered calls under the account's rate limits from the client's side, and
// when the exchange rate-limits a call anyway, holds that class back for as long as it asked.
// It implements [rails.Pacer]; install it with [rails.WithPacer].
//
// Limits are per account and shared by everything using the key, so a program running several
// instances should divide its limits between them with [WithLimits].
type Pacer struct {
	cfg    pace.Config
	engine *pace.Pacer
}

// PacerOption configures a [Pacer].
type PacerOption func(*Pacer)

// NewPacer builds a pacer on [DefaultLimits].
func NewPacer(opts ...PacerOption) *Pacer {
	p := &Pacer{
		cfg: pace.Config{
			All:         ClassAll,
			Explicit:    pace.Limits{},
			Preset:      limitsForAccountType,
			Throttle:    throttled,
			MaxCooldown: defaultMaxCooldown,
			Now:         time.Now,
		},
	}
	for _, opt := range opts {
		opt(p)
	}
	p.engine = pace.New(p.cfg)
	return p
}

// WithLimits lays ceilings over the preset, window by window: a non-zero window replaces the
// preset's, a zero one keeps it.
func WithLimits(l Limits) PacerOption {
	return func(p *Pacer) {
		for class, limit := range l {
			p.cfg.Explicit[class] = limit.windows().Over(p.cfg.Explicit[class])
		}
	}
}

// WithLimitsFromToken picks the preset from the accountType claim of the token in use, so a
// market-maker key gets [MarketMakerLimits]. [DefaultLimits] apply until a token is seen.
func WithLimitsFromToken() PacerOption {
	return func(p *Pacer) { p.cfg.FromToken = true }
}

// WithFailFast refuses a call that would have to wait, with [rails.ErrNotSent] wrapping
// [rails.ErrPaced], instead of waiting.
func WithFailFast() PacerOption {
	return func(p *Pacer) { p.cfg.FailFast = true }
}

// WithMaxCooldown caps how long a Retry-After from the exchange holds a class back. Defaults
// to five minutes. A cap below the exchange's Retry-After does not shorten its throttle: the
// next call goes out sooner and is likely refused again.
func WithMaxCooldown(d time.Duration) PacerOption {
	return func(p *Pacer) { p.cfg.MaxCooldown = d }
}

// Wait implements [rails.Pacer].
func (p *Pacer) Wait(ctx context.Context, class rails.OpClass, op string) error {
	return p.engine.Wait(ctx, class, op)
}

// Observe implements [rails.Pacer]: a rate limit or a creation ban puts the class on cooldown
// for its Retry-After, capped by [WithMaxCooldown]. A longer cooldown already running stands.
func (p *Pacer) Observe(class rails.OpClass, err error) { p.engine.Observe(class, err) }
