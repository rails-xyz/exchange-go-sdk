package options

import (
	"context"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/pace"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// The budgets the exchange meters separately. The contract reads are not metered. ClassAll
// caps every metered call together.
const (
	ClassAll             rails.OpClass = "*"
	ClassOrderCreate     rails.OpClass = "order-create"
	ClassOrderModify     rails.OpClass = "order-modify"
	ClassOrderCancel     rails.OpClass = "order-cancel"
	ClassOrderCancelAll  rails.OpClass = "order-cancel-all"
	ClassOrderReduceOnly rails.OpClass = "order-reduce-only" // creates with ReduceOnly set
	ClassReadOrders      rails.OpClass = "read-orders"       // open orders and order status
	ClassReadAccount     rails.OpClass = "read-account"      // summary, positions, order history, deposits, withdrawals
)

const (
	defaultMaxCooldown         = 5 * time.Minute
	defaultRateLimitedCooldown = time.Second
	accountTypeMarketMaker     = "market_maker"
)

// Limit is a ceiling per window. Zero means none for that window.
type Limit struct {
	PerSecond int
	PerMinute int
	PerHour   int
	PerDay    int
}

func (l Limit) windows() pace.Limit {
	return pace.Limit{time.Second: l.PerSecond, time.Minute: l.PerMinute, time.Hour: l.PerHour, 24 * time.Hour: l.PerDay}
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

// DefaultLimits is the documented budget of a retail account. Cancel-all and reduce-only
// creates are unlimited.
func DefaultLimits() Limits {
	return Limits{
		ClassOrderCreate: {PerSecond: 25, PerMinute: 250, PerHour: 7_500, PerDay: 90_000},
		ClassOrderModify: {PerSecond: 25, PerMinute: 250, PerHour: 7_500, PerDay: 90_000},
		ClassOrderCancel: {PerSecond: 50, PerMinute: 500, PerHour: 15_000, PerDay: 200_000},
		ClassReadOrders:  {PerSecond: 50, PerMinute: 1_000, PerHour: 30_000, PerDay: 500_000},
		ClassReadAccount: {PerSecond: 50, PerMinute: 1_000, PerHour: 30_000, PerDay: 500_000},
	}
}

// MarketMakerLimits is the documented budget of a market-maker account. Its reads have the
// default budget.
func MarketMakerLimits() Limits {
	return Limits{
		ClassOrderCreate: {PerSecond: 4_000, PerMinute: 40_000, PerHour: 1_200_000, PerDay: 16_000_000},
		ClassOrderModify: {PerSecond: 1_000, PerMinute: 10_000, PerHour: 300_000, PerDay: 4_000_000},
		ClassOrderCancel: {PerSecond: 2_000, PerMinute: 20_000, PerHour: 600_000, PerDay: 7_500_000},
		ClassReadOrders:  {PerSecond: 50, PerMinute: 1_000, PerHour: 30_000, PerDay: 500_000},
		ClassReadAccount: {PerSecond: 50, PerMinute: 1_000, PerHour: 30_000, PerDay: 500_000},
	}
}

func limitsForAccountType(accountType string) pace.Limits {
	if accountType == accountTypeMarketMaker {
		return MarketMakerLimits().windows()
	}
	return DefaultLimits().windows()
}

func createClass(reduceOnly bool) rails.OpClass {
	if reduceOnly {
		return ClassOrderReduceOnly
	}
	return ClassOrderCreate
}

// Pacer keeps the metered calls under the account's rate limits from the client's side, and
// when the exchange rate-limits a call anyway, holds that class back. It implements
// [rails.Pacer]; install it with [rails.WithPacer].
//
// Commands sent over HTTP are refused for their budget on the orders stream rather than with a
// 429, so the pacer sees those refusals only while a [Conn] of the same client is open. A
// command a [Conn] sends over HTTP because its socket is down is paced once.
//
// Limits are per account and shared by everything using the key, so a program running several
// instances should divide its limits between them with [WithLimits].
type Pacer struct {
	cfg                 pace.Config
	rateLimitedCooldown time.Duration
	engine              *pace.Pacer
}

// PacerOption configures a [Pacer].
type PacerOption func(*Pacer)

// NewPacer builds a pacer on [DefaultLimits].
func NewPacer(opts ...PacerOption) *Pacer {
	p := &Pacer{rateLimitedCooldown: defaultRateLimitedCooldown}
	p.cfg = pace.Config{
		All:         ClassAll,
		Explicit:    pace.Limits{},
		Preset:      limitsForAccountType,
		Throttle:    p.throttled,
		MaxCooldown: defaultMaxCooldown,
		Now:         time.Now,
	}
	for _, opt := range opts {
		opt(p)
	}
	p.engine = pace.New(p.cfg)
	return p
}

// throttled recognises a refusal for a spent budget, and how long to hold its class back.
func (p *Pacer) throttled(err *rails.Error) (time.Duration, bool) {
	switch {
	case err.Code == CodeRateLimited:
		return p.rateLimitedCooldown, true
	case err.Code == rails.CodeTooManyRequests && err.RetryAfter > 0:
		return err.RetryAfter, true
	}
	return 0, false
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

// WithMaxCooldown caps how long a refusal holds a class back. Defaults to five minutes. A cap
// below the exchange's Retry-After does not shorten its throttle: the next call goes out sooner
// and is likely refused again.
func WithMaxCooldown(d time.Duration) PacerOption {
	return func(p *Pacer) { p.cfg.MaxCooldown = d }
}

// WithRateLimitedCooldown sets how long a [CodeRateLimited] refusal, which names no wait, holds
// its class back. Defaults to one second.
func WithRateLimitedCooldown(d time.Duration) PacerOption {
	return func(p *Pacer) { p.rateLimitedCooldown = d }
}

// Wait implements [rails.Pacer].
func (p *Pacer) Wait(ctx context.Context, class rails.OpClass, op string) error {
	return p.engine.Wait(ctx, class, op)
}

// Observe implements [rails.Pacer]: a [CodeRateLimited] refusal puts the class on cooldown for
// [WithRateLimitedCooldown], a 429 for its Retry-After, both capped by [WithMaxCooldown]. A
// longer cooldown already running stands.
func (p *Pacer) Observe(class rails.OpClass, err error) { p.engine.Observe(class, err) }
