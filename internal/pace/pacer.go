// Package pace is the rate-limit engine behind each product's pacer. The product supplies the
// classes, windows, presets and the predicate that recognises a throttle.
package pace

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/auth"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// cooldownMargin keeps the first call after a cooldown off the exact boundary.
const cooldownMargin = 250 * time.Millisecond

// Limit is a quota per window. A window absent or zero is unlimited.
type Limit map[time.Duration]int

// Over lays l's non-zero windows over base.
func (l Limit) Over(base Limit) Limit {
	out := Limit{}
	for window, quota := range base {
		out[window] = quota
	}
	for window, quota := range l {
		if quota != 0 {
			out[window] = quota
		}
	}
	return out
}

// Limits is a quota per class. A class absent from it is unlimited.
type Limits map[rails.OpClass]Limit

// Config is what a product decides about its pacer.
type Config struct {
	// All is the class every metered call also draws on.
	All rails.OpClass
	// Explicit is laid over whichever preset is in force.
	Explicit Limits
	// Preset gives the limits for a token's accountType; "" is before any token is seen.
	Preset func(accountType string) Limits
	// Throttle reports how long the exchange asked a class to hold back; false if not a throttle.
	Throttle    func(*rails.Error) (time.Duration, bool)
	FailFast    bool
	FromToken   bool
	MaxCooldown time.Duration
	Now         func() time.Time
}

// Pacer implements [rails.Pacer] over a [Config].
type Pacer struct {
	cfg Config

	// No goroutine of its own, so a lock guards the state.
	mu          sync.Mutex
	hooks       rails.Hooks
	accountType string
	quotas      map[bucketKey]int
	buckets     map[bucketKey]*bucket
	cooldown    map[rails.OpClass]time.Time
}

// New builds a pacer on cfg.Preset("").
func New(cfg Config) *Pacer {
	p := &Pacer{
		cfg: cfg,
		hooks: rails.Hooks{
			Paced:     func(rails.PaceInfo) {},
			Throttled: func(rails.OpClass, time.Duration) {},
		},
		buckets:  map[bucketKey]*bucket{},
		cooldown: map[rails.OpClass]time.Time{},
	}
	p.setLimits(cfg.Preset(""))
	return p
}

// Wait implements [rails.Pacer].
func (p *Pacer) Wait(ctx context.Context, class rails.OpClass, op string) error {
	delay, release, hooks := p.reserve(class)
	if delay == 0 {
		return nil
	}
	if p.cfg.FailFast {
		release()
		hooks.Paced(rails.PaceInfo{Class: class, Op: op, Delay: delay, Refused: true})
		return fmt.Errorf("%w: %w: %w", rails.ErrNotSent, rails.ErrPaced, &rails.Error{
			Op:         op,
			Message:    fmt.Sprintf("the pacer would wait %s", delay),
			RetryAfter: delay,
		})
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		hooks.Paced(rails.PaceInfo{Class: class, Op: op, Delay: delay})
		return nil
	case <-ctx.Done():
		release()
		return ctx.Err()
	}
}

// reserve books one call on the class's and the All class's buckets, at the first moment every
// one of them has room; the func gives the bookings back.
func (p *Pacer) reserve(class rails.OpClass) (time.Duration, func(), rails.Hooks) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Bookings start where the cooldown ends, so its lifting cannot release them all at once.
	now := p.cfg.Now()
	start := now
	for _, c := range []rails.OpClass{class, p.cfg.All} {
		if until := p.cooldown[c]; until.After(start) {
			start = until
		}
	}
	at := start
	var booked []bucketKey
	for key, b := range p.buckets {
		if key.class == class || key.class == p.cfg.All {
			b.forget(now)
			at = latest(at, b.room(start, p.quotas[key]))
			booked = append(booked, key)
		}
	}
	for _, key := range booked {
		p.buckets[key].book(at, now)
	}
	// By key, not pointer: a preset switch may have dropped the bucket meanwhile.
	release := func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, key := range booked {
			if b := p.buckets[key]; b != nil {
				b.unbook(at)
			}
		}
	}
	return at.Sub(now), release, p.hooks
}

// Observe implements [rails.Pacer]: a throttle puts the class on cooldown for as long as the
// exchange asked, capped by MaxCooldown. A longer cooldown already running stands.
func (p *Pacer) Observe(class rails.OpClass, err error) {
	var apiErr *rails.Error
	if !errors.As(err, &apiErr) {
		return
	}
	wait, ok := p.cfg.Throttle(apiErr)
	if !ok {
		return
	}
	wait = min(wait+cooldownMargin, p.cfg.MaxCooldown)

	p.mu.Lock()
	until := p.cfg.Now().Add(wait)
	if until.After(p.cooldown[class]) {
		p.cooldown[class] = until
	}
	hooks := p.hooks
	p.mu.Unlock()
	hooks.Throttled(class, wait)
}

// Install hands the pacer the client's hooks.
func (p *Pacer) Install(hooks rails.Hooks) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hooks = hooks
}

// Tokens wraps a token source so the pacer sees each token's accountType, when it picks its
// preset from the token.
func (p *Pacer) Tokens(ts rails.TokenSource) rails.TokenSource {
	if !p.cfg.FromToken {
		return ts
	}
	return pacedTokenSource{TokenSource: ts, pacer: p}
}

func (p *Pacer) observeToken(token string) {
	accountType := auth.AccountType(token)
	if accountType == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if accountType == p.accountType {
		return
	}
	p.accountType = accountType
	p.setLimits(p.cfg.Preset(accountType))
}

// setLimits replaces the quotas. A bucket still limited keeps what it has counted.
func (p *Pacer) setLimits(preset Limits) {
	limits := Limits{}
	for class, limit := range preset {
		limits[class] = limit
	}
	for class, limit := range p.cfg.Explicit {
		limits[class] = limit.Over(limits[class])
	}

	quotas := map[bucketKey]int{}
	buckets := map[bucketKey]*bucket{}
	for class, limit := range limits {
		for window, quota := range limit {
			if quota <= 0 {
				continue
			}
			key := bucketKey{class: class, window: window}
			quotas[key] = quota
			if buckets[key] = p.buckets[key]; buckets[key] == nil {
				buckets[key] = &bucket{window: window, counts: map[int64]int{}}
			}
		}
	}
	p.quotas, p.buckets = quotas, buckets
}

type pacedTokenSource struct {
	rails.TokenSource
	pacer *Pacer
}

func (s pacedTokenSource) Token(ctx context.Context) (string, error) {
	token, err := s.TokenSource.Token(ctx)
	if err == nil {
		s.pacer.observeToken(token)
	}
	return token, err
}

type bucketKey struct {
	class  rails.OpClass
	window time.Duration
}

// bucket counts the calls booked in one window the way the exchange meters them: a sliding
// window counter, whose count at t is the calls in t's fixed window plus those of the window
// before, weighted by how much of it the trailing window still overlaps. Windows are aligned to
// the Unix epoch, as the exchange's are.
type bucket struct {
	window time.Duration
	counts map[int64]int // calls booked, by fixed window index
	// ahead is the bookings still to go out, in order. A new one never goes before the last, so
	// the count only falls from there.
	ahead []time.Time
}

// index is t's fixed window, and how far into it t is.
func (b *bucket) index(t time.Time) (int64, time.Duration) {
	n, w := t.UnixNano(), int64(b.window)
	return n / w, time.Duration(n % w)
}

// room is the first moment from start on at which one more call keeps the count within quota.
func (b *bucket) room(start time.Time, quota int) time.Time {
	t := start
	if n := len(b.ahead); n > 0 {
		t = latest(t, b.ahead[n-1])
	}
	idx, into := b.index(t)
	prev, cur, limit := float64(b.counts[idx-1]), float64(b.counts[idx]), float64(quota)
	window, rest := float64(b.window), float64(b.window-into)
	excess := prev*rest/window + cur + 1 - limit
	if excess <= 0 {
		return t
	}
	// The previous window's weight decays until the boundary; past it, this window's does.
	if prev > 0 {
		if wait := excess * window / prev; wait <= rest {
			return t.Add(nanos(wait))
		}
	}
	if cur+1 <= limit {
		return t.Add(nanos(rest))
	}
	return t.Add(nanos(rest + (cur+1-limit)*window/cur))
}

func (b *bucket) book(at, now time.Time) {
	idx, _ := b.index(at)
	b.counts[idx]++
	if at.After(now) {
		b.ahead = append(b.ahead, at)
	}
}

// unbook gives back a booking the caller did not use.
func (b *bucket) unbook(at time.Time) {
	idx, _ := b.index(at)
	if b.counts[idx] > 0 {
		b.counts[idx]--
	}
	if i := slices.Index(b.ahead, at); i >= 0 {
		b.ahead = slices.Delete(b.ahead, i, i+1)
	}
}

// forget drops what no longer counts at now: windows before the previous one, and bookings
// already gone out.
func (b *bucket) forget(now time.Time) {
	idx, _ := b.index(now)
	for i := range b.counts {
		if i < idx-1 {
			delete(b.counts, i)
		}
	}
	b.ahead = slices.DeleteFunc(b.ahead, func(t time.Time) bool { return !t.After(now) })
}

// nanos rounds a wait up, so a call never goes before it fits, but not over float noise.
func nanos(f float64) time.Duration { return time.Duration(math.Ceil(f - 1e-3)) }

func latest(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
