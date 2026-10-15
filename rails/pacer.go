package rails

import (
	"context"
	"time"
)

// OpClass names a budget the exchange meters separately. The product packages define the
// values.
type OpClass string

// Pacer decides when a metered call may go out. Wait returns nil to send now, the context's
// error when the caller gave up first, or [ErrNotSent] wrapping [ErrPaced] when the pacer
// refuses instead of waiting. Observe sees every failure of a metered call, so a pacer can
// honour what the exchange asked for. Both are called from any goroutine. Install one with
// [WithPacer].
type Pacer interface {
	Wait(ctx context.Context, class OpClass, op string) error
	Observe(class OpClass, err error)
}

// PaceInfo describes a call the pacer held back.
type PaceInfo struct {
	Class OpClass
	Op    string
	// Delay is how long the call waited, or with Refused how long it would have.
	Delay   time.Duration
	Refused bool
}
