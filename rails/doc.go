// Package rails holds the configuration, error and instrumentation types shared by every Rails
// product client. Import a product package such as
// [github.com/rails-xyz/exchange-go-sdk/perps] to talk to the exchange; this package only
// supplies what those clients are built from.
//
// # Construction
//
// Credentials are positional, everything else is an option:
//
//	client, err := perps.NewClient(
//		rails.Credentials{APIKey: key, APISecret: secret},
//		rails.WithEnvironment(rails.Sandbox()), // Production() is the default
//	)
//
// # Errors
//
// Every failure the exchange described is an [*Error]. Branch on the condition with
// [errors.Is], reach for the detail with [errors.As]:
//
//	if errors.Is(err, rails.ErrRateLimited) {
//		var apiErr *rails.Error
//		errors.As(err, &apiErr)
//		time.Sleep(apiErr.RetryAfter)
//	}
//
// [ErrTimeout] and [ErrConnClosed] mean a command may still execute: look it up by its
// clientRequestId rather than sending it again. [ErrNotSent] means it never left.
//
// # Instrumentation
//
// [Hooks] is a struct of optional funcs that receive every measurement the SDK takes. The same
// numbers go to Rails once a minute unless [WithTelemetry] or RAILS_SDK_TELEMETRY=off turns
// that off.
//
// # Money
//
// Prices, quantities and balances are strings everywhere, exactly as the exchange sends them.
// Timestamps are int64 milliseconds since the Unix epoch.
package rails
