// Package telemetry feeds the SDK's measurements to two places: the caller's [rails.Hooks],
// synchronously where the measurement happened, and a report sent to Rails once a window. Both
// see the same numbers.
//
// Reports carry latency, counts and failure classes only — never prices, quantities, balances,
// order ids, account identity or addresses. Who sent one comes from the bearer token.
//
// Nothing here can delay or fail a trading call: aggregation runs on its own goroutine, an
// observation is dropped rather than made to wait, and a failed send is a debug line.
package telemetry
