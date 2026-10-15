# exchange-go-sdk

The Go client for the Rails API. Placing and cancelling orders goes over the WebSocket, reads
over HTTP. The client takes care of the token exchange and its refresh, the single-subprotocol
handshake, the acknowledge-then-answer order protocol, reconnects with backoff, session resume,
keepalive, and the rate-limit rules.

## Documentation

The API and the SDK are documented at [docs.rails.xyz](https://docs.rails.xyz).

## Install

```sh
go get github.com/rails-xyz/exchange-go-sdk
```

Go 1.25 or later. The module follows [semantic versioning](https://semver.org/); changes are
listed in [CHANGELOG.md](CHANGELOG.md).

## Products

Each product has its own package and guide:

- [`perps`](perps/README.md): the perpetuals API — quickstart, orders, the HTTP API, streams,
  pacing and error codes.
- [`options`](options/README.md): the options API — quickstart, orders over the WebSocket,
  scopes, streams, pacing and error codes.

## Retries, timeouts and rate limits

Every call takes a `context.Context`, and its deadline is the only timeout the SDK applies. On
HTTP it bounds the whole call, retries included; on the WebSocket, a command not answered by the
deadline returns `rails.ErrTimeout` carrying its `clientRequestId`.

HTTP reads are retried up to two more times on connection errors, `408`, `429` and `5xx`, with
jittered backoff or after the wait the exchange named in `Retry-After`. Nothing that changes
state is retried, on HTTP or on the WebSocket, except a cancel the exchange itself asked to
have sent again ([perps](perps/README.md#how-orders-work)) and an options cancel whose answer
the connection lost, sent again over HTTP under a fresh `clientRequestId`
([options](options/README.md#when-the-socket-is-down)).

Rate limits are per account and shared with everything else using it. The SDK exposes what the
exchange reports, such as `RetryAfter` on a `429`, and paces only when asked to (below).

## Pacing

A product's pacer keeps the metered calls under the account's budgets from the client's side,
so they wait a little rather than come back as `429`s. It is off unless installed with
`rails.WithPacer`; each product's guide lists its classes and presets
([perps](perps/README.md#pacing), [options](options/README.md#pacing)).

A call that would exceed a ceiling waits until it fits, on the caller's goroutine and within
its context; a context that ends during the wait returns its error and nothing was sent. Each
window is counted as the exchange counts it, as a sliding window, so after a burst that spends a
window's quota the next call waits for the burst to slide out, not only for the next slot at the
average rate.

When the exchange rate-limits a call anyway, the pacer holds that class back for the
`Retry-After` it named, plus a small margin, capped by the pacer's `WithMaxCooldown`. With
`WithFailFast` a call that would have to wait is refused instead, with `rails.ErrNotSent`
wrapping `rails.ErrPaced` and `RetryAfter` set to the wait it would have imposed.

Budgets are per account, so a program running several instances on one key should divide its
limits between them, or plug in a `rails.Pacer` of its own that coordinates them.

## Environments

`rails.Production()` is the default, and orders placed there are real. `rails.Sandbox()` is the
public test environment. A custom `rails.Environment{...}` takes every host explicitly. API keys
are created in the Rails app.

## Configuration

Everything after the credentials is an option on a product's `NewClient`, such as
`perps.NewClient`:

- `rails.WithEnvironment(env)`: the hosts, as above.
- `rails.WithHTTPClient(c)`: the `*http.Client` behind HTTP calls and the WebSocket upgrade, for
  a proxy, your own TLS setup, or a tracing transport.
- `rails.WithLogger(l)`: a `*slog.Logger` for the SDK's own diagnostics, reconnects and telemetry
  delivery among them. The default discards them.
- `rails.WithTokenSource(ts)`: replaces the built-in token cache, for pre-minted tokens or a
  cache shared across processes.
- `rails.WithPacer(p)`: client-side pacing, above.
- `rails.WithHooks(h)` and `rails.WithTelemetry(on)`: below.

## Errors

Every failure the exchange described is a `*rails.Error` carrying the operation, the status
code, the machine-readable `Slug` and `Code`, the message, how long the exchange asked you to
wait, the `clientRequestId`, and for an options create whose outcome is unknown the `OrderID`.
The message is for people and may be reworded; the code never changes meaning, so branch on it.
Shared codes are `rails.Code*`, each product's route codes are in its package (`perps.Code*`,
[with an example](perps/README.md#errors), and `options.Code*`), one constant per condition the
[Error Reference](https://docs.rails.xyz/latest/error-reference/error-code) documents.

What happened to the request itself is a sentinel, matched with `errors.Is`: `ErrUnauthorized`,
`ErrForbidden`, `ErrNotFound`, `ErrConflict` and `ErrRateLimited` follow from the status code;
`ErrSoftBanned`, `ErrTimeout`, `ErrConnClosed` and `ErrNotSent` describe what the SDK knows
about a command that got no answer; `ErrPaced` is a call a fail-fast pacer refused.

## Instrumentation

`rails.Hooks` is a struct of optional funcs. Set the ones you want:

```go
perps.NewClient(creds, rails.WithHooks(rails.Hooks{
	PingRTT:     func(info rails.ConnInfo, rtt time.Duration) { /* your metrics */ },
	RequestDone: func(info rails.ReqInfo, took time.Duration, err error) { /* … */ },
}))
```

The hooks:

- `ConnectStarted(ConnInfo)`: before every dial, reconnects included.
- `ConnectFinished(ConnInfo, ConnectStats, error)`: how the dial went, with its DNS, TCP, TLS and
  upgrade phases, or the error.
- `Disconnected(ConnInfo, DisconnectReason)`: a socket was lost, before the reconnect that
  follows. Closing the connection yourself does not fire it.
- `PingRTT(ConnInfo, time.Duration)`: the round trip of each keepalive ping.
- `RequestAcked(ReqInfo, time.Duration)`: send to `202` acknowledgement.
- `RequestDone(ReqInfo, time.Duration, error)`: send to the answer, or the failure that replaced
  it.
- `HTTPDone(HTTPInfo, time.Duration, error)`: every HTTP attempt, retries included, with its
  status or its error.
- `Paced(PaceInfo)`: a call the pacer delayed or refused, with the delay.
- `Throttled(OpClass, time.Duration)`: the exchange rate-limited a call and the pacer is holding
  the class back for that long.

`ConnInfo.ConnID` stays the same across the reconnects of one connection, so its observations
can be joined; `ReqInfo` names the command, its market and its `clientRequestId`.

Hooks run on the goroutine that made the observation and must not block.

## Telemetry

The SDK reports the latency it observes — connect, ping, order acknowledgement and answer, HTTP
calls — back to Rails once a minute, so problems are visible from the client's side before
anyone writes in. Reports carry latency histograms, counts, failure classes, the markets
involved, and the `clientRequestId` of commands that were slow or failed, so a bad experience
can be matched to the exchange's own logs. They never carry prices, quantities, balances or
order ids. They are sent with your API token, so Rails knows which account they came from.
The report types in [`internal/telemetry/report.go`](internal/telemetry/report.go) are the
complete field list.

It is on by default. Turn it off with `rails.WithTelemetry(false)` or `RAILS_SDK_TELEMETRY=off`.

## Contributing

Found a bug or missing a feature? Please [open an issue](https://github.com/rails-xyz/exchange-go-sdk/issues)
rather than a pull request.
