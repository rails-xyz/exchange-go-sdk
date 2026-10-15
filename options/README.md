# options

The client for the Rails options API. What every product shares — install, environments,
configuration, the error model, instrumentation and telemetry — is in the
[main README](../README.md).

## Quickstart

Find a contract over HTTP, then on a WebSocket connection to its expiry place a post-only bid
well below the book and cancel it, timing both.

```go
client, err := options.NewClient(
	rails.Credentials{
		APIKey:    os.Getenv("RAILS_API_KEY"),
		APISecret: os.Getenv("RAILS_API_SECRET"),
	},
)
if err != nil {
	return err
}
defer client.Close()

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

expirations, err := client.Expirations(ctx, "BTC-USDT")
if err != nil {
	return err
}
if len(expirations) == 0 {
	return errors.New("no expirations on BTC-USDT")
}
contracts, err := client.Contracts(ctx, "BTC-USDT", expirations[0].ExpiryDate)
if err != nil {
	return err
}
if len(contracts) == 0 {
	return errors.New("no contracts on BTC-USDT")
}
// A small bid rests on the most expensive contract.
contract, highest := contracts[0].ContractName, 0.0
for _, c := range contracts {
	if mark, _ := strconv.ParseFloat(c.MarkPrice, 64); mark > highest {
		contract, highest = c.ContractName, mark
	}
}

// One connection follows every contract of the expiry, which contract names spell second.
conn, err := client.Connect(ctx, options.Scope{Market: "BTC-USDT", Expiration: strings.Split(contract, "-")[1]})
if err != nil {
	return err
}
defer conn.Close()

start := time.Now()
created, err := conn.CreateOrder(ctx, options.CreateOrderRequest{
	Contract: contract, OrderType: options.OrderTypeBuy, TradeType: options.TradeTypeLimit,
	Price: "10", Quantity: "1", PostOnly: true,
})
if err != nil {
	return err
}
fmt.Printf("created %s: acknowledged in %s, %s in %s\n", created.Order.OrderID,
	created.AckAt.Sub(start), created.Order.Status, created.TerminalAt.Sub(start))

start = time.Now()
cancelled, err := conn.CancelOrder(ctx, options.CancelOrderRequest{Contract: contract, OrderID: created.Order.OrderID})
if err != nil {
	return err
}
fmt.Printf("%s in %s\n", cancelled.Order.Status, cancelled.TerminalAt.Sub(start))
```

The complete program is [`examples/options-quickstart`](../examples/options-quickstart/main.go):

```sh
RAILS_API_KEY=<key> RAILS_API_SECRET=<secret> go run ./examples/options-quickstart
```

## HTTP API

The HTTP operations on `client`:

- Contracts: `Expirations` (the expiry dates of a market such as `BTC-USDT`), `Contracts` (every
  contract of one expiry, with its prices, implied volatility and greeks).
- Account: `AccountSummary`, `Positions`, `Deposits`, `Withdrawals`.
- Orders: `OpenOrders`, `RecentOrders` (completed or cancelled), `OrderStatus`.
- Cancel all: `CancelAllOrders`, which needs no WebSocket connection.

Contracts are named in full, such as `BTC-7FEB26-100000-C`: underlying, expiry, strike, and `C`
for a call or `P` for a put. A position's `Qty`, `Value` and `NotionalValue` are signed, negative
for a short. Every amount is USDT.

## Cancelling everything over HTTP

`CancelAllOrders` sweeps one contract, or every contract of the account when `Contract` is
empty. It is queued, not executed: it queues one cancel per open order and returns them all in an
`options.QueuedCancelAll`, and each cancel's outcome arrives on the order stream of the options
WebSocket. Any order it lists in `Unqueued` is still open, so send it again.

The request carries a `clientRequestId`, minted as a UUID when the request does not set one. The
SDK never sends it twice. A `502` or `504` leaves the outcome unknown; the `*rails.Error`
carries the `ClientRequestID`, and `OpenOrders` shows what is still open.

## WebSocket

`client.Connect(ctx, scope)` opens a connection that reconnects on its own. It follows the
contracts of one `options.Scope`: every contract of a `Market` such as `BTC-USDT`, narrowed in
turn to an `Expiration` (spelled as in contract names, such as `24JUN26`), a `Strike` and a
`Type`, or every market with `options.ScopeAll()`. A strike needs an expiration and a type needs a
strike; `Connect` refuses a scope that breaks the chain without dialling.

The connection always receives the orders stream (`options.SubscriptionOrders`), which carries
the answer to every order command; `options.WithSubscriptions` adds others for the connection's
scope, and [`conn.Subscribe`](#streams-and-snapshots) for any scope later. `options.WithSessionID`,
`options.WithPingInterval` and `options.WithEventBuffer` work as they do for perpetuals.

### Commands

`conn.CreateOrder`, `conn.ModifyOrder`, `conn.CancelOrder` and `conn.CancelAllOrders` block until
the exchange answers:

- `CreateOrder` and `ModifyOrder` return an `*options.OrderResult` once the order rests, fills on
  arrival or is refused. A `200` is not always resting: read `Order.Status`, since a market order
  that finds no liquidity is answered with the order `cancelled`. A create takes an `OrderID` of
  your own, a ULID, and mints one when it has none; a modify keeps the order's `OrderID`.
- `CancelOrder` is answered without an acknowledgement, so `OrderResult.AckAt` is zero. An order
  already gone is refused with `options.CodeOrderNotFound`.
- `CancelAllOrders` sweeps one contract, or every contract of the connection's scope when
  `Contract` is empty, and returns an `*options.CancelAllResult` as soon as the exchange takes the
  sweep on. `Count` is the number of cancels it dispatched; each one is answered separately, as
  an `options.OrderCancelled` or `options.OrderRejected` event carrying the sweep's
  `ClientRequestID`, and those events may arrive before `CancelAllOrders` returns as well as
  after.

The exchange delivers a command's answers only to connections whose orders stream covers the
command's contract. A command the connection could not hear back about is refused before
anything is sent: `options.ErrNotSubscribed` without the orders stream, `options.ErrOutOfScope`
for a contract outside every scope it is held for.

A create or modify is never resent. After `rails.ErrTimeout` or `rails.ErrConnClosed` it may
still have been applied; the error carries the `ClientRequestID`, and for a create the
`OrderID`. Before placing it again, look for that order in `client.OpenOrders` and in both
`client.RecentOrders` histories, since it may already have filled or been cancelled.

### When the socket is down

Cancelling and closing positions go over HTTP when the connection cannot carry them:

- `CancelOrder` and `CancelAllOrders`, when the socket is down or dropped before the answer
  came. A cancel is idempotent by order id, so one that may have reached the exchange is sent
  again under a fresh `ClientRequestID`. `CancelAllOrders` without a `Contract` goes over HTTP
  only on a `ScopeAll()` connection, since there it sweeps every contract of the account.
- A `CreateOrder` with `ReduceOnly`, when the socket is down. One that may have reached the
  exchange is never resent, as it could close the position twice.

The result then has `ViaHTTP` set. The command is only queued: the result carries the ids, and
the outcome arrives as an event, as for a command another connection sent. A sweep's cancels
each carry their own `ClientRequestID`, listed in `CancelAllResult.Cancels`, and any order in
`Unqueued` is still open.

With no connection at all, `client.CancelAllOrders` sweeps one contract or the whole account.

### Events

`conn.Events()` delivers typed events and must be drained. For the contracts the orders stream is
held for, answers to commands sent elsewhere arrive too: a fill no command of this connection was
waiting for is an `options.OrderFilled`, a refusal an `options.OrderRejected`, a cancellation an
`options.OrderCancelled`. `options.Liquidation` is sent once for each contract a liquidation acts
on. Each other stream has its event, listed [below](#streams-and-snapshots). `Connected`,
`Disconnected`, `ConnectionShutdown`, `SessionBackfill`, `ServerError` and `Unknown` are as for
perpetuals.

```go
for event := range conn.Events() {
	switch e := event.(type) {
	case options.OrderFilled:
		// e.Order.FilledQuantity
	case options.Liquidation:
		// e.Contract, e.MarginRatio
	case options.SessionBackfill:
		if !e.Resumed || len(e.ExpiredMarkets) > 0 {
			// rebuild order state over HTTP
		}
	}
}
```

### Streams and snapshots

`conn.Subscribe(ctx, scope, streams...)` adds streams for any scope, not only the connection's
own, and `conn.Unsubscribe` removes them. The exchange keeps no scope that another one it holds
covers: subscribing a broader scope replaces the narrower ones under it, subscribing a covered
one changes nothing, and unsubscribing a scope removes every one under it (`options.ScopeAll()`
removes the stream everywhere). The account summary is account-wide, so any scope subscribes it
for every market. Subscriptions survive reconnects; one the exchange refuses to restore, such as
one over the connection's limit, is reported as an `options.SubscriptionLost` event and is no
longer held.

| Stream | Event | Snapshot |
|---|---|---|
| `SubscriptionContracts` | `ContractUpdate` | `EmitContracts` |
| `SubscriptionQuotes` | `QuoteUpdate` | `EmitQuotes` |
| `SubscriptionPublicOrderBook` | `OrderBookDelta` | `EmitOrderBook` |
| `SubscriptionPublicTrades` | `PublicTrades` | `EmitPublicTrades` |
| `SubscriptionPositions` | `PositionsDelta` | `EmitPositions` |
| `SubscriptionOpenOrders` | `OpenOrdersDelta` | `EmitOpenOrders` |
| `SubscriptionRecentOrders` | `RecentOrdersDelta` | `EmitRecentOrders` |
| `SubscriptionSettlements` | — | `EmitSettlements` |
| `SubscriptionAccountSummary` | — | `EmitAccountSummary` |

A snapshot is delivered on its stream, so each `Emit` method needs that stream held for what it
names, and without it returns `options.ErrNotSubscribed` having sent nothing:

- `EmitContracts` and `EmitQuotes` name an expiry of a market, and need the stream for
  `ScopeAll()` or any scope of that market.
- `EmitOrderBook` and `EmitPublicTrades` name a contract, and need a scope covering it.
- `EmitPositions`, `EmitOpenOrders`, `EmitRecentOrders` and `EmitSettlements` name a contract, or
  every contract when `Contract` is empty, which needs the stream for `ScopeAll()`. Settlements
  come back for every contract whichever one is named.
- `EmitAccountSummary` needs its stream for any scope.

A position that closes arrives once in a `PositionsDelta`, with `Side` `options.PositionSideFlat`.
Only the orders stream is replayed after a reconnect: take a new snapshot of every other stream
on each `Connected`.

```go
if err := conn.Subscribe(ctx, options.ScopeAll(), options.SubscriptionAccountSummary); err != nil {
	return err
}
summary, err := conn.EmitAccountSummary(ctx)
```

### Sessions

A connection keeps one session across its reconnects, so order events missed while it was down
are replayed. The replay is per contract and covers at most 10 contracts per reconnect, and
none with more than 500 missed events; the rest are listed in `SessionBackfill.ExpiredMarkets`,
and their state has to be read over HTTP.

## Paging

`OpenOrders`, `RecentOrders`, `Deposits` and `Withdrawals` return one `options.Page[T]` at a time:
pass `Next` back as the `PageToken` until it comes back empty. A page can hold fewer items than
asked for and still not be the last.

```go
for query := (options.PageQuery{}); ; {
	page, err := client.Deposits(ctx, query)
	if err != nil {
		return err
	}
	// page.Items
	if page.Next == "" {
		break
	}
	query.PageToken = page.Next
}
```

## Pacing

`options.NewPacer` is the options pacer; how pacing works is in the
[main README](../README.md#pacing). It is off unless installed:

```go
client, err := options.NewClient(creds,
	rails.WithPacer(
		options.NewPacer(
			options.WithLimitsFromToken(),
			options.WithLimits(
				options.Limits{
					options.ClassOrderCreate: {PerSecond: 5},
				},
			),
		),
	),
)
```

Ceilings are per second, minute, hour and day, per class of operation
(`options.ClassOrderCreate`, `ClassOrderModify`, `ClassOrderCancel`, `ClassOrderCancelAll`,
`ClassOrderReduceOnly`, `ClassReadOrders`, `ClassReadAccount`), and `options.ClassAll` caps
everything together. `options.DefaultLimits()` and `options.MarketMakerLimits()` are the
documented budgets of each account type; the pacer starts on the default and
`WithLimitsFromToken` switches to the one your token's account type names. Cancel-all and
reduce-only creates have no documented budget, so they are unlimited until `WithLimits` gives
them a ceiling. A modify is paced as a modify, although the exchange charges the modify of a
reduce-only order to reduce-only. `Ping`, connecting, subscribing, the snapshots and the
contract reads are never paced.

An `options.CodeRateLimited` refusal names no wait, so it holds its class back for a second
(`WithRateLimitedCooldown`). Commands sent over HTTP are refused on the order stream rather
than with a `429`, so the pacer sees those refusals only while a connection of the same client
is open. A command that goes over HTTP because the socket is down is paced once. A rate limit
the gateway reports outside any command holds back every class.

## Errors

Every failure is a `*rails.Error`; the options order codes are `options.Code*`, the codes every
product shares `rails.Code*`, and the error model is in the [main README](../README.md#errors).

```go
var apiErr *rails.Error
if errors.As(err, &apiErr) {
	switch apiErr.Code {
	case rails.CodeTooManyRequests:
		time.Sleep(apiErr.RetryAfter)
	case rails.CodeServiceUnavailable:
		// the outcome is unknown: look the order up before sending it again
	}
}
```
