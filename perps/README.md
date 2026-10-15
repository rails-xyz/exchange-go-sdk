# perps

The client for the Rails perpetuals API. What every product shares — install, environments,
configuration, the error model, instrumentation and telemetry — is in the
[main README](../README.md).

## Quickstart

Place a post-only bid, cancel it, and print how long the exchange took at each step. Pick a
price well below the market so the bid rests instead of trading.

```go
client, err := perps.NewClient(
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

conn, err := client.Connect(ctx, "BTC-USDT")
if err != nil {
	return err
}
defer conn.Close()

start := time.Now()
created, err := conn.CreateOrder(ctx, perps.CreateOrderRequest{
	Price: "50000", Quantity: "0.0001",
	OrderType: perps.OrderTypeBuy, TradeType: perps.TradeTypeLimit, PostOnly: true,
})
if err != nil {
	return err
}
fmt.Printf("created %s: acknowledged in %s, %s in %s\n", created.Order.OrderID,
	created.Ack.At.Sub(start), created.Order.OrderStatus, created.TerminalAt.Sub(start))

_, err = conn.CancelOrder(ctx, perps.CancelOrderRequest{OrderID: created.Order.OrderID})
return err
```

The complete program is [`examples/perps-quickstart`](../examples/perps-quickstart/main.go):

```sh
RAILS_API_KEY=<key> RAILS_API_SECRET=<secret> go run ./examples/perps-quickstart -market BTC-USDT -price 50000
```

## How orders work

Nothing on the WebSocket is synchronous. Every order command is answered twice: a `202`
acknowledgement that says the exchange took it, then the answer, a `200` or an error, which can
arrive seconds later. `CreateOrder`, `ModifyOrder`, `CancelOrder` and `UpdateMargin` block until
the answer and report both moments, `Ack.At` and `TerminalAt`, so acknowledgement latency and
answer latency can be watched separately.

`ModifyOrder` replaces the order: the original is cancelled and a new one, under a new id, takes
its place at the new price and quantity. The result describes the replacement. A rejection means
nothing changed and the original still rests; a timeout or a closed connection leaves the outcome
unknown, so reconcile with `client.OrderByClientRequestID` as for a create.

`UpdateMargin` sets the margin of an isolated position to an absolute amount. Asking for the
amount already in place is answered without an acknowledgement, so `MarginResult.Ack.At` is zero
when nothing changed.

Every command carries a `clientRequestId`, minted as a UUID when the request does not set one.
The SDK never resends a create: a reused id is rejected as a duplicate or silently dropped by
the exchange. When a call returns `rails.ErrTimeout` or `rails.ErrConnClosed` the order may
still be live, so look it up with `client.OrderByClientRequestID` before deciding what to do.
`rails.ErrNotSent`, or a bare context error, means it never left and can be sent again.

Cancelling is idempotent by order id, and `CancelOrder` uses that. When the exchange answers
that it could not apply the cancel because of a race inside it (`perps.CodeCancelOrderConflictError`),
the call sends it again under a fresh id, up to two more times. When it answers that another
cancel of the same order is already in flight (`perps.CodeCancelOrderAlreadyCancelling`), the
call waits for that cancel's answer and returns the cancelled order, or the error that cancel
got, such as `perps.CodeCancelOrderNotCancellable` when a fill got there first.

A `Conn` is safe for concurrent use: commands from any number of goroutines share the
connection, and each is matched to its answer by its `clientRequestId`.

`perps.MarketAll` names every market at once on `Connect`, `OpenOrders`, `Positions`,
`Balances`, `Fundings` and `CancelAllOrders`. A connection to it receives every market's
events, and each request on it has to name its own market.

## HTTP API

The HTTP operations on `client`:

- Orders: `OpenOrders` (drains every page), `OrderByID`, `OrderByClientRequestID`,
  `CompletedOrders` (fills, newest first, one page at a time), `CancelAllOrders`.
- Account: `Balances`, `Positions`, `Fundings`, `Deposits`, `Withdrawals`.
- Settings: `UpdateLeverage`, `UpdateMarginMode`. The exchange refuses both while the market
  has open orders or a position.
- Market data: `Markets`, `ProductSpecifications` (tick and lot sizes, order size bounds),
  `ContractSpecifications`, `MarketData`, `OrderBook`.

Paginated reads return a `perps.Page[T]`: pass `Next` back as the cursor until it comes back
empty.

```go
for cursor := ""; ; {
	page, err := client.Deposits(ctx, cursor)
	if err != nil {
		return err
	}
	// page.Items
	if cursor = page.Next; cursor == "" {
		break
	}
}
```

## Pacing

During a soft ban, when the WebSocket cannot be dialled, `client.CancelAllOrders` stays reachable.

`perps.NewPacer` is the perps pacer; how pacing works is in the
[main README](../README.md#pacing). It is off unless installed:

```go
client, err := perps.NewClient(creds,
	rails.WithPacer(
		perps.NewPacer(
			perps.WithLimitsFromToken(), 
			perps.WithLimits(
				perps.Limits{
					perps.ClassOrderCreate: {PerSecond: 5},
				}, 
			), 
		), 
	),
)
```

Ceilings are per second and per minute, per class of operation (`perps.ClassOrderCreate`,
`ClassOrderModify`, `ClassOrderCancel`, `ClassAccountWrite`, `ClassReadOrders`,
`ClassReadAccount`, `ClassReadLedger`), and `perps.ClassAll` caps everything together. The
presets cover the order classes only; the others are unlimited until `WithLimits` gives them a
ceiling. `CancelAllOrders`, `Ping`, connecting, subscribing, the snapshots and the market-data reads are never paced. `perps.DefaultLimits()` and `perps.MarketMakerLimits()` are the documented
budgets of each account type; the pacer starts on the default and `WithLimitsFromToken` switches
to the one your token's account type names. `WithLimits` lays your own ceilings over the preset.

The pacer treats a refusal of creates for a while (`perps.CodeCreateOrderBanned`) as a rate
limit on the create class, and a rate limit the gateway reports outside any command as one on
every class.

## Events and streams

Fills, rejections, stream data and connection events arrive on `conn.Events()`. The channel is
buffered and applies backpressure rather than dropping frames, so it must be read.

A connection receives the `orders` stream, always, plus whatever `perps.WithSubscriptions` asked
for at `Connect`; `conn.Subscribe` and `conn.Unsubscribe` change that later and return the
active set, which a reconnect asks for again. The `orders` stream cannot be removed. The `trades` stream carries the account's fills (`perps.Trades`), `publicTrades` a
market's fills (`perps.PublicTrades`), and `publicOrderBook` changes to its book
(`perps.OrderBookDelta`). `conn.EmitPublicTrades` and `conn.EmitPublicOrderBook` return the
snapshot a stream continues from; deltas carry no sequence number, so a book kept from them is
only as good as its last snapshot.

```go
for event := range conn.Events() {
	switch e := event.(type) {
	case perps.OrderFilled:
		// e.Order, e.Partial
	case perps.OrderRejected:
		// e.Order, e.Body
	case perps.Trades:
		// e.Trades: fills on your orders, every market
	case perps.PublicTrades, perps.OrderBookDelta:
		// market data for the connection's market
	case perps.SessionBackfill:
		if !e.Resumed {
			// the session could not be resumed after a reconnect: rebuild from client.OpenOrders
		}
	case perps.Disconnected, perps.Connected, perps.ConnectionShutdown:
		// the SDK reconnects on its own; these are for your own monitoring
	case perps.Unknown:
		// a frame this release has no type for
	}
}
```

Reconnects happen on their own, with full-jitter backoff that never dials faster than the
exchange allows, and the same session id every time so missed order events are backfilled. A
soft ban stops dialling until it lifts.

## Errors

The perpetuals route codes are `perps.Code*`; the error model is in the
[main README](../README.md#errors).

```go
var apiErr *rails.Error
if errors.As(err, &apiErr) {
	switch apiErr.Code {
	case perps.CodeCreateOrderInsufficientBalance:
		// reduce size
	case perps.CodeCreateOrderMaxOpenOrders, perps.CodeCreateOrderBanned:
		// stop creating for apiErr.RetryAfter
	case rails.CodeTooManyRequests:
		time.Sleep(apiErr.RetryAfter)
	case rails.CodeRequestTimeout, rails.CodeServerError:
		// the effect is unknown: look the order up by apiErr.ClientRequestID
	}
}
```

A rejection the exchange pushes after accepting an order, a post-only that would have matched
for instance, arrives as a `perps.OrderRejected` event with the same `Slug` and `Code`
(`perps.CodeOrderRejectedPostOnly`). An error the gateway reports outside any command, such as
its message limiter, is a `perps.ServerError` event; when exactly one command was in flight and
not yet acknowledged, that command also fails with `rails.ErrNotSent`, since it never reached the
exchange and can be sent again.
