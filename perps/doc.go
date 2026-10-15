// Package perps is the client for the Rails perpetuals API.
//
// Orders are created, modified and cancelled over a WebSocket, which also carries the account's
// fills and a market's trades and order book, and sets an isolated position's margin. HTTP
// covers the reads (orders, fills, balances, positions, the ledger, public market data), the
// account's leverage and margin mode, and the cancel-all escape hatch.
//
//	client, err := perps.NewClient(creds, rails.WithEnvironment(rails.Sandbox()))
//	conn, err := client.Connect(ctx, "BTC-USDT")
//	res, err := conn.CreateOrder(ctx, perps.CreateOrderRequest{
//		Price: "50000", Quantity: "0.0001",
//		OrderType: perps.OrderTypeBuy, TradeType: perps.TradeTypeLimit, PostOnly: true,
//	})
//
// The commands on [Conn] return once the exchange has answered, not when it has acknowledged:
// the acknowledgement only says the command was taken. Fills, rejections, stream data and
// connection events arrive on [Conn.Events], which must be drained.
//
// Money and quantities are strings, exactly as the exchange sends them. Timestamps are int64
// milliseconds since the Unix epoch.
//
// Request and result structs open with a blank field, so they can only be built with keyed
// literals and fields can be added to them in any release.
package perps
