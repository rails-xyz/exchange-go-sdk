// Package options is the client for the Rails options API.
//
// Contracts are named in full, such as BTC-7FEB26-100000-C: underlying, expiry, strike, and C
// for a call or P for a put. [Client.Expirations] and [Client.Contracts] list what trades.
//
//	client, err := options.NewClient(creds, rails.WithEnvironment(rails.Sandbox()))
//	conn, err := client.Connect(ctx, options.Scope{Market: "BTC-USDT"})
//	result, err := conn.CreateOrder(ctx, options.CreateOrderRequest{
//		Contract: "BTC-7FEB26-100000-C", Price: "10", Quantity: "1",
//		OrderType: options.OrderTypeBuy, TradeType: options.TradeTypeLimit, PostOnly: true,
//	})
//
// [Client.Connect] opens the options WebSocket for the contracts of one [Scope], and its order
// commands wait for the outcome.
//
// Money, quantities and greeks are strings, exactly as the exchange sends them. Timestamps are
// int64 milliseconds since the Unix epoch.
//
// Request and result structs open with a blank field, so they can only be built with keyed
// literals and fields can be added to them in any release.
package options
