package options_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/rails-xyz/exchange-go-sdk/internal/spectest"
	"github.com/rails-xyz/exchange-go-sdk/options"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every example in api/options-ws.yaml goes through this release's types; a frame that failed to
// decode would reach callers as Unknown unnoticed. A message with no check fails the test.
// Decoded values are compared by name with what a caller sees, so a field mapped to the wrong
// one fails too.

const wsSpec = "../api/options-ws.yaml"

func TestSpecExamplesConform(t *testing.T) {
	examples := spectest.LoadExamples(t, wsSpec)
	require.NotEmpty(t, examples)

	checks := map[string]func(*testing.T, spectest.Example){
		"PingRequest":              checkPingRequest,
		"PongResponse":             checkPong,
		"ConnectionShutdownNotice": checkShutdown,

		"CreateOrderRequest":      checkCreateOrderRequest,
		"CreateOrderAck":          nil, // driven together with the confirmation below
		"CreateOrderConfirmation": nil,
		"CreateOrderError":        checkCommandError("createOrder"),
		"ModifyOrderRequest":      checkModifyOrderRequest,
		"ModifyOrderAck":          nil,
		"ModifyOrderConfirmation": nil,
		"ModifyOrderError":        checkCommandError("modifyOrder"),
		"CancelOrderRequest":      checkCancelOrderRequest,
		"CancelOrderConfirmation": checkCancelled,
		"CancelOrderError":        checkCommandError("cancelOrder"),
		"CancelAllOrdersRequest":  checkCancelAllRequest,
		"CancelAllOrdersAck":      checkCancelAllAck,
		"CancelAllOrdersError":    checkCommandError("cancelAllOrders"),

		"CompleteFillNotification":    checkFill,
		"PartialFillNotification":     checkFill,
		"SessionBackfillNotification": checkBackfill,
		"LiquidationNotification":     checkLiquidation,

		"SubscribeRequest":     checkSubscriptionRequest("subscribe"),
		"SubscribedResponse":   checkSubscriptionFlow("subscribe"),
		"SubscribeError":       checkSubscriptionError("subscribe"),
		"UnsubscribeRequest":   checkSubscriptionRequest("unsubscribe"),
		"UnsubscribedResponse": checkSubscriptionFlow("unsubscribe"),
		"UnsubscribeError":     checkSubscriptionError("unsubscribe"),

		"EmitOptionsContractsRequest":      checkEmitRequest("emitOptionsContracts"),
		"EmitOptionsQuotesRequest":         checkEmitRequest("emitOptionsQuotes"),
		"EmitPublicOrderBookRequest":       checkEmitRequest("emitPublicOrderBook"),
		"EmitPublicCompletedOrdersRequest": checkEmitRequest("emitPublicCompletedOrders"),
		"EmitOptionsPositionsRequest":      checkEmitRequest("emitOptionsPositions"),
		"EmitOptionsOpenOrdersRequest":     checkEmitRequest("emitOptionsOpenOrders"),
		"EmitOptionsRecentOrdersRequest":   checkEmitRequest("emitOptionsRecentOrders"),
		"EmitOptionsSettlementsRequest":    checkEmitRequest("emitOptionsSettlements"),
		"EmitOptionsAccountSummaryRequest": checkEmitRequest("emitOptionsAccountSummary"),

		"OptionsContractsResponse":      checkEmitFlow("emitOptionsContracts"),
		"OptionsQuotesResponse":         checkEmitFlow("emitOptionsQuotes"),
		"PublicOrderBookResponse":       checkEmitFlow("emitPublicOrderBook"),
		"PublicCompletedOrdersResponse": checkEmitFlow("emitPublicCompletedOrders"),
		"OptionsPositionsResponse":      checkEmitFlow("emitOptionsPositions"),
		"OptionsOpenOrdersResponse":     checkEmitFlow("emitOptionsOpenOrders"),
		"OptionsRecentOrdersResponse":   checkEmitFlow("emitOptionsRecentOrders"),
		"OptionsSettlementsResponse":    checkEmitFlow("emitOptionsSettlements"),
		"OptionsAccountSummaryResponse": checkEmitFlow("emitOptionsAccountSummary"),

		"OptionsContractUpdate":      checkContractUpdate,
		"OptionsQuotesDelta":         checkQuoteUpdate,
		"PublicOrderBookDelta":       checkOrderBookDelta,
		"PublicCompletedOrdersDelta": checkPublicTrades,
		"OptionsPositionsDelta":      checkPositionsDelta,
		"OptionsOpenOrdersDelta":     checkOpenOrdersDelta,
		"OptionsRecentOrdersDelta":   checkRecentOrdersDelta,
	}

	for message := range examples {
		assert.Contains(t, checks, message, "%s is in the spec but not checked", message)
	}
	for message := range checks {
		assert.Contains(t, examples, message, "%s is checked but no longer in the spec", message)
	}

	for message, check := range checks {
		if check == nil {
			continue
		}
		for _, ex := range examples[message] {
			name := message
			if ex.Name != "" {
				name += "/" + ex.Name
			}
			t.Run(name, func(t *testing.T) { check(t, ex) })
		}
	}

	// Examples leave optional keys out, so a command's keys are checked against its schema.
	properties := spectest.ContentProperties(t, wsSpec)
	spectest.AssertTagsDocumented(t, options.CreateOrderContent{}, properties["CreateOrderRequest"])
	spectest.AssertTagsDocumented(t, options.ModifyOrderContent{}, properties["ModifyOrderRequest"])
	spectest.AssertTagsDocumented(t, options.CancelOrderContent{}, properties["CancelOrderRequest"])
	spectest.AssertTagsDocumented(t, options.CancelAllContent{}, properties["CancelAllOrdersRequest"])
	spectest.AssertTagsDocumented(t, options.SubscriptionContent{}, properties["SubscribeRequest"])
	spectest.AssertTagsDocumented(t, options.SubscriptionContent{}, properties["UnsubscribeRequest"])
	spectest.AssertTagsDocumented(t, options.ExpiryContent{}, properties["EmitOptionsContractsRequest"])
	spectest.AssertTagsDocumented(t, options.ExpiryContent{}, properties["EmitOptionsQuotesRequest"])
	for _, message := range []string{
		"EmitPublicCompletedOrdersRequest", "EmitOptionsPositionsRequest", "EmitOptionsOpenOrdersRequest",
		"EmitOptionsRecentOrdersRequest", "EmitOptionsSettlementsRequest",
	} {
		spectest.AssertTagsDocumented(t, options.ContractContent{}, properties[message])
	}
	spectest.AssertTagsDocumented(t, options.AccountSummaryContent{}, properties["EmitOptionsAccountSummaryRequest"])

	t.Run("CreateOrder/AckThenConfirmation", func(t *testing.T) {
		checkCommandFlow(t, "createOrder", examples["CreateOrderAck"], examples["CreateOrderConfirmation"])
	})
	t.Run("ModifyOrder/AckThenConfirmation", func(t *testing.T) {
		checkCommandFlow(t, "modifyOrder", examples["ModifyOrderAck"], examples["ModifyOrderConfirmation"])
	})
}

// Pushes: decoded strictly, so a field the spec has and this release does not is a failure,
// then turned into the event a caller would see when no command of its own claims them.

func checkShutdown(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	spectest.Strict(t, frame.Data, new(options.ShutdownData))
	data := spectest.Decode(t, frame.Data)

	event := spectest.EventAs[options.ConnectionShutdown](t, options.ToEvent, frame)
	assert.Equal(t, time.Duration(spectest.Num(data["forceCloseInMs"]))*time.Millisecond, event.ForceCloseIn)
	assert.Equal(t, time.UnixMilli(int64(spectest.Num(data["ts"]))), event.At)
}

func checkBackfill(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	var data options.BackfillWire
	spectest.Strict(t, frame.Data, &data)

	event := spectest.EventAs[options.SessionBackfill](t, options.ToEvent, frame)
	assert.Equal(t, options.SessionBackfill(data), event)
	assert.NotEmpty(t, event.SessionID)
}

func checkFill(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	var data options.OrderData
	spectest.Strict(t, frame.Data, &data)
	require.NotNil(t, data.Order)

	event := spectest.EventAs[options.OrderFilled](t, options.ToEvent, frame)
	assert.Equal(t, options.OrderFromWire(data.Order, frame.Market), event.Order)
	assert.Equal(t, frame.ResultType == "partialFillOrder", event.Partial)
	assert.NotEmpty(t, event.Order.OrderID)
}

func checkCancelled(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	var data options.OrderData
	spectest.Strict(t, frame.Data, &data)
	require.NotNil(t, data.Order)

	event := spectest.EventAs[options.OrderCancelled](t, options.ToEvent, frame)
	assert.Equal(t, options.OrderFromWire(data.Order, frame.Market), event.Order)
	assert.Equal(t, data.ClientRequestID, event.ClientRequestID)
	assert.Equal(t, options.StatusCancelled, event.Order.Status)
}

func checkLiquidation(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	var data options.LiquidationData
	spectest.Strict(t, frame.Data, &data)

	event := spectest.EventAs[options.Liquidation](t, options.ToEvent, frame)
	assert.Equal(t, options.Liquidation{
		Contract:      frame.Market,
		LiquidationID: data.LiquidationID,
		MarginRatio:   data.MarginRatio,
		UsedMargin:    data.UsedMargin,
		TotalEquity:   data.TotalEquity,
		TriggeredAt:   data.TriggeredAt,
	}, event)
	assert.NotEmpty(t, event.LiquidationID)
}

// Command answers: pushed by the fake exchange at a real connection, so the routing fields are
// read by the code that reads them in production.

func checkPong(t *testing.T, ex spectest.Example) {
	srv := fakeExchange(t)
	srv.SetAutoPong(false)
	conn := tradingConn(t, srv, options.ScopeAll())

	pinged := async(func() (time.Duration, error) { return conn.Ping(t.Context()) })
	crid := command(t, srv, "ping").ClientRequestID()
	srv.Push(spectest.WithClientRequestID(t, ex, crid))

	require.NoError(t, spectest.Receive(t, pinged).err, "the documented pong did not end the ping")
}

func checkCommandFlow(t *testing.T, op string, acks, confirmations []spectest.Example) {
	require.Len(t, acks, 1)
	require.Len(t, confirmations, 1)
	ack, confirmation := spectest.FrameOf(t, acks[0]), spectest.FrameOf(t, confirmations[0])
	spectest.Strict(t, ack.Data, new(options.OrderData))
	var confirmed options.OrderData
	spectest.Strict(t, confirmation.Data, &confirmed)
	require.NotNil(t, confirmed.Order)
	crid := confirmed.ClientRequestID
	require.Equal(t, crid, spectest.Str(spectest.Decode(t, ack.Data)["clientRequestId"]), "the two examples must describe one command")

	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())
	got := sendCommand(t, conn, op, crid)
	command(t, srv, op)
	srv.Push(json.RawMessage(acks[0].Payload))
	srv.Push(json.RawMessage(confirmations[0].Payload))

	res := spectest.Receive(t, got)
	require.NoError(t, res.err)
	result := res.value.(*options.OrderResult)
	assert.Equal(t, crid, result.ClientRequestID)
	assert.False(t, result.AckAt.IsZero(), "the acknowledgement went unrecorded")
	assert.Equal(t, options.OrderFromWire(confirmed.Order, confirmation.Market), result.Order)
}

func checkCancelAllAck(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	var data options.CancelAllData
	spectest.Strict(t, frame.Data, &data)

	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())
	got := sendCommand(t, conn, "cancelAllOrders", data.ClientRequestID)
	command(t, srv, "cancelAllOrders")
	srv.Push(json.RawMessage(ex.Payload))

	res := spectest.Receive(t, got)
	require.NoError(t, res.err)
	assert.Equal(t, &options.CancelAllResult{ClientRequestID: data.ClientRequestID, Count: data.Count}, res.value)
}

// checkCommandError answers a command with the documented rejection, which must come back as
// the error it describes and, unclaimed, as the event a caller would see.
func checkCommandError(op string) func(*testing.T, spectest.Example) {
	return func(t *testing.T, ex spectest.Example) {
		frame := spectest.FrameOf(t, ex)
		var data options.OrderData
		spectest.Strict(t, frame.Data, &data)
		require.NotNil(t, data.Error)
		assert.Contains(t, knownCodes, data.Error.Code, "the spec names a code this release has no constant for")

		srv := fakeExchange(t)
		conn := tradingConn(t, srv, options.ScopeAll())
		got := sendCommand(t, conn, op, data.ClientRequestID)
		command(t, srv, op)
		srv.Push(json.RawMessage(ex.Payload))

		var apiErr *rails.Error
		require.ErrorAs(t, spectest.Receive(t, got).err, &apiErr)
		assert.Equal(t, rails.Error{
			Op:              op,
			StatusCode:      data.StatusCode,
			Slug:            data.Error.Slug,
			Code:            data.Error.Code,
			Message:         data.Body,
			ClientRequestID: data.ClientRequestID,
		}, *apiErr)

		if op == "cancelAllOrders" {
			return
		}
		want := options.OrderRejected{
			Body: data.Body, Slug: data.Error.Slug, Code: data.Error.Code, ClientRequestID: data.ClientRequestID,
		}
		if data.Order != nil {
			want.Order = options.OrderFromWire(data.Order, frame.Market)
		}
		assert.Equal(t, want, spectest.EventAs[options.OrderRejected](t, options.ToEvent, frame))
	}
}

// Requests: built from the example's content and sent for real, so what goes on the wire is
// compared with what the spec says goes on the wire.

func exampleContent(t *testing.T, ex spectest.Example) map[string]any {
	t.Helper()
	return spectest.Decode(t, spectest.Raw(t, spectest.Decode(t, ex.Payload)["content"]))
}

func checkPingRequest(t *testing.T, ex spectest.Example) {
	srv := fakeExchange(t)
	srv.SetAutoPong(false)
	conn := tradingConn(t, srv, options.ScopeAll())
	go func() { _, _ = conn.Ping(t.Context()) }()

	sent := spectest.Decode(t, command(t, srv, "ping").Content)
	assert.ElementsMatch(t, spectest.Keys(exampleContent(t, ex)), spectest.Keys(sent))
}

func checkCreateOrderRequest(t *testing.T, ex spectest.Example) {
	content := exampleContent(t, ex)
	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())
	go func() {
		_, _ = conn.CreateOrder(t.Context(), options.CreateOrderRequest{
			Contract:        spectest.Str(content["market"]),
			Price:           spectest.Str(content["price"]),
			Quantity:        spectest.Str(content["quantity"]),
			OrderType:       options.OrderType(spectest.Str(content["orderType"])),
			TradeType:       options.TradeType(spectest.Str(content["tradeType"])),
			PostOnly:        content["postOnly"] == true,
			ReduceOnly:      content["reduceOnly"] == true,
			MaxSlippage:     spectest.Str(content["maxSlippage"]),
			OrderID:         spectest.Str(content["orderId"]),
			ClientRequestID: spectest.Str(content["clientRequestId"]),
		})
	}()
	sent := spectest.Decode(t, command(t, srv, "createOrder").Content)
	if _, given := content["orderId"]; !given {
		_, err := ulid.ParseStrict(spectest.Str(sent["orderId"]))
		assert.NoError(t, err, "an orderId should be minted when the request has none")
		delete(sent, "orderId")
	}
	spectest.AssertSends(t, sent, content)
}

func checkModifyOrderRequest(t *testing.T, ex spectest.Example) {
	content := exampleContent(t, ex)
	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())
	go func() {
		_, _ = conn.ModifyOrder(t.Context(), options.ModifyOrderRequest{
			Contract:        spectest.Str(content["market"]),
			OrderID:         spectest.Str(content["orderId"]),
			Price:           spectest.Str(content["price"]),
			Quantity:        spectest.Str(content["quantity"]),
			ClientRequestID: spectest.Str(content["clientRequestId"]),
		})
	}()
	spectest.AssertSends(t, spectest.Decode(t, command(t, srv, "modifyOrder").Content), content)
}

func checkCancelOrderRequest(t *testing.T, ex spectest.Example) {
	content := exampleContent(t, ex)
	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())
	go func() {
		_, _ = conn.CancelOrder(t.Context(), options.CancelOrderRequest{
			Contract:        spectest.Str(content["market"]),
			OrderID:         spectest.Str(content["orderId"]),
			ClientRequestID: spectest.Str(content["clientRequestId"]),
		})
	}()
	spectest.AssertSends(t, spectest.Decode(t, command(t, srv, "cancelOrder").Content), content)
}

func checkCancelAllRequest(t *testing.T, ex spectest.Example) {
	content := exampleContent(t, ex)
	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())
	go func() {
		_, _ = conn.CancelAllOrders(t.Context(), options.CancelAllOrdersRequest{
			Contract:        spectest.Str(content["market"]),
			ClientRequestID: spectest.Str(content["clientRequestId"]),
		})
	}()
	spectest.AssertSends(t, spectest.Decode(t, command(t, srv, "cancelAllOrders").Content), content)
}

// sendCommand issues op under crid and returns where its result will arrive.
func sendCommand(t *testing.T, conn *options.Conn, op, crid string) <-chan outcome[any] {
	t.Helper()
	return async(func() (any, error) {
		switch op {
		case "createOrder":
			req := limitOrder()
			req.ClientRequestID = crid
			return conn.CreateOrder(t.Context(), req)
		case "modifyOrder":
			return conn.ModifyOrder(t.Context(), options.ModifyOrderRequest{
				Contract: testContract, OrderID: "01KWK47RT6A4DD1TCQE662QJ3W", Price: "6300", Quantity: "1", ClientRequestID: crid,
			})
		case "cancelOrder":
			return conn.CancelOrder(t.Context(), options.CancelOrderRequest{
				Contract: testContract, OrderID: "01KWK47RT6A4DD1TCQE662QJ3W", ClientRequestID: crid,
			})
		case "cancelAllOrders":
			return conn.CancelAllOrders(t.Context(), options.CancelAllOrdersRequest{ClientRequestID: crid})
		}
		t.Errorf("no way to send %s", op)
		return nil, nil
	})
}

// Subscriptions.

func exampleScope(content map[string]any) options.Scope {
	return options.Scope{
		Market:     spectest.Str(content["market"]),
		Expiration: spectest.Str(content["expiration"]),
		Strike:     spectest.Str(content["strike"]),
		Type:       options.OptionType(spectest.Str(content["type"])),
	}
}

func exampleSubscriptions(v any) []options.Subscription {
	var out []options.Subscription
	for _, s := range v.([]any) {
		out = append(out, options.Subscription(s.(string)))
	}
	return out
}

func changeSubscription(ctx context.Context, conn *options.Conn, op string, scope options.Scope, subs []options.Subscription) error {
	if op == "subscribe" {
		return conn.Subscribe(ctx, scope, subs...)
	}
	return conn.Unsubscribe(ctx, scope, subs...)
}

func checkSubscriptionRequest(op string) func(*testing.T, spectest.Example) {
	return func(t *testing.T, ex spectest.Example) {
		content := exampleContent(t, ex)
		srv := fakeExchange(t)
		conn := tradingConn(t, srv, options.ScopeAll())
		go func() {
			_ = changeSubscription(t.Context(), conn, op, exampleScope(content), exampleSubscriptions(content["subscriptions"]))
		}()
		sent := spectest.Decode(t, command(t, srv, op).Content)
		delete(content, "clientRequestId")
		delete(sent, "clientRequestId")
		spectest.AssertSends(t, sent, content)
	}
}

// checkSubscriptionFlow answers a change with the documented confirmation, after which the
// connection holds exactly the streams it lists.
func checkSubscriptionFlow(op string) func(*testing.T, spectest.Example) {
	return func(t *testing.T, ex spectest.Example) {
		frame := spectest.FrameOf(t, ex)
		var data options.SubscriptionData
		spectest.Strict(t, frame.Data, &data)
		changed := data.RequestedSubscriptions
		if op == "unsubscribe" {
			changed = data.RequestedUnsubscriptions
		}

		srv := fakeExchange(t)
		conn, err := wsClient(t, srv).Connect(t.Context(), options.ScopeAll(),
			options.WithSubscriptions(slices.Concat(data.ActiveSubscriptions, changed)...), options.WithPingInterval(0))
		require.NoError(t, err)
		defer conn.Close()

		got := async(func() (struct{}, error) {
			return struct{}{}, changeSubscription(t.Context(), conn, op, options.ScopeAll(), changed)
		})
		crid := command(t, srv, op).ClientRequestID()
		srv.Push(spectest.WithClientRequestID(t, ex, crid))
		require.NoError(t, spectest.Receive(t, got).err)
		assert.ElementsMatch(t, data.ActiveSubscriptions, slices.Collect(maps.Keys(options.Held(conn))))
	}
}

func checkSubscriptionError(op string) func(*testing.T, spectest.Example) {
	return func(t *testing.T, ex spectest.Example) {
		frame := spectest.FrameOf(t, ex)
		var data options.SubscriptionData
		spectest.Strict(t, frame.Data, &data)
		require.NotNil(t, data.Error)
		assert.Contains(t, knownCodes, data.Error.Code, "the spec names a code this release has no constant for")

		srv := fakeExchange(t)
		conn := tradingConn(t, srv, options.ScopeAll())
		got := async(func() (struct{}, error) {
			return struct{}{}, changeSubscription(t.Context(), conn, op, options.ScopeAll(), []options.Subscription{options.SubscriptionQuotes})
		})
		crid := command(t, srv, op).ClientRequestID()
		srv.Push(spectest.WithClientRequestID(t, ex, crid))

		var apiErr *rails.Error
		require.ErrorAs(t, spectest.Receive(t, got).err, &apiErr)
		assert.Equal(t, rails.Error{
			Op: op, StatusCode: data.StatusCode, Slug: data.Error.Slug, Code: data.Error.Code,
			Message: data.Body, ClientRequestID: crid,
		}, *apiErr)
		assert.Equal(t, map[options.Subscription][]string{options.SubscriptionOrders: {"ALL"}}, options.Held(conn),
			"a refused change changed what the connection holds")
	}
}

// Emits.

// callEmit sends op with the arguments content names, on a connection holding every stream.
func callEmit(ctx context.Context, conn *options.Conn, op string, content map[string]any) (any, error) {
	expiry := options.ExpiryQuery{
		Market: spectest.Str(content["underlying"]), ExpiryDate: spectest.Str(content["expirationDate"]),
		PageSize: int(spectest.Num(content["pageSize"])), PageToken: spectest.Str(content["pageToken"]),
	}
	contract := options.ContractQuery{
		Contract: spectest.Str(content["market"]), PageSize: int(spectest.Num(content["pageSize"])),
		PageToken: spectest.Str(content["pageToken"]),
	}
	if contract.Contract == "ALL" {
		contract.Contract = ""
	}
	switch op {
	case "emitOptionsContracts":
		return conn.EmitContracts(ctx, expiry)
	case "emitOptionsQuotes":
		return conn.EmitQuotes(ctx, expiry)
	case "emitPublicOrderBook":
		return conn.EmitOrderBook(ctx, contract.Contract)
	case "emitPublicCompletedOrders":
		return conn.EmitPublicTrades(ctx, contract)
	case "emitOptionsPositions":
		return conn.EmitPositions(ctx, contract)
	case "emitOptionsOpenOrders":
		return conn.EmitOpenOrders(ctx, contract)
	case "emitOptionsRecentOrders":
		return conn.EmitRecentOrders(ctx, contract)
	case "emitOptionsSettlements":
		return conn.EmitSettlements(ctx, contract)
	case "emitOptionsAccountSummary":
		return conn.EmitAccountSummary(ctx)
	}
	return nil, fmt.Errorf("no way to send %s", op)
}

func checkEmitRequest(op string) func(*testing.T, spectest.Example) {
	return func(t *testing.T, ex spectest.Example) {
		content := exampleContent(t, ex)
		srv := fakeExchange(t)
		conn := streamingConn(t, srv)
		go func() { _, _ = callEmit(t.Context(), conn, op, content) }()

		sent := spectest.Decode(t, command(t, srv, op).Content)
		// Minted by the SDK, as every emit's is.
		delete(content, "clientRequestId")
		delete(sent, "clientRequestId")
		spectest.AssertSends(t, sent, content)
	}
}

// emitAnswers says, per emit, what its answer decodes into, which part of it is the snapshot,
// and where a caller finds that in the result.
var emitAnswers = map[string]struct {
	data    func() any
	key     string // empty for the whole data object
	result  func(any) any
	dropped []string
}{
	"emitOptionsContracts": {
		data: func() any { return new(options.ContractsData) }, result: func(r any) any { return r },
		dropped: []string{"statusCode", "clientRequestId"},
	},
	"emitOptionsQuotes": {
		data: func() any { return new(options.QuotesData) }, key: "quotes",
		result: func(r any) any { return r.(*options.Page[options.Quote]).Items },
	},
	"emitPublicOrderBook": {
		data: func() any { return new(options.OrderBookData) }, key: "orderBook", result: func(r any) any { return r },
	},
	"emitPublicCompletedOrders": {
		data: func() any { return new(options.PublicTradesData) }, key: "trades",
		result: func(r any) any { return r.(*options.Page[options.PublicTrade]).Items },
	},
	"emitOptionsPositions": {
		data: func() any { return new(options.PositionsData) }, key: "positions",
		result: func(r any) any { return r.(*options.Page[options.Position]).Items },
	},
	"emitOptionsOpenOrders": {
		data: func() any { return new(options.OpenOrdersData) }, key: "orders",
		result:  func(r any) any { return r.(*options.Page[options.OpenOrder]).Items },
		dropped: []string{"initiatorType", "leverage"},
	},
	"emitOptionsRecentOrders": {
		data: func() any { return new(options.RecentOrdersData) }, key: "orders",
		result:  func(r any) any { return r.(*options.Page[options.RecentOrder]).Items },
		dropped: []string{"leverage"},
	},
	"emitOptionsSettlements": {
		data: func() any { return new(options.SettlementsData) }, key: "settlements",
		result: func(r any) any { return r.(*options.Page[options.Settlement]).Items },
	},
	"emitOptionsAccountSummary": {
		data: func() any { return new(options.AccountSummaryData) }, key: "accountSummary", result: func(r any) any { return r },
	},
}

// checkEmitFlow answers an emit with the documented snapshot, which must come back as its
// result with every value in place.
func checkEmitFlow(op string) func(*testing.T, spectest.Example) {
	return func(t *testing.T, ex spectest.Example) {
		answer := emitAnswers[op]
		frame := spectest.FrameOf(t, ex)
		spectest.Strict(t, frame.Data, answer.data())

		srv := fakeExchange(t)
		conn := streamingConn(t, srv)
		args := map[string]any{"underlying": "BTC", "expirationDate": "2026-07-10", "market": testContract}
		got := async(func() (any, error) { return callEmit(t.Context(), conn, op, args) })
		crid := command(t, srv, op).ClientRequestID()
		srv.Push(spectest.WithClientRequestID(t, ex, crid))

		res := spectest.Receive(t, got)
		require.NoError(t, res.err)
		var snapshot any = spectest.Decode(t, frame.Data)
		if answer.key != "" {
			snapshot = snapshot.(map[string]any)[answer.key]
		}
		spectest.AssertCarried(t, snapshot, answer.result(res.value), answer.dropped...)
	}
}

// Stream deltas.

func checkContractUpdate(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	spectest.Strict(t, frame.Data, new(options.WireContractUpdate))
	event := spectest.EventAs[options.ContractUpdate](t, options.ToEvent, frame)
	spectest.AssertCarried(t, spectest.Decode(t, frame.Data), event)
}

func checkQuoteUpdate(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	spectest.Strict(t, frame.Data, new(options.WireQuote))
	event := spectest.EventAs[options.QuoteUpdate](t, options.ToEvent, frame)
	spectest.AssertCarried(t, spectest.Decode(t, frame.Data), event.Quote)
}

func checkOrderBookDelta(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	spectest.Strict(t, frame.Data, new(options.OrderBookData))
	event := spectest.EventAs[options.OrderBookDelta](t, options.ToEvent, frame)
	spectest.AssertCarried(t, spectest.Decode(t, frame.Data)["orderBook"], event)
}

func checkPublicTrades(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	spectest.Strict(t, frame.Data, new(options.PublicTradesData))
	event := spectest.EventAs[options.PublicTrades](t, options.ToEvent, frame)
	spectest.AssertCarried(t, spectest.Decode(t, frame.Data)["trades"], event.Trades)
	assert.Equal(t, frame.Market, event.Contract)
}

func checkPositionsDelta(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	spectest.Strict(t, frame.Data, new(options.PositionsData))
	event := spectest.EventAs[options.PositionsDelta](t, options.ToEvent, frame)
	spectest.AssertCarried(t, spectest.Decode(t, frame.Data)["positions"], event.Positions)
	assert.Equal(t, frame.Market, event.Contract)
}

func checkOpenOrdersDelta(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	spectest.Strict(t, frame.Data, new(options.OpenOrdersData))
	event := spectest.EventAs[options.OpenOrdersDelta](t, options.ToEvent, frame)
	orders := spectest.Decode(t, frame.Data)["orders"].([]any)
	require.Len(t, event.Changes, len(orders))
	for i, order := range orders {
		assert.Equal(t, options.OpenOrderKind(spectest.Str(order.(map[string]any)["kind"])), event.Changes[i].Kind)
		spectest.AssertCarried(t, order, event.Changes[i].Order, "kind", "initiatorType", "leverage")
	}
	assert.Equal(t, frame.Market, event.Contract)
}

func checkRecentOrdersDelta(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	spectest.Strict(t, frame.Data, new(options.RecentOrdersData))
	event := spectest.EventAs[options.RecentOrdersDelta](t, options.ToEvent, frame)
	spectest.AssertCarried(t, spectest.Decode(t, frame.Data)["orders"], event.Orders, "leverage")
	assert.Equal(t, frame.Market, event.Contract)
}
