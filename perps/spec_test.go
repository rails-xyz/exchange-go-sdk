package perps_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/perpstest"
	"github.com/rails-xyz/exchange-go-sdk/internal/spectest"
	"github.com/rails-xyz/exchange-go-sdk/perps"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The WebSocket types are hand-written, and a frame that fails to decode reaches the caller as
// Unknown, so nothing in production notices when the spec and the code drift apart. This test
// does: every example payload in api/perps-ws.yaml is driven through this release's types to
// the documented result, and a message with no check fails, so a new one forces a decision.

func TestSpecExamplesConform(t *testing.T) {
	examples := spectest.LoadExamples(t, "../api/perps-ws.yaml")
	require.NotEmpty(t, examples)

	checks := map[string]func(*testing.T, spectest.Example){
		"PingRequest":                 checkPingRequest,
		"PongResponse":                checkPong,
		"ConnectionShutdownNotice":    checkShutdown,
		"CreateOrderRequest":          checkCreateOrderRequest,
		"CreateOrderAck":              nil, // driven together with the confirmation below
		"CreateOrderConfirmation":     nil,
		"CreateOrderError":            checkCommandError("createOrder"),
		"CancelOrderRequest":          checkCancelOrderRequest,
		"CancelOrderAck":              nil,
		"CancelOrderConfirmation":     nil,
		"CancelOrderError":            checkCommandError("cancelOrder"),
		"CompleteFillNotification":    checkFill,
		"PartialFillNotification":     checkFill,
		"RejectOrderNotification":     checkReject,
		"SessionBackfillNotification": checkBackfill,

		"ModifyOrderRequest":       checkModifyOrderRequest,
		"ModifyOrderAck":           nil,
		"ModifyOrderConfirmation":  nil,
		"ModifyOrderError":         checkCommandError("modifyOrder"),
		"UpdateMarginRequest":      checkUpdateMarginRequest,
		"UpdateMarginAck":          nil,
		"UpdateMarginConfirmation": nil,
		"UpdateMarginError":        checkCommandError("updateMargin"),

		"SubscribeRequest":     checkSubscriptionRequest("subscribe"),
		"SubscribedResponse":   checkSubscriptionFlow("subscribe"),
		"SubscribeError":       checkCommandError("subscribe"),
		"UnsubscribeRequest":   checkSubscriptionRequest("unsubscribe"),
		"UnsubscribedResponse": checkSubscriptionFlow("unsubscribe"),
		"UnsubscribeError":     checkCommandError("unsubscribe"),

		"CompletedOrdersDelta":             checkTrades,
		"PublicCompletedOrdersDelta":       checkPublicTrades,
		"PublicOrderBookDelta":             checkOrderBookDelta,
		"EmitPublicCompletedOrdersRequest": checkEmitRequest("emitPublicCompletedOrders"),
		"EmitPublicOrderBookRequest":       checkEmitRequest("emitPublicOrderBook"),
		"PublicCompletedOrdersResponse":    checkEmitFlow("emitPublicCompletedOrders"),
		"PublicOrderBookResponse":          checkEmitFlow("emitPublicOrderBook"),
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
	// Optional fields appear in some examples and not others, so a command's keys are checked
	// against everything the spec shows for it.
	spectest.AssertTagsDocumented(t, perps.CreateOrderContent{}, spectest.DocumentedContent(t, examples["CreateOrderRequest"]))
	spectest.AssertTagsDocumented(t, perps.CancelOrderContent{}, spectest.DocumentedContent(t, examples["CancelOrderRequest"]))
	spectest.AssertTagsDocumented(t, perps.ModifyOrderContent{}, spectest.DocumentedContent(t, examples["ModifyOrderRequest"]))
	spectest.AssertTagsDocumented(t, perps.UpdateMarginContent{}, spectest.DocumentedContent(t, examples["UpdateMarginRequest"]))
	spectest.AssertTagsDocumented(t, perps.SubscriptionContent{}, spectest.DocumentedContent(t, examples["SubscribeRequest"]))
	spectest.AssertTagsDocumented(t, perps.EmitContent{}, spectest.DocumentedContent(t, examples["EmitPublicOrderBookRequest"]))

	t.Run("CreateOrder/AckThenConfirmation", func(t *testing.T) {
		checkCommandFlow(t, "createOrder", examples["CreateOrderAck"], examples["CreateOrderConfirmation"])
	})
	t.Run("CancelOrder/AckThenConfirmation", func(t *testing.T) {
		checkCommandFlow(t, "cancelOrder", examples["CancelOrderAck"], examples["CancelOrderConfirmation"])
	})
	t.Run("ModifyOrder/AckThenConfirmation", func(t *testing.T) {
		checkCommandFlow(t, "modifyOrder", examples["ModifyOrderAck"], examples["ModifyOrderConfirmation"])
	})
	t.Run("UpdateMargin", func(t *testing.T) {
		checkUpdateMarginFlow(t, examples["UpdateMarginAck"], examples["UpdateMarginConfirmation"])
	})
}

// Streams: decoded strictly and turned into the event a caller would see.

func checkTrades(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	var data []perps.WireTrade
	spectest.Strict(t, frame.Data, &data)

	event := spectest.EventAs[perps.Trades](t, perps.ToEvent, frame)
	assert.Equal(t, perps.TradesFromWire(data), event.Trades)
	assert.NotEmpty(t, event.Trades)
}

func checkPublicTrades(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	var data perps.PublicTradesData
	spectest.Strict(t, frame.Data, &data)

	event := spectest.EventAs[perps.PublicTrades](t, perps.ToEvent, frame)
	assert.Equal(t, frame.Market, event.Market)
	assert.Equal(t, perps.PublicTradesFromWire(data.Orders), event.Trades)
	assert.NotEmpty(t, event.Trades)
}

func checkOrderBookDelta(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	var data perps.OrderBookData
	spectest.Strict(t, frame.Data, &data)

	event := spectest.EventAs[perps.OrderBookDelta](t, perps.ToEvent, frame)
	assert.Equal(t, frame.Market, event.Market)
	assert.Equal(t, perps.LevelsFromWire(data.Orders), event.Orders)
	assert.Equal(t, data.Ts, event.Ts)
	assert.NotEmpty(t, event.Orders)
}

// Notifications: decoded strictly, so a field the spec has and this release does not is a
// failure, then turned into the event a caller would see.

func checkShutdown(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	spectest.Strict(t, frame.Data, new(perps.ShutdownData))
	data := spectest.Decode(t, frame.Data)

	event := spectest.EventAs[perps.ConnectionShutdown](t, perps.ToEvent, frame)
	assert.Equal(t, time.Duration(spectest.Num(data["forceCloseInMs"]))*time.Millisecond, event.ForceCloseIn)
	assert.Equal(t, time.UnixMilli(int64(spectest.Num(data["ts"]))), event.At)
}

func checkFill(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	var data perps.NotificationData
	spectest.Strict(t, frame.Data, &data)

	event := spectest.EventAs[perps.OrderFilled](t, perps.ToEvent, frame)
	assert.Equal(t, perps.OrderFromWire(data.Order, frame.Market), event.Order)
	assert.Equal(t, frame.ResultType == "partialFillOrder", event.Partial)
	assert.NotEmpty(t, event.Order.OrderID)
}

func checkReject(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	var data perps.NotificationData
	spectest.Strict(t, frame.Data, &data)

	event := spectest.EventAs[perps.OrderRejected](t, perps.ToEvent, frame)
	assert.Equal(t, perps.OrderFromWire(data.Order, frame.Market), event.Order)
	assert.Equal(t, data.Body, event.Body)
	assert.Equal(t, data.ClientRequestID, event.ClientRequestID)
	assert.NotEmpty(t, event.Body)
	if data.Error != nil {
		assert.Equal(t, data.Error.Slug, event.Slug)
		assert.Equal(t, data.Error.Code, event.Code)
		assert.Contains(t, knownCodes, event.Code, "the spec names a code this release has no constant for")
	}
}

func checkBackfill(t *testing.T, ex spectest.Example) {
	frame := spectest.FrameOf(t, ex)
	var data perps.BackfillWire
	spectest.Strict(t, frame.Data, &data)

	event := spectest.EventAs[perps.SessionBackfill](t, perps.ToEvent, frame)
	assert.Equal(t, perps.SessionBackfill(data), event)
	assert.NotEmpty(t, event.SessionID)
}

// Command answers: pushed by the fake exchange at a real connection, so the routing fields are
// read by the code that reads them in production.

func checkPong(t *testing.T, ex spectest.Example) {
	srv := perpstest.New(t)
	srv.SetAutoPong(false)
	conn := tradingConn(t, srv)

	pinged := make(chan error, 1)
	go func() {
		_, err := conn.Ping(t.Context())
		pinged <- err
	}()
	crid := command(t, srv, "ping").ClientRequestID()

	// The example answers its own ping; make it answer ours.
	frame := spectest.Decode(t, ex.Payload)
	frame["data"].(map[string]any)["clientRequestId"] = crid
	srv.Push(frame)

	require.NoError(t, spectest.Receive(t, pinged), "the documented pong did not end the ping")
}

func checkCommandFlow(t *testing.T, op string, acks, confirmations []spectest.Example) {
	require.Len(t, acks, 1)
	require.Len(t, confirmations, 1)
	ack, confirmation := spectest.FrameOf(t, acks[0]), spectest.FrameOf(t, confirmations[0])
	ackData, confirmationData := spectest.Decode(t, ack.Data), spectest.Decode(t, confirmation.Data)
	crid := spectest.Str(ackData["clientRequestId"])
	require.Equal(t, crid, spectest.Str(confirmationData["clientRequestId"]), "the two examples must describe one command")

	spectest.Strict(t, spectest.Raw(t, ackData["order"]), new(perps.WireOrder))
	var confirmed perps.WireOrder
	spectest.Strict(t, spectest.Raw(t, confirmationData["order"]), &confirmed)

	srv := perpstest.New(t)
	conn := tradingConn(t, srv)
	got := sendCommand(t, conn, op, crid)
	command(t, srv, op)
	srv.Push(json.RawMessage(acks[0].Payload))
	srv.Push(json.RawMessage(confirmations[0].Payload))

	result := spectest.Receive(t, got).value.(*perps.OrderResult)
	assert.Equal(t, crid, result.ClientRequestID)
	assert.EqualValues(t, spectest.Num(ackData["apiQuotaUsed"]), result.Ack.APIQuotaUsed)
	assert.False(t, result.Ack.At.IsZero(), "the acknowledgement went unrecorded")
	assert.Equal(t, perps.OrderFromWire(confirmed, confirmation.Market), result.Order)
}

// checkUpdateMarginFlow drives each confirmation: after the acknowledgement, or on its own
// when nothing had to change.
func checkUpdateMarginFlow(t *testing.T, acks, confirmations []spectest.Example) {
	require.Len(t, acks, 1)
	require.NotEmpty(t, confirmations)
	ackData := spectest.Decode(t, spectest.FrameOf(t, acks[0]).Data)
	crid := spectest.Str(ackData["clientRequestId"])

	for _, confirmation := range confirmations {
		t.Run(confirmation.Name, func(t *testing.T) {
			data := spectest.Decode(t, spectest.FrameOf(t, confirmation).Data)
			require.Equal(t, crid, spectest.Str(data["clientRequestId"]), "the examples must describe one command")
			acknowledged := confirmation.Name != "NoopSameMargin"

			srv := perpstest.New(t)
			conn := tradingConn(t, srv)
			got := sendCommand(t, conn, "updateMargin", crid)
			command(t, srv, "updateMargin")
			if acknowledged {
				srv.Push(json.RawMessage(acks[0].Payload))
			}
			srv.Push(json.RawMessage(confirmation.Payload))

			res := spectest.Receive(t, got)
			require.NoError(t, res.err)
			result := res.value.(*perps.MarginResult)
			assert.Equal(t, crid, result.ClientRequestID)
			assert.Equal(t, acknowledged, !result.Ack.At.IsZero())
			if acknowledged {
				assert.EqualValues(t, spectest.Num(ackData["apiQuotaUsed"]), result.Ack.APIQuotaUsed)
			}
		})
	}
}

// checkSubscriptionFlow answers a subscribe or unsubscribe with the documented frame and
// expects the documented active set back.
func checkSubscriptionFlow(op string) func(*testing.T, spectest.Example) {
	return func(t *testing.T, ex spectest.Example) {
		frame := spectest.FrameOf(t, ex)
		spectest.Strict(t, frame.Data, new(perps.SubscriptionData))
		data := spectest.Decode(t, frame.Data)

		srv := perpstest.New(t)
		conn := tradingConn(t, srv)
		got := sendCommand(t, conn, op, "")
		sent := command(t, srv, op).ClientRequestID()
		srv.Push(spectest.WithClientRequestID(t, ex, sent))

		res := spectest.Receive(t, got)
		require.NoError(t, res.err)
		assert.Equal(t, subscriptions(data["activeSubscriptions"]), res.value)
	}
}

// checkEmitFlow answers a snapshot request with the documented frame and expects the
// documented snapshot back.
func checkEmitFlow(op string) func(*testing.T, spectest.Example) {
	return func(t *testing.T, ex spectest.Example) {
		frame := spectest.FrameOf(t, ex)

		srv := perpstest.New(t)
		conn := tradingConn(t, srv)
		got := sendCommand(t, conn, op, "")
		sent := command(t, srv, op).ClientRequestID()
		srv.Push(spectest.WithClientRequestID(t, ex, sent))

		res := spectest.Receive(t, got)
		require.NoError(t, res.err)
		switch op {
		case "emitPublicCompletedOrders":
			var data perps.PublicTradesData
			spectest.Strict(t, frame.Data, &data)
			assert.Equal(t, perps.PublicTradesFromWire(data.Orders), res.value)
			assert.NotEmpty(t, data.Orders)
		case "emitPublicOrderBook":
			var data perps.OrderBookData
			spectest.Strict(t, frame.Data, &data)
			assert.Equal(t, &perps.OrderBookSnapshot{Orders: perps.LevelsFromWire(data.Orders), Ts: data.Ts}, res.value)
			assert.NotEmpty(t, data.Orders)
		}
	}
}

func subscriptions(v any) []perps.Subscription {
	var out []perps.Subscription
	for _, name := range v.([]any) {
		out = append(out, perps.Subscription(spectest.Str(name)))
	}
	return out
}

func checkCommandError(op string) func(*testing.T, spectest.Example) {
	return func(t *testing.T, ex spectest.Example) {
		frame := spectest.FrameOf(t, ex)
		data := spectest.Decode(t, frame.Data)
		crid := spectest.Str(data["clientRequestId"])
		if order, ok := data["order"]; ok {
			spectest.Strict(t, spectest.Raw(t, order), new(perps.WireOrder))
		}

		srv := perpstest.New(t)
		conn := tradingConn(t, srv)
		got := sendCommand(t, conn, op, crid)
		// Some commands mint their own id, so the example answers whatever went out.
		crid = command(t, srv, op).ClientRequestID()
		srv.Push(spectest.WithClientRequestID(t, ex, crid))

		var code rails.Code
		if detail, ok := data["error"].(map[string]any); ok {
			code = rails.Code(spectest.Str(detail["code"]))
		}
		switch code {
		case perps.CodeCancelOrderConflictError:
			// The SDK sends a cancel it could not apply again, twice, under fresh ids.
			for range 2 {
				retry := command(t, srv, op).ClientRequestID()
				assert.NotEqual(t, crid, retry)
				frame := spectest.Decode(t, ex.Payload)
				frame["data"].(map[string]any)["clientRequestId"] = retry
				srv.Push(frame)
				crid = retry
			}
		case perps.CodeCancelOrderAlreadyCancelling:
			// The SDK waits for the other cancel's answer instead of failing.
			other := spectest.Decode(t, ex.Payload)
			otherData := other["data"].(map[string]any)
			otherData["statusCode"], otherData["body"], otherData["clientRequestId"] = 200, "order cancelled", "other-cancel"
			delete(otherData, "error")
			otherData["order"] = map[string]any{"orderId": "01HH0X2VD6R0CJE4X0P0D1NQ11", "orderStatus": "cancelled"}
			srv.Push(other)
			result := spectest.Receive(t, got)
			require.NoError(t, result.err)
			assert.Equal(t, perps.OrderStatusCancelled, result.value.(*perps.OrderResult).Order.OrderStatus)
			return
		}

		var apiErr *rails.Error
		require.ErrorAs(t, spectest.Receive(t, got).err, &apiErr)
		want := rails.Error{
			Op:              op,
			StatusCode:      int(spectest.Num(data["statusCode"])),
			Message:         spectest.Str(data["body"]),
			RetryAfter:      time.Duration(spectest.Num(data["retryAfterSec"])) * time.Second,
			APIQuotaUsed:    int64(spectest.Num(data["apiQuotaUsed"])),
			ClientRequestID: crid,
		}
		if detail, ok := data["error"].(map[string]any); ok {
			want.Slug, want.Code = spectest.Str(detail["slug"]), rails.Code(spectest.Str(detail["code"]))
			assert.Contains(t, knownCodes, want.Code, "the spec names a code this release has no constant for")
		}
		assert.Equal(t, want, *apiErr)
	}
}

// Requests: built from the example's content and sent for real, so what goes on the wire is
// compared with what the spec says goes on the wire.

func checkPingRequest(t *testing.T, ex spectest.Example) {
	srv := perpstest.New(t)
	srv.SetAutoPong(false)
	conn := tradingConn(t, srv)
	go func() { _, _ = conn.Ping(t.Context()) }()

	sent := spectest.Decode(t, command(t, srv, "ping").Content)
	assert.Equal(t, spectest.Keys(spectest.Decode(t, spectest.Raw(t, spectest.Decode(t, ex.Payload)["content"]))), spectest.Keys(sent))
}

func checkCreateOrderRequest(t *testing.T, ex spectest.Example) {
	content := spectest.Decode(t, spectest.Raw(t, spectest.Decode(t, ex.Payload)["content"]))

	srv := perpstest.New(t)
	conn := tradingConn(t, srv)
	go func() {
		_, _ = conn.CreateOrder(t.Context(), perps.CreateOrderRequest{
			Market:          spectest.Str(content["market"]),
			Price:           spectest.Str(content["price"]),
			Quantity:        spectest.Str(content["quantity"]),
			OrderType:       perps.OrderType(spectest.Str(content["orderType"])),
			TradeType:       perps.TradeType(spectest.Str(content["tradeType"])),
			PostOnly:        content["postOnly"] == true,
			MaxSlippage:     spectest.Str(content["maxSlippage"]),
			ClientRequestID: spectest.Str(content["clientRequestId"]),
		})
	}()
	spectest.AssertSends(t, spectest.Decode(t, command(t, srv, "createOrder").Content), content)
}

func checkCancelOrderRequest(t *testing.T, ex spectest.Example) {
	content := spectest.Decode(t, spectest.Raw(t, spectest.Decode(t, ex.Payload)["content"]))

	srv := perpstest.New(t)
	conn := tradingConn(t, srv)
	go func() {
		_, _ = conn.CancelOrder(t.Context(), perps.CancelOrderRequest{
			Market:          spectest.Str(content["market"]),
			OrderID:         spectest.Str(content["orderId"]),
			ClientRequestID: spectest.Str(content["clientRequestId"]),
		})
	}()
	spectest.AssertSends(t, spectest.Decode(t, command(t, srv, "cancelOrder").Content), content)
}

func checkModifyOrderRequest(t *testing.T, ex spectest.Example) {
	content := spectest.Decode(t, spectest.Raw(t, spectest.Decode(t, ex.Payload)["content"]))

	srv := perpstest.New(t)
	conn := tradingConn(t, srv)
	go func() {
		_, _ = conn.ModifyOrder(t.Context(), perps.ModifyOrderRequest{
			Market:          spectest.Str(content["market"]),
			OrderID:         spectest.Str(content["orderId"]),
			Price:           spectest.Str(content["price"]),
			Quantity:        spectest.Str(content["quantity"]),
			ClientRequestID: spectest.Str(content["clientRequestId"]),
		})
	}()
	spectest.AssertSends(t, spectest.Decode(t, command(t, srv, "modifyOrder").Content), content)
}

func checkUpdateMarginRequest(t *testing.T, ex spectest.Example) {
	content := spectest.Decode(t, spectest.Raw(t, spectest.Decode(t, ex.Payload)["content"]))

	srv := perpstest.New(t)
	conn := tradingConn(t, srv)
	go func() {
		_, _ = conn.UpdateMargin(t.Context(), perps.UpdateMarginRequest{
			Market:          spectest.Str(content["market"]),
			Margin:          spectest.Str(content["margin"]),
			ClientRequestID: spectest.Str(content["clientRequestId"]),
		})
	}()
	spectest.AssertSends(t, spectest.Decode(t, command(t, srv, "updateMargin").Content), content)
}

// The commands below mint their own clientRequestId, so the documented one is not compared.

func checkSubscriptionRequest(op string) func(*testing.T, spectest.Example) {
	return func(t *testing.T, ex spectest.Example) {
		content := spectest.Decode(t, spectest.Raw(t, spectest.Decode(t, ex.Payload)["content"]))

		srv := perpstest.New(t)
		conn := tradingConn(t, srv)
		go func() {
			subs := subscriptions(content["subscriptions"])
			if op == "subscribe" {
				_, _ = conn.Subscribe(t.Context(), subs...)
			} else {
				_, _ = conn.Unsubscribe(t.Context(), subs...)
			}
		}()
		sent := spectest.Decode(t, command(t, srv, op).Content)
		content["clientRequestId"] = sent["clientRequestId"]
		spectest.AssertSends(t, sent, content)
	}
}

func checkEmitRequest(op string) func(*testing.T, spectest.Example) {
	return func(t *testing.T, ex spectest.Example) {
		content := spectest.Decode(t, spectest.Raw(t, spectest.Decode(t, ex.Payload)["content"]))

		srv := perpstest.New(t)
		conn := tradingConn(t, srv)
		go func() {
			if op == "emitPublicCompletedOrders" {
				_, _ = conn.EmitPublicTrades(t.Context(), spectest.Str(content["market"]))
			} else {
				_, _ = conn.EmitPublicOrderBook(t.Context(), spectest.Str(content["market"]))
			}
		}()
		sent := spectest.Decode(t, command(t, srv, op).Content)
		content["clientRequestId"] = sent["clientRequestId"]
		spectest.AssertSends(t, sent, content)
	}
}

// commandResult is a command's return value and error.
type commandResult struct {
	value any
	err   error
}

// sendCommand issues op, under crid where the command takes one, and returns where its result
// will arrive.
func sendCommand(t *testing.T, conn *perps.Conn, op, crid string) <-chan commandResult {
	t.Helper()
	got := make(chan commandResult, 1)
	go func() {
		var res commandResult
		switch op {
		case "createOrder":
			req := limitOrder()
			req.ClientRequestID = crid
			res.value, res.err = conn.CreateOrder(t.Context(), req)
		case "cancelOrder":
			res.value, res.err = conn.CancelOrder(t.Context(),
				perps.CancelOrderRequest{OrderID: "01HH0X2VD6R0CJE4X0P0D1NQ11", ClientRequestID: crid})
		case "modifyOrder":
			res.value, res.err = conn.ModifyOrder(t.Context(), perps.ModifyOrderRequest{
				OrderID: "01KHPJAA0QBAJEXQJ941F7SWW7", Price: "1999", Quantity: "1", ClientRequestID: crid,
			})
		case "updateMargin":
			res.value, res.err = conn.UpdateMargin(t.Context(), perps.UpdateMarginRequest{Margin: "201", ClientRequestID: crid})
		case "subscribe":
			res.value, res.err = conn.Subscribe(t.Context(), perps.SubscriptionTrades)
		case "unsubscribe":
			res.value, res.err = conn.Unsubscribe(t.Context(), perps.SubscriptionTrades)
		case "emitPublicCompletedOrders":
			res.value, res.err = conn.EmitPublicTrades(t.Context(), "")
		case "emitPublicOrderBook":
			res.value, res.err = conn.EmitPublicOrderBook(t.Context(), "")
		default:
			t.Errorf("no way to send %s", op)
		}
		got <- res
	}()
	return got
}
