package perps_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rails-xyz/exchange-go-sdk/internal/perpstest"
	"github.com/rails-xyz/exchange-go-sdk/internal/spectest"
	"github.com/rails-xyz/exchange-go-sdk/internal/telemetry"
	"github.com/rails-xyz/exchange-go-sdk/perps"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const market = "BTC-USDT"

// tradingConn opens a connection with the keepalive off, so the only frames on the wire are
// the test's.
func tradingConn(t *testing.T, srv *perpstest.Server, opts ...rails.Option) *perps.Conn {
	t.Helper()
	conn, err := dial(t, srv, opts...).Connect(t.Context(), market, perps.WithPingInterval(0))
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return conn
}

// command waits for the next command a connection sends and checks which one it is.
func command(t *testing.T, srv *perpstest.Server, want string) perpstest.Message {
	t.Helper()
	msg := srv.NextMessage(settle)
	require.Equal(t, want, msg.Message)
	// Anything but the dashed 36-character form is rejected.
	require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
		msg.ClientRequestID(), "clientRequestId is not a canonical UUID v4")
	return msg
}

// envelope is the frame shape every command's answer arrives in.
func envelope(resultType string, data map[string]any) map[string]any {
	return map[string]any{"resultType": resultType, "market": market, "data": data}
}

// orderObject is the order as the exchange reports it inside a frame.
func orderObject(crid, orderID, status string) map[string]any {
	return map[string]any{
		"market": market, "orderId": orderID, "orderType": "buy", "tradeType": "limit",
		"price": "50000", "quantity": "1", "filledQuantity": "0", "leverage": "1",
		"createdAt": 1735930753267, "updatedAt": 1735930753267,
		"orderStatus": status, "clientRequestId": crid, "reduceOnly": false, "postOnly": true,
	}
}

func limitOrder() perps.CreateOrderRequest {
	return perps.CreateOrderRequest{
		Price: "50000", Quantity: "1",
		OrderType: perps.OrderTypeBuy, TradeType: perps.TradeTypeLimit, PostOnly: true,
	}
}

// The whole trace of a successful order: what goes out, the acknowledgement, and the frame
// that decides it.
func TestCreateOrderWaitsForTheAnswerAfterTheAcknowledgement(t *testing.T) {
	srv := perpstest.New(t)
	conn := tradingConn(t, srv)

	result := make(chan *perps.OrderResult, 1)
	go func() {
		got, err := conn.CreateOrder(t.Context(), limitOrder())
		assert.NoError(t, err)
		result <- got
	}()

	sent := command(t, srv, "createOrder")
	crid := sent.ClientRequestID()
	t.Logf("→ %s", mustJSON(t, sent))

	ack := envelope("createOrder", map[string]any{
		"statusCode": 202, "body": "order created, pending acceptance",
		"clientRequestId": crid, "apiQuotaUsed": 1,
		"order": orderObject(crid, "01JGPRJC7KHY8GP4FT6WRQG193", "pending"),
	})
	srv.Push(ack)
	t.Logf("← %s", mustJSON(t, ack))

	time.Sleep(20 * time.Millisecond) // a real gap between the two

	confirmation := envelope("createOrder", map[string]any{
		"statusCode": 200, "body": "order accepted", "clientRequestId": crid,
		"order": orderObject(crid, "01JGPRJC7KHY8GP4FT6WRQG193", "active"),
	})
	srv.Push(confirmation)
	t.Logf("← %s", mustJSON(t, confirmation))

	got := <-result
	t.Logf("CreateOrder → %+v", *got)

	// The whole content, so the keys left off are proved absent too.
	assert.Equal(t, map[string]any{
		"clientRequestId": crid,
		"price":           "50000",
		"quantity":        "1",
		"orderType":       "buy",
		"tradeType":       "limit",
		"postOnly":        true,
	}, spectest.Decode(t, sent.Content), "the order on the wire is not the one asked for")

	assert.Equal(t, crid, got.ClientRequestID)
	assert.Equal(t, perps.OrderStatusActive, got.Order.OrderStatus)
	assert.Equal(t, "01JGPRJC7KHY8GP4FT6WRQG193", got.Order.OrderID)
	assert.EqualValues(t, 1, got.Ack.APIQuotaUsed, "the quota figure only ever rides the 202")
	assert.False(t, got.Ack.At.IsZero(), "the acknowledgement went unrecorded")
	assert.Greater(t, got.TerminalAt.Sub(got.Ack.At), time.Duration(0),
		"the answer was not timed separately from the acknowledgement")
}

// An acknowledgement is not acceptance: the order can still be rejected seconds later, and
// both moments are reported as they happen.
func TestCreateOrderRejectedAfterItsAcknowledgement(t *testing.T) {
	srv := perpstest.New(t)
	hooks := newOrderHooks()
	conn := tradingConn(t, srv, rails.WithHooks(hooks.hooks()))

	failed := make(chan error, 1)
	go func() {
		_, err := conn.CreateOrder(t.Context(), limitOrder())
		failed <- err
	}()

	crid := command(t, srv, "createOrder").ClientRequestID()
	srv.Push(envelope("createOrder", map[string]any{
		"statusCode": 202, "body": "order created, pending acceptance",
		"clientRequestId": crid, "apiQuotaUsed": 1,
		"order": orderObject(crid, "01JGPRJC7KHY8GP4FT6WRQG193", "pending"),
	}))
	srv.Push(envelope("createOrder", map[string]any{
		"statusCode": 400, "body": "receive window exceeded, order rejected",
		"error":           map[string]string{"slug": "CREATE_ORDER_ERROR", "code": "PRP-0100"},
		"clientRequestId": crid, "apiQuotaUsed": 1,
		"order": orderObject(crid, "01JGPRJC7KHY8GP4FT6WRQG193", "rejected"),
	}))

	var apiErr *rails.Error
	require.ErrorAs(t, spectest.Receive(t, failed), &apiErr)
	assert.Equal(t, 400, apiErr.StatusCode)
	assert.Equal(t, "receive window exceeded, order rejected", apiErr.Message)
	assert.Equal(t, "CREATE_ORDER_ERROR", apiErr.Slug)
	assert.Equal(t, perps.CodeCreateOrderError, apiErr.Code)
	assert.Equal(t, crid, apiErr.ClientRequestID)
	assert.Equal(t, "createOrder", apiErr.Op)

	// One connection id across every hook is what ties a request to its connection.
	want := rails.ReqInfo{
		ConnID: spectest.Receive(t, hooks.connected).ConnID, Op: "createOrder",
		Market: market, ClientRequestID: crid,
	}
	assert.Equal(t, want, spectest.Receive(t, hooks.acked), "RequestAcked")
	done := spectest.Receive(t, hooks.done)
	assert.Equal(t, want, done.info, "RequestDone")
	assert.Equal(t, apiErr, done.err, "the hook and the caller must see the same failure")
}

func TestCancelOrderNamesTheOrderAndTheCancellationMode(t *testing.T) {
	srv := perpstest.New(t)
	conn := tradingConn(t, srv)

	const orderID = "01JGPRJC7KHY8GP4FT6WRQG193"
	result := make(chan *perps.OrderResult, 1)
	go func() {
		got, err := conn.CancelOrder(t.Context(), perps.CancelOrderRequest{OrderID: orderID})
		assert.NoError(t, err)
		result <- got
	}()

	sent := command(t, srv, "cancelOrder")
	crid := sent.ClientRequestID()
	assert.Equal(t, map[string]any{
		"clientRequestId": crid,
		"orderId":         orderID,
		"cancelBy":        "orderId",
	}, spectest.Decode(t, sent.Content))

	srv.Push(envelope("cancelOrder", map[string]any{
		"statusCode": 202, "body": "cancelling order", "clientRequestId": crid, "apiQuotaUsed": 2,
		"order": orderObject(crid, orderID, "cancelling"),
	}))
	srv.Push(envelope("cancelOrder", map[string]any{
		"statusCode": 200, "body": "order cancelled", "clientRequestId": crid,
		"order": orderObject(crid, orderID, "cancelled"),
	}))

	got := <-result
	assert.Equal(t, perps.OrderStatusCancelled, got.Order.OrderStatus)
	assert.EqualValues(t, 2, got.Ack.APIQuotaUsed)
}

func cancelAnswer(crid, orderID string, status int, body, slug string, code rails.Code) map[string]any {
	data := map[string]any{
		"statusCode": status, "body": body, "clientRequestId": crid, "apiQuotaUsed": 1,
		"order": orderObject(crid, orderID, "cancelling"),
	}
	if code != "" {
		data["error"] = map[string]string{"slug": slug, "code": string(code)}
	}
	return envelope("cancelOrder", data)
}

// A cancel the exchange could not apply is sent again under a fresh id, twice at most.
func TestCancelOrderResendsACancelTheExchangeCouldNotApply(t *testing.T) {
	srv := perpstest.New(t)
	conn := tradingConn(t, srv)
	const orderID = "01JGPRJC7KHY8GP4FT6WRQG193"
	const first = "6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e5f"

	failed := make(chan error, 1)
	go func() {
		_, err := conn.CancelOrder(t.Context(), perps.CancelOrderRequest{OrderID: orderID, ClientRequestID: first})
		failed <- err
	}()

	seen := map[string]bool{}
	for range 3 {
		sent := command(t, srv, "cancelOrder")
		crid := sent.ClientRequestID()
		assert.False(t, seen[crid], "a retry reused clientRequestId %s", crid)
		seen[crid] = true
		assert.Equal(t, orderID, spectest.Decode(t, sent.Content)["orderId"])
		srv.Push(cancelAnswer(crid, orderID, 409, "order cannot be cancelled at the moment, please try again",
			"CANCEL_ORDER_CONFLICT_ERROR", perps.CodeCancelOrderConflictError))
	}
	assert.True(t, seen[first], "the caller's own id goes out first")

	var apiErr *rails.Error
	require.ErrorAs(t, spectest.Receive(t, failed), &apiErr)
	assert.Equal(t, perps.CodeCancelOrderConflictError, apiErr.Code)
	assert.Len(t, srv.Received(), 3, "nothing else was sent")
}

// With another cancel of the order in flight, the call returns that cancel's answer.
func TestCancelOrderWaitsForTheCancelAlreadyInFlight(t *testing.T) {
	const orderID = "01JGPRJC7KHY8GP4FT6WRQG193"
	start := func(t *testing.T) (*perpstest.Server, *perps.Conn, chan commandResult, string) {
		srv := perpstest.New(t)
		conn := tradingConn(t, srv)
		got := make(chan commandResult, 1)
		go func() {
			order, err := conn.CancelOrder(t.Context(), perps.CancelOrderRequest{OrderID: orderID})
			got <- commandResult{value: order, err: err}
		}()
		crid := command(t, srv, "cancelOrder").ClientRequestID()
		srv.Push(envelope("cancelOrder", map[string]any{
			"statusCode": 202, "body": "cancelling order", "clientRequestId": crid, "apiQuotaUsed": 7,
			"order": orderObject(crid, orderID, "cancelling"),
		}))
		srv.Push(cancelAnswer(crid, orderID, 409, "order is already being cancelled",
			"CANCEL_ORDER_ALREADY_CANCELLING", perps.CodeCancelOrderAlreadyCancelling))
		return srv, conn, got, crid
	}

	t.Run("the other cancel succeeds", func(t *testing.T) {
		srv, conn, got, crid := start(t)
		// Another order's cancel and this order's own acknowledgement are not the answer.
		srv.Push(cancelAnswer("someone-else", "01JOTHERORDER000000000000", 200, "order cancelled", "", ""))
		srv.Push(envelope("cancelOrder", map[string]any{
			"statusCode": 202, "body": "cancelling order", "clientRequestId": "other-cancel",
			"order": orderObject("other-cancel", orderID, "cancelling"),
		}))
		srv.Push(envelope("cancelOrder", map[string]any{
			"statusCode": 200, "body": "order cancelled", "clientRequestId": "other-cancel",
			"order": orderObject("other-cancel", orderID, "cancelled"),
		}))

		result := spectest.Receive(t, got)
		require.NoError(t, result.err)
		order := result.value.(*perps.OrderResult)
		assert.Equal(t, crid, order.ClientRequestID)
		assert.Equal(t, perps.OrderStatusCancelled, order.Order.OrderStatus)
		assert.Equal(t, orderID, order.Order.OrderID)
		assert.EqualValues(t, 7, order.Ack.APIQuotaUsed, "the caller's own acknowledgement is kept")
		unknown := waitEvent[perps.Unknown](t, conn.Events())
		assert.Contains(t, string(unknown.Raw), "someone-else", "the unrelated answer still reaches the caller")
	})

	t.Run("the other cancel fails", func(t *testing.T) {
		srv, _, got, _ := start(t)
		srv.Push(cancelAnswer("other-cancel", orderID, 409, "invalid order status 'filled' for cancel",
			"CANCEL_ORDER_NOT_CANCELLABLE", perps.CodeCancelOrderNotCancellable))

		var apiErr *rails.Error
		require.ErrorAs(t, spectest.Receive(t, got).err, &apiErr)
		assert.Equal(t, perps.CodeCancelOrderNotCancellable, apiErr.Code)
		assert.Equal(t, "other-cancel", apiErr.ClientRequestID)
		assert.Equal(t, "cancelOrder", apiErr.Op)
	})

	t.Run("the connection closes while waiting", func(t *testing.T) {
		_, conn, got, _ := start(t)
		conn.Close()
		assert.ErrorIs(t, spectest.Receive(t, got).err, rails.ErrConnClosed)
	})
}

// Several callers cancelling one order on one connection all get the cancelled order.
func TestConcurrentCancelsOfOneOrderAllResolve(t *testing.T) {
	srv := perpstest.New(t)
	conn := tradingConn(t, srv)
	const orderID = "01JGPRJC7KHY8GP4FT6WRQG193"

	got := make(chan commandResult, 3)
	for range 3 {
		go func() {
			order, err := conn.CancelOrder(t.Context(), perps.CancelOrderRequest{OrderID: orderID})
			got <- commandResult{value: order, err: err}
		}()
	}
	var crids []string
	for range 3 {
		crids = append(crids, command(t, srv, "cancelOrder").ClientRequestID())
	}
	for _, crid := range crids {
		srv.Push(envelope("cancelOrder", map[string]any{
			"statusCode": 202, "body": "cancelling order", "clientRequestId": crid, "apiQuotaUsed": 1,
			"order": orderObject(crid, orderID, "cancelling"),
		}))
	}
	for _, crid := range crids[1:] {
		srv.Push(cancelAnswer(crid, orderID, 409, "order is already being cancelled",
			"CANCEL_ORDER_ALREADY_CANCELLING", perps.CodeCancelOrderAlreadyCancelling))
	}
	srv.Push(envelope("cancelOrder", map[string]any{
		"statusCode": 200, "body": "order cancelled", "clientRequestId": crids[0],
		"order": orderObject(crids[0], orderID, "cancelled"),
	}))

	var returned []string
	for range 3 {
		result := spectest.Receive(t, got)
		require.NoError(t, result.err)
		order := result.value.(*perps.OrderResult)
		assert.Equal(t, perps.OrderStatusCancelled, order.Order.OrderStatus)
		returned = append(returned, order.ClientRequestID)
	}
	assert.ElementsMatch(t, crids, returned, "each caller gets its own id back")
}

// A rejection arrives in several shapes, and each has to reach the caller as the same
// *rails.Error with the wording, the pair and the wait in the right fields.
func TestRejectionShapesMapToErrors(t *testing.T) {
	cases := []struct {
		name string
		data map[string]any
		want rails.Error
	}{
		{
			name: "cancelling an order that is gone",
			data: map[string]any{
				"statusCode": 404, "body": "order not found", "apiQuotaUsed": 1,
				"error": map[string]string{"slug": "NOT_FOUND", "code": "SYS-0007"},
			},
			want: rails.Error{StatusCode: 404, Message: "order not found",
				Slug: "NOT_FOUND", Code: "SYS-0007", APIQuotaUsed: 1},
		},
		{
			name: "cancelling an order already being cancelled",
			data: map[string]any{
				"statusCode": 409, "body": "order is already being cancelled", "apiQuotaUsed": 1,
				"error": map[string]string{"slug": "CANCEL_ORDER_CONFLICT_ERROR", "code": "PRP-0201"},
			},
			want: rails.Error{StatusCode: 409, Message: "order is already being cancelled",
				Slug: "CANCEL_ORDER_CONFLICT_ERROR", Code: "PRP-0201", APIQuotaUsed: 1},
		},
		{
			// Refused by the gateway before the exchange saw it, so no quota was spent.
			name: "a clientRequestId used twice",
			data: map[string]any{
				"statusCode": 409, "body": "duplicate clientRequestId, request already accepted",
				"error": map[string]string{"slug": "DUPLICATE_REQUEST", "code": "SYS-0010"},
			},
			want: rails.Error{StatusCode: 409, Message: "duplicate clientRequestId, request already accepted",
				Slug: "DUPLICATE_REQUEST", Code: rails.CodeDuplicateRequest},
		},
		{
			name: "over the API call rate limit",
			data: map[string]any{
				"statusCode": 429, "body": "api call rate limit exceeded", "retryAfterSec": 5,
				"error": map[string]string{"slug": "TOO_MANY_REQUESTS", "code": "SYS-0003"}, "apiQuotaUsed": 10001,
			},
			want: rails.Error{StatusCode: 429, Message: "api call rate limit exceeded",
				Slug: "TOO_MANY_REQUESTS", Code: "SYS-0003", RetryAfter: 5 * time.Second, APIQuotaUsed: 10001},
		},
		{
			name: "barred from creating orders for a while",
			data: map[string]any{
				"statusCode": 403, "retryAfterSec": 300, "apiQuotaUsed": 1,
				"body":  "user is banned from creating orders due to max open order limit, try again after 300 seconds",
				"error": map[string]string{"slug": "FORBIDDEN", "code": "SYS-0004"},
			},
			want: rails.Error{StatusCode: 403, Slug: "FORBIDDEN", Code: "SYS-0004", RetryAfter: 300 * time.Second,
				Message:      "user is banned from creating orders due to max open order limit, try again after 300 seconds",
				APIQuotaUsed: 1},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := perpstest.New(t)
			conn := tradingConn(t, srv)

			failed := make(chan error, 1)
			go func() {
				_, err := conn.CreateOrder(t.Context(), limitOrder())
				failed <- err
			}()

			crid := command(t, srv, "createOrder").ClientRequestID()
			tc.data["clientRequestId"] = crid
			srv.Push(envelope("createOrder", tc.data))

			var apiErr *rails.Error
			require.ErrorAs(t, spectest.Receive(t, failed), &apiErr)
			want := tc.want
			want.Op, want.ClientRequestID = "createOrder", crid
			assert.Equal(t, want, *apiErr)
		})
	}
}

// A frame refused before it reached a command names no request, so it is an event.
func TestBareErrorFramesBecomeEvents(t *testing.T) {
	cases := []struct {
		name        string
		frame       map[string]any
		want        rails.Error
		rateLimited bool
	}{
		{
			name: "the socket's message limiter",
			frame: map[string]any{
				"statusCode": 429, "body": "Too many messages",
				"error": map[string]string{"slug": "TOO_MANY_REQUESTS", "code": "SYS-0003"}, "retryAfterSec": 5,
			},
			want: rails.Error{StatusCode: 429, Message: "Too many messages", Slug: "TOO_MANY_REQUESTS",
				Code: rails.CodeTooManyRequests, RetryAfter: 5 * time.Second},
			rateLimited: true,
		},
		{
			name: "a message the connection could not read",
			frame: map[string]any{
				"statusCode": 400, "body": "Invalid JSON payload",
				"error": map[string]string{"slug": "BAD_REQUEST", "code": "SYS-0006"},
			},
			want: rails.Error{StatusCode: 400, Message: "Invalid JSON payload", Slug: "BAD_REQUEST",
				Code: rails.CodeBadRequest},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := perpstest.New(t)
			conn := tradingConn(t, srv)
			srv.Push(tc.frame)

			event := waitEvent[perps.ServerError](t, conn.Events())
			assert.Equal(t, tc.want, *event.Err)
			assert.Equal(t, tc.rateLimited, errors.Is(event.Err, rails.ErrRateLimited))
		})
	}
}

// A bare rate limit names no request. With exactly one command sent and unacknowledged, that
// is the one dropped and its caller may send again; otherwise nothing can be attributed.
func TestBareRateLimitAttribution(t *testing.T) {
	bare := map[string]any{
		"statusCode": 429, "body": "Too many messages",
		"error": map[string]string{"slug": "TOO_MANY_REQUESTS", "code": "SYS-0003"}, "retryAfterSec": 2,
	}
	answer := func(srv *perpstest.Server, crid string) {
		srv.Push(envelope("createOrder", map[string]any{
			"statusCode": 202, "body": "order created, pending acceptance", "clientRequestId": crid, "apiQuotaUsed": 1,
			"order": orderObject(crid, "01JGPRJC7KHY8GP4FT6WRQG193", "pending"),
		}))
		srv.Push(envelope("createOrder", map[string]any{
			"statusCode": 200, "body": "order created", "clientRequestId": crid, "apiQuotaUsed": 1,
			"order": orderObject(crid, "01JGPRJC7KHY8GP4FT6WRQG193", "active"),
		}))
	}

	t.Run("one unacknowledged command is the dropped one", func(t *testing.T) {
		srv := perpstest.New(t)
		conn := tradingConn(t, srv)
		failed := make(chan error, 1)
		go func() {
			_, err := conn.CreateOrder(t.Context(), limitOrder())
			failed <- err
		}()
		crid := command(t, srv, "createOrder").ClientRequestID()
		srv.Push(bare)

		err := spectest.Receive(t, failed)
		assert.ErrorIs(t, err, rails.ErrNotSent)
		assert.ErrorIs(t, err, rails.ErrRateLimited)
		var apiErr *rails.Error
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, crid, apiErr.ClientRequestID)
		assert.Equal(t, "createOrder", apiErr.Op)
		assert.Equal(t, rails.CodeTooManyRequests, apiErr.Code)
		assert.Equal(t, 2*time.Second, apiErr.RetryAfter)
		waitEvent[perps.ServerError](t, conn.Events())
	})

	t.Run("an acknowledged command was not the dropped one", func(t *testing.T) {
		srv := perpstest.New(t)
		conn := tradingConn(t, srv)
		done := make(chan error, 1)
		go func() {
			_, err := conn.CreateOrder(t.Context(), limitOrder())
			done <- err
		}()
		crid := command(t, srv, "createOrder").ClientRequestID()
		srv.Push(envelope("createOrder", map[string]any{
			"statusCode": 202, "body": "order created, pending acceptance", "clientRequestId": crid, "apiQuotaUsed": 1,
			"order": orderObject(crid, "01JGPRJC7KHY8GP4FT6WRQG193", "pending"),
		}))
		srv.Push(bare)
		waitEvent[perps.ServerError](t, conn.Events())
		srv.Push(envelope("createOrder", map[string]any{
			"statusCode": 200, "body": "order created", "clientRequestId": crid, "apiQuotaUsed": 1,
			"order": orderObject(crid, "01JGPRJC7KHY8GP4FT6WRQG193", "active"),
		}))
		assert.NoError(t, spectest.Receive(t, done))
	})

	t.Run("two unacknowledged commands cannot be told apart", func(t *testing.T) {
		srv := perpstest.New(t)
		conn := tradingConn(t, srv)
		done := make(chan error, 2)
		for range 2 {
			go func() {
				_, err := conn.CreateOrder(t.Context(), limitOrder())
				done <- err
			}()
		}
		first := command(t, srv, "createOrder").ClientRequestID()
		second := command(t, srv, "createOrder").ClientRequestID()
		srv.Push(bare)
		waitEvent[perps.ServerError](t, conn.Events())
		answer(srv, first)
		answer(srv, second)
		assert.NoError(t, spectest.Receive(t, done))
		assert.NoError(t, spectest.Receive(t, done))
	})
}

// orderHooks records the observations an order produces.
type orderHooks struct {
	connected chan rails.ConnInfo
	acked     chan rails.ReqInfo
	done      chan doneCall
}

type doneCall struct {
	info rails.ReqInfo
	err  error
}

func newOrderHooks() *orderHooks {
	return &orderHooks{
		connected: make(chan rails.ConnInfo, 4),
		acked:     make(chan rails.ReqInfo, 4),
		done:      make(chan doneCall, 4),
	}
}

func (h *orderHooks) hooks() rails.Hooks {
	return rails.Hooks{
		ConnectFinished: func(info rails.ConnInfo, _ rails.ConnectStats, _ error) { h.connected <- info },
		RequestAcked:    func(info rails.ReqInfo, _ time.Duration) { h.acked <- info },
		RequestDone: func(info rails.ReqInfo, _ time.Duration, err error) {
			h.done <- doneCall{info: info, err: err}
		},
	}
}

// An order that goes unanswered leaves the caller only the id, so the minted one has to come
// back with the error.
func TestTimedOutOrderReportsTheClientRequestIDItMinted(t *testing.T) {
	srv := perpstest.New(t)
	conn := tradingConn(t, srv)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := conn.CreateOrder(ctx, limitOrder())

	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	assert.ErrorIs(t, err, rails.ErrTimeout)
	assert.Equal(t, srv.Received()[0].ClientRequestID(), apiErr.ClientRequestID,
		"the caller cannot reconcile the order without the id it went out under")
	assert.Equal(t, "createOrder", apiErr.Op)
}

// Frames are matched on clientRequestId alone, so answers arriving out of order still reach
// the caller that asked.
func TestConcurrentOrdersAreDemultiplexed(t *testing.T) {
	srv := perpstest.New(t)
	conn := tradingConn(t, srv)

	const orders = 8
	crids := make([]string, orders)
	results := make([]*perps.OrderResult, orders)
	errs := make([]error, orders)
	var wg sync.WaitGroup
	for i := range orders {
		crids[i] = uuid.NewString()
		req := limitOrder()
		req.ClientRequestID = crids[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = conn.CreateOrder(t.Context(), req)
		}()
	}
	for range orders {
		command(t, srv, "createOrder")
	}

	// Back to front, so answering positionally would hand every caller the wrong order.
	for i := orders - 1; i >= 0; i-- {
		srv.Push(envelope("createOrder", map[string]any{
			"statusCode": 200, "body": "order accepted", "clientRequestId": crids[i],
			"order": orderObject(crids[i], fmt.Sprintf("order-%d", i), "active"),
		}))
	}
	wg.Wait()

	for i := range orders {
		require.NoError(t, errs[i], "order %d", i)
		assert.Equal(t, crids[i], results[i].ClientRequestID, "order %d", i)
		assert.Equal(t, fmt.Sprintf("order-%d", i), results[i].Order.OrderID,
			"order %d was given another caller's answer", i)
	}
}

// A socket that goes away mid-request has to end every request on it, or a caller waits for a
// reply that can no longer come.
func TestDisconnectFailsRequestsInFlight(t *testing.T) {
	srv := perpstest.New(t)
	hooks := newOrderHooks()
	conn := tradingConn(t, srv, rails.WithHooks(hooks.hooks()))

	failed := make(chan error, 1)
	go func() {
		_, err := conn.CreateOrder(t.Context(), limitOrder())
		failed <- err
	}()

	crid := command(t, srv, "createOrder").ClientRequestID()
	srv.Push(envelope("createOrder", map[string]any{
		"statusCode": 202, "body": "order created, pending acceptance",
		"clientRequestId": crid, "apiQuotaUsed": 1,
		"order": orderObject(crid, "01JGPRJC7KHY8GP4FT6WRQG193", "pending"),
	}))
	srv.CloseConn(0, 1012, "Service restarting. Please reconnect.")

	err := spectest.Receive(t, failed)
	assert.ErrorIs(t, err, rails.ErrConnClosed)
	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, crid, apiErr.ClientRequestID)
	assert.Equal(t, "createOrder", apiErr.Op)

	// The hook sees it too, not only the caller.
	assert.ErrorIs(t, spectest.Receive(t, hooks.done).err, rails.ErrConnClosed)
}

// A post-only rejection is pushed on the orders stream carrying the id it rejected. It ends
// the request, and stays an event: a caller tracking its own book has to see it either way.
func TestPushedRejectionIsBothTheAnswerAndAnEvent(t *testing.T) {
	srv := perpstest.New(t)
	conn := tradingConn(t, srv)

	failed := make(chan error, 1)
	go func() {
		_, err := conn.CreateOrder(t.Context(), limitOrder())
		failed <- err
	}()

	crid := command(t, srv, "createOrder").ClientRequestID()
	srv.Push(envelope("rejectOrder", map[string]any{
		"statusCode": 400, "clientRequestId": crid,
		"body":  "Post-only order rejected as it would have matched immediately",
		"order": orderObject(crid, "01JGPRJC7KHY8GP4FT6WRQG193", "rejected"),
	}))

	var apiErr *rails.Error
	require.ErrorAs(t, spectest.Receive(t, failed), &apiErr)
	assert.Equal(t, 400, apiErr.StatusCode)

	rejected := waitEvent[perps.OrderRejected](t, conn.Events())
	assert.Equal(t, crid, rejected.ClientRequestID)
	assert.Equal(t, "Post-only order rejected as it would have matched immediately", rejected.Body)
}

// The other half of that rule: a frame the caller waited for is its answer and nothing else,
// or every command would echo into the event stream.
func TestACommandsOwnReplyIsNotAlsoAnEvent(t *testing.T) {
	srv := perpstest.New(t)
	conn := tradingConn(t, srv)
	waitEvent[perps.Connected](t, conn.Events())

	_, err := conn.Ping(t.Context())
	require.NoError(t, err)

	// Events keep their arrival order, so an echoed pong would come out ahead of this.
	srv.Push(envelope("somethingNew", map[string]any{"statusCode": 200}))
	assert.Equal(t, "somethingNew", waitEvent[perps.Unknown](t, conn.Events()).ResultType)
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	body, err := json.Marshal(v)
	require.NoError(t, err)
	return string(body)
}

// A report is built from the same measurements the hooks see, on the path that places orders.
func TestOrdersReachTheTelemetryReport(t *testing.T) {
	reports := make(chan telemetry.Report, 4)
	ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var report telemetry.Report
		require.NoError(t, json.NewDecoder(r.Body).Decode(&report))
		select {
		case reports <- report:
		default:
		}
	}))
	defer ingest.Close()

	srv := perpstest.New(t)
	client := dial(t, srv, rails.WithEnvironment(rails.Environment{
		AuthURL: "https://auth.invalid", OrderURL: "https://order.invalid",
		AccountURL: "https://account.invalid", MarketDataURL: "https://market-data.invalid",
		WSURL: srv.URL, TelemetryURL: ingest.URL,
	}))
	conn, err := client.Connect(t.Context(), market, perps.WithPingInterval(0))
	require.NoError(t, err)
	defer conn.Close()

	placed := make(chan struct{})
	go func() {
		defer close(placed)
		_, err := conn.CreateOrder(t.Context(), limitOrder())
		assert.NoError(t, err)
	}()
	crid := command(t, srv, "createOrder").ClientRequestID()
	srv.Push(envelope("createOrder", map[string]any{
		"statusCode": 202, "body": "order created, pending acceptance",
		"clientRequestId": crid, "apiQuotaUsed": 1,
		"order": orderObject(crid, "01JGPRJC7KHY8GP4FT6WRQG193", "pending"),
	}))
	srv.Push(envelope("createOrder", map[string]any{
		"statusCode": 200, "body": "order accepted", "clientRequestId": crid,
		"order": orderObject(crid, "01JGPRJC7KHY8GP4FT6WRQG193", "active"),
	}))
	<-placed // the measurements exist once the order is answered, not once it is sent
	client.Close()

	report := spectest.Receive(t, reports)
	assert.Equal(t, "perps", report.Product)
	require.Len(t, report.Requests, 1)
	assert.Equal(t, "createOrder", report.Requests[0].Op)
	assert.EqualValues(t, 1, report.Requests[0].AckMs.Count, "the acknowledgement was not timed")
	assert.Equal(t, map[string]int64{"200": 1}, report.Requests[0].Status)

	require.Len(t, report.Connections, 1)
	assert.Equal(t, market, report.Connections[0].Market)
	assert.EqualValues(t, 1, report.Connections[0].Connect.Successes)
	assert.Positive(t, report.Connections[0].Connect.DialMs.SumMs, "the dial was not timed")
}
