package options_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/perpstest"
	"github.com/rails-xyz/exchange-go-sdk/internal/spectest"
	"github.com/rails-xyz/exchange-go-sdk/options"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testContract = "BTC-24JUN26-62000-C"

// outcome is a call's return value and error.
type outcome[T any] struct {
	value T
	err   error
}

// async runs a call on its own goroutine, so the test can answer it.
func async[T any](call func() (T, error)) <-chan outcome[T] {
	got := make(chan outcome[T], 1)
	go func() {
		value, err := call()
		got <- outcome[T]{value, err}
	}()
	return got
}

// command waits for the next command a connection sends and checks which one it is.
func command(t *testing.T, srv *perpstest.Server, want string) perpstest.Message {
	t.Helper()
	msg := srv.NextMessage(settle)
	require.Equal(t, want, msg.Message)
	require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
		msg.ClientRequestID(), "clientRequestId is not a canonical UUID v4")
	return msg
}

// frame is the envelope every answer and push arrives in.
func frame(resultType, market string, data map[string]any) map[string]any {
	return map[string]any{"resultType": resultType, "market": market, "data": data}
}

// orderObject is the order as the exchange reports it inside a frame.
func orderObject(crid, orderID, status string) map[string]any {
	return map[string]any{
		"contract": testContract, "orderId": orderID, "orderType": "buy", "tradeType": "limit",
		"price": "100", "quantity": "1", "filledQuantity": "0", "avgFillPrice": "0", "leverage": "0",
		"orderStatus": status, "postOnly": true, "createdAt": 1783053411143, "updatedAt": 1783053411171,
		"clientRequestId": crid,
	}
}

func orderFrame(resultType string, status int, crid, orderID, orderStatus string) map[string]any {
	return frame(resultType, testContract, map[string]any{
		"statusCode": status, "body": "", "clientRequestId": crid,
		"order": orderObject(crid, orderID, orderStatus),
	})
}

func limitOrder() options.CreateOrderRequest {
	return options.CreateOrderRequest{
		Contract: testContract, Price: "100", Quantity: "1",
		OrderType: options.OrderTypeBuy, TradeType: options.TradeTypeLimit, PostOnly: true,
	}
}

// A create that takes liquidity is answered by the fill, which ends the call and is not also
// an event.
func TestCreateOrderResolvesOnTheFillThatAnswersIt(t *testing.T) {
	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())
	waitEvent[options.Connected](t, conn.Events())

	got := async(func() (*options.OrderResult, error) { return conn.CreateOrder(t.Context(), limitOrder()) })
	crid := command(t, srv, "createOrder").ClientRequestID()
	srv.Push(orderFrame("createOrder", 202, crid, "01KWVNC7T8XWVHGKY00A9TD0YG", "pending"))
	srv.Push(orderFrame("completeFillOrder", 200, crid, "01KWVNC7T8XWVHGKY00A9TD0YG", "completed"))

	res := spectest.Receive(t, got)
	require.NoError(t, res.err)
	assert.Equal(t, crid, res.value.ClientRequestID, "the minted id is not the one reported")
	assert.Equal(t, options.StatusCompleted, res.value.Order.Status)
	assert.False(t, res.value.AckAt.IsZero(), "the acknowledgement went unrecorded")

	srv.Push(frame("somethingNew", "", map[string]any{"statusCode": 200}))
	for {
		event := spectest.Receive(t, conn.Events())
		_, filled := event.(options.OrderFilled)
		require.False(t, filled, "the fill that answered the create was also an event")
		if unknown, ok := event.(options.Unknown); ok {
			assert.Equal(t, "somethingNew", unknown.ResultType)
			break
		}
	}
}

// A market order that finds no liquidity is answered 200 with the order cancelled: an answer,
// not a failure.
func TestCreateOrderAnsweredCancelledIsAResult(t *testing.T) {
	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())

	req := limitOrder()
	req.TradeType, req.PostOnly = options.TradeTypeMarket, false
	got := async(func() (*options.OrderResult, error) { return conn.CreateOrder(t.Context(), req) })
	crid := command(t, srv, "createOrder").ClientRequestID()
	srv.Push(orderFrame("createOrder", 200, crid, "01KWVNC7T8XWVHGKY00A9TD0YG", "cancelled"))

	res := spectest.Receive(t, got)
	require.NoError(t, res.err)
	assert.Equal(t, options.StatusCancelled, res.value.Order.Status)
}

func TestCancelOrderResolvesWithoutAnAcknowledgement(t *testing.T) {
	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())

	const orderID = "01KWK47RT6A4DD1TCQE662QJ3W"
	got := async(func() (*options.OrderResult, error) {
		return conn.CancelOrder(t.Context(), options.CancelOrderRequest{Contract: testContract, OrderID: orderID})
	})
	crid := command(t, srv, "cancelOrder").ClientRequestID()
	srv.Push(orderFrame("cancelOrder", 200, crid, orderID, "cancelled"))

	res := spectest.Receive(t, got)
	require.NoError(t, res.err)
	assert.Equal(t, options.StatusCancelled, res.value.Order.Status)
	assert.Equal(t, orderID, res.value.Order.OrderID)
	assert.True(t, res.value.AckAt.IsZero(), "a cancel is never acknowledged")
}

// The sweep's acknowledgement is its answer, and each cancel it dispatched is an event carrying
// the sweep's id, in whatever order the exchange sends them.
func TestCancelAllOrdersResolvesOnTheAcknowledgement(t *testing.T) {
	for _, childrenFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("children first: %v", childrenFirst), func(t *testing.T) {
			srv := fakeExchange(t)
			conn := tradingConn(t, srv, options.Scope{Market: "BTC-USDT", Expiration: "24JUN26"})

			got := async(func() (*options.CancelAllResult, error) {
				return conn.CancelAllOrders(t.Context(), options.CancelAllOrdersRequest{})
			})
			sent := command(t, srv, "cancelAllOrders")
			crid := sent.ClientRequestID()
			assert.Equal(t, map[string]any{"clientRequestId": crid}, spectest.Decode(t, sent.Content),
				"a sweep of the connection's scope names no market")

			children := func() {
				for i := range 2 {
					srv.Push(orderFrame("cancelOrder", 200, crid, fmt.Sprintf("order-%d", i), "cancelled"))
				}
			}
			if childrenFirst {
				children()
			}
			srv.Push(frame("cancelAllOrders", "BTC-24JUN26", map[string]any{
				"statusCode": 202, "body": "cancel all received", "count": 2, "clientRequestId": crid,
			}))
			res := spectest.Receive(t, got)
			require.NoError(t, res.err)
			assert.Equal(t, 2, res.value.Count)
			assert.Equal(t, crid, res.value.ClientRequestID)
			if !childrenFirst {
				children()
			}

			for i := range 2 {
				cancelled := waitEvent[options.OrderCancelled](t, conn.Events())
				assert.Equal(t, crid, cancelled.ClientRequestID)
				assert.Equal(t, fmt.Sprintf("order-%d", i), cancelled.Order.OrderID)
				assert.Equal(t, options.StatusCancelled, cancelled.Order.Status)
			}
		})
	}
}

// A sweep can be acknowledged twice when the exchange fails over; the second answers nothing.
func TestRepeatedCancelAllAcknowledgementIsUnknown(t *testing.T) {
	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())

	got := async(func() (*options.CancelAllResult, error) {
		return conn.CancelAllOrders(t.Context(), options.CancelAllOrdersRequest{})
	})
	crid := command(t, srv, "cancelAllOrders").ClientRequestID()
	ack := frame("cancelAllOrders", "ALL", map[string]any{
		"statusCode": 202, "body": "cancel all received", "count": 0, "clientRequestId": crid,
	})
	srv.Push(ack)
	require.NoError(t, spectest.Receive(t, got).err)
	srv.Push(ack)

	assert.Equal(t, "cancelAllOrders", waitEvent[options.Unknown](t, conn.Events()).ResultType)
}

// A command whose answers could not reach the connection is refused before anything is sent.
func TestOrderCommandsOutsideTheConnectionSendNothing(t *testing.T) {
	scope := options.Scope{Market: "BTC-USDT", Expiration: "24JUN26", Strike: "6200"}
	commands := map[string]func(ctx context.Context, conn *options.Conn, contract string) error{
		"createOrder": func(ctx context.Context, conn *options.Conn, contract string) error {
			req := limitOrder()
			req.Contract = contract
			_, err := conn.CreateOrder(ctx, req)
			return err
		},
		"modifyOrder": func(ctx context.Context, conn *options.Conn, contract string) error {
			_, err := conn.ModifyOrder(ctx, options.ModifyOrderRequest{Contract: contract, OrderID: "o", Price: "1", Quantity: "1"})
			return err
		},
		"cancelOrder": func(ctx context.Context, conn *options.Conn, contract string) error {
			_, err := conn.CancelOrder(ctx, options.CancelOrderRequest{Contract: contract, OrderID: "o"})
			return err
		},
		"cancelAllOrders": func(ctx context.Context, conn *options.Conn, contract string) error {
			_, err := conn.CancelAllOrders(ctx, options.CancelAllOrdersRequest{Contract: contract})
			return err
		},
	}
	for op, send := range commands {
		t.Run(op, func(t *testing.T) {
			srv := fakeExchange(t)
			conn := tradingConn(t, srv, scope)

			// Bounded, so a command that does go out fails here rather than waiting for an answer.
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			for _, contract := range []string{"BTC-25JUN26-6200-C", "BTC-24JUN26-62000-C", "ETH-24JUN26-6200-C"} {
				assert.ErrorIs(t, send(ctx, conn, contract), options.ErrOutOfScope, contract)
			}
			changeSubscriptions(t, srv, "unsubscribe", func() error {
				return conn.Unsubscribe(t.Context(), scope, options.SubscriptionOrders)
			})
			assert.ErrorIs(t, send(ctx, conn, "BTC-24JUN26-6200-C"), options.ErrNotSubscribed)

			_, err := conn.Ping(t.Context())
			require.NoError(t, err)
			var sent []string
			for _, msg := range srv.Received() {
				sent = append(sent, msg.Message)
			}
			assert.Equal(t, []string{"unsubscribe", "ping"}, sent, "a refused command reached the exchange")
		})
	}
}

// Each command draws on its own budget, and the hooks see the command's contract, or the
// connection's scope when it names none.
func TestOrderCommandsDrawOnTheirClassesAndReachTheHooks(t *testing.T) {
	srv := fakeExchange(t)
	pacer := &classPacer{}
	done := make(chan rails.ReqInfo, 8)
	connected := make(chan rails.ConnInfo, 1)
	conn := tradingConn(t, srv, options.Scope{Market: "BTC-USDT", Expiration: "24JUN26"},
		rails.WithPacer(pacer),
		rails.WithHooks(rails.Hooks{
			ConnectFinished: func(info rails.ConnInfo, _ rails.ConnectStats, _ error) { connected <- info },
			RequestDone:     func(info rails.ReqInfo, _ time.Duration, _ error) { done <- info },
		}))
	assert.Equal(t, "BTC-24JUN26", spectest.Receive(t, connected).Market)

	reduceOnly := limitOrder()
	reduceOnly.ReduceOnly = true
	steps := []struct {
		op     string
		call   func() error
		answer func(crid string) map[string]any
		market string
	}{
		{"createOrder", func() error { _, err := conn.CreateOrder(t.Context(), limitOrder()); return err },
			func(crid string) map[string]any { return orderFrame("createOrder", 200, crid, "o", "open") }, testContract},
		{"createOrder", func() error { _, err := conn.CreateOrder(t.Context(), reduceOnly); return err },
			func(crid string) map[string]any { return orderFrame("createOrder", 200, crid, "o", "open") }, testContract},
		{"modifyOrder", func() error {
			_, err := conn.ModifyOrder(t.Context(), options.ModifyOrderRequest{Contract: testContract, OrderID: "o", Price: "1", Quantity: "1"})
			return err
		}, func(crid string) map[string]any { return orderFrame("modifyOrder", 200, crid, "o", "open") }, testContract},
		{"cancelOrder", func() error {
			_, err := conn.CancelOrder(t.Context(), options.CancelOrderRequest{Contract: testContract, OrderID: "o"})
			return err
		}, func(crid string) map[string]any { return orderFrame("cancelOrder", 200, crid, "o", "cancelled") }, testContract},
		{"cancelAllOrders", func() error {
			_, err := conn.CancelAllOrders(t.Context(), options.CancelAllOrdersRequest{})
			return err
		}, func(crid string) map[string]any {
			return frame("cancelAllOrders", "BTC-24JUN26", map[string]any{"statusCode": 202, "count": 0, "clientRequestId": crid})
		}, "BTC-24JUN26"},
	}
	for _, step := range steps {
		got := async(func() (struct{}, error) { return struct{}{}, step.call() })
		crid := command(t, srv, step.op).ClientRequestID()
		srv.Push(step.answer(crid))
		require.NoError(t, spectest.Receive(t, got).err, step.op)
		info := spectest.Receive(t, done)
		assert.Equal(t, step.op, info.Op)
		assert.Equal(t, step.market, info.Market, step.op)
	}
	assert.Equal(t, []rails.OpClass{
		options.ClassOrderCreate, options.ClassOrderReduceOnly, options.ClassOrderModify,
		options.ClassOrderCancel, options.ClassOrderCancelAll,
	}, pacer.classes)
}

// fallbackConn is a connection to the fake exchange whose HTTP API is e, where the commands
// its socket cannot carry go.
func fallbackConn(t *testing.T, e *exchange, scope options.Scope, opts ...rails.Option) (*perpstest.Server, *options.Conn) {
	t.Helper()
	srv := fakeExchange(t)
	env := rails.WithEnvironment(rails.Environment{
		AuthURL: "https://auth.invalid", OptionsURL: e.srv.URL,
		WSURL: srv.URL, TelemetryURL: "https://telemetry.invalid",
	})
	return srv, tradingConn(t, srv, scope, append(opts, env)...)
}

// takeDown drops the socket and refuses every redial.
func takeDown(t *testing.T, srv *perpstest.Server, conn *options.Conn) {
	t.Helper()
	srv.SetReject(func(w http.ResponseWriter, _ *http.Request) bool {
		http.Error(w, "DRAINING", http.StatusServiceUnavailable)
		return true
	})
	srv.CloseConn(0, 1012, "Service restarting. Please reconnect.")
	waitEvent[options.Disconnected](t, conn.Events())
}

func answeredBySpec(t *testing.T, path string) *exchange {
	t.Helper()
	spec := specOperation(t, "options-matching", http.MethodPost, path)
	return newExchange(t, spec.status, spec.example)
}

func TestCancelOrderGoesOverHTTPWhenTheSocketIsDown(t *testing.T) {
	e := answeredBySpec(t, "/v1/matching/orders/cancel")
	srv, conn := fallbackConn(t, e, options.ScopeAll())
	takeDown(t, srv, conn)

	const crid = "9f1c2c9e-3f4a-4a4e-9f10-2b7d3f2a1c55"
	got, err := conn.CancelOrder(t.Context(), options.CancelOrderRequest{Contract: testContract, OrderID: "o1", ClientRequestID: crid})
	require.NoError(t, err)
	assert.True(t, got.ViaHTTP)
	assert.NotEmpty(t, got.Order.OrderID)

	sent := e.request()
	assert.Equal(t, "/v1/matching/orders/cancel", sent.path)
	assert.Equal(t, crid, spectest.Decode(t, sent.body)["clientRequestId"], "a cancel that never left keeps its id")
}

func TestCancelOrderWhoseAnswerWasLostGoesOverHTTPUnderAFreshID(t *testing.T) {
	// The queued answer may leave clientRequestId out.
	e := newExchange(t, http.StatusAccepted, []byte(`{"action":"cancel","queued":true,"accepted":false,`+
		`"orderId":"o1","contract":"`+testContract+`"}`))
	srv, conn := fallbackConn(t, e, options.ScopeAll())

	got := async(func() (*options.OrderResult, error) {
		return conn.CancelOrder(t.Context(), options.CancelOrderRequest{Contract: testContract, OrderID: "o1"})
	})
	crid := command(t, srv, "cancelOrder").ClientRequestID()
	takeDown(t, srv, conn)

	res := spectest.Receive(t, got)
	require.NoError(t, res.err)
	assert.True(t, res.value.ViaHTTP)
	resent := spectest.Decode(t, e.request().body)["clientRequestId"]
	assert.NotEqual(t, crid, resent, "a cancel that may have reached the exchange is resent under a fresh id")
	assert.Equal(t, resent, res.value.ClientRequestID, "the result names the id the cancel was resent under")
}

// socketPacer refuses the WebSocket commands, as a fail-fast pacer out of budget does, and
// admits HTTP, so a fallback would show as a request.
type socketPacer struct{}

func (socketPacer) Wait(_ context.Context, _ rails.OpClass, op string) error {
	if strings.Contains(op, " ") { // an HTTP route, "POST /v1/..."
		return nil
	}
	return fmt.Errorf("%w: %w", rails.ErrNotSent, rails.ErrPaced)
}

func (socketPacer) Observe(rails.OpClass, error) {}

func TestAPacedCancelStaysOffHTTP(t *testing.T) {
	e := answeredBySpec(t, "/v1/matching/orders/cancel")
	_, conn := fallbackConn(t, e, options.ScopeAll(), rails.WithPacer(socketPacer{}))

	_, err := conn.CancelOrder(t.Context(), options.CancelOrderRequest{Contract: testContract, OrderID: "o1"})
	assert.ErrorIs(t, err, rails.ErrPaced)
	assert.Empty(t, e.requests)
}

func TestOnlyAReduceOnlyCreateGoesOverHTTPWhenTheSocketIsDown(t *testing.T) {
	for _, reduceOnly := range []bool{true, false} {
		t.Run(fmt.Sprint("reduceOnly=", reduceOnly), func(t *testing.T) {
			e := answeredBySpec(t, "/v1/matching/orders/create")
			srv, conn := fallbackConn(t, e, options.ScopeAll())
			takeDown(t, srv, conn)

			const crid, orderID = "9f1c2c9e-3f4a-4a4e-9f10-2b7d3f2a1c55", "2b6c8e1a-0d4f-4e7b-9c3a-1f5d7e9b0a2c"
			req := limitOrder()
			req.ReduceOnly, req.ClientRequestID, req.OrderID = reduceOnly, crid, orderID
			got, err := conn.CreateOrder(t.Context(), req)

			if !reduceOnly {
				assert.ErrorIs(t, err, rails.ErrNotSent)
				assert.Empty(t, e.requests)
				return
			}
			require.NoError(t, err)
			assert.True(t, got.ViaHTTP)
			body := spectest.Decode(t, e.request().body)
			assert.Equal(t, crid, body["clientRequestId"])
			assert.Equal(t, orderID, spectest.Decode(t, spectest.Raw(t, body["order"]))["orderId"])
		})
	}
}

// A create that may have executed is never resent, even reduce-only; its error names the order
// to look up.
func TestACreateWithAnUnknownOutcomeCarriesItsOrderID(t *testing.T) {
	e := answeredBySpec(t, "/v1/matching/orders/create")
	hooked := make(chan error, 1)
	srv, conn := fallbackConn(t, e, options.ScopeAll(), rails.WithHooks(rails.Hooks{
		RequestDone: func(info rails.ReqInfo, _ time.Duration, err error) {
			if info.Op == "createOrder" {
				hooked <- err
			}
		},
	}))

	req := limitOrder()
	req.ReduceOnly = true
	got := async(func() (*options.OrderResult, error) { return conn.CreateOrder(t.Context(), req) })
	sent := spectest.Decode(t, command(t, srv, "createOrder").Content)
	takeDown(t, srv, conn)

	var apiErr *rails.Error
	err := spectest.Receive(t, got).err
	require.ErrorAs(t, err, &apiErr)
	assert.ErrorIs(t, err, rails.ErrConnClosed)
	assert.Equal(t, sent["orderId"], apiErr.OrderID)
	assert.Empty(t, e.requests)

	var seen *rails.Error
	require.ErrorAs(t, spectest.Receive(t, hooked), &seen)
	assert.Empty(t, seen.OrderID, "the error the hooks hold was written to")
}

// A refusal over HTTP says what happened to the create; anything else may have queued it, so
// the error names the order to look up.
func TestAReduceOnlyCreateOverHTTPNamesItsOrderWhenTheOutcomeIsUnknown(t *testing.T) {
	refused := func(status int) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"slug":"BAD_REQUEST","code":"SYS-0006"},"message":"refused"}`))
		}
	}
	for name, tc := range map[string]struct {
		handler    func(http.ResponseWriter, *http.Request)
		unknown    bool
		connClosed bool
	}{
		"refused":     {handler: refused(http.StatusBadRequest)},
		"bad gateway": {handler: refused(http.StatusBadGateway), unknown: true},
		"no answer": {handler: func(w http.ResponseWriter, _ *http.Request) {
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
		}, unknown: true, connClosed: true},
	} {
		t.Run(name, func(t *testing.T) {
			e := &exchange{t: t, srv: httptest.NewServer(http.HandlerFunc(tc.handler)), requests: make(chan request, 8), pacer: &classPacer{}}
			t.Cleanup(e.srv.Close)
			srv, conn := fallbackConn(t, e, options.ScopeAll())
			takeDown(t, srv, conn)

			const crid, orderID = "9f1c2c9e-3f4a-4a4e-9f10-2b7d3f2a1c55", "2b6c8e1a-0d4f-4e7b-9c3a-1f5d7e9b0a2c"
			req := limitOrder()
			req.ReduceOnly, req.ClientRequestID, req.OrderID = true, crid, orderID
			_, err := conn.CreateOrder(t.Context(), req)

			var apiErr *rails.Error
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, crid, apiErr.ClientRequestID)
			if !tc.unknown {
				assert.Empty(t, apiErr.OrderID)
				return
			}
			assert.Equal(t, orderID, apiErr.OrderID)
			if tc.connClosed {
				assert.ErrorIs(t, err, rails.ErrConnClosed)
			}
		})
	}
}

// Over HTTP an empty Contract sweeps the whole account, so a narrower scope keeps its sweep on
// the socket.
func TestCancelAllOrdersGoesOverHTTPOnlyWhenItSweepsNoMore(t *testing.T) {
	narrow := options.Scope{Market: "BTC-USDT", Expiration: "24JUN26"}
	for name, tc := range map[string]struct {
		scope    options.Scope
		contract string
		viaHTTP  bool
	}{
		"one contract":            {scope: narrow, contract: testContract, viaHTTP: true},
		"every contract of ALL":   {scope: options.ScopeAll(), viaHTTP: true},
		"every contract of scope": {scope: narrow},
	} {
		t.Run(name, func(t *testing.T) {
			e := answeredBySpec(t, "/v1/matching/orders/cancel-all")
			srv, conn := fallbackConn(t, e, tc.scope)
			takeDown(t, srv, conn)

			got, err := conn.CancelAllOrders(t.Context(), options.CancelAllOrdersRequest{Contract: tc.contract})
			if !tc.viaHTTP {
				assert.ErrorIs(t, err, rails.ErrNotSent)
				assert.Empty(t, e.requests)
				return
			}
			require.NoError(t, err)
			assert.True(t, got.ViaHTTP)
			assert.Len(t, got.Cancels, got.Count)
			body := spectest.Decode(t, e.request().body)
			assert.Equal(t, tc.contract, spectest.Str(body["contract"]))
		})
	}
}

// ledgerPacer books every call it is asked to pace and keeps every outcome it is shown.
type ledgerPacer struct {
	mu       sync.Mutex
	booked   []rails.OpClass
	observed []error
}

func (p *ledgerPacer) Wait(_ context.Context, class rails.OpClass, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.booked = append(p.booked, class)
	return nil
}

func (p *ledgerPacer) Observe(_ rails.OpClass, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observed = append(p.observed, err)
}

// A cancel the socket could not carry is booked once, and its HTTP refusal still reaches the
// pacer.
func TestACancelOverHTTPIsPacedOnce(t *testing.T) {
	e := newExchange(t, http.StatusTooManyRequests,
		[]byte(`{"error":{"slug":"TOO_MANY_REQUESTS","code":"SYS-0003"},"message":"rate limit exceeded"}`))
	pacer := &ledgerPacer{}
	srv, conn := fallbackConn(t, e, options.ScopeAll(), rails.WithPacer(pacer))
	takeDown(t, srv, conn)

	_, err := conn.CancelOrder(t.Context(), options.CancelOrderRequest{Contract: testContract, OrderID: "o1"})
	require.ErrorIs(t, err, rails.ErrRateLimited)
	e.request()

	pacer.mu.Lock()
	defer pacer.mu.Unlock()
	assert.Equal(t, []rails.OpClass{options.ClassOrderCancel}, pacer.booked)
	assert.True(t, slices.ContainsFunc(pacer.observed, func(err error) bool { return errors.Is(err, rails.ErrRateLimited) }),
		"the pacer never saw the HTTP refusal")
}
