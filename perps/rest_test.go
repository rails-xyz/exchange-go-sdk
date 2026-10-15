package perps_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rails-xyz/exchange-go-sdk/perps"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticToken string

func (s staticToken) Token(context.Context) (string, error) { return string(s), nil }

// exchange stands in for both REST hosts: one server answers every route the client calls.
type exchange struct {
	t   *testing.T
	srv *httptest.Server
}

func newExchange(t *testing.T, h http.HandlerFunc) *exchange {
	t.Helper()
	e := &exchange{t: t, srv: httptest.NewServer(h)}
	t.Cleanup(e.srv.Close)
	return e
}

func (e *exchange) client(opts ...rails.Option) *perps.Client {
	e.t.Helper()
	env := rails.Environment{
		AuthURL: "https://auth.invalid", OrderURL: e.srv.URL, AccountURL: e.srv.URL,
		MarketDataURL: e.srv.URL,
		WSURL:         "wss://ws.invalid",
		TelemetryURL:  "https://telemetry.invalid",
	}
	c, err := perps.NewClient(
		rails.Credentials{APIKey: "key", APISecret: "secret"},
		append([]rails.Option{
			rails.WithEnvironment(env),
			rails.WithTokenSource(staticToken("jwt")),
		}, opts...)...,
	)
	require.NoError(e.t, err)
	e.t.Cleanup(c.Close)
	return c
}

func openOrder(market, id string) map[string]any {
	return map[string]any{
		"market": market, "orderId": id, "orderType": "buy", "tradeType": "limit",
		"price": "65000", "quantity": "0.5", "filledQuantity": "0.1", "leverage": "1",
		"createdAt": 1746642892940, "updatedAt": 1746642892941, "orderStatus": "active",
		"clientRequestId": "0835bf3e-4a20-41f0-908d-8213f2b7a285",
		"reduceOnly":      false, "postOnly": true, "avgFillPrice": "65000",
	}
}

// The cursor is an opaque key fragment full of #, so it only survives the round trip if it is
// escaped going out and read back decoded.
func TestOpenOrdersFollowsTheCursor(t *testing.T) {
	// A cursor as the API hands it back: # separates keys, @ and + appear in timestamps.
	const cursor = "ETH-USDT#01HYXJVRCTDXQSTKZ17ND2SBJH@1746642892940+c3029869"
	var starts []string

	e := newExchange(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/open-orders", r.URL.Path)
		start := r.URL.Query().Get("start")
		starts = append(starts, start)
		assert.Equal(t, "BTC-USDT", r.URL.Query().Get("market"))
		body := map[string]any{"openOrders": []any{openOrder("BTC-USDT", "order-"+fmt.Sprint(len(starts)))}}
		if start == "" {
			body["last"] = cursor // one more page to fetch
		}
		_ = json.NewEncoder(w).Encode(body)
	})

	orders, err := e.client().OpenOrders(t.Context(), "BTC-USDT")
	require.NoError(t, err)
	require.Len(t, orders, 2, "orders across the pages")
	assert.Equal(t, []string{"", cursor}, starts, "the cursor should be replayed as start")
	// Every field, because the mapping is fifteen mechanical assignments between two structs
	// whose fields are mostly strings: a swap the compiler cannot see.
	want := perps.Order{
		Market: "BTC-USDT", OrderID: "order-1",
		OrderType: perps.OrderTypeBuy, TradeType: perps.TradeTypeLimit,
		Price: "65000", Quantity: "0.5", FilledQuantity: "0.1", Leverage: "1",
		CreatedAt: 1746642892940, UpdatedAt: 1746642892941,
		OrderStatus:     perps.OrderStatusActive,
		ClientRequestID: "0835bf3e-4a20-41f0-908d-8213f2b7a285",
		ReduceOnly:      false, PostOnly: true, AvgFillPrice: "65000",
	}
	assert.Equal(t, want, orders[0], "the wire order did not map")
}

// The optional wire fields are pointers; absent must read as empty, not panic.
func TestOpenOrdersMapsAbsentOptionalFields(t *testing.T) {
	e := newExchange(t, func(w http.ResponseWriter, _ *http.Request) {
		order := openOrder("BTC-USDT", "order-1")
		delete(order, "clientRequestId")
		delete(order, "avgFillPrice")
		_ = json.NewEncoder(w).Encode(map[string]any{"openOrders": []any{order}})
	})

	orders, err := e.client().OpenOrders(t.Context(), "BTC-USDT")
	require.NoError(t, err)
	require.NotEmpty(t, orders)
	assert.Empty(t, orders[0].ClientRequestID)
	assert.Empty(t, orders[0].AvgFillPrice)
}

// A cursor that comes round again means the page just collected is a repeat, so the result
// would be both duplicated and short. That is an error, not a quiet success.
func TestOpenOrdersRejectsACursorCycle(t *testing.T) {
	cycles := map[string][]string{
		"immediate repeat": {"A", "A"},
		"A to B to A":      {"A", "B", "A"},
	}
	for name, sequence := range cycles {
		t.Run(name, func(t *testing.T) {
			var page int
			e := newExchange(t, func(w http.ResponseWriter, _ *http.Request) {
				last := sequence[min(page, len(sequence)-1)]
				page++
				_ = json.NewEncoder(w).Encode(map[string]any{
					"openOrders": []any{openOrder("BTC-USDT", "same")},
					"last":       last,
				})
			})

			orders, err := e.client().OpenOrders(t.Context(), "BTC-USDT")
			require.Error(t, err, "a repeated cursor should not be reported as success")
			assert.Nil(t, orders, "a failed page walk must not return a partial result")
		})
	}
}

func TestOpenOrdersForEveryMarketOmitsTheParameter(t *testing.T) {
	e := newExchange(t, func(w http.ResponseWriter, r *http.Request) {
		assert.NotContains(t, r.URL.Query(), "market", "an all-markets read should omit it")
		_ = json.NewEncoder(w).Encode(map[string]any{"openOrders": []any{}})
	})
	_, err := e.client().OpenOrders(t.Context(), perps.MarketAll)
	require.NoError(t, err)
}

func TestOrderLookupsHitTheOrderHost(t *testing.T) {
	cases := []struct {
		name  string
		call  func(*perps.Client) (*perps.Order, error)
		param string
		value string
	}{
		{"by id", func(c *perps.Client) (*perps.Order, error) {
			return c.OrderByID(context.Background(), "ETH-USDT", "01HZ36SYBP4Z80GRX30P49XHB5")
		}, "order-id", "01HZ36SYBP4Z80GRX30P49XHB5"},
		{"by clientRequestId", func(c *perps.Client) (*perps.Order, error) {
			return c.OrderByClientRequestID(context.Background(), "ETH-USDT", "0835bf3e-4a20-41f0-908d-8213f2b7a285")
		}, "client-request-id", "0835bf3e-4a20-41f0-908d-8213f2b7a285"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newExchange(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/api/v1/order", r.URL.Path)
				assert.Equal(t, tc.value, r.URL.Query().Get(tc.param))
				body := openOrder("ETH-USDT", "01HZ36SYBP4Z80GRX30P49XHB5")
				body["orderStatus"] = "filled" // terminal: only this endpoint returns it
				_ = json.NewEncoder(w).Encode(map[string]any{"order": body})
			})

			got, err := tc.call(e.client())
			require.NoError(t, err)
			assert.Equal(t, perps.OrderStatusFilled, got.OrderStatus,
				"only this endpoint returns terminal orders")
		})
	}
}

func TestOrderNotFound(t *testing.T) {
	e := newExchange(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, `{"error":{"slug":"NOT_FOUND","code":"SYS-0007"},"message":"order not found"}`)
	})

	_, err := e.client().OrderByID(t.Context(), "ETH-USDT", "nope")
	require.ErrorIs(t, err, rails.ErrNotFound)

	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NOT_FOUND", apiErr.Slug)
	assert.Equal(t, "GET /api/v1/order", apiErr.Op)
}

// The route spells every market in lower case, unlike the WebSocket handshake.
func TestCancelAllOrdersTranslatesMarketAll(t *testing.T) {
	var gotMethod, gotMarket string
	e := newExchange(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotMarket = r.Method, r.URL.Query().Get("market")
		_, _ = fmt.Fprint(w, `{"message":"Cancelling 1 order for market all without any errors"}`)
	})

	require.NoError(t, e.client().CancelAllOrders(t.Context(), perps.CancelAllOrdersRequest{Market: perps.MarketAll}))
	assert.Equal(t, http.MethodDelete, gotMethod)
	assert.Equal(t, "all", gotMarket)
}

func position(market string) map[string]any {
	return map[string]any{
		"market": market, "value": "-65000.272138", "quantity": "-1.0",
		"funding": "920.49110924999998", "realizedPnl": "1.5", "unrealizedPnl": "400.25166999999965",
		"liquidationPrice": "61033.135417", "type": "SHORT", "notionalValue": "-65400.523808",
		"marginMode": "I",
		"fees":       "12.5", // the exchange sends more than the spec lists; extra fields are ignored
	}
}

// Every field, for the same reason as the open-orders mapping: ten string assignments the
// compiler cannot check for a swap.
func TestPositionsMapsEveryField(t *testing.T) {
	e := newExchange(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/positions", r.URL.Path)
		assert.Equal(t, "BTC-USDT", r.URL.Query().Get("market"))
		_ = json.NewEncoder(w).Encode(map[string]any{"positions": []any{position("BTC-USDT")}})
	})

	positions, err := e.client().Positions(t.Context(), "BTC-USDT")
	require.NoError(t, err)
	want := []perps.Position{{
		Market: "BTC-USDT", Value: "-65000.272138", Quantity: "-1.0",
		Funding: "920.49110924999998", RealizedPnL: "1.5", UnrealizedPnL: "400.25166999999965",
		LiquidationPrice: "61033.135417", Type: perps.PositionTypeShort,
		NotionalValue: "-65400.523808", MarginMode: perps.MarginModeIsolated,
	}}
	assert.Equal(t, want, positions, "the wire position did not map")
}

func TestPositionsForEveryMarketOmitsTheParameter(t *testing.T) {
	e := newExchange(t, func(w http.ResponseWriter, r *http.Request) {
		assert.NotContains(t, r.URL.Query(), "market", "an all-markets read should omit it")
		_ = json.NewEncoder(w).Encode(map[string]any{"positions": []any{position("BTC-USDT"), position("ETH-USDT")}})
	})
	positions, err := e.client().Positions(t.Context(), perps.MarketAll)
	require.NoError(t, err)
	assert.Len(t, positions, 2)
}

// An account with nothing open answers with an empty list, which must read as no positions
// rather than as a failure or a nil that a caller then ranges over differently.
func TestPositionsWithNothingOpenIsEmptyNotNil(t *testing.T) {
	e := newExchange(t, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"positions": []any{}})
	})
	positions, err := e.client().Positions(t.Context(), perps.MarketAll)
	require.NoError(t, err)
	assert.NotNil(t, positions)
	assert.Empty(t, positions)
}

func TestPositionsRequiresAMarket(t *testing.T) {
	e := newExchange(t, func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("no request should be made without a market")
	})
	_, err := e.client().Positions(t.Context(), "")
	require.Error(t, err)
}

// The exchange answers an unknown market with a 400 in the `{message}` envelope.
func TestPositionsUnknownMarket(t *testing.T) {
	e := newExchange(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `{"message":"invalid market"}`)
	})

	_, err := e.client().Positions(t.Context(), "NOPE-USDT")
	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.StatusCode)
	assert.Equal(t, "invalid market", apiErr.Message)
	assert.Equal(t, "GET /api/v1/positions", apiErr.Op)
}
