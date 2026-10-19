package options_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rails-xyz/exchange-go-sdk/internal/spectest"
	"github.com/rails-xyz/exchange-go-sdk/internal/wire/optionsaccount"
	"github.com/rails-xyz/exchange-go-sdk/internal/wire/optionsmatching"
	"github.com/rails-xyz/exchange-go-sdk/options"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticToken string

func (s staticToken) Token(context.Context) (string, error) { return string(s), nil }

// classPacer records the class of every metered call and never holds one back.
type classPacer struct{ classes []rails.OpClass }

func (p *classPacer) Wait(_ context.Context, class rails.OpClass, _ string) error {
	p.classes = append(p.classes, class)
	return nil
}

func (p *classPacer) Observe(rails.OpClass, error) {}

type request struct {
	method, path string // path as sent, still escaped
	query        url.Values
	auth         string
	body         []byte
}

// exchange answers with canned responses and hands each request it saw to the test.
type exchange struct {
	t        *testing.T
	srv      *httptest.Server
	requests chan request
	pacer    *classPacer
}

// newExchange answers with the bodies in turn, the last one for good.
func newExchange(t *testing.T, status int, bodies ...[]byte) *exchange {
	t.Helper()
	e := &exchange{t: t, requests: make(chan request, 8), pacer: &classPacer{}}
	var served atomic.Int32
	e.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent, _ := io.ReadAll(r.Body)
		e.requests <- request{
			method: r.Method, path: r.URL.EscapedPath(), query: r.URL.Query(),
			auth: r.Header.Get("Authorization"), body: sent,
		}
		w.WriteHeader(status)
		_, _ = w.Write(bodies[min(int(served.Add(1)), len(bodies))-1])
	}))
	t.Cleanup(e.srv.Close)
	return e
}

func (e *exchange) client() *options.Client {
	e.t.Helper()
	env := rails.Environment{
		AuthURL: "https://auth.invalid", OptionsURL: e.srv.URL,
		WSURL: "wss://ws.invalid", TelemetryURL: "https://telemetry.invalid",
	}
	c, err := options.NewClient(rails.Credentials{APIKey: "key", APISecret: "secret"},
		rails.WithEnvironment(env),
		rails.WithTokenSource(staticToken("jwt")),
		rails.WithPacer(e.pacer),
	)
	require.NoError(e.t, err)
	e.t.Cleanup(c.Close)
	return c
}

func (e *exchange) request() request {
	e.t.Helper()
	select {
	case r := <-e.requests:
		return r
	default:
		e.t.Fatal("no request reached the exchange")
		return request{}
	}
}

// operation is a vendored spec operation's success status and example, plus a write's example body.
type operation struct {
	status  int
	example json.RawMessage
	request json.RawMessage
}

type specDoc struct {
	Paths map[string]map[string]struct {
		RequestBody struct {
			Content map[string]struct {
				Example json.RawMessage `json:"example"`
			} `json:"content"`
		} `json:"requestBody"`
		Responses map[string]struct {
			Content map[string]struct {
				Example json.RawMessage `json:"example"`
			} `json:"content"`
		} `json:"responses"`
	} `json:"paths"`
}

func loadSpec(t *testing.T, spec string) specDoc {
	t.Helper()
	raw, err := os.ReadFile("../api/" + spec + ".json")
	require.NoError(t, err)
	var doc specDoc
	require.NoError(t, json.Unmarshal(raw, &doc))
	return doc
}

func specOperation(t *testing.T, spec, method, path string) operation {
	t.Helper()
	op, ok := loadSpec(t, spec).Paths[path][strings.ToLower(method)]
	require.True(t, ok, "%s has no %s %s", spec, method, path)
	for _, status := range []int{http.StatusOK, http.StatusAccepted} {
		if resp, ok := op.Responses[strconv.Itoa(status)]; ok {
			example := resp.Content["application/json"].Example
			require.NotEmpty(t, example, "%s %s has no %d example", method, path, status)
			return operation{status: status, example: example, request: op.RequestBody.Content["application/json"].Example}
		}
	}
	t.Fatalf("%s %s documents no success response", method, path)
	return operation{}
}

// Every operation of both specs against its own example: request, class, strict decode, and the
// public mapping field by field, since a swap between two string fields would compile.
func TestEveryOperationAgainstItsSpecExample(t *testing.T) {
	const (
		crid     = "9f1c2c9e-3f4a-4a4e-9f10-2b7d3f2a1c55"
		contract = "BTC-7FEB26-100000-C"
		orderID  = "01JRQM8H5XYZABCDEFGHJKMNPQ"
	)

	cases := []struct {
		spec, op  string // op is the telemetry name: method and path template
		call      func(context.Context, *options.Client) (any, error)
		path      string
		query     url.Values
		class     rails.OpClass
		wire      any
		want      any
		wantItems int // the whole list's length, when want is its first element
	}{
		{
			spec: "options", op: "GET /v1/contracts/markets/{market}/expirations",
			call: func(ctx context.Context, c *options.Client) (any, error) { return c.Expirations(ctx, "BTC-USDT") },
			path: "/v1/contracts/markets/BTC-USDT/expirations",
			wire: &optionsaccount.Expirations{},
			want: options.Expiration{
				ExpiryDate: "2026-07-03", ExpiryType: options.ExpiryTypeWeekly, ContractCount: 32,
				AtmIV: "0.3925", ExpectedPriceMove: "3853.10", IndexPrice: "61500.00",
			},
			wantItems: 3,
		},
		{
			spec: "options", op: "GET /v1/contracts/markets/{market}/expirations/{expiry}/contracts",
			call: func(ctx context.Context, c *options.Client) (any, error) {
				return c.Contracts(ctx, "BTC-USDT", "2026-07-03")
			},
			path: "/v1/contracts/markets/BTC-USDT/expirations/2026-07-03/contracts",
			wire: &optionsaccount.Contracts{},
			want: options.Contract{
				ContractName: "BTC-3JUL26-60000-C", Underlying: "BTC-USDT", Strike: "60000",
				OptionType: options.OptionTypeCall, ExpiryDate: "2026-07-03", ExpiryType: options.ExpiryTypeWeekly,
				MarkPrice: "1677.30", BidPrice: "0", AskPrice: "0", MarkIV: "0.3925", BidIV: "0", AskIV: "0",
				Forward: "61665.1057145", TickSize: "0.01", ContractSize: "1", LotSize: "0.01", SettlementType: "CASH",
				Delta: "0.9666", Gamma: "0.00008040", Vega: "1.7499", Theta: "-64.49", Rho: "-0.024469",
			},
			wantItems: 4,
		},
		{
			spec: "options", op: "GET /v1/account/summary",
			call:  func(ctx context.Context, c *options.Client) (any, error) { return c.AccountSummary(ctx) },
			path:  "/v1/account/summary",
			class: options.ClassReadAccount,
			wire:  &optionsaccount.AccountSummary{},
			want: &options.AccountSummary{
				AccountBalance: "12500.00", AvailableBalance: "9806.75", WithdrawableBalance: "9612.50",
				TotalEquity: "12761.75", MarginUsed: "2693.25", MaintenanceMargin: "1204.18",
				InitialMarginRatio: "0.2110", MaintenanceMarginRatio: "0.0943", PnL: "1362.80",
				UnrealizedPnL: "312.40", Fees: "142.60", Volume: "486000.00",
			},
		},
		{
			spec: "options", op: "GET /v1/account/positions",
			call:  func(ctx context.Context, c *options.Client) (any, error) { return c.Positions(ctx) },
			path:  "/v1/account/positions",
			class: options.ClassReadAccount,
			wire:  &optionsaccount.Positions{},
			want: []options.Position{
				{
					Contract: "BTC-3JUL26-62000-C", Side: options.PositionSideLong, OptionType: options.OptionTypeCall,
					Qty: "2", Value: "240", EntryPrice: "120", MarkPrice: "149.16", MarkIV: "0.3035",
					UnderlyingPrice: "61650", LiqPrice: "--", BreakEvenPrice: "62120", InitialMargin: "--",
					RealizedPnL: "0", UnrealizedPnL: "58.32", NotionalValue: "298.32",
				},
				{
					Contract: "BTC-3JUL26-60000-P", Side: options.PositionSideShort, OptionType: options.OptionTypePut,
					Qty: "-3", Value: "-45", EntryPrice: "15", MarkPrice: "12.19", MarkIV: "0.3925",
					UnderlyingPrice: "61650", LiqPrice: "--", BreakEvenPrice: "59985", InitialMargin: "22829.07",
					RealizedPnL: "0", UnrealizedPnL: "8.43", NotionalValue: "-36.57",
				},
			},
		},
		{
			spec: "options", op: "GET /v1/account/orders",
			call: func(ctx context.Context, c *options.Client) (any, error) {
				return c.RecentOrders(ctx, options.RecentOrdersQuery{
					Status: options.RecentOrdersCompleted, PageSize: 20, PageToken: "eyJ+/=",
				})
			},
			path:  "/v1/account/orders",
			query: url.Values{"status": {"completed"}, "pageSize": {"20"}, "pageToken": {"eyJ+/="}},
			class: options.ClassReadAccount,
			wire:  &optionsaccount.RecentOrders{},
			want: &options.Page[options.RecentOrder]{
				Items: []options.RecentOrder{
					{
						OrderID: "01J9G4K19776J1CTA8YXA8AA5H", Contract: "BTC-3JUL26-62000-C",
						Side: options.OrderTypeBuy, Type: options.TradeTypeLimit, FillType: options.FillTypeComplete,
						Price: "120", Quantity: "2", FilledQuantity: "2", AvgFillPrice: "110", Fees: "36.9",
						CreatedAt: 1751284800000, UpdatedAt: 1751284803120,
					},
					{
						OrderID: "01J9G51ABC7QK2DTB9ZYC9BB6J", Contract: "BTC-3JUL26-60000-P",
						Side: options.OrderTypeSell, Type: options.TradeTypeMarket, FillType: options.FillTypePartial,
						Price: "15.40", Quantity: "3", FilledQuantity: "1", AvgFillPrice: "15.75", Fees: "1.52375",
						CreatedAt: 1751288400000, UpdatedAt: 1751288460500,
					},
				},
				Next: "eyJwSyI6eyJTIjoiQlRDLTNKVUwyNi02MjAwMC1DIn19",
			},
		},
		{
			spec: "options", op: "GET /v1/account/deposits",
			call: func(ctx context.Context, c *options.Client) (any, error) {
				return c.Deposits(ctx, options.PageQuery{PageSize: 200, PageToken: "eyJ+/="})
			},
			path:  "/v1/account/deposits",
			query: url.Values{"pageSize": {"200"}, "pageToken": {"eyJ+/="}},
			class: options.ClassReadAccount,
			wire:  &optionsaccount.Deposits{},
			want: &options.Page[options.Deposit]{
				Items: []options.Deposit{{
					Market: "USDT", Value: "1000", CreatedAt: 1751284800000,
					RefChainTxID: "0x19a89550272fc44a881d5e28a3619eace550b80191ef5a803163f90024d9f702",
				}},
				Next: "eyJzSyI6eyJTIjoiT1BfREVQT1NJVCJ9fQ",
			},
		},
		{
			spec: "options", op: "GET /v1/account/withdrawals",
			call: func(ctx context.Context, c *options.Client) (any, error) {
				return c.Withdrawals(ctx, options.PageQuery{})
			},
			path:  "/v1/account/withdrawals",
			class: options.ClassReadAccount,
			wire:  &optionsaccount.Withdrawals{},
			want: &options.Page[options.Withdrawal]{
				Items: []options.Withdrawal{{
					Market: "USDT", Value: "990", Fees: "10", CreatedAt: 1751288400000,
					TxHash: "0x19a89550272fc44a881d5e28a3619eace550b80191ef5a803163f90024d9f702",
				}},
				Next: "eyJzSyI6eyJTIjoiT1BfV0lUSERSQVcifX0",
			},
		},
		{
			spec: "options-matching", op: "GET /v1/matching/orders/open",
			call: func(ctx context.Context, c *options.Client) (any, error) {
				return c.OpenOrders(ctx, options.PageQuery{PageSize: 1000})
			},
			path:  "/v1/matching/orders/open",
			query: url.Values{"pageSize": {"1000"}},
			class: options.ClassReadOrders,
			wire:  &optionsmatching.OpenOrders{},
			want: &options.Page[options.OpenOrder]{
				Items: []options.OpenOrder{{
					OrderID: orderID, Contract: contract, Side: options.OrderTypeBuy, Type: options.TradeTypeLimit,
					Price: "1250.5", Quantity: "2", FilledQuantity: "1", AvgFillPrice: "1249",
					CreatedAt: 1770000000000, UpdatedAt: 1770000001500,
				}},
			},
		},
		{
			spec: "options-matching", op: "GET /v1/matching/orders/{orderId}",
			call:  func(ctx context.Context, c *options.Client) (any, error) { return c.OrderStatus(ctx, orderID) },
			path:  "/v1/matching/orders/" + orderID,
			class: options.ClassReadOrders,
			wire:  &optionsmatching.OrderStatus{},
			want: &options.OrderStatus{
				OrderID: orderID, ClientRequestID: crid, Contract: contract,
				Status: options.StatusOpen, Action: options.ActionCreate, Body: "order accepted",
				Quantity: "2", FilledQuantity: "1", RemainingQuantity: "1", UpdatedAt: 1770000001500,
			},
		},
		{
			spec: "options-matching", op: "POST /v1/matching/orders/cancel-all",
			call: func(ctx context.Context, c *options.Client) (any, error) {
				return c.CancelAllOrders(ctx, options.CancelAllOrdersRequest{
					Contract: contract, ClientRequestID: "c0a0d2d0-6b1e-4f3a-8d2b-5e7f9a1b2c3d",
				})
			},
			path:  "/v1/matching/orders/cancel-all",
			class: options.ClassOrderCancelAll,
			wire:  &optionsmatching.CancelAllOrdersQueued{},
			want: &options.QueuedCancelAll{
				ClientRequestID: "c0a0d2d0-6b1e-4f3a-8d2b-5e7f9a1b2c3d", Contract: contract, Count: 2,
				Cancels: []options.Queued{
					{OrderID: orderID, ClientRequestID: "2b6c8e1a-0d4f-4e7b-9c3a-1f5d7e9b0a2c", Contract: contract, MessageID: "1770000000000-0"},
					{OrderID: "01JRQM8H5XYZABCDEFGHJKMNPR", ClientRequestID: "7d3e5f1b-2a4c-4b6d-8e0f-9a1b3c5d7e9f", Contract: contract, MessageID: "1770000000000-1"},
				},
			},
		},
	}

	covered := map[string]bool{}
	for _, tc := range cases {
		covered[tc.spec+" "+tc.op] = true
		t.Run(tc.op, func(t *testing.T) {
			method, path, _ := strings.Cut(tc.op, " ")
			spec := specOperation(t, tc.spec, method, path)
			e := newExchange(t, spec.status, spec.example)

			result, err := tc.call(t.Context(), e.client())
			require.NoError(t, err)

			got := e.request()
			assert.Equal(t, method, got.method)
			assert.Equal(t, tc.path, got.path)
			assert.Equal(t, "Bearer jwt", got.auth)
			if tc.query == nil {
				assert.Empty(t, got.query)
			} else {
				assert.Equal(t, tc.query, got.query)
			}
			if tc.class == "" {
				assert.Empty(t, e.pacer.classes, "the contract reads are not metered")
			} else {
				assert.Equal(t, []rails.OpClass{tc.class}, e.pacer.classes)
			}
			if spec.request != nil {
				// Every key of the spec's example with its value, and nothing else but zeros.
				spectest.AssertSends(t, flatten("", spectest.Decode(t, got.body)), flatten("", spectest.Decode(t, spec.request)))
			}

			spectest.Strict(t, spec.example, tc.wire)
			if tc.wantItems > 0 {
				assert.Equal(t, tc.want, firstOf(t, result, tc.wantItems))
			} else {
				assert.Equal(t, tc.want, result)
			}
		})
	}

	for _, op := range []string{"POST /v1/matching/orders/create", "POST /v1/matching/orders/modify", "POST /v1/matching/orders/cancel"} {
		covered["options-matching "+op] = true
	}
	// A new operation in either spec needs a case here before the SDK can claim to cover it.
	for _, spec := range []string{"options", "options-matching"} {
		for path, methods := range loadSpec(t, spec).Paths {
			for method := range methods {
				assert.True(t, covered[spec+" "+strings.ToUpper(method)+" "+path], "%s %s %s has no case", spec, method, path)
			}
		}
	}
}

// flatten keys a request body by its dotted paths, so nested objects compare key by key.
func flatten(prefix string, m map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range m {
		if nested, ok := value.(map[string]any); ok {
			for k, v := range flatten(prefix+key+".", nested) {
				out[k] = v
			}
			continue
		}
		out[prefix+key] = value
	}
	return out
}

// firstOf returns the first element of a list result after checking its length.
func firstOf(t *testing.T, result any, n int) any {
	t.Helper()
	switch list := result.(type) {
	case []options.Expiration:
		require.Len(t, list, n)
		return list[0]
	case []options.Contract:
		require.Len(t, list, n)
		return list[0]
	}
	t.Fatalf("no first element for %T", result)
	return nil
}

// After a 504 the outcome is unknown, so the error carries the minted id; the write isn't retried.
func TestWriteErrorCarriesTheClientRequestID(t *testing.T) {
	e := newExchange(t, http.StatusGatewayTimeout,
		[]byte(`{"error":{"slug":"SERVICE_UNAVAILABLE","code":"SYS-0004"},"message":"matching service timed out"}`))

	_, err := e.client().CancelAllOrders(t.Context(), options.CancelAllOrdersRequest{})
	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, rails.CodeServiceUnavailable, apiErr.Code)
	assert.Equal(t, "POST /v1/matching/orders/cancel-all", apiErr.Op)

	sent := spectest.Decode(t, e.request().body)
	assert.Equal(t, sent["clientRequestId"], apiErr.ClientRequestID)
	assert.Empty(t, e.requests, "a write is never sent twice")
}

// A sweep the queue refused part-way lists the orders it could not queue a cancel for.
func TestCancelAllOrdersUnqueued(t *testing.T) {
	e := newExchange(t, http.StatusAccepted, []byte(`{"action":"cancelAll","clientRequestId":"c0a0d2d0-6b1e-4f3a-8d2b-5e7f9a1b2c3d",`+
		`"count":2,"cancels":[{"orderId":"o1","clientRequestId":"2b6c8e1a-0d4f-4e7b-9c3a-1f5d7e9b0a2c","contract":"BTC-7FEB26-100000-C"}],`+
		`"unqueued":[{"orderId":"o2","contract":"BTC-7FEB26-100000-P"}]}`))

	got, err := e.client().CancelAllOrders(t.Context(), options.CancelAllOrdersRequest{})
	require.NoError(t, err)
	assert.Equal(t, []options.UnqueuedCancel{{OrderID: "o2", Contract: "BTC-7FEB26-100000-P"}}, got.Unqueued)
}

// Page tokens are base64 and carry characters a query string must escape.
func TestPageTokenRoundTrip(t *testing.T) {
	const token = "eyJwSyI6+/Bz=="
	e := newExchange(t, http.StatusOK,
		[]byte(`{"deposits":[],"nextPageToken":"`+token+`"}`),
		[]byte(`{"deposits":[]}`))
	client := e.client()

	first, err := client.Deposits(t.Context(), options.PageQuery{})
	require.NoError(t, err)
	assert.False(t, e.request().query.Has("pageToken"))

	last, err := client.Deposits(t.Context(), options.PageQuery{PageToken: first.Next})
	require.NoError(t, err)
	assert.Equal(t, token, e.request().query.Get("pageToken"))
	assert.Empty(t, last.Next)
}

// A page returning the token that fetched it would loop a caller forever.
func TestRepeatedPageTokenIsAnError(t *testing.T) {
	e := newExchange(t, http.StatusOK, []byte(`{"orders":[],"nextPageToken":"A"}`))
	_, err := e.client().OpenOrders(t.Context(), options.PageQuery{PageToken: "A"})
	assert.Error(t, err)
}

func TestPathParametersAreEscaped(t *testing.T) {
	e := newExchange(t, http.StatusOK, []byte(`{"status":"pending","updatedAt":1}`))
	_, err := e.client().OrderStatus(t.Context(), "my/order 1")
	require.NoError(t, err)
	assert.Equal(t, "/v1/matching/orders/my%2Forder%201", e.request().path)
}

func TestRequiredInputsAreCheckedBeforeSending(t *testing.T) {
	e := newExchange(t, http.StatusOK, []byte(`{}`))
	client := e.client()
	ctx := t.Context()

	for name, call := range map[string]func() error{
		"expirations without a market": func() error { _, err := client.Expirations(ctx, ""); return err },
		"contracts without an expiry":  func() error { _, err := client.Contracts(ctx, "BTC-USDT", ""); return err },
		"order status without an id":   func() error { _, err := client.OrderStatus(ctx, ""); return err },
		"recent orders without status": func() error {
			_, err := client.RecentOrders(ctx, options.RecentOrdersQuery{})
			return err
		},
	} {
		assert.Error(t, call(), name)
	}
	assert.Empty(t, e.requests, "nothing should be sent")
}
