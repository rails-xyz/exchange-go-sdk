package perps_test

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"testing"

	"github.com/rails-xyz/exchange-go-sdk/perps"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// specExample is the spec's 200 example for a GET route, so the fixtures are the exchange's own.
func specExample(t *testing.T, spec, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile("../api/" + spec + ".json")
	require.NoError(t, err)
	var doc struct {
		Paths map[string]struct {
			Get struct {
				Responses map[string]struct {
					Ref     string `json:"$ref"`
					Content map[string]struct {
						Example json.RawMessage `json:"example"`
					} `json:"content"`
				} `json:"responses"`
			} `json:"get"`
		} `json:"paths"`
	}
	require.NoError(t, json.Unmarshal(raw, &doc))
	ok := doc.Paths[path].Get.Responses["200"]
	require.Empty(t, ok.Ref, "%s %s: the 200 is a $ref, resolve it here", spec, path)
	example := ok.Content["application/json"].Example
	require.NotEmpty(t, example, "%s %s has no 200 example", spec, path)
	return example
}

func serveExample(t *testing.T, spec, path string) (*exchange, *atomic.Pointer[http.Request]) {
	t.Helper()
	var got atomic.Pointer[http.Request]
	example := specExample(t, spec, path)
	e := newExchange(t, func(w http.ResponseWriter, r *http.Request) {
		got.Store(r)
		_, _ = w.Write(example)
	})
	return e, &got
}

// Every field, because each mapping is mechanical assignments between two structs whose fields
// are mostly strings: a swap the compiler cannot see.
func TestBalancesMapsEveryField(t *testing.T) {
	e, got := serveExample(t, "account", "/api/v1/balances")

	balances, err := e.client().Balances(t.Context(), "BTC-USDT")
	require.NoError(t, err)
	assert.Equal(t, "/api/v1/balances", got.Load().URL.Path)
	assert.Equal(t, "BTC-USDT", got.Load().URL.Query().Get("market"))

	eth := perps.MarketBalance{
		Position: "28894.648055999998", PositionQuantity: "8", PositionPnL: "406.2397599999995",
		PositionFunding: "-486.82005787000037", PositionFees: "12.10884451", UnrealizedPnL: "189.54",
		OpenBuy: "0", OpenBuyQty: "0", OpenSell: "8000", OpenSellQty: "3",
		Margin: "762320.7471059860003337", MaintenanceMargin: "28902.648056", Leverage: "5",
		MarginMode: perps.MarginModeIsolated, LiquidationPrice: "28902.648056", LiquidationFees: "0",
		MarkPrice: "3635.098",
		PnL:       "406.23975999", Funding: "-486.82005787000037", Fees: "43.54509196", Volume: "43545.091964",
	}
	want := &perps.Balances{
		TotalBalance: "99996952.15429778", CrossMarginBalance: "99234631.40719179",
		AvailableBalance: "99521353.92161779", WithdrawableBalance: "92673870.38",
		Deposit: "100000000", Withdraw: "879.12", WithdrawFees: "8.88", Leverage: "5",
		PnL: "-1292.78457349", Funding: "-259.04082851", Fees: "608.02030021", Volume: "608020.300274",
		Markets: map[string]perps.MarketBalance{"BTC-USDT": balances.Markets["BTC-USDT"], "ETH-USDT": eth},
	}
	assert.Equal(t, want, balances)
	assert.Equal(t, perps.MarginModeCross, balances.Markets["BTC-USDT"].MarginMode)
}

func TestBalancesForEveryMarketOmitsTheParameter(t *testing.T) {
	e, got := serveExample(t, "account", "/api/v1/balances")
	_, err := e.client().Balances(t.Context(), perps.MarketAll)
	require.NoError(t, err)
	assert.False(t, got.Load().URL.Query().Has("market"))
}

func TestCompletedOrdersPagesAndMapsEveryField(t *testing.T) {
	e, got := serveExample(t, "order", "/api/v1/completed-orders")

	page, err := e.client().CompletedOrders(t.Context(), perps.CompletedOrdersQuery{
		PageSize: 50, StartDate: 1747000000000, EndDate: 1748000000000, Cursor: "BTC-USDT#FEES#a@1",
	})
	require.NoError(t, err)

	query := got.Load().URL.Query()
	assert.Equal(t, "/api/v1/completed-orders", got.Load().URL.Path)
	assert.Equal(t, "50", query.Get("pageSize"))
	assert.Equal(t, "1747000000000", query.Get("startDate"))
	assert.Equal(t, "1748000000000", query.Get("endDate"))
	assert.Equal(t, "BTC-USDT#FEES#a@1", query.Get("start"))

	require.Len(t, page.Items, 2)
	assert.Equal(t, perps.CompletedOrder{
		Market: "ETH-USDT", MatchID: "a59141f0-9c13-4ec8-bc37-beceb7fb284a", OrderID: "01JP39KFBF8SHJAT3ERHPF3YSP",
		OrderType: perps.OrderTypeBuy, TradeType: perps.TradeTypeLimit, Price: "1949", Quantity: "273.78842139",
		ExecutionType: perps.ExecutionTypeMaker, FillType: perps.FillTypeComplete, Fees: "533.61363328",
		FilledAt: 1747426816421, TransactionHash: "335a50808317116f2fc1147277bcc226e0e52dfea7b15899d66add371d2ab499",
		TriggerType: perps.TriggerTypeNone, TriggerPrice: "0",
	}, page.Items[0])
	assert.Equal(t, perps.ExecutionTypeTaker, page.Items[1].ExecutionType)
	assert.Equal(t, "BTC-USDT#FEES#01JP390Z8WVR9EP6X1P187P722#01JP36QTCRJCGN7NC0CPZ22NAB@1741719371497", page.Next)
}

// An empty query sends nothing, so the exchange's own defaults apply.
func TestCompletedOrdersDefaultsAreTheExchanges(t *testing.T) {
	e, got := serveExample(t, "order", "/api/v1/completed-orders")
	_, err := e.client().CompletedOrders(t.Context(), perps.CompletedOrdersQuery{})
	require.NoError(t, err)
	assert.Empty(t, got.Load().URL.RawQuery)
}

func TestLedgerReadsMapEveryFieldAndReplayTheCursor(t *testing.T) {
	const cursor = "USDT#DEPOSIT#0xabc#1"

	t.Run("deposits", func(t *testing.T) {
		e, got := serveExample(t, "account", "/api/v1/deposits")
		page, err := e.client().Deposits(t.Context(), cursor)
		require.NoError(t, err)
		assert.Equal(t, cursor, got.Load().URL.Query().Get("start"))
		assert.Equal(t, []perps.Deposit{{
			Market: "USDT", Value: "1000", TxCreated: 1717466763457,
			RefChainTxID: "0x19a89550272fc44a881d5e28a3619eace550b80191ef5a803163f90024d9f702",
		}}, page.Items)
		assert.Equal(t, "USDT#DEPOSIT#0x19a89550272fc44a881d5e28a3619eace550b80191ef5a803163f90024d9f702#1717466763457", page.Next)
	})

	t.Run("withdrawals", func(t *testing.T) {
		e, got := serveExample(t, "account", "/api/v1/withdrawals")
		page, err := e.client().Withdrawals(t.Context(), cursor)
		require.NoError(t, err)
		assert.Equal(t, cursor, got.Load().URL.Query().Get("start"))
		assert.Equal(t, []perps.Withdrawal{{
			Market: "USDT", Value: "990.0", Fees: "10.0", TxCreated: 1717466763457,
			TxHash: "0x19a89550272fc44a881d5e28a3619eace550b80191ef5a803163f90024d9f702",
		}}, page.Items)
		assert.Equal(t, "USDT#WITHDRAW#0x19a89550272fc44a881d5e28a3619eace550b80191ef5a803163f90024d9f702#1717466763457", page.Next)
	})

	t.Run("fundings", func(t *testing.T) {
		e, got := serveExample(t, "account", "/api/v1/fundings")
		page, err := e.client().Fundings(t.Context(), "ETH-USDT", cursor)
		require.NoError(t, err)
		assert.Equal(t, cursor, got.Load().URL.Query().Get("start"))
		assert.Equal(t, "ETH-USDT", got.Load().URL.Query().Get("market"))
		assert.Equal(t, []perps.Funding{{
			Market: "ETH-USDT", Value: "0.42747", Rate: "0.0001125", IndexPrice: "3799.73421126312",
			PositionSize: "-1", TxCreated: 1716921945759,
		}}, page.Items)
		assert.Equal(t, "ETH-USDT#FUNDING#01J4MD3R7N4HH4G76PT47VG3QC#1716921945759", page.Next)
	})
}

func serveExampleBody(t *testing.T, body string) (*exchange, *atomic.Pointer[http.Request]) {
	t.Helper()
	var got atomic.Pointer[http.Request]
	e := newExchange(t, func(w http.ResponseWriter, r *http.Request) {
		got.Store(r)
		_, _ = w.Write([]byte(body))
	})
	return e, &got
}

func TestAccountWritesPostJSON(t *testing.T) {
	var gotPath string
	var gotBody []byte
	e := newExchange(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{"message":"updated"}`))
	})
	client := e.client()

	require.NoError(t, client.UpdateLeverage(t.Context(), "BTC-USDT", "5"))
	assert.Equal(t, "/api/v1/update-leverage", gotPath)
	assert.JSONEq(t, `{"market":"BTC-USDT","leverage":"5"}`, string(gotBody))

	require.NoError(t, client.UpdateMarginMode(t.Context(), "BTC-USDT", perps.MarginModeIsolated))
	assert.Equal(t, "/api/v1/update-margin-mode", gotPath)
	assert.JSONEq(t, `{"market":"BTC-USDT","marginMode":"I"}`, string(gotBody))
}

func TestCancelAllOrdersSendsTheClientRequestID(t *testing.T) {
	e, got := serveExampleBody(t, `{"message":"Cancelling 1 order"}`)
	require.NoError(t, e.client().CancelAllOrders(t.Context(), perps.CancelAllOrdersRequest{
		Market: "BTC-USDT", ClientRequestID: "0835bf3e-4a20-41f0-908d-8213f2b7a285",
	}))
	assert.Equal(t, "BTC-USDT", got.Load().URL.Query().Get("market"))
	assert.Equal(t, "0835bf3e-4a20-41f0-908d-8213f2b7a285", got.Load().URL.Query().Get("clientRequestId"))
}

func TestMarketDataReadsMapEveryField(t *testing.T) {
	cases := map[string]struct {
		call func(t *testing.T, c *perps.Client) (any, error)
		want any
	}{
		"/api/v1/markets": {
			call: func(t *testing.T, c *perps.Client) (any, error) { return c.Markets(t.Context()) },
			want: []string{"BTC-USDT", "ETH-USDT"},
		},
		"/api/v1/product-specifications": {
			call: func(t *testing.T, c *perps.Client) (any, error) { return c.ProductSpecifications(t.Context()) },
			want: []perps.ProductSpecification{
				{Market: "BTC-USDT", TickSize: "0.01", LotSize: "0.0000001", FundingIntervalHours: 1, MinOrderSizeUSD: "1", MaxOrderSizeUSD: "", ContractType: "Vanilla"},
				{Market: "ETH-USDT", TickSize: "0.01", LotSize: "0.0000001", FundingIntervalHours: 1, MinOrderSizeUSD: "1", MaxOrderSizeUSD: "", ContractType: "Vanilla"},
			},
		},
		"/api/v1/contract-specifications": {
			call: func(t *testing.T, c *perps.Client) (any, error) {
				specs, err := c.ContractSpecifications(t.Context())
				if err != nil {
					return nil, err
				}
				return specs[0], nil
			},
			want: perps.ContractSpecification{
				ContractName: "BTC-USDT", UnderlyingPrice: "63568.7382843144",
				PriceIndex: "CCIX (CoinDesk Aggregated Index, formerly CCCAGG)", IndexUpdateFrequency: "1 second",
				ContractType: "Linear (USDT-margined)", MarginType: "Isolated, Cross",
				TradingHours: "24/7 (except during scheduled maintenance)", FundingTimes: "Hourly funding",
				FundingRateCap: "±0.5% per 1-hour interval", TickSize: "0.01", LotSize: "0.0000001",
				MinOrderNotional: "1 USDT", PriceUnit: "USDT per BTC", QuantityUnit: "BTC",
			},
		},
		"/api/v1/market-data": {
			call: func(t *testing.T, c *perps.Client) (any, error) {
				points, err := c.MarketData(t.Context(), "BTC-USDT", "ETH-USDT")
				if err != nil {
					return nil, err
				}
				return points[:2], nil
			},
			want: []perps.MarketDataPoint{
				{Market: "BTC-USDT", Type: perps.MarketDataTypeIndex, Index: "CCIX", Value: "58400.1937255873", UpdatedAt: 1726171141994},
				{Market: "BTC-USDT", Type: perps.MarketDataTypeMark, Index: "RAILS", Value: "58193.018", UpdatedAt: 1726174865750},
			},
		},
		"/api/v1/order-book": {
			call: func(t *testing.T, c *perps.Client) (any, error) { return c.OrderBook(t.Context(), "ETH-USDT") },
			want: &perps.OrderBookSnapshot{
				Orders: []perps.OrderBookLevel{
					{OrderType: perps.OrderTypeBuy, Price: "4355.1", Quantity: "0.1"},
					{OrderType: perps.OrderTypeSell, Price: "4355.2", Quantity: "0.2"},
				},
				Ts: 1772576154169,
			},
		},
	}
	for path, tc := range cases {
		t.Run(path, func(t *testing.T) {
			e, got := serveExample(t, "market", path)
			result, err := tc.call(t, e.client())
			require.NoError(t, err)
			assert.Equal(t, path, got.Load().URL.Path)
			assert.Equal(t, tc.want, result)
		})
	}
}

func TestMarketDataJoinsTheMarkets(t *testing.T) {
	e, got := serveExampleBody(t, `[]`)
	_, err := e.client().MarketData(t.Context(), "BTC-USDT", "ETH-USDT")
	require.NoError(t, err)
	assert.Equal(t, "BTC-USDT,ETH-USDT", got.Load().URL.Query().Get("markets"))

	_, err = e.client().MarketData(t.Context())
	assert.Error(t, err)
}
