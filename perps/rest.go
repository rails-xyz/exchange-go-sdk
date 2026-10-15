package perps

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/rails-xyz/exchange-go-sdk/internal/wire/account"
	marketdata "github.com/rails-xyz/exchange-go-sdk/internal/wire/market"
	"github.com/rails-xyz/exchange-go-sdk/internal/wire/order"
)

// OpenOrders returns every order still working on a market, or on all of them for [MarketAll],
// following the cursor until the exchange stops handing one back.
func (c *Client) OpenOrders(ctx context.Context, market string) ([]Order, error) {
	const op = "GET /api/v1/open-orders"

	if market == "" {
		return nil, fmt.Errorf("%s: market is required; pass perps.MarketAll for every market", op)
	}

	var (
		orders []Order
		cursor string
		seen   = map[string]bool{}
	)
	for {
		query := url.Values{}
		if market != MarketAll {
			query.Set("market", market)
		}
		if cursor != "" {
			query.Set("start", cursor)
		}

		var page order.OpenOrdersResponse
		if err := c.rest.Get(ctx, op, ClassReadOrders, c.cfg.Environment.OrderURL, "/api/v1/open-orders", query, &page); err != nil {
			return nil, err
		}
		for _, wire := range page.OpenOrders {
			orders = append(orders, restOrderToOrder(wire))
		}

		next := ""
		if page.Last != nil {
			next = *page.Last
		}
		if next == "" {
			return orders, nil
		}
		// A cursor that comes round again means the page just collected is a repeat, so the
		// result would be both duplicated and short. Say so rather than return it.
		if seen[next] {
			return nil, fmt.Errorf("%s: the exchange repeated a pagination cursor", op)
		}
		seen[next] = true
		cursor = next
	}
}

// Positions returns the account's open positions on a market, or on all of them for
// [MarketAll]. A market with no position is absent from the result, and a market with a position
// yields exactly one entry, so the result for a single market has zero or one element.
func (c *Client) Positions(ctx context.Context, market string) ([]Position, error) {
	const op = "GET /api/v1/positions"

	if market == "" {
		return nil, fmt.Errorf("%s: market is required; pass perps.MarketAll for every market", op)
	}

	query := url.Values{}
	if market != MarketAll {
		query.Set("market", market)
	}
	var resp account.PositionsResponse
	if err := c.rest.Get(ctx, op, ClassReadAccount, c.cfg.Environment.AccountURL, "/api/v1/positions", query, &resp); err != nil {
		return nil, err
	}
	return positionsResponseToPositions(resp), nil
}

// OrderByID looks an order up by the id the exchange assigned it. Unlike [Client.OpenOrders]
// it finds orders in a terminal state too, which is what makes it the way to reconcile after a
// timeout.
func (c *Client) OrderByID(ctx context.Context, market, orderID string) (*Order, error) {
	return c.orderBy(ctx, market, "order-id", orderID)
}

// OrderByClientRequestID looks an order up by the clientRequestId that created it, which is
// the only handle a caller has when a create timed out before any id came back.
func (c *Client) OrderByClientRequestID(ctx context.Context, market, clientRequestID string) (*Order, error) {
	return c.orderBy(ctx, market, "client-request-id", clientRequestID)
}

func (c *Client) orderBy(ctx context.Context, market, param, value string) (*Order, error) {
	const op = "GET /api/v1/order"

	if market == "" {
		return nil, fmt.Errorf("%s: market is required", op)
	}
	if value == "" {
		return nil, fmt.Errorf("%s: %s is required", op, param)
	}

	query := url.Values{"market": {market}, param: {value}}
	var found order.GetOrderByIdResponse
	if err := c.rest.Get(ctx, op, ClassReadOrders, c.cfg.Environment.OrderURL, "/api/v1/order", query, &found); err != nil {
		return nil, err
	}
	result := restOrderToOrder(found.Order)
	return &result, nil
}

// CancelAllOrdersRequest cancels every working order on a market, or on all of them for
// [MarketAll]. ClientRequestID is an optional correlation id of your own.
type CancelAllOrdersRequest struct {
	_ struct{}

	Market          string
	ClientRequestID string
}

// CancelAllOrders cancels every working order on a market, or on all of them for [MarketAll].
// It stays reachable during a soft ban, which is when it matters most, and is never paced.
func (c *Client) CancelAllOrders(ctx context.Context, req CancelAllOrdersRequest) error {
	const op = "DELETE /api/v1/orders"

	if req.Market == "" {
		return fmt.Errorf("%s: market is required; pass perps.MarketAll to cancel everywhere", op)
	}
	market := req.Market
	if market == MarketAll {
		market = "all" // this route spells it in lower case
	}
	query := url.Values{"market": {market}}
	if req.ClientRequestID != "" {
		query.Set("clientRequestId", req.ClientRequestID)
	}
	return c.rest.Delete(ctx, op, c.cfg.Environment.OrderURL, "/api/v1/orders", query, nil)
}

// CompletedOrdersQuery selects fills for [Client.CompletedOrders]. Every field is optional:
// PageSize is 20 to 200, StartDate and EndDate are epoch milliseconds, Cursor a page's Next.
type CompletedOrdersQuery struct {
	_ struct{}

	PageSize  int
	StartDate int64
	EndDate   int64
	Cursor    string
}

// CompletedOrders returns the account's fills across every market, newest first, one page at
// a time.
func (c *Client) CompletedOrders(ctx context.Context, q CompletedOrdersQuery) (*Page[CompletedOrder], error) {
	const op = "GET /api/v1/completed-orders"

	query := url.Values{}
	if q.PageSize != 0 {
		query.Set("pageSize", strconv.Itoa(q.PageSize))
	}
	if q.StartDate != 0 {
		query.Set("startDate", strconv.FormatInt(q.StartDate, 10))
	}
	if q.EndDate != 0 {
		query.Set("endDate", strconv.FormatInt(q.EndDate, 10))
	}
	if q.Cursor != "" {
		query.Set("start", q.Cursor)
	}
	var resp order.CompletedOrdersResponse
	if err := c.rest.Get(ctx, op, ClassReadOrders, c.cfg.Environment.OrderURL, "/api/v1/completed-orders", query, &resp); err != nil {
		return nil, err
	}
	return completedOrdersResponseToPage(resp), nil
}

// UpdateLeverage sets the leverage used for new positions on a market. The exchange refuses
// the change while the market has open orders or a position ([CodeUpdateLeverageNotAllowed]).
func (c *Client) UpdateLeverage(ctx context.Context, market, leverage string) error {
	const op = "POST /api/v1/update-leverage"

	if market == "" || leverage == "" {
		return fmt.Errorf("%s: market and leverage are required", op)
	}
	body := order.UpdateLeverageJSONRequestBody{Market: market, Leverage: leverage}
	return c.rest.Post(ctx, op, ClassAccountWrite, c.cfg.Environment.OrderURL, "/api/v1/update-leverage", body, nil)
}

// UpdateMarginMode switches a market between cross and isolated margin. The exchange refuses
// the change while the market has open orders or a position ([CodeUpdateMarginModeMarketInUse]).
func (c *Client) UpdateMarginMode(ctx context.Context, market string, mode MarginMode) error {
	const op = "POST /api/v1/update-margin-mode"

	if market == "" || mode == "" {
		return fmt.Errorf("%s: market and margin mode are required", op)
	}
	body := order.UpdateMarginModeJSONRequestBody{
		Market:     market,
		MarginMode: order.UpdateMarginModeJSONBodyMarginMode(mode),
	}
	return c.rest.Post(ctx, op, ClassAccountWrite, c.cfg.Environment.OrderURL, "/api/v1/update-margin-mode", body, nil)
}

// Balances returns the account's balances, with the per-market breakdown limited to one market
// or covering all of them for [MarketAll].
func (c *Client) Balances(ctx context.Context, market string) (*Balances, error) {
	const op = "GET /api/v1/balances"

	if market == "" {
		return nil, fmt.Errorf("%s: market is required; pass perps.MarketAll for every market", op)
	}
	query := url.Values{}
	if market != MarketAll {
		query.Set("market", market)
	}
	var resp account.BalancesResponse
	if err := c.rest.Get(ctx, op, ClassReadAccount, c.cfg.Environment.AccountURL, "/api/v1/balances", query, &resp); err != nil {
		return nil, err
	}
	return balancesResponseToBalances(resp), nil
}

// Deposits returns one page of the account's deposits; cursor is the Next of the previous
// page, empty for the first.
func (c *Client) Deposits(ctx context.Context, cursor string) (*Page[Deposit], error) {
	const op = "GET /api/v1/deposits"

	var resp account.DepositsResponse
	if err := c.rest.Get(ctx, op, ClassReadLedger, c.cfg.Environment.AccountURL, "/api/v1/deposits", cursorQuery(cursor), &resp); err != nil {
		return nil, err
	}
	return depositsResponseToPage(resp), nil
}

// Withdrawals returns one page of the account's withdrawals; cursor is the Next of the
// previous page, empty for the first.
func (c *Client) Withdrawals(ctx context.Context, cursor string) (*Page[Withdrawal], error) {
	const op = "GET /api/v1/withdrawals"

	var resp account.WithdrawalsResponse
	if err := c.rest.Get(ctx, op, ClassReadLedger, c.cfg.Environment.AccountURL, "/api/v1/withdrawals", cursorQuery(cursor), &resp); err != nil {
		return nil, err
	}
	return withdrawalsResponseToPage(resp), nil
}

// Fundings returns one page of the funding payments on a market, or on all of them for
// [MarketAll]; cursor is the Next of the previous page, empty for the first.
func (c *Client) Fundings(ctx context.Context, market, cursor string) (*Page[Funding], error) {
	const op = "GET /api/v1/fundings"

	if market == "" {
		return nil, fmt.Errorf("%s: market is required; pass perps.MarketAll for every market", op)
	}
	query := cursorQuery(cursor)
	if market != MarketAll {
		query.Set("market", market)
	}
	var resp account.FundingsResponse
	if err := c.rest.Get(ctx, op, ClassReadAccount, c.cfg.Environment.AccountURL, "/api/v1/fundings", query, &resp); err != nil {
		return nil, err
	}
	return fundingsResponseToPage(resp), nil
}

// Cursors are opaque and carry characters a query string cannot, so they are escaped rather
// than pasted back.
func cursorQuery(cursor string) url.Values {
	query := url.Values{}
	if cursor != "" {
		query.Set("start", cursor)
	}
	return query
}

// Markets lists every market the exchange trades.
func (c *Client) Markets(ctx context.Context) ([]string, error) {
	var resp marketdata.SupportedMarkets
	if err := c.rest.Get(ctx, "GET /api/v1/markets", "", c.cfg.Environment.MarketDataURL, "/api/v1/markets", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Markets, nil
}

// ProductSpecifications returns every market's trading rules.
func (c *Client) ProductSpecifications(ctx context.Context) ([]ProductSpecification, error) {
	var resp []marketdata.ProductSpecification
	if err := c.rest.Get(ctx, "GET /api/v1/product-specifications", "", c.cfg.Environment.MarketDataURL, "/api/v1/product-specifications", nil, &resp); err != nil {
		return nil, err
	}
	specs := make([]ProductSpecification, 0, len(resp))
	for _, w := range resp {
		specs = append(specs, restProductSpecificationToSpecification(w))
	}
	return specs, nil
}

// ContractSpecifications returns every contract's description.
func (c *Client) ContractSpecifications(ctx context.Context) ([]ContractSpecification, error) {
	var resp []marketdata.ContractSpecification
	if err := c.rest.Get(ctx, "GET /api/v1/contract-specifications", "", c.cfg.Environment.MarketDataURL, "/api/v1/contract-specifications", nil, &resp); err != nil {
		return nil, err
	}
	specs := make([]ContractSpecification, 0, len(resp))
	for _, w := range resp {
		specs = append(specs, restContractSpecificationToSpecification(w))
	}
	return specs, nil
}

// MarketData returns the current figures (index, mark, bid, ask, funding, open interest, 24h
// volume and count) for the markets named.
func (c *Client) MarketData(ctx context.Context, markets ...string) ([]MarketDataPoint, error) {
	const op = "GET /api/v1/market-data"

	if len(markets) == 0 {
		return nil, fmt.Errorf("%s: at least one market is required", op)
	}
	query := url.Values{"markets": {strings.Join(markets, ",")}}
	var resp []marketdata.MarketDataPoint
	if err := c.rest.Get(ctx, op, "", c.cfg.Environment.MarketDataURL, "/api/v1/market-data", query, &resp); err != nil {
		return nil, err
	}
	points := make([]MarketDataPoint, 0, len(resp))
	for _, w := range resp {
		points = append(points, restMarketDataPointToPoint(w))
	}
	return points, nil
}

// OrderBook returns the aggregated book of a market.
func (c *Client) OrderBook(ctx context.Context, market string) (*OrderBookSnapshot, error) {
	const op = "GET /api/v1/order-book"

	if market == "" {
		return nil, fmt.Errorf("%s: market is required", op)
	}
	query := url.Values{"market": {market}}
	var resp marketdata.OrderBookSnapshot
	if err := c.rest.Get(ctx, op, "", c.cfg.Environment.MarketDataURL, "/api/v1/order-book", query, &resp); err != nil {
		return nil, err
	}
	return restOrderBookToSnapshot(resp), nil
}
