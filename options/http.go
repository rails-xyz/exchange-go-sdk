package options

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/google/uuid"
	"github.com/rails-xyz/exchange-go-sdk/internal/wire/optionsaccount"
	"github.com/rails-xyz/exchange-go-sdk/internal/wire/optionsmatching"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// Expirations lists the expiry dates trading on a market, such as BTC-USDT.
func (c *Client) Expirations(ctx context.Context, market string) ([]Expiration, error) {
	const op = "GET /v1/contracts/markets/{market}/expirations"

	if market == "" {
		return nil, fmt.Errorf("%s: market is required", op)
	}
	path := "/v1/contracts/markets/" + url.PathEscape(market) + "/expirations"
	var resp optionsaccount.Expirations
	if err := c.http.Get(ctx, op, "", c.cfg.Environment.OptionsURL, path, nil, &resp); err != nil {
		return nil, err
	}
	return expirationsResponseToExpirations(resp), nil
}

// Contracts lists the contracts tradable on a market for one expiry date, an
// [Expiration.ExpiryDate] (YYYY-MM-DD).
func (c *Client) Contracts(ctx context.Context, market, expiry string) ([]Contract, error) {
	const op = "GET /v1/contracts/markets/{market}/expirations/{expiry}/contracts"

	if market == "" || expiry == "" {
		return nil, fmt.Errorf("%s: market and expiry are required", op)
	}
	path := "/v1/contracts/markets/" + url.PathEscape(market) + "/expirations/" + url.PathEscape(expiry) + "/contracts"
	var resp optionsaccount.Contracts
	if err := c.http.Get(ctx, op, "", c.cfg.Environment.OptionsURL, path, nil, &resp); err != nil {
		return nil, err
	}
	return contractsResponseToContracts(resp), nil
}

// AccountSummary returns the account's balance sheet.
func (c *Client) AccountSummary(ctx context.Context) (*AccountSummary, error) {
	const op = "GET /v1/account/summary"

	var resp optionsaccount.AccountSummary
	if err := c.http.Get(ctx, op, ClassReadAccount, c.cfg.Environment.OptionsURL, "/v1/account/summary", nil, &resp); err != nil {
		return nil, err
	}
	return accountSummaryResponseToSummary(resp), nil
}

// Positions returns every open position.
func (c *Client) Positions(ctx context.Context) ([]Position, error) {
	const op = "GET /v1/account/positions"

	var resp optionsaccount.Positions
	if err := c.http.Get(ctx, op, ClassReadAccount, c.cfg.Environment.OptionsURL, "/v1/account/positions", nil, &resp); err != nil {
		return nil, err
	}
	return positionsResponseToPositions(resp), nil
}

// PageQuery selects one page of a paginated read. Both fields are optional: PageSize zero
// takes the exchange's default, and PageToken is the Next of the previous page, empty for the
// first.
type PageQuery struct {
	_ struct{}

	PageSize  int
	PageToken string
}

func (q PageQuery) values() url.Values {
	query := url.Values{}
	if q.PageSize != 0 {
		query.Set("pageSize", strconv.Itoa(q.PageSize))
	}
	if q.PageToken != "" {
		query.Set("pageToken", q.PageToken)
	}
	return query
}

// RecentOrdersStatus picks which history [Client.RecentOrders] reads.
type RecentOrdersStatus string

const (
	// RecentOrdersCompleted is the orders that filled, in whole or in part.
	RecentOrdersCompleted RecentOrdersStatus = "completed"
	// RecentOrdersCancelled is the orders that were cancelled.
	RecentOrdersCancelled RecentOrdersStatus = "cancelled"
)

// RecentOrdersQuery selects one page of [Client.RecentOrders]. Status is required; PageSize
// and PageToken are as in [PageQuery].
type RecentOrdersQuery struct {
	_ struct{}

	Status    RecentOrdersStatus
	PageSize  int
	PageToken string
}

// RecentOrders returns one page of the account's completed or cancelled orders, newest first.
// A page may hold fewer than PageSize orders and still not be the last; only an empty Next ends
// the history.
func (c *Client) RecentOrders(ctx context.Context, q RecentOrdersQuery) (*Page[RecentOrder], error) {
	const op = "GET /v1/account/orders"

	if q.Status == "" {
		return nil, fmt.Errorf("%s: status is required", op)
	}
	query := PageQuery{PageSize: q.PageSize, PageToken: q.PageToken}.values()
	query.Set("status", string(q.Status))
	var resp optionsaccount.RecentOrders
	if err := c.http.Get(ctx, op, ClassReadAccount, c.cfg.Environment.OptionsURL, "/v1/account/orders", query, &resp); err != nil {
		return nil, err
	}
	page := recentOrdersResponseToPage(resp)
	return page, repeatedToken(op, q.PageToken, page.Next)
}

// Deposits returns one page of the account's deposits, newest first.
func (c *Client) Deposits(ctx context.Context, q PageQuery) (*Page[Deposit], error) {
	const op = "GET /v1/account/deposits"

	var resp optionsaccount.Deposits
	if err := c.http.Get(ctx, op, ClassReadAccount, c.cfg.Environment.OptionsURL, "/v1/account/deposits", q.values(), &resp); err != nil {
		return nil, err
	}
	page := depositsResponseToPage(resp)
	return page, repeatedToken(op, q.PageToken, page.Next)
}

// Withdrawals returns one page of the account's withdrawals, newest first.
func (c *Client) Withdrawals(ctx context.Context, q PageQuery) (*Page[Withdrawal], error) {
	const op = "GET /v1/account/withdrawals"

	var resp optionsaccount.Withdrawals
	if err := c.http.Get(ctx, op, ClassReadAccount, c.cfg.Environment.OptionsURL, "/v1/account/withdrawals", q.values(), &resp); err != nil {
		return nil, err
	}
	page := withdrawalsResponseToPage(resp)
	return page, repeatedToken(op, q.PageToken, page.Next)
}

// OpenOrders returns one page of the orders resting on the book, newest first. A page may hold
// fewer than PageSize orders and still not be the last; only an empty Next ends the list.
func (c *Client) OpenOrders(ctx context.Context, q PageQuery) (*Page[OpenOrder], error) {
	const op = "GET /v1/matching/orders/open"

	var resp optionsmatching.OpenOrders
	if err := c.http.Get(ctx, op, ClassReadOrders, c.cfg.Environment.OptionsURL, "/v1/matching/orders/open", q.values(), &resp); err != nil {
		return nil, err
	}
	page := openOrdersResponseToPage(resp)
	return page, repeatedToken(op, q.PageToken, page.Next)
}

// A page handing back the token that fetched it would have a caller walking the list read the
// same page forever.
func repeatedToken(op, sent, next string) error {
	if next != "" && next == sent {
		return fmt.Errorf("%s: the exchange repeated a pagination token", op)
	}
	return nil
}

// OrderStatus looks up what the exchange has recorded about an order, by the OrderID a queued
// request returned. The exchange forgets an order an hour after its last update and answers
// [rails.ErrNotFound].
func (c *Client) OrderStatus(ctx context.Context, orderID string) (*OrderStatus, error) {
	const op = "GET /v1/matching/orders/{orderId}"

	if orderID == "" {
		return nil, fmt.Errorf("%s: order id is required", op)
	}
	var resp optionsmatching.OrderStatus
	if err := c.http.Get(ctx, op, ClassReadOrders, c.cfg.Environment.OptionsURL, "/v1/matching/orders/"+url.PathEscape(orderID), nil, &resp); err != nil {
		return nil, err
	}
	return orderStatusResponseToStatus(resp), nil
}

// createOrder, cancelOrder and cancelAllOrders carry a [Conn] command its socket could not,
// with the ids it chose, so the outcome on the order stream answers to them. They are unpaced:
// the command was paced before it reached the socket.
func (c *Client) createOrder(ctx context.Context, req CreateOrderRequest, crid, orderID string) (Queued, error) {
	const op = "POST /v1/matching/orders/create"

	body := optionsmatching.CreateOptionsOrderJSONRequestBody{
		ClientRequestId: &crid,
		Order: optionsmatching.CreateOrderPayload{
			Contract:    req.Contract,
			OrderType:   optionsmatching.CreateOrderPayloadOrderType(req.OrderType),
			TradeType:   optionsmatching.CreateOrderPayloadTradeType(req.TradeType),
			Price:       req.Price,
			Quantity:    req.Quantity,
			PostOnly:    &req.PostOnly,
			ReduceOnly:  &req.ReduceOnly,
			MaxSlippage: optional(req.MaxSlippage),
			OrderId:     &orderID,
		},
	}
	var resp optionsmatching.CreateOrderQueued
	if err := c.http.Post(ctx, op, "", c.cfg.Environment.OptionsURL, "/v1/matching/orders/create", body, &resp); err != nil {
		return Queued{}, withClientRequestID(err, crid)
	}
	return queuedResponseToQueued(optionsmatching.CancelOrderQueued(resp)), nil
}

func (c *Client) cancelOrder(ctx context.Context, req CancelOrderRequest, crid string) (Queued, error) {
	const op = "POST /v1/matching/orders/cancel"

	body := optionsmatching.CancelOptionsOrderJSONRequestBody{
		ClientRequestId: &crid,
		Cancel:          optionsmatching.CancelTarget{Contract: req.Contract, OrderId: req.OrderID},
	}
	var resp optionsmatching.CancelOrderQueued
	if err := c.http.Post(ctx, op, "", c.cfg.Environment.OptionsURL, "/v1/matching/orders/cancel", body, &resp); err != nil {
		return Queued{}, withClientRequestID(err, crid)
	}
	return queuedResponseToQueued(resp), nil
}

// CancelAllOrdersRequest cancels every open order on one contract, or when Contract is empty on
// every contract: every contract of the account over HTTP, every contract of the connection's
// scope over the WebSocket. ClientRequestID is minted when empty.
type CancelAllOrdersRequest struct {
	_ struct{}

	Contract        string
	ClientRequestID string
}

// CancelAllOrders queues one cancel per open order and returns once they are queued, not
// applied: each cancel's outcome arrives on the order stream of the options WebSocket, and
// [Client.OrderStatus] looks it up by the cancel's OrderID.
//
// It is never sent twice. A [*rails.Error] it returns carries the request's ClientRequestID;
// after a 502 or a 504 the outcome is unknown, so check [Client.OpenOrders] before sending it
// again.
func (c *Client) CancelAllOrders(ctx context.Context, req CancelAllOrdersRequest) (*QueuedCancelAll, error) {
	return c.sweep(ctx, req, ClassOrderCancelAll)
}

func (c *Client) cancelAllOrders(ctx context.Context, req CancelAllOrdersRequest) (*QueuedCancelAll, error) {
	return c.sweep(ctx, req, "")
}

func (c *Client) sweep(ctx context.Context, req CancelAllOrdersRequest, class rails.OpClass) (*QueuedCancelAll, error) {
	const op = "POST /v1/matching/orders/cancel-all"

	crid := clientRequestID(req.ClientRequestID)
	body := optionsmatching.CancelAllOptionsOrdersJSONRequestBody{
		ClientRequestId: &crid,
		Contract:        optional(req.Contract),
	}
	var resp optionsmatching.CancelAllOrdersQueued
	if err := c.http.Post(ctx, op, class, c.cfg.Environment.OptionsURL, "/v1/matching/orders/cancel-all", body, &resp); err != nil {
		return nil, withClientRequestID(err, crid)
	}
	return cancelAllResponseToQueued(resp), nil
}

func clientRequestID(given string) string {
	if given != "" {
		return given
	}
	return uuid.NewString()
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func withClientRequestID(err error, crid string) error {
	var apiErr *rails.Error
	if errors.As(err, &apiErr) {
		apiErr.ClientRequestID = crid
	}
	return err
}
