package options

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"github.com/rails-xyz/exchange-go-sdk/internal/ws"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// WebSocket command names, which are also the resultTypes of their answers.
const (
	opCreateOrder     = "createOrder"
	opModifyOrder     = "modifyOrder"
	opCancelOrder     = "cancelOrder"
	opCancelAllOrders = "cancelAllOrders"

	// cancelByOrderID is the only documented cancellation mode.
	cancelByOrderID = "orderId"
)

// OrderResult is how the exchange answered an order command over the WebSocket, with the order
// as it then stood. AckAt is when the exchange acknowledged the command, zero when it answered
// without acknowledging first, as it does every cancel.
//
// ViaHTTP is a command the socket could not carry, sent over HTTP instead, where it is only
// queued: Order holds just its OrderID and Contract, AckAt and TerminalAt are zero, and the
// outcome arrives on the orders stream as for a command another connection sent.
type OrderResult struct {
	_ struct{}

	ClientRequestID string
	Order           Order
	AckAt           time.Time
	TerminalAt      time.Time
	ViaHTTP         bool
}

// CancelAllResult is the exchange taking a cancel-all on. Count is the number of cancels it
// dispatched; each one's outcome arrives as its own event carrying ClientRequestID.
//
// ViaHTTP is a sweep sent over HTTP because the socket could not carry it. Each cancel's event
// then carries that cancel's own ClientRequestID, as listed in Cancels, and the orders in
// Unqueued got no cancel and are still open.
type CancelAllResult struct {
	_ struct{}

	ClientRequestID string
	Count           int
	ViaHTTP         bool
	Cancels         []Queued
	Unqueued        []UnqueuedCancel
}

// httpOrders is the HTTP side of the order commands, for those a [Conn] cannot send.
type httpOrders interface {
	createOrder(ctx context.Context, req CreateOrderRequest, crid, orderID string) (Queued, error)
	cancelOrder(ctx context.Context, req CancelOrderRequest, crid string) (Queued, error)
	cancelAllOrders(ctx context.Context, req CancelAllOrdersRequest) (*QueuedCancelAll, error)
}

// CreateOrderRequest places an order on a contract. MaxSlippage applies to a market order only:
// a fraction from 0 to 1 bounding how far past Price it fills, the rest being cancelled, so 0
// fills only at Price or better; the exchange defaults it. OrderID is an optional ULID of your
// own for the order and ClientRequestID one for the request; each is minted when empty.
type CreateOrderRequest struct {
	_ struct{}

	Contract        string
	OrderType       OrderType
	TradeType       TradeType
	Price           string // the limit price, or for a market order the price expected
	Quantity        string
	PostOnly        bool
	ReduceOnly      bool
	MaxSlippage     string
	OrderID         string
	ClientRequestID string
}

// ModifyOrderRequest changes a resting order's price and quantity; everything else about the
// order is kept, including its OrderID. ClientRequestID is minted when empty.
type ModifyOrderRequest struct {
	_ struct{}

	Contract        string
	OrderID         string
	Price           string
	Quantity        string
	ClientRequestID string
}

// CancelOrderRequest cancels one order. ClientRequestID is minted when empty.
type CancelOrderRequest struct {
	_ struct{}

	Contract        string
	OrderID         string
	ClientRequestID string
}

// CreateOrder places an order and waits for the exchange's answer: the order resting, filled on
// arrival, or rejected. A 200 does not always mean resting, so read Order.Status: a market
// order that finds no liquidity is answered with the order cancelled. A create with ReduceOnly
// draws on [ClassOrderReduceOnly] rather than [ClassOrderCreate].
//
// The order may be live when this returns [rails.ErrTimeout] or [rails.ErrConnClosed], or a 502
// or 504 after it went over HTTP, and the SDK never resends. Look for the OrderID the [*rails.Error] carries in [Client.OpenOrders] and
// both [Client.RecentOrders] histories, since it may already have filled or been cancelled,
// before placing it again. [ErrNotSubscribed], [ErrOutOfScope], [rails.ErrNotSent] or a bare
// [context] error mean it never left.
//
// A ReduceOnly create, which closes a position, goes over HTTP when the socket is down
// ([OrderResult.ViaHTTP]).
func (c *Conn) CreateOrder(ctx context.Context, req CreateOrderRequest) (*OrderResult, error) {
	if err := c.guardContract(opCreateOrder, req.Contract); err != nil {
		return nil, err
	}
	crid := clientRequestID(req.ClientRequestID)
	orderID := req.OrderID
	if orderID == "" {
		orderID = ulid.Make().String()
	}
	reply, err := c.send(ctx, createClass(req.ReduceOnly), ws.Send{
		Message:         opCreateOrder,
		ClientRequestID: crid,
		ReplyTypes:      []string{opCreateOrder, resultPartialFill, resultCompleteFill},
		Market:          req.Contract,
		Content: createOrderContent{
			ClientRequestID: crid,
			OrderID:         orderID,
			Market:          req.Contract,
			Price:           req.Price,
			Quantity:        req.Quantity,
			OrderType:       string(req.OrderType),
			TradeType:       string(req.TradeType),
			PostOnly:        req.PostOnly,
			ReduceOnly:      req.ReduceOnly,
			MaxSlippage:     req.MaxSlippage,
		},
	})
	if err == nil {
		return orderResult(crid, reply)
	}
	if errors.Is(err, rails.ErrTimeout) || errors.Is(err, rails.ErrConnClosed) {
		return nil, withOrderID(err, orderID)
	}
	// Only a create that never left goes over HTTP: one that may have could close twice.
	if !req.ReduceOnly || !ws.IsDown(err) || ctx.Err() != nil {
		return nil, err
	}
	queued, httpErr := c.overHTTP.createOrder(ctx, req, crid, orderID)
	if httpErr != nil {
		return nil, c.fallbackError(createClass(req.ReduceOnly), err, createOutcome(httpErr, crid, orderID))
	}
	return queuedResult(queued, crid), nil
}

// ModifyOrder changes a resting order's price and quantity in place, keeping its OrderID, and
// waits for the exchange's answer; a new price that crosses the book is answered with the fill.
// A rejection leaves the order as it was. On [rails.ErrTimeout] or [rails.ErrConnClosed]
// reconcile as after [Conn.CreateOrder]: nothing is ever resent.
func (c *Conn) ModifyOrder(ctx context.Context, req ModifyOrderRequest) (*OrderResult, error) {
	if err := c.guardContract(opModifyOrder, req.Contract); err != nil {
		return nil, err
	}
	crid := clientRequestID(req.ClientRequestID)
	reply, err := c.send(ctx, ClassOrderModify, ws.Send{
		Message:         opModifyOrder,
		ClientRequestID: crid,
		ReplyTypes:      []string{opModifyOrder, resultPartialFill, resultCompleteFill},
		Market:          req.Contract,
		Content: modifyOrderContent{
			ClientRequestID: crid,
			Market:          req.Contract,
			OrderID:         req.OrderID,
			Price:           req.Price,
			Quantity:        req.Quantity,
		},
	})
	if err != nil {
		return nil, err
	}
	return orderResult(crid, reply)
}

// CancelOrder cancels an order and waits for the exchange's answer, which comes without an
// acknowledgement. An order already gone is refused with [CodeOrderNotFound].
//
// Cancelling is idempotent by order id, so a cancel the socket could not carry, or lost the
// answer to with [rails.ErrConnClosed], goes over HTTP instead ([OrderResult.ViaHTTP]), under a
// fresh ClientRequestID when the first one may have reached the exchange.
func (c *Conn) CancelOrder(ctx context.Context, req CancelOrderRequest) (*OrderResult, error) {
	if err := c.guardContract(opCancelOrder, req.Contract); err != nil {
		return nil, err
	}
	crid := clientRequestID(req.ClientRequestID)
	reply, err := c.send(ctx, ClassOrderCancel, ws.Send{
		Message:         opCancelOrder,
		ClientRequestID: crid,
		Market:          req.Contract,
		Content: cancelOrderContent{
			ClientRequestID: crid,
			Market:          req.Contract,
			OrderID:         req.OrderID,
			CancelBy:        cancelByOrderID,
		},
	})
	if err == nil {
		return orderResult(crid, reply)
	}
	crid, ok := cancelOverHTTP(ctx, err, crid)
	if !ok {
		return nil, err
	}
	queued, httpErr := c.overHTTP.cancelOrder(ctx, req, crid)
	if httpErr != nil {
		return nil, c.fallbackError(ClassOrderCancel, err, httpErr)
	}
	return queuedResult(queued, crid), nil
}

// CancelAllOrders cancels every resting order on req.Contract, or on every contract of the
// connection's scope when it is empty, and returns once the exchange has taken the sweep on.
//
// Each cancel is answered separately, as an [OrderCancelled] or [OrderRejected] event carrying
// the result's ClientRequestID, and those events can arrive before this returns as well as
// after. Count says how many to expect.
//
// A sweep the socket could not carry goes over HTTP instead, as for [Conn.CancelOrder], when it
// names a contract or the scope is [ScopeAll]: over HTTP an empty Contract sweeps the whole
// account, which a narrower scope must not.
func (c *Conn) CancelAllOrders(ctx context.Context, req CancelAllOrdersRequest) (*CancelAllResult, error) {
	if err := c.guard(opCancelAllOrders, req.Contract); err != nil {
		return nil, err
	}
	crid := clientRequestID(req.ClientRequestID)
	reply, err := c.send(ctx, ClassOrderCancelAll, ws.Send{
		Message:         opCancelAllOrders,
		ClientRequestID: crid,
		AckIsAnswer:     true,
		Market:          req.Contract,
		Content:         cancelAllContent{ClientRequestID: crid, Market: req.Contract},
	})
	if err != nil {
		retryID, ok := cancelOverHTTP(ctx, err, crid)
		if !ok || (req.Contract == "" && c.scope.Market != marketAll) {
			return nil, err
		}
		req.ClientRequestID = retryID
		queued, httpErr := c.overHTTP.cancelAllOrders(ctx, req)
		if httpErr != nil {
			return nil, c.fallbackError(ClassOrderCancelAll, err, httpErr)
		}
		return &CancelAllResult{
			ClientRequestID: queued.ClientRequestID,
			Count:           len(queued.Cancels),
			ViaHTTP:         true,
			Cancels:         queued.Cancels,
			Unqueued:        queued.Unqueued,
		}, nil
	}
	var data cancelAllData
	if err := json.Unmarshal(reply.Frame.Data, &data); err != nil {
		return nil, fmt.Errorf("%s: decoding the answer: %w", opCancelAllOrders, err)
	}
	return &CancelAllResult{ClientRequestID: crid, Count: data.Count}, nil
}

type createOrderContent struct {
	ClientRequestID string `json:"clientRequestId"`
	OrderID         string `json:"orderId"`
	Market          string `json:"market"`
	Price           string `json:"price"`
	Quantity        string `json:"quantity"`
	OrderType       string `json:"orderType"`
	TradeType       string `json:"tradeType"`
	PostOnly        bool   `json:"postOnly"`
	ReduceOnly      bool   `json:"reduceOnly"`
	MaxSlippage     string `json:"maxSlippage,omitempty"`
}

type modifyOrderContent struct {
	ClientRequestID string `json:"clientRequestId"`
	Market          string `json:"market"`
	OrderID         string `json:"orderId"`
	Price           string `json:"price"`
	Quantity        string `json:"quantity"`
}

type cancelOrderContent struct {
	ClientRequestID string `json:"clientRequestId"`
	Market          string `json:"market"`
	OrderID         string `json:"orderId"`
	CancelBy        string `json:"cancelBy"`
}

// Market is omitted when empty, so the exchange sweeps the connection's scope.
type cancelAllContent struct {
	ClientRequestID string `json:"clientRequestId"`
	Market          string `json:"market,omitempty"`
}

// send paces a command, sends it and reports the outcome to the pacer.
func (c *Conn) send(ctx context.Context, class rails.OpClass, s ws.Send) (ws.Reply, error) {
	if c.pacer != nil {
		if err := c.pacer.Wait(ctx, class, s.Message); err != nil {
			return ws.Reply{}, err
		}
	}
	reply, err := c.ws.Send(ctx, s)
	if c.pacer != nil && err != nil {
		c.pacer.Observe(class, err)
	}
	return reply, err
}

// cancelOverHTTP says whether a cancel the socket failed may go over HTTP, and under which
// clientRequestId: its own when it never left, a fresh one when it may have reached the exchange.
func cancelOverHTTP(ctx context.Context, err error, crid string) (string, bool) {
	switch {
	case ctx.Err() != nil:
		return "", false
	case ws.IsDown(err):
		return crid, true
	case errors.Is(err, rails.ErrConnClosed):
		return uuid.NewString(), true
	}
	return "", false
}

// fallbackError shows the pacer the HTTP failure and returns it, which is what the caller acts
// on, with the WebSocket one for context.
func (c *Conn) fallbackError(class rails.OpClass, wsErr, httpErr error) error {
	if c.pacer != nil {
		c.pacer.Observe(class, httpErr)
	}
	return fmt.Errorf("%w (sent over HTTP after: %v)", httpErr, wsErr)
}

// createOutcome is an HTTP create's failure as the caller reconciles it: a refusal as the
// exchange gave it, and anything that may have been queued with the order to look up.
func createOutcome(err error, crid, orderID string) error {
	var apiErr *rails.Error
	if errors.As(err, &apiErr) && apiErr.StatusCode != 0 {
		if apiErr.StatusCode == http.StatusBadGateway || apiErr.StatusCode == http.StatusGatewayTimeout {
			return withOrderID(err, orderID)
		}
		return err
	}
	return fmt.Errorf("%w: %w", rails.ErrConnClosed, &rails.Error{
		Op:              "POST /v1/matching/orders/create",
		Message:         err.Error(),
		ClientRequestID: crid,
		OrderID:         orderID,
	})
}

// withOrderID names the order on a copy of err's [*rails.Error]: hooks may still be reading the
// original.
func withOrderID(err error, orderID string) error {
	var apiErr *rails.Error
	if !errors.As(err, &apiErr) {
		return err
	}
	named := *apiErr
	named.OrderID = orderID
	return orderIDError{err: err, named: &named}
}

// orderIDError is err with its [*rails.Error] replaced by named for [errors.As], and its
// sentinels kept for [errors.Is].
type orderIDError struct {
	err   error
	named *rails.Error
}

func (e orderIDError) Error() string   { return e.err.Error() }
func (e orderIDError) Unwrap() []error { return []error{e.named, e.err} }

func queuedResult(q Queued, crid string) *OrderResult {
	return &OrderResult{
		ClientRequestID: crid,
		Order:           Order{Contract: q.Contract, OrderID: q.OrderID, ClientRequestID: crid},
		ViaHTTP:         true,
	}
}

// orderResult reads the order out of the frame that ended a command.
func orderResult(clientRequestID string, reply ws.Reply) (*OrderResult, error) {
	var data orderData
	if err := json.Unmarshal(reply.Frame.Data, &data); err != nil {
		return nil, fmt.Errorf("%s: decoding the answer: %w", reply.Frame.ResultType, err)
	}
	if data.Order == nil {
		return nil, fmt.Errorf("%s: the answer carries no order", reply.Frame.ResultType)
	}
	return &OrderResult{
		ClientRequestID: clientRequestID,
		Order:           data.Order.toOrder(reply.Frame.Market),
		AckAt:           reply.AckAt,
		TerminalAt:      reply.At,
	}, nil
}
