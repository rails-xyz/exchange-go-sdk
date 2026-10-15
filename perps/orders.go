package perps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rails-xyz/exchange-go-sdk/internal/ws"
	"github.com/rails-xyz/exchange-go-sdk/internal/wsevent"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

const (
	opCreateOrder  = "createOrder"
	opModifyOrder  = "modifyOrder"
	opCancelOrder  = "cancelOrder"
	opUpdateMargin = "updateMargin"

	// cancelByOrderID is the only documented cancellation mode.
	cancelByOrderID = "orderId"

	// cancelAttempts bounds the resends of a cancel the exchange could not apply.
	cancelAttempts = 3
)

// CreateOrderRequest is one order to place.
type CreateOrderRequest struct {
	_ struct{}

	// Market is required on a connection to [MarketAll], filled in from the connection
	// otherwise.
	Market string
	// Price is the limit price, or for a market order the price you expect.
	Price     string
	Quantity  string
	OrderType OrderType
	TradeType TradeType
	// PostOnly rejects the order instead of letting it take liquidity.
	PostOnly bool
	// MaxSlippage bounds how far past Price a market order fills, "0" to "1" inclusive; what cannot
	// fill inside the bound is cancelled, so "0" fills only at Price or better.
	MaxSlippage string
	// ClientRequestID correlates the order with every answer about it. Minted when empty.
	ClientRequestID string
}

// CancelOrderRequest cancels one order by the id the exchange gave it.
type CancelOrderRequest struct {
	_ struct{}

	// Market is required on a connection to [MarketAll], filled in from the connection
	// otherwise.
	Market          string
	OrderID         string
	ClientRequestID string
}

// ModifyOrderRequest replaces one order's price and quantity.
type ModifyOrderRequest struct {
	_ struct{}

	// Market is required on a connection to [MarketAll], filled in from the connection
	// otherwise.
	Market          string
	OrderID         string
	Price           string
	Quantity        string
	ClientRequestID string
}

// UpdateMarginRequest sets the margin of an isolated position to an absolute amount.
type UpdateMarginRequest struct {
	_ struct{}

	// Market is required on a connection to [MarketAll], filled in from the connection
	// otherwise.
	Market          string
	Margin          string
	ClientRequestID string
}

// Ack is the exchange taking a command on. At is zero when it answered without acknowledging
// first.
type Ack struct {
	At           time.Time
	APIQuotaUsed int64
}

// OrderResult is what a command did, with the order as the exchange last described it.
type OrderResult struct {
	_ struct{}

	ClientRequestID string
	Ack             Ack
	Order           Order
	TerminalAt      time.Time
}

// MarginResult is what a margin update did. A request for the margin already in place is
// answered without an acknowledgement, so Ack.At is zero when nothing changed.
type MarginResult struct {
	_ struct{}

	ClientRequestID string
	Ack             Ack
	TerminalAt      time.Time
}

// CreateOrder places an order and waits for the exchange to accept or reject it. Nothing here
// is synchronous: the acknowledgement only says the order was taken, and the answer that
// follows can be a rejection seconds later.
//
// The order may still be live when this returns [rails.ErrTimeout] or [rails.ErrConnClosed]:
// find it with [Client.OrderByClientRequestID] rather than placing it again, since the SDK
// never resends. [rails.ErrNotSent] or a bare [context] error means it never left, and it can
// be placed again.
func (c *Conn) CreateOrder(ctx context.Context, req CreateOrderRequest) (*OrderResult, error) {
	if err := c.requireMarket(req.Market); err != nil {
		return nil, err
	}
	if req.ClientRequestID == "" {
		req.ClientRequestID = uuid.NewString()
	}
	return c.await(ctx, ClassOrderCreate, ws.Send{
		Message:         opCreateOrder,
		ClientRequestID: req.ClientRequestID,
		Market:          req.Market,
		Content: createOrderContent{
			ClientRequestID: req.ClientRequestID,
			Market:          req.Market,
			Price:           req.Price,
			Quantity:        req.Quantity,
			OrderType:       string(req.OrderType),
			TradeType:       string(req.TradeType),
			PostOnly:        req.PostOnly,
			MaxSlippage:     req.MaxSlippage,
		},
	})
}

// ModifyOrder replaces an order with one at a new price and quantity, under a new id, and
// waits for it to be on the book. A rejection leaves the original as it was; on
// [rails.ErrTimeout] or [rails.ErrConnClosed] reconcile as after [Conn.CreateOrder]. Nothing
// is ever resent.
func (c *Conn) ModifyOrder(ctx context.Context, req ModifyOrderRequest) (*OrderResult, error) {
	if err := c.requireMarket(req.Market); err != nil {
		return nil, err
	}
	if req.ClientRequestID == "" {
		req.ClientRequestID = uuid.NewString()
	}
	return c.await(ctx, ClassOrderModify, ws.Send{
		Message:         opModifyOrder,
		ClientRequestID: req.ClientRequestID,
		Market:          req.Market,
		Content: modifyOrderContent{
			ClientRequestID: req.ClientRequestID,
			Market:          req.Market,
			OrderID:         req.OrderID,
			Price:           req.Price,
			Quantity:        req.Quantity,
		},
	})
}

// UpdateMargin sets the margin of an isolated position and waits for the exchange to apply
// it. Margin is the amount the position should hold afterwards, not a change to it.
func (c *Conn) UpdateMargin(ctx context.Context, req UpdateMarginRequest) (*MarginResult, error) {
	if err := c.requireMarket(req.Market); err != nil {
		return nil, err
	}
	if req.ClientRequestID == "" {
		req.ClientRequestID = uuid.NewString()
	}
	if err := c.pace(ctx, ClassAccountWrite, opUpdateMargin); err != nil {
		return nil, err
	}
	reply, err := c.ws.Send(ctx, ws.Send{
		Message:         opUpdateMargin,
		ClientRequestID: req.ClientRequestID,
		Market:          req.Market,
		Content: updateMarginContent{
			ClientRequestID: req.ClientRequestID,
			Market:          req.Market,
			Margin:          req.Margin,
		},
	})
	c.observe(ClassAccountWrite, err)
	if err != nil {
		return nil, err
	}
	return &MarginResult{
		ClientRequestID: req.ClientRequestID,
		Ack:             Ack{At: reply.AckAt, APIQuotaUsed: reply.APIQuotaUsed},
		TerminalAt:      reply.At,
	}, nil
}

// CancelOrder cancels an order and waits for the exchange to confirm it. The same
// acknowledgement and reconciliation rules as [Conn.CreateOrder] apply.
//
// Cancelling is idempotent by order id. On [CodeCancelOrderConflictError] the cancel is sent
// again under a fresh id, up to two more times. On [CodeCancelOrderAlreadyCancelling] the call
// waits for the cancel already in flight and returns its answer, the cancelled order or its
// error, under that request's id.
func (c *Conn) CancelOrder(ctx context.Context, req CancelOrderRequest) (*OrderResult, error) {
	if err := c.requireMarket(req.Market); err != nil {
		return nil, err
	}
	crid := req.ClientRequestID
	for attempt := 1; ; attempt++ {
		if crid == "" {
			crid = uuid.NewString()
		}
		if err := c.pace(ctx, ClassOrderCancel, opCancelOrder); err != nil {
			return nil, err
		}
		// Armed before the send, so the other cancel's answer cannot slip past.
		watch, err := c.ws.Watch(ctx, ws.Watch{
			Op: opCancelOrder, ClientRequestID: crid, Match: cancelAnswerFor(req.OrderID, crid),
		})
		if err != nil {
			return nil, err
		}
		reply, err := c.ws.Send(ctx, ws.Send{
			Message:         opCancelOrder,
			ClientRequestID: crid,
			Market:          req.Market,
			Content: cancelOrderContent{
				ClientRequestID: crid,
				Market:          req.Market,
				OrderID:         req.OrderID,
				CancelBy:        cancelByOrderID,
			},
		})
		c.observe(ClassOrderCancel, err)
		var apiErr *rails.Error
		switch {
		case err == nil:
			watch.Stop()
			return orderResult(crid, reply)
		case errors.As(err, &apiErr) && apiErr.Code == CodeCancelOrderAlreadyCancelling:
			other, err := watch.Wait(ctx)
			if err != nil {
				return nil, err
			}
			other.AckAt, other.APIQuotaUsed = reply.AckAt, reply.APIQuotaUsed
			return orderResult(crid, other)
		case errors.As(err, &apiErr) && apiErr.Code == CodeCancelOrderConflictError && attempt < cancelAttempts:
			watch.Stop()
			crid = ""
		default:
			watch.Stop()
			return nil, err
		}
	}
}

// cancelAnswerFor matches the outcome of cancelling orderID under any other clientRequestId.
// Another caller being told a cancel is in flight, or could not be applied, is not an outcome.
func cancelAnswerFor(orderID, ownCRID string) func(ws.Frame) bool {
	return func(frame ws.Frame) bool {
		if frame.ResultType != opCancelOrder {
			return false
		}
		var data struct {
			ClientRequestID string         `json:"clientRequestId"`
			Error           *wsevent.Error `json:"error"`
			Order           struct {
				OrderID string `json:"orderId"`
			} `json:"order"`
		}
		if json.Unmarshal(frame.Data, &data) != nil {
			return false
		}
		if data.Error != nil && (data.Error.Code == CodeCancelOrderAlreadyCancelling ||
			data.Error.Code == CodeCancelOrderConflictError) {
			return false
		}
		return data.Order.OrderID == orderID && data.ClientRequestID != ownCRID
	}
}

// Market is omitted when empty, so a single-market connection fills it in.
type createOrderContent struct {
	ClientRequestID string `json:"clientRequestId"`
	Market          string `json:"market,omitempty"`
	Price           string `json:"price,omitempty"`
	Quantity        string `json:"quantity"`
	OrderType       string `json:"orderType"`
	TradeType       string `json:"tradeType"`
	PostOnly        bool   `json:"postOnly"`
	MaxSlippage     string `json:"maxSlippage,omitempty"`
}

type modifyOrderContent struct {
	ClientRequestID string `json:"clientRequestId"`
	Market          string `json:"market,omitempty"`
	OrderID         string `json:"orderId"`
	Price           string `json:"price"`
	Quantity        string `json:"quantity"`
}

type cancelOrderContent struct {
	ClientRequestID string `json:"clientRequestId"`
	Market          string `json:"market,omitempty"`
	OrderID         string `json:"orderId"`
	CancelBy        string `json:"cancelBy"`
}

type updateMarginContent struct {
	ClientRequestID string `json:"clientRequestId"`
	Market          string `json:"market,omitempty"`
	Margin          string `json:"margin"`
}

func (c *Conn) await(ctx context.Context, class rails.OpClass, send ws.Send) (*OrderResult, error) {
	if err := c.pace(ctx, class, send.Message); err != nil {
		return nil, err
	}
	reply, err := c.ws.Send(ctx, send)
	c.observe(class, err)
	if err != nil {
		return nil, err
	}
	return orderResult(send.ClientRequestID, reply)
}

func (c *Conn) pace(ctx context.Context, class rails.OpClass, op string) error {
	if c.pacer == nil {
		return nil
	}
	return c.pacer.Wait(ctx, class, op)
}

func (c *Conn) observe(class rails.OpClass, err error) {
	if c.pacer != nil && err != nil {
		c.pacer.Observe(class, err)
	}
}

// orderResult reads the order out of the frame that ended a command.
func orderResult(clientRequestID string, reply ws.Reply) (*OrderResult, error) {
	var data orderData
	if err := json.Unmarshal(reply.Frame.Data, &data); err != nil {
		return nil, fmt.Errorf("%s: decoding the answer: %w", reply.Frame.ResultType, err)
	}
	return &OrderResult{
		ClientRequestID: clientRequestID,
		Ack:             Ack{At: reply.AckAt, APIQuotaUsed: reply.APIQuotaUsed},
		Order:           data.Order.toOrder(reply.Frame.Market),
		TerminalAt:      reply.At,
	}, nil
}
