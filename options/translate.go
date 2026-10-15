package options

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/ws"
	"github.com/rails-xyz/exchange-go-sdk/internal/wsevent"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// Pushed resultTypes this release understands besides the command names. Anything else reaches
// the caller as [Unknown].
const (
	resultCompleteFill = "completeFillOrder"
	resultPartialFill  = "partialFillOrder"
	resultLiquidation  = "liquidation"

	resultContractUpdate    = "optionsContractUpdate"
	resultQuotesDelta       = "optionsQuotesDelta"
	resultOrderBookDelta    = "publicOrderBookDelta"
	resultPublicTradesDelta = "publicCompletedOrdersDelta"
	resultPositionsDelta    = "optionsPositionsDelta"
	resultOpenOrdersDelta   = "optionsOpenOrdersDelta"
	resultRecentOrdersDelta = "optionsRecentOrdersDelta"
)

// onFrame turns a frame into a typed event and delivers it. It runs on the connection's owner
// goroutine, so a consumer that stops reading stalls the connection rather than losing frames.
func (c *Conn) onFrame(frame ws.Frame) {
	event, resumed := c.onResumed(frame)
	if resumed && event == nil {
		return
	}
	if !resumed {
		event = toEvent(frame)
	}
	if c.pacer != nil {
		c.observe(frame.ResultType, event)
	}
	if backfill, ok := event.(SessionBackfill); ok {
		c.tel.Session(c.connID, c.scope.label(), wsevent.Backfill(backfill).Telemetry())
	}
	c.publish(event)
}

// observe shows the pacer the refusals no command of this connection was waiting for, such as
// those of commands sent over HTTP.
func (c *Conn) observe(resultType string, event Event) {
	switch e := event.(type) {
	case ServerError:
		// Names no command, so the whole account is what the exchange throttled.
		c.pacer.Observe(ClassAll, e.Err)
	case OrderRejected:
		// A refusal does not say whether the create was reduce-only; that budget is unlimited.
		class := map[string]rails.OpClass{
			opCreateOrder: ClassOrderCreate, opModifyOrder: ClassOrderModify, opCancelOrder: ClassOrderCancel,
		}[resultType]
		c.pacer.Observe(class, &rails.Error{Op: resultType, Slug: e.Slug, Code: e.Code, Message: e.Body})
	}
}

func toEvent(frame ws.Frame) Event {
	if err := frame.BareError(); err != nil {
		return ServerError{Err: err, At: time.Now()}
	}
	if data, ok := wsevent.DecodeShutdown(frame); ok {
		return ConnectionShutdown{ForceCloseIn: data.ForceCloseIn(), At: data.At()}
	}
	if data, ok := wsevent.DecodeBackfill(frame); ok {
		return SessionBackfill(data)
	}

	switch frame.ResultType {
	case resultCompleteFill, resultPartialFill:
		var data orderData
		if json.Unmarshal(frame.Data, &data) == nil && data.Order != nil {
			return OrderFilled{
				Order:   data.Order.toOrder(frame.Market),
				Partial: frame.ResultType == resultPartialFill,
			}
		}

	case opCreateOrder, opModifyOrder, opCancelOrder:
		var data orderData
		if json.Unmarshal(frame.Data, &data) != nil {
			break
		}
		if data.StatusCode >= http.StatusBadRequest {
			event := OrderRejected{Body: data.Body, ClientRequestID: data.ClientRequestID}
			if data.Order != nil {
				event.Order = data.Order.toOrder(frame.Market)
			}
			if data.Error != nil {
				event.Slug, event.Code = data.Error.Slug, data.Error.Code
			}
			return event
		}
		if frame.ResultType == opCancelOrder && data.StatusCode == http.StatusOK && data.Order != nil {
			return OrderCancelled{Order: data.Order.toOrder(frame.Market), ClientRequestID: data.ClientRequestID}
		}

	case resultLiquidation:
		var data liquidationData
		if json.Unmarshal(frame.Data, &data) == nil {
			return Liquidation{
				Contract:      frame.Market,
				LiquidationID: data.LiquidationID,
				MarginRatio:   data.MarginRatio,
				UsedMargin:    data.UsedMargin,
				TotalEquity:   data.TotalEquity,
				TriggeredAt:   data.TriggeredAt,
			}
		}
	}

	if event, ok := streamEvent(frame); ok {
		return event
	}
	return Unknown{ResultType: frame.ResultType, Raw: frame.Raw}
}

// streamEvent decodes a delta of a stream other than the orders stream.
func streamEvent(frame ws.Frame) (Event, bool) {
	switch frame.ResultType {
	case resultContractUpdate:
		var data wireContractUpdate
		if json.Unmarshal(frame.Data, &data) == nil {
			return data.toContractUpdate(), true
		}

	case resultQuotesDelta:
		var data wireQuote
		if json.Unmarshal(frame.Data, &data) == nil {
			return QuoteUpdate{Quote: data.toQuote()}, true
		}

	case resultOrderBookDelta:
		var data orderBookData
		if json.Unmarshal(frame.Data, &data) == nil {
			book := data.OrderBook.toOrderBook()
			return OrderBookDelta{
				Contract: frame.Market, Orders: book.Orders, BestBid: book.BestBid, BestAsk: book.BestAsk, Ts: book.Ts,
			}, true
		}

	case resultPublicTradesDelta:
		var data publicTradesData
		if json.Unmarshal(frame.Data, &data) == nil {
			return PublicTrades{Contract: frame.Market, Trades: convert(data.Trades, wirePublicTrade.toPublicTrade)}, true
		}

	case resultPositionsDelta:
		var data positionsData
		if json.Unmarshal(frame.Data, &data) == nil {
			return PositionsDelta{Contract: frame.Market, Positions: convert(data.Positions, wirePosition.toPosition)}, true
		}

	case resultOpenOrdersDelta:
		var data openOrdersData
		if json.Unmarshal(frame.Data, &data) == nil {
			return OpenOrdersDelta{Contract: frame.Market, Changes: convert(data.Orders, wireOpenOrder.toChange)}, true
		}

	case resultRecentOrdersDelta:
		var data recentOrdersData
		if json.Unmarshal(frame.Data, &data) == nil {
			return RecentOrdersDelta{Contract: frame.Market, Orders: convert(data.Orders, wireRecentOrder.toRecentOrder)}, true
		}
	}
	return nil, false
}

// The data objects this release decodes out of a frame, in their wire spelling. The public
// types they become carry no JSON tags.

// orderData is an order command's answer, or a fill.
type orderData struct {
	StatusCode      int            `json:"statusCode"`
	Body            string         `json:"body"`
	Order           *wireOrder     `json:"order"`
	Error           *wsevent.Error `json:"error"`
	ClientRequestID string         `json:"clientRequestId"`
}

// cancelAllData is a cancel-all's acknowledgement.
type cancelAllData struct {
	StatusCode      int    `json:"statusCode"`
	Body            string `json:"body"`
	Count           int    `json:"count"`
	ClientRequestID string `json:"clientRequestId"`
}

type liquidationData struct {
	StatusCode    int    `json:"statusCode"`
	Body          string `json:"body"`
	LiquidationID string `json:"liquidationId"`
	MarginRatio   string `json:"marginRatio"`
	UsedMargin    string `json:"usedMargin"`
	TotalEquity   string `json:"totalEquity"`
	TriggeredAt   int64  `json:"triggeredAt"`
}

// wireOrder is the order object as it arrives over the WebSocket. Leverage is always "0" on
// options, so it is decoded but not passed on.
type wireOrder struct {
	Contract        string `json:"contract"`
	OrderID         string `json:"orderId"`
	OrderType       string `json:"orderType"`
	TradeType       string `json:"tradeType"`
	Price           string `json:"price"`
	Quantity        string `json:"quantity"`
	FilledQuantity  string `json:"filledQuantity"`
	AvgFillPrice    string `json:"avgFillPrice"`
	Leverage        string `json:"leverage"`
	OrderStatus     string `json:"orderStatus"`
	PostOnly        bool   `json:"postOnly"`
	CreatedAt       int64  `json:"createdAt"`
	UpdatedAt       int64  `json:"updatedAt"`
	ClientRequestID string `json:"clientRequestId"`
}

// toOrder fills in the contract from the frame when the order object omits it.
func (w *wireOrder) toOrder(contract string) Order {
	if w.Contract != "" {
		contract = w.Contract
	}
	return Order{
		Contract:        contract,
		OrderID:         w.OrderID,
		OrderType:       OrderType(w.OrderType),
		TradeType:       TradeType(w.TradeType),
		Price:           w.Price,
		Quantity:        w.Quantity,
		FilledQuantity:  w.FilledQuantity,
		AvgFillPrice:    w.AvgFillPrice,
		Status:          Status(w.OrderStatus),
		PostOnly:        w.PostOnly,
		CreatedAt:       w.CreatedAt,
		UpdatedAt:       w.UpdatedAt,
		ClientRequestID: w.ClientRequestID,
	}
}
