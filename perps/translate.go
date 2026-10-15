package perps

import (
	"encoding/json"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/ws"
	"github.com/rails-xyz/exchange-go-sdk/internal/wsevent"
)

// Wire result types this release understands. Anything else reaches the caller as [Unknown].
const (
	resultCompleteFill   = "completeFillOrder"
	resultPartialFill    = "partialFillOrder"
	resultRejectOrder    = "rejectOrder"
	resultTrades         = "completedOrdersDelta"
	resultPublicTrades   = "publicCompletedOrdersDelta"
	resultOrderBookDelta = "publicOrderBookDelta"
)

// onFrame turns a frame into a typed event and delivers it. It runs on the connection's owner
// goroutine, so a consumer that stops reading stalls the connection rather than losing frames.
func (c *Conn) onFrame(frame ws.Frame) {
	event := toEvent(frame)
	if failure, ok := event.(ServerError); ok && c.pacer != nil {
		// Names no command, so the whole account is what the exchange throttled.
		c.pacer.Observe(ClassAll, failure.Err)
	}
	if backfill, ok := event.(SessionBackfill); ok {
		c.tel.Session(c.connID, c.market, wsevent.Backfill(backfill).Telemetry())
	}
	c.publish(event)
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
		var data notificationData
		if json.Unmarshal(frame.Data, &data) == nil {
			return OrderFilled{
				Order:   data.Order.toOrder(frame.Market),
				Partial: frame.ResultType == resultPartialFill,
			}
		}

	case resultRejectOrder:
		var data notificationData
		if json.Unmarshal(frame.Data, &data) == nil {
			event := OrderRejected{
				Order:           data.Order.toOrder(frame.Market),
				Body:            data.Body,
				ClientRequestID: data.ClientRequestID,
			}
			if data.Error != nil {
				event.Slug, event.Code = data.Error.Slug, data.Error.Code
			}
			return event
		}

	case resultTrades:
		var data []wireTrade
		if json.Unmarshal(frame.Data, &data) == nil {
			return Trades{Trades: tradesFromWire(data)}
		}

	case resultPublicTrades:
		var data publicTradesData
		if json.Unmarshal(frame.Data, &data) == nil {
			return PublicTrades{Market: frame.Market, Trades: publicTradesFromWire(data.Orders)}
		}

	case resultOrderBookDelta:
		var data orderBookData
		if json.Unmarshal(frame.Data, &data) == nil {
			return OrderBookDelta{Market: frame.Market, Orders: levelsFromWire(data.Orders), Ts: data.Ts}
		}
	}

	return Unknown{ResultType: frame.ResultType, Raw: frame.Raw}
}

// The data objects this release decodes out of a frame, in their wire spelling. The public
// types they become carry no JSON tags.

// orderData is what a command's answer carries besides the routing fields the connection reads.
type orderData struct {
	Order wireOrder `json:"order"`
}

// notificationData is a fill or a rejection pushed on the orders stream.
type notificationData struct {
	StatusCode      int            `json:"statusCode"`
	Body            string         `json:"body"`
	Order           wireOrder      `json:"order"`
	Error           *wsevent.Error `json:"error"`
	ClientRequestID string         `json:"clientRequestId"`
}

// wireOrder is the order object as it arrives over the WebSocket. The HTTP hosts describe the
// same thing with generated types; nothing generates Go from the WebSocket spec.
type wireOrder struct {
	Market          string `json:"market"`
	OrderID         string `json:"orderId"`
	OrderType       string `json:"orderType"`
	TradeType       string `json:"tradeType"`
	Price           string `json:"price"`
	Quantity        string `json:"quantity"`
	FilledQuantity  string `json:"filledQuantity"`
	Leverage        string `json:"leverage"`
	CreatedAt       int64  `json:"createdAt"`
	UpdatedAt       int64  `json:"updatedAt"`
	OrderStatus     string `json:"orderStatus"`
	ClientRequestID string `json:"clientRequestId"`
	ReduceOnly      bool   `json:"reduceOnly"`
	PostOnly        bool   `json:"postOnly"`
	AvgFillPrice    string `json:"avgFillPrice"`
}

// toOrder fills in the market from the frame when the order object omits it.
func (w wireOrder) toOrder(market string) Order {
	if w.Market == "" {
		w.Market = market
	}
	return Order{
		Market:          w.Market,
		OrderID:         w.OrderID,
		OrderType:       OrderType(w.OrderType),
		TradeType:       TradeType(w.TradeType),
		Price:           w.Price,
		Quantity:        w.Quantity,
		FilledQuantity:  w.FilledQuantity,
		Leverage:        w.Leverage,
		CreatedAt:       w.CreatedAt,
		UpdatedAt:       w.UpdatedAt,
		OrderStatus:     OrderStatus(w.OrderStatus),
		ClientRequestID: w.ClientRequestID,
		ReduceOnly:      w.ReduceOnly,
		PostOnly:        w.PostOnly,
		AvgFillPrice:    w.AvgFillPrice,
	}
}

// wireTrade is one entry of the trades stream's batch.
type wireTrade struct {
	Market           string `json:"market"`
	OrderID          string `json:"orderId"`
	MatchID          string `json:"matchId"`
	OrderType        string `json:"orderType"`
	TradeType        string `json:"tradeType"`
	ExecutionType    string `json:"executionType"`
	FillType         string `json:"fillType"`
	Price            string `json:"price"`
	Quantity         string `json:"quantity"`
	OriginalQuantity string `json:"originalQuantity"`
	Leverage         string `json:"leverage"`
	Fees             string `json:"fees"`
	CreatedAt        int64  `json:"createdAt"`
	UpdatedAt        int64  `json:"updatedAt"`
	TriggerType      string `json:"triggerType"`
	TriggerPrice     string `json:"triggerPrice"`
	TriggerCreatedAt int64  `json:"triggerCreatedAt"`
}

func tradesFromWire(wire []wireTrade) []Trade {
	trades := make([]Trade, len(wire))
	for i, w := range wire {
		trades[i] = Trade{
			Market:           w.Market,
			OrderID:          w.OrderID,
			MatchID:          w.MatchID,
			OrderType:        OrderType(w.OrderType),
			TradeType:        TradeType(w.TradeType),
			ExecutionType:    ExecutionType(w.ExecutionType),
			FillType:         FillType(w.FillType),
			Price:            w.Price,
			Quantity:         w.Quantity,
			OriginalQuantity: w.OriginalQuantity,
			Leverage:         w.Leverage,
			Fees:             w.Fees,
			CreatedAt:        w.CreatedAt,
			UpdatedAt:        w.UpdatedAt,
			TriggerType:      TriggerType(w.TriggerType),
			TriggerPrice:     w.TriggerPrice,
			TriggerCreatedAt: w.TriggerCreatedAt,
		}
	}
	return trades
}

// publicTradesData is a publicTrades delta or snapshot.
type publicTradesData struct {
	StatusCode      int               `json:"statusCode"`
	ClientRequestID string            `json:"clientRequestId"`
	Orders          []wirePublicTrade `json:"orders"`
}

type wirePublicTrade struct {
	ExecutionType string `json:"executionType"`
	MatchID       string `json:"matchId"`
	OrderType     string `json:"orderType"`
	Price         string `json:"price"`
	Quantity      string `json:"quantity"`
	UpdatedAt     int64  `json:"updatedAt"`
}

func publicTradesFromWire(wire []wirePublicTrade) []PublicTrade {
	trades := make([]PublicTrade, len(wire))
	for i, w := range wire {
		trades[i] = PublicTrade{
			ExecutionType: ExecutionType(w.ExecutionType),
			MatchID:       w.MatchID,
			OrderType:     OrderType(w.OrderType),
			Price:         w.Price,
			Quantity:      w.Quantity,
			UpdatedAt:     w.UpdatedAt,
		}
	}
	return trades
}

// orderBookData is a publicOrderBook delta or snapshot.
type orderBookData struct {
	StatusCode      int         `json:"statusCode"`
	ClientRequestID string      `json:"clientRequestId"`
	Orders          []wireLevel `json:"orders"`
	Ts              int64       `json:"ts"`
}

type wireLevel struct {
	OrderType string `json:"orderType"`
	Price     string `json:"price"`
	Quantity  string `json:"quantity"`
}

func levelsFromWire(wire []wireLevel) []OrderBookLevel {
	levels := make([]OrderBookLevel, len(wire))
	for i, w := range wire {
		levels[i] = OrderBookLevel{OrderType: OrderType(w.OrderType), Price: w.Price, Quantity: w.Quantity}
	}
	return levels
}
