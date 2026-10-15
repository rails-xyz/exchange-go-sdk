package perps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/rails-xyz/exchange-go-sdk/internal/ws"
)

const (
	opSubscribe           = "subscribe"
	opUnsubscribe         = "unsubscribe"
	opEmitPublicTrades    = "emitPublicCompletedOrders"
	opEmitPublicOrderBook = "emitPublicOrderBook"

	resultSubscribed           = "subscribed"
	resultUnsubscribed         = "unsubscribed"
	resultPublicTradesSnapshot = "publicCompletedOrders"
	resultOrderBookSnapshot    = "publicOrderBook"
)

// Subscribe adds streams to the connection and returns every stream it now receives; one
// already active is a no-op. The set survives reconnects. Over the connection's limit the error
// carries [rails.CodeSubscriptionLimit].
func (c *Conn) Subscribe(ctx context.Context, subscriptions ...Subscription) ([]Subscription, error) {
	return c.subscription(ctx, opSubscribe, resultSubscribed, subscriptions)
}

// Unsubscribe removes streams from the connection and returns every stream it still receives;
// one not active is a no-op. The orders stream cannot be removed: every command is answered
// on it.
func (c *Conn) Unsubscribe(ctx context.Context, subscriptions ...Subscription) ([]Subscription, error) {
	if slices.Contains(subscriptions, SubscriptionOrders) {
		return nil, errors.New("perps: the orders stream cannot be removed")
	}
	return c.subscription(ctx, opUnsubscribe, resultUnsubscribed, subscriptions)
}

func (c *Conn) subscription(ctx context.Context, op, resultType string, subscriptions []Subscription) ([]Subscription, error) {
	id := uuid.NewString()
	reply, err := c.ws.Send(ctx, ws.Send{
		Message:         op,
		ClientRequestID: id,
		// An error is answered under the command's own name.
		ReplyTypes: []string{resultType, op},
		Content:    subscriptionContent{ClientRequestID: id, Subscriptions: subscriptions},
	})
	if err != nil {
		return nil, err
	}
	var data subscriptionData
	if err := json.Unmarshal(reply.Frame.Data, &data); err != nil {
		return nil, fmt.Errorf("%s: decoding the answer: %w", op, err)
	}
	c.subscriptions.Store(&data.ActiveSubscriptions)
	return data.ActiveSubscriptions, nil
}

// EmitPublicTrades asks for the market's recent fills, the state a stream of [PublicTrades]
// continues from. Market is required on a connection to [MarketAll], filled in from the
// connection otherwise.
func (c *Conn) EmitPublicTrades(ctx context.Context, market string) ([]PublicTrade, error) {
	reply, err := c.emit(ctx, opEmitPublicTrades, resultPublicTradesSnapshot, market)
	if err != nil {
		return nil, err
	}
	var data publicTradesData
	if err := json.Unmarshal(reply.Frame.Data, &data); err != nil {
		return nil, fmt.Errorf("%s: decoding the answer: %w", opEmitPublicTrades, err)
	}
	return publicTradesFromWire(data.Orders), nil
}

// EmitPublicOrderBook asks for the market's whole book, the state a stream of [OrderBookDelta]
// continues from. Market is required on a connection to [MarketAll], filled in from the
// connection otherwise.
func (c *Conn) EmitPublicOrderBook(ctx context.Context, market string) (*OrderBookSnapshot, error) {
	reply, err := c.emit(ctx, opEmitPublicOrderBook, resultOrderBookSnapshot, market)
	if err != nil {
		return nil, err
	}
	var data orderBookData
	if err := json.Unmarshal(reply.Frame.Data, &data); err != nil {
		return nil, fmt.Errorf("%s: decoding the answer: %w", opEmitPublicOrderBook, err)
	}
	return &OrderBookSnapshot{Orders: levelsFromWire(data.Orders), Ts: data.Ts}, nil
}

func (c *Conn) emit(ctx context.Context, op, resultType, market string) (ws.Reply, error) {
	if err := c.requireMarket(market); err != nil {
		return ws.Reply{}, err
	}
	id := uuid.NewString()
	return c.ws.Send(ctx, ws.Send{
		Message:         op,
		ClientRequestID: id,
		Market:          market,
		ReplyTypes:      []string{resultType, op},
		Content:         emitContent{ClientRequestID: id, Market: market},
	})
}

type subscriptionContent struct {
	ClientRequestID string         `json:"clientRequestId"`
	Subscriptions   []Subscription `json:"subscriptions"`
}

type subscriptionData struct {
	StatusCode               int            `json:"statusCode"`
	ClientRequestID          string         `json:"clientRequestId"`
	ActiveSubscriptions      []Subscription `json:"activeSubscriptions"`
	RequestedSubscriptions   []Subscription `json:"requestedSubscriptions"`
	RequestedUnsubscriptions []Subscription `json:"requestedUnsubscriptions"`
}

type emitContent struct {
	ClientRequestID string `json:"clientRequestId"`
	Market          string `json:"market,omitempty"`
}
