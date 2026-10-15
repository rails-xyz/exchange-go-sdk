package perps

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rails-xyz/exchange-go-sdk/internal/telemetry"
	"github.com/rails-xyz/exchange-go-sdk/internal/ws"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// Subscription is a stream to receive on the connection. orders carries command answers,
// [OrderFilled] and [OrderRejected]; trades [Trades]; publicTrades [PublicTrades];
// publicOrderBook [OrderBookDelta].
type Subscription string

const (
	SubscriptionOrders          Subscription = "orders"
	SubscriptionTrades          Subscription = "trades"
	SubscriptionPublicTrades    Subscription = "publicTrades"
	SubscriptionPublicOrderBook Subscription = "publicOrderBook"
)

const defaultEventBuffer = 1024

type connConfig struct {
	subscriptions []Subscription
	sessionID     string
	pingInterval  time.Duration
	eventBuffer   int
}

// ConnOption configures a connection at the point it is opened. The session is fixed for its
// lifetime; the subscriptions are the starting set.
type ConnOption func(*connConfig)

// WithSubscriptions chooses the streams to receive from the start; [Conn.Subscribe] adds more
// later. Defaults to orders, which is always included: without it no order command is ever
// answered.
func WithSubscriptions(subscriptions ...Subscription) ConnOption {
	return func(c *connConfig) { c.subscriptions = subscriptions }
}

// WithSessionID reuses a session across reconnects so missed order events are backfilled.
// Defaults to a fresh UUID per connection, which is what makes reconnects lossless.
func WithSessionID(sessionID string) ConnOption {
	return func(c *connConfig) { c.sessionID = sessionID }
}

// WithPingInterval sets how often the application-level ping goes out; zero disables it.
// Defaults to 20 seconds. Each ping spends one message unit of the account's rate limit.
func WithPingInterval(d time.Duration) ConnOption {
	return func(c *connConfig) { c.pingInterval = d }
}

// WithEventBuffer sizes the channel [Conn.Events] returns. Defaults to 1024.
func WithEventBuffer(n int) ConnOption {
	return func(c *connConfig) { c.eventBuffer = n }
}

// Conn is a WebSocket connection to one market, or to every market with [MarketAll]. It
// reconnects on its own; a caller sees that only as events. Its methods are safe for concurrent
// use: commands from any number of goroutines share the connection and each is answered by its
// own clientRequestId.
type Conn struct {
	ws     *ws.Conn
	tel    *telemetry.Collector
	pacer  rails.Pacer
	events chan Event
	market string
	connID string

	// subscriptions is the active set, as the exchange last confirmed it; every dial asks for it.
	subscriptions atomic.Pointer[[]Subscription]

	// closed releases a publish blocked on a consumer that stopped reading, so Close never
	// waits on one.
	closed    chan struct{}
	closeOnce sync.Once
}

// Connect opens a connection for a market. It returns once the socket is up, so a bad token or
// a soft ban is reported here rather than retried in the background.
func (c *Client) Connect(ctx context.Context, market string, opts ...ConnOption) (*Conn, error) {
	if market == "" {
		return nil, errors.New(`perps: market is required; pass perps.MarketAll for every market`)
	}

	cfg := connConfig{
		subscriptions: []Subscription{SubscriptionOrders},
		sessionID:     uuid.NewString(),
		pingInterval:  ws.DefaultPingInterval,
		eventBuffer:   defaultEventBuffer,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.eventBuffer < 0 {
		return nil, errors.New("perps: the event buffer cannot be negative")
	}

	conn := &Conn{
		tel:    c.tel,
		pacer:  c.cfg.Pacer,
		events: make(chan Event, cfg.eventBuffer),
		market: market,
		connID: uuid.NewString(),
		closed: make(chan struct{}),
	}

	streams := withOrders(cfg.subscriptions)
	conn.subscriptions.Store(&streams)
	endpoint := func(ctx context.Context) (string, string, error) {
		token, err := c.cfg.TokenSource.Token(ctx)
		if err != nil {
			return "", "", fmt.Errorf("connect: %w", err)
		}
		return connectURL(c.cfg.Environment.WSURL, market, cfg.sessionID, *conn.subscriptions.Load()), "authorization#" + token, nil
	}

	underlying, err := ws.Open(ctx, ws.Options{
		Endpoint:     endpoint,
		HTTPClient:   c.cfg.HTTPClient,
		UserAgent:    rails.UserAgent(),
		Hooks:        c.cfg.Hooks,
		Logger:       c.cfg.Logger,
		ConnID:       conn.connID,
		Market:       market,
		PingInterval: cfg.pingInterval,
		OnFrame:      conn.onFrame,
		OnConnected:  func() { conn.publish(Connected{At: time.Now()}) },
		OnDisconnected: func(reason rails.DisconnectReason) {
			conn.publish(Disconnected{At: time.Now(), Reason: reason})
		},
	})
	if err != nil {
		return nil, err
	}
	conn.ws = underlying
	return conn, nil
}

// withOrders is the set with orders first and no repeats.
func withOrders(subscriptions []Subscription) []Subscription {
	streams := []Subscription{SubscriptionOrders}
	for _, s := range subscriptions {
		if !slices.Contains(streams, s) {
			streams = append(streams, s)
		}
	}
	return streams
}

func connectURL(base, market, sessionID string, streams []Subscription) string {
	names := make([]string, len(streams))
	for i, s := range streams {
		names[i] = string(s)
	}
	query := url.Values{}
	query.Set("product", "perpetuals")
	query.Set("market", market)
	query.Set("subscriptions", strings.Join(names, ","))
	if sessionID != "" {
		query.Set("sessionId", sessionID)
	}
	return strings.TrimSuffix(base, "/") + "/?" + query.Encode()
}

// requireMarket refuses a command that names no market on a connection to every market, which
// the exchange cannot route.
func (c *Conn) requireMarket(market string) error {
	if market == "" && strings.EqualFold(c.market, MarketAll) {
		return errors.New("perps: market is required on a connection to perps.MarketAll")
	}
	return nil
}

// Events delivers fills, rejects and lifecycle changes. It is buffered, and a consumer that
// stops draining it stalls the connection rather than losing frames, so it must be read.
func (c *Conn) Events() <-chan Event { return c.events }

// Ping measures the round trip of an application-level ping, from write to pong. It is the
// same measurement the keepalive reports through [rails.Hooks], and it travels the path orders
// take, so it fails when they would.
func (c *Conn) Ping(ctx context.Context) (time.Duration, error) {
	// The pong echoes the id in the body, so that is the correlation id.
	id := uuid.NewString()
	reply, err := c.ws.Send(ctx, ws.Send{
		Message:         "ping",
		ReplyTypes:      []string{"pong"},
		Content:         map[string]string{"clientRequestId": id},
		ClientRequestID: id,
	})
	if err != nil {
		return 0, err
	}
	return reply.At.Sub(reply.SentAt), nil
}

// publish delivers an event, blocking until the consumer takes it or the connection closes.
func (c *Conn) publish(event Event) {
	select {
	case c.events <- event:
	case <-c.closed:
	}
}

// Close shuts the connection down and waits for every goroutine it started. Events is closed,
// so a consumer ranging over it finishes too — including one that had stopped reading.
func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.ws.Close()
		close(c.events)
	})
}
