package options

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

// Commands check these before sending anything: the exchange answers only on a stream the
// connection holds for what a command names, so without one the answer never arrives.
var (
	// ErrNotSubscribed is a command whose answer rides on a stream the connection does not
	// hold for what the command names.
	ErrNotSubscribed = errors.New("the connection does not hold the stream the answer arrives on")
	// ErrOutOfScope is an order command for a contract outside every scope the orders stream
	// is held for.
	ErrOutOfScope = errors.New("the contract is outside the orders stream's scopes")
)

// Subscription is a stream to receive on the connection. The orders stream carries the answers
// to every order command, [OrderFilled], [OrderRejected], [OrderCancelled] and [Liquidation].
type Subscription string

const (
	SubscriptionOrders          Subscription = "optionsOrders"
	SubscriptionContracts       Subscription = "optionsContracts"
	SubscriptionQuotes          Subscription = "optionsQuotes"
	SubscriptionPublicOrderBook Subscription = "optionsPublicOrderBook"
	SubscriptionPublicTrades    Subscription = "optionsPublicTrades"
	SubscriptionPositions       Subscription = "optionsPositions"
	SubscriptionOpenOrders      Subscription = "optionsOpenOrders"
	SubscriptionRecentOrders    Subscription = "optionsRecentOrders"
	SubscriptionSettlements     Subscription = "optionsSettlements"
	SubscriptionAccountSummary  Subscription = "optionsAccountSummary"
)

const (
	marketAll          = "ALL"
	defaultEventBuffer = 1024
)

// Scope is the contracts a connection follows: every contract of Market, narrowed in turn to
// one Expiration, one Strike and one Type. Market is required; Strike needs Expiration, and
// Type needs Strike. [ScopeAll] follows every market.
type Scope struct {
	_ struct{}

	Market     string // such as BTC-USDT
	Expiration string // as contract names spell it, such as 24JUN26
	Strike     string // such as 62000
	Type       OptionType
}

// ScopeAll is the scope of every contract on every market.
func ScopeAll() Scope { return Scope{Market: marketAll} }

func (s Scope) validate() error {
	switch {
	case s.Market == "":
		return errors.New("options: the scope needs a market; pass options.ScopeAll() for every market")
	case s.Market == marketAll && (s.Expiration != "" || s.Strike != "" || s.Type != ""):
		return errors.New("options: the scope of every market cannot be narrowed")
	case s.Strike != "" && s.Expiration == "":
		return errors.New("options: a scope with a strike needs an expiration")
	case s.Type != "" && s.Strike == "":
		return errors.New("options: a scope with a type needs a strike")
	}
	return nil
}

// label is the scope as the contract-name prefix it covers, such as BTC-24JUN26, or ALL.
func (s Scope) label() string {
	if s.Market == marketAll {
		return marketAll
	}
	underlying, _, _ := strings.Cut(s.Market, "-")
	parts := []string{underlying}
	for _, refiner := range []string{s.Expiration, s.Strike, string(s.Type)} {
		if refiner == "" {
			break
		}
		parts = append(parts, refiner)
	}
	return strings.Join(parts, "-")
}

// covers reports whether a contract, or the label of a narrower scope, is inside the scope.
func (s Scope) covers(target string) bool {
	prefix := s.label()
	return prefix == marketAll || target == prefix || strings.HasPrefix(target, prefix+"-")
}

type connConfig struct {
	subscriptions []Subscription
	sessionID     string
	pingInterval  time.Duration
	eventBuffer   int
}

// ConnOption configures a connection at the point it is opened. The session is fixed for its
// lifetime; the subscriptions are the starting set.
type ConnOption func(*connConfig)

// WithSubscriptions chooses the streams to receive from the start, for the connection's scope.
// Defaults to the orders stream, which is always included: without it no order command is ever
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

// Conn is a WebSocket connection to the contracts of one [Scope]. It reconnects on its own; a
// caller sees that only as events. Its methods are safe for concurrent use: commands from any
// number of goroutines share the connection and each is answered by its own clientRequestId.
type Conn struct {
	ws       *ws.Conn
	overHTTP httpOrders
	tel      *telemetry.Collector
	pacer    rails.Pacer
	events   chan Event
	scope    Scope
	connID   string

	// subscribed is the set held. Every dial asks for its streams at the connection's scope in
	// the URL, and resume for the rest.
	subscribed atomic.Pointer[subscriptionSet]
	changing   chan struct{}
	// carried is what the last dial URL subscribed, and resumes counts the sockets resumed.
	carried atomic.Pointer[[]Subscription]
	resumes atomic.Uint64
	// resuming is resume's changes not yet answered, by clientRequestId. Only the owner
	// goroutine touches it.
	resuming map[string]resumedChange

	// closed releases a publish blocked on a consumer that stopped reading, so Close never
	// waits on one.
	closed    chan struct{}
	closeOnce sync.Once
}

// Connect opens a connection for the contracts of a scope. It returns once the socket is up, so
// a bad token or a refused scope is reported here rather than retried in the background.
func (c *Client) Connect(ctx context.Context, scope Scope, opts ...ConnOption) (*Conn, error) {
	if err := scope.validate(); err != nil {
		return nil, err
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
		return nil, errors.New("options: the event buffer cannot be negative")
	}

	conn := &Conn{
		overHTTP: c,
		tel:      c.tel,
		pacer:    c.cfg.Pacer,
		events:   make(chan Event, cfg.eventBuffer),
		scope:    scope,
		connID:   uuid.NewString(),
		closed:   make(chan struct{}),
		changing: make(chan struct{}, 1),
		resuming: map[string]resumedChange{},
	}

	initial := subscriptionSet{}.subscribe(scope, withOrders(cfg.subscriptions))
	conn.subscribed.Store(&initial)
	endpoint := func(ctx context.Context) (string, string, error) {
		token, err := c.cfg.TokenSource.Token(ctx)
		if err != nil {
			return "", "", fmt.Errorf("connect: %w", err)
		}
		streams := conn.subscribed.Load().dialed(scope)
		conn.carried.Store(&streams)
		return connectURL(c.cfg.Environment.WSURL, scope, cfg.sessionID, streams), "authorization#" + token, nil
	}

	underlying, err := ws.Open(ctx, ws.Options{
		Endpoint:     endpoint,
		HTTPClient:   c.cfg.HTTPClient,
		UserAgent:    rails.UserAgent(),
		Hooks:        c.cfg.Hooks,
		Logger:       c.cfg.Logger,
		ConnID:       conn.connID,
		Market:       scope.label(),
		PingInterval: cfg.pingInterval,
		OnFrame:      conn.onFrame,
		Resume:       conn.resume,
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

// withOrders is the set with the orders stream first and no repeats.
func withOrders(subscriptions []Subscription) []Subscription {
	streams := []Subscription{SubscriptionOrders}
	for _, s := range subscriptions {
		if !slices.Contains(streams, s) {
			streams = append(streams, s)
		}
	}
	return streams
}

func connectURL(base string, scope Scope, sessionID string, streams []Subscription) string {
	names := make([]string, len(streams))
	for i, s := range streams {
		names[i] = string(s)
	}
	query := url.Values{}
	query.Set("product", "options")
	query.Set("market", scope.Market)
	for key, value := range map[string]string{
		"expiration": scope.Expiration, "strike": scope.Strike, "type": string(scope.Type),
	} {
		if value != "" {
			query.Set(key, value)
		}
	}
	query.Set("subscriptions", strings.Join(names, ","))
	if sessionID != "" {
		query.Set("sessionId", sessionID)
	}
	return strings.TrimSuffix(base, "/") + "/?" + query.Encode()
}

// guard refuses an order command whose answers could not reach this connection. An empty
// contract stands for the connection's own scope.
func (c *Conn) guard(op, contract string) error {
	subscribed := *c.subscribed.Load()
	if _, ok := subscribed[SubscriptionOrders]; !ok {
		return fmt.Errorf("%s: %w: %s", op, ErrNotSubscribed, SubscriptionOrders)
	}
	target := contract
	if target == "" {
		target = c.scope.label()
	}
	if !subscribed.covers(SubscriptionOrders, target) {
		return fmt.Errorf("%s: %w: %s", op, ErrOutOfScope, target)
	}
	return nil
}

// guardContract is [Conn.guard] for a command that has to name its contract.
func (c *Conn) guardContract(op, contract string) error {
	if contract == "" {
		return fmt.Errorf("%s: contract is required", op)
	}
	return c.guard(op, contract)
}

// Events delivers fills, rejections, cancellations and lifecycle changes. It must be drained: a
// full buffer stalls the connection rather than dropping frames.
func (c *Conn) Events() <-chan Event { return c.events }

// Ping measures an application-level round trip, write to pong, over the path orders take.
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
