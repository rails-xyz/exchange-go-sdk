package options

import (
	"encoding/json"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// Event is one thing that happened on a connection. Type-switch on it:
//
//	for event := range conn.Events() {
//		switch e := event.(type) {
//		case options.OrderFilled:
//			…
//		}
//	}
//
// Events added in a later release will appear as new types, so a switch needs a default.
//
// For the contracts the orders stream is held for, answers to commands sent on the account's
// other connections, or to a command whose caller stopped waiting, arrive too. Such a fill,
// rejection or cancellation is an event of its own; every other such answer is [Unknown].
type Event interface{ isEvent() }

// Connected is sent each time a socket is established, including after a reconnect.
type Connected struct{ At time.Time }

// Disconnected is sent when a socket ends, with the reason it did.
type Disconnected struct {
	At     time.Time
	Reason rails.DisconnectReason
}

// OrderFilled is a fill on one of your orders that no command of this connection was waiting
// for. Partial says whether any quantity remains.
type OrderFilled struct {
	Order   Order
	Partial bool
}

// OrderRejected is an order command the exchange refused that no command of this connection
// was waiting for, including one cancel of a [Conn.CancelAllOrders] sweep. Order is zero when
// the refusal names none. Body is the exchange's wording; Code the reason.
type OrderRejected struct {
	Order           Order
	Body            string
	Slug            string
	Code            rails.Code
	ClientRequestID string
}

// OrderCancelled is an order cancelled by a cancel no command of this connection was waiting
// for: one cancel of a [Conn.CancelAllOrders] sweep, carrying the sweep's ClientRequestID, or a
// cancel sent elsewhere.
type OrderCancelled struct {
	Order           Order
	ClientRequestID string
}

// Liquidation is your account being liquidated, sent once per contract it acts on, before it
// does; LiquidationID is shared by all of them. MarginRatio is maintenance margin over
// TotalEquity (1 when equity is not positive); UsedMargin and TotalEquity are USDT at TriggeredAt.
type Liquidation struct {
	Contract      string
	LiquidationID string
	MarginRatio   string
	UsedMargin    string
	TotalEquity   string
	TriggeredAt   int64
}

// ContractUpdate is a change to a contract's mark price, IV, greeks or lot size, from the
// contracts stream.
type ContractUpdate struct {
	ContractName string
	Underlying   string // such as BTC-USDT
	Strike       string
	OptionType   OptionType
	ExpiryDate   string // YYYY-MM-DD
	MarkPrice    string
	MarkIV       string
	Forward      string
	Delta        string
	Gamma        string
	Vega         string
	Theta        string
	Rho          string
	LotSize      string
}

// QuoteUpdate is a change to the top of a contract's book, from the quotes stream.
type QuoteUpdate struct{ Quote Quote }

// OrderBookDelta is a change to a contract's book, from the order book stream: each level
// replaces the one at its price, and a Quantity of "0" removes it. Deltas carry no sequence
// number; resync with [Conn.EmitOrderBook].
type OrderBookDelta struct {
	Contract string
	Orders   []OrderBookLevel
	BestBid  BestQuote
	BestAsk  BestQuote
	Ts       int64
}

// PublicTrades is a batch of matches on a contract, from the public trades stream.
type PublicTrades struct {
	Contract string
	Trades   []PublicTrade
}

// PositionsDelta is positions that changed, from the positions stream. A position that closed
// arrives once, with Side [PositionSideFlat].
type PositionsDelta struct {
	Contract  string
	Positions []Position
}

// OpenOrdersDelta is orders joining or leaving your open orders, from the open orders stream.
type OpenOrdersDelta struct {
	Contract string
	Changes  []OpenOrderChange
}

// OpenOrderChange is one order joining or leaving the open orders.
type OpenOrderChange struct {
	Kind  OpenOrderKind
	Order OpenOrder
}

// RecentOrdersDelta is new or updated entries of your order history, from the recent orders
// stream.
type RecentOrdersDelta struct {
	Contract string
	Orders   []RecentOrder
}

// SubscriptionLost is a subscription the exchange refused to restore after a reconnect, such
// as one over the connection's limit. The connection no longer holds it.
type SubscriptionLost struct {
	Scope         Scope
	Subscriptions []Subscription
	Err           *rails.Error
}

// ConnectionShutdown warns that this socket will be closed. The SDK is already opening a
// replacement; the event is for a caller that wants to know.
type ConnectionShutdown struct {
	ForceCloseIn time.Duration
	At           time.Time
}

// SessionBackfill reports what a reconnect recovered, one entry per contract. Resumed false, or
// any contract in ExpiredMarkets, means local order state has to be rebuilt from HTTP.
type SessionBackfill struct {
	SessionID         string
	Resumed           bool
	Events            int
	BackfilledMarkets []string
	ExpiredMarkets    []string
	From              int64
	To                int64
}

// ServerError is an error the exchange reported outside the command envelope — a malformed
// frame, a rate limit, a service being down. It names no clientRequestId; a command it dropped
// fails with [rails.ErrNotSent] when it can be told apart, and at its deadline otherwise.
type ServerError struct {
	Err *rails.Error
	At  time.Time
}

// Unknown is a frame this release has no type for. That includes a repeated acknowledgement of
// a [Conn.CancelAllOrders] sweep, and a snapshot no Emit of this connection was waiting for,
// such as one asked for on another connection of the account. Raw is the whole frame as it
// arrived.
type Unknown struct {
	ResultType string
	Raw        json.RawMessage
}

func (Connected) isEvent()          {}
func (Disconnected) isEvent()       {}
func (OrderFilled) isEvent()        {}
func (OrderRejected) isEvent()      {}
func (OrderCancelled) isEvent()     {}
func (Liquidation) isEvent()        {}
func (ContractUpdate) isEvent()     {}
func (QuoteUpdate) isEvent()        {}
func (OrderBookDelta) isEvent()     {}
func (PublicTrades) isEvent()       {}
func (PositionsDelta) isEvent()     {}
func (OpenOrdersDelta) isEvent()    {}
func (RecentOrdersDelta) isEvent()  {}
func (SubscriptionLost) isEvent()   {}
func (ConnectionShutdown) isEvent() {}
func (SessionBackfill) isEvent()    {}
func (ServerError) isEvent()        {}
func (Unknown) isEvent()            {}
