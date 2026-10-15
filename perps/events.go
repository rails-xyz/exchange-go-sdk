package perps

import (
	"encoding/json"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// Event is one thing that happened on a connection. Type-switch on it:
//
//	for event := range conn.Events() {
//		switch e := event.(type) {
//		case perps.OrderFilled:
//			…
//		}
//	}
//
// Events added in a later release will appear as new types, so a switch needs a default.
type Event interface{ isEvent() }

// Connected is sent each time a socket is established, including after a reconnect.
type Connected struct{ At time.Time }

// Disconnected is sent when a socket ends, with the reason it did.
type Disconnected struct {
	At     time.Time
	Reason rails.DisconnectReason
}

// OrderFilled is a fill on one of your orders. Partial says whether any quantity remains.
type OrderFilled struct {
	Order   Order
	Partial bool
}

// OrderRejected is an order the exchange accepted and then would not keep — a post-only that
// would have matched immediately, for instance. Body is the exchange's wording; Code the reason.
type OrderRejected struct {
	Order           Order
	Body            string
	Slug            string
	Code            rails.Code
	ClientRequestID string
}

// ConnectionShutdown warns that this socket will be closed. The SDK is already opening a
// replacement; the event is for a caller that wants to know.
type ConnectionShutdown struct {
	ForceCloseIn time.Duration
	At           time.Time
}

// SessionBackfill reports what a reconnect recovered. Resumed false, or any market in
// ExpiredMarkets, means local order state has to be rebuilt from HTTP.
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

// Trades is a batch of fills on the account's orders, from the trades stream, in any market.
type Trades struct{ Trades []Trade }

// PublicTrades is a batch of fills on a market, from the publicTrades stream.
type PublicTrades struct {
	Market string
	Trades []PublicTrade
}

// OrderBookDelta is a change to a market's book, from the publicOrderBook stream: each level
// replaces the one at its price, and a Quantity of "0" removes it. Deltas carry no sequence
// number; resync with [Conn.EmitPublicOrderBook].
type OrderBookDelta struct {
	Market string
	Orders []OrderBookLevel
	Ts     int64
}

// Unknown is a frame this release has no type for. That includes two kinds of order-stream
// frame the exchange sends about commands this connection did not issue: the answers to a
// command sent on another connection of the same account, and the second "order cancelled"
// notice a cancel produces, filed under the original order's clientRequestId. Raw is the whole
// frame as it arrived.
type Unknown struct {
	ResultType string
	Raw        json.RawMessage
}

func (Connected) isEvent()          {}
func (Disconnected) isEvent()       {}
func (OrderFilled) isEvent()        {}
func (OrderRejected) isEvent()      {}
func (ConnectionShutdown) isEvent() {}
func (SessionBackfill) isEvent()    {}
func (ServerError) isEvent()        {}
func (Trades) isEvent()             {}
func (PublicTrades) isEvent()       {}
func (OrderBookDelta) isEvent()     {}
func (Unknown) isEvent()            {}
