package options

import (
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/pace"
	"github.com/rails-xyz/exchange-go-sdk/internal/wsevent"
)

// Test-only exports, for the tests in package options_test.
type (
	WireOrder             = wireOrder
	OrderData             = orderData
	CancelAllData         = cancelAllData
	LiquidationData       = liquidationData
	ShutdownData          = wsevent.Shutdown
	BackfillWire          = wsevent.Backfill
	CreateOrderContent    = createOrderContent
	ModifyOrderContent    = modifyOrderContent
	CancelOrderContent    = cancelOrderContent
	CancelAllContent      = cancelAllContent
	SubscriptionContent   = subscriptionContent
	SubscriptionData      = subscriptionData
	ExpiryContent         = expiryContent
	ContractContent       = contractContent
	AccountSummaryContent = accountSummaryContent
	ContractsData         = contractsData
	QuotesData            = quotesData
	WireQuote             = wireQuote
	WireContractUpdate    = wireContractUpdate
	OrderBookData         = orderBookData
	PublicTradesData      = publicTradesData
	PositionsData         = positionsData
	OpenOrdersData        = openOrdersData
	RecentOrdersData      = recentOrdersData
	SettlementsData       = settlementsData
	AccountSummaryData    = accountSummaryData
)

var (
	ToEvent       = toEvent
	OrderFromWire = (*wireOrder).toOrder
)

// Held is the scopes the connection holds each stream for, by label.
func Held(c *Conn) map[Subscription][]string {
	out := map[Subscription][]string{}
	for s, scopes := range *c.subscribed.Load() {
		for _, scope := range scopes {
			out[s] = append(out[s], scope.label())
		}
	}
	return out
}

// SetPacerClock rebuilds the engine on now, so it must come before the pacer is used.
func SetPacerClock(p *Pacer, now func() time.Time) {
	p.cfg.Now = now
	p.engine = pace.New(p.cfg)
}
