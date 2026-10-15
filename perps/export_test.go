package perps

import (
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/pace"
	"github.com/rails-xyz/exchange-go-sdk/internal/wsevent"
)

// Test-only exports, for the spec conformance test in package perps_test.
type (
	WireOrder           = wireOrder
	NotificationData    = notificationData
	ShutdownData        = wsevent.Shutdown
	BackfillWire        = wsevent.Backfill
	CreateOrderContent  = createOrderContent
	CancelOrderContent  = cancelOrderContent
	ModifyOrderContent  = modifyOrderContent
	UpdateMarginContent = updateMarginContent
	SubscriptionContent = subscriptionContent
	SubscriptionData    = subscriptionData
	EmitContent         = emitContent
	WireTrade           = wireTrade
	PublicTradesData    = publicTradesData
	OrderBookData       = orderBookData
)

var (
	ToEvent              = toEvent
	OrderFromWire        = wireOrder.toOrder
	TradesFromWire       = tradesFromWire
	PublicTradesFromWire = publicTradesFromWire
	LevelsFromWire       = levelsFromWire
)

// SetPacerClock rebuilds the engine on now, so it must come before the pacer is used.
func SetPacerClock(p *Pacer, now func() time.Time) {
	p.cfg.Now = now
	p.engine = pace.New(p.cfg)
}
