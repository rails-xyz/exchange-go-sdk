package perps_test

import (
	"testing"

	"github.com/rails-xyz/exchange-go-sdk/perps"
	"github.com/stretchr/testify/assert"
)

// Only what a snapshot rebuilds may be dropped for a consumer that is behind.
func TestOnlyMarketDataMayBeDropped(t *testing.T) {
	for _, e := range []perps.Event{
		perps.PublicTrades{},
		perps.OrderBookDelta{},
	} {
		assert.True(t, perps.IsMarketData(e), "%T", e)
	}
	for _, e := range []perps.Event{
		perps.OrderFilled{},
		perps.Trades{},
	} {
		assert.False(t, perps.IsMarketData(e), "%T", e)
	}
}
