package options_test

import (
	"testing"

	"github.com/rails-xyz/exchange-go-sdk/options"
	"github.com/stretchr/testify/assert"
)

// Only what a snapshot rebuilds may be dropped for a consumer that is behind.
func TestOnlyMarketDataMayBeDropped(t *testing.T) {
	for _, e := range []options.Event{
		options.ContractUpdate{},
		options.QuoteUpdate{},
		options.OrderBookDelta{},
		options.PublicTrades{},
	} {
		assert.True(t, options.IsMarketData(e), "%T", e)
	}
	for _, e := range []options.Event{
		options.OrderFilled{},
		options.PositionsDelta{},
	} {
		assert.False(t, options.IsMarketData(e), "%T", e)
	}
}
