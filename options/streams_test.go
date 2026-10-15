package options_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/perpstest"
	"github.com/rails-xyz/exchange-go-sdk/internal/spectest"
	"github.com/rails-xyz/exchange-go-sdk/options"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var everyStream = []options.Subscription{
	options.SubscriptionOrders, options.SubscriptionContracts, options.SubscriptionQuotes,
	options.SubscriptionPublicOrderBook, options.SubscriptionPublicTrades, options.SubscriptionPositions,
	options.SubscriptionOpenOrders, options.SubscriptionRecentOrders, options.SubscriptionSettlements,
	options.SubscriptionAccountSummary,
}

// streamingConn is a connection to every market holding every stream, so no emit is refused.
func streamingConn(t *testing.T, srv *perpstest.Server) *options.Conn {
	t.Helper()
	conn, err := wsClient(t, srv).Connect(t.Context(), options.ScopeAll(),
		options.WithSubscriptions(everyStream...), options.WithPingInterval(0))
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return conn
}

// emits calls each Emit method, naming contract where it takes one.
var emits = map[options.Subscription]func(ctx context.Context, conn *options.Conn, contract string) error{
	options.SubscriptionContracts: func(ctx context.Context, conn *options.Conn, _ string) error {
		_, err := conn.EmitContracts(ctx, options.ExpiryQuery{Market: "BTC-USDT", ExpiryDate: "2026-06-24"})
		return err
	},
	options.SubscriptionQuotes: func(ctx context.Context, conn *options.Conn, _ string) error {
		_, err := conn.EmitQuotes(ctx, options.ExpiryQuery{Market: "BTC-USDT", ExpiryDate: "2026-06-24"})
		return err
	},
	options.SubscriptionPublicOrderBook: func(ctx context.Context, conn *options.Conn, contract string) error {
		_, err := conn.EmitOrderBook(ctx, contract)
		return err
	},
	options.SubscriptionPublicTrades: func(ctx context.Context, conn *options.Conn, contract string) error {
		_, err := conn.EmitPublicTrades(ctx, options.ContractQuery{Contract: contract})
		return err
	},
	options.SubscriptionPositions: func(ctx context.Context, conn *options.Conn, contract string) error {
		_, err := conn.EmitPositions(ctx, options.ContractQuery{Contract: contract})
		return err
	},
	options.SubscriptionOpenOrders: func(ctx context.Context, conn *options.Conn, contract string) error {
		_, err := conn.EmitOpenOrders(ctx, options.ContractQuery{Contract: contract})
		return err
	},
	options.SubscriptionRecentOrders: func(ctx context.Context, conn *options.Conn, contract string) error {
		_, err := conn.EmitRecentOrders(ctx, options.ContractQuery{Contract: contract})
		return err
	},
	options.SubscriptionSettlements: func(ctx context.Context, conn *options.Conn, contract string) error {
		_, err := conn.EmitSettlements(ctx, options.ContractQuery{Contract: contract})
		return err
	},
	options.SubscriptionAccountSummary: func(ctx context.Context, conn *options.Conn, _ string) error {
		_, err := conn.EmitAccountSummary(ctx)
		return err
	},
}

// Each emit checks for the one stream its answer arrives on, and sends nothing without it.
func TestEmitsNeedTheirStream(t *testing.T) {
	for stream, emit := range emits {
		t.Run(string(stream), func(t *testing.T) {
			srv := fakeExchange(t)
			others := slices.DeleteFunc(slices.Clone(everyStream), func(s options.Subscription) bool { return s == stream })
			conn, err := wsClient(t, srv).Connect(t.Context(), options.ScopeAll(),
				options.WithSubscriptions(others...), options.WithPingInterval(0))
			require.NoError(t, err)
			defer conn.Close()

			assert.ErrorIs(t, emit(t.Context(), conn, testContract), options.ErrNotSubscribed)
			assert.Empty(t, srv.Received(), "a refused emit reached the exchange")
		})
	}
}

// An emit needs its stream held for what it names: its contract, every contract, or for the
// expiry emits, any scope of the market.
func TestEmitsNeedAScopeCoveringWhatTheyName(t *testing.T) {
	srv := fakeExchange(t)
	conn, err := wsClient(t, srv).Connect(t.Context(), options.Scope{Market: "BTC-USDT", Expiration: "24JUN26", Strike: "62000"},
		options.WithSubscriptions(everyStream...), options.WithPingInterval(0))
	require.NoError(t, err)
	defer conn.Close()

	for name, tc := range map[string]struct {
		emit    func(ctx context.Context) error
		refused bool
	}{
		"a contract in the scope": {emit: func(ctx context.Context) error {
			_, err := conn.EmitOrderBook(ctx, "BTC-24JUN26-62000-P")
			return err
		}},
		"a contract outside it": {refused: true, emit: func(ctx context.Context) error {
			_, err := conn.EmitOrderBook(ctx, "BTC-24JUN26-63000-C")
			return err
		}},
		"every contract": {refused: true, emit: func(ctx context.Context) error {
			_, err := conn.EmitPositions(ctx, options.ContractQuery{})
			return err
		}},
		"another expiry of the market": {emit: func(ctx context.Context) error {
			_, err := conn.EmitQuotes(ctx, options.ExpiryQuery{Market: "BTC-USDT", ExpiryDate: "2026-06-26"})
			return err
		}},
		"another market": {refused: true, emit: func(ctx context.Context) error {
			_, err := conn.EmitQuotes(ctx, options.ExpiryQuery{Market: "ETH-USDT", ExpiryDate: "2026-06-24"})
			return err
		}},
	} {
		t.Run(name, func(t *testing.T) {
			// Bounded, so an emit that does go out fails here rather than waiting for an answer.
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			err := tc.emit(ctx)
			if tc.refused {
				assert.ErrorIs(t, err, options.ErrNotSubscribed)
			} else {
				assert.ErrorIs(t, err, rails.ErrTimeout)
			}
		})
	}
}

// The page token goes out as given, and the next one comes back.
func TestEmitPagesThrough(t *testing.T) {
	srv := fakeExchange(t)
	conn := streamingConn(t, srv)

	got := async(func() (*options.Page[options.RecentOrder], error) {
		return conn.EmitRecentOrders(t.Context(), options.ContractQuery{Contract: testContract, PageSize: 2, PageToken: "page-2"})
	})
	msg := command(t, srv, "emitOptionsRecentOrders")
	assert.Equal(t, "page-2", spectest.Decode(t, msg.Content)["pageToken"])
	srv.Push(frame("optionsRecentOrders", testContract, map[string]any{
		"statusCode": 200, "clientRequestId": msg.ClientRequestID(), "orders": []any{}, "nextPageToken": "page-3",
	}))

	res := spectest.Receive(t, got)
	require.NoError(t, res.err)
	assert.Equal(t, "page-3", res.value.Next)
}

// A page returning the token that fetched it would loop a caller forever.
func TestEmitRepeatedPageTokenIsAnError(t *testing.T) {
	srv := fakeExchange(t)
	conn := streamingConn(t, srv)

	got := async(func() (*options.Page[options.RecentOrder], error) {
		return conn.EmitRecentOrders(t.Context(), options.ContractQuery{Contract: testContract, PageToken: "page-2"})
	})
	msg := command(t, srv, "emitOptionsRecentOrders")
	srv.Push(frame("optionsRecentOrders", testContract, map[string]any{
		"statusCode": 200, "clientRequestId": msg.ClientRequestID(), "orders": []any{}, "nextPageToken": "page-2",
	}))

	assert.Error(t, spectest.Receive(t, got).err)
}
