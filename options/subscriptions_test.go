package options_test

import (
	"context"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/perpstest"
	"github.com/rails-xyz/exchange-go-sdk/internal/spectest"
	"github.com/rails-xyz/exchange-go-sdk/options"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// changeSubscriptions runs a Subscribe or Unsubscribe and confirms it as the exchange would,
// with the streams the connection then receives.
func changeSubscriptions(t *testing.T, srv *perpstest.Server, op string, change func() error, active ...options.Subscription) {
	t.Helper()
	got := async(func() (struct{}, error) { return struct{}{}, change() })
	crid := command(t, srv, op).ClientRequestID()
	srv.Push(frame(op+"d", "", map[string]any{"statusCode": 200, "clientRequestId": crid, "activeSubscriptions": active}))
	require.NoError(t, spectest.Receive(t, got).err)
}

// The set held follows the exchange's rules, so the guards see what the connection receives.
func TestSubscriptionsFollowTheExchangesRules(t *testing.T) {
	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())
	quotes := options.SubscriptionQuotes
	change := func(op string, scope options.Scope, stream options.Subscription, active ...options.Subscription) {
		t.Helper()
		changeSubscriptions(t, srv, op, func() error {
			if op == "subscribe" {
				return conn.Subscribe(t.Context(), scope, stream)
			}
			return conn.Unsubscribe(t.Context(), scope, stream)
		}, append(active, options.SubscriptionOrders)...)
	}
	held := func() []string { return options.Held(conn)[quotes] }

	for _, scope := range []options.Scope{
		{Market: "ETH-USDT", Expiration: "2OCT26", Strike: "2550"},
		{Market: "BTC-USDT", Expiration: "2OCT26", Strike: "75000", Type: options.OptionTypeCall},
		{Market: "BTC-USDT", Expiration: "2OCT26", Strike: "84000"},
		{Market: "BTC-USDT", Expiration: "30OCT26", Strike: "84000", Type: options.OptionTypeCall},
	} {
		change("subscribe", scope, quotes, quotes)
	}

	change("unsubscribe", options.Scope{Market: "BTC-USDT", Expiration: "2OCT26"}, quotes, quotes)
	assert.ElementsMatch(t, []string{"ETH-2OCT26-2550", "BTC-30OCT26-84000-C"}, held(), "an unsubscribe removes every scope it covers")

	change("subscribe", options.Scope{Market: "BTC-USDT"}, quotes, quotes)
	assert.ElementsMatch(t, []string{"ETH-2OCT26-2550", "BTC"}, held(), "a broader scope replaces the ones it covers")

	change("subscribe", options.Scope{Market: "BTC-USDT", Expiration: "30OCT26"}, quotes, quotes)
	assert.ElementsMatch(t, []string{"ETH-2OCT26-2550", "BTC"}, held(), "a covered scope is a no-op")

	change("subscribe", options.Scope{Market: "BTC-USDT", Expiration: "2OCT26"}, options.SubscriptionAccountSummary,
		quotes, options.SubscriptionAccountSummary)
	assert.Equal(t, []string{"ALL"}, options.Held(conn)[options.SubscriptionAccountSummary], "the account summary is account-wide")

	change("unsubscribe", options.ScopeAll(), quotes, options.SubscriptionAccountSummary)
	assert.NotContains(t, options.Held(conn), quotes, "unsubscribing every market removes the stream")
}

// The dial URL carries the connection's own scope; every other one is subscribed again on the
// new socket, and one the exchange refuses is reported and no longer held.
func TestResumeRestoresEveryOtherScope(t *testing.T) {
	srv := fakeExchange(t)
	scope := options.Scope{Market: "BTC-USDT", Expiration: "24JUN26"}
	other := options.Scope{Market: "BTC-USDT", Expiration: "26JUN26"}
	conn, err := wsClient(t, srv).Connect(t.Context(), scope,
		options.WithSubscriptions(options.SubscriptionPositions), options.WithPingInterval(0))
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	changeSubscriptions(t, srv, "subscribe", func() error {
		return conn.Subscribe(t.Context(), other, options.SubscriptionQuotes)
	}, options.SubscriptionOrders, options.SubscriptionPositions, options.SubscriptionQuotes)
	changeSubscriptions(t, srv, "subscribe", func() error {
		return conn.Subscribe(t.Context(), options.ScopeAll(), options.SubscriptionSettlements)
	}, options.SubscriptionOrders, options.SubscriptionPositions, options.SubscriptionQuotes, options.SubscriptionSettlements)
	changeSubscriptions(t, srv, "subscribe", func() error {
		return conn.Subscribe(t.Context(), scope, options.SubscriptionAccountSummary)
	}, options.SubscriptionOrders, options.SubscriptionPositions, options.SubscriptionQuotes,
		options.SubscriptionSettlements, options.SubscriptionAccountSummary)

	srv.CloseConn(0, 1012, "Service restarting. Please reconnect.")
	// The first reconnect waits the connect budget out.
	srv.WaitConnections(2, 10*time.Second)
	assert.Equal(t, "optionsOrders,optionsAccountSummary,optionsPositions", srv.Handshakes()[1].Subscriptions)

	settlements := command(t, srv, "subscribe")
	assert.Equal(t, map[string]any{
		"clientRequestId": settlements.ClientRequestID(), "subscriptions": []any{"optionsSettlements"}, "market": "ALL",
	}, spectest.Decode(t, settlements.Content))
	quotes := command(t, srv, "subscribe")
	assert.Equal(t, map[string]any{
		"clientRequestId": quotes.ClientRequestID(), "subscriptions": []any{"optionsQuotes"},
		"market": "BTC-USDT", "expiration": "26JUN26",
	}, spectest.Decode(t, quotes.Content))

	srv.Push(frame("subscribed", "", map[string]any{"statusCode": 200, "clientRequestId": settlements.ClientRequestID()}))
	srv.Push(frame("subscribe", "", map[string]any{
		"statusCode": 429, "body": "Subscription limit reached (max 100)", "clientRequestId": quotes.ClientRequestID(),
		"error": map[string]string{"slug": "SUBSCRIPTION_LIMIT", "code": "SYS-0012"},
	}))

	for event := range conn.Events() {
		_, unknown := event.(options.Unknown)
		require.False(t, unknown, "an answer to a resumed subscription reached Events: %+v", event)
		if lost, ok := event.(options.SubscriptionLost); ok {
			assert.Equal(t, other, lost.Scope)
			assert.Equal(t, []options.Subscription{options.SubscriptionQuotes}, lost.Subscriptions)
			assert.Equal(t, rails.CodeSubscriptionLimit, lost.Err.Code)
			break
		}
	}
	assert.NotContains(t, options.Held(conn), options.SubscriptionQuotes)
	assert.Contains(t, options.Held(conn), options.SubscriptionSettlements)
}

// holdHandover starts a handover and holds the replacement socket's upgrade, its URL already
// built, until release. The old socket keeps working meanwhile.
func holdHandover(t *testing.T, srv *perpstest.Server) (release func()) {
	t.Helper()
	dialled, proceed := make(chan struct{}), make(chan struct{})
	var held, released sync.Once
	srv.SetReject(func(http.ResponseWriter, *http.Request) bool {
		held.Do(func() {
			close(dialled)
			<-proceed
		})
		return false
	})
	release = func() { released.Do(func() { close(proceed) }) }
	t.Cleanup(release)

	srv.PushTo(0, frame("connectionShutdown", "", map[string]any{"statusCode": 200, "forceCloseInMs": 60000, "ts": 1756000000000}))
	select {
	case <-dialled:
	case <-time.After(settle):
		t.Fatal("no replacement dialled")
	}
	return release
}

// A change the old socket confirms after the new socket's URL was built is caught up with.
func TestResumeCatchesUpWithChangesMadeWhileDialling(t *testing.T) {
	srv := fakeExchange(t)
	scope := options.Scope{Market: "BTC-USDT", Expiration: "24JUN26"}
	conn, err := wsClient(t, srv).Connect(t.Context(), scope,
		options.WithSubscriptions(options.SubscriptionPositions), options.WithPingInterval(0))
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	release := holdHandover(t, srv)
	changeSubscriptions(t, srv, "subscribe", func() error {
		return conn.Subscribe(t.Context(), scope, options.SubscriptionQuotes)
	}, options.SubscriptionOrders, options.SubscriptionPositions, options.SubscriptionQuotes)
	changeSubscriptions(t, srv, "unsubscribe", func() error {
		return conn.Unsubscribe(t.Context(), scope, options.SubscriptionPositions)
	}, options.SubscriptionOrders, options.SubscriptionQuotes)
	release()

	srv.WaitConnections(2, settle)
	assert.Equal(t, "optionsOrders,optionsPositions", srv.Handshakes()[1].Subscriptions, "the URL was built before the changes")
	for _, want := range []struct {
		op     string
		stream string
	}{{"unsubscribe", "optionsPositions"}, {"subscribe", "optionsQuotes"}} {
		msg := command(t, srv, want.op)
		assert.Equal(t, map[string]any{
			"clientRequestId": msg.ClientRequestID(), "subscriptions": []any{want.stream},
			"market": "BTC-USDT", "expiration": "24JUN26",
		}, spectest.Decode(t, msg.Content))
	}
}

// A change still in flight when the new socket comes up is sent on it again.
func TestAChangeOverlappingAResumeIsSentAgain(t *testing.T) {
	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())
	waitEvent[options.Connected](t, conn.Events())
	other := options.Scope{Market: "BTC-USDT", Expiration: "26JUN26"}

	release := holdHandover(t, srv)
	got := async(func() (struct{}, error) {
		return struct{}{}, conn.Subscribe(t.Context(), other, options.SubscriptionQuotes)
	})
	first := command(t, srv, "subscribe")
	release()
	waitEvent[options.Connected](t, conn.Events())

	active := []options.Subscription{options.SubscriptionOrders, options.SubscriptionQuotes}
	srv.PushTo(0, frame("subscribed", "", map[string]any{
		"statusCode": 200, "clientRequestId": first.ClientRequestID(), "activeSubscriptions": active,
	}))
	again := command(t, srv, "subscribe")
	sent := spectest.Decode(t, again.Content)
	delete(sent, "clientRequestId")
	assert.Equal(t, map[string]any{"subscriptions": []any{"optionsQuotes"}, "market": "BTC-USDT", "expiration": "26JUN26"}, sent)
	srv.Push(frame("subscribed", "", map[string]any{
		"statusCode": 200, "clientRequestId": again.ClientRequestID(), "activeSubscriptions": active,
	}))

	require.NoError(t, spectest.Receive(t, got).err)
	assert.Equal(t, []string{"BTC-26JUN26"}, options.Held(conn)[options.SubscriptionQuotes])
}

// A change sent again that the new socket never answers is not held, since it may lack it.
func TestAnUnansweredResendIsNotHeld(t *testing.T) {
	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())
	waitEvent[options.Connected](t, conn.Events())
	other := options.Scope{Market: "BTC-USDT", Expiration: "26JUN26"}

	release := holdHandover(t, srv)
	got := async(func() (struct{}, error) {
		return struct{}{}, conn.Subscribe(t.Context(), other, options.SubscriptionQuotes)
	})
	first := command(t, srv, "subscribe")
	release()
	waitEvent[options.Connected](t, conn.Events())

	srv.PushTo(0, frame("subscribed", "", map[string]any{
		"statusCode": 200, "clientRequestId": first.ClientRequestID(),
		"activeSubscriptions": []options.Subscription{options.SubscriptionOrders, options.SubscriptionQuotes},
	}))
	command(t, srv, "subscribe")
	srv.CloseConn(1, 1011, "gone")

	assert.ErrorIs(t, spectest.Receive(t, got).err, rails.ErrConnClosed)
	assert.NotContains(t, options.Held(conn), options.SubscriptionQuotes)
}

// An unsubscribe sent again may find nothing left to remove; that refusal is not an error.
func TestARefusedUnsubscribeResendSucceeds(t *testing.T) {
	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())
	waitEvent[options.Connected](t, conn.Events())
	other := options.Scope{Market: "BTC-USDT", Expiration: "26JUN26"}
	changeSubscriptions(t, srv, "subscribe", func() error {
		return conn.Subscribe(t.Context(), other, options.SubscriptionQuotes)
	}, options.SubscriptionOrders, options.SubscriptionQuotes)

	release := holdHandover(t, srv)
	got := async(func() (struct{}, error) {
		return struct{}{}, conn.Unsubscribe(t.Context(), other, options.SubscriptionQuotes)
	})
	first := command(t, srv, "unsubscribe")
	release()
	waitEvent[options.Connected](t, conn.Events())

	srv.PushTo(0, frame("unsubscribed", "", map[string]any{
		"statusCode": 200, "clientRequestId": first.ClientRequestID(),
		"activeSubscriptions": []options.Subscription{options.SubscriptionOrders},
	}))
	command(t, srv, "subscribe") // the resume, from the set held when the new socket came up
	again := command(t, srv, "unsubscribe")
	srv.Push(frame("unsubscribe", "", map[string]any{
		"statusCode": 400, "body": "no matching subscription", "clientRequestId": again.ClientRequestID(),
		"error": map[string]string{"slug": "BAD_REQUEST", "code": "SYS-0006"},
	}))

	require.NoError(t, spectest.Receive(t, got).err)
	assert.NotContains(t, options.Held(conn), options.SubscriptionQuotes)
}

// A change whose caller stopped waiting is still held once the exchange confirms it.
func TestAChangeAnsweredLateIsHeld(t *testing.T) {
	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())
	other := options.Scope{Market: "BTC-USDT", Expiration: "26JUN26"}

	ctx, cancel := context.WithCancel(t.Context())
	got := async(func() (struct{}, error) { return struct{}{}, conn.Subscribe(ctx, other, options.SubscriptionQuotes) })
	msg := command(t, srv, "subscribe")
	cancel()
	require.ErrorIs(t, spectest.Receive(t, got).err, rails.ErrTimeout)

	srv.Push(frame("subscribed", "", map[string]any{
		"statusCode": 200, "clientRequestId": msg.ClientRequestID(),
		"activeSubscriptions": []options.Subscription{options.SubscriptionOrders, options.SubscriptionQuotes},
	}))
	assert.Eventually(t, func() bool {
		return slices.Equal(options.Held(conn)[options.SubscriptionQuotes], []string{"BTC-26JUN26"})
	}, settle, 10*time.Millisecond)
}

// A change whose context has already ended is not sent.
func TestAChangeWithAnEndedContextIsNotSent(t *testing.T) {
	srv := fakeExchange(t)
	conn := tradingConn(t, srv, options.ScopeAll())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for range 20 { // the gate is free, so an unchecked select would take it half the time
		require.ErrorIs(t, conn.Subscribe(ctx, options.ScopeAll(), options.SubscriptionQuotes), context.Canceled)
	}
	time.Sleep(100 * time.Millisecond)
	for _, msg := range srv.Received() {
		assert.NotEqual(t, "subscribe", msg.Message, "nothing should be sent")
	}
}
