package perps_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/perpstest"
	"github.com/rails-xyz/exchange-go-sdk/perps"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const settle = 2 * time.Second

// dial builds a client pointed at the fake exchange.
func dial(t *testing.T, srv *perpstest.Server, opts ...rails.Option) *perps.Client {
	t.Helper()
	base := append([]rails.Option{
		rails.WithEnvironment(rails.Environment{
			AuthURL: "https://auth.invalid", OrderURL: "https://order.invalid",
			AccountURL: "https://account.invalid", MarketDataURL: "https://market-data.invalid",
			WSURL: srv.URL, TelemetryURL: "https://telemetry.invalid",
		}),
		rails.WithTokenSource(staticToken("jwt-token")),
	}, opts...)

	c, err := perps.NewClient(rails.Credentials{APIKey: "key", APISecret: "secret"}, base...)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

// waitEvent reads events until one matches, so unrelated lifecycle events do not fail a test.
func waitEvent[T perps.Event](t *testing.T, events <-chan perps.Event) T {
	t.Helper()
	deadline := time.After(settle)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("events closed while waiting for %T", *new(T))
			}
			if typed, is := event.(T); is {
				return typed
			}
		case <-deadline:
			t.Fatalf("no %T within %v", *new(T), settle)
		}
	}
}

func TestConnectSendsTheDocumentedHandshake(t *testing.T) {
	srv := perpstest.New(t)
	conn, err := dial(t, srv).Connect(t.Context(), "BTC-USDT",
		perps.WithSubscriptions(perps.SubscriptionPublicTrades),
		perps.WithPingInterval(0))
	require.NoError(t, err)
	defer conn.Close()

	hs := srv.Handshakes()
	require.Len(t, hs, 1)
	assert.Equal(t, "perpetuals", hs[0].Product)
	assert.Equal(t, "BTC-USDT", hs[0].Market)
	// orders is always present: without it no order command is ever answered.
	assert.Equal(t, "orders,publicTrades", hs[0].Subscriptions)
	assert.NotEmpty(t, hs[0].SessionID, "without one a reconnect could not backfill")
	// Exactly one value: the exchange rejects an offer list.
	assert.Len(t, hs[0].Subprotocols, 1)
	assert.Equal(t, "jwt-token", hs[0].Token)
	assert.True(t, strings.HasPrefix(hs[0].UserAgent, "exchange-go-sdk/"),
		"User-Agent = %q", hs[0].UserAgent)
}

func TestPingMeasuresRoundTrip(t *testing.T) {
	srv := perpstest.New(t)
	conn, err := dial(t, srv).Connect(t.Context(), "BTC-USDT", perps.WithPingInterval(0))
	require.NoError(t, err)
	defer conn.Close()

	rtt, err := conn.Ping(t.Context())
	require.NoError(t, err)
	assert.Positive(t, rtt)
	assert.LessOrEqual(t, rtt, settle)
}

// The keepalive ping is what the connection's latency is measured from, and it travels the
// same path as orders.
func TestKeepalivePingReportsRTTToHooks(t *testing.T) {
	srv := perpstest.New(t)
	rtt := make(chan time.Duration, 4)
	conn, err := dial(t, srv, rails.WithHooks(rttHooks(rtt))).
		Connect(t.Context(), "BTC-USDT", perps.WithPingInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer conn.Close()

	select {
	case got := <-rtt:
		assert.Positive(t, got)
	case <-time.After(settle):
		t.Fatal("no PingRTT hook")
	}
	assert.Equal(t, "ping", srv.NextMessage(settle).Message)
}

// 1012 is the only close code the exchange emits, and it means the server is going away.
func TestServerRestartCloseReconnects(t *testing.T) {
	srv := perpstest.New(t)
	conn, err := dial(t, srv).Connect(t.Context(), "BTC-USDT", perps.WithPingInterval(0))
	require.NoError(t, err)
	defer conn.Close()

	waitEvent[perps.Connected](t, conn.Events())
	srv.CloseConn(0, 1012, "Service restarting. Please reconnect.")

	assert.Equal(t, rails.ReasonServerShutdown, waitEvent[perps.Disconnected](t, conn.Events()).Reason)
	srv.WaitConnections(2, 10*time.Second)
	waitEvent[perps.Connected](t, conn.Events())
}

// The replacement is opened before the draining socket is dropped, and that socket keeps
// delivering in the meantime — a graceful close would discard whatever was still in flight.
func TestConnectionShutdownHandsOverWithoutLosingFrames(t *testing.T) {
	srv := perpstest.New(t)
	conn, err := dial(t, srv).Connect(t.Context(), "BTC-USDT", perps.WithPingInterval(0))
	require.NoError(t, err)
	defer conn.Close()
	waitEvent[perps.Connected](t, conn.Events())

	srv.PushTo(0, map[string]any{
		"resultType": "connectionShutdown",
		"data":       map[string]any{"statusCode": 200, "forceCloseInMs": 60000, "ts": 1756000000000},
	})
	notice := waitEvent[perps.ConnectionShutdown](t, conn.Events())
	assert.Equal(t, time.Minute, notice.ForceCloseIn)

	// The replacement has to be up before the old socket is touched, and a drain must not wait
	// out the reconnect budget the way an unexpected drop does.
	started := time.Now()
	srv.WaitConnections(2, 5*time.Second)
	waitEvent[perps.Connected](t, conn.Events())
	assert.LessOrEqual(t, time.Since(started), 2*time.Second,
		"a drain should not wait out the connect budget")

	// Only now push on the draining socket. It is still live, so the frame still arrives.
	srv.PushTo(0, map[string]any{
		"resultType": "completeFillOrder",
		"market":     "BTC-USDT",
		"data": map[string]any{"statusCode": 200, "body": "order filled",
			"order": map[string]any{"orderId": "after-handover", "orderStatus": "active"}},
	})
	assert.Equal(t, "after-handover", waitEvent[perps.OrderFilled](t, conn.Events()).Order.OrderID,
		"the draining socket stopped delivering")
}

func TestSessionBackfillBecomesAnEvent(t *testing.T) {
	cases := map[string]struct {
		resumed bool
		expired []string
	}{
		"resumed": {resumed: true, expired: []string{}},
		"expired": {resumed: false, expired: []string{"BTC-USDT"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := perpstest.New(t)
			conn, err := dial(t, srv).Connect(t.Context(), "BTC-USDT", perps.WithPingInterval(0))
			require.NoError(t, err)
			defer conn.Close()

			srv.Push(map[string]any{
				"resultType": "sessionBackfill",
				"data": map[string]any{
					"sessionId": "d785d51c-054e-4e33-9738-92d04ede9fab", "resumed": tc.resumed,
					"from": 1784834389615, "to": 1784834449615, "events": 6,
					"backfilledMarkets": []string{"ETH-USDT"}, "expiredMarkets": tc.expired,
				},
			})

			got := waitEvent[perps.SessionBackfill](t, conn.Events())
			assert.Equal(t, tc.resumed, got.Resumed)
			assert.Equal(t, tc.expired, got.ExpiredMarkets)
			assert.Equal(t, 6, got.Events)
			assert.NotEmpty(t, got.SessionID)
		})
	}
}

// A stream this release has no type for still reaches the caller whole.
func TestUnrecognisedFrameBecomesUnknown(t *testing.T) {
	srv := perpstest.New(t)
	conn, err := dial(t, srv).Connect(t.Context(), "BTC-USDT", perps.WithPingInterval(0))
	require.NoError(t, err)
	defer conn.Close()

	srv.Push(map[string]any{"resultType": "somethingNew", "data": map[string]any{"ts": 1}})

	got := waitEvent[perps.Unknown](t, conn.Events())
	assert.Equal(t, "somethingNew", got.ResultType)
	assert.Contains(t, string(got.Raw), "somethingNew", "the whole frame should survive")
}

// A 429 on the upgrade carries the error envelope, and names its wait in milliseconds under its
// own header as well as in whole seconds.
func TestRateLimitedUpgradeSurfacesTheCodeAndRetryAfter(t *testing.T) {
	srv := perpstest.New(t)
	srv.SetReject(func(w http.ResponseWriter, _ *http.Request) bool {
		w.Header().Set("retry-after-ms", "1500")
		w.Header().Set("retry-after", "2")
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"slug":"TOO_MANY_REQUESTS","code":"SYS-0003"},"message":"Too Many Requests: retry in 1500ms"}`))
		return true
	})

	_, err := dial(t, srv).Connect(t.Context(), "BTC-USDT")

	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	assert.ErrorIs(t, err, rails.ErrRateLimited)
	assert.Equal(t, rails.CodeTooManyRequests, apiErr.Code)
	assert.Equal(t, "TOO_MANY_REQUESTS", apiErr.Slug)
	assert.Equal(t, "Too Many Requests: retry in 1500ms", apiErr.Message)
	assert.Equal(t, 1500*time.Millisecond, apiErr.RetryAfter)
}

func TestDrainingUpgradeSurfacesRetryAfter(t *testing.T) {
	srv := perpstest.New(t)
	srv.SetReject(func(w http.ResponseWriter, _ *http.Request) bool {
		w.Header().Set("retry-after", "5")
		http.Error(w, "DRAINING", http.StatusServiceUnavailable)
		return true
	})

	_, err := dial(t, srv).Connect(t.Context(), "BTC-USDT")

	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusServiceUnavailable, apiErr.StatusCode)
	assert.Equal(t, 5*time.Second, apiErr.RetryAfter)
}

// Close must return even when nobody is draining Events, and must leave the channel closed so
// a consumer ranging over it finishes too.
func TestCloseCompletesWithAStalledConsumer(t *testing.T) {
	srv := perpstest.New(t)
	conn, err := dial(t, srv).Connect(t.Context(), "BTC-USDT",
		perps.WithPingInterval(0), perps.WithEventBuffer(1))
	require.NoError(t, err)

	// Fill the buffer and leave it full: nothing reads these.
	for range 5 {
		srv.Push(map[string]any{"resultType": "somethingNew", "data": map[string]any{"ts": 1}})
	}
	time.Sleep(200 * time.Millisecond)

	closed := make(chan struct{})
	go func() { conn.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(settle):
		t.Fatal("Close hung on a consumer that had stopped reading")
	}

	for range conn.Events() { //nolint:revive // draining to completion is the point
	}
}

func TestSendAfterCloseIsNotSent(t *testing.T) {
	srv := perpstest.New(t)
	conn, err := dial(t, srv).Connect(t.Context(), "BTC-USDT", perps.WithPingInterval(0))
	require.NoError(t, err)
	conn.Close()

	_, err = conn.Ping(t.Context())
	assert.ErrorIs(t, err, rails.ErrNotSent, "nothing left, so the caller may send again")
	assert.NotErrorIs(t, err, rails.ErrConnClosed)
}

// rttHooks hands every keepalive round trip to a channel.
func rttHooks(rtt chan time.Duration) rails.Hooks {
	return rails.Hooks{PingRTT: func(_ rails.ConnInfo, d time.Duration) {
		select {
		case rtt <- d:
		default:
		}
	}}
}

// Control pings are answered only while a read is in flight, and a client that stops answering
// is silently dropped from the exchange's drain notifications, so the reader must never stop.
func TestServerControlPingsAreAnswered(t *testing.T) {
	srv := perpstest.New(t)
	srv.SetControlPingInterval(20 * time.Millisecond)

	conn, err := dial(t, srv).Connect(t.Context(), "BTC-USDT", perps.WithPingInterval(0))
	require.NoError(t, err)
	defer conn.Close()
	waitEvent[perps.Connected](t, conn.Events())

	time.Sleep(300 * time.Millisecond) // a dozen or so control pings
	assert.Zero(t, srv.MissedControlPongs(), "control pings went unanswered")
	assert.Equal(t, 1, srv.Connections(), "the socket should not have churned")
}

// One unrecognised stream name rejects the whole handshake rather than being dropped.
func TestInvalidSubscriptionIsRejected(t *testing.T) {
	srv := perpstest.New(t)
	_, err := dial(t, srv).Connect(t.Context(), "BTC-USDT",
		perps.WithSubscriptions(perps.Subscription("noSuchStream")))

	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.StatusCode)
}

// A connection to every market cannot route a command that names none, so it is refused before
// anything is sent.
func TestCommandsOnAnAllMarketsConnectionMustNameAMarket(t *testing.T) {
	srv := perpstest.New(t)
	conn, err := dial(t, srv).Connect(t.Context(), perps.MarketAll, perps.WithPingInterval(0))
	require.NoError(t, err)
	defer conn.Close()

	// Bounded, so a command that did go out fails here instead of waiting for an answer.
	ctx, cancel := context.WithTimeout(t.Context(), settle)
	defer cancel()
	commands := map[string]func() error{
		"CreateOrder": func() error { _, err := conn.CreateOrder(ctx, limitOrder()); return err },
		"ModifyOrder": func() error {
			_, err := conn.ModifyOrder(ctx, perps.ModifyOrderRequest{OrderID: "o", Price: "1", Quantity: "1"})
			return err
		},
		"CancelOrder":         func() error { _, err := conn.CancelOrder(ctx, perps.CancelOrderRequest{OrderID: "o"}); return err },
		"UpdateMargin":        func() error { _, err := conn.UpdateMargin(ctx, perps.UpdateMarginRequest{Margin: "1"}); return err },
		"EmitPublicTrades":    func() error { _, err := conn.EmitPublicTrades(ctx, ""); return err },
		"EmitPublicOrderBook": func() error { _, err := conn.EmitPublicOrderBook(ctx, ""); return err },
	}
	for name, send := range commands {
		assert.ErrorContains(t, send(), "market is required", name)
	}

	_, err = conn.Ping(ctx)
	require.NoError(t, err)
	received := srv.Received()
	require.Len(t, received, 1, "only the ping should have reached the exchange")
	assert.Equal(t, "ping", received[0].Message)
}
