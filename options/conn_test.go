package options_test

import (
	"context"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/perpstest"
	"github.com/rails-xyz/exchange-go-sdk/options"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const settle = 2 * time.Second

// fakeExchange serves the options product and every stream it publishes.
func fakeExchange(t *testing.T) *perpstest.Server {
	t.Helper()
	return perpstest.NewProduct(t, perpstest.Product{Name: "options", Streams: []string{
		"optionsOrders", "optionsContracts", "optionsQuotes", "optionsPublicOrderBook",
		"optionsPublicTrades", "optionsPositions", "optionsOpenOrders", "optionsRecentOrders",
		"optionsSettlements", "optionsAccountSummary",
	}})
}

// wsClient builds a client whose WebSocket is the fake exchange.
func wsClient(t *testing.T, srv *perpstest.Server, opts ...rails.Option) *options.Client {
	t.Helper()
	base := append([]rails.Option{
		rails.WithEnvironment(rails.Environment{
			AuthURL: "https://auth.invalid", OptionsURL: "https://options.invalid",
			WSURL: srv.URL, TelemetryURL: "https://telemetry.invalid",
		}),
		rails.WithTokenSource(staticToken("jwt")),
	}, opts...)
	c, err := options.NewClient(rails.Credentials{APIKey: "key", APISecret: "secret"}, base...)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

// tradingConn opens a connection with the keepalive off, so the only frames on the wire are
// the test's.
func tradingConn(t *testing.T, srv *perpstest.Server, scope options.Scope, opts ...rails.Option) *options.Conn {
	t.Helper()
	conn, err := wsClient(t, srv, opts...).Connect(t.Context(), scope, options.WithPingInterval(0))
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return conn
}

// waitEvent reads events until one matches, so unrelated lifecycle events do not fail a test.
func waitEvent[T options.Event](t *testing.T, events <-chan options.Event) T {
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

func TestConnectURLCarriesTheScope(t *testing.T) {
	const session = "e4a1c6f0-7b2d-4c8e-9f3a-5d6b8c1e2f4a"
	cases := map[string]struct {
		scope options.Scope
		opts  []options.ConnOption
		want  url.Values
	}{
		"every market": {
			scope: options.ScopeAll(),
			want:  url.Values{"market": {"ALL"}},
		},
		"a market": {
			scope: options.Scope{Market: "BTC-USDT"},
			want:  url.Values{"market": {"BTC-USDT"}},
		},
		"an expiry": {
			scope: options.Scope{Market: "BTC-USDT", Expiration: "24JUN26"},
			want:  url.Values{"market": {"BTC-USDT"}, "expiration": {"24JUN26"}},
		},
		"a strike": {
			scope: options.Scope{Market: "BTC-USDT", Expiration: "24JUN26", Strike: "62000"},
			want:  url.Values{"market": {"BTC-USDT"}, "expiration": {"24JUN26"}, "strike": {"62000"}},
		},
		// The account summary is held for every market, whichever scope subscribes it.
		"one contract, with more streams and a session": {
			scope: options.Scope{Market: "BTC-USDT", Expiration: "24JUN26", Strike: "62000", Type: options.OptionTypeCall},
			opts: []options.ConnOption{
				options.WithSubscriptions(options.SubscriptionQuotes, options.SubscriptionAccountSummary),
				options.WithSessionID(session),
			},
			want: url.Values{
				"market": {"BTC-USDT"}, "expiration": {"24JUN26"}, "strike": {"62000"}, "type": {"C"},
				"subscriptions": {"optionsOrders,optionsAccountSummary,optionsQuotes"}, "sessionId": {session},
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := fakeExchange(t)
			conn, err := wsClient(t, srv).Connect(t.Context(), tc.scope, append(tc.opts, options.WithPingInterval(0))...)
			require.NoError(t, err)
			defer conn.Close()

			hs := srv.Handshakes()
			require.Len(t, hs, 1)
			query := hs[0].Query
			assert.Equal(t, "options", query.Get("product"))
			if _, ok := tc.want["subscriptions"]; !ok {
				assert.Equal(t, "optionsOrders", query.Get("subscriptions"))
				assert.Regexp(t, `^[0-9a-f-]{36}$`, query.Get("sessionId"))
				query.Del("subscriptions")
				query.Del("sessionId")
			}
			query.Del("product")
			assert.Equal(t, tc.want, query)
		})
	}
}

func TestConnectRefusesAMalformedScope(t *testing.T) {
	for name, scope := range map[string]options.Scope{
		"no market":                  {Expiration: "24JUN26"},
		"a strike with no expiry":    {Market: "BTC-USDT", Strike: "62000"},
		"a type with no strike":      {Market: "BTC-USDT", Expiration: "24JUN26", Type: options.OptionTypePut},
		"every market, but narrowed": {Market: "ALL", Expiration: "24JUN26"},
	} {
		t.Run(name, func(t *testing.T) {
			srv := fakeExchange(t)
			_, err := wsClient(t, srv).Connect(t.Context(), scope)
			require.Error(t, err)
			assert.Empty(t, srv.Handshakes(), "a scope the exchange would refuse was dialled")
		})
	}
}

// sdkGoroutines returns the stacks of goroutines still inside SDK code. The fake exchange has
// goroutines of its own and the tests live in options_test; neither counts.
func sdkGoroutines() []string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	for n == len(buf) {
		buf = make([]byte, 2*len(buf))
		n = runtime.Stack(buf, true)
	}
	var running []string
	for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(stack, "exchange-go-sdk/internal/perpstest") {
			continue
		}
		if strings.Contains(stack, "exchange-go-sdk/internal/") || strings.Contains(stack, "exchange-go-sdk/options.") {
			running = append(running, stack)
		}
	}
	return running
}

// Whatever state a connection is in, Close must take every goroutine it started with it.
func TestCloseLeavesNoGoroutines(t *testing.T) {
	cases := map[string]func(t *testing.T, srv *perpstest.Server, conn *options.Conn){
		"an idle connection": func(t *testing.T, _ *perpstest.Server, conn *options.Conn) {
			waitEvent[options.Connected](t, conn.Events())
		},
		"a handover in progress": func(t *testing.T, srv *perpstest.Server, conn *options.Conn) {
			waitEvent[options.Connected](t, conn.Events())
			srv.PushTo(0, map[string]any{
				"resultType": "connectionShutdown",
				"data":       map[string]any{"statusCode": 200, "forceCloseInMs": 60000, "ts": 1756000000000},
			})
			waitEvent[options.ConnectionShutdown](t, conn.Events())
			waitEvent[options.Connected](t, conn.Events()) // the replacement; the old socket is still draining
		},
		"a reconnect wait": func(t *testing.T, srv *perpstest.Server, conn *options.Conn) {
			waitEvent[options.Connected](t, conn.Events())
			srv.CloseConn(0, 1012, "Service restarting. Please reconnect.")
			waitEvent[options.Disconnected](t, conn.Events())
		},
		"a request whose caller gave up": func(t *testing.T, srv *perpstest.Server, conn *options.Conn) {
			srv.SetAutoPong(false)
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			_, err := conn.Ping(ctx)
			require.ErrorIs(t, err, rails.ErrTimeout)
		},
	}

	for name, arrange := range cases {
		t.Run(name, func(t *testing.T) {
			srv := fakeExchange(t)
			client := wsClient(t, srv)
			conn, err := client.Connect(t.Context(), options.ScopeAll(), options.WithPingInterval(0))
			require.NoError(t, err)

			arrange(t, srv, conn)

			conn.Close()
			client.Close()

			deadline := time.Now().Add(settle)
			running := sdkGoroutines()
			for len(running) > 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
				running = sdkGoroutines()
			}
			assert.Empty(t, running, "SDK goroutines still running after Close:\n\n%s", strings.Join(running, "\n\n"))
		})
	}
}
