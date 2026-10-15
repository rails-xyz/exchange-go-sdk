package perps_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/perpstest"
	"github.com/rails-xyz/exchange-go-sdk/perps"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkGoroutines returns the stacks of goroutines still inside SDK code. The fake exchange has
// goroutines of its own and the tests live in perps_test; neither counts.
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
		if strings.Contains(stack, "exchange-go-sdk/internal/") || strings.Contains(stack, "exchange-go-sdk/perps.") {
			running = append(running, stack)
		}
	}
	return running
}

// assertNoSDKGoroutines fails if SDK goroutines are still running once everything is closed.
func assertNoSDKGoroutines(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(settle)
	var running []string
	for time.Now().Before(deadline) {
		if running = sdkGoroutines(); len(running) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("%d SDK goroutines still running after Close:\n\n%s", len(running), strings.Join(running, "\n\n"))
}

// The SDK lives inside a process for days, so a goroutine leaked per connection or per
// reconnect compounds. Whatever state a connection is in, Close must take everything with it.
func TestCloseLeavesNoGoroutines(t *testing.T) {
	cases := map[string]func(t *testing.T, srv *perpstest.Server, conn *perps.Conn){
		"an idle connection": func(t *testing.T, _ *perpstest.Server, conn *perps.Conn) {
			waitEvent[perps.Connected](t, conn.Events())
		},
		"a handover in progress": func(t *testing.T, srv *perpstest.Server, conn *perps.Conn) {
			waitEvent[perps.Connected](t, conn.Events())
			srv.PushTo(0, map[string]any{
				"resultType": "connectionShutdown",
				"data":       map[string]any{"statusCode": 200, "forceCloseInMs": 60000, "ts": 1756000000000},
			})
			waitEvent[perps.ConnectionShutdown](t, conn.Events())
			waitEvent[perps.Connected](t, conn.Events()) // the replacement; the old socket is still draining
		},
		"a reconnect wait": func(t *testing.T, srv *perpstest.Server, conn *perps.Conn) {
			waitEvent[perps.Connected](t, conn.Events())
			srv.CloseConn(0, 1012, "Service restarting. Please reconnect.")
			waitEvent[perps.Disconnected](t, conn.Events()) // the owner is now waiting out the connect budget
		},
		"a request whose caller gave up": func(t *testing.T, srv *perpstest.Server, conn *perps.Conn) {
			srv.SetAutoPong(false)
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			_, err := conn.Ping(ctx)
			require.ErrorIs(t, err, rails.ErrTimeout)
		},
	}

	for name, arrange := range cases {
		t.Run(name, func(t *testing.T) {
			srv := perpstest.New(t)
			client := dial(t, srv)
			conn, err := client.Connect(t.Context(), market, perps.WithPingInterval(0))
			require.NoError(t, err)

			arrange(t, srv, conn)

			conn.Close()
			client.Close()
			assertNoSDKGoroutines(t)
		})
	}
}

// Closing is bounded by one attempt at the ingest, and the reporter has to be gone after it.
func TestCloseWithAHungIngestLeavesNoGoroutines(t *testing.T) {
	release := make(chan struct{})
	defer close(release) // before the server's cleanup, which waits for its handler
	ingest := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(ingest.Close)

	srv := perpstest.New(t)
	client := dial(t, srv, rails.WithEnvironment(rails.Environment{
		AuthURL: "https://auth.invalid", OrderURL: "https://order.invalid",
		AccountURL: "https://account.invalid", MarketDataURL: "https://market-data.invalid",
		WSURL: srv.URL, TelemetryURL: ingest.URL,
	}))
	conn, err := client.Connect(t.Context(), market, perps.WithPingInterval(0))
	require.NoError(t, err)
	_, err = conn.Ping(t.Context()) // something to report
	require.NoError(t, err)
	conn.Close()

	start := time.Now()
	client.Close()
	assert.Less(t, time.Since(start), 8*time.Second, "Close outlived the send timeout on a hung ingest")
	assertNoSDKGoroutines(t)
}
