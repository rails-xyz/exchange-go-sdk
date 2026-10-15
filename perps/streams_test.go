package perps_test

import (
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/perpstest"
	"github.com/rails-xyz/exchange-go-sdk/internal/spectest"
	"github.com/rails-xyz/exchange-go-sdk/perps"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A refusal is answered under the command's name, not the confirmation's, and is still not an
// event.
func TestSubscribeRefusalIsNotAnEvent(t *testing.T) {
	srv := perpstest.New(t)
	conn := tradingConn(t, srv)
	waitEvent[perps.Connected](t, conn.Events())

	got := sendCommand(t, conn, "subscribe", "")
	crid := command(t, srv, "subscribe").ClientRequestID()
	srv.Push(map[string]any{"resultType": "subscribe", "data": map[string]any{
		"statusCode": 429, "body": "Subscription limit reached (max 100)", "clientRequestId": crid,
		"error": map[string]string{"slug": "SUBSCRIPTION_LIMIT", "code": "SYS-0012"},
	}})

	require.ErrorIs(t, spectest.Receive(t, got).err, rails.ErrRateLimited)

	srv.Push(envelope("somethingNew", map[string]any{"statusCode": 200}))
	assert.Equal(t, "somethingNew", waitEvent[perps.Unknown](t, conn.Events()).ResultType,
		"the refusal reached Events")
}

// A stream added at runtime is asked for again when the connection dials anew.
func TestRuntimeSubscriptionsSurviveAReconnect(t *testing.T) {
	srv := perpstest.New(t)
	conn := tradingConn(t, srv)
	waitEvent[perps.Connected](t, conn.Events())

	got := sendCommand(t, conn, "subscribe", "")
	crid := command(t, srv, "subscribe").ClientRequestID()
	srv.Push(map[string]any{"resultType": "subscribed", "data": map[string]any{
		"statusCode": 200, "clientRequestId": crid, "activeSubscriptions": []string{"orders", "trades"},
	}})
	require.NoError(t, spectest.Receive(t, got).err)

	srv.CloseConn(0, 1012, "Service restarting. Please reconnect.")
	srv.WaitConnections(2, 10*time.Second)
	assert.Equal(t, "orders,trades", srv.Handshakes()[1].Subscriptions)
}
