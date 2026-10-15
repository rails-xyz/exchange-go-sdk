package perps_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/perpstest"
	"github.com/rails-xyz/exchange-go-sdk/perps"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clock is a settable time source.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// dayStart is the start of a day, and so of every window the pacer meters.
var dayStart = time.Unix(1_699_920_000, 0)

// failFastPacer reads the reservation arithmetic through refusals, without sleeping.
func failFastPacer(t *testing.T, opts ...perps.PacerOption) (*perps.Pacer, *clock) {
	t.Helper()
	clk := &clock{now: dayStart}
	p := perps.NewPacer(append(opts, perps.WithFailFast())...)
	perps.SetPacerClock(p, clk.Now)
	return p, clk
}

// refusedFor is the wait behind a refusal, or zero for a call that may go now.
func refusedFor(t *testing.T, p *perps.Pacer, class rails.OpClass) time.Duration {
	t.Helper()
	err := p.Wait(t.Context(), class, "op")
	if err == nil {
		return 0
	}
	require.ErrorIs(t, err, rails.ErrNotSent)
	require.ErrorIs(t, err, rails.ErrPaced)
	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "op", apiErr.Op)
	require.Positive(t, apiErr.RetryAfter)
	return apiErr.RetryAfter
}

// A window's quota may be spent at once. Past it, a call waits until the sliding window has let
// enough of the earlier calls go, as the exchange counts them, and a refusal costs nothing.
func TestPacerWaitsForTheWindowToSlide(t *testing.T) {
	p, clk := failFastPacer(t, perps.WithLimits(perps.Limits{perps.ClassOrderCreate: {PerMinute: 10}}))

	for i := range 10 {
		assert.Zero(t, refusedFor(t, p, perps.ClassOrderCreate), "call %d", i+1)
	}
	// The next minute still counts the last one's 10 in full at its start, one fewer every 6s.
	assert.Equal(t, 66*time.Second, refusedFor(t, p, perps.ClassOrderCreate))
	assert.Equal(t, 66*time.Second, refusedFor(t, p, perps.ClassOrderCreate), "a refusal is not a booking")
	assert.Zero(t, refusedFor(t, p, perps.ClassOrderCancel), "another class has its own budget")

	clk.Advance(66 * time.Second)
	assert.Zero(t, refusedFor(t, p, perps.ClassOrderCreate))
	assert.Equal(t, 6*time.Second, refusedFor(t, p, perps.ClassOrderCreate))
}

func TestPacerClassAllCapsEveryClassTogether(t *testing.T) {
	p, _ := failFastPacer(t, perps.WithLimits(perps.Limits{
		perps.ClassAll:         {PerMinute: 15},
		perps.ClassOrderCreate: {PerMinute: 10},
		perps.ClassOrderCancel: {PerMinute: 10},
	}))

	for range 8 {
		require.Zero(t, refusedFor(t, p, perps.ClassOrderCreate))
	}
	for range 7 {
		require.Zero(t, refusedFor(t, p, perps.ClassOrderCancel))
	}
	assert.Equal(t, 64*time.Second, refusedFor(t, p, perps.ClassOrderCancel), "the shared window is spent")
	assert.Equal(t, 64*time.Second, refusedFor(t, p, perps.ClassOrderCreate))
}

// The per-second window is the one that bounds a burst; the minute window only counts.
func TestPacerBurstWaitsOnTheSecondWindow(t *testing.T) {
	p, _ := failFastPacer(t, perps.WithLimits(perps.Limits{perps.ClassOrderCreate: {PerSecond: 2, PerMinute: 100}}))

	require.Zero(t, refusedFor(t, p, perps.ClassOrderCreate))
	require.Zero(t, refusedFor(t, p, perps.ClassOrderCreate))
	assert.Equal(t, 1500*time.Millisecond, refusedFor(t, p, perps.ClassOrderCreate))
}

// A caller that gives up during the wait gets the context's error and its booking back.
func TestPacerReleasesTheBookingWhenTheCallerGivesUp(t *testing.T) {
	clk := &clock{now: dayStart}
	p := perps.NewPacer(perps.WithLimits(perps.Limits{perps.ClassOrderCreate: {PerMinute: 1}}))
	perps.SetPacerClock(p, clk.Now)

	require.NoError(t, p.Wait(t.Context(), perps.ClassOrderCreate, "createOrder"))

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	assert.ErrorIs(t, p.Wait(cancelled, perps.ClassOrderCreate, "createOrder"), context.Canceled)

	// Had the abandoned call stayed booked, this one would wait another two minutes.
	clk.Advance(2 * time.Minute)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	assert.NoError(t, p.Wait(ctx, perps.ClassOrderCreate, "createOrder"))
}

// A rate limit or a creation ban holds its class back for the Retry-After plus a margin, capped;
// any other failure changes nothing.
func TestPacerCoolsDownOnTheExchangesWord(t *testing.T) {
	const margin = 250 * time.Millisecond
	cases := map[string]struct {
		err  error
		want time.Duration // zero: no cooldown
	}{
		"429": {
			err:  &rails.Error{StatusCode: 429, Code: rails.CodeTooManyRequests, RetryAfter: 3 * time.Second},
			want: 3*time.Second + margin,
		},
		"429 past the cap": {
			err:  &rails.Error{StatusCode: 429, Code: rails.CodeTooManyRequests, RetryAfter: 10 * time.Minute},
			want: 5 * time.Minute,
		},
		"creation ban": {
			err:  &rails.Error{StatusCode: 403, Code: perps.CodeCreateOrderBanned, RetryAfter: 300 * time.Second},
			want: 5 * time.Minute,
		},
		"429 without a Retry-After": {
			err: &rails.Error{StatusCode: 429, Code: rails.CodeTooManyRequests},
		},
		"400": {
			err: &rails.Error{StatusCode: 400, Code: rails.CodeBadRequest},
		},
		"not an exchange error": {
			err: errors.New("dial tcp: connection refused"),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			p, _ := failFastPacer(t)
			p.Observe(perps.ClassOrderCreate, tc.err)

			assert.Equal(t, tc.want, refusedFor(t, p, perps.ClassOrderCreate))
			assert.Zero(t, refusedFor(t, p, perps.ClassOrderCancel), "another class is not on cooldown")
		})
	}
}

// Calls booked during a cooldown go out after it, no more at once than the window allows.
func TestPacerSpacesCallsAfterACooldown(t *testing.T) {
	clk := &clock{now: dayStart}
	p := perps.NewPacer(perps.WithLimits(perps.Limits{perps.ClassOrderCreate: {PerSecond: 4}}))
	perps.SetPacerClock(p, clk.Now)
	var mu sync.Mutex
	var delays []time.Duration
	c := newExchange(t, func(http.ResponseWriter, *http.Request) {}).client(
		rails.WithPacer(p),
		rails.WithHooks(rails.Hooks{Paced: func(info rails.PaceInfo) {
			mu.Lock()
			defer mu.Unlock()
			delays = append(delays, info.Delay)
		}}))
	defer c.Close()

	p.Observe(perps.ClassOrderCreate, &rails.Error{Code: rails.CodeTooManyRequests, RetryAfter: 100 * time.Millisecond})

	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() { assert.NoError(t, p.Wait(t.Context(), perps.ClassOrderCreate, "createOrder")) })
	}
	wg.Wait()

	// The cooldown is 350ms and four fit at its end. The fifth waits for the next second, a
	// quarter into it, when the four have slid out far enough; the sixth a quarter more.
	mu.Lock()
	defer mu.Unlock()
	slices.Sort(delays)
	assert.Equal(t, []time.Duration{
		350 * time.Millisecond, 350 * time.Millisecond, 350 * time.Millisecond, 350 * time.Millisecond,
		1250 * time.Millisecond, 1500 * time.Millisecond,
	}, delays)
}

// A shorter throttle arriving during a longer cooldown does not cut it short.
func TestPacerKeepsTheLongerCooldown(t *testing.T) {
	p, _ := failFastPacer(t)
	p.Observe(perps.ClassOrderCreate, &rails.Error{Code: rails.CodeTooManyRequests, RetryAfter: 10 * time.Second})
	p.Observe(perps.ClassOrderCreate, &rails.Error{Code: rails.CodeTooManyRequests, RetryAfter: time.Second})
	assert.Equal(t, 10*time.Second+250*time.Millisecond, refusedFor(t, p, perps.ClassOrderCreate))
}

// jwt builds a token carrying the claims.
func jwt(t *testing.T, claims map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	require.NoError(t, err)
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"none"}`)) + "." + enc(payload) + ".sig"
}

// The preset follows the token's account type; ceilings given by hand survive the switch.
func TestPacerPicksThePresetFromTheToken(t *testing.T) {
	p, _ := failFastPacer(t, perps.WithLimitsFromToken(),
		perps.WithLimits(perps.Limits{perps.ClassOrderCancel: {PerSecond: 3}}))
	token := jwt(t, map[string]any{"accountType": "market_maker", "exp": time.Now().Add(time.Hour).Unix()})
	srv := perpstest.New(t)
	client := dial(t, srv, rails.WithPacer(p), rails.WithTokenSource(staticToken(token)))

	// DefaultLimits until a token is seen.
	for range 250 {
		require.Zero(t, refusedFor(t, p, perps.ClassOrderCreate))
	}
	require.Positive(t, refusedFor(t, p, perps.ClassOrderCreate))

	conn, err := client.Connect(t.Context(), market, perps.WithPingInterval(0))
	require.NoError(t, err)
	defer conn.Close()

	// The market-maker minute is 10 000 wide, so the 251st call now fits.
	assert.Zero(t, refusedFor(t, p, perps.ClassOrderCreate))
	for range 3 {
		require.Zero(t, refusedFor(t, p, perps.ClassOrderCancel))
	}
	assert.Positive(t, refusedFor(t, p, perps.ClassOrderCancel), "the explicit per-second ceiling still holds")
}

// recordingPacer notes every class it is asked about.
type recordingPacer struct {
	mu       sync.Mutex
	waits    []rails.OpClass
	observed []rails.OpClass
}

func (p *recordingPacer) Wait(_ context.Context, class rails.OpClass, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.waits = append(p.waits, class)
	return nil
}

func (p *recordingPacer) Observe(class rails.OpClass, _ error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observed = append(p.observed, class)
}

func (p *recordingPacer) seen() (waits, observed []rails.OpClass) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waits, p.observed
}

// Every budgeted call runs through the pacer under its class and reports its failure; the
// escape hatch, the market-data reads and the keepalive never touch it.
func TestMeteredCallsRunThroughThePacer(t *testing.T) {
	pacer := &recordingPacer{}
	e := newExchange(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/open-orders":
			_, _ = w.Write([]byte(`{"openOrders":[]}`))
		case "/api/v1/positions":
			_, _ = w.Write([]byte(`{"positions":[]}`))
		case "/api/v1/order":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"slug":"NOT_FOUND","code":"SYS-0007"},"message":"not found"}`))
		case "/api/v1/markets":
			_, _ = w.Write([]byte(`{"markets":[]}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	})
	client := e.client(rails.WithPacer(pacer))

	_, err := client.OpenOrders(t.Context(), market)
	require.NoError(t, err)
	_, err = client.Positions(t.Context(), market)
	require.NoError(t, err)
	_, err = client.OrderByID(t.Context(), market, "missing")
	require.ErrorIs(t, err, rails.ErrNotFound)
	_, err = client.Deposits(t.Context(), "")
	require.NoError(t, err)
	require.NoError(t, client.UpdateLeverage(t.Context(), market, "5"))
	require.NoError(t, client.CancelAllOrders(t.Context(), perps.CancelAllOrdersRequest{Market: market}))
	_, err = client.Markets(t.Context())
	require.NoError(t, err)

	waits, observed := pacer.seen()
	assert.Equal(t, []rails.OpClass{
		perps.ClassReadOrders, perps.ClassReadAccount, perps.ClassReadOrders, perps.ClassReadLedger, perps.ClassAccountWrite,
	}, waits)
	assert.Equal(t, []rails.OpClass{perps.ClassReadOrders}, observed)

	srv := perpstest.New(t)
	conn := tradingConn(t, srv, rails.WithPacer(pacer))
	_, err = conn.Ping(t.Context())
	require.NoError(t, err)
	require.Equal(t, "ping", srv.NextMessage(settle).Message)
	go func() {
		_, _ = conn.CreateOrder(t.Context(), limitOrder())
	}()
	crid := command(t, srv, "createOrder").ClientRequestID()
	srv.Push(envelope("createOrder", map[string]any{
		"statusCode": 400, "body": "insufficient balance", "clientRequestId": crid,
		"error": map[string]string{"slug": "CREATE_ORDER_INSUFFICIENT_BALANCE", "code": "PRP-0101"},
	}))
	go func() {
		_, _ = conn.CancelOrder(t.Context(), perps.CancelOrderRequest{OrderID: "01JGPRJC7KHY8GP4FT6WRQG193"})
	}()
	crid = command(t, srv, "cancelOrder").ClientRequestID()
	srv.Push(envelope("cancelOrder", map[string]any{
		"statusCode": 200, "body": "order cancelled", "clientRequestId": crid,
		"order": orderObject(crid, "01JGPRJC7KHY8GP4FT6WRQG193", "cancelled"),
	}))
	go func() {
		_, _ = conn.ModifyOrder(t.Context(), perps.ModifyOrderRequest{OrderID: "01JGPRJC7KHY8GP4FT6WRQG193", Price: "1", Quantity: "1"})
	}()
	crid = command(t, srv, "modifyOrder").ClientRequestID()
	srv.Push(envelope("modifyOrder", map[string]any{
		"statusCode": 409, "body": "Order is not active", "clientRequestId": crid,
		"error": map[string]string{"slug": "MODIFY_ORDER_NOT_ACTIVE", "code": "PRP-0305"},
	}))
	go func() {
		_, _ = conn.UpdateMargin(t.Context(), perps.UpdateMarginRequest{Margin: "201"})
	}()
	crid = command(t, srv, "updateMargin").ClientRequestID()
	srv.Push(envelope("updateMargin", map[string]any{"statusCode": 200, "body": "margin updated successfully", "clientRequestId": crid}))
	go func() {
		_, _ = conn.Subscribe(t.Context(), perps.SubscriptionTrades)
	}()
	crid = command(t, srv, "subscribe").ClientRequestID()
	srv.Push(map[string]any{"resultType": "subscribed", "data": map[string]any{
		"statusCode": 200, "clientRequestId": crid, "activeSubscriptions": []string{"orders", "trades"},
	}})

	assert.Eventually(t, func() bool {
		waits, observed := pacer.seen()
		return len(waits) == 9 && len(observed) == 3
	}, settle, 10*time.Millisecond)
	waits, observed = pacer.seen()
	assert.Equal(t, []rails.OpClass{perps.ClassOrderCreate, perps.ClassOrderCancel, perps.ClassOrderModify, perps.ClassAccountWrite}, waits[5:])
	assert.Equal(t, []rails.OpClass{perps.ClassOrderCreate, perps.ClassOrderModify}, observed[1:])
}

// A rate limit reported outside any command names no class, so every class is held back.
func TestBareRateLimitCoolsDownEveryClass(t *testing.T) {
	var paced []rails.PaceInfo
	var throttled []rails.OpClass
	var mu sync.Mutex
	pacer := perps.NewPacer(perps.WithFailFast())
	srv := perpstest.New(t)
	conn := tradingConn(t, srv, rails.WithPacer(pacer), rails.WithHooks(rails.Hooks{
		Paced: func(info rails.PaceInfo) {
			mu.Lock()
			defer mu.Unlock()
			paced = append(paced, info)
		},
		Throttled: func(class rails.OpClass, _ time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			throttled = append(throttled, class)
		},
	}))

	srv.Push(map[string]any{
		"statusCode": 429, "body": "Too many messages",
		"error": map[string]string{"slug": "TOO_MANY_REQUESTS", "code": "SYS-0003"}, "retryAfterSec": 2,
	})
	waitEvent[perps.ServerError](t, conn.Events())

	_, err := conn.CreateOrder(t.Context(), limitOrder())
	require.ErrorIs(t, err, rails.ErrPaced)
	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "createOrder", apiErr.Op)
	assert.InDelta(t, 2250*time.Millisecond, apiErr.RetryAfter, float64(100*time.Millisecond))
	assert.Empty(t, srv.Received(), "nothing went out")

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []rails.OpClass{perps.ClassAll}, throttled)
	require.Len(t, paced, 1)
	assert.Equal(t, rails.PaceInfo{Class: perps.ClassOrderCreate, Op: "createOrder", Delay: apiErr.RetryAfter, Refused: true}, paced[0])
}
