package options_test

import (
	"encoding/base64"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/options"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cooldownMargin = 250 * time.Millisecond

// failFastPacer reads the reservation arithmetic through refusals, on a clock that stands still
// at the start of a day, and so of every window.
func failFastPacer(t *testing.T, opts ...options.PacerOption) *options.Pacer {
	t.Helper()
	p := options.NewPacer(append(opts, options.WithFailFast())...)
	now := time.Unix(1_699_920_000, 0)
	options.SetPacerClock(p, func() time.Time { return now })
	return p
}

// refusedFor is the wait behind a refusal, or zero for a call that may go now.
func refusedFor(t *testing.T, p *options.Pacer, class rails.OpClass) time.Duration {
	t.Helper()
	err := p.Wait(t.Context(), class, "op")
	if err == nil {
		return 0
	}
	require.ErrorIs(t, err, rails.ErrPaced)
	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	return apiErr.RetryAfter
}

func TestPacerMetersEveryWindow(t *testing.T) {
	for name, tc := range map[string]struct {
		limit options.Limit
		want  time.Duration
	}{
		// The next window, half through, when one of the two has slid out.
		"second": {limit: options.Limit{PerSecond: 2}, want: 1500 * time.Millisecond},
		"minute": {limit: options.Limit{PerMinute: 2}, want: 90 * time.Second},
		"hour":   {limit: options.Limit{PerHour: 2}, want: 90 * time.Minute},
		"day":    {limit: options.Limit{PerDay: 2}, want: 36 * time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			p := failFastPacer(t, options.WithLimits(options.Limits{options.ClassOrderReduceOnly: tc.limit}))
			require.Zero(t, refusedFor(t, p, options.ClassOrderReduceOnly))
			require.Zero(t, refusedFor(t, p, options.ClassOrderReduceOnly))
			assert.Equal(t, tc.want, refusedFor(t, p, options.ClassOrderReduceOnly))
		})
	}
}

// jwt builds a token carrying the claims.
func jwt(t *testing.T, claims map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	require.NoError(t, err)
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"none"}`)) + "." + enc(payload) + ".sig"
}

// The default budget holds until the token names a market maker.
func TestPacerPicksThePresetFromTheToken(t *testing.T) {
	p := failFastPacer(t, options.WithLimitsFromToken())
	token := jwt(t, map[string]any{"accountType": "market_maker", "exp": time.Now().Add(time.Hour).Unix()})

	for range 25 {
		require.Zero(t, refusedFor(t, p, options.ClassOrderCreate))
	}
	require.Positive(t, refusedFor(t, p, options.ClassOrderCreate), "the default second is 25 wide")

	tradingConn(t, fakeExchange(t), options.ScopeAll(), rails.WithPacer(p), rails.WithTokenSource(staticToken(token)))
	assert.Zero(t, refusedFor(t, p, options.ClassOrderCreate), "the market-maker second is 4 000 wide")
}

func TestPacerCoolsDownOnTheExchangesWord(t *testing.T) {
	for name, tc := range map[string]struct {
		opts []options.PacerOption
		err  error
		want time.Duration // zero: no cooldown
	}{
		"rate limited": {
			err:  &rails.Error{StatusCode: 400, Code: options.CodeRateLimited},
			want: time.Second + cooldownMargin,
		},
		"rate limited, cooldown set": {
			opts: []options.PacerOption{options.WithRateLimitedCooldown(3 * time.Second)},
			err:  &rails.Error{StatusCode: 400, Code: options.CodeRateLimited},
			want: 3*time.Second + cooldownMargin,
		},
		"429": {
			err:  &rails.Error{StatusCode: 429, Code: rails.CodeTooManyRequests, RetryAfter: 3 * time.Second},
			want: 3*time.Second + cooldownMargin,
		},
		"429 without a Retry-After": {
			err: &rails.Error{StatusCode: 429, Code: rails.CodeTooManyRequests},
		},
		"another refusal": {
			err: &rails.Error{StatusCode: 400, Code: options.CodeTooManyInflightOrders},
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := failFastPacer(t, tc.opts...)
			p.Observe(options.ClassOrderCreate, tc.err)
			assert.Equal(t, tc.want, refusedFor(t, p, options.ClassOrderCreate))
		})
	}
}

// A refusal no command of the connection waited for cools the class it names, and one outside
// any command cools every class.
func TestRefusalsOnTheConnectionCoolThePacer(t *testing.T) {
	refused := func(op string) map[string]any {
		return frame(op, testContract, map[string]any{
			"statusCode": 400, "body": "rate limit exceeded", "clientRequestId": "4b0d7a7e-5f1c-4c55-9d7e-2f1a0c9b3e11",
			"error": map[string]string{"slug": "RATE_LIMITED", "code": "OPT-0006"},
		})
	}
	classes := []rails.OpClass{options.ClassOrderCreate, options.ClassOrderModify, options.ClassOrderCancel}
	for name, tc := range map[string]struct {
		push   map[string]any
		cooled []rails.OpClass
	}{
		"create": {push: refused("createOrder"), cooled: []rails.OpClass{options.ClassOrderCreate}},
		"modify": {push: refused("modifyOrder"), cooled: []rails.OpClass{options.ClassOrderModify}},
		"cancel": {push: refused("cancelOrder"), cooled: []rails.OpClass{options.ClassOrderCancel}},
		"bare 429": {
			push: map[string]any{
				"statusCode": 429, "body": "Too many messages",
				"error": map[string]string{"slug": "TOO_MANY_REQUESTS", "code": "SYS-0003"}, "retryAfterSec": 1,
			},
			cooled: classes,
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := options.NewPacer(options.WithFailFast())
			srv := fakeExchange(t)
			conn := tradingConn(t, srv, options.ScopeAll(), rails.WithPacer(p))
			srv.Push(tc.push)
			if tc.push["resultType"] == nil {
				waitEvent[options.ServerError](t, conn.Events())
			} else {
				waitEvent[options.OrderRejected](t, conn.Events())
			}

			for _, class := range classes {
				if slices.Contains(tc.cooled, class) {
					assert.InDelta(t, time.Second+cooldownMargin, refusedFor(t, p, class), float64(100*time.Millisecond), class)
				} else {
					assert.Zero(t, refusedFor(t, p, class), class)
				}
			}
		})
	}
}
