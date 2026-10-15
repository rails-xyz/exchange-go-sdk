package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clock is a hand-wound time source, so refresh timing is asserted rather than waited out.
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

// jwt builds a token whose payload carries exp and nothing the SDK reads.
func jwt(exp time.Time) string {
	payload, _ := json.Marshal(map[string]int64{"exp": exp.Unix()})
	return "header." + base64.RawURLEncoding.EncodeToString(payload) + ".signature"
}

// tokenServer answers the token endpoint with a fresh 1800-second token each time, counting
// mints, and lets a test take over the response.
type tokenServer struct {
	mints   atomic.Int64
	clock   *clock
	respond func(w http.ResponseWriter, mint int64) bool // returns true when it handled it
}

func (s *tokenServer) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mint := s.mints.Add(1)
		if r.Header.Get("X-Api-Key") != "key" || r.Header.Get("X-Api-Secret") != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		// -1 would mean a body of unknown length, which is still a body.
		assert.Zero(t, r.ContentLength, "the token request takes headers only")
		if s.respond != nil && s.respond(w, mint) {
			return
		}
		body, _ := json.Marshal(map[string]any{
			"access_token": jwt(s.clock.Now().Add(1800 * time.Second)),
			"token_type":   "Bearer",
			"expires_in":   1800,
		})
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newSource(t *testing.T, srv *httptest.Server, c *clock) *Source {
	t.Helper()
	cfg := rails.NewConfig(rails.WithEnvironment(rails.Environment{AuthURL: srv.URL}))
	s := New(rails.Credentials{APIKey: "key", APISecret: "secret"}, cfg)
	s.now = c.Now
	return s
}

// One mint should cover the whole life of a token up to its refresh point, and exactly one
// more should happen after it.
func TestTokenRefreshesAtEightyPercentOfLife(t *testing.T) {
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	server := &tokenServer{clock: c}
	src := newSource(t, server.start(t), c)

	first, err := src.Token(t.Context())
	require.NoError(t, err)

	// 1439s in: still short of 80% of 1800s, so nothing should be minted.
	c.Advance(1439 * time.Second)
	again, err := src.Token(t.Context())
	require.NoError(t, err)
	assert.Equal(t, first, again, "token was refreshed before its refresh point")
	assert.EqualValues(t, 1, server.mints.Load(), "mints before the refresh point")

	// 1441s in: past 80%, so exactly one refresh.
	c.Advance(2 * time.Second)
	refreshed, err := src.Token(t.Context())
	require.NoError(t, err)
	assert.NotEqual(t, first, refreshed, "token was not refreshed after its refresh point")
	assert.EqualValues(t, 2, server.mints.Load(), "mints after the refresh point")
}

// The token endpoint allows 20 mints a minute, so a burst of callers arriving on a cold cache
// must produce one request, not one each.
func TestConcurrentCallersShareOneMint(t *testing.T) {
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	release := make(chan struct{})
	server := &tokenServer{clock: c, respond: func(http.ResponseWriter, int64) bool {
		<-release // hold the first mint open so every caller piles up behind it
		return false
	}}
	src := newSource(t, server.start(t), c)

	const callers = 50
	var wg sync.WaitGroup
	tokens := make([]string, callers)
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tokens[i], errs[i] = src.Token(t.Context())
		}()
	}
	time.Sleep(50 * time.Millisecond) // let them all reach the source
	close(release)
	wg.Wait()

	for i := range callers {
		require.NoError(t, errs[i], "caller %d", i)
		assert.Equal(t, tokens[0], tokens[i], "caller %d got a different token", i)
	}
	assert.EqualValues(t, 1, server.mints.Load(), "%d concurrent callers should share one mint", callers)
}

// A refresh that is rate-limited must not take down a caller who still holds a valid token.
func TestRateLimitedRefreshKeepsServingTheOldToken(t *testing.T) {
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	server := &tokenServer{clock: c, respond: func(w http.ResponseWriter, mint int64) bool {
		if mint == 1 {
			return false
		}
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"slug":"TOO_MANY_REQUESTS","code":"SYS-0003"},"message":"Too Many Requests"}`))
		return true
	}}
	src := newSource(t, server.start(t), c)

	first, err := src.Token(t.Context())
	require.NoError(t, err)

	c.Advance(1500 * time.Second) // past refresh, well short of expiry
	served, err := src.Token(t.Context())
	require.NoError(t, err, "a rate-limited refresh must not fail a caller holding a valid token")
	assert.Equal(t, first, served, "did not fall back to the token still in hand")

	// The failure must be left alone for a while: every later call minting again is how a
	// busy client spends a 20-per-minute endpoint.
	for range 20 {
		_, err := src.Token(t.Context())
		require.NoError(t, err)
	}
	assert.EqualValues(t, 2, server.mints.Load(), "mints while backing off")

	// Once the backoff lapses, one more attempt is made.
	c.Advance(refreshBackoff + time.Second)
	_, err = src.Token(t.Context())
	require.NoError(t, err)
	assert.EqualValues(t, 3, server.mints.Load(), "mints after the backoff lapsed")

	// Once it has actually expired there is nothing to fall back to, so the error surfaces.
	c.Advance(400 * time.Second)
	_, err = src.Token(t.Context())
	assert.ErrorIs(t, err, rails.ErrRateLimited)
}

// exp is authoritative: expires_in describes the mint, exp describes the token.
func TestExpiryPrefersTheJWTClaim(t *testing.T) {
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	server := &tokenServer{clock: c, respond: func(w http.ResponseWriter, _ int64) bool {
		body, _ := json.Marshal(map[string]any{
			"access_token": jwt(c.Now().Add(600 * time.Second)), // the truth
			"token_type":   "Bearer",
			"expires_in":   1800, // a lie
		})
		_, _ = w.Write(body)
		return true
	}}
	src := newSource(t, server.start(t), c)

	_, err := src.Token(t.Context())
	require.NoError(t, err)

	c.Advance(500 * time.Second) // past 80% of 600s, short of 80% of 1800s
	_, err = src.Token(t.Context())
	require.NoError(t, err)
	assert.EqualValues(t, 2, server.mints.Load(), "expires_in was trusted over exp")
}

func TestExpiryIgnoresAnUnreadableToken(t *testing.T) {
	for _, token := range []string{"", "not-a-jwt", "a.b", "a.!!!.c", "a." + base64.RawURLEncoding.EncodeToString([]byte(`{"exp":0}`)) + ".c"} {
		_, ok := expiry(token)
		assert.False(t, ok, "%q was read as carrying an expiry", token)
	}
}

// A herd arriving on a cold cache against a failing endpoint must spend one mint between them,
// not one each: the endpoint allows 20 per minute.
func TestFailedRefreshIsNotRetriedByEveryWaiter(t *testing.T) {
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	release := make(chan struct{})
	server := &tokenServer{clock: c, respond: func(w http.ResponseWriter, _ int64) bool {
		<-release
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	}}
	src := newSource(t, server.start(t), c)

	const callers = 30
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = src.Token(t.Context())
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	for i, err := range errs {
		require.Error(t, err, "caller %d succeeded against a failing endpoint", i)
	}
	assert.EqualValues(t, 1, server.mints.Load(), "%d callers should share one failed mint", callers)
}

// The caller who happens to start a refresh must not be able to fail it for everyone else by
// cancelling, and must not leave the source wedged.
func TestGivingUpDoesNotAbandonTheRefresh(t *testing.T) {
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	release := make(chan struct{})
	server := &tokenServer{clock: c, respond: func(http.ResponseWriter, int64) bool {
		<-release
		return false
	}}
	src := newSource(t, server.start(t), c)

	ctx, cancel := context.WithCancel(t.Context())
	started, abandoned := make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		_, err := src.Token(ctx)
		abandoned <- err
	}()
	<-started
	time.Sleep(50 * time.Millisecond)
	cancel()
	close(release)

	// A later caller gets a token from the refresh the first one walked away from.
	token, err := src.Token(t.Context())
	require.NoError(t, err, "the abandoned refresh took the source down with it")
	assert.NotEmpty(t, token)
	assert.EqualValues(t, 1, server.mints.Load())
	assert.ErrorIs(t, <-abandoned, context.Canceled, "the caller that cancelled should see its own error")
}

// With nothing to fall back on, a failed mint is answered from memory until its retry point:
// sequential callers during an outage must not each spend a mint.
func TestFailedMintIsNotRepeatedUntilItsRetryPoint(t *testing.T) {
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	server := &tokenServer{clock: c, respond: func(w http.ResponseWriter, _ int64) bool {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
		return true
	}}
	src := newSource(t, server.start(t), c)

	for range 20 {
		_, err := src.Token(t.Context())
		assert.ErrorIs(t, err, rails.ErrRateLimited)
	}
	assert.EqualValues(t, 1, server.mints.Load(), "every call minted again")

	c.Advance(119 * time.Second)
	_, _ = src.Token(t.Context())
	assert.EqualValues(t, 1, server.mints.Load(), "minted before the Retry-After the endpoint asked for")

	c.Advance(2 * time.Second)
	_, _ = src.Token(t.Context())
	assert.EqualValues(t, 2, server.mints.Load(), "the retry point passed and nothing was minted")
}
