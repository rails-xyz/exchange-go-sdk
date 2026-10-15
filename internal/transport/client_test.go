package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticToken string

func (s staticToken) Token(context.Context) (string, error) { return string(s), nil }

// countingToken fails every time and counts how often it was asked.
type countingToken struct{ calls atomic.Int64 }

func (c *countingToken) Token(context.Context) (string, error) {
	c.calls.Add(1)
	return "", errors.New("credentials rejected")
}

// recordingHooks captures what the transport reports, so the tests can assert on retries and
// statuses without reaching into the client.
type recordingHooks struct {
	calls atomic.Int64
	last  atomic.Value // rails.HTTPInfo
}

func (h *recordingHooks) hooks() rails.Hooks {
	return rails.Hooks{HTTPDone: func(info rails.HTTPInfo, _ time.Duration, _ error) {
		h.calls.Add(1)
		h.last.Store(info)
	}}
}

func newClient(t *testing.T, hooks rails.Hooks) *Client {
	t.Helper()
	cfg := rails.NewConfig(rails.WithHooks(hooks), rails.WithHTTPClient(&http.Client{}))
	return New(cfg, staticToken("jwt-token"))
}

func TestRequestCarriesAuthAndUserAgent(t *testing.T) {
	var gotAuth, gotAgent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotAgent = r.Header.Get("Authorization"), r.Header.Get("User-Agent")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	var out struct{}
	require.NoError(t, newClient(t, rails.Hooks{}).Get(t.Context(), "op", "", srv.URL, "/x", nil, &out))

	assert.Equal(t, "Bearer jwt-token", gotAuth)
	assert.Equal(t, rails.UserAgent(), gotAgent)
}

func TestGetRetriesARateLimit(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "0") // the wait itself is pinned by TestRetryAfterIsWaitedOut
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"body":"api call rate limit exceeded","statusCode":429}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	hooks := &recordingHooks{}
	var out struct{ OK bool }
	require.NoError(t, newClient(t, hooks.hooks()).Get(t.Context(), "op", "", srv.URL, "/x", nil, &out))

	assert.True(t, out.OK, "the second attempt should have been decoded")
	assert.EqualValues(t, 2, attempts.Load(), "the server should have seen two attempts")
	assert.EqualValues(t, 2, hooks.calls.Load(), "HTTPDone should fire once per attempt")

	info, ok := hooks.last.Load().(rails.HTTPInfo)
	require.True(t, ok)
	assert.Equal(t, 1, info.Attempt)
	assert.Equal(t, http.StatusOK, info.StatusCode)
}

// Retry-After is the server telling us how long to stay away, and it has to beat the jittered
// backoff, which for the first retry can only be under 250ms.
func TestRetryAfterIsWaitedOut(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	start := time.Now()
	require.NoError(t, newClient(t, rails.Hooks{}).Get(t.Context(), "op", "", srv.URL, "/x", nil, &struct{}{}))

	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, time.Second, "the backoff was used instead of Retry-After")
	assert.LessOrEqual(t, elapsed, 4*time.Second, "waited far longer than asked")
}

func TestGetGivesUpAfterThreeAttempts(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	err := newClient(t, rails.Hooks{}).Get(t.Context(), "op", "", srv.URL, "/x", nil, &struct{}{})

	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusBadGateway, apiErr.StatusCode)
	assert.EqualValues(t, maxAttempts, attempts.Load())
}

func TestNotRetriedStatuses(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusNotFound, http.StatusConflict} {
		var attempts atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts.Add(1)
			w.WriteHeader(status)
		}))

		err := newClient(t, rails.Hooks{}).Get(t.Context(), "op", "", srv.URL, "/x", nil, &struct{}{})
		assert.Error(t, err, "status %d", status)
		assert.EqualValues(t, 1, attempts.Load(), "status %d should not be retried", status)
		srv.Close()
	}
}

// A mutating call must go out exactly once whatever the server says: a repeated
// clientRequestId is either rejected as a duplicate or silently dropped.
func TestDeleteIsNeverRetried(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	require.Error(t, newClient(t, rails.Hooks{}).Delete(t.Context(), "op", srv.URL, "/x", nil, nil))
	assert.EqualValues(t, 1, attempts.Load())
}

func TestContextBoundsTheWholeCallIncludingRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := newClient(t, rails.Hooks{}).Get(ctx, "op", "", srv.URL, "/x", nil, &struct{}{})

	elapsed := time.Since(start)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.LessOrEqual(t, elapsed, 5*time.Second, "the 30s Retry-After outlived the context")
}

// A token failure is terminal: the token source has already applied its own cache, fallback and
// single-flight, so repeating the request would only mint again.
func TestTokenFailureIsNotRetried(t *testing.T) {
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	tokens := &countingToken{}
	pacer := &recordingPacer{}
	cfg := rails.NewConfig(rails.WithHTTPClient(&http.Client{}), rails.WithPacer(pacer))

	require.Error(t, New(cfg, tokens).Get(t.Context(), "op", "read", srv.URL, "/x", nil, &struct{}{}))
	assert.Zero(t, requests.Load(), "the request should never have gone out")
	assert.EqualValues(t, 1, tokens.calls.Load(), "the token source should be asked once")
	assert.Zero(t, pacer.waits.Load(), "nothing was sent, so nothing was booked")
}

// recordingPacer counts what the transport asks of it; refuseFrom > 0 refuses that wait onwards.
type recordingPacer struct {
	waits      atomic.Int64
	observed   atomic.Int64
	refuseFrom int64
}

func (p *recordingPacer) Wait(context.Context, rails.OpClass, string) error {
	if n := p.waits.Add(1); p.refuseFrom > 0 && n >= p.refuseFrom {
		return fmt.Errorf("%w: %w", rails.ErrNotSent, rails.ErrPaced)
	}
	return nil
}

func (p *recordingPacer) Observe(_ rails.OpClass, err error) {
	if err != nil {
		p.observed.Add(1)
	}
}

// A pacer that refuses the retry is heard before the retry delay is slept.
func TestPacerRefusalSkipsTheRetryDelay(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	pacer := &recordingPacer{refuseFrom: 2}
	cfg := rails.NewConfig(rails.WithHTTPClient(&http.Client{}), rails.WithPacer(pacer))

	start := time.Now()
	err := New(cfg, staticToken("jwt-token")).Get(t.Context(), "op", "read", srv.URL, "/x", nil, &struct{}{})

	assert.ErrorIs(t, err, rails.ErrPaced)
	assert.Less(t, time.Since(start), 5*time.Second, "the 30s Retry-After was slept before the pacer spoke")
}

// Every attempt is a request to the exchange, so the pacer runs before each one and hears
// about each failure.
func TestPacerRunsBeforeEveryAttempt(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	pacer := &recordingPacer{}
	cfg := rails.NewConfig(rails.WithHTTPClient(&http.Client{}), rails.WithPacer(pacer))
	client := New(cfg, staticToken("jwt-token"))

	require.NoError(t, client.Get(t.Context(), "op", "read", srv.URL, "/x", nil, &struct{}{}))
	assert.EqualValues(t, 2, pacer.waits.Load())
	assert.EqualValues(t, 1, pacer.observed.Load())
}

// A mutating call must go out exactly once whatever the server says; Post carries its body as
// JSON.
func TestPostSendsJSONAndIsNeverRetried(t *testing.T) {
	var attempts atomic.Int64
	var gotType, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		body, _ := io.ReadAll(r.Body)
		gotType, gotBody = r.Header.Get("Content-Type"), string(body)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	err := newClient(t, rails.Hooks{}).Post(t.Context(), "op", "", srv.URL, "/x", map[string]string{"a": "b"}, nil)
	require.Error(t, err)
	assert.EqualValues(t, 1, attempts.Load())
	assert.Equal(t, "application/json", gotType)
	assert.JSONEq(t, `{"a":"b"}`, gotBody)
}
