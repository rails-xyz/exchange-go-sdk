package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/transport"
	"github.com/rails-xyz/exchange-go-sdk/internal/wire/auth"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// refreshAt is the fraction of a token's life after which the SDK mints a new one, leaving
// several minutes of headroom for a failed mint to be retried before anything expires.
const refreshAt = 0.8

const (
	// mintTimeout bounds a refresh, so a hung auth service cannot pin the shared attempt.
	mintTimeout = 30 * time.Second
	// refreshBackoff is how long a failed refresh is left alone while the token in hand is
	// still valid, so a busy client does not mint again on every call past the refresh point.
	refreshBackoff = 30 * time.Second
)

// flight is one shared refresh. Its result is written before done is closed, so every waiter
// reads the same outcome rather than starting a mint of its own.
type flight struct {
	done  chan struct{}
	token string
	err   error
}

// Source is the default [rails.TokenSource]: one cached token, refreshed ahead of expiry, with
// concurrent refreshes collapsed into one request. It is a leaf with no goroutine of its own,
// so a mutex guards the cache.
type Source struct {
	creds rails.Credentials
	url   string
	http  *http.Client
	agent string
	now   func() time.Time // injectable so tests can move time without sleeping

	mu       sync.Mutex
	token    string
	expires  time.Time
	refresh  time.Time
	retryAt  time.Time // a failed mint is not repeated before this
	failure  error     // what the last mint failed with, while there is no token to fall back on
	inflight *flight
}

// New builds the default token source. The returned Source is safe for concurrent use.
func New(creds rails.Credentials, cfg *rails.Config) *Source {
	return &Source{
		creds: creds,
		url:   cfg.Environment.AuthURL + "/api/v1/token",
		http:  cfg.HTTPClient,
		agent: rails.UserAgent(),
		now:   time.Now,
	}
}

// Token returns a cached token, refreshing it once it is past its refresh point. A token that
// is merely due for refresh is still served if the mint fails, so a rate-limited or briefly
// unreachable auth service cannot stop trading that a valid token would allow.
//
// Giving up on ctx abandons the wait, not the refresh: the shared attempt runs to completion
// for whoever is still waiting on it.
func (s *Source) Token(ctx context.Context) (string, error) {
	token, wait := s.claim(ctx)
	if wait == nil {
		return token, nil
	}
	select {
	case <-wait.done:
		return wait.token, wait.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// claim decides this caller's next move under the lock: use the cached token, join the refresh
// already running, or start one.
func (s *Source) claim(ctx context.Context) (string, *flight) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	usable := s.token != "" && now.Before(s.expires)
	if s.token != "" && now.Before(s.refresh) {
		return s.token, nil
	}
	// A mint that just failed is not worth repeating yet: serve what we hold, or fail the way
	// it failed, rather than spend another mint against an endpoint that is refusing them.
	if now.Before(s.retryAt) {
		if usable {
			return s.token, nil
		}
		if s.failure != nil {
			f := &flight{done: make(chan struct{}), err: s.failure}
			close(f.done)
			return "", f
		}
	}
	if s.inflight != nil {
		// Serve a usable token rather than blocking on someone else's refresh.
		if usable {
			return s.token, nil
		}
		return "", s.inflight
	}

	f := &flight{done: make(chan struct{})}
	s.inflight = f
	go s.refreshOnce(ctx, f)
	return "", f
}

// refreshOnce mints a token and publishes the outcome to everyone waiting on f.
//
// The request keeps ctx's values but not its cancellation: the caller who happened to start
// the refresh must not be able to fail it for everyone else by giving up.
func (s *Source) refreshOnce(ctx context.Context, f *flight) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mintTimeout)
	defer cancel()

	token, expires, err := s.fetch(ctx)

	s.mu.Lock()
	now := s.now()
	switch {
	case err == nil:
		s.token, s.expires = token, expires
		s.refresh = now.Add(time.Duration(float64(expires.Sub(now)) * refreshAt))
		s.retryAt, s.failure = time.Time{}, nil
		f.token = token
	case s.token != "" && now.Before(s.expires):
		// The refresh failed, but what we hold is still good. Sit on it for a while rather
		// than minting again on the very next call.
		s.retryAt = now.Add(retryDelay(err))
		f.token = s.token
	default:
		s.retryAt, s.failure = now.Add(retryDelay(err)), err
		f.err = err
	}
	s.inflight = nil
	s.mu.Unlock()

	close(f.done)
}

// retryDelay is how long to leave a failed refresh alone: whatever the server asked for, or a
// fixed pause when it did not say.
func retryDelay(err error) time.Duration {
	var apiErr *rails.Error
	if errors.As(err, &apiErr) && apiErr.RetryAfter > 0 {
		return apiErr.RetryAfter
	}
	return refreshBackoff
}

// fetch calls the token endpoint. It sends no body: the credentials are headers.
//
// It returns the absolute moment the token expires rather than a lifetime, so no elapsed time
// between reading the clock here and storing the result can stretch the deadline.
func (s *Source) fetch(ctx context.Context) (string, time.Time, error) {
	const op = "POST /api/v1/token"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, nil)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("%s: %w", op, err)
	}
	req.Header.Set("X-Api-Key", s.creds.APIKey)
	req.Header.Set("X-Api-Secret", s.creds.APISecret)
	req.Header.Set("User-Agent", s.agent)
	req.Header.Set("Accept", "application/json")

	resp, err := s.http.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("%s: %w", op, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", time.Time{}, transport.ParseError(op, resp, body)
	}
	if err != nil {
		return "", time.Time{}, fmt.Errorf("%s: reading response: %w", op, err)
	}

	var minted auth.TokenResponse
	if err := json.Unmarshal(body, &minted); err != nil {
		return "", time.Time{}, fmt.Errorf("%s: decoding response: %w", op, err)
	}
	if minted.AccessToken == "" {
		return "", time.Time{}, fmt.Errorf("%s: response carried no access_token", op)
	}

	// exp describes the token; expires_in describes the mint. Prefer the token's own claim.
	expires, ok := expiry(minted.AccessToken)
	if !ok {
		expires = s.now().Add(time.Duration(minted.ExpiresIn) * time.Second)
	}
	if !expires.After(s.now()) {
		return "", time.Time{}, fmt.Errorf("%s: token is already expired", op)
	}
	return minted.AccessToken, expires, nil
}

// expiry reads exp out of the JWT payload.
func expiry(token string) (time.Time, bool) {
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if !decodeClaims(token, &claims) || claims.Exp <= 0 {
		return time.Time{}, false
	}
	return time.Unix(claims.Exp, 0), true
}

// AccountType reads the accountType claim out of a token, or "" when it has none.
func AccountType(token string) string {
	var claims struct {
		AccountType string `json:"accountType"`
	}
	decodeClaims(token, &claims)
	return claims.AccountType
}

// decodeClaims unmarshals the JWT payload. The signature is never checked: there is no JWKS
// and no published key, so verification is the server's job.
func decodeClaims(token string, claims any) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return false
	}
	return json.Unmarshal(payload, claims) == nil
}
