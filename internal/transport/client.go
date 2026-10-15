package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// maxBody bounds what is read from a response, well above the largest documented page.
const maxBody = 8 << 20

// Client issues authenticated HTTP calls. One is shared by every method on a product client.
type Client struct {
	http      *http.Client
	tokens    rails.TokenSource
	hooks     rails.Hooks
	pacer     rails.Pacer // nil paces nothing
	userAgent string
}

// New builds a Client from an assembled config. tokens is taken separately because a product
// client installs its own default when the caller supplied none.
func New(cfg *rails.Config, tokens rails.TokenSource) *Client {
	hooks := cfg.Hooks
	if hooks.HTTPDone == nil {
		hooks.HTTPDone = func(rails.HTTPInfo, time.Duration, error) {}
	}
	return &Client{
		http:      cfg.HTTPClient,
		tokens:    tokens,
		hooks:     hooks,
		pacer:     cfg.Pacer,
		userAgent: rails.UserAgent(),
	}
}

// Get performs an idempotent read and decodes the body into out. It retries connection
// failures, 408, 429 and 5xx up to two more times, honouring Retry-After when the server sent
// one. ctx bounds every attempt together. class paces every attempt; empty is unpaced.
func (c *Client) Get(ctx context.Context, op string, class rails.OpClass, base, path string, query url.Values, out any) error {
	return c.do(ctx, call{method: http.MethodGet, op: op, class: class, base: base, path: path, query: query, retry: true}, out)
}

// Post sends body as JSON. It is never retried.
func (c *Client) Post(ctx context.Context, op string, class rails.OpClass, base, path string, body any, out any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("%s: encoding request: %w", op, err)
	}
	return c.do(ctx, call{method: http.MethodPost, op: op, class: class, base: base, path: path, body: encoded}, out)
}

// Delete performs a mutating call. It is never retried, and never paced.
func (c *Client) Delete(ctx context.Context, op, base, path string, query url.Values, out any) error {
	return c.do(ctx, call{method: http.MethodDelete, op: op, base: base, path: path, query: query}, out)
}

type call struct {
	method     string
	op         string
	class      rails.OpClass // empty: unpaced
	base, path string
	query      url.Values
	body       []byte
	retry      bool // idempotent reads only
}

func (c *Client) do(ctx context.Context, req call, out any) error {
	endpoint := req.base + req.path
	if encoded := req.query.Encode(); encoded != "" {
		endpoint += "?" + encoded
	}

	attempts := 1
	if req.retry {
		attempts = maxAttempts
	}

	paced := c.pacer != nil && req.class != ""
	var lastErr error
	for attempt := range attempts {
		// A token failure has already been through the source's own cache and fallback, so
		// retrying here would only mint again.
		token, err := c.tokens.Token(ctx)
		if err != nil {
			return fmt.Errorf("%s: %w", req.op, err)
		}

		// Pacer first, so fail-fast refuses before the retry delay, which then counts the wait.
		start := time.Now()
		if paced {
			if err := c.pacer.Wait(ctx, req.class, req.op); err != nil {
				return err
			}
		}
		if attempt > 0 {
			delay := backoff(attempt - 1)
			var apiErr *rails.Error
			if errors.As(lastErr, &apiErr) && apiErr.RetryAfter > 0 {
				delay = apiErr.RetryAfter
			}
			if err := wait(ctx, delay-time.Since(start)); err != nil {
				return fmt.Errorf("%s: %w", req.op, err)
			}
		}

		body, resp, err := c.attempt(ctx, req, endpoint, token, attempt)
		switch {
		case err != nil:
			if paced {
				c.pacer.Observe(req.class, err)
			}
			lastErr = err
			if !req.retry || !retryable(resp) || ctx.Err() != nil {
				return err
			}
		case out == nil:
			return nil
		default:
			if err := json.Unmarshal(body, out); err != nil {
				return fmt.Errorf("%s: decoding response: %w", req.op, err)
			}
			return nil
		}
	}
	return lastErr
}

// attempt performs one request. It returns the response alongside the error so the caller can
// decide whether the failure is worth repeating.
func (c *Client) attempt(ctx context.Context, req call, endpoint, token string, attempt int) ([]byte, *http.Response, error) {
	op := req.op
	var body io.Reader
	if req.body != nil {
		body = bytes.NewReader(req.body)
	}
	httpReq, err := http.NewRequestWithContext(ctx, req.method, endpoint, body)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", op, err)
	}
	httpReq.Header.Set("User-Agent", c.userAgent)
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+token)
	if req.body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	info := rails.HTTPInfo{Op: op, Method: req.method, URL: endpoint, Attempt: attempt}
	start := time.Now()
	resp, err := c.http.Do(httpReq)
	if err != nil {
		c.hooks.HTTPDone(info, time.Since(start), err)
		return nil, nil, fmt.Errorf("%s: %w", op, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	info.StatusCode = resp.StatusCode
	elapsed := time.Since(start)

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		apiErr := ParseError(op, resp, respBody)
		c.hooks.HTTPDone(info, elapsed, apiErr)
		return nil, resp, apiErr
	}
	if readErr != nil {
		c.hooks.HTTPDone(info, elapsed, readErr)
		return nil, resp, fmt.Errorf("%s: reading response: %w", op, readErr)
	}
	c.hooks.HTTPDone(info, elapsed, nil)
	return respBody, resp, nil
}
