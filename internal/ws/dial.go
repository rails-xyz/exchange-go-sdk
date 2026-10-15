package ws

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/rails-xyz/exchange-go-sdk/internal/transport"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// readLimit matches the server's maximum frame size.
const readLimit = 256_000

// Endpoint resolves what a dial needs, fresh each attempt: the full URL including its query,
// and the single subprotocol value carrying the token. Called before every dial so a reconnect
// never reuses a token that has since expired.
type Endpoint func(ctx context.Context) (url, subprotocol string, err error)

// socket is the part of a WebSocket the connection uses, kept narrow so the library behind it
// stays swappable.
type socket interface {
	Read(ctx context.Context) (websocket.MessageType, []byte, error)
	Write(ctx context.Context, typ websocket.MessageType, p []byte) error
	Ping(ctx context.Context) error
	Close(code websocket.StatusCode, reason string) error
	CloseNow() error
}

// softBanned matches the plain-text 403 the exchange serves to an account that tripped the
// rate limiter, whose deadline is the only thing that lifts it.
var softBanned = regexp.MustCompile(`soft banned till (\d+)`)

// dial opens one connection. Failures come back as a *rails.Error carrying whatever the server
// said about when to try again.
func dial(ctx context.Context, httpClient *http.Client, url, subprotocol, userAgent string) (socket, rails.ConnectStats, error) {
	const op = "connect"

	var trace dialTrace
	conn, resp, err := websocket.Dial(trace.trace(ctx), url, &websocket.DialOptions{
		HTTPClient:   httpClient,
		HTTPHeader:   http.Header{"User-Agent": []string{userAgent}},
		Subprotocols: []string{subprotocol},
	})
	stats := trace.result()
	if err == nil {
		conn.SetReadLimit(readLimit)
		return conn, stats, nil
	}
	if resp == nil {
		return nil, stats, fmt.Errorf("%s: %w", op, err)
	}
	return nil, stats, upgradeError(op, resp)
}

// upgradeError turns a failed upgrade into a *rails.Error. The body is the HTTP error envelope,
// or plain text from an edge answering for the exchange.
func upgradeError(op string, resp *http.Response) error {
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	apiErr := transport.ParseError(op, resp, body)

	switch {
	case resp.StatusCode == http.StatusForbidden && softBanned.MatchString(apiErr.Message):
		// The body names a Unix deadline; nothing before it will be allowed to connect.
		lifts, _ := strconv.ParseInt(softBanned.FindStringSubmatch(apiErr.Message)[1], 10, 64)
		apiErr.RetryAfter = max(0, time.Until(time.Unix(lifts, 0)))
		return fmt.Errorf("%w: %w", rails.ErrSoftBanned, apiErr)
	case resp.StatusCode == http.StatusTooManyRequests:
		if wait := retryAfterMillis(resp.Header); wait > 0 {
			apiErr.RetryAfter = wait
		}
	}
	return apiErr
}

// retryAfterMillis reads the retry-after-ms header a 429 on the upgrade carries.
func retryAfterMillis(h http.Header) time.Duration {
	ms, err := strconv.ParseInt(h.Get("retry-after-ms"), 10, 64)
	if err != nil || ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// statusServiceRestart is WebSocket close code 1012.
const statusServiceRestart = 1012

// closeReason describes why a socket ended, for the disconnect hook and for telemetry.
func closeReason(err error) rails.DisconnectReason {
	switch code := websocket.CloseStatus(err); {
	case code == statusServiceRestart:
		// The only code the exchange emits, and it means it is going away.
		return rails.ReasonServerShutdown
	case code != -1:
		return rails.ReasonCloseFrame
	default:
		return rails.ReasonReadError
	}
}
