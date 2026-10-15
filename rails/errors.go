package rails

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Sentinels for the conditions callers branch on. Match them with [errors.Is]; reach for the
// detail behind them with [errors.As] and [*Error].
//
// ErrTimeout and ErrConnClosed both mean the command may still execute: reconcile on
// ClientRequestID, never resend, because a reused one is either rejected as a duplicate or
// silently swallowed. ErrNotSent means it never left, and may be sent again.
var (
	ErrUnauthorized = errors.New("unauthorized")
	ErrForbidden    = errors.New("forbidden")
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("conflict")
	ErrRateLimited  = errors.New("rate limited")
	// ErrSoftBanned bars the account from connecting until a deadline the server chose. The
	// SDK stops dialling until then; so should any reconnect logic of your own.
	ErrSoftBanned = errors.New("soft banned")
	ErrTimeout    = errors.New("timed out")
	ErrConnClosed = errors.New("connection closed")
	// ErrNotSent is a command that found no connection to go out on, during a reconnect or
	// after Close, or one a pacer refused.
	ErrNotSent = errors.New("not sent")
	// ErrPaced is a call a fail-fast [Pacer] refused. It always comes wrapped in ErrNotSent,
	// with an [*Error] whose RetryAfter says how long the pacer would have waited.
	ErrPaced = errors.New("paced")
)

// Error is every failure the exchange described, on HTTP or on the WebSocket, or a call a
// fail-fast [Pacer] refused. Fields the server did not send are zero.
type Error struct {
	// Op is the WebSocket command ("createOrder") or HTTP route ("GET /api/v1/order").
	Op string
	// StatusCode is the HTTP status, or the statusCode carried inside a WebSocket frame.
	StatusCode int
	// Slug and Code identify the condition; the perpetuals account reads send neither. Message
	// is for people and may be reworded, so branch on Code.
	Slug    string
	Code    Code
	Message string
	// RetryAfter is how long the server asked us to wait, from any of the four spellings it
	// uses. Zero when it did not say.
	RetryAfter time.Duration
	// APIQuotaUsed and APIQuotaLimit come from the X-Api-Quota-* headers, which only the
	// perpetuals order reads (GET /api/v1/order and its siblings on the order host) send, and
	// not when the account is unlimited; a perpetuals WebSocket refusal carries APIQuotaUsed
	// too. Zero otherwise.
	APIQuotaUsed  int64
	APIQuotaLimit int64
	// ClientRequestID correlates the failure with the exchange's own logs.
	ClientRequestID string
	// OrderID is the order a create named, set when its outcome is unknown so the order can be
	// looked up.
	OrderID string
}

func (e *Error) Error() string {
	var b strings.Builder
	if e.Op != "" {
		b.WriteString(e.Op)
		b.WriteString(": ")
	}
	if e.StatusCode != 0 {
		fmt.Fprintf(&b, "%d ", e.StatusCode)
	}
	if e.Message != "" {
		b.WriteString(e.Message)
	} else {
		b.WriteString("request failed")
	}
	if e.Slug != "" || e.Code != "" {
		fmt.Fprintf(&b, " (%s/%s)", e.Slug, e.Code)
	}
	if e.ClientRequestID != "" {
		fmt.Fprintf(&b, " [clientRequestId=%s]", e.ClientRequestID)
	}
	return b.String()
}

// Is matches the sentinels a status code identifies. The others (a soft ban among ordinary
// 403s, a timeout, a closed connection) are wrapped around the Error instead.
func (e *Error) Is(target error) bool {
	switch target {
	case ErrUnauthorized:
		return e.StatusCode == http.StatusUnauthorized
	case ErrForbidden:
		return e.StatusCode == http.StatusForbidden
	case ErrNotFound:
		return e.StatusCode == http.StatusNotFound
	case ErrConflict:
		return e.StatusCode == http.StatusConflict
	case ErrRateLimited:
		return e.StatusCode == http.StatusTooManyRequests
	}
	return false
}
