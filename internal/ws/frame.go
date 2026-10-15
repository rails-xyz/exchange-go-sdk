package ws

import (
	"encoding/json"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// Request is the envelope every command goes out in.
type Request struct {
	Message string `json:"message"`
	Content any    `json:"content"`
}

// Frame is one message from the server, parsed only as far as routing needs.
//
// Two shapes arrive on the same socket: the envelope below, and a bare
// `{statusCode, body, error, retryAfterSec?}` for a message refused before it reached a
// command. Only the envelope carries a clientRequestId; a bare frame has ResultType empty.
type Frame struct {
	ResultType string          `json:"resultType"`
	Market     string          `json:"market"`
	Data       json.RawMessage `json:"data"`

	// The bare shape's fields, outside any envelope.
	StatusCode    int        `json:"statusCode"`
	Body          string     `json:"body"`
	Error         *errorPair `json:"error"`
	RetryAfterSec int64      `json:"retryAfterSec"`

	// Raw is the whole message, for a product package to decode into its own types.
	Raw json.RawMessage `json:"-"`
}

type errorPair struct {
	Slug string     `json:"slug"`
	Code rails.Code `json:"code"`
}

// routing is the part of a frame's data the connection itself reads: enough to match a reply
// to its request and to describe a rejection.
type routing struct {
	ClientRequestID string     `json:"clientRequestId"`
	StatusCode      int        `json:"statusCode"`
	Body            string     `json:"body"`
	Error           *errorPair `json:"error"`
	RetryAfterSec   int64      `json:"retryAfterSec"`
	APIQuotaUsed    int64      `json:"apiQuotaUsed"`
}

// parse reads a message into a Frame. A message that is not an envelope still produces a Frame
// with Raw set, because bare error frames have to reach the caller too.
func parse(message []byte) Frame {
	frame := Frame{Raw: json.RawMessage(message)}
	_ = json.Unmarshal(message, &frame)
	return frame
}

// route reads the correlation fields out of a frame's data.
func (f Frame) route() routing {
	var r routing
	if len(f.Data) > 0 {
		_ = json.Unmarshal(f.Data, &r)
	}
	return r
}

// BareError is the error a frame carries outside the envelope. It names no clientRequestId,
// so it cannot be attributed to one request. Nil for an envelope.
func (f Frame) BareError() *rails.Error {
	if f.ResultType != "" || f.Error == nil {
		return nil
	}
	return &rails.Error{
		StatusCode: f.StatusCode,
		Slug:       f.Error.Slug,
		Code:       f.Error.Code,
		Message:    f.Body,
		RetryAfter: time.Duration(f.RetryAfterSec) * time.Second,
	}
}

// toError builds the error a rejection describes.
func (r routing) toError(op string) *rails.Error {
	err := &rails.Error{
		Op:              op,
		StatusCode:      r.StatusCode,
		Message:         r.Body,
		RetryAfter:      time.Duration(r.RetryAfterSec) * time.Second,
		APIQuotaUsed:    r.APIQuotaUsed,
		ClientRequestID: r.ClientRequestID,
	}
	if r.Error != nil {
		err.Slug, err.Code = r.Error.Slug, r.Error.Code
	}
	return err
}

// terminal reports whether a status code ends a request. 202 is the protocol's one non-terminal
// status, accepted but not yet answered; anything else is the answer, success below 400 and an
// error from there, whatever resultType the frame carries.
func terminal(statusCode int) bool {
	return statusCode != 0 && statusCode != statusAccepted
}
