// Package wsevent decodes the frames every product receives alike: shutdown warnings, session
// backfills and the error pair in a frame's data.
package wsevent

import (
	"encoding/json"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/telemetry"
	"github.com/rails-xyz/exchange-go-sdk/internal/ws"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

const (
	resultConnectionShutdown = "connectionShutdown"
	resultSessionBackfill    = "sessionBackfill"
)

// Error is the {slug, code} pair a frame's data carries.
type Error struct {
	Slug string     `json:"slug"`
	Code rails.Code `json:"code"`
}

// Shutdown is a connectionShutdown frame's data.
type Shutdown struct {
	StatusCode     int   `json:"statusCode"`
	ForceCloseInMs int64 `json:"forceCloseInMs"`
	Ts             int64 `json:"ts"`
}

// ForceCloseIn is how long until the server closes the socket.
func (s Shutdown) ForceCloseIn() time.Duration {
	return time.Duration(s.ForceCloseInMs) * time.Millisecond
}

// At is when the server sent the warning.
func (s Shutdown) At() time.Time { return time.UnixMilli(s.Ts) }

// Backfill is a sessionBackfill frame's data. A product's public event converts from it
// directly, so the two keep the same fields in the same order.
type Backfill struct {
	SessionID         string   `json:"sessionId"`
	Resumed           bool     `json:"resumed"`
	Events            int      `json:"events"`
	BackfilledMarkets []string `json:"backfilledMarkets"`
	ExpiredMarkets    []string `json:"expiredMarkets"`
	From              int64    `json:"from"`
	To                int64    `json:"to"`
}

// Telemetry is what the collector keeps of a backfill.
func (b Backfill) Telemetry() telemetry.Backfill {
	return telemetry.Backfill{Resumed: b.Resumed, Events: b.Events, ExpiredMarkets: b.ExpiredMarkets}
}

// DecodeShutdown reads a connectionShutdown frame; false for any other or an undecodable one.
func DecodeShutdown(frame ws.Frame) (Shutdown, bool) {
	var data Shutdown
	ok := frame.ResultType == resultConnectionShutdown && json.Unmarshal(frame.Data, &data) == nil
	return data, ok
}

// DecodeBackfill reads a sessionBackfill frame; false for any other or an undecodable one.
func DecodeBackfill(frame ws.Frame) (Backfill, bool) {
	var data Backfill
	ok := frame.ResultType == resultSessionBackfill && json.Unmarshal(frame.Data, &data) == nil
	return data, ok
}
