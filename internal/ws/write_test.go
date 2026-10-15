package ws

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// halfOpen is a socket whose writes fail while its reads block forever, which is what a
// half-open TCP connection looks like and what no real server can be asked to produce.
type halfOpen struct {
	writeErr error
	closed   atomic.Bool
	release  chan struct{}
}

func (h *halfOpen) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case <-h.release:
		return 0, nil, errors.New("closed")
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (h *halfOpen) Write(context.Context, websocket.MessageType, []byte) error { return h.writeErr }
func (h *halfOpen) Ping(context.Context) error                                 { return nil }
func (h *halfOpen) CloseNow() error                                            { h.stop(); return nil }

func (h *halfOpen) Close(websocket.StatusCode, string) error { h.stop(); return nil }

func (h *halfOpen) stop() {
	if h.closed.CompareAndSwap(false, true) {
		close(h.release)
	}
}

// A write that fails means the socket is gone, whoever noticed first. Waiting for the read side
// to agree wedges a connection whose reads block forever.
func TestFailedWriteTearsTheSocketDown(t *testing.T) {
	var dialled atomic.Int64
	sockets := make(chan *halfOpen, 4)
	lifecycle := make(chan string, 4)

	conn, err := Open(t.Context(), Options{
		Endpoint:       func(context.Context) (string, string, error) { return "wss://fake", "authorization#jwt", nil },
		OnDisconnected: func(r rails.DisconnectReason) { lifecycle <- "disconnected:" + string(r) },
		OnConnected:    func() { lifecycle <- "connected" },
		backoff:        func(int) time.Duration { return 10 * time.Millisecond },
		dialer: func(context.Context, string, string) (socket, error) {
			s := &halfOpen{release: make(chan struct{})}
			if dialled.Add(1) == 1 {
				s.writeErr = errors.New("broken pipe") // only the first socket is half-open
			}
			sockets <- s
			return s, nil
		},
	})
	require.NoError(t, err)
	defer conn.Close()
	first := <-sockets

	_, err = conn.Send(t.Context(), Send{
		Message: "ping", Content: map[string]string{}, ClientRequestID: "a-crid",
	})
	// Bytes may have gone out, so the caller gets what a disconnect gives it: an id to
	// reconcile on, and no suggestion that resending is safe.
	assert.ErrorIs(t, err, rails.ErrConnClosed, "the failed write was reported as success")
	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "a-crid", apiErr.ClientRequestID)

	// One loss, one reconnect: closing the half-open socket wakes its reader, whose error must
	// not be reported as a second disconnect.
	for _, want := range []string{"connected", "disconnected:writeError", "connected"} {
		select {
		case got := <-lifecycle:
			assert.Equal(t, want, got)
		case <-time.After(settle):
			t.Fatalf("no %q", want)
		}
	}
	assert.True(t, waitFor(func() bool { return first.closed.Load() }),
		"the socket that refused the write was left installed")
	assert.True(t, waitFor(func() bool { return dialled.Load() >= 2 }),
		"the connection did not reconnect")
}

func waitFor(cond func() bool) bool {
	deadline := time.Now().Add(settle)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}
