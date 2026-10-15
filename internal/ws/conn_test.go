package ws

import (
	"context"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/perpstest"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const settle = 2 * time.Second

// lifecycle is one OnConnected or OnDisconnected call.
type lifecycle struct {
	connected bool
	reason    rails.DisconnectReason
}

// sink collects what a connection delivers, so a test can wait on it.
type sink struct {
	frames    chan Frame
	lifecycle chan lifecycle
}

func (s *sink) install(o *Options) {
	o.OnFrame = func(f Frame) { s.frames <- f }
	o.OnConnected = func() { s.lifecycle <- lifecycle{connected: true} }
	o.OnDisconnected = func(r rails.DisconnectReason) { s.lifecycle <- lifecycle{reason: r} }
}

// waitLifecycle reads lifecycle notices until one matching connected arrives.
func (s *sink) waitLifecycle(t *testing.T, connected bool) lifecycle {
	t.Helper()
	deadline := time.After(settle)
	for {
		select {
		case l := <-s.lifecycle:
			if l.connected == connected {
				return l
			}
		case <-deadline:
			t.Fatalf("no lifecycle notice (connected=%v) within %v", connected, settle)
		}
	}
}

// open connects to the fake exchange with test-sized timers and an instant reconnect wait.
func open(t *testing.T, srv *perpstest.Server, mutate func(*Options)) (*Conn, *sink) {
	t.Helper()
	s := &sink{frames: make(chan Frame, 256), lifecycle: make(chan lifecycle, 256)}
	opts := Options{
		Endpoint: func(context.Context) (string, string, error) {
			return srv.URL + "/?product=perpetuals&market=BTC-USDT", "authorization#jwt", nil
		},
		ConnID:   "test-conn",
		Market:   "BTC-USDT",
		PongWait: 100 * time.Millisecond,
		backoff:  func(int) time.Duration { return 10 * time.Millisecond },
	}
	s.install(&opts)
	if mutate != nil {
		mutate(&opts)
	}
	conn, err := Open(t.Context(), opts)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return conn, s
}

// A pong that never arrives means the path orders take is broken, even though the socket is
// still open and writable.
func TestMissedPongReconnects(t *testing.T) {
	srv := perpstest.New(t)
	srv.SetAutoPong(false)

	_, s := open(t, srv, func(o *Options) {
		o.PingInterval = 20 * time.Millisecond
		o.PongWait = 50 * time.Millisecond
	})

	assert.Equal(t, rails.ReasonMissedPong, s.waitLifecycle(t, false).reason)
	// Closing the dead socket wakes its reader, whose error must not count as a second loss.
	select {
	case l := <-s.lifecycle:
		assert.True(t, l.connected, "a second disconnect (%q) was reported for one lost socket", l.reason)
	case <-time.After(settle):
		t.Fatal("no reconnect")
	}
	srv.WaitConnections(2, settle)
}

// Every reconnect waits at least the connect budget, so a reconnect loop cannot outrun the 20
// per minute the exchange allows, and never longer than the cap.
func TestBackoffStaysWithinTheBudgetAndTheCap(t *testing.T) {
	for attempt := range 40 {
		for range 20 {
			wait := backoff(attempt)
			assert.GreaterOrEqual(t, wait, connectBudget, "attempt %d", attempt)
			assert.LessOrEqual(t, wait, backoffCap, "attempt %d", attempt)
		}
	}
}

// A soft ban lifts only at its deadline, so dialling before it just spends attempts.
func TestSoftBanStopsDialling(t *testing.T) {
	srv := perpstest.New(t)
	var attempts atomic.Int64
	lifts := time.Now().Add(time.Hour).Unix()
	srv.SetReject(func(w http.ResponseWriter, _ *http.Request) bool {
		attempts.Add(1)
		http.Error(w, "user soft banned till "+strconv.FormatInt(lifts, 10), http.StatusForbidden)
		return true
	})

	_, err := Open(t.Context(), Options{
		Endpoint: func(context.Context) (string, string, error) {
			return srv.URL + "/?product=perpetuals&market=BTC-USDT", "authorization#jwt", nil
		},
	})
	require.ErrorIs(t, err, rails.ErrSoftBanned)

	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	assert.GreaterOrEqual(t, apiErr.RetryAfter, 50*time.Minute, "the deadline should be roughly an hour out")
	assert.EqualValues(t, 1, attempts.Load(), "nothing should be dialled while banned")
}

// Every dial resolves the endpoint again, so a reconnect never reuses a token that expired
// while the old socket was up.
func TestReconnectResolvesTheEndpointAgain(t *testing.T) {
	srv := perpstest.New(t)
	var minted atomic.Int64

	_, s := open(t, srv, func(o *Options) {
		o.Endpoint = func(context.Context) (string, string, error) {
			n := minted.Add(1)
			return srv.URL + "/?product=perpetuals&market=BTC-USDT",
				"authorization#token-" + strconv.FormatInt(n, 10), nil
		}
	})
	s.waitLifecycle(t, true)

	srv.CloseConn(0, 1012, "Service restarting. Please reconnect.")
	srv.WaitConnections(2, settle)

	hs := srv.Handshakes()
	require.GreaterOrEqual(t, len(hs), 2)
	assert.NotEqual(t, hs[0].Token, hs[1].Token, "the reconnect reused a token")
}

// What the endpoint cannot carry is written on the replacement socket before anything a caller
// sends, and never on the first socket, where the endpoint says it all.
func TestResumeGoesFirstOnEveryReplacementSocket(t *testing.T) {
	srv := perpstest.New(t)
	conn, s := open(t, srv, func(o *Options) {
		o.Resume = func() []Send {
			return []Send{{Message: "subscribe", Content: map[string]string{"clientRequestId": "resume"}}}
		}
	})
	s.waitLifecycle(t, true)
	_, _ = conn.Send(t.Context(), Send{Message: "ping", Content: map[string]string{"clientRequestId": "first"}, ClientRequestID: "first"})

	srv.CloseConn(0, 1012, "Service restarting. Please reconnect.")
	s.waitLifecycle(t, true)
	go func() {
		_, _ = conn.Send(t.Context(), Send{Message: "ping", Content: map[string]string{"clientRequestId": "after"}, ClientRequestID: "after"})
	}()

	var sent []string
	for range 3 {
		msg := srv.NextMessage(settle)
		sent = append(sent, msg.Message+":"+msg.ClientRequestID())
	}
	assert.Equal(t, []string{"ping:first", "subscribe:resume", "ping:after"}, sent)
}

// A caller that gives up gets ErrTimeout carrying its clientRequestId, which is the only
// handle it has left to reconcile with.
func TestGivingUpReturnsTimeoutWithTheRequestID(t *testing.T) {
	srv := perpstest.New(t)
	srv.SetAutoPong(false)
	conn, _ := open(t, srv, nil)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	_, err := conn.Send(ctx, Send{
		Message:         "ping",
		Content:         map[string]string{"clientRequestId": "a-crid"},
		ClientRequestID: "a-crid",
	})
	require.ErrorIs(t, err, rails.ErrTimeout)

	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "a-crid", apiErr.ClientRequestID, "the crid is the only handle left to reconcile with")

	// The answer still owed arrives, with nobody left waiting for it.
	srv.Push(map[string]any{
		"resultType": "pong",
		"data":       map[string]any{"statusCode": 200, "clientRequestId": "a-crid"},
	})

	// An owner that blocked handing a reply to a caller who walked away would never answer
	// again.
	srv.SetAutoPong(true)
	_, err = conn.Send(t.Context(), Send{
		Message:         "ping",
		ReplyTypes:      []string{"pong"},
		Content:         map[string]string{"clientRequestId": "another-crid"},
		ClientRequestID: "another-crid",
	})
	require.NoError(t, err, "the connection was wedged by the answer nobody wanted")
}

// A consumer that stops taking frames stalls the owner, and a call must still come back on its
// own deadline.
func TestADeadlineHoldsWhileTheConsumerIsStalled(t *testing.T) {
	srv := perpstest.New(t)
	srv.SetAutoPong(false)
	release := make(chan struct{})
	conn, _ := open(t, srv, func(o *Options) {
		o.OnFrame = func(Frame) { <-release }
	})
	t.Cleanup(func() { close(release) }) // registered after open, so it runs before Close

	sent := make(chan struct{})
	answered := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		close(sent)
		_, err := conn.Send(ctx, Send{
			Message:         "ping",
			Content:         map[string]string{"clientRequestId": "a-crid"},
			ClientRequestID: "a-crid",
		})
		answered <- err
	}()
	<-sent
	srv.NextMessage(settle) // the request is registered; nobody is taking frames

	// A frame nobody asked for wedges the owner in the consumer's callback.
	srv.Push(map[string]any{"resultType": "publicOrderBookDelta", "data": map[string]any{}})

	select {
	case err := <-answered:
		assert.ErrorIs(t, err, rails.ErrTimeout, "handed over before the stall, so it may have executed")
	case <-time.After(settle):
		t.Fatal("the deadline did not hold: a stalled consumer held the caller up")
	}
}

// A context that is already over sends nothing, and says so with a bare context error rather
// than one that tells the caller to reconcile.
func TestACancelledContextSendsNothing(t *testing.T) {
	srv := perpstest.New(t)
	conn, s := open(t, srv, nil)
	s.waitLifecycle(t, true)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := conn.Send(ctx, Send{Message: "ping", Content: map[string]string{}, ClientRequestID: "dead"})
	assert.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, rails.ErrTimeout)

	// A command that does go out would arrive after anything the dead one had sent.
	_, err = conn.Send(t.Context(), Send{
		Message: "ping", ReplyTypes: []string{"pong"},
		Content: map[string]string{"clientRequestId": "live"}, ClientRequestID: "live",
	})
	require.NoError(t, err)
	for _, msg := range srv.Received() {
		assert.NotEqual(t, "dead", msg.ClientRequestID(), "the cancelled command went out")
	}
}

// A ping outstanding when the socket dies leaves its deadline armed. If it is not disarmed it
// fires against the connection that replaced the dead one and tears it down.
func TestDeadSocketDisarmsThePongDeadline(t *testing.T) {
	srv := perpstest.New(t)
	srv.SetAutoPong(false)

	_, s := open(t, srv, func(o *Options) {
		o.PingInterval = 20 * time.Millisecond
		o.PongWait = time.Second
	})
	s.waitLifecycle(t, true)
	srv.NextMessage(settle) // the ping that will never be answered

	srv.SetAutoPong(true) // the replacement's pings are answered, so only a stale timer can fire
	srv.CloseConn(0, 1012, "Service restarting. Please reconnect.")
	assert.Equal(t, rails.ReasonServerShutdown, s.waitLifecycle(t, false).reason)
	s.waitLifecycle(t, true)

	select {
	case l := <-s.lifecycle:
		t.Errorf("a second disconnect (%q) for a socket that was already gone", l.reason)
	case <-time.After(1500 * time.Millisecond):
	}
}

// Two live requests under one clientRequestId would leave the first waiter with no reply of its
// own, since every frame is matched on that id alone.
func TestDuplicateInFlightRequestIsRefused(t *testing.T) {
	srv := perpstest.New(t)
	srv.SetAutoPong(false)
	conn, s := open(t, srv, nil)
	s.waitLifecycle(t, true)

	first := make(chan error, 1)
	go func() {
		_, err := conn.Send(t.Context(), Send{
			Message: "ping", Content: map[string]string{}, ClientRequestID: "shared",
		})
		first <- err
	}()
	srv.NextMessage(settle) // the first is now in flight

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := conn.Send(ctx, Send{
		Message: "ping", Content: map[string]string{}, ClientRequestID: "shared",
	})
	assert.ErrorIs(t, err, rails.ErrConflict)

	// The first waiter is untouched and still resolves normally.
	srv.Push(map[string]any{
		"resultType": "pong",
		"data":       map[string]any{"statusCode": 200, "clientRequestId": "shared"},
	})
	select {
	case err := <-first:
		assert.NoError(t, err, "the first request was disturbed")
	case <-time.After(settle):
		t.Error("the first request never resolved")
	}
}

// Close must return promptly even with a dial stuck on an endpoint that never answers.
func TestCloseCancelsAnInFlightDial(t *testing.T) {
	srv := perpstest.New(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	var dials atomic.Int64
	conn, s := open(t, srv, func(o *Options) {
		o.Endpoint = func(ctx context.Context) (string, string, error) {
			if dials.Add(1) == 1 {
				return srv.URL + "/?product=perpetuals&market=BTC-USDT", "authorization#jwt", nil
			}
			select { // a token lookup that never comes back
			case <-release:
			case <-ctx.Done():
			}
			return "", "", ctx.Err()
		}
	})
	s.waitLifecycle(t, true)

	srv.CloseConn(0, 1012, "Service restarting. Please reconnect.")
	s.waitLifecycle(t, false)

	done := make(chan struct{})
	go func() { conn.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(settle):
		t.Fatal("Close hung on a dial that never returned")
	}
}

// With no socket to write to, a command is refused outright and says nothing left: the caller
// may send it again, unlike one whose socket died after the write.
func TestSendDuringAReconnectIsNotSent(t *testing.T) {
	srv := perpstest.New(t)
	conn, s := open(t, srv, func(o *Options) {
		o.backoff = func(int) time.Duration { return time.Second } // wide enough to send into the gap
	})
	s.waitLifecycle(t, true)
	srv.CloseConn(0, 1012, "Service restarting. Please reconnect.")
	s.waitLifecycle(t, false)

	_, err := conn.Send(t.Context(), Send{Message: "ping", Content: map[string]string{}, ClientRequestID: "gap"})
	assert.ErrorIs(t, err, rails.ErrNotSent)
	assert.NotErrorIs(t, err, rails.ErrConnClosed)
	var apiErr *rails.Error
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "gap", apiErr.ClientRequestID)
}

// A matched frame goes to the watch and is not an event; everything else still is.
func TestWatchTakesTheMatchedFrame(t *testing.T) {
	srv := perpstest.New(t)
	conn, s := open(t, srv, nil)
	isCancelOf := func(id string) func(Frame) bool {
		return func(f Frame) bool {
			return f.ResultType == "cancelOrder" && string(f.Data) != "" && f.route().ClientRequestID == id
		}
	}

	w, err := conn.Watch(t.Context(), Watch{Op: "cancelOrder", ClientRequestID: "mine", Match: isCancelOf("theirs")})
	require.NoError(t, err)

	srv.Push(map[string]any{"resultType": "cancelOrder", "data": map[string]any{"statusCode": 202, "clientRequestId": "theirs"}})
	srv.Push(map[string]any{"resultType": "cancelOrder", "data": map[string]any{"statusCode": 200, "clientRequestId": "other"}})
	srv.Push(map[string]any{"resultType": "cancelOrder", "data": map[string]any{"statusCode": 200, "clientRequestId": "theirs"}})

	reply, err := w.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "theirs", reply.Frame.route().ClientRequestID)
	assert.False(t, reply.At.IsZero())

	// The acknowledgement and the unrelated answer were events; the match was not.
	var seen []string
	for range 2 {
		f := <-s.frames
		seen = append(seen, f.route().ClientRequestID)
	}
	assert.ElementsMatch(t, []string{"theirs", "other"}, seen)
	select {
	case f := <-s.frames:
		t.Fatalf("matched frame also delivered as an event: %s", f.Raw)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestWatchStopAndDeadline(t *testing.T) {
	t.Run("a stopped watch delivers nothing", func(t *testing.T) {
		srv := perpstest.New(t)
		conn, s := open(t, srv, nil)
		w, err := conn.Watch(t.Context(), Watch{Match: func(Frame) bool { return true }})
		require.NoError(t, err)
		w.Stop()
		srv.Push(map[string]any{"resultType": "cancelOrder", "data": map[string]any{"statusCode": 200, "clientRequestId": "x"}})
		f := <-s.frames
		assert.Equal(t, "x", f.route().ClientRequestID, "the frame reached the caller as an event")
	})

	t.Run("a context deadline ends the wait", func(t *testing.T) {
		srv := perpstest.New(t)
		conn, _ := open(t, srv, nil)
		w, err := conn.Watch(t.Context(), Watch{Op: "cancelOrder", ClientRequestID: "mine", Match: func(Frame) bool { return false }})
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, err = w.Wait(ctx)
		assert.ErrorIs(t, err, rails.ErrTimeout)
	})

}

// A frame delivered just before Close is the answer, not a closure.
func TestWatchDeliveredBeforeCloseIsKept(t *testing.T) {
	srv := perpstest.New(t)
	conn, s := open(t, srv, nil)
	w, err := conn.Watch(t.Context(), Watch{Op: "cancelOrder", ClientRequestID: "mine",
		Match: func(f Frame) bool { return f.ResultType == "cancelOrder" }})
	require.NoError(t, err)
	srv.Push(map[string]any{"resultType": "cancelOrder", "data": map[string]any{"statusCode": 200, "clientRequestId": "theirs"}})
	srv.Push(map[string]any{"resultType": "pong", "data": map[string]any{"statusCode": 200, "clientRequestId": "marker"}})
	<-s.frames // the marker: the matched frame before it has been routed
	conn.Close()

	reply, err := w.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "theirs", reply.Frame.route().ClientRequestID)
}

// A bare rate limit may be the answer to a write no caller waits on, so a command is not
// failed as dropped while one is unanswered.
func TestBareRateLimitWithAnotherWriteUnansweredIsNotPinned(t *testing.T) {
	bare := map[string]any{
		"statusCode": 429, "body": "Too many messages",
		"error": map[string]string{"slug": "TOO_MANY_REQUESTS", "code": "SYS-0003"}, "retryAfterSec": 1,
	}
	for name, tc := range map[string]struct {
		mutate  func(*Options)
		prepare func(t *testing.T, srv *perpstest.Server, s *sink)
	}{
		"a keepalive ping": {
			mutate: func(o *Options) { o.PingInterval, o.PongWait = 20*time.Millisecond, settle },
			prepare: func(t *testing.T, srv *perpstest.Server, _ *sink) {
				srv.SetAutoPong(false)
				require.Equal(t, "ping", srv.NextMessage(settle).Message)
			},
		},
		"a resume": {
			mutate: func(o *Options) {
				o.Resume = func() []Send {
					return []Send{{Message: "subscribe", ClientRequestID: "resume", Content: map[string]string{"clientRequestId": "resume"}}}
				}
			},
			prepare: func(t *testing.T, srv *perpstest.Server, s *sink) {
				s.waitLifecycle(t, true)
				srv.CloseConn(0, 1012, "Service restarting. Please reconnect.")
				s.waitLifecycle(t, true)
				require.Equal(t, "subscribe", srv.NextMessage(settle).Message)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			srv := perpstest.New(t)
			conn, s := open(t, srv, tc.mutate)
			tc.prepare(t, srv, s)

			done := make(chan error, 1)
			go func() {
				_, err := conn.Send(t.Context(), Send{Message: "createOrder", ClientRequestID: "order", Content: map[string]string{"clientRequestId": "order"}})
				done <- err
			}()
			require.Equal(t, "createOrder", srv.NextMessage(settle).Message)
			srv.Push(bare)
			srv.Push(map[string]any{"resultType": "createOrder", "data": map[string]any{"statusCode": 200, "clientRequestId": "order"}})

			select {
			case err := <-done:
				assert.NoError(t, err)
			case <-time.After(settle):
				t.Fatal("no answer")
			}
		})
	}
}

// A command left unanswered on a handed-over socket fails when that socket is dropped, rather
// than waiting on an answer that can no longer come.
func TestAnUnansweredCommandOnAHandedOverSocketFails(t *testing.T) {
	srv := perpstest.New(t)
	conn, s := open(t, srv, func(o *Options) { o.grace = 50 * time.Millisecond })
	s.waitLifecycle(t, true)

	done := make(chan error, 1)
	go func() {
		_, err := conn.Send(context.WithoutCancel(t.Context()), Send{Message: "createOrder", ClientRequestID: "order", Content: map[string]string{"clientRequestId": "order"}})
		done <- err
	}()
	require.Equal(t, "createOrder", srv.NextMessage(settle).Message)
	srv.PushTo(0, map[string]any{"resultType": "connectionShutdown", "data": map[string]any{"statusCode": 200, "forceCloseInMs": 60000, "ts": 1756000000000}})

	select {
	case err := <-done:
		assert.ErrorIs(t, err, rails.ErrConnClosed)
	case <-time.After(settle):
		t.Fatal("the command was never settled")
	}
}

// A bare rate limit with only a resume unanswered was that resume's, so it no longer hides
// the next command's.
func TestABareRateLimitSettlesALoneResume(t *testing.T) {
	bare := map[string]any{
		"statusCode": 429, "body": "Too many messages",
		"error": map[string]string{"slug": "TOO_MANY_REQUESTS", "code": "SYS-0003"}, "retryAfterSec": 1,
	}
	srv := perpstest.New(t)
	conn, s := open(t, srv, func(o *Options) {
		o.Resume = func() []Send {
			return []Send{{Message: "subscribe", ClientRequestID: "resume", Content: map[string]string{"clientRequestId": "resume"}}}
		}
	})
	s.waitLifecycle(t, true)
	srv.CloseConn(0, 1012, "Service restarting. Please reconnect.")
	s.waitLifecycle(t, true)
	require.Equal(t, "subscribe", srv.NextMessage(settle).Message)
	srv.Push(bare)
	<-s.frames

	done := make(chan error, 1)
	go func() {
		_, err := conn.Send(t.Context(), Send{Message: "createOrder", ClientRequestID: "order", Content: map[string]string{"clientRequestId": "order"}})
		done <- err
	}()
	require.Equal(t, "createOrder", srv.NextMessage(settle).Message)
	srv.Push(bare)

	select {
	case err := <-done:
		assert.ErrorIs(t, err, rails.ErrNotSent)
	case <-time.After(settle):
		t.Fatal("the dropped command was not failed")
	}
}
