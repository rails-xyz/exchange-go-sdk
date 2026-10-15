package ws

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// Defaults for keepalive. The application-level ping is preferred over a control frame because
// it travels the same path as subscriptions and requests, so it fails when they would.
const (
	DefaultPingInterval = 20 * time.Second
	DefaultPongWait     = 10 * time.Second
	writeTimeout        = 10 * time.Second
	handoverGrace       = 5 * time.Second
)

// Options configure a connection. Endpoint is the only one with no useful zero value.
type Options struct {
	Endpoint     Endpoint
	HTTPClient   *http.Client
	UserAgent    string
	Hooks        rails.Hooks
	Logger       *slog.Logger
	ConnID       string
	Market       string
	PingInterval time.Duration // zero disables the application-level ping
	PongWait     time.Duration

	// OnFrame receives every frame the connection did not consume as a command's answer;
	// OnConnected and OnDisconnected report each socket coming up or going away. All three run
	// on the connection's owner goroutine, so one that blocks stalls the connection — which for
	// order traffic beats dropping frames — and all three must return once [Conn.Close] has
	// been called.
	OnFrame        func(Frame)
	OnConnected    func()
	OnDisconnected func(rails.DisconnectReason)

	// Resume returns commands to write first on every socket after the first, for state the
	// endpoint cannot carry. It runs on the owner goroutine; their answers go to OnFrame. One
	// with a ClientRequestID counts as unanswered until its answer arrives.
	Resume func() []Send

	// Test seams: dialer replaces the real dial, backoff the wait before a reconnect, grace how
	// long a handed-over socket keeps delivering.
	dialer  func(ctx context.Context, url, subprotocol string) (socket, error)
	backoff func(attempt int) time.Duration
	grace   time.Duration
}

// Conn is one WebSocket connection that survives reconnects. Every method is safe to call from
// any goroutine; none of them touch shared state directly.
type Conn struct {
	opts     Options
	requests chan request
	done     chan struct{}
	finished chan struct{}

	closeOnce sync.Once
}

// Send delivers one command and waits for the frame that ends it.
type Send struct {
	Message         string
	Content         any
	ClientRequestID string

	// ReplyTypes are the resultTypes this command's answers carry; defaults to the command's
	// name. A matched frame with any other resultType is passed to OnFrame as well as ending
	// the request.
	ReplyTypes []string

	// AckIsAnswer makes the reply type's 202 the answer; other resultTypes under the same id go
	// to OnFrame, before or after it, as a fan-out command's per-item results do.
	AckIsAnswer bool

	// Market is the market the command names, for [rails.Hooks] only.
	Market string
}

type request struct {
	send *Send
	// forget is the clientRequestId to stop tracking; reply says which request that was.
	forget         string
	reply          chan result
	watch, unwatch *Watch
}

type result struct {
	reply Reply
	err   error
}

// Reply is how the exchange answered a command.
type Reply struct {
	// Frame ended the request. SentAt is when the command was written, At when Frame arrived,
	// AckAt when the 202 before it did; AckAt is zero when the exchange answered without
	// acknowledging first.
	Frame  Frame
	SentAt time.Time
	At     time.Time
	AckAt  time.Time
	// APIQuotaUsed is what the acknowledgement reported.
	APIQuotaUsed int64
}

// Watch waits for a terminal frame no request of this connection is waiting for, such as the
// answer to a command sent elsewhere. Register it before sending your own command, so nothing
// can land in between.
type Watch struct {
	// Op and ClientRequestID name the caller's own command, for the errors a wait produces.
	Op              string
	ClientRequestID string
	// Match runs on the connection's owner goroutine.
	Match func(Frame) bool

	conn  *Conn
	reply chan result
}

// Watch registers w. Stop it when the frame is no longer wanted.
func (c *Conn) Watch(ctx context.Context, w Watch) (*Watch, error) {
	w.conn, w.reply = c, make(chan result, 1)
	select {
	case c.requests <- request{watch: &w}:
		return &w, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, closedError(w.Op, w.ClientRequestID)
	}
}

// Wait blocks until the matched frame arrives; an error status comes back as the error it
// describes, as a command's own answer would.
func (w *Watch) Wait(ctx context.Context) (Reply, error) {
	select {
	case got := <-w.reply:
		return got.reply, got.err
	case <-ctx.Done():
		w.Stop()
		return Reply{}, timeoutError(w.Op, w.ClientRequestID,
			"the exchange did not answer before the context expired")
	case <-w.conn.done:
		// The frame may have landed just before the close.
		select {
		case got := <-w.reply:
			return got.reply, got.err
		default:
			return Reply{}, closedError(w.Op, w.ClientRequestID)
		}
	}
}

// Stop withdraws the watch. Safe after the frame arrived or the connection closed.
func (w *Watch) Stop() {
	select {
	case w.conn.requests <- request{unwatch: w}:
	case <-w.conn.done:
	}
}

// pending is a request the exchange has not finished answering.
type pending struct {
	op          string
	replyTypes  []string
	ackIsAnswer bool
	market      string
	sentAt      time.Time
	ackAt       time.Time
	quota       int64
	reply       chan result // nil for the connection's own keepalive ping, and for a resume
	resumed     bool        // written by Resume; its answer goes to OnFrame
	gen         int         // the socket it was written on
}

func (s Send) replyTypes() []string {
	if len(s.ReplyTypes) > 0 {
		return s.ReplyTypes
	}
	return []string{s.Message}
}

// Open dials and starts the connection. The first dial is synchronous so a bad token, a soft
// ban or a wrong URL is reported to the caller rather than retried in the background.
func Open(ctx context.Context, opts Options) (*Conn, error) {
	opts.applyDefaults()

	c := &Conn{
		opts:     opts,
		requests: make(chan request),
		done:     make(chan struct{}),
		finished: make(chan struct{}),
	}

	info := rails.ConnInfo{ConnID: opts.ConnID, Market: opts.Market}
	opts.Hooks.ConnectStarted(info)
	sock, stats, err := c.dialOnce(ctx)
	opts.Hooks.ConnectFinished(info, stats, err)
	if err != nil {
		return nil, err
	}
	go c.run(sock)
	return c, nil
}

func (o *Options) applyDefaults() {
	if o.HTTPClient == nil {
		o.HTTPClient = &http.Client{}
	}
	fillHooks(&o.Hooks)
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.PongWait <= 0 {
		o.PongWait = DefaultPongWait
	}
	if o.OnFrame == nil {
		o.OnFrame = func(Frame) {}
	}
	if o.OnConnected == nil {
		o.OnConnected = func() {}
	}
	if o.OnDisconnected == nil {
		o.OnDisconnected = func(rails.DisconnectReason) {}
	}
	if o.Resume == nil {
		o.Resume = func() []Send { return nil }
	}
	if o.grace == 0 {
		o.grace = handoverGrace
	}
	if o.backoff == nil {
		o.backoff = backoff
	}
}

// fillHooks gives every unset hook a no-op, so the owner calls them outright.
func fillHooks(h *rails.Hooks) {
	if h.ConnectStarted == nil {
		h.ConnectStarted = func(rails.ConnInfo) {}
	}
	if h.ConnectFinished == nil {
		h.ConnectFinished = func(rails.ConnInfo, rails.ConnectStats, error) {}
	}
	if h.Disconnected == nil {
		h.Disconnected = func(rails.ConnInfo, rails.DisconnectReason) {}
	}
	if h.PingRTT == nil {
		h.PingRTT = func(rails.ConnInfo, time.Duration) {}
	}
	if h.RequestAcked == nil {
		h.RequestAcked = func(rails.ReqInfo, time.Duration) {}
	}
	if h.RequestDone == nil {
		h.RequestDone = func(rails.ReqInfo, time.Duration, error) {}
	}
}

// Send issues a command and blocks until the exchange answers it terminally. It is never
// resent: a reused clientRequestId is rejected or silently dropped, so a caller that gives up
// reconciles on the id instead. A bare context error means the command never went out.
func (c *Conn) Send(ctx context.Context, s Send) (Reply, error) {
	// Checked first: a select with a dead context and a free owner would pick at random.
	if err := ctx.Err(); err != nil {
		return Reply{}, err
	}
	reply := make(chan result, 1)

	select {
	case c.requests <- request{send: &s, reply: reply}:
	case <-ctx.Done():
		return Reply{}, ctx.Err()
	case <-c.done:
		return Reply{}, notSentError(s.Message, s.ClientRequestID)
	}

	select {
	case got := <-reply:
		return got.reply, got.err
	case <-ctx.Done():
		// The answer may have landed in the same instant; take it rather than send the caller
		// off to reconcile an order it could have known about.
		select {
		case got := <-reply:
			return got.reply, got.err
		default:
		}
		// Handed off: this call is bounded by the caller's context, not by when the owner
		// next comes free.
		go c.forget(s.ClientRequestID, reply)
		return Reply{}, timeoutError(s.Message, s.ClientRequestID,
			"the exchange did not answer before the context expired")
	case <-c.done:
		return Reply{}, closedError(s.Message, s.ClientRequestID)
	}
}

// forget tells the owner to stop tracking a request whose caller has walked away.
func (c *Conn) forget(clientRequestID string, reply chan result) {
	select {
	case c.requests <- request{forget: clientRequestID, reply: reply}:
	case <-c.done:
	}
}

func timeoutError(op, clientRequestID, message string) error {
	return fmt.Errorf("%w: %w", rails.ErrTimeout, &rails.Error{
		Op:              op,
		Message:         message,
		ClientRequestID: clientRequestID,
	})
}

func closedError(op, clientRequestID string) error {
	return fmt.Errorf("%w: %w", rails.ErrConnClosed, &rails.Error{
		Op:              op,
		Message:         "no answer before the connection closed",
		ClientRequestID: clientRequestID,
	})
}

// notSentError is for a command that found no socket to go out on, during a reconnect or after
// Close. Nothing left, so unlike closedError it invites a retry.
func notSentError(op, clientRequestID string) error {
	return fmt.Errorf("%w: %w", rails.ErrNotSent, downError{&rails.Error{
		Op:              op,
		Message:         "the connection is down",
		ClientRequestID: clientRequestID,
	}})
}

// downError tells a command that found no socket apart from one a pacer or the exchange's rate
// limit refused, which are not sent either.
type downError struct{ err *rails.Error }

func (e downError) Error() string { return e.err.Error() }
func (e downError) Unwrap() error { return e.err }

// IsDown reports whether a command failed because it found no socket to go out on.
func IsDown(err error) bool {
	var down downError
	return errors.As(err, &down)
}

// Close shuts the connection down and waits for its goroutines to finish.
func (c *Conn) Close() {
	c.closeOnce.Do(func() { close(c.done) })
	<-c.finished
}
