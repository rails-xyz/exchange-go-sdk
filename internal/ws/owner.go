package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// inbound is one frame, or the error that ended a socket, tagged with the socket it came from.
type inbound struct {
	gen   int
	frame Frame
	err   error
}

// owner holds everything a connection knows. Only the goroutine running run touches it.
type owner struct {
	*Conn

	sock     socket
	gen      int
	pending  map[string]*pending
	watchers map[*Watch]struct{}

	frames chan inbound
	dials  chan dialResult

	dialCtx    context.Context
	cancelDial context.CancelFunc
	dialers    sync.WaitGroup

	retiring    socket // the draining half of a handover, still delivering
	retiringGen int
	retireTimer *time.Timer // when the draining half is dropped

	dialing   bool // a dial goroutine is running
	scheduled bool // a dial is armed on retryTimer
	attempt   int
	notReady  time.Time // no dial before this; a soft ban or a Retry-After sets it

	pingTicker *time.Ticker
	pongTimer  *time.Timer
	retryTimer *time.Timer

	pingWaiting string // clientRequestId of the keepalive ping in flight
}

type dialResult struct {
	sock  socket
	stats rails.ConnectStats
	err   error
}

// run is the connection's single owner. Every piece of mutable state above lives here and is
// reached only through the channels this loop selects on.
func (c *Conn) run(sock socket) {
	defer close(c.finished)

	o := &owner{
		Conn:     c,
		sock:     sock,
		pending:  map[string]*pending{},
		watchers: map[*Watch]struct{}{},
		frames:   make(chan inbound, 64),
		dials:    make(chan dialResult),
	}
	o.pingTicker = newTicker(c.opts.PingInterval)
	o.pongTimer = stoppedTimer()
	o.retryTimer = stoppedTimer()
	o.retireTimer = stoppedTimer()
	o.dialCtx, o.cancelDial = context.WithCancel(context.Background())
	defer o.shutdown()

	go o.read(o.sock, o.gen)
	o.opts.OnConnected()

	for {
		select {
		case <-c.done:
			return

		case in := <-o.frames:
			o.onInbound(in)

		case req := <-c.requests:
			o.onRequest(req)

		case <-o.pingTicker.C:
			o.sendKeepalivePing()

		case <-o.pongTimer.C:
			// The socket may still be writable, but a ping that goes unanswered means the
			// path the orders take is not working.
			o.opts.Logger.Debug("no pong before the deadline, reconnecting", "connId", o.opts.ConnID)
			o.disconnect(rails.ReasonMissedPong)
			o.startDial(0)

		case res := <-o.dials:
			o.onDialResult(res)

		case <-o.retryTimer.C:
			o.scheduled = false
			o.beginDial()

		case <-o.retireTimer.C:
			o.dropRetiring()
		}
	}
}

// onInbound routes one frame from any live socket. Frames from a socket being handed over are
// still real events, so they are delivered until its reader stops.
func (o *owner) onInbound(in inbound) {
	if in.err != nil {
		if in.gen != o.gen || o.sock == nil {
			return // a handover's old half, or the echo of a socket this loop already closed
		}
		o.opts.Logger.Debug("read failed, reconnecting", "connId", o.opts.ConnID, "err", in.err)
		o.disconnect(closeReason(in.err))
		o.startDial(0)
		return
	}

	route := in.frame.route()
	consumed := false
	if p, ok := o.pending[route.ClientRequestID]; ok {
		consumed = o.answer(route.ClientRequestID, p, in.frame, route)
	}
	if bare := in.frame.BareError(); bare != nil && bare.StatusCode == http.StatusTooManyRequests {
		o.failDropped(bare)
	}
	// A request's own answer may be what another caller is watching for.
	if terminal(route.StatusCode) && o.notifyWatchers(in.frame, route) {
		consumed = true
	}
	if consumed {
		return
	}

	o.opts.OnFrame(in.frame)

	if in.frame.ResultType == resultConnectionShutdown {
		// Open the replacement before closing this one: the old socket keeps delivering until
		// the deadline, so a handover loses nothing.
		o.opts.Logger.Info("server is draining, handing over", "connId", o.opts.ConnID)
		o.handOver()
	}
}

const (
	resultConnectionShutdown = "connectionShutdown"
	// statusAccepted is the only non-terminal status: the command was taken, not yet answered.
	statusAccepted = 202
)

// answer applies one frame to the request it correlates with, and reports whether that
// consumed it. A frame the request did not ask for still ends it, and is still an event,
// unless the request is [Send.AckIsAnswer].
func (o *owner) answer(id string, p *pending, frame Frame, route routing) bool {
	if p.resumed {
		delete(o.pending, id)
		return false
	}
	if p.reply == nil { // the connection's own keepalive ping
		if id == o.pingWaiting {
			o.pingWaiting = ""
			o.pongTimer.Stop()
			o.hooks().PingRTT(o.connInfo(), time.Since(p.sentAt))
		}
		delete(o.pending, id)
		return true
	}

	consumed := slices.Contains(p.replyTypes, frame.ResultType)
	if p.ackIsAnswer && !consumed {
		return false
	}

	if !terminal(route.StatusCode) {
		if route.StatusCode != statusAccepted {
			return consumed
		}
		p.ackAt = time.Now()
		p.quota = route.APIQuotaUsed
		o.hooks().RequestAcked(o.reqInfo(p, id), p.ackAt.Sub(p.sentAt))
		if !p.ackIsAnswer {
			return consumed
		}
	}

	delete(o.pending, id)
	at := time.Now()
	var err error
	if route.StatusCode >= 400 {
		err = route.toError(p.op)
	}
	o.hooks().RequestDone(o.reqInfo(p, id), at.Sub(p.sentAt), err)
	p.reply <- result{
		reply: Reply{Frame: frame, SentAt: p.sentAt, At: at, AckAt: p.ackAt, APIQuotaUsed: p.quota},
		err:   err,
	}
	return consumed
}

// failDropped attributes a bare rate-limit frame, which names no clientRequestId: with exactly
// one write unanswered and not yet acknowledged, and that one a caller's command, it is the one
// dropped and may be sent again. Otherwise nothing is known.
func (o *owner) failDropped(bare *rails.Error) {
	var id string
	var dropped *pending
	for candidate, p := range o.pending {
		if !p.ackAt.IsZero() {
			continue
		}
		if dropped != nil {
			return
		}
		id, dropped = candidate, p
	}
	if dropped == nil {
		return
	}
	if dropped.resumed {
		delete(o.pending, id) // its answer is not coming, so it no longer hides a command's
		return
	}
	if dropped.reply == nil {
		return
	}
	delete(o.pending, id)
	failure := &rails.Error{
		Op:              dropped.op,
		StatusCode:      bare.StatusCode,
		Slug:            bare.Slug,
		Code:            bare.Code,
		Message:         bare.Message,
		RetryAfter:      bare.RetryAfter,
		ClientRequestID: id,
	}
	err := fmt.Errorf("%w: %w", rails.ErrNotSent, failure)
	o.hooks().RequestDone(o.reqInfo(dropped, id), time.Since(dropped.sentAt), err)
	dropped.reply <- result{err: err}
}

// notifyWatchers hands a terminal frame to every watch it satisfies and reports whether any
// took it.
func (o *owner) notifyWatchers(frame Frame, route routing) bool {
	taken := false
	for w := range o.watchers {
		if !w.Match(frame) {
			continue
		}
		delete(o.watchers, w)
		var err error
		if route.StatusCode >= 400 {
			err = route.toError(w.Op)
		}
		w.reply <- result{reply: Reply{Frame: frame, At: time.Now()}, err: err}
		taken = true
	}
	return taken
}

// onRequest writes a caller's command, or drops a request whose caller gave up.
func (o *owner) onRequest(req request) {
	if req.watch != nil {
		o.watchers[req.watch] = struct{}{}
		return
	}
	if req.unwatch != nil {
		delete(o.watchers, req.unwatch)
		return
	}
	if req.forget != "" {
		p, ok := o.pending[req.forget]
		if !ok || p.reply != req.reply {
			return // already answered, or a later request under the same id
		}
		delete(o.pending, req.forget)
		o.hooks().RequestDone(o.reqInfo(p, req.forget), time.Since(p.sentAt),
			timeoutError(p.op, req.forget, "the caller stopped waiting"))
		return
	}

	s := req.send
	if o.sock == nil {
		req.reply <- result{err: notSentError(s.Message, s.ClientRequestID)}
		return
	}
	// Frames are matched on that id alone, so a second live request under it would leave the
	// first waiter with no reply of its own.
	if _, exists := o.pending[s.ClientRequestID]; exists {
		req.reply <- result{err: fmt.Errorf("%w: %w", rails.ErrConflict, &rails.Error{
			Op:              s.Message,
			Message:         "a request with this clientRequestId is already in flight",
			ClientRequestID: s.ClientRequestID,
		})}
		return
	}
	if err := o.write(s.Message, s.Content); err != nil {
		// The frame may still have been partly written, so the caller is told what a
		// disconnect tells it: reconcile on the id, do not resend.
		failure := fmt.Errorf("%w: %w", rails.ErrConnClosed, &rails.Error{
			Op:              s.Message,
			Message:         err.Error(),
			ClientRequestID: s.ClientRequestID,
		})
		req.reply <- result{err: failure}
		o.hooks().RequestDone(rails.ReqInfo{
			ConnID: o.opts.ConnID, Op: s.Message, Market: o.market(s.Market),
			ClientRequestID: s.ClientRequestID,
		}, 0, failure)
		o.onWriteFailed(err)
		return
	}
	o.pending[s.ClientRequestID] = &pending{
		op:          s.Message,
		replyTypes:  s.replyTypes(),
		ackIsAnswer: s.AckIsAnswer,
		market:      s.Market,
		sentAt:      time.Now(),
		reply:       req.reply,
		gen:         o.gen,
	}
}

// sendKeepalivePing sends the application-level ping whose round trip is the connection's
// latency measurement, and arms the deadline that declares the socket dead without one.
func (o *owner) sendKeepalivePing() {
	if o.sock == nil || o.pingWaiting != "" {
		return // still waiting on the last one; the pong deadline will deal with it
	}
	id := uuid.NewString()
	if err := o.write("ping", map[string]string{"clientRequestId": id}); err != nil {
		o.onWriteFailed(err)
		return
	}
	o.pingWaiting = id
	o.pending[id] = &pending{op: "ping", sentAt: time.Now(), gen: o.gen}
	o.pongTimer.Reset(o.opts.PongWait)
}

// onWriteFailed tears the socket down. A write that fails is not something the read side is
// guaranteed to notice: a half-open socket can refuse writes while its read blocks forever.
func (o *owner) onWriteFailed(err error) {
	o.opts.Logger.Debug("write failed, reconnecting", "connId", o.opts.ConnID, "err", err)
	o.disconnect(rails.ReasonWriteError)
	o.startDial(0)
}

func (o *owner) write(message string, content any) error {
	body, err := json.Marshal(Request{Message: message, Content: content})
	if err != nil {
		return fmt.Errorf("%s: encoding request: %w", message, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	if err := o.sock.Write(ctx, websocket.MessageText, body); err != nil {
		return fmt.Errorf("%s: %w", message, err)
	}
	return nil
}

// handOver opens the replacement for a draining socket straight away. The exchange keeps the
// old one alive until its deadline, so the budget floor that paces reconnects does not apply.
func (o *owner) handOver() {
	if o.dialing || o.scheduled {
		return
	}
	if until := time.Until(o.notReady); until > 0 {
		o.startDial(0)
		return
	}
	o.beginDial()
}

// startDial schedules a reconnect, honouring anything the server said about when to come back.
func (o *owner) startDial(after time.Duration) {
	if o.dialing || o.scheduled {
		return
	}
	wait := max(after, o.opts.backoff(o.attempt))
	if until := time.Until(o.notReady); until > wait {
		wait = until
	}
	o.attempt++
	o.scheduled = true
	o.retryTimer.Reset(wait)
}

func (o *owner) beginDial() {
	if o.dialing {
		return
	}
	o.dialing = true
	o.hooks().ConnectStarted(o.connInfo())
	o.dialers.Add(1)
	go func() {
		defer o.dialers.Done()
		// Bound to the connection, so Close cancels a dial that is stuck on a slow token
		// lookup or an unresponsive upgrade.
		sock, stats, err := o.dialOnce(o.dialCtx)
		select {
		case o.dials <- dialResult{sock: sock, stats: stats, err: err}:
		case <-o.done:
			if sock != nil {
				_ = sock.CloseNow()
			}
		}
	}()
}

// onDialResult installs a new socket, or schedules another attempt.
func (o *owner) onDialResult(res dialResult) {
	o.dialing = false

	if res.err != nil {
		o.hooks().ConnectFinished(o.connInfo(), res.stats, res.err)
		var apiErr *rails.Error
		if errors.As(res.err, &apiErr) && apiErr.RetryAfter > 0 {
			// Nothing connects before then, so do not spend attempts finding out.
			o.notReady = time.Now().Add(apiErr.RetryAfter)
			if errors.Is(res.err, rails.ErrSoftBanned) {
				o.opts.Logger.Warn("account is soft banned", "connId", o.opts.ConnID,
					"retryAfter", apiErr.RetryAfter)
			}
		}
		o.startDial(0)
		return
	}

	old := o.sock
	o.sock = res.sock
	o.gen++
	o.notReady = time.Time{}
	if o.pingWaiting != "" {
		// Its pong, if one still comes on the old socket, is not worth a map entry for ever.
		delete(o.pending, o.pingWaiting)
		o.pingWaiting = ""
	}
	for id, p := range o.pending {
		if p.resumed {
			delete(o.pending, id) // the old socket's; nothing waits on it
		}
	}
	// A ping outstanding on the dead socket left its deadline armed; letting it fire would
	// tear down the connection that just replaced it.
	o.pongTimer.Stop()
	go o.read(o.sock, o.gen)
	o.hooks().ConnectFinished(o.connInfo(), res.stats, nil)
	o.attempt = 0
	o.opts.OnConnected()

	if old != nil {
		// Keep the draining socket reading. A graceful close discards data messages received
		// during its handshake, so anything still in flight would be lost by closing now.
		o.dropRetiring()
		o.retiring, o.retiringGen = old, o.gen-1
		o.retireTimer.Reset(o.opts.grace)
	}

	// Nothing a caller sends can be written before this handler returns, so these go first.
	for _, s := range o.opts.Resume() {
		if err := o.write(s.Message, s.Content); err != nil {
			o.onWriteFailed(err)
			return
		}
		if s.ClientRequestID != "" {
			o.pending[s.ClientRequestID] = &pending{op: s.Message, sentAt: time.Now(), resumed: true, gen: o.gen}
		}
	}
}

// dropRetiring ends the draining half of a handover once its grace period is up, failing what
// was written on it and is still unanswered: no answer comes once it is closed.
func (o *owner) dropRetiring() {
	if o.retiring == nil {
		return
	}
	_ = o.retiring.CloseNow()
	o.retiring = nil
	o.retireTimer.Stop()
	for id, p := range o.pending {
		if p.gen != o.retiringGen {
			continue
		}
		delete(o.pending, id)
		if p.reply == nil {
			continue
		}
		err := closedError(p.op, id)
		p.reply <- result{err: err}
		o.hooks().RequestDone(o.reqInfo(p, id), time.Since(p.sentAt), err)
	}
}

// disconnect drops the current socket and fails every request still waiting on it, so no
// caller is left hanging on a connection that no longer exists.
func (o *owner) disconnect(reason rails.DisconnectReason) {
	if o.sock != nil {
		_ = o.sock.CloseNow()
		o.sock = nil
	}
	o.pongTimer.Stop()
	o.pingWaiting = ""
	// Before the callback, which may block until a consumer takes the event.
	o.failPending()
	o.hooks().Disconnected(o.connInfo(), reason)
	o.opts.OnDisconnected(reason)
}

// failPending answers every request and watch still waiting, so no caller is left holding a
// reply channel nothing will write to.
func (o *owner) failPending() {
	for w := range o.watchers {
		delete(o.watchers, w)
		w.reply <- result{err: closedError(w.Op, w.ClientRequestID)}
	}
	for id, p := range o.pending {
		delete(o.pending, id)
		if p.reply == nil {
			continue // the connection's own keepalive ping
		}
		err := closedError(p.op, id)
		p.reply <- result{err: err}
		o.hooks().RequestDone(o.reqInfo(p, id), time.Since(p.sentAt), err)
	}
}

// shutdown runs when the owner stops, on Close or on a fatal error.
func (o *owner) shutdown() {
	o.pingTicker.Stop()
	o.pongTimer.Stop()
	o.retryTimer.Stop()
	o.retireTimer.Stop()
	o.cancelDial()
	o.dialers.Wait()
	o.dropRetiring()
	if o.sock != nil {
		_ = o.sock.Close(websocket.StatusNormalClosure, "client closing")
		o.sock = nil
	}
	o.failPending()
}

// read is the only goroutine that touches a socket's read side. It forwards frames and exits
// on the first error, which tells the owner the socket is gone.
func (o *owner) read(sock socket, gen int) {
	for {
		_, message, err := sock.Read(context.Background())
		if err != nil {
			select {
			case o.frames <- inbound{gen: gen, err: err}:
			case <-o.done:
			}
			return
		}
		select {
		case o.frames <- inbound{gen: gen, frame: parse(message)}:
		case <-o.done:
			return
		}
	}
}

func (c *Conn) dialOnce(ctx context.Context) (socket, rails.ConnectStats, error) {
	url, subprotocol, err := c.opts.Endpoint(ctx)
	if err != nil {
		return nil, rails.ConnectStats{}, err
	}
	if c.opts.dialer != nil {
		sock, err := c.opts.dialer(ctx, url, subprotocol)
		return sock, rails.ConnectStats{}, err
	}
	return dial(ctx, c.opts.HTTPClient, url, subprotocol, c.opts.UserAgent)
}

func (o *owner) hooks() rails.Hooks { return o.opts.Hooks }

func (o *owner) connInfo() rails.ConnInfo {
	return rails.ConnInfo{ConnID: o.opts.ConnID, Market: o.opts.Market, Attempt: o.attempt}
}

func (o *owner) reqInfo(p *pending, id string) rails.ReqInfo {
	return rails.ReqInfo{
		ConnID: o.opts.ConnID, Op: p.op, Market: o.market(p.market), ClientRequestID: id,
	}
}

// market falls back to the connection's, which is what a command naming none gets.
func (o *owner) market(named string) string {
	if named == "" {
		return o.opts.Market
	}
	return named
}
