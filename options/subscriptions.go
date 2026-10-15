package options

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/rails-xyz/exchange-go-sdk/internal/ws"
	"github.com/rails-xyz/exchange-go-sdk/internal/wsevent"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

const (
	opSubscribe   = "subscribe"
	opUnsubscribe = "unsubscribe"

	resultSubscribed   = "subscribed"
	resultUnsubscribed = "unsubscribed"
)

// Subscribe adds streams for the contracts of scope, which need not be the connection's own. A
// scope covered by one already held is a no-op; a broader one replaces the narrower ones. The
// account summary is always held for every market. Subscriptions survive reconnects; over the
// connection's limit the error carries [rails.CodeSubscriptionLimit]. When ctx ends before the
// exchange answers, the error wraps [rails.ErrTimeout] and the change counts once it does.
func (c *Conn) Subscribe(ctx context.Context, scope Scope, subscriptions ...Subscription) error {
	return c.changeSubscriptions(ctx, opSubscribe, resultSubscribed, scope, subscriptions)
}

// Unsubscribe removes streams from every held scope that scope covers; [ScopeAll] removes them
// everywhere. The exchange refuses one that removes nothing, and one that names a scope of a
// stream held for every market. Without the orders stream, every order command fails with
// [ErrNotSubscribed]. A timeout is as for [Conn.Subscribe].
func (c *Conn) Unsubscribe(ctx context.Context, scope Scope, subscriptions ...Subscription) error {
	return c.changeSubscriptions(ctx, opUnsubscribe, resultUnsubscribed, scope, subscriptions)
}

func (c *Conn) changeSubscriptions(ctx context.Context, op, resultType string, scope Scope, subscriptions []Subscription) error {
	if err := scope.validate(); err != nil {
		return err
	}
	if len(subscriptions) == 0 {
		return fmt.Errorf("%s: no subscriptions given", op)
	}
	// One change at a time, so the set here changes in the order the exchange's does.
	select {
	case c.changing <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil { // both were ready, and the select took the gate
		<-c.changing
		return err
	}
	// The exchange applies a change whether or not the caller still waits, so the set follows
	// its answer either way. The caller may reuse its slice once this returns.
	subscriptions = slices.Clone(subscriptions)
	done := make(chan error, 1)
	go func() {
		defer func() { <-c.changing }()
		done <- c.applySubscriptionChange(context.WithoutCancel(ctx), op, resultType, scope, subscriptions)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", rails.ErrTimeout, &rails.Error{
			Op: op, Message: "the exchange did not answer before the context expired",
		})
	}
}

func (c *Conn) applySubscriptionChange(ctx context.Context, op, resultType string, scope Scope, subscriptions []Subscription) error {
	resumes := c.resumes.Load()
	if err := c.sendSubscriptionChange(ctx, op, resultType, scope, subscriptions); err != nil {
		return err
	}
	// A socket replaced meanwhile may have taken the change with it, so the new one gets it too.
	// Sending it twice is harmless, but a repeated unsubscribe is refused for removing nothing.
	for r := c.resumes.Load(); r != resumes; r = c.resumes.Load() {
		resumes = r
		err := c.sendSubscriptionChange(ctx, op, resultType, scope, subscriptions)
		var apiErr *rails.Error
		switch {
		case err == nil:
		case op == opUnsubscribe && errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusBadRequest:
		case op == opSubscribe:
			// The new socket may not hold it, so neither does the set.
			c.updateSubscribed(func(set subscriptionSet) subscriptionSet { return set.unsubscribe(scope, subscriptions) })
			return err
		default:
			return err
		}
	}
	return nil
}

func (c *Conn) sendSubscriptionChange(ctx context.Context, op, resultType string, scope Scope, subscriptions []Subscription) error {
	id := uuid.NewString()
	reply, err := c.ws.Send(ctx, ws.Send{
		Message:         op,
		ClientRequestID: id,
		// An error is answered under the command's own name.
		ReplyTypes: []string{resultType, op},
		Content:    newSubscriptionContent(id, scope, subscriptions),
	})
	if err != nil {
		return err
	}
	var data subscriptionData
	if err := json.Unmarshal(reply.Frame.Data, &data); err != nil {
		return fmt.Errorf("%s: decoding the answer: %w", op, err)
	}
	c.updateSubscribed(func(set subscriptionSet) subscriptionSet {
		if op == opSubscribe {
			set = set.subscribe(scope, subscriptions)
		} else {
			set = set.unsubscribe(scope, subscriptions)
		}
		return set.only(data.ActiveSubscriptions)
	})
	return nil
}

// updateSubscribed applies change, again if a refused resume changed the set meanwhile.
func (c *Conn) updateSubscribed(change func(subscriptionSet) subscriptionSet) {
	for {
		old := c.subscribed.Load()
		updated := change(*old)
		if c.subscribed.CompareAndSwap(old, &updated) {
			return
		}
	}
}

// resume brings the new socket from what its URL carried to the set held now, which a change
// on the replaced socket may have moved on since. It runs on the owner goroutine.
func (c *Conn) resume() []ws.Send {
	c.resumes.Add(1)
	clear(c.resuming)
	set, carried := *c.subscribed.Load(), *c.carried.Load()

	now := set.dialed(c.scope)
	stale := slices.DeleteFunc(slices.Clone(carried), func(s Subscription) bool { return slices.Contains(now, s) })
	out := c.resumeSend(nil, opUnsubscribe, c.scope, stale)
	for _, g := range set.groups() {
		g.streams = slices.DeleteFunc(g.streams, func(s Subscription) bool {
			return slices.Contains(carried, s) && (accountWide(s) || g.scope.label() == c.scope.label())
		})
		out = c.resumeSend(out, opSubscribe, g.scope, g.streams)
	}
	return out
}

func (c *Conn) resumeSend(sends []ws.Send, op string, scope Scope, streams []Subscription) []ws.Send {
	if len(streams) == 0 {
		return sends
	}
	id := uuid.NewString()
	c.resuming[id] = resumedChange{op: op, group: subscriptionGroup{scope: scope, streams: streams}}
	return append(sends, ws.Send{Message: op, ClientRequestID: id, Content: newSubscriptionContent(id, scope, streams)})
}

// resumedChange is a change resume sent, awaiting its answer.
type resumedChange struct {
	op    string
	group subscriptionGroup
}

// onResumed takes the answer to a change resume sent, reporting whether the frame was one. A
// refused subscribe drops the scope, so the set held is only what the connection receives.
func (c *Conn) onResumed(frame ws.Frame) (Event, bool) {
	switch frame.ResultType {
	case resultSubscribed, opSubscribe, resultUnsubscribed, opUnsubscribe:
	default:
		return nil, false
	}
	var data subscriptionData
	if json.Unmarshal(frame.Data, &data) != nil {
		return nil, false
	}
	change, ok := c.resuming[data.ClientRequestID]
	if !ok {
		return nil, false
	}
	delete(c.resuming, data.ClientRequestID)
	if change.op == opUnsubscribe || data.StatusCode < 400 {
		return nil, true
	}
	g := change.group
	c.updateSubscribed(func(set subscriptionSet) subscriptionSet { return set.unsubscribe(g.scope, g.streams) })
	lost := SubscriptionLost{
		Scope:         g.scope,
		Subscriptions: g.streams,
		Err: &rails.Error{
			Op: opSubscribe, StatusCode: data.StatusCode, Message: data.Body, ClientRequestID: data.ClientRequestID,
		},
	}
	if data.Error != nil {
		lost.Err.Slug, lost.Err.Code = data.Error.Slug, data.Error.Code
	}
	return lost, true
}

// subscriptionSet is the scopes each stream is held for, kept by the exchange's rules: no scope
// in a stream's list covers another, and an account-wide stream is held for every market. A set
// is never changed once stored; every change makes a new one.
type subscriptionSet map[Subscription][]Scope

func (set subscriptionSet) clone() subscriptionSet {
	out := make(subscriptionSet, len(set))
	for s, scopes := range set {
		out[s] = slices.Clone(scopes)
	}
	return out
}

func (set subscriptionSet) subscribe(scope Scope, streams []Subscription) subscriptionSet {
	out := set.clone()
	for _, s := range streams {
		target := trackedScope(s, scope)
		if slices.ContainsFunc(out[s], func(have Scope) bool { return have.covers(target.label()) }) {
			continue
		}
		kept := slices.DeleteFunc(out[s], func(have Scope) bool { return target.covers(have.label()) })
		out[s] = append(kept, target)
	}
	return out
}

func (set subscriptionSet) unsubscribe(scope Scope, streams []Subscription) subscriptionSet {
	out := set.clone()
	for _, s := range streams {
		target := trackedScope(s, scope)
		out[s] = slices.DeleteFunc(out[s], func(have Scope) bool { return target.covers(have.label()) })
		if len(out[s]) == 0 {
			delete(out, s)
		}
	}
	return out
}

// only drops the streams the exchange says the connection no longer receives.
func (set subscriptionSet) only(active []Subscription) subscriptionSet {
	out := set.clone()
	maps.DeleteFunc(out, func(s Subscription, _ []Scope) bool { return !slices.Contains(active, s) })
	return out
}

// covers reports whether stream is held for a scope covering target, a contract name or a
// scope's label.
func (set subscriptionSet) covers(stream Subscription, target string) bool {
	return slices.ContainsFunc(set[stream], func(have Scope) bool { return have.covers(target) })
}

// coversUnderlying reports whether stream is held for every market or any scope of underlying.
func (set subscriptionSet) coversUnderlying(stream Subscription, underlying string) bool {
	return slices.ContainsFunc(set[stream], func(have Scope) bool {
		label := have.label()
		return label == marketAll || label == underlying || strings.HasPrefix(label, underlying+"-")
	})
}

// dialed is the streams a dial URL for scope subscribes as held: those held for exactly scope,
// and the account-wide ones, which any scope subscribes for every market. The orders stream
// comes first.
func (set subscriptionSet) dialed(scope Scope) []Subscription {
	var out []Subscription
	for _, s := range slices.Sorted(maps.Keys(set)) {
		if accountWide(s) || slices.ContainsFunc(set[s], func(have Scope) bool { return have.label() == scope.label() }) {
			out = append(out, s)
		}
	}
	if i := slices.Index(out, SubscriptionOrders); i > 0 {
		out = slices.Concat([]Subscription{SubscriptionOrders}, slices.Delete(out, i, i+1))
	}
	return out
}

type subscriptionGroup struct {
	scope   Scope
	streams []Subscription
}

func (set subscriptionSet) groups() []subscriptionGroup {
	byLabel := map[string]*subscriptionGroup{}
	for s, scopes := range set {
		for _, scope := range scopes {
			g, ok := byLabel[scope.label()]
			if !ok {
				g = &subscriptionGroup{scope: scope}
				byLabel[scope.label()] = g
			}
			g.streams = append(g.streams, s)
		}
	}
	out := make([]subscriptionGroup, 0, len(byLabel))
	for _, label := range slices.Sorted(maps.Keys(byLabel)) {
		g := byLabel[label]
		slices.Sort(g.streams)
		out = append(out, *g)
	}
	return out
}

// trackedScope is the scope the exchange files a subscription under.
func trackedScope(stream Subscription, scope Scope) Scope {
	if accountWide(stream) {
		return ScopeAll()
	}
	return scope
}

func accountWide(stream Subscription) bool { return stream == SubscriptionAccountSummary }

// newSubscriptionContent names the scope in full, so a subscribe never falls back to the
// connection's own and an unsubscribe never means every market by omission.
func newSubscriptionContent(id string, scope Scope, streams []Subscription) subscriptionContent {
	return subscriptionContent{
		ClientRequestID: id,
		Subscriptions:   streams,
		Market:          scope.Market,
		Expiration:      scope.Expiration,
		Strike:          scope.Strike,
		Type:            string(scope.Type),
	}
}

type subscriptionContent struct {
	ClientRequestID string         `json:"clientRequestId"`
	Subscriptions   []Subscription `json:"subscriptions"`
	Market          string         `json:"market"`
	Expiration      string         `json:"expiration,omitempty"`
	Strike          string         `json:"strike,omitempty"`
	Type            string         `json:"type,omitempty"`
}

type subscriptionData struct {
	StatusCode               int            `json:"statusCode"`
	Body                     string         `json:"body"`
	Error                    *wsevent.Error `json:"error"`
	ClientRequestID          string         `json:"clientRequestId"`
	ActiveSubscriptions      []Subscription `json:"activeSubscriptions"`
	RequestedSubscriptions   []Subscription `json:"requestedSubscriptions"`
	RequestedUnsubscriptions []Subscription `json:"requestedUnsubscriptions"`
}
