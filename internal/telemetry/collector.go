package telemetry

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// sdkName matches the name in [rails.UserAgent], so a report and a request can be joined.
const sdkName = "exchange-go-sdk"

// Defaults for a reporting period.
const (
	defaultWindow     = 60 * time.Second
	defaultSlow       = 2 * time.Second
	defaultMaxSamples = 20
	defaultQueue      = 4
	// burstFlush sends the window early after this many failed dials in a row, so an outage
	// is visible while it is happening.
	burstFlush = 3
	// eventBuffer absorbs what a busy client observes between aggregator turns.
	eventBuffer = 1024
	// maxTracked bounds acknowledgements waiting for their answer; only a dropped
	// observation leaves one behind.
	maxTracked = 4096
	// maxConnections and maxExpiredMarkets match what the endpoint accepts: a report past
	// them is refused whole, and most of a window beats none of it.
	maxConnections    = 64
	maxExpiredMarkets = 32
)

type settings struct {
	window     time.Duration
	slow       time.Duration
	maxSamples int
	queue      int
}

func defaults() settings {
	return settings{
		window:     defaultWindow,
		slow:       defaultSlow,
		maxSamples: defaultMaxSamples,
		queue:      defaultQueue,
	}
}

// Collector produces the [rails.Hooks] a product client installs: every observation reaches
// the caller's own funcs, and a bounded reduction of it reaches Rails.
type Collector struct {
	user     rails.Hooks
	product  string
	env      string
	set      settings
	events   chan event
	reports  chan queued
	done     chan struct{}
	finished chan struct{}
	cancel   context.CancelFunc

	closeOnce sync.Once
}

// New wraps the caller's hooks. With reporting off, the collector only forwards to them.
// With reporting on it needs somewhere to report, so a custom Environment cannot fall silent
// by leaving the host out: the caller either names it or switches reporting off.
func New(cfg *rails.Config, tokens rails.TokenSource, product string) (*Collector, error) {
	return newWith(cfg, tokens, product, defaults())
}

func newWith(cfg *rails.Config, tokens rails.TokenSource, product string, set settings) (*Collector, error) {
	if cfg.Telemetry && cfg.Environment.TelemetryURL == "" {
		return nil, errors.New("the environment needs TelemetryURL, or telemetry disabled with rails.WithTelemetry(false)")
	}
	// Off, the collector is only the caller's hooks: no channels, so record drops and Close
	// returns at once.
	if !cfg.Telemetry {
		return &Collector{user: filled(cfg.Hooks)}, nil
	}
	c := &Collector{
		user:     filled(cfg.Hooks),
		product:  product,
		env:      environmentName(cfg.Environment),
		set:      set,
		finished: make(chan struct{}),
		events:   make(chan event, eventBuffer),
		reports:  make(chan queued, set.queue),
		done:     make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	go c.aggregate()
	go newReporter(cfg, tokens).run(ctx, c.reports, c.finished)
	return c, nil
}

// Hooks is what a product client installs in place of the caller's own.
func (c *Collector) Hooks() rails.Hooks {
	return rails.Hooks{
		ConnectStarted:  c.ConnectStarted,
		ConnectFinished: c.ConnectFinished,
		Disconnected:    c.Disconnected,
		PingRTT:         c.PingRTT,
		RequestAcked:    c.RequestAcked,
		RequestDone:     c.RequestDone,
		HTTPDone:        c.HTTPDone,
		Paced:           c.user.Paced,
		Throttled:       c.user.Throttled,
	}
}

// filled gives every func the caller left nil a no-op, so the hooks below call them outright.
func filled(h rails.Hooks) rails.Hooks {
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
	if h.HTTPDone == nil {
		h.HTTPDone = func(rails.HTTPInfo, time.Duration, error) {}
	}
	if h.Paced == nil {
		h.Paced = func(rails.PaceInfo) {}
	}
	if h.Throttled == nil {
		h.Throttled = func(rails.OpClass, time.Duration) {}
	}
	return h
}

// Close sends the current window and stops reporting. A send already under way is abandoned,
// so closing costs one attempt.
func (c *Collector) Close() {
	if c.done == nil {
		return
	}
	c.closeOnce.Do(func() {
		close(c.done)
		c.cancel()
	})
	<-c.finished
}

// record hands an observation to the aggregator, or drops it: a caller placing an order never
// waits on telemetry.
func (c *Collector) record(e event) {
	if c.events == nil {
		return
	}
	select {
	case c.events <- e:
	default:
	}
}

// aggregate owns the window and the requests waiting on an answer. Everything it touches is
// reached only through the channels this loop selects on.
func (c *Collector) aggregate() {
	defer close(c.reports)

	ticker := time.NewTicker(c.set.window)
	defer ticker.Stop()
	current := newWindow(time.Now())
	// acks outlive a window: an ack in one and its answer in the next are one request.
	acks := map[string]time.Duration{}

	roll := func(final bool) {
		now := time.Now()
		if !current.empty() {
			c.enqueue(queued{report: current.report(c.product, c.env, now), final: final})
		}
		current = newWindow(now)
	}

	for {
		select {
		case <-c.done:
			c.drain(current, acks)
			// Closing is bounded by one send, so anything still queued is already
			// stale: the last window replaces it.
			c.discardQueued()
			roll(true)
			return
		case e := <-c.events:
			e.apply(current, acks, c.set)
			if current.failures >= burstFlush {
				roll(false)
			}
		case <-ticker.C:
			roll(false)
		}
	}
}

// drain folds in what was observed just before closing. A select has no preference among its
// ready cases, so without this the last observations would be a coin toss.
func (c *Collector) drain(current *window, acks map[string]time.Duration) {
	for {
		select {
		case e := <-c.events:
			e.apply(current, acks, c.set)
		default:
			return
		}
	}
}

func (c *Collector) discardQueued() {
	for {
		select {
		case <-c.reports:
		default:
			return
		}
	}
}

// enqueue drops the oldest report when the reporter is behind: a client that cannot report
// keeps trading, and keeps the newest picture of itself.
func (c *Collector) enqueue(report queued) {
	select {
	case c.reports <- report:
		return
	default:
	}
	select {
	case <-c.reports:
	default:
	}
	select {
	case c.reports <- report:
	default:
	}
}

// The hooks. Each reduces the observation to what a report may carry and calls the caller's
// own func, on the caller's goroutine.

func (c *Collector) ConnectStarted(info rails.ConnInfo) {
	c.user.ConnectStarted(info)
	c.record(connectStarted{info})
}

func (c *Collector) ConnectFinished(info rails.ConnInfo, stats rails.ConnectStats, err error) {
	e := connectFinished{info: info, stats: stats, failed: err != nil}
	if e.failed {
		e.class = failureClass(err)
	}
	c.user.ConnectFinished(info, stats, err)
	c.record(e)
}

func (c *Collector) Disconnected(info rails.ConnInfo, reason rails.DisconnectReason) {
	c.user.Disconnected(info, reason)
	c.record(disconnected{info: info, reason: reason})
}

func (c *Collector) PingRTT(info rails.ConnInfo, rtt time.Duration) {
	c.user.PingRTT(info, rtt)
	c.record(pingRTT{info: info, rtt: rtt})
}

func (c *Collector) RequestAcked(info rails.ReqInfo, d time.Duration) {
	c.user.RequestAcked(info, d)
	c.record(requestAcked{info: info, took: d})
}

func (c *Collector) RequestDone(info rails.ReqInfo, d time.Duration, err error) {
	e := requestDone{info: info, took: d, code: 200, failed: err != nil}
	var apiErr *rails.Error
	if errors.As(err, &apiErr) {
		e.code, e.slug = apiErr.StatusCode, safeSlug(apiErr.Slug)
	}
	e.timedOut = errors.Is(err, rails.ErrTimeout)

	c.user.RequestDone(info, d, err)
	c.record(e)
}

func (c *Collector) HTTPDone(info rails.HTTPInfo, d time.Duration, err error) {
	e := httpDone{info: info, took: d}
	if info.StatusCode == 0 && err != nil {
		e.class = failureClass(err)
	}
	c.user.HTTPDone(info, d, err)
	c.record(e)
}

// Session records what a reconnect recovered, copying as much of the caller's slice as a
// report may carry.
func (c *Collector) Session(connID, market string, backfill Backfill) {
	expired := backfill.ExpiredMarkets
	backfill.ExpiredMarkets = append([]string(nil), expired[:min(len(expired), maxExpiredMarkets)]...)
	c.record(sessionBackfill{connID: connID, market: market, backfill: backfill})
}

// event is one observation, and how it lands in a window. Only bounded values reach here —
// nothing a server or a caller can make arbitrary.
type event interface {
	apply(w *window, acks map[string]time.Duration, set settings)
}

type connectStarted struct{ info rails.ConnInfo }

func (e connectStarted) apply(w *window, _ map[string]time.Duration, _ settings) {
	if conn := w.conn(e.info); conn != nil {
		conn.Connect.Attempts++
	}
}

type connectFinished struct {
	info   rails.ConnInfo
	stats  rails.ConnectStats
	failed bool
	class  string
}

func (e connectFinished) apply(w *window, _ map[string]time.Duration, _ settings) {
	conn := w.conn(e.info)
	if conn == nil {
		return
	}
	connect := &conn.Connect
	// Timed either way: how long a failing dial takes is the shape of an outage.
	connect.DialMs.observe(e.stats.Total)
	if e.failed {
		connect.Failures = counted(connect.Failures, e.class)
		w.failures++
		return
	}
	w.failures = 0
	connect.Successes++
	// A phase that did not happen is not a fast one: a pooled connection skips DNS, TCP and TLS.
	observeIf(&connect.DNSMs, e.stats.DNS)
	observeIf(&connect.TCPMs, e.stats.TCP)
	observeIf(&connect.TLSMs, e.stats.TLS)
	observeIf(&connect.UpgradeMs, e.stats.Upgrade)
}

type disconnected struct {
	info   rails.ConnInfo
	reason rails.DisconnectReason
}

func (e disconnected) apply(w *window, _ map[string]time.Duration, _ settings) {
	if conn := w.conn(e.info); conn != nil {
		conn.Reconnects = counted(conn.Reconnects, string(e.reason))
	}
}

type pingRTT struct {
	info rails.ConnInfo
	rtt  time.Duration
}

func (e pingRTT) apply(w *window, _ map[string]time.Duration, _ settings) {
	if conn := w.conn(e.info); conn != nil {
		conn.Ping.RTTMs.observe(e.rtt)
	}
}

type requestAcked struct {
	info rails.ReqInfo
	took time.Duration
}

func (e requestAcked) apply(w *window, acks map[string]time.Duration, _ settings) {
	w.request(e.info.Op).AckMs.observe(e.took)
	if len(acks) >= maxTracked {
		clear(acks)
	}
	acks[trackingKey(e.info)] = e.took
}

type requestDone struct {
	info     rails.ReqInfo
	took     time.Duration
	code     int
	slug     string
	failed   bool
	timedOut bool
}

func (e requestDone) apply(w *window, acks map[string]time.Duration, set settings) {
	request := w.request(e.info.Op)
	request.TerminalMs.observe(e.took)
	request.Status = counted(request.Status, statusKey(e.code, e.slug))
	if e.timedOut {
		request.Timeouts++
	}

	key := trackingKey(e.info)
	ack := acks[key]
	delete(acks, key)
	if !e.failed && e.took < set.slow {
		return
	}
	w.sample(Sample{
		Op:              e.info.Op,
		ClientRequestID: sampledID(e.info.ClientRequestID),
		Market:          e.info.Market,
		AckMs:           millis(ack),
		TerminalMs:      millis(e.took),
		StatusCode:      e.code,
		Slug:            e.slug,
	}, e.failed, set.maxSamples)
}

type httpDone struct {
	info  rails.HTTPInfo
	took  time.Duration
	class string
}

func (e httpDone) apply(w *window, _ map[string]time.Duration, _ settings) {
	endpoint := w.endpoint(e.info.Op)
	endpoint.Ms.observe(e.took)
	if e.class != "" {
		// No response at all, which has to be distinguishable from a 500.
		endpoint.Status = counted(endpoint.Status, e.class)
		return
	}
	endpoint.Status = counted(endpoint.Status, statusKey(e.info.StatusCode, ""))
}

type sessionBackfill struct {
	connID, market string
	backfill       Backfill
}

func (e sessionBackfill) apply(w *window, _ map[string]time.Duration, _ settings) {
	conn := w.conn(rails.ConnInfo{ConnID: e.connID, Market: e.market})
	if conn == nil {
		return
	}
	backfill := e.backfill
	conn.Backfill = &backfill
}

// trackingKey pairs a request with its connection: a caller minting its own ids can reuse one
// across two of them.
func trackingKey(info rails.ReqInfo) string {
	return info.ConnID + "\x00" + info.ClientRequestID
}

// safeSlug keeps the machine-readable code and drops anything else: the value comes off the
// wire, and a report carries no free text.
func safeSlug(slug string) string {
	if slug == "" || len(slug) > 40 {
		return ""
	}
	for _, r := range slug {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return ""
		}
	}
	return slug
}

// sampledID keeps a correlation id only in the canonical form the exchange issues. Anything
// else was rejected anyway, so it correlates with nothing.
func sampledID(id string) string {
	if len(id) == 36 && uuid.Validate(id) == nil {
		return id
	}
	return ""
}

func observeIf(h *Histogram, d time.Duration) {
	if d > 0 {
		h.observe(d)
	}
}

func millis(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
