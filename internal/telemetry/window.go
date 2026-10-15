package telemetry

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"runtime"
	"strconv"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// window accumulates one reporting period. Only the aggregator goroutine touches it.
type window struct {
	start       time.Time
	connections map[string]*Connection
	requests    map[string]*Request
	rest        map[string]*REST
	samples     []scored
	// failures counts failed dials in a row, which is what sends a window early.
	failures int
}

func newWindow(now time.Time) *window {
	return &window{
		start:       now,
		connections: map[string]*Connection{},
		requests:    map[string]*Request{},
		rest:        map[string]*REST{},
	}
}

func (w *window) empty() bool {
	return len(w.connections) == 0 && len(w.requests) == 0 && len(w.rest) == 0
}

// conn is nil once a window holds as many connections as a report may carry.
func (w *window) conn(info rails.ConnInfo) *Connection {
	state, ok := w.connections[info.ConnID]
	if ok {
		return state
	}
	if len(w.connections) >= maxConnections {
		return nil
	}
	state = &Connection{ConnID: info.ConnID, Market: info.Market}
	w.connections[info.ConnID] = state
	return state
}

func (w *window) request(op string) *Request {
	req, ok := w.requests[op]
	if !ok {
		req = &Request{Op: op}
		w.requests[op] = req
	}
	return req
}

func (w *window) endpoint(name string) *REST {
	entry, ok := w.rest[name]
	if !ok {
		entry = &REST{Endpoint: name}
		w.rest[name] = entry
	}
	return entry
}

// report renders the window, in a stable order so two of them differ only in their numbers.
func (w *window) report(product, env string, end time.Time) *Report {
	report := &Report{
		SDK:         sdkName,
		Version:     rails.Version(),
		Go:          runtime.Version(),
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		Product:     product,
		Env:         env,
		WindowStart: w.start.UnixMilli(),
		WindowEnd:   end.UnixMilli(),
		BucketsMs:   bucketBounds,
	}
	for _, entry := range w.samples {
		report.Samples = append(report.Samples, entry.Sample)
	}
	for _, id := range sortedKeys(w.connections) {
		report.Connections = append(report.Connections, *w.connections[id])
	}
	for _, op := range sortedKeys(w.requests) {
		report.Requests = append(report.Requests, *w.requests[op])
	}
	for _, name := range sortedKeys(w.rest) {
		report.REST = append(report.REST, *w.rest[name])
	}
	return report
}

// scored is a sample and how interesting it is, so the cap keeps the right ones.
type scored struct {
	Sample
	rank float64
}

// sample keeps everything that failed and the slowest of what did not, up to a cap.
func (w *window) sample(candidate Sample, failed bool, limit int) {
	entry := scored{Sample: candidate, rank: candidate.TerminalMs}
	if failed {
		entry.rank += failedRank
	}
	if len(w.samples) < limit {
		w.samples = append(w.samples, entry)
		return
	}
	weakest := 0
	for i := range w.samples {
		if w.samples[i].rank < w.samples[weakest].rank {
			weakest = i
		}
	}
	if entry.rank > w.samples[weakest].rank {
		w.samples[weakest] = entry
	}
}

// failedRank puts every failure above every merely slow request, whatever the durations are.
const failedRank = 1 << 40

// statusKey labels a result by its code and, when the exchange named one, its slug.
func statusKey(code int, slug string) string {
	key := strconv.Itoa(code)
	if slug != "" {
		key += "_" + slug
	}
	return key
}

// failureClass names why a dial failed, in a handful of values.
func failureClass(err error) string {
	if errors.Is(err, rails.ErrSoftBanned) {
		return "softBan"
	}
	var apiErr *rails.Error
	if errors.As(err, &apiErr) && apiErr.StatusCode != 0 {
		return strconv.Itoa(apiErr.StatusCode)
	}
	var dnsErr *net.DNSError
	var certErr *tls.CertificateVerificationError
	var recordErr tls.RecordHeaderError
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "timeout"
	case errors.As(err, &dnsErr):
		return "dns"
	case errors.As(err, &certErr), errors.As(err, &recordErr):
		return "tls"
	case errors.Is(err, net.ErrClosed):
		return "closed"
	}
	return "other"
}

// environmentName labels the preset a client is pointed at, so a private deployment does not
// land with production.
func environmentName(env rails.Environment) string {
	switch env {
	case rails.Production():
		return "production"
	case rails.Sandbox():
		return "sandbox"
	}
	return "custom"
}
