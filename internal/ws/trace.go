package ws

import (
	"context"
	"crypto/tls"
	"net/http/httptrace"
	"sync"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// dialTrace times the phases of one upgrade. Its callbacks run on the HTTP client's own
// goroutines, so it guards itself.
type dialTrace struct {
	mu    sync.Mutex
	start time.Time

	dnsStart, tcpStart, tlsStart, wrote time.Time
	stats                               rails.ConnectStats
}

// trace returns a context that records the dial's phases into t.
func (t *dialTrace) trace(ctx context.Context) context.Context {
	t.start = time.Now()
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) { t.mark(&t.dnsStart) },
		DNSDone:  func(httptrace.DNSDoneInfo) { t.since(&t.dnsStart, &t.stats.DNS) },
		ConnectStart: func(string, string) {
			t.mu.Lock()
			defer t.mu.Unlock()
			if t.tcpStart.IsZero() {
				t.tcpStart = time.Now()
			}
		},
		ConnectDone:          func(string, string, error) { t.since(&t.tcpStart, &t.stats.TCP) },
		TLSHandshakeStart:    func() { t.mark(&t.tlsStart) },
		TLSHandshakeDone:     func(tls.ConnectionState, error) { t.since(&t.tlsStart, &t.stats.TLS) },
		WroteRequest:         func(httptrace.WroteRequestInfo) { t.mark(&t.wrote) },
		GotFirstResponseByte: func() { t.since(&t.wrote, &t.stats.Upgrade) },
	})
}

// result is the timing of the dial that has finished.
func (t *dialTrace) result() rails.ConnectStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := t.stats
	stats.Total = time.Since(t.start)
	return stats
}

func (t *dialTrace) mark(at *time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	*at = time.Now()
}

// since fills a phase, leaving one that never started at zero rather than the whole dial. The
// start is taken by address because these callbacks can run at the same time.
func (t *dialTrace) since(start *time.Time, into *time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !start.IsZero() {
		*into = time.Since(*start)
	}
}
