package telemetry

import (
	"maps"
	"slices"
	"time"
)

// bucketBounds are fixed, so a report converts to a time series without interpolation. They
// ride on the report so it reads on its own.
var bucketBounds = []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

// Report is one window of client-observed measurements. It carries no prices, quantities,
// balances, order ids, account identity or addresses.
type Report struct {
	SDK         string    `json:"sdk"`
	Version     string    `json:"version"`
	Go          string    `json:"go"`
	OS          string    `json:"os"`
	Arch        string    `json:"arch"`
	Product     string    `json:"product"`
	Env         string    `json:"env"`
	WindowStart int64     `json:"windowStart"`
	WindowEnd   int64     `json:"windowEnd"`
	BucketsMs   []float64 `json:"bucketsMs"`

	Connections []Connection `json:"connections,omitempty"`
	Requests    []Request    `json:"requests,omitempty"`
	REST        []REST       `json:"rest,omitempty"`
	Samples     []Sample     `json:"samples,omitempty"`
}

// Connection is what one WebSocket connection did during the window.
type Connection struct {
	ConnID     string           `json:"connId"`
	Market     string           `json:"market"`
	Connect    Connect          `json:"connect"`
	Ping       Ping             `json:"ping"`
	Reconnects map[string]int64 `json:"reconnects,omitempty"`
	Backfill   *Backfill        `json:"backfill,omitempty"`
}

// Connect counts dials and how they ended. Failures are keyed by class, never by message.
type Connect struct {
	Attempts  int64            `json:"attempts"`
	Successes int64            `json:"successes"`
	Failures  map[string]int64 `json:"failures,omitempty"`
	DialMs    Histogram        `json:"dialMs"`
	DNSMs     Histogram        `json:"dnsMs"`
	TCPMs     Histogram        `json:"tcpMs"`
	TLSMs     Histogram        `json:"tlsMs"`
	UpgradeMs Histogram        `json:"upgradeMs"`
}

// Ping holds the round trip of the application-level keepalive.
type Ping struct {
	RTTMs Histogram `json:"rttMs"`
}

// Backfill is the last session recovery on the connection. Resumed false, or any expired
// market, means the client had to rebuild its order state.
type Backfill struct {
	Resumed        bool     `json:"resumed"`
	Events         int      `json:"events"`
	ExpiredMarkets []string `json:"expiredMarkets,omitempty"`
}

// Request is one WebSocket command over the window. Status keys are the code and, where the
// exchange named one, its slug.
type Request struct {
	Op         string           `json:"op"`
	AckMs      Histogram        `json:"ackMs"`
	TerminalMs Histogram        `json:"terminalMs"`
	Status     map[string]int64 `json:"status,omitempty"`
	Timeouts   int64            `json:"timeouts"`
}

// REST is one endpoint, named by method and path — never by host.
type REST struct {
	Endpoint string           `json:"endpoint"`
	Ms       Histogram        `json:"ms"`
	Status   map[string]int64 `json:"status,omitempty"`
}

// Sample is one request that was slow or failed. The clientRequestId is the exchange's own,
// so the same request can be found in its logs.
type Sample struct {
	Op              string  `json:"op"`
	ClientRequestID string  `json:"clientRequestId"`
	Market          string  `json:"market"`
	AckMs           float64 `json:"ackMs,omitempty"`
	TerminalMs      float64 `json:"terminalMs"`
	StatusCode      int     `json:"statusCode,omitempty"`
	Slug            string  `json:"slug,omitempty"`
}

// Histogram counts observations into [bucketBounds], cumulatively. Anything above the last
// bound is counted but in no bucket.
type Histogram struct {
	Count   int64   `json:"count"`
	SumMs   float64 `json:"sumMs"`
	Buckets []int64 `json:"buckets,omitempty"`
}

func (h *Histogram) observe(d time.Duration) {
	ms := float64(d) / float64(time.Millisecond)
	h.Count++
	h.SumMs += ms
	if h.Buckets == nil {
		h.Buckets = make([]int64, len(bucketBounds))
	}
	for i, bound := range bucketBounds {
		if ms <= bound {
			h.Buckets[i]++
		}
	}
}

// counted keeps map counters out of every caller's nil check.
func counted(m map[string]int64, key string) map[string]int64 {
	if m == nil {
		m = map[string]int64{}
	}
	m[key]++
	return m
}

// sortedKeys makes a report's arrays stable, so two windows of the same traffic diff cleanly.
func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}
