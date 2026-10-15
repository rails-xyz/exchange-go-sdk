package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const settle = 2 * time.Second

type staticToken string

func (s staticToken) Token(context.Context) (string, error) { return string(s), nil }

// ingest stands in for the Rails endpoint.
type ingest struct {
	server  *httptest.Server
	reports chan *Report
	auth    chan string
	calls   atomic.Int64
}

func newIngest(t *testing.T, respond func(w http.ResponseWriter) bool) *ingest {
	t.Helper()
	in := &ingest{reports: make(chan *Report, 8), auth: make(chan string, 8)}
	in.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		in.calls.Add(1)
		if respond != nil && respond(w) {
			return
		}
		// assert, not require: this runs on the server's goroutine, where FailNow cannot.
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var report Report
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&report)) {
			return
		}

		select {
		case in.auth <- r.Header.Get("Authorization"):
		default:
		}
		select {
		case in.reports <- &report:
		default:
		}
	}))
	t.Cleanup(in.server.Close)
	return in
}

func (i *ingest) next(t *testing.T) *Report {
	t.Helper()
	select {
	case report := <-i.reports:
		return report
	case <-time.After(settle):
		t.Fatal("no report reached the ingest")
		return nil
	}
}

// collect builds a collector pointed at an ingest, with a window a test drives.
func collect(t *testing.T, url string, tune func(*settings), opts ...rails.Option) *Collector {
	t.Helper()
	set := defaults()
	set.window = time.Hour // rolled by Close, not by the clock, unless a test says otherwise
	if tune != nil {
		tune(&set)
	}
	cfg := rails.NewConfig(append([]rails.Option{
		rails.WithEnvironment(rails.Environment{TelemetryURL: url}),
	}, opts...)...)

	c, err := newWith(cfg, staticToken("jwt-token"), "options", set)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

// One window end to end: what a client measured, and what the ingest receives.
func TestReportDescribesTheWindow(t *testing.T) {
	in := newIngest(t, nil)
	c := collect(t, in.server.URL, nil)

	conn := rails.ConnInfo{ConnID: "conn-1", Market: "BTC-USDT"}
	c.ConnectStarted(conn)
	c.ConnectFinished(conn, rails.ConnectStats{DNS: 3 * time.Millisecond, TCP: 8 * time.Millisecond,
		TLS: 20 * time.Millisecond, Upgrade: 30 * time.Millisecond, Total: 64 * time.Millisecond}, nil)
	c.PingRTT(conn, 12*time.Millisecond)
	c.Session("conn-1", "BTC-USDT", Backfill{Resumed: true, Events: 4, ExpiredMarkets: []string{"ETH-USDT"}})
	c.Disconnected(conn, rails.ReasonServerShutdown)

	req := rails.ReqInfo{ConnID: "conn-1", Op: "createOrder", Market: "BTC-USDT",
		ClientRequestID: "2f6635e5-bdab-4fbc-8e2e-21252d2219dc"}
	c.RequestAcked(req, 40*time.Millisecond)
	c.RequestDone(req, 120*time.Millisecond, nil)
	c.RequestDone(rails.ReqInfo{ConnID: "conn-1", Op: "cancelOrder", Market: "BTC-USDT",
		ClientRequestID: "9d3a1a1e-1f4c-4a2f-9a5e-0c2f5f2f1a11"}, 3*time.Second,
		(&rails.Error{StatusCode: 429, Slug: "TOO_MANY_REQUESTS", Code: "SYS-0003"}))
	c.HTTPDone(rails.HTTPInfo{Op: "GET /api/v1/order", StatusCode: 200}, 55*time.Millisecond, nil)

	c.Close()
	report := in.next(t)

	body, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)
	t.Logf("report:\n%s", body)

	assert.Equal(t, "Bearer jwt-token", <-in.auth, "the ingest identifies the sender from the token")
	assert.True(t, strings.HasPrefix(rails.UserAgent(), report.SDK+"/"),
		"the report and the User-Agent must name the SDK the same way")
	assert.Equal(t, "options", report.Product)
	assert.Equal(t, "custom", report.Env)
	assert.Equal(t, bucketBounds, report.BucketsMs)
	assert.Positive(t, report.WindowEnd)
	assert.GreaterOrEqual(t, report.WindowEnd, report.WindowStart)

	require.Len(t, report.Connections, 1)
	got := report.Connections[0]
	assert.Equal(t, "conn-1", got.ConnID)
	assert.EqualValues(t, 1, got.Connect.Attempts)
	assert.EqualValues(t, 1, got.Connect.Successes)
	assert.Empty(t, got.Connect.Failures)
	assert.EqualValues(t, 64, got.Connect.DialMs.SumMs)
	assert.EqualValues(t, 20, got.Connect.TLSMs.SumMs)
	assert.EqualValues(t, 12, got.Ping.RTTMs.SumMs)
	assert.Equal(t, map[string]int64{"serverShutdown": 1}, got.Reconnects)
	require.NotNil(t, got.Backfill)
	assert.Equal(t, Backfill{Resumed: true, Events: 4, ExpiredMarkets: []string{"ETH-USDT"}}, *got.Backfill)

	require.Len(t, report.Requests, 2)
	assert.Equal(t, "cancelOrder", report.Requests[0].Op, "requests come out in a stable order")
	assert.Equal(t, map[string]int64{"429_TOO_MANY_REQUESTS": 1}, report.Requests[0].Status)
	assert.Equal(t, "createOrder", report.Requests[1].Op)
	assert.EqualValues(t, 40, report.Requests[1].AckMs.SumMs)
	assert.Equal(t, map[string]int64{"200": 1}, report.Requests[1].Status)

	require.Len(t, report.REST, 1)
	assert.Equal(t, "GET /api/v1/order", report.REST[0].Endpoint)
	assert.Equal(t, map[string]int64{"200": 1}, report.REST[0].Status)

	// The fast, successful order is not worth a sample; the rate-limited one is.
	require.Len(t, report.Samples, 1)
	assert.Equal(t, "cancelOrder", report.Samples[0].Op)
	assert.Equal(t, 429, report.Samples[0].StatusCode)
	assert.Equal(t, "TOO_MANY_REQUESTS", report.Samples[0].Slug)
}

// A report is a window, not a running total: the numbers reset once it is sent.
func TestWindowRollsOnItsInterval(t *testing.T) {
	in := newIngest(t, nil)
	c := collect(t, in.server.URL, func(s *settings) { s.window = 50 * time.Millisecond })

	c.PingRTT(rails.ConnInfo{ConnID: "conn-1"}, 5*time.Millisecond)
	assert.EqualValues(t, 1, in.next(t).Connections[0].Ping.RTTMs.Count)

	c.PingRTT(rails.ConnInfo{ConnID: "conn-1"}, 5*time.Millisecond)
	c.PingRTT(rails.ConnInfo{ConnID: "conn-1"}, 5*time.Millisecond)
	assert.EqualValues(t, 2, in.next(t).Connections[0].Ping.RTTMs.Count,
		"the second window still carried the first window's pings")
}

// Samples are read by a person, so a bad window cannot become an unbounded upload.
func TestSamplesAreCappedAndKeepTheWorst(t *testing.T) {
	in := newIngest(t, nil)
	c := collect(t, in.server.URL, func(s *settings) { s.maxSamples = 3 })

	for i := range 10 {
		c.RequestDone(rails.ReqInfo{Op: "createOrder", ClientRequestID: string(rune('a' + i))},
			time.Duration(i+1)*time.Second, nil)
	}
	c.Close()

	report := in.next(t)
	require.Len(t, report.Samples, 3)
	kept := []float64{}
	for _, s := range report.Samples {
		kept = append(kept, s.TerminalMs)
	}
	assert.ElementsMatch(t, []float64{8000, 9000, 10000}, kept, "the slowest requests were not the ones kept")
}

// A failure is always more interesting than a slow success, however slow.
func TestAFailureOutranksASlowerSuccess(t *testing.T) {
	in := newIngest(t, nil)
	c := collect(t, in.server.URL, func(s *settings) { s.maxSamples = 1 })

	const failed = "9d3a1a1e-1f4c-4a2f-9a5e-0c2f5f2f1a11"
	c.RequestDone(rails.ReqInfo{Op: "createOrder", ClientRequestID: uuid.NewString()}, time.Minute, nil)
	c.RequestDone(rails.ReqInfo{Op: "createOrder", ClientRequestID: failed}, time.Millisecond,
		&rails.Error{StatusCode: 400, Slug: "CREATE_ORDER_ERROR"})
	c.Close()

	report := in.next(t)
	require.Len(t, report.Samples, 1)
	assert.Equal(t, failed, report.Samples[0].ClientRequestID)
}

// A report carries no text a caller or a server chose: one leaks, the other adds a metric
// series per request.
func TestAReportCarriesNoFreeText(t *testing.T) {
	in := newIngest(t, nil)
	c := collect(t, in.server.URL, nil)

	c.RequestDone(rails.ReqInfo{Op: "createOrder", ClientRequestID: "account-12345 for alice@example.com"},
		time.Minute, &rails.Error{StatusCode: 400, Slug: "insufficient balance of 12.5 BTC"})
	// A real UUID with its hyphens removed: valid to a parser, issued by nobody.
	c.RequestDone(rails.ReqInfo{Op: "cancelOrder", ClientRequestID: "2f6635e5bdab4fbc8e2e21252d2219dc"},
		time.Minute, nil)
	c.Close()

	report := in.next(t)
	require.Len(t, report.Samples, 2)
	for _, sample := range report.Samples {
		assert.Empty(t, sample.ClientRequestID, "an id the exchange could not have issued was reported")
	}
	assert.Empty(t, report.Samples[0].Slug, "free text arrived in a sample")
	assert.Equal(t, map[string]int64{"400": 1}, report.Requests[1].Status,
		"free text became a metric label")
}

// A queue backed up behind an unreachable ingest keeps the freshest report, not the stalest.
func TestQueueDropsTheOldestReport(t *testing.T) {
	c := &Collector{reports: make(chan queued, 1)}

	c.enqueue(queued{report: &Report{WindowEnd: 1}})
	c.enqueue(queued{report: &Report{WindowEnd: 2}})

	assert.EqualValues(t, 2, (<-c.reports).report.WindowEnd, "the newest window was the one dropped")
}

// A refused report is a whole window lost, so a window fills only to what one may carry.
func TestAWindowStopsAtWhatAReportMayCarry(t *testing.T) {
	in := newIngest(t, nil)
	c := collect(t, in.server.URL, nil)

	for i := range maxConnections + 10 {
		c.PingRTT(rails.ConnInfo{ConnID: fmt.Sprintf("conn-%d", i)}, time.Millisecond)
	}
	c.Session("conn-0", "BTC-USDT", Backfill{
		ExpiredMarkets: make([]string, maxExpiredMarkets+10),
	})
	c.Close()

	report := in.next(t)
	assert.Len(t, report.Connections, maxConnections)
	for _, conn := range report.Connections {
		if conn.Backfill != nil {
			assert.Len(t, conn.Backfill.ExpiredMarkets, maxExpiredMarkets)
		}
	}
}

// Whoever runs the process decides what leaves it.
func TestReportingStaysOffWhenItIsNotWanted(t *testing.T) {
	cases := []struct {
		name string
		env  string
		opts []rails.Option
	}{
		{name: "switched off in code", opts: []rails.Option{rails.WithTelemetry(false)}},
		{name: "switched off in the environment", env: "off"},
		{name: "switched off in the environment, shouted", env: "OFF"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := newIngest(t, nil)
			t.Setenv("RAILS_SDK_TELEMETRY", tc.env)

			c := collect(t, in.server.URL, nil, tc.opts...)
			c.PingRTT(rails.ConnInfo{ConnID: "conn-1"}, time.Millisecond)
			c.Close()

			assert.Zero(t, in.calls.Load(), "a report was sent by a client that opted out")
		})
	}
}

// Reporting on with nowhere to report is a misconfiguration, not silence.
func TestAnEmptyTelemetryHostIsRefusedUnlessReportingIsOff(t *testing.T) {
	t.Setenv("RAILS_SDK_TELEMETRY", "")

	_, err := New(rails.NewConfig(rails.WithEnvironment(rails.Environment{})), staticToken("jwt-token"), "perps")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TelemetryURL")

	c, err := New(rails.NewConfig(rails.WithEnvironment(rails.Environment{}), rails.WithTelemetry(false)),
		staticToken("jwt-token"), "perps")
	require.NoError(t, err)
	c.Close()
}

// The caller's hooks are the point of the collector: it must never swallow one.
func TestTheCallersHooksStillSeeEverything(t *testing.T) {
	seen := &countingHooks{}
	c := collect(t, "", nil, rails.WithHooks(seen.hooks()), rails.WithTelemetry(false))

	conn := rails.ConnInfo{ConnID: "conn-1"}
	c.ConnectStarted(conn)
	c.ConnectFinished(conn, rails.ConnectStats{}, nil)
	c.Disconnected(conn, rails.ReasonReadError)
	c.PingRTT(conn, time.Millisecond)
	c.RequestAcked(rails.ReqInfo{Op: "createOrder"}, time.Millisecond)
	c.RequestDone(rails.ReqInfo{Op: "createOrder"}, time.Millisecond, nil)
	c.HTTPDone(rails.HTTPInfo{Op: "GET /x"}, time.Millisecond, nil)

	assert.EqualValues(t, 7, seen.calls.Load(), "the collector dropped one of the caller's hooks")
}

// The hook runs on the goroutine placing the order, so an ingest that never answers must
// cost it nothing.
func TestAHungIngestDoesNotDelayTheCaller(t *testing.T) {
	hold, hung := make(chan struct{}), make(chan struct{}, 1)
	// Released before any cleanup runs, so the server is not left waiting on its own handler.
	defer close(hold)
	in := newIngest(t, func(http.ResponseWriter) bool {
		select {
		case hung <- struct{}{}:
		default:
		}
		<-hold
		return true
	})
	// A queue of one, so the aggregator has nowhere to put the next report either.
	c := collect(t, in.server.URL, func(s *settings) { s.window = 5 * time.Millisecond; s.queue = 1 })

	// An empty window is never sent, so there would be nothing hung to be delayed by.
	c.PingRTT(rails.ConnInfo{ConnID: "conn-1"}, time.Millisecond)
	select {
	case <-hung:
	case <-time.After(settle):
		t.Fatal("no report reached the ingest, so nothing was hung")
	}

	// Long enough to fill the queue and then the event buffer behind it.
	observed := make(chan time.Duration, 1)
	go func() {
		worst := time.Duration(0)
		for deadline := time.Now().Add(200 * time.Millisecond); time.Now().Before(deadline); {
			start := time.Now()
			c.RequestDone(rails.ReqInfo{ConnID: "conn-1", Op: "createOrder"}, time.Millisecond, nil)
			worst = max(worst, time.Since(start))
			time.Sleep(50 * time.Microsecond)
		}
		observed <- worst
	}()

	select {
	case worst := <-observed:
		assert.Less(t, worst, 50*time.Millisecond, "observing a request kept pace with the ingest")
	case <-time.After(settle):
		t.Fatal("observing a request waited on the hung ingest")
	}
}

// A dead ingest must not fail a client: reporting is best effort by construction.
func TestADeadIngestIsNotAnError(t *testing.T) {
	in := newIngest(t, func(w http.ResponseWriter) bool {
		w.WriteHeader(http.StatusInternalServerError)
		return true
	})
	c := collect(t, in.server.URL, func(s *settings) { s.window = time.Hour })

	c.PingRTT(rails.ConnInfo{ConnID: "conn-1"}, time.Millisecond)
	c.Close()
	assert.EqualValues(t, 1, in.calls.Load(), "the last report is sent once, never retried")
}

func TestFailureClasses(t *testing.T) {
	cases := map[string]error{
		"softBan": fmt.Errorf("%w: %w", rails.ErrSoftBanned, &rails.Error{StatusCode: 403}),
		"403":     &rails.Error{StatusCode: 403},
		"429":     &rails.Error{StatusCode: 429},
		"timeout": context.DeadlineExceeded,
		"other":   errors.New("something else"),
	}
	for want, err := range cases {
		assert.Equal(t, want, failureClass(err), "%v", err)
	}
}

// countingHooks counts every call the collector forwards.
type countingHooks struct{ calls atomic.Int64 }

func (h *countingHooks) hooks() rails.Hooks {
	count := func() { h.calls.Add(1) }
	return rails.Hooks{
		ConnectStarted:  func(rails.ConnInfo) { count() },
		ConnectFinished: func(rails.ConnInfo, rails.ConnectStats, error) { count() },
		Disconnected:    func(rails.ConnInfo, rails.DisconnectReason) { count() },
		PingRTT:         func(rails.ConnInfo, time.Duration) { count() },
		RequestAcked:    func(rails.ReqInfo, time.Duration) { count() },
		RequestDone:     func(rails.ReqInfo, time.Duration, error) { count() },
		HTTPDone:        func(rails.HTTPInfo, time.Duration, error) { count() },
	}
}
