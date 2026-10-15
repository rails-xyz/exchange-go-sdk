package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/rails"
)

const (
	// sendTimeout bounds one attempt, and so bounds how long [Collector.Close] can take.
	sendTimeout = 5 * time.Second
	retryWait   = time.Second
	path        = "/api/v1/telemetry"
)

// queued is a report waiting to be sent. The last of a client's life is not retried.
type queued struct {
	report *Report
	final  bool
}

// reporter sends reports to Rails. A failure never leaves this file: it is a debug line, and
// the next window carries fresher numbers.
type reporter struct {
	http      *http.Client
	tokens    rails.TokenSource
	logger    *slog.Logger
	url       string
	userAgent string
}

func newReporter(cfg *rails.Config, tokens rails.TokenSource) *reporter {
	return &reporter{
		http:      cfg.HTTPClient,
		tokens:    tokens,
		logger:    cfg.Logger,
		url:       strings.TrimSuffix(cfg.Environment.TelemetryURL, "/") + path,
		userAgent: rails.UserAgent(),
	}
}

// run sends until the queue closes. ctx ends with the client and abandons the send under way;
// the last report goes regardless, so closing still flushes.
func (r *reporter) run(ctx context.Context, reports <-chan queued, finished chan struct{}) {
	defer close(finished)
	for q := range reports {
		r.send(ctx, q)
	}
}

func (r *reporter) send(ctx context.Context, q queued) {
	body, err := encode(q.report)
	if err != nil {
		r.logger.Debug("telemetry report could not be encoded", "err", err)
		return
	}
	attempts := 2
	if q.final {
		ctx, attempts = context.WithoutCancel(ctx), 1
	}

	for attempt := range attempts {
		if attempt > 0 {
			select {
			case <-time.After(retryWait):
			case <-ctx.Done():
				return
			}
		}
		if err = r.post(ctx, body); err == nil {
			return
		}
	}
	r.logger.Debug("telemetry report not delivered", "attempts", attempts, "err", err)
}

func (r *reporter) post(parent context.Context, body []byte) error {
	ctx, cancel := context.WithTimeout(parent, sendTimeout)
	defer cancel()

	token, err := r.tokens.Token(ctx)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", r.userAgent)

	resp, err := r.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("telemetry: %s", resp.Status)
	}
	return nil
}

func encode(report *Report) ([]byte, error) {
	return json.Marshal(report)
}
