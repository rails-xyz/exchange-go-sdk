package options_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/rails-xyz/exchange-go-sdk/internal/spectest"
	"github.com/rails-xyz/exchange-go-sdk/options"
	"github.com/rails-xyz/exchange-go-sdk/rails"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every code this release has a constant for.
var knownCodes = map[rails.Code]struct{}{
	rails.CodeUnauthorized:       {},
	rails.CodeServerError:        {},
	rails.CodeTooManyRequests:    {},
	rails.CodeServiceUnavailable: {},
	rails.CodeRetryableError:     {},
	rails.CodeBadRequest:         {},
	rails.CodeNotFound:           {},
	rails.CodeForbidden:          {},
	rails.CodeRequestTimeout:     {},
	rails.CodeDuplicateRequest:   {},
	rails.CodeSoftBanned:         {},
	rails.CodeSubscriptionLimit:  {},
	rails.CodeConnectionLimit:    {},
	rails.CodeTradingDisabled:    {},

	options.CodeContractNotReady:               {},
	options.CodeInactiveContract:               {},
	options.CodeOrderRejected:                  {},
	options.CodeOrderNotFound:                  {},
	options.CodeReceiveWindowExceeded:          {},
	options.CodeRateLimited:                    {},
	options.CodeTooManyInflightOrders:          {},
	options.CodeEngineOverloaded:               {},
	options.CodeEngineTimeout:                  {},
	options.CodeCommitFailed:                   {},
	options.CodeInternalError:                  {},
	options.CodeMarkPriceUnavailable:           {},
	options.CodeCreateOrderInsufficientBalance: {},
	options.CodeCreateOrderMarginRatioExceeded: {},
	options.CodeCreateOrderMaxOpenOrders:       {},
	options.CodeCreateOrderSelfTrade:           {},
	options.CodeCreateOrderWouldMatch:          {},
	options.CodeCreateOrderReduceOnly:          {},
	options.CodeCreateOrderMinValue:            {},
	options.CodeCreateOrderDuplicateOrderID:    {},
	options.CodeCancelOrderNotOwned:            {},
	options.CodeModifyOrderInsufficientBalance: {},
	options.CodeModifyOrderMarginRatioExceeded: {},
	options.CodeModifyOrderMaxOpenOrders:       {},
	options.CodeModifyOrderSelfTrade:           {},
	options.CodeModifyOrderWouldMatch:          {},
	options.CodeModifyOrderReduceOnly:          {},
	options.CodeModifyOrderMinValue:            {},
	options.CodeModifyOrderNotOwned:            {},
	options.CodeModifyOrderQuantityBelowFilled: {},
}

// Every error code the vendored specs show has a constant.
func TestSpecCodesHaveConstants(t *testing.T) {
	var docs []any
	for _, ex := range spectest.LoadExamples(t, "../api/options-ws.yaml") {
		for _, e := range ex {
			var payload any
			require.NoError(t, json.Unmarshal(e.Payload, &payload))
			docs = append(docs, payload)
		}
	}
	for _, path := range []string{"../api/options.json", "../api/options-matching.json"} {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		var doc any
		require.NoError(t, json.Unmarshal(raw, &doc))
		docs = append(docs, doc)
	}

	seen := 0
	for _, doc := range docs {
		for _, code := range errorCodes(doc) {
			seen++
			assert.Contains(t, knownCodes, rails.Code(code), "the specs name a code this release has no constant for")
		}
	}
	require.NotZero(t, seen, "no error codes found in the specs")
}

func errorCodes(v any) []string {
	var out []string
	switch v := v.(type) {
	case map[string]any:
		slug, slugOK := v["slug"].(string)
		code, codeOK := v["code"].(string)
		if slugOK && codeOK && slug != "" {
			out = append(out, code)
		}
		for _, child := range v {
			out = append(out, errorCodes(child)...)
		}
	case []any:
		for _, child := range v {
			out = append(out, errorCodes(child)...)
		}
	}
	return out
}
