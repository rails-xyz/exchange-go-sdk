package options_test

import (
	"testing"

	"github.com/rails-xyz/exchange-go-sdk/internal/ws"
	"github.com/rails-xyz/exchange-go-sdk/options"
	"github.com/stretchr/testify/assert"
)

func TestFillWithoutAnOrderIsUnknown(t *testing.T) {
	event := options.ToEvent(ws.Frame{ResultType: "completeFillOrder", Data: []byte(`{"statusCode":200,"order":null}`)})
	assert.IsType(t, options.Unknown{}, event)
}
