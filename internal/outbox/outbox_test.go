package outbox

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type event struct {
	kind    string // "order", "book" or "dropped"
	dropped int
}

func newTestOutbox(buffer int) *Outbox[event] {
	return New(Options[event]{
		Buffer:    buffer,
		Droppable: func(e event) bool { return e.kind == "book" },
		Dropped:   func(n int) event { return event{kind: "dropped", dropped: n} },
	})
}

func publishAll(t *testing.T, o *Outbox[event], events ...event) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		for _, e := range events {
			o.Publish(e)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish waited for the consumer")
	}
}

func take(t *testing.T, o *Outbox[event]) event {
	t.Helper()
	select {
	case e := <-o.Events():
		return e
	case <-time.After(2 * time.Second):
		t.Fatal("no event")
		return event{}
	}
}

// Past the backlog market data is dropped, never an order event, until the consumer has caught
// up; then one event counts the gap and market data flows again.
func TestMarketDataIsDroppedUntilTheConsumerCatchesUp(t *testing.T) {
	o := newTestOutbox(1)
	t.Cleanup(o.Close)

	var flood []event
	for i := range 100 {
		flood = append(flood, event{kind: "book"})
		if i == 50 {
			flood = append(flood, event{kind: "order"})
		}
	}
	publishAll(t, o, flood...)

	books, orders := 0, 0
	for {
		e := take(t, o)
		if e.kind == "dropped" {
			assert.Equal(t, 100-books, e.dropped, "the count is not what went missing")
			break
		}
		if e.kind == "order" {
			orders++
		} else {
			books++
		}
	}
	assert.Equal(t, 1, orders)
	assert.Less(t, books, 100)

	publishAll(t, o, event{kind: "book"})
	assert.Equal(t, "book", take(t, o).kind, "market data still dropped after the consumer caught up")
}

// Close hands over what fits and closes Events, whether or not anyone is reading.
func TestCloseDoesNotWaitForTheConsumer(t *testing.T) {
	o := newTestOutbox(2)
	publishAll(t, o, event{kind: "order"}, event{kind: "order"}, event{kind: "order"})

	closed := make(chan struct{})
	go func() { o.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close waited for the consumer")
	}

	var got []event
	for e := range o.Events() {
		got = append(got, e)
	}
	require.Len(t, got, 2)
	publishAll(t, o, event{kind: "order"})
}
