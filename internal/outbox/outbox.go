// Package outbox delivers a connection's events to its consumer without the connection ever
// waiting on it. The connection's owner goroutine has to keep reading the socket, so the
// control pings it answers there, the timers and the answers to commands are never held up by
// a consumer that is behind.
package outbox

import "sync"

// backlogFactor sizes the backlog, in event buffers, past which market data is dropped.
const backlogFactor = 16

// Options say which events may be dropped when the consumer falls behind, and how to tell it
// that some were.
type Options[E any] struct {
	// Buffer is the capacity of the channel Events returns.
	Buffer int
	// Droppable reports an event a consumer can rebuild from a snapshot, such as public market
	// data. Everything else is always delivered.
	Droppable func(E) bool
	// Dropped is the event delivered once the consumer has caught up, counting what was
	// dropped. Droppable events flow again after it.
	Dropped func(n int) E
}

// Outbox holds what the consumer has not taken yet. Its goroutine owns the backlog; Publish
// and Close reach it through channels.
type Outbox[E any] struct {
	opts     Options[E]
	in       chan E
	out      chan E
	done     chan struct{}
	finished chan struct{}
	stop     sync.Once
}

// New starts the goroutine that feeds Events. Close stops it.
func New[E any](opts Options[E]) *Outbox[E] {
	o := &Outbox[E]{
		opts:     opts,
		in:       make(chan E),
		out:      make(chan E, opts.Buffer),
		done:     make(chan struct{}),
		finished: make(chan struct{}),
	}
	go o.run()
	return o
}

// Events is the channel the consumer reads. It is closed by Close.
func (o *Outbox[E]) Events() <-chan E { return o.out }

// Publish hands an event over. It never waits for the consumer, and does nothing after Close.
func (o *Outbox[E]) Publish(event E) {
	select {
	case o.in <- event:
	case <-o.done:
	}
}

// Close hands over what still fits in Events without waiting for the consumer, closes it, and
// returns once the goroutine has stopped. Call it after the last Publish.
func (o *Outbox[E]) Close() {
	o.stop.Do(func() { close(o.done) })
	<-o.finished
}

func (o *Outbox[E]) run() {
	defer close(o.finished)
	limit := max(o.opts.Buffer, 1) * backlogFactor
	var backlog []E
	dropped := 0
	for {
		var out chan E
		var head E
		if len(backlog) > 0 {
			out, head = o.out, backlog[0]
		}
		select {
		case event := <-o.in:
			// Once dropping starts it lasts until the consumer has caught up, so the gap is one
			// stretch that a single snapshot repairs.
			if (dropped > 0 || len(backlog) >= limit) && o.opts.Droppable(event) {
				dropped++
				continue
			}
			backlog = append(backlog, event)
		case out <- head:
			var zero E
			backlog[0] = zero
			backlog = backlog[1:]
			if len(backlog) == 0 {
				backlog = nil // a burst's array is not kept once it has been read
				if dropped > 0 {
					backlog = append(backlog, o.opts.Dropped(dropped))
					dropped = 0
				}
			}
		case <-o.done:
			for _, event := range backlog {
				select {
				case o.out <- event:
				default:
					close(o.out)
					return
				}
			}
			close(o.out)
			return
		}
	}
}
