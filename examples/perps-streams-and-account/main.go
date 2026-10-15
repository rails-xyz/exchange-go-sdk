// Command perps-streams-and-account reads the account over HTTP, then follows a market's book,
// its trades and the account's fills over the WebSocket for a while, printing the top of the
// book as it changes.
//
// Set RAILS_API_KEY and RAILS_API_SECRET:
//
//	go run ./examples/perps-streams-and-account -market BTC-USDT -for 30s
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/perps"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	market := flag.String("market", "BTC-USDT", "market to follow")
	duration := flag.Duration("for", 30*time.Second, "how long to follow the streams")
	flag.Parse()

	client, err := perps.NewClient(rails.Credentials{
		APIKey:    os.Getenv("RAILS_API_KEY"),
		APISecret: os.Getenv("RAILS_API_SECRET"),
	})
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	if err := printAccount(ctx, client); err != nil {
		return err
	}

	conn, err := client.Connect(ctx, *market, perps.WithSubscriptions(
		perps.SubscriptionTrades,
		perps.SubscriptionPublicTrades,
		perps.SubscriptionPublicOrderBook,
	))
	if err != nil {
		return err
	}
	defer conn.Close()

	book := newOrderBook()
	snapshots := make(chan bookSnapshot, 1)
	for {
		select {
		case <-ctx.Done():
			return nil
		case s := <-snapshots:
			if err := book.load(s); err != nil {
				return err
			}
		case event, ok := <-conn.Events():
			if !ok {
				return nil
			}
			switch e := event.(type) {
			case perps.Connected:
				// On the first socket and after every reconnect the book restarts from a snapshot.
				// It is asked for off this loop, which has to keep draining events meanwhile.
				generation := book.resync()
				go func() {
					snapshot, err := conn.EmitPublicOrderBook(ctx, *market)
					select {
					case snapshots <- bookSnapshot{generation, snapshot, err}:
					case <-ctx.Done():
					}
				}()
			case perps.OrderBookDelta:
				if book.update(e) {
					bid, ask := book.top()
					fmt.Printf("book %s: %s / %s\n", *market, bid, ask)
				}
			case perps.PublicTrades:
				for _, t := range e.Trades {
					fmt.Printf("trade %s: %s %s at %s\n",
						*market, t.OrderType, t.Quantity, t.Price)
				}
			case perps.Trades:
				for _, t := range e.Trades {
					fmt.Printf("your fill %s: %s %s at %s\n",
						t.Market, t.OrderType, t.Quantity, t.Price)
				}
			case perps.SessionBackfill:
				if !e.Resumed || len(e.ExpiredMarkets) > 0 {
					// Order events were lost while disconnected: rebuild order state over HTTP.
					orders, err := client.OpenOrders(ctx, perps.MarketAll)
					if err != nil {
						return err
					}
					fmt.Printf("rebuilt: %d open orders\n", len(orders))
				}
			case perps.Disconnected:
				fmt.Println("disconnected:", e.Reason)
			}
		}
	}
}

func printAccount(ctx context.Context, client *perps.Client) error {
	balances, err := client.Balances(ctx, perps.MarketAll)
	if err != nil {
		return err
	}
	fmt.Printf("balance %s USDT, available %s\n",
		balances.TotalBalance, balances.AvailableBalance)

	positions, err := client.Positions(ctx, perps.MarketAll)
	if err != nil {
		return err
	}
	for _, p := range positions {
		fmt.Printf("position %s: %s, unrealized PnL %s\n",
			p.Market, p.Quantity, p.UnrealizedPnL)
	}

	// Paginated reads hand back one page at a time: pass Next back until it comes back empty.
	deposits := 0
	for cursor := ""; ; {
		page, err := client.Deposits(ctx, cursor)
		if err != nil {
			return err
		}
		deposits += len(page.Items)
		if cursor = page.Next; cursor == "" {
			break
		}
	}
	fmt.Printf("%d deposits\n", deposits)
	return nil
}

// orderBook is a market's book kept from a snapshot and the deltas after it, keyed by price.
// Deltas that arrive while a snapshot is on its way are held back, then replayed onto it; a
// delta older than the snapshot, held or still queued behind it, is dropped.
type orderBook struct {
	bids, asks map[string]string
	generation int // only the answer to the latest snapshot request is loaded
	syncing    bool
	held       []perps.OrderBookDelta
	snapshotTs int64
}

type bookSnapshot struct {
	generation int
	snapshot   *perps.OrderBookSnapshot
	err        error
}

func newOrderBook() *orderBook {
	return &orderBook{bids: map[string]string{}, asks: map[string]string{}}
}

// resync starts waiting for a new snapshot, and returns the generation its answer carries.
func (b *orderBook) resync() int {
	b.generation++
	b.syncing, b.held = true, nil
	return b.generation
}

// update applies a delta, or holds it back while a snapshot is on its way. It reports whether
// the book changed.
func (b *orderBook) update(d perps.OrderBookDelta) bool {
	if b.syncing {
		b.held = append(b.held, d)
		return false
	}
	if d.Ts < b.snapshotTs {
		return false
	}
	b.apply(d.Orders)
	return true
}

// load replaces the book with the latest snapshot requested, then replays the deltas held back
// that are not older than it.
func (b *orderBook) load(s bookSnapshot) error {
	if s.generation != b.generation {
		return nil
	}
	if s.err != nil {
		return fmt.Errorf("order book snapshot: %w", s.err)
	}
	b.bids, b.asks = map[string]string{}, map[string]string{}
	b.apply(s.snapshot.Orders)
	b.snapshotTs = s.snapshot.Ts
	for _, d := range b.held {
		if d.Ts >= b.snapshotTs {
			b.apply(d.Orders)
		}
	}
	b.syncing, b.held = false, nil
	return nil
}

// apply replaces each level at its price; a quantity of "0" removes it.
func (b *orderBook) apply(levels []perps.OrderBookLevel) {
	for _, l := range levels {
		side := b.asks
		if l.OrderType == perps.OrderTypeBuy {
			side = b.bids
		}
		if q, _ := strconv.ParseFloat(l.Quantity, 64); q == 0 {
			delete(side, l.Price)
		} else {
			side[l.Price] = l.Quantity
		}
	}
}

func (b *orderBook) top() (bid, ask string) {
	higher := func(p, q float64) bool { return p > q }
	lower := func(p, q float64) bool { return p < q }
	return best(b.bids, higher), best(b.asks, lower)
}

func best(side map[string]string, better func(p, q float64) bool) string {
	top, found := 0.0, ""
	for price := range side {
		p, err := strconv.ParseFloat(price, 64)
		if err != nil {
			continue
		}
		if found == "" || better(p, top) {
			top, found = p, price
		}
	}
	if found == "" {
		return "-"
	}
	return found
}
