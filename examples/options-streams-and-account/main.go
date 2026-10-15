// Command options-streams-and-account reads the account over HTTP, then follows the nearest
// expiry of a market over the WebSocket for a while: its quotes, mark prices and greeks, and the
// account's positions and open orders.
//
// Set RAILS_API_KEY and RAILS_API_SECRET:
//
//	go run ./examples/options-streams-and-account -market BTC-USDT -for 30s
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/options"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	market := flag.String("market", "BTC-USDT", "underlying market")
	duration := flag.Duration("for", 30*time.Second, "how long to follow the streams")
	flag.Parse()

	client, err := options.NewClient(rails.Credentials{
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

	expirations, err := client.Expirations(ctx, *market)
	if err != nil {
		return err
	}
	if len(expirations) == 0 {
		return errors.New("no expirations on " + *market)
	}
	expiry := expirations[0].ExpiryDate
	contracts, err := client.Contracts(ctx, *market, expiry)
	if err != nil {
		return err
	}
	if len(contracts) == 0 {
		return errors.New("no contracts on " + *market)
	}

	// A scope names the expiry as contract names spell it, second: BTC-7FEB26-100000-C.
	expiration := strings.Split(contracts[0].ContractName, "-")[1]
	scope := options.Scope{Market: *market, Expiration: expiration}
	conn, err := client.Connect(ctx, scope, options.WithSubscriptions(
		options.SubscriptionQuotes, options.SubscriptionContracts,
		options.SubscriptionPositions, options.SubscriptionOpenOrders,
	))
	if err != nil {
		return err
	}
	defer conn.Close()

	quotes := map[string]options.Quote{}
	snapshots := make(chan []options.Quote, 1)
	for {
		select {
		case <-ctx.Done():
			return nil
		case snapshot := <-snapshots:
			for _, q := range snapshot {
				keepNewer(quotes, q)
			}
			fmt.Printf("%d quotes on %s\n", len(quotes), expiry)
		case event, ok := <-conn.Events():
			if !ok {
				return nil
			}
			switch e := event.(type) {
			case options.Connected:
				// Only the orders stream is replayed after a reconnect, so every other stream
				// restarts from a snapshot, asked for off this loop, which keeps draining events.
				go func() {
					snapshot, err := quoteSnapshot(ctx, conn, *market, expiry)
					if err != nil {
						fmt.Fprintln(os.Stderr, "quotes snapshot:", err)
						return
					}
					select {
					case snapshots <- snapshot:
					case <-ctx.Done():
					}
				}()
			case options.QuoteUpdate:
				keepNewer(quotes, e.Quote)
				fmt.Printf("quote %s: %s / %s\n",
					e.Quote.Contract, e.Quote.BestBid, e.Quote.BestAsk)
			case options.ContractUpdate:
				fmt.Printf("mark %s: %s, IV %s, delta %s\n",
					e.ContractName, e.MarkPrice, e.MarkIV, e.Delta)
			case options.PositionsDelta:
				for _, p := range e.Positions {
					fmt.Printf("position %s: %s %s\n", p.Contract, p.Side, p.Qty)
				}
			case options.OpenOrdersDelta:
				for _, c := range e.Changes {
					fmt.Printf("open orders %s: %s %s\n",
						e.Contract, c.Kind, c.Order.OrderID)
				}
			case options.Liquidation:
				fmt.Printf("liquidation on %s: margin ratio %s\n",
					e.Contract, e.MarginRatio)
			case options.SubscriptionLost:
				fmt.Printf("lost %v on %+v: %v\n", e.Subscriptions, e.Scope, e.Err)
			case options.SessionBackfill:
				if !e.Resumed || len(e.ExpiredMarkets) > 0 {
					// Order events were lost while disconnected: rebuild order state over HTTP.
					page, err := client.OpenOrders(ctx, options.PageQuery{})
					if err != nil {
						return err
					}
					fmt.Printf("rebuilt: %d open orders on the first page\n",
						len(page.Items))
				}
			case options.Disconnected:
				fmt.Println("disconnected:", e.Reason)
			}
		}
	}
}

// keepNewer stores q unless the quote already held for its contract is newer. A snapshot can
// arrive after updates that followed it, and must not overwrite them.
func keepNewer(quotes map[string]options.Quote, q options.Quote) {
	if held, ok := quotes[q.Contract]; !ok || q.Ts >= held.Ts {
		quotes[q.Contract] = q
	}
}

// quoteSnapshot reads every page of an expiry's quotes, the state QuoteUpdate continues from.
func quoteSnapshot(
	ctx context.Context, conn *options.Conn, market, expiry string,
) ([]options.Quote, error) {
	var quotes []options.Quote
	for query := (options.ExpiryQuery{Market: market, ExpiryDate: expiry}); ; {
		page, err := conn.EmitQuotes(ctx, query)
		if err != nil {
			return nil, err
		}
		quotes = append(quotes, page.Items...)
		if page.Next == "" {
			return quotes, nil
		}
		query.PageToken = page.Next
	}
}

func printAccount(ctx context.Context, client *options.Client) error {
	summary, err := client.AccountSummary(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("equity %s USDT, available %s\n",
		summary.TotalEquity, summary.AvailableBalance)

	positions, err := client.Positions(ctx)
	if err != nil {
		return err
	}
	for _, p := range positions {
		fmt.Printf("position %s: %s, unrealized PnL %s\n",
			p.Contract, p.Qty, p.UnrealizedPnL)
	}

	// Paginated reads hand back one page at a time: pass Next back until it comes back empty.
	deposits := 0
	for query := (options.PageQuery{}); ; {
		page, err := client.Deposits(ctx, query)
		if err != nil {
			return err
		}
		deposits += len(page.Items)
		if page.Next == "" {
			break
		}
		query.PageToken = page.Next
	}
	fmt.Printf("%d deposits\n", deposits)
	return nil
}
