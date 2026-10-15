// Command options-orders places a post-only bid, modifies it and cancels it, then places two more
// bids and sweeps the contract with a cancel-all whose cancels arrive as events. Every order goes
// out under an order id of the program's own, so one whose outcome is unknown can still be
// cancelled when the program stops. Orders are paced to the budget of the account's type.
//
// Set RAILS_API_KEY and RAILS_API_SECRET:
//
//	go run ./examples/options-orders -market BTC-USDT -price 10
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
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
	price := flag.String("price", "10",
		"limit price, well below the contract's book so the bids rest")
	flag.Parse()

	client, err := options.NewClient(
		rails.Credentials{
			APIKey:    os.Getenv("RAILS_API_KEY"),
			APISecret: os.Getenv("RAILS_API_SECRET"),
		},
		rails.WithPacer(options.NewPacer(options.WithLimitsFromToken())),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	contract, err := mostExpensive(ctx, client, *market)
	if err != nil {
		return err
	}
	// Contract names spell the expiry second: BTC-7FEB26-100000-C.
	expiry := strings.Split(contract, "-")[1]
	conn, err := client.Connect(ctx, options.Scope{Market: *market, Expiration: expiry})
	if err != nil {
		return err
	}
	defer conn.Close()

	// Orders that may be on the book, cancelled if the program stops before it is done.
	live := map[string]bool{}
	defer func() {
		for orderID := range live {
			cancelLeftover(conn, contract, orderID)
		}
	}()

	bid := options.CreateOrderRequest{
		Contract:  contract,
		OrderType: options.OrderTypeBuy,
		TradeType: options.TradeTypeLimit,
		Price:     *price,
		Quantity:  "1",
		PostOnly:  true,
	}

	// Each command blocks until the exchange answers it.
	created, err := place(ctx, conn, bid, live)
	if err != nil {
		return reconcile(client, err)
	}
	fmt.Printf("created %s on %s: %s\n",
		created.Order.OrderID, contract, created.Order.Status)

	// A modify keeps the order's id.
	modified, err := conn.ModifyOrder(ctx, options.ModifyOrderRequest{
		Contract: contract,
		OrderID:  created.Order.OrderID,
		Price:    *price,
		Quantity: "2",
	})
	if err != nil {
		return describe(err)
	}
	fmt.Printf("modified %s: quantity %s\n",
		modified.Order.OrderID, modified.Order.Quantity)

	cancelled, err := conn.CancelOrder(ctx, options.CancelOrderRequest{
		Contract: contract,
		OrderID:  created.Order.OrderID,
	})
	if err != nil {
		return describe(err)
	}
	if cancelled.ViaHTTP {
		// Only queued: the answer arrives as an event, caught with the sweep's below.
		fmt.Printf("cancel of %s queued over HTTP\n", created.Order.OrderID)
	} else {
		delete(live, created.Order.OrderID)
		fmt.Printf("cancelled %s\n", created.Order.OrderID)
	}

	for range 2 {
		created, err := place(ctx, conn, bid, live)
		if err != nil {
			return reconcile(client, err)
		}
		fmt.Printf("created %s\n", created.Order.OrderID)
	}

	// Cancel-all returns once the sweep is taken on; each cancel is answered as its own event.
	// It cancels every order of yours on the contract, not only this program's.
	sweep, err := conn.CancelAllOrders(ctx, options.CancelAllOrdersRequest{
		Contract: contract,
	})
	if err != nil {
		return describe(err)
	}
	if sweep.ViaHTTP {
		fmt.Printf("sweep of %d orders queued over HTTP\n", sweep.Count)
	} else {
		fmt.Printf("sweeping %d orders\n", sweep.Count)
	}
	for _, u := range sweep.Unqueued {
		if live[u.OrderID] {
			// Still open; the cleanup above cancels it one by one.
			return fmt.Errorf("the sweep could not reach %s", u.OrderID)
		}
	}

	// Each order the program placed is answered once: cancelled by the sweep, or gone before it.
	for len(live) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-conn.Events():
			if !ok {
				return nil
			}
			switch e := event.(type) {
			case options.OrderCancelled:
				if live[e.Order.OrderID] {
					delete(live, e.Order.OrderID)
					fmt.Printf("swept %s\n", e.Order.OrderID)
				}
			case options.OrderFilled:
				if live[e.Order.OrderID] && !e.Partial {
					delete(live, e.Order.OrderID)
					fmt.Printf("%s filled before the sweep\n", e.Order.OrderID)
				}
			case options.OrderRejected:
				switch {
				case !live[e.Order.OrderID]:
				case e.Code == options.CodeOrderNotFound:
					delete(live, e.Order.OrderID)
					fmt.Printf("%s was already gone\n", e.Order.OrderID)
				default:
					return fmt.Errorf("could not cancel %s: %s",
						e.Order.OrderID, e.Code)
				}
			}
		}
	}
	return nil
}

// place sends a bid under an order id of its own, tracked in live from before it goes out, so
// a create whose outcome is unknown is still cancelled on exit. A refusal, or a command that
// never left, placed nothing.
func place(
	ctx context.Context,
	conn *options.Conn,
	bid options.CreateOrderRequest,
	live map[string]bool,
) (*options.OrderResult, error) {
	bid.OrderID = ulid.Make().String()
	live[bid.OrderID] = true
	created, err := conn.CreateOrder(ctx, bid)
	if err != nil {
		if !outcomeUnknown(err) {
			delete(live, bid.OrderID)
		}
		return nil, err
	}
	return created, nil
}

// reconcile says what a create's failure means for the order. One that got no answer may still
// have been applied, so the order is looked up by the id it went out under.
func reconcile(client *options.Client, err error) error {
	var apiErr *rails.Error
	if !outcomeUnknown(err) || !errors.As(err, &apiErr) || apiErr.OrderID == "" {
		return describe(err)
	}
	// The command's own context may be what ran out, so the lookup gets a fresh one.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, lookupErr := client.OrderStatus(ctx, apiErr.OrderID)
	if lookupErr != nil {
		return fmt.Errorf("%w; looking order %s up: %w", err, apiErr.OrderID, lookupErr)
	}
	return fmt.Errorf("%w; order %s is %s", err, apiErr.OrderID, status.Status)
}

// outcomeUnknown reports whether a failed command may still have been applied.
func outcomeUnknown(err error) bool {
	var apiErr *rails.Error
	if errors.Is(err, rails.ErrTimeout) || errors.Is(err, rails.ErrConnClosed) {
		return true
	}
	return errors.As(err, &apiErr) &&
		(apiErr.StatusCode == http.StatusBadGateway || apiErr.StatusCode == http.StatusGatewayTimeout)
}

// describe says what an order command's failure means for the order.
func describe(err error) error {
	var apiErr *rails.Error
	errors.As(err, &apiErr)
	switch {
	case errors.Is(err, rails.ErrNotSent):
		return fmt.Errorf("never left, so it can be sent again: %w", err)
	case outcomeUnknown(err):
		// Never sent again; the order may be on the book, so look for it before placing it again.
		return fmt.Errorf("outcome unknown: %w", err)
	case apiErr != nil:
		return fmt.Errorf("refused with %s (%s): %w", apiErr.Code, apiErr.Slug, err)
	}
	return err
}

// cancelLeftover cancels an order the program placed and could not finish with. The cancel goes
// over HTTP when the connection cannot carry it, and is then only queued.
func cancelLeftover(conn *options.Conn, contract, orderID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cancelled, err := conn.CancelOrder(ctx, options.CancelOrderRequest{
		Contract: contract,
		OrderID:  orderID,
	})
	switch {
	case err != nil:
		fmt.Fprintf(os.Stderr, "cancelling %s: %v\n", orderID, err)
	case cancelled.ViaHTTP:
		fmt.Fprintf(os.Stderr, "cancel of %s queued over HTTP; check OpenOrders\n",
			orderID)
	}
}

// mostExpensive returns the contract with the highest mark price on the nearest expiry, on which
// a small bid rests rather than trades.
func mostExpensive(
	ctx context.Context, client *options.Client, market string,
) (string, error) {
	expirations, err := client.Expirations(ctx, market)
	if err != nil {
		return "", err
	}
	if len(expirations) == 0 {
		return "", errors.New("no expirations on " + market)
	}
	contracts, err := client.Contracts(ctx, market, expirations[0].ExpiryDate)
	if err != nil {
		return "", err
	}
	if len(contracts) == 0 {
		return "", errors.New("no contracts on " + market)
	}
	contract, highest := contracts[0].ContractName, 0.0
	for _, c := range contracts {
		if mark, _ := strconv.ParseFloat(c.MarkPrice, 64); mark > highest {
			contract, highest = c.ContractName, mark
		}
	}
	return contract, nil
}
