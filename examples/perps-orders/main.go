// Command perps-orders places a post-only bid, replaces it, and cancels it, reconciling any
// command that got no answer instead of sending it again. Orders are paced to the budget of the
// account's type.
//
// Set RAILS_API_KEY and RAILS_API_SECRET, and pass a price well below the current mark so the
// bid rests on the book instead of trading:
//
//	go run ./examples/perps-orders -market BTC-USDT -price 50000
package main

import (
	"context"
	"errors"
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
	market := flag.String("market", "BTC-USDT", "market to trade")
	price := flag.String("price", "",
		"limit price, well below the market so the bid rests")
	quantity := flag.String("quantity", "0.0001", "order size")
	flag.Parse()
	if *price == "" {
		return errors.New("-price is required")
	}

	client, err := perps.NewClient(
		rails.Credentials{
			APIKey:    os.Getenv("RAILS_API_KEY"),
			APISecret: os.Getenv("RAILS_API_SECRET"),
		},
		rails.WithPacer(perps.NewPacer(perps.WithLimitsFromToken())),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := client.Connect(ctx, *market)
	if err != nil {
		return err
	}
	defer conn.Close()
	go drain(conn)

	created, err := conn.CreateOrder(ctx, perps.CreateOrderRequest{
		OrderType: perps.OrderTypeBuy,
		TradeType: perps.TradeTypeLimit,
		Price:     *price,
		Quantity:  *quantity,
		PostOnly:  true,
	})
	if err != nil {
		return reconcile(client, conn, *market, err)
	}
	fmt.Printf("created %s: %s\n", created.Order.OrderID, created.Order.OrderStatus)
	live := created.Order.OrderID
	defer func() {
		if live != "" {
			cancelLeftover(conn, live)
		}
	}()

	// A modify replaces the order; the answer describes the replacement, under a new id.
	size, err := strconv.ParseFloat(*quantity, 64)
	if err != nil {
		return err
	}
	modified, err := conn.ModifyOrder(ctx, perps.ModifyOrderRequest{
		OrderID:  created.Order.OrderID,
		Price:    *price,
		Quantity: fmt.Sprint(2 * size),
	})
	if err != nil {
		return reconcile(client, conn, *market, err)
	}
	fmt.Printf("modified %s into %s: quantity %s\n",
		created.Order.OrderID, modified.Order.OrderID, modified.Order.Quantity)
	live = modified.Order.OrderID

	cancelled, err := conn.CancelOrder(ctx, perps.CancelOrderRequest{
		OrderID: modified.Order.OrderID,
	})
	if err != nil {
		return err
	}
	fmt.Printf("cancelled %s: %s\n",
		cancelled.Order.OrderID, cancelled.Order.OrderStatus)
	live = ""
	return nil
}

// reconcile looks up a command that may have been applied without an answer reaching us, and
// cancels the order it finds still live, since the program is about to exit.
func reconcile(client *perps.Client, conn *perps.Conn, market string, err error) error {
	var apiErr *rails.Error
	if !errors.As(err, &apiErr) {
		return err
	}
	if errors.Is(err, rails.ErrNotSent) {
		return fmt.Errorf("never left, so it can be sent again: %w", err)
	}
	if !errors.Is(err, rails.ErrTimeout) && !errors.Is(err, rails.ErrConnClosed) {
		// The exchange answered: nothing was applied, and Code says why.
		return fmt.Errorf("refused with %s (%s): %w", apiErr.Code, apiErr.Slug, err)
	}
	// The command's own context may be what ran out, so the lookup gets a fresh one.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	order, lookupErr := client.OrderByClientRequestID(ctx, market,
		apiErr.ClientRequestID)
	if lookupErr != nil {
		return fmt.Errorf("%w; looking the order up: %w", err, lookupErr)
	}
	switch order.OrderStatus {
	case perps.OrderStatusFilled, perps.OrderStatusCancelled, perps.OrderStatusRejected:
		return fmt.Errorf("%w; order %s is %s", err, order.OrderID, order.OrderStatus)
	}
	cancelLeftover(conn, order.OrderID)
	return fmt.Errorf("%w; order %s was %s and is now cancelled", err, order.OrderID,
		order.OrderStatus)
}

// cancelLeftover cancels an order the program placed and could not finish with. Cancelling is
// idempotent by order id, so it is safe after a cancel that got no answer too.
func cancelLeftover(conn *perps.Conn, orderID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := perps.CancelOrderRequest{OrderID: orderID}
	if _, err := conn.CancelOrder(ctx, req); err != nil {
		fmt.Fprintf(os.Stderr, "cancelling %s: %v\n", orderID, err)
	}
}

// drain reads the events the connection delivers; they must be read even when unused.
func drain(conn *perps.Conn) {
	for range conn.Events() {
	}
}
