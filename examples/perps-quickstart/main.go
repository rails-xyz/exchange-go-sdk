// Command perps-quickstart places a post-only bid in production, cancels it, and prints how long
// the exchange took to acknowledge and to answer each command.
//
// Set RAILS_API_KEY and RAILS_API_SECRET, and pass a price well below the current mark so the
// bid rests on the book instead of trading:
//
//	go run ./examples/perps-quickstart -market BTC-USDT -price 50000
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
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

	start := time.Now()
	created, err := conn.CreateOrder(ctx, perps.CreateOrderRequest{
		OrderType: perps.OrderTypeBuy,
		TradeType: perps.TradeTypeLimit,
		Price:     *price,
		Quantity:  *quantity,
		PostOnly:  true,
	})
	if err != nil {
		return err
	}
	fmt.Printf("created %s: acknowledged in %s, %s in %s\n",
		created.Order.OrderID, created.Ack.At.Sub(start),
		created.Order.OrderStatus, created.TerminalAt.Sub(start))

	start = time.Now()
	cancelled, err := conn.CancelOrder(ctx, perps.CancelOrderRequest{
		OrderID: created.Order.OrderID,
	})
	if err != nil {
		return err
	}
	fmt.Printf("cancelled: acknowledged in %s, %s in %s\n",
		cancelled.Ack.At.Sub(start), cancelled.Order.OrderStatus,
		cancelled.TerminalAt.Sub(start))
	return nil
}
