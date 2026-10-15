// Command options-quickstart finds an options contract over HTTP, places a post-only bid far
// below its book over the WebSocket, cancels it, and prints how long the exchange took to
// acknowledge and to answer each command.
//
// Set RAILS_API_KEY and RAILS_API_SECRET:
//
//	go run ./examples/options-quickstart -market BTC-USDT
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
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
	price := flag.String("price", "10",
		"limit price, well below the contract's book so the bid rests")
	flag.Parse()

	client, err := options.NewClient(
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

	expirations, err := client.Expirations(ctx, *market)
	if err != nil {
		return err
	}
	if len(expirations) == 0 {
		return errors.New("no expirations on " + *market)
	}
	contracts, err := client.Contracts(ctx, *market, expirations[0].ExpiryDate)
	if err != nil {
		return err
	}
	if len(contracts) == 0 {
		return errors.New("no contracts on " + *market)
	}
	// A small bid rests on the most expensive contract.
	contract, highest := contracts[0].ContractName, 0.0
	for _, c := range contracts {
		if mark, _ := strconv.ParseFloat(c.MarkPrice, 64); mark > highest {
			contract, highest = c.ContractName, mark
		}
	}

	// One connection follows the whole expiry, which contract names spell second.
	expiry := strings.Split(contract, "-")[1]
	conn, err := client.Connect(ctx, options.Scope{Market: *market, Expiration: expiry})
	if err != nil {
		return err
	}
	defer conn.Close()

	start := time.Now()
	created, err := conn.CreateOrder(ctx, options.CreateOrderRequest{
		Contract:  contract,
		OrderType: options.OrderTypeBuy,
		TradeType: options.TradeTypeLimit,
		Price:     *price,
		Quantity:  "1",
		PostOnly:  true,
	})
	if err != nil {
		return err
	}
	fmt.Printf("created %s on %s: acknowledged in %s, %s in %s\n",
		created.Order.OrderID, contract, created.AckAt.Sub(start),
		created.Order.Status, created.TerminalAt.Sub(start))

	start = time.Now()
	cancelled, err := conn.CancelOrder(ctx, options.CancelOrderRequest{
		Contract: contract,
		OrderID:  created.Order.OrderID,
	})
	if err != nil {
		return err
	}
	fmt.Printf("%s in %s\n", cancelled.Order.Status, cancelled.TerminalAt.Sub(start))
	return nil
}
