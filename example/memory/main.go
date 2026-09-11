package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/itnxs/delaytimer"
)

func main() {
	handled := make(chan *OrderTimeout, 1)
	ctx, cancel := context.WithCancel(context.Background())
	timer := delaytimer.New(delaytimer.NewMemory(),
		delaytimer.WithHandlers(delaytimer.Bind(&OrderTimeout{}, func(_ context.Context, p *OrderTimeout) error {
			handled <- p
			return nil
		})),
		delaytimer.WithPollInterval(10*time.Millisecond),
	)
	defer func() {
		cancel()
		timer.Close()
	}()
	go func() { _ = timer.Run(ctx) }()

	if err := timer.SetEvent(time.Now().Add(-time.Second), &OrderTimeout{OrderID: "ok"}); err != nil {
		log.Fatal(err)
	}

	select {
	case p := <-handled:
		fmt.Printf("handled order_timeout: %s\n", p.OrderID)
	case <-time.After(5 * time.Second):
		log.Fatal("timeout waiting for handler")
	}
}
