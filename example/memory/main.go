// 演示 delaytimer 的 Memory 后端：到期后执行 OrderTimeout Handler。
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
	timer := delaytimer.New(delaytimer.NewMemory(),
		delaytimer.WithHandlers(delaytimer.Bind(&OrderTimeout{}, func(_ context.Context, p *OrderTimeout) error {
			handled <- p
			return nil
		})),
		delaytimer.WithPollInterval(10*time.Millisecond),
	)
	defer timer.Close()
	if err := timer.Start(context.Background()); err != nil {
		log.Fatal(err)
	}

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
