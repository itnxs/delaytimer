// 演示 delaytimer 的 AMQP 后端：到期后执行 OrderTimeout Handler。
// 需要 RabbitMQ 插件 rabbitmq_delayed_message_exchange。
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "time"

    "github.com/itnxs/delaytimer"
    amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
    url := os.Getenv("AMQP_URL")
    if url == "" {
        url = "amqp://guest:guest@127.0.0.1:5672/"
    }
    conn, err := amqp.Dial(url)
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    handled := make(chan *OrderTimeout, 1)
    timer := delaytimer.New(
        delaytimer.NewAMQP(conn, delaytimer.AMQPConfig{
            Exchange:   "delay.ex",
            RoutingKey: "delay.rk",
            Queue:      "delay.q",
        }),
        delaytimer.WithHandlers(delaytimer.Bind(&OrderTimeout{}, func(_ context.Context, p *OrderTimeout) error {
            handled <- p
            return nil
        })),
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
