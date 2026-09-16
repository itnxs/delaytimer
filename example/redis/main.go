// 演示 delaytimer 的 Redis 后端：到期后执行 OrderTimeout Handler。
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/itnxs/delaytimer"
)

func main() {
    addr := os.Getenv("REDIS_ADDR")
    if addr == "" {
        addr = "127.0.0.1:6379"
    }
    rdb := redis.NewClient(&redis.Options{Addr: addr})
    defer rdb.Close()
    if err := rdb.Ping(context.Background()).Err(); err != nil {
        log.Fatal(err)
    }

    handled := make(chan *OrderTimeout, 1)
    timer := delaytimer.New(
        delaytimer.NewRedis(rdb, "delaytimer:jobs"),
        delaytimer.WithHandlers(delaytimer.Bind(&OrderTimeout{}, func(_ context.Context, p *OrderTimeout) error {
            handled <- p
            return nil
        }),
            &DemoTimeoutHandler{},
        ),
        delaytimer.WithPollInterval(50*time.Millisecond),
    )
    defer timer.Close()
    if err := timer.Start(context.Background()); err != nil {
        log.Fatal(err)
    }

    if err := timer.SetEvent(time.Now().Add(-time.Second), &OrderTimeout{OrderID: "ok"}); err != nil {
        log.Fatal(err)
    }

    if err := timer.SetEvent(time.Now(), &DemoTimeout{Time: time.Now().Unix()}); err != nil {
        log.Fatal(err)
    }

    select {
    case p := <-handled:
        fmt.Printf("handled order_timeout: %s\n", p.OrderID)
    case <-time.After(5 * time.Second):
        log.Fatal("timeout waiting for handler")
    }
}

type DemoTimeoutHandler struct {
}

func (h *DemoTimeoutHandler) Event() delaytimer.Event {
    return "demo_timeout"
}

// NewParams 复制参数
func (h *DemoTimeoutHandler) NewParams() delaytimer.Params {
    return &DemoTimeout{}
}

// Handle 处理程序
func (h *DemoTimeoutHandler) Handle(ctx context.Context, p delaytimer.Params) error {
    t, ok := p.(*DemoTimeout)
    if ok {
        fmt.Printf("time: %d\n", t.Time)
    }
    return nil
}
