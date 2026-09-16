package main

import (
    "context"
    "fmt"
    "io"
    "os"
    "testing"
    "time"

    "github.com/itnxs/delaytimer"
    "github.com/redis/go-redis/v9"
    "github.com/sirupsen/logrus"
)

func TestRedisEventTimer(t *testing.T) {
    addr := os.Getenv("REDIS_ADDR")
    if addr == "" {
        addr = "127.0.0.1:6379"
    }
    rdb := redis.NewClient(&redis.Options{Addr: addr})
    t.Cleanup(func() { _ = rdb.Close() })
    ctx := context.Background()
    if err := rdb.Ping(ctx).Err(); err != nil {
        t.Skip(err)
    }

    key := fmt.Sprintf("delaytimer:example:%d", time.Now().UnixNano())
    t.Cleanup(func() { _ = rdb.Del(ctx, key).Err() })

    handled := make(chan *OrderTimeout, 4)
    timer := delaytimer.New(
        delaytimer.NewRedis(rdb, key),
        delaytimer.WithHandlers(delaytimer.Bind(&OrderTimeout{}, func(_ context.Context, p *OrderTimeout) error {
            handled <- p
            return nil
        })),
        delaytimer.WithLogger(silentLogger()),
        delaytimer.WithPollInterval(20*time.Millisecond),
    )
    defer timer.Close()
    if err := timer.Start(ctx); err != nil {
        t.Fatal(err)
    }

    if err := timer.SetEvent(time.Now().Add(-time.Second), &OrderTimeout{OrderID: "ok"}); err != nil {
        t.Fatal(err)
    }
    select {
    case p := <-handled:
        if p.OrderID != "ok" {
            t.Fatalf("got=%s", p.OrderID)
        }
    case <-time.After(2 * time.Second):
        t.Fatal("due event was not handled")
    }

    skip := &OrderTimeout{OrderID: "skip"}
    if err := timer.SetEvent(time.Now().Add(time.Hour), skip); err != nil {
        t.Fatal(err)
    }
    if err := timer.DelEvent(skip); err != nil {
        t.Fatal(err)
    }
    select {
    case p := <-handled:
        t.Fatalf("canceled event should not run, got=%s", p.OrderID)
    case <-time.After(300 * time.Millisecond):
    }
}

func silentLogger() logrus.FieldLogger {
    l := logrus.New()
    l.SetOutput(io.Discard)
    return l
}
