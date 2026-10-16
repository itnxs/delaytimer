package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"gitlab.haochang.tv/gopkg/delaytimer"
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
	timer := delaytimer.Must(
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

	if err := timer.SetEvent(ctx, time.Now().Add(-time.Second), &OrderTimeout{OrderID: "ok"}); err != nil {
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
	if err := timer.SetEvent(ctx, time.Now().Add(time.Hour), skip); err != nil {
		t.Fatal(err)
	}
	if err := timer.DelEvent(ctx, skip); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-handled:
		t.Fatalf("canceled event should not run, got=%s", p.OrderID)
	case <-time.After(300 * time.Millisecond):
	}
}

type silentLog struct{}

func (silentLog) Info(context.Context, string, ...any)         {}
func (silentLog) Error(context.Context, string, error, ...any) {}

func silentLogger() delaytimer.Logger { return silentLog{} }
