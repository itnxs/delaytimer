package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"gitlab.haochang.tv/gopkg/delaytimer"
)

func TestAMQPEventTimer(t *testing.T) {
	url := os.Getenv("AMQP_URL")
	if url == "" {
		url = "amqp://guest:guest@127.0.0.1:5672/"
	}
	conn, err := amqp.DialConfig(url, amqp.Config{
		Dial: amqp.DefaultDial(2 * time.Second),
	})
	if err != nil {
		t.Skip(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	name := fmt.Sprintf("delaytimer.ex.%d", time.Now().UnixNano())
	handled := make(chan *OrderTimeout, 4)
	timer := delaytimer.Must(
		delaytimer.NewAMQP(conn, delaytimer.AMQPConfig{
			Exchange:   name,
			RoutingKey: name,
			Queue:      name,
		}),
		delaytimer.WithHandlers(delaytimer.Bind(&OrderTimeout{}, func(_ context.Context, p *OrderTimeout) error {
			handled <- p
			return nil
		})),
		delaytimer.WithLogger(silentLogger()),
	)
	defer timer.Close()
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := timer.SetEvent(context.Background(), time.Now().Add(-time.Second), &OrderTimeout{OrderID: "ok"}); err != nil {
		t.Skip(err)
	}
	select {
	case p := <-handled:
		if p.OrderID != "ok" {
			t.Fatalf("got=%s", p.OrderID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("due event was not handled")
	}

	if err := timer.DelEvent(context.Background(), &OrderTimeout{OrderID: "skip"}); !errors.Is(err, delaytimer.ErrCancelUnsupported) {
		t.Fatalf("del=%v", err)
	}
}

type silentLog struct{}

func (silentLog) Info(context.Context, string, ...any)         {}
func (silentLog) Error(context.Context, string, error, ...any) {}

func silentLogger() delaytimer.Logger { return silentLog{} }
