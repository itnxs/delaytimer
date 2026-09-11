package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/itnxs/delaytimer"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
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
	timer := delaytimer.New(
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

	if err := timer.SetEvent(time.Now().Add(-time.Second), &OrderTimeout{OrderID: "ok"}); err != nil {
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

	if err := timer.DelEvent(&OrderTimeout{OrderID: "skip"}); !errors.Is(err, delaytimer.ErrCancelUnsupported) {
		t.Fatalf("del=%v", err)
	}
}

func silentLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}
