package main

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/itnxs/delaytimer"
	"github.com/sirupsen/logrus"
)

func TestMemoryEventTimer(t *testing.T) {
	handled := make(chan *OrderTimeout, 4)
	timer := delaytimer.New(delaytimer.NewMemory(),
		delaytimer.WithHandlers(delaytimer.Bind(&OrderTimeout{}, func(_ context.Context, p *OrderTimeout) error {
			handled <- p
			return nil
		})),
		delaytimer.WithLogger(silentLogger()),
		delaytimer.WithPollInterval(10*time.Millisecond),
	)
	defer timer.Close()
	if err := timer.Start(context.Background()); err != nil {
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
