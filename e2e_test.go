package delaytimer

import (
	"context"
	"testing"
	"time"
)

func TestEndToEndSetEventMemoryRun(t *testing.T) {
	resetSink()
	mem := NewMemory()
	tm := New(mem, WithHandlers(timeoutHandler()), WithPollInterval(time.Millisecond), WithConcurrency(2))
	bus := NewBus()
	defer bus.SetEvents().Close()
	defer bus.DelEvents().Close()
	Subscribe(tm, bus)

	if err := bus.SetEvent(time.Now().Add(-time.Second), &timeoutEvent{Play: "play-1"}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = tm.Run(ctx) }()

	waitUntil(t, 2*time.Second, func() bool { return sink.count() == 1 })
	if sink.last() != "play-1" {
		t.Fatalf("play %s", sink.last())
	}
}

func TestEndToEndDelEventCancelsMemoryJob(t *testing.T) {
	resetSink()
	mem := NewMemory()
	tm := New(mem, WithHandlers(timeoutHandler()), WithPollInterval(time.Millisecond))
	bus := NewBus()
	defer bus.SetEvents().Close()
	defer bus.DelEvents().Close()
	Subscribe(tm, bus)

	p := &timeoutEvent{Play: "play-1"}
	if err := bus.SetEvent(time.Now().Add(time.Hour), p); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool {
		mem.mu.Lock()
		n := len(mem.byKey)
		mem.mu.Unlock()
		return n == 1
	})

	if err := bus.DelEvent(p); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool {
		mem.mu.Lock()
		n := len(mem.byKey)
		mem.mu.Unlock()
		return n == 0
	})

	if sink.count() != 0 {
		t.Fatal("handler should not run")
	}
}
