package delaytimer

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
)

func TestNewNilStore(t *testing.T) {
	expectPanicIs(t, ErrNilTaskStore, func() { New(nil) })
}

func TestBindHandlersPanics(t *testing.T) {
	store := &fakeStore{}
	t.Run("nilHandler", func(t *testing.T) {
		expectPanicIs(t, ErrNilEventHandler, func() {
			New(store, WithHandlers(nil), WithLogger(silentLogger()))
		})
	})
	t.Run("emptyEvent", func(t *testing.T) {
		expectPanicIs(t, ErrEmptyEvent, func() {
			New(store, WithHandlers(emptyEventHandler{}), WithLogger(silentLogger()))
		})
	})
	t.Run("duplicate", func(t *testing.T) {
		h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error { return nil })
		expectPanicIs(t, ErrDuplicateEvent, func() {
			New(store, WithHandlers(h, h), WithLogger(silentLogger()))
		})
	})
}

type emptyEventHandler struct{}

func (emptyEventHandler) Event() Event                         { return "" }
func (emptyEventHandler) NewParams() Params                    { return &sampleParams{} }
func (emptyEventHandler) Handle(context.Context, Params) error { return nil }

func TestTimerRunAcksOnSuccess(t *testing.T) {
	task := sampleTaskAt(time.Unix(1, 0))
	store := &fakeStore{claims: []Task{task}}
	handled := make(chan struct{}, 1)
	h := Bind(&sampleParams{}, func(_ context.Context, p *sampleParams) error {
		if p.ID != "1" {
			t.Errorf("id=%s", p.ID)
		}
		handled <- struct{}{}
		return nil
	})
	timer := New(store, WithHandlers(h), WithLogger(silentLogger()), WithPollInterval(time.Millisecond), WithConcurrency(1))
	t.Cleanup(timer.Close)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- timer.Run(ctx) }()

	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatal("handler not called")
	}
	waitUntil(t, time.Second, func() bool {
		_, _, ackN, failN := store.snapshot()
		return ackN == 1 && failN == 0
	})
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestTimerRunHandleErrorDoesNotFail(t *testing.T) {
	task := sampleTaskAt(time.Unix(1, 0))
	store := &fakeStore{claims: []Task{task}}
	handled := make(chan struct{}, 1)
	h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error {
		handled <- struct{}{}
		return errors.New("boom")
	})
	timer := New(store, WithHandlers(h), WithLogger(silentLogger()), WithPollInterval(time.Millisecond))
	t.Cleanup(timer.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = timer.Run(ctx) }()

	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatal("handler not called")
	}
	time.Sleep(50 * time.Millisecond)
	_, _, ackN, failN := store.snapshot()
	if ackN != 0 || failN != 0 {
		t.Fatalf("current dispatch does not Fail/Ack on handler error: ack=%d fail=%d", ackN, failN)
	}
}

func TestTimerRunUnknownKind(t *testing.T) {
	store := &fakeStore{claims: []Task{{Key: "x", Kind: "missing", Payload: "{}", At: time.Unix(1, 0)}}}
	timer := New(store, WithLogger(silentLogger()), WithPollInterval(time.Millisecond))
	t.Cleanup(timer.Close)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- timer.Run(ctx) }()
	waitUntil(t, time.Second, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return len(store.claims) == 0
	})
	_, _, ackN, failN := store.snapshot()
	if ackN != 0 || failN != 0 {
		t.Fatalf("ack=%d fail=%d", ackN, failN)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestTimerSetEventAndDelEvent(t *testing.T) {
	handled := make(chan *sampleParams, 4)
	h := Bind(&sampleParams{}, func(_ context.Context, p *sampleParams) error {
		handled <- p
		return nil
	})
	timer := New(NewMemory(), WithHandlers(h), WithLogger(silentLogger()), WithPollInterval(10*time.Millisecond))
	t.Cleanup(timer.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = timer.Run(ctx) }()

	if err := timer.SetEvent(time.Now().Add(-time.Second), &sampleParams{ID: "ok"}); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-handled:
		if p.ID != "ok" {
			t.Fatalf("id=%s", p.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("due event was not handled")
	}

	skip := &sampleParams{ID: "skip"}
	if err := timer.SetEvent(time.Now().Add(time.Hour), skip); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := timer.DelEvent(skip); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-handled:
		t.Fatalf("canceled event should not run, got=%s", p.ID)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestTimerSetEventNilParams(t *testing.T) {
	timer := New(&fakeStore{}, WithLogger(silentLogger()))
	t.Cleanup(timer.Close)
	if err := timer.SetEvent(time.Now(), nil); !errors.Is(err, ErrNilParam) {
		t.Fatalf("set=%v", err)
	}
	if err := timer.DelEvent(nil); !errors.Is(err, ErrNilParam) {
		t.Fatalf("del=%v", err)
	}
}
