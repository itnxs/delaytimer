package delaytimer

import (
	"bytes"
	"context"
	"github.com/pkg/errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestTimerRunDispatchesHandleAndAck(t *testing.T) {
	resetSink()
	b := &fakeBackend{toClaim: []Job{{
		Key: JobKey("answer_timeout", `{"play":"p1"}`), Kind: "answer_timeout", Payload: `{"play":"p1"}`, At: time.Now(),
	}}}
	tm := New(b, WithHandlers(timeoutHandler()), WithPollInterval(time.Millisecond), WithConcurrency(2), WithBatchSize(8))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tm.Run(ctx) }()

	waitUntil(t, time.Second, func() bool { return sink.count() == 1 })
	if sink.last() != "p1" {
		t.Fatalf("play %s", sink.last())
	}
	waitUntil(t, time.Second, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.acked) == 1
	})
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return")
	}
}

func TestTimerRunUndefinedEventFails(t *testing.T) {
	b := &fakeBackend{toClaim: []Job{{
		Key: "k", Kind: "missing", Payload: "p",
	}}}
	tm := New(b, WithHandlers(nopHandler("other")), WithPollInterval(time.Millisecond), WithFailPolicy(FailRequeue))

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = tm.Run(ctx) }()
	waitUntil(t, time.Second, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.failed) == 1 && len(b.acked) == 0
	})
	cancel()
}

func TestTimerRunCancelFailsUndispatchedJobs(t *testing.T) {
	g := withBlockGate()
	b := &fakeBackend{toClaim: []Job{
		{Key: "k1", Kind: "block", Payload: `{}`},
		{Key: "k2", Kind: "block", Payload: `{}`},
		{Key: "k3", Kind: "block", Payload: `{}`},
	}}
	tm := New(b, WithHandlers(blockHandler()),
		WithPollInterval(time.Millisecond), WithConcurrency(1), WithBatchSize(8))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tm.Run(ctx) }()

	select {
	case <-g.started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	close(g.hold)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.failed) < 2 {
		t.Fatalf("undispatched jobs should Fail, failed=%d acked=%d", len(b.failed), len(b.acked))
	}
}

func TestTimerRunHandleErrorCallsFail(t *testing.T) {
	b := &fakeBackend{toClaim: []Job{{
		Key: "k", Kind: "answer_timeout", Payload: `{"play":"p"}`,
	}}}
	tm := New(b, WithHandlers(failHandler()), WithPollInterval(time.Millisecond), WithFailPolicy(FailRequeue))

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = tm.Run(ctx) }()
	waitUntil(t, time.Second, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.failed) == 1
	})
	cancel()
}

func TestTimerRunHandlerPanicCallsFail(t *testing.T) {
	b := &fakeBackend{toClaim: []Job{{
		Key: "k", Kind: "boom", Payload: `{}`,
	}}}
	h := Bind(&stubEvent{name: "boom"}, func(context.Context, *stubEvent) error {
		panic("handler boom")
	})
	tm := New(b, WithHandlers(h), WithPollInterval(time.Millisecond), WithFailPolicy(FailRequeue))

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = tm.Run(ctx) }()
	waitUntil(t, time.Second, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.failed) == 1
	})
	cancel()
}

func TestTimerRunUnmarshalErrorCallsFail(t *testing.T) {
	b := &fakeBackend{toClaim: []Job{{
		Key: "k", Kind: "answer_timeout", Payload: "not-json",
	}}}
	tm := New(b, WithHandlers(timeoutHandler()), WithPollInterval(time.Millisecond), WithFailPolicy(FailRequeue))

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = tm.Run(ctx) }()
	waitUntil(t, time.Second, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.failed) == 1
	})
	cancel()
}

func TestTimerRunHandleErrorAcksByDefault(t *testing.T) {
	b := &fakeBackend{toClaim: []Job{{
		Key: "k", Kind: "answer_timeout", Payload: `{"play":"p"}`,
	}}}
	tm := New(b, WithHandlers(failHandler()), WithPollInterval(time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = tm.Run(ctx) }()
	waitUntil(t, time.Second, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.acked) == 1 && len(b.failed) == 0
	})
	cancel()
}

func TestTimerRunContextCancel(t *testing.T) {
	b := &fakeBackend{}
	tm := New(b, WithHandlers(nopHandler("a")), WithPollInterval(time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tm.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return")
	}
}

func TestNewPanicsOnDuplicateEventName(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	New(&fakeBackend{}, WithHandlers(nopHandler("a"), nopHandler("a")))
}

func TestWithLogLevelAppliesToLogger(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	_ = New(&fakeBackend{}, WithHandlers(nopHandler("a")), WithLogger(logger), WithLogLevel(logrus.DebugLevel))
	if logger.GetLevel() != logrus.DebugLevel {
		t.Fatalf("level %s", logger.GetLevel())
	}
}

func TestSetEventWritesInfoLog(t *testing.T) {
	logger := logrus.New()
	buf := &syncBuffer{}
	logger.SetOutput(buf)
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	b := &fakeBackend{}
	tm := New(b, WithLogger(logger))
	bus := NewBus()
	defer bus.SetEvents().Close()
	defer bus.DelEvents().Close()
	Subscribe(tm, bus)

	if err := bus.SetEvent(time.Unix(1, 0), &timeoutEvent{Play: "p1"}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool {
		return len(b.scheduledJobs()) == 1 && strings.Contains(buf.String(), "set timer event")
	})
}

func TestHandleEventWritesInfoLog(t *testing.T) {
	resetSink()
	logger := logrus.New()
	buf := &syncBuffer{}
	logger.SetOutput(buf)
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	b := &fakeBackend{toClaim: []Job{{
		Kind: "answer_timeout", Payload: `{"play":"p1"}`,
	}}}
	tm := New(b, WithHandlers(timeoutHandler()), WithLogger(logger), WithPollInterval(time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = tm.Run(ctx) }()
	waitUntil(t, time.Second, func() bool {
		return sink.count() == 1 && strings.Contains(buf.String(), "handle timer event")
	})
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
