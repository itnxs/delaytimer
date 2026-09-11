package delaytimer

import (
	"context"
	"github.com/pkg/errors"
	"testing"
	"time"
)

func TestEventChannelPublishThenObservableReceives(t *testing.T) {
	ch := NewEventChannel()
	defer ch.Close()

	got := make(chan any, 1)
	ch.Observable().ForEach(
		func(i interface{}) { got <- i },
		func(error) {},
		func() {},
	)
	if err := ch.Publish("hello"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case v := <-got:
		if v != "hello" {
			t.Fatalf("got %v", v)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting observable")
	}
}

func TestEventChannelPublishAfterClose(t *testing.T) {
	ch := NewEventChannel()
	ch.Close()
	err := ch.Publish("x")
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("got %v", err)
	}
}

func TestEventChannelPublishTimeout(t *testing.T) {
	ch := newEventChannel(1)
	if err := ch.Publish("full"); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ok, err := ch.PublishItem(ctx, "blocked")
	if err != nil {
		t.Fatalf("PublishItem: %v", err)
	}
	if ok {
		t.Fatal("expected send to fail on timeout")
	}
}

func TestNewBusWithChannelSize(t *testing.T) {
	bus := NewBus(WithChannelSize(1))
	defer bus.SetEvents().Close()
	defer bus.DelEvents().Close()
	if err := bus.SetEvent(time.Unix(1, 0), &timeoutEvent{Play: "a"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ok, err := bus.SetEvents().PublishItem(ctx, &SetEventParam{Time: time.Unix(2, 0), Params: &timeoutEvent{Play: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected second publish to block on size 1")
	}
}

func TestBusSetAndDelPublishToChannels(t *testing.T) {
	bus := NewBus()
	defer bus.SetEvents().Close()
	defer bus.DelEvents().Close()

	setCh := make(chan *SetEventParam, 1)
	bus.SetEvents().Observable().ForEach(
		func(i interface{}) { setCh <- i.(*SetEventParam) },
		func(error) {},
		func() {},
	)
	delCh := make(chan *DelEventParam, 1)
	bus.DelEvents().Observable().ForEach(
		func(i interface{}) { delCh <- i.(*DelEventParam) },
		func(error) {},
		func() {},
	)

	p := &stubEvent{name: "answer_timeout"}
	if err := bus.SetEvent(time.Unix(10, 0), p); err != nil {
		t.Fatal(err)
	}
	if err := bus.DelEvent(p); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-setCh:
		if got.Params.EventName() != "answer_timeout" {
			t.Fatalf("name %s", got.Params.EventName())
		}
	case <-time.After(time.Second):
		t.Fatal("timeout set")
	}
	select {
	case got := <-delCh:
		if got.Params.EventName() != "answer_timeout" {
			t.Fatalf("name %s", got.Params.EventName())
		}
	case <-time.After(time.Second):
		t.Fatal("timeout del")
	}
}

func TestSubscribeSetEventSchedulesJob(t *testing.T) {
	b := &fakeBackend{}
	tm := New(b)
	bus := NewBus()
	defer bus.SetEvents().Close()
	defer bus.DelEvents().Close()
	Subscribe(tm, bus)

	at := time.Unix(50, 0)
	if err := bus.SetEvent(at, &timeoutEvent{Play: "p1"}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool { return len(b.scheduledJobs()) == 1 })
	job := b.scheduledJobs()[0]
	wantPayload := `{"play":"p1"}`
	if job.Kind != "answer_timeout" || job.Payload != wantPayload {
		t.Fatalf("job %+v", job)
	}
	if job.Key != JobKey("answer_timeout", wantPayload) {
		t.Fatalf("key %s", job.Key)
	}
	if !job.At.Equal(at) {
		t.Fatalf("at %v", job.At)
	}
}

func TestSubscribeDelEventCancelsJob(t *testing.T) {
	b := &fakeBackend{}
	tm := New(b)
	bus := NewBus()
	defer bus.SetEvents().Close()
	defer bus.DelEvents().Close()
	Subscribe(tm, bus)

	p := &timeoutEvent{Play: "p1"}
	if err := bus.DelEvent(p); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool { return len(b.canceledKeys()) == 1 })
	want := JobKey("answer_timeout", `{"play":"p1"}`)
	if b.canceledKeys()[0] != want {
		t.Fatalf("got %s", b.canceledKeys()[0])
	}
}

func TestSubscribeMarshalErrorDoesNotSchedule(t *testing.T) {
	b := &fakeBackend{}
	tm := New(b)
	bus := NewBus()
	defer bus.SetEvents().Close()
	defer bus.DelEvents().Close()
	Subscribe(tm, bus)

	if err := bus.SetEvent(time.Now(), &badJSONEvent{Ch: make(chan int)}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if len(b.scheduledJobs()) != 0 {
		t.Fatalf("scheduled %v", b.scheduledJobs())
	}
}

func TestSubscribeSchedulesWithoutHandler(t *testing.T) {
	b := &fakeBackend{}
	tm := New(b)
	bus := NewBus()
	defer bus.SetEvents().Close()
	defer bus.DelEvents().Close()
	Subscribe(tm, bus)

	if err := bus.SetEvent(time.Now(), &stubEvent{name: "answer_timeout"}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool { return len(b.scheduledJobs()) == 1 })
}

func TestSubscribeCancelUnsupportedIsIgnored(t *testing.T) {
	b := &fakeBackend{cancelErr: ErrCancelUnsupported}
	tm := New(b)
	bus := NewBus()
	defer bus.SetEvents().Close()
	defer bus.DelEvents().Close()
	Subscribe(tm, bus)

	if err := bus.DelEvent(&timeoutEvent{Play: "x"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
}

type panicNameEvent struct{}

func (*panicNameEvent) EventName() EventName { panic("event name") }

func TestSubscribeRecoversEventNamePanic(t *testing.T) {
	tm := New(&fakeBackend{})
	bus := NewBus()
	defer bus.SetEvents().Close()
	defer bus.DelEvents().Close()
	Subscribe(tm, bus)

	if err := bus.SetEvent(time.Now(), &panicNameEvent{}); err != nil {
		t.Fatal(err)
	}
	if err := bus.DelEvent(&panicNameEvent{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
}

type blockingScheduleBackend struct {
	fakeBackend
	started chan struct{}
	block   chan struct{}
}

func (b *blockingScheduleBackend) Schedule(ctx context.Context, job Job) error {
	select {
	case b.started <- struct{}{}:
	default:
	}
	<-b.block
	return b.fakeBackend.Schedule(ctx, job)
}

func TestSubscribeSlowScheduleDoesNotBlockLaterPublish(t *testing.T) {
	b := &blockingScheduleBackend{
		started: make(chan struct{}, 1),
		block:   make(chan struct{}),
	}
	tm := New(b)
	bus := NewBus()
	defer bus.SetEvents().Close()
	defer bus.DelEvents().Close()
	Subscribe(tm, bus)

	if err := bus.SetEvent(time.Now(), &timeoutEvent{Play: "a"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-b.started:
	case <-time.After(time.Second):
		t.Fatal("first schedule did not start")
	}

	done := make(chan error, 1)
	go func() {
		done <- bus.SetEvent(time.Now(), &timeoutEvent{Play: "b"})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second SetEvent: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second SetEvent blocked by in-flight schedule")
	}

	if err := bus.DelEvent(&timeoutEvent{Play: "c"}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool { return len(b.canceledKeys()) == 1 })

	close(b.block)
	waitUntil(t, time.Second, func() bool { return len(b.scheduledJobs()) >= 1 })
}
