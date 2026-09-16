package delaytimer

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
)

func TestBusNilParams(t *testing.T) {
	b := NewBus()
	if err := b.SetEvent(time.Now(), nil); !errors.Is(err, ErrNilParam) {
		t.Fatalf("set=%v", err)
	}
	if err := b.DelEvent(nil); !errors.Is(err, ErrNilParam) {
		t.Fatalf("del=%v", err)
	}
}

func TestEventChannelClosed(t *testing.T) {
	ch := NewEventChannel()
	ch.Close()
	if err := ch.Publish("x"); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("err=%v", err)
	}
}

func TestEventChannelPublishFailed(t *testing.T) {
	ch := newEventChannel(1, 20*time.Millisecond)
	if err := ch.Publish("a"); err != nil {
		t.Fatal(err)
	}
	if err := ch.Publish("b"); !errors.Is(err, ErrPublishFailed) {
		t.Fatalf("err=%v", err)
	}
}

func TestEventChannelCloseDoesNotWaitForPublish(t *testing.T) {
	ch := newEventChannel(1, 2*time.Second)
	if err := ch.Publish("a"); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	go func() {
		close(started)
		_ = ch.Publish("b")
	}()
	<-started
	time.Sleep(30 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		ch.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Close blocked behind Publish")
	}
}

func TestSubscribeNilSafe(t *testing.T) {
	subscribe(nil, nil)
	timer := New(&fakeStore{}, WithLogger(silentLogger()))
	t.Cleanup(timer.Close)
	subscribe(timer, nil)
}

func TestBusWithoutSubscribeDoesNotWriteStore(t *testing.T) {
	store := &fakeStore{}
	bus := NewBus()
	timer := New(store, WithLogger(silentLogger()))
	t.Cleanup(timer.Close)
	if err := bus.SetEvent(time.Now(), &sampleParams{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	scheduled, _, _, _ := store.snapshot()
	if len(scheduled) != 0 {
		t.Fatalf("bus without WithBus must not write store, got %d", len(scheduled))
	}
}

type routeBus struct {
	setN int
	delN int
}

func (b *routeBus) SetEvents() *EventChannel { return nil }
func (b *routeBus) DelEvents() *EventChannel { return nil }
func (b *routeBus) SetEvent(time.Time, Params) error {
	b.setN++
	return nil
}
func (b *routeBus) DelEvent(Params) error {
	b.delN++
	return nil
}

func TestTimerSetEventUsesBusWhenConfigured(t *testing.T) {
	store := &fakeStore{}
	bus := &routeBus{}
	timer := New(store, WithBus(bus), WithLogger(silentLogger()))
	t.Cleanup(timer.Close)
	p := &sampleParams{ID: "1"}
	if err := timer.SetEvent(time.Unix(1000, 0), p); err != nil {
		t.Fatal(err)
	}
	if err := timer.DelEvent(p); err != nil {
		t.Fatal(err)
	}
	if bus.setN != 1 || bus.delN != 1 {
		t.Fatalf("set=%d del=%d", bus.setN, bus.delN)
	}
	scheduled, canceled, _, _ := store.snapshot()
	if len(scheduled) != 0 || len(canceled) != 0 {
		t.Fatalf("with bus, SetEvent/DelEvent must not write store directly: scheduled=%d canceled=%d", len(scheduled), len(canceled))
	}
}

func TestWithBusCloseWaitsForPersist(t *testing.T) {
	store := &slowCloseStore{delay: 40 * time.Millisecond}
	bus := NewBus()
	timer := New(store, WithBus(bus), WithLogger(silentLogger()))
	if err := timer.SetEvent(time.Unix(1000, 0), &sampleParams{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	timer.Close()
	scheduled, _, _, _ := store.snapshot()
	if len(scheduled) != 1 {
		t.Fatalf("Close returned before bus persist, scheduled=%d", len(scheduled))
	}
	if !store.closedAfterSchedule() {
		t.Fatal("store.Close ran before bus Schedule finished")
	}
}

type slowCloseStore struct {
	fakeStore
	delay time.Duration
	order []string
}

func (s *slowCloseStore) Schedule(ctx context.Context, task Task) error {
	time.Sleep(s.delay)
	s.mu.Lock()
	s.order = append(s.order, "schedule")
	s.mu.Unlock()
	return s.fakeStore.Schedule(ctx, task)
}

func (s *slowCloseStore) Close() error {
	s.mu.Lock()
	s.order = append(s.order, "close")
	s.mu.Unlock()
	return s.fakeStore.Close()
}

func (s *slowCloseStore) closedAfterSchedule() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.order) < 2 {
		return false
	}
	return s.order[0] == "schedule" && s.order[1] == "close"
}

func TestWithBusSetEventWritesStoreEventually(t *testing.T) {
	store := &fakeStore{}
	bus := NewBus()
	timer := New(store, WithBus(bus), WithLogger(silentLogger()))
	t.Cleanup(timer.Close)
	at := time.Unix(1000, 0)
	p := &sampleParams{ID: "1"}
	if err := timer.SetEvent(at, p); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool {
		scheduled, _, _, _ := store.snapshot()
		return len(scheduled) == 1
	})
	payload, err := encodeParams(p)
	if err != nil {
		t.Fatal(err)
	}
	wantKey := encodeTaskKey("sample", payload)
	scheduled, _, _, _ := store.snapshot()
	if scheduled[0].Key != wantKey || scheduled[0].Kind != "sample" || scheduled[0].At != at {
		t.Fatalf("task=%+v", scheduled[0])
	}

	if err := timer.DelEvent(p); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool {
		_, canceled, _, _ := store.snapshot()
		return len(canceled) == 1
	})
	_, canceled, _, _ := store.snapshot()
	if canceled[0] != wantKey {
		t.Fatalf("canceled=%v", canceled)
	}
}
