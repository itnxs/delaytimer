package delaytimer

import (
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

func TestSubscribeSetAndDel(t *testing.T) {
	store := &fakeStore{}
	timer := New(store, WithLogger(silentLogger()))
	t.Cleanup(timer.Close)
	at := time.Unix(1000, 0)
	p := &sampleParams{ID: "1"}
	if err := timer.SetEvent(at, p); err != nil {
		t.Fatal(err)
	}
	scheduled, _, _, _ := store.snapshot()
	if len(scheduled) != 1 {
		t.Fatalf("expected sync schedule, got %d", len(scheduled))
	}
	payload, err := encodeParams(p)
	if err != nil {
		t.Fatal(err)
	}
	wantKey := "sample:" + payload
	if scheduled[0].Key != wantKey || scheduled[0].Kind != "sample" || scheduled[0].At != at {
		t.Fatalf("task=%+v", scheduled[0])
	}

	if err := timer.DelEvent(p); err != nil {
		t.Fatal(err)
	}
	_, canceled, _, _ := store.snapshot()
	if len(canceled) != 1 || canceled[0] != wantKey {
		t.Fatalf("canceled=%v", canceled)
	}
}
