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

func TestSubscribeNilSafe(t *testing.T) {
	Subscribe(nil, nil)
	timer := New(&fakeStore{}, WithLogger(silentLogger()))
	t.Cleanup(timer.Close)
	Subscribe(timer, nil)
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
	waitUntil(t, 2*time.Second, func() bool {
		scheduled, _, _, _ := store.snapshot()
		return len(scheduled) == 1
	})
	scheduled, _, _, _ := store.snapshot()
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
	waitUntil(t, 2*time.Second, func() bool {
		_, canceled, _, _ := store.snapshot()
		return len(canceled) == 1
	})
	_, canceled, _, _ := store.snapshot()
	if canceled[0] != wantKey {
		t.Fatalf("canceled=%v", canceled)
	}
}
