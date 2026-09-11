package delaytimer

import (
	"context"
	"testing"
)

func TestBindHandleRoundTrip(t *testing.T) {
	var got *sampleParams
	h := Bind(&sampleParams{ID: "proto"}, func(_ context.Context, p *sampleParams) error {
		got = p
		return nil
	})
	if h.Event() != "sample" {
		t.Fatalf("event=%s", h.Event())
	}
	inst := h.NewParams()
	if inst == nil || inst == h.(*boundHandler[*sampleParams]).proto {
		t.Fatal("NewParams should clone")
	}
	if err := decodeParams(`{"id":"9"}`, inst); err != nil {
		t.Fatal(err)
	}
	if err := h.Handle(context.Background(), inst); err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ID != "9" {
		t.Fatalf("got=%+v", got)
	}
}

func TestBindPanics(t *testing.T) {
	t.Run("nilHandler", func(t *testing.T) {
		expectPanicIs(t, ErrNilEventHandler, func() {
			Bind(&sampleParams{}, nil)
		})
	})
	t.Run("emptyEvent", func(t *testing.T) {
		expectPanicIs(t, ErrEmptyEvent, func() {
			Bind(&emptyEventParams{}, func(context.Context, *emptyEventParams) error { return nil })
		})
	})
}

func TestEncodeDecodeParams(t *testing.T) {
	p := &sampleParams{ID: "abc"}
	payload, err := encodeParams(p)
	if err != nil {
		t.Fatal(err)
	}
	out := &sampleParams{}
	if err := decodeParams(payload, out); err != nil {
		t.Fatal(err)
	}
	if out.ID != "abc" {
		t.Fatalf("id=%s", out.ID)
	}
	if err := decodeParams("not-json", &sampleParams{}); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestEncodeParamsFieldOrderStable(t *testing.T) {
	p := &orderedParams{A: "1", B: "2"}
	first, err := encodeParams(p)
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeParams(p)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("%s vs %s", first, second)
	}
	if first != `{"a":"1","b":"2"}` {
		t.Fatalf("payload=%s", first)
	}
}

func TestHandleTypeMismatch(t *testing.T) {
	h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error { return nil })
	err := h.Handle(context.Background(), &orderedParams{})
	if err == nil {
		t.Fatal("expected type mismatch")
	}
}

func TestEventHelpers(t *testing.T) {
	if !Event("").IsNil() {
		t.Fatal("empty should be nil")
	}
	if Event("x").IsNil() || Event("x").String() != "x" {
		t.Fatal("event string")
	}
}

func TestEncodeDecodeTaskKey(t *testing.T) {
	kind, payload := "order:timeout|v2", `{"a":"b|c","n":1}`
	key := encodeTaskKey(kind, payload)
	if key != kind+taskKeySep+payload {
		t.Fatalf("key=%q", key)
	}
	k, p := decodeTaskKey(key)
	if k != kind || p != payload {
		t.Fatalf("kind=%q payload=%q", k, p)
	}
}
