package delaytimer

import (
	"context"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := &timeoutEvent{Play: "p1"}
	payload, err := encodeParams(in)
	if err != nil {
		t.Fatal(err)
	}
	if payload != `{"play":"p1"}` {
		t.Fatalf("payload %s", payload)
	}
	out := &timeoutEvent{}
	if err := decodeParams(payload, out); err != nil {
		t.Fatal(err)
	}
	if out.Play != "p1" {
		t.Fatalf("play %s", out.Play)
	}
}

func TestEncodeParamsFieldOrder(t *testing.T) {
	in := &orderEvent{Z: "1", A: "2"}
	payload, err := encodeParams(in)
	if err != nil {
		t.Fatal(err)
	}
	if payload != `{"z":"1","a":"2"}` {
		t.Fatalf("want struct field order, got %s", payload)
	}
}

func TestEncodeParamsChanFails(t *testing.T) {
	_, err := encodeParams(&badJSONEvent{Ch: make(chan int)})
	if err == nil {
		t.Fatal("expected marshal error")
	}
}

func TestDecodeParamsInvalidJSON(t *testing.T) {
	out := &timeoutEvent{}
	if err := decodeParams("not-json", out); err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestBindHandleReceivesDecodedParams(t *testing.T) {
	var got string
	h := Bind(&timeoutEvent{}, func(_ context.Context, p *timeoutEvent) error {
		got = p.Play
		return nil
	})
	if h.EventName() != "answer_timeout" {
		t.Fatalf("name %s", h.EventName())
	}
	p := h.NewParams()
	if err := decodeParams(`{"play":"p1"}`, p); err != nil {
		t.Fatal(err)
	}
	if err := h.Handle(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if got != "p1" {
		t.Fatalf("got %s", got)
	}
}

func TestBindHandleTypeMismatch(t *testing.T) {
	h := Bind(&timeoutEvent{}, func(context.Context, *timeoutEvent) error { return nil })
	if err := h.Handle(context.Background(), &stubEvent{name: "x"}); err == nil {
		t.Fatal("expected type mismatch")
	}
}

func TestBindPanicsOnEmptyEventName(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = Bind(&stubEvent{name: ""}, func(context.Context, *stubEvent) error { return nil })
}

func TestBindPanicsOnNonPointerParams(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = Bind(valueEvent{}, func(context.Context, valueEvent) error { return nil })
}
