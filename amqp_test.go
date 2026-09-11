package delaytimer

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeAMQPChannel struct {
	mu         sync.Mutex
	pubs       []amqpPublishRec
	deliveries chan amqp.Delivery
	consumed   []string
}

type amqpPublishRec struct {
	exchange string
	key      string
	msg      amqp.Publishing
}

func (c *fakeAMQPChannel) PublishWithContext(_ context.Context, exchange, key string, _, _ bool, msg amqp.Publishing) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	msg.Body = append([]byte(nil), msg.Body...)
	c.pubs = append(c.pubs, amqpPublishRec{exchange: exchange, key: key, msg: msg})
	return nil
}

func (c *fakeAMQPChannel) Consume(queue string, _ string, _, _, _, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumed = append(c.consumed, queue)
	return c.deliveries, nil
}

type fakeAcknowledger struct {
	mu      sync.Mutex
	acked   bool
	nacked  bool
	requeue bool
}

func (a *fakeAcknowledger) Ack(uint64, bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acked = true
	return nil
}

func (a *fakeAcknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nacked = true
	a.requeue = requeue
	return nil
}

func (a *fakeAcknowledger) Reject(uint64, bool) error { return nil }

func testAMQPConfig() AMQPConfig {
	return AMQPConfig{
		Exchange:   "delay.ex",
		RoutingKey: "delay.rk",
		Queue:      "delay.q",
	}
}

func TestAMQPSchedulePublishesToRoutingKey(t *testing.T) {
	ch := &fakeAMQPChannel{}
	now := time.UnixMilli(1_700_000_000_000)
	a := NewAMQP(ch, testAMQPConfig(), WithAMQPClock(func() time.Time { return now }))

	task := Task{Key: "order:1", Kind: "order", Payload: `{"id":1}`, At: now.Add(5 * time.Second)}
	if err := a.Schedule(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if len(ch.pubs) != 1 {
		t.Fatalf("pubs=%d", len(ch.pubs))
	}
	rec := ch.pubs[0]
	if rec.exchange != "delay.ex" || rec.key != "delay.rk" {
		t.Fatalf("route=%s/%s", rec.exchange, rec.key)
	}
	delay, _ := rec.msg.Headers["x-delay"].(int64)
	if delay != 5000 {
		t.Fatalf("x-delay=%v", rec.msg.Headers["x-delay"])
	}

	past := Task{Key: "order:2", Kind: "order", Payload: "{}", At: now.Add(-time.Second)}
	if err := a.Schedule(context.Background(), past); err != nil {
		t.Fatal(err)
	}
	delay, _ = ch.pubs[1].msg.Headers["x-delay"].(int64)
	if delay != 0 {
		t.Fatalf("past x-delay=%v", ch.pubs[1].msg.Headers["x-delay"])
	}
	var msg amqpMessage
	if err := json.Unmarshal(rec.msg.Body, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.Key != task.Key || msg.Kind != task.Kind || msg.Payload != task.Payload {
		t.Fatalf("msg=%+v", msg)
	}
}

func TestAMQPCancelUnsupported(t *testing.T) {
	a := NewAMQP(&fakeAMQPChannel{}, testAMQPConfig())
	if err := a.Cancel(context.Background(), "k"); !errors.Is(err, ErrCancelUnsupported) {
		t.Fatalf("err=%v", err)
	}
}

func TestAMQPNilChannel(t *testing.T) {
	a := NewAMQP(nil, testAMQPConfig())
	if err := a.Schedule(context.Background(), Task{}); !errors.Is(err, ErrPublishFailed) {
		t.Fatalf("schedule err=%v", err)
	}
	if _, err := a.Claim(context.Background(), 1); !errors.Is(err, ErrPublishFailed) {
		t.Fatalf("claim err=%v", err)
	}
}

func TestAMQPClaimConsumesConfiguredQueue(t *testing.T) {
	ack := &fakeAcknowledger{}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{
		Acknowledger: ack,
		Body:         []byte(`{"key":"order:1","kind":"order","payload":"{}","at":1}`),
	}
	ch := &fakeAMQPChannel{deliveries: deliveries}
	a := NewAMQP(ch, testAMQPConfig())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tasks, err := a.Claim(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(ch.consumed) != 1 || ch.consumed[0] != "delay.q" {
		t.Fatalf("consumed=%v", ch.consumed)
	}
	if len(tasks) != 1 || tasks[0].Key != "order:1" || tasks[0].Kind != "order" {
		t.Fatalf("tasks=%+v", tasks)
	}
	if tasks[0].ack != nil {
		t.Fatal("claimed task should already be acked")
	}
	ack.mu.Lock()
	defer ack.mu.Unlock()
	if !ack.acked {
		t.Fatal("claim should ack immediately")
	}
	if ack.nacked {
		t.Fatal("claim should not nack")
	}
}

func TestAMQPClaimQueueFallsBackToRoutingKey(t *testing.T) {
	deliveries := make(chan amqp.Delivery)
	ch := &fakeAMQPChannel{deliveries: deliveries}
	a := NewAMQP(ch, AMQPConfig{Exchange: "ex", RoutingKey: "rk"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = a.Claim(ctx, 1)
	if len(ch.consumed) != 1 || ch.consumed[0] != "rk" {
		t.Fatalf("consumed=%v", ch.consumed)
	}
}

func TestAMQPClaimSkipsInvalidPayload(t *testing.T) {
	ack := &fakeAcknowledger{}
	deliveries := make(chan amqp.Delivery, 2)
	deliveries <- amqp.Delivery{Acknowledger: ack, Body: []byte(`not-json`)}
	okAck := &fakeAcknowledger{}
	deliveries <- amqp.Delivery{
		Acknowledger: okAck,
		Body:         []byte(`{"key":"ok","kind":"order","payload":"{}","at":1}`),
	}
	ch := &fakeAMQPChannel{deliveries: deliveries}
	a := NewAMQP(ch, testAMQPConfig())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tasks, err := a.Claim(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !ack.acked {
		t.Fatal("invalid payload should be acked")
	}
	if len(tasks) != 1 || tasks[0].Key != "ok" {
		t.Fatalf("tasks=%+v", tasks)
	}
	okAck.mu.Lock()
	defer okAck.mu.Unlock()
	if !okAck.acked {
		t.Fatal("valid payload should be acked on claim")
	}
}

func TestAMQPClaimClosedChannel(t *testing.T) {
	deliveries := make(chan amqp.Delivery)
	close(deliveries)
	a := NewAMQP(&fakeAMQPChannel{deliveries: deliveries}, testAMQPConfig())
	_, err := a.Claim(context.Background(), 1)
	if err == nil {
		t.Fatal("expected closed channel error")
	}
}

func TestAMQPClaimClosedAfterPartial(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 1)
	okAck := &fakeAcknowledger{}
	deliveries <- amqp.Delivery{
		Acknowledger: okAck,
		Body:         []byte(`{"key":"ok","kind":"order","payload":"{}","at":1}`),
	}
	close(deliveries)
	a := NewAMQP(&fakeAMQPChannel{deliveries: deliveries}, testAMQPConfig())
	tasks, err := a.Claim(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Key != "ok" {
		t.Fatalf("tasks=%+v", tasks)
	}
}

func TestAMQPAckFailRelease(t *testing.T) {
	ack := &fakeAcknowledger{}
	task := Task{ack: &amqpDeliveryAck{d: amqp.Delivery{Acknowledger: ack}}}
	a := NewAMQP(&fakeAMQPChannel{}, testAMQPConfig())
	if err := a.Ack(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if !ack.acked {
		t.Fatal("expected ack")
	}

	nack := &fakeAcknowledger{}
	task.ack = &amqpDeliveryAck{d: amqp.Delivery{Acknowledger: nack}}
	if err := a.Fail(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if !nack.nacked || !nack.requeue {
		t.Fatalf("fail nack=%+v", nack)
	}

	rel := &fakeAcknowledger{}
	task.ack = &amqpDeliveryAck{d: amqp.Delivery{Acknowledger: rel}}
	if err := a.Release(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if !rel.nacked || !rel.requeue {
		t.Fatalf("release nack=%+v", rel)
	}

	if err := a.Ack(context.Background(), Task{}); err != nil {
		t.Fatal(err)
	}
}

func TestAMQPFailRepublishesAfterClaimAck(t *testing.T) {
	ack := &fakeAcknowledger{}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{
		Acknowledger: ack,
		Body:         []byte(`{"key":"order:1","kind":"order","payload":"{}","at":1}`),
	}
	ch := &fakeAMQPChannel{deliveries: deliveries}
	now := time.UnixMilli(1_700_000_000_000)
	a := NewAMQP(ch, testAMQPConfig(), WithAMQPClock(func() time.Time { return now }))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tasks, err := a.Claim(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("tasks=%d", len(tasks))
	}
	ack.mu.Lock()
	claimed := ack.acked
	ack.mu.Unlock()
	if !claimed {
		t.Fatal("claim should ack")
	}

	if err := a.Fail(ctx, tasks[0]); err != nil {
		t.Fatal(err)
	}
	if len(ch.pubs) != 1 {
		t.Fatalf("fail should republish, pubs=%d", len(ch.pubs))
	}
	delay, _ := ch.pubs[0].msg.Headers["x-delay"].(int64)
	if delay != failRequeueDelay.Milliseconds() {
		t.Fatalf("x-delay=%v", ch.pubs[0].msg.Headers["x-delay"])
	}
}
