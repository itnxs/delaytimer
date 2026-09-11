package delaytimer

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeAMQPChannel struct {
	mu         sync.Mutex
	pubs       []amqpPublish
	deliveries chan amqp.Delivery
	consumed   []string
}

type amqpPublish struct {
	exchange string
	key      string
	msg      amqp.Publishing
}

func (c *fakeAMQPChannel) PublishWithContext(_ context.Context, exchange, key string, _, _ bool, msg amqp.Publishing) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	body := append([]byte(nil), msg.Body...)
	msg.Body = body
	c.pubs = append(c.pubs, amqpPublish{exchange: exchange, key: key, msg: msg})
	return nil
}

func (c *fakeAMQPChannel) Consume(queue string, _ string, _ bool, _ bool, _ bool, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	c.consumed = append(c.consumed, queue)
	c.mu.Unlock()
	return c.deliveries, nil
}

func (c *fakeAMQPChannel) lastPublish() amqpPublish {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pubs[len(c.pubs)-1]
}

type ackSpy struct {
	mu      sync.Mutex
	acked   bool
	nacked  bool
	requeue bool
}

func (s *ackSpy) Ack(uint64, bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acked = true
	return nil
}

func (s *ackSpy) Nack(_ uint64, _ bool, requeue bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nacked = true
	s.requeue = requeue
	return nil
}

func (s *ackSpy) Reject(uint64, bool) error { return nil }

func (s *ackSpy) snapshot() (acked, nacked, requeue bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acked, s.nacked, s.requeue
}

func testDelivery(body []byte, ack amqp.Acknowledger) amqp.Delivery {
	return amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, Body: body}
}

func TestAMQPSchedulePublishesDelayHeader(t *testing.T) {
	now := time.Unix(1000, 0)
	ch := &fakeAMQPChannel{}
	a := NewAMQP(ch, AMQPConfig{Exchange: "chunk_delay", RoutingKey: "timer.job"}, WithAMQPClock(func() time.Time { return now }))

	job := Job{Key: "k1", Kind: "answer_timeout", Payload: "p1", At: now.Add(2 * time.Second)}
	if err := a.Schedule(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	m := ch.lastPublish()
	if m.exchange != "chunk_delay" || m.key != "timer.job" {
		t.Fatalf("%s %s", m.exchange, m.key)
	}
	if m.msg.DeliveryMode != amqp.Persistent {
		t.Fatalf("delivery mode %d", m.msg.DeliveryMode)
	}
	delay, _ := m.msg.Headers["x-delay"].(int64)
	if delay != 2000 {
		t.Fatalf("x-delay=%v", m.msg.Headers["x-delay"])
	}
	var got amqpMessage
	if err := json.Unmarshal(m.msg.Body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Key != "k1" || got.Kind != "answer_timeout" || got.Payload != "p1" {
		t.Fatalf("%+v", got)
	}
}

func TestAMQPCancelUnsupported(t *testing.T) {
	a := NewAMQP(&fakeAMQPChannel{}, AMQPConfig{})
	err := a.Cancel(context.Background(), "k")
	if !errors.Is(err, ErrCancelUnsupported) {
		t.Fatalf("err=%v", err)
	}
}

func TestAMQPClaimCancelledContextDoesNotConsumeDelivery(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 1)
	ch := &fakeAMQPChannel{deliveries: deliveries}
	a := NewAMQP(ch, AMQPConfig{Queue: "q"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Claim(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("first claim err=%v", err)
	}

	body, _ := json.Marshal(amqpMessage{Key: "k1", Kind: "kind", Payload: "p", At: 1})
	deliveries <- testDelivery(body, &ackSpy{})

	got, err := a.Claim(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "k1" {
		t.Fatalf("%+v", got)
	}
}

func TestAMQPClaimAndAck(t *testing.T) {
	body, _ := json.Marshal(amqpMessage{Key: "k1", Kind: "kind", Payload: "p", At: 1})
	spy := &ackSpy{}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- testDelivery(body, spy)
	a := NewAMQP(&fakeAMQPChannel{deliveries: deliveries}, AMQPConfig{Queue: "q"})

	got, err := a.Claim(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "k1" {
		t.Fatalf("%+v", got)
	}
	if err := a.Ack(context.Background(), got[0]); err != nil {
		t.Fatal(err)
	}
	acked, _, _ := spy.snapshot()
	if !acked {
		t.Fatal("not acked")
	}
}

func TestAMQPFailNackRequeues(t *testing.T) {
	body, _ := json.Marshal(amqpMessage{Key: "k1", Kind: "kind", Payload: "p", At: 1})
	spy := &ackSpy{}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- testDelivery(body, spy)
	a := NewAMQP(&fakeAMQPChannel{deliveries: deliveries}, AMQPConfig{Queue: "q"})

	got, _ := a.Claim(context.Background(), 1)
	if err := a.Fail(context.Background(), got[0]); err != nil {
		t.Fatal(err)
	}
	_, nacked, requeue := spy.snapshot()
	if !nacked || !requeue {
		t.Fatalf("nacked=%v requeue=%v", nacked, requeue)
	}
}

func TestAMQPClaimClosedConsumeChannel(t *testing.T) {
	deliveries := make(chan amqp.Delivery)
	close(deliveries)
	a := NewAMQP(&fakeAMQPChannel{deliveries: deliveries}, AMQPConfig{Queue: "q"})
	_, err := a.Claim(context.Background(), 1)
	if err == nil {
		t.Fatal("expected consume channel closed error")
	}
}

func TestAMQPShutdownRequeuesUndispatched(t *testing.T) {
	g := withBlockGate()
	body, _ := json.Marshal(amqpMessage{Key: "k", Kind: "block", Payload: `{}`, At: 1})
	spies := []*ackSpy{{}, {}, {}}
	deliveries := make(chan amqp.Delivery, 3)
	for _, s := range spies {
		deliveries <- testDelivery(append([]byte(nil), body...), s)
	}
	a := NewAMQP(&fakeAMQPChannel{deliveries: deliveries}, AMQPConfig{Queue: "q"})
	tm := New(a, WithHandlers(blockHandler()),
		WithConcurrency(1), WithBatchSize(8), WithPollInterval(time.Millisecond))

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

	requeued := 0
	for _, s := range spies {
		_, nacked, requeue := s.snapshot()
		if nacked && requeue {
			requeued++
		}
	}
	if requeued < 2 {
		t.Fatalf("undispatched AMQP jobs should requeue, requeued=%d", requeued)
	}
}

func TestAMQPHandleContextCancelRequeues(t *testing.T) {
	started := make(chan struct{})
	body, _ := json.Marshal(amqpMessage{Key: "k", Kind: "block", Payload: `{}`, At: 1})
	spy := &ackSpy{}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- testDelivery(body, spy)
	a := NewAMQP(&fakeAMQPChannel{deliveries: deliveries}, AMQPConfig{Queue: "q"})
	h := Bind(&blockEvent{}, func(ctx context.Context, _ *blockEvent) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	tm := New(a, WithHandlers(h), WithPollInterval(time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tm.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return")
	}
	acked, nacked, requeue := spy.snapshot()
	if acked || !nacked || !requeue {
		t.Fatalf("acked=%v nacked=%v requeue=%v", acked, nacked, requeue)
	}
}

func TestAMQPHandleErrorDoesNotRequeueByDefault(t *testing.T) {
	body, _ := json.Marshal(amqpMessage{Key: "k", Kind: "answer_timeout", Payload: `{"play":"p"}`, At: 1})
	spy := &ackSpy{}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- testDelivery(body, spy)
	a := NewAMQP(&fakeAMQPChannel{deliveries: deliveries}, AMQPConfig{Queue: "q"})
	tm := New(a, WithHandlers(failHandler()), WithPollInterval(time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = tm.Run(ctx) }()
	waitUntil(t, time.Second, func() bool {
		acked, _, _ := spy.snapshot()
		return acked
	})
	acked, nacked, requeue := spy.snapshot()
	if !acked || nacked || requeue {
		t.Fatalf("acked=%v nacked=%v requeue=%v", acked, nacked, requeue)
	}
}

func TestAMQPHandleErrorRequeuesWithFailPolicy(t *testing.T) {
	body, _ := json.Marshal(amqpMessage{Key: "k", Kind: "answer_timeout", Payload: `{"play":"p"}`, At: 1})
	spy := &ackSpy{}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- testDelivery(body, spy)
	a := NewAMQP(&fakeAMQPChannel{deliveries: deliveries}, AMQPConfig{Queue: "q"})
	tm := New(a, WithHandlers(failHandler()), WithPollInterval(time.Millisecond), WithFailPolicy(FailRequeue))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = tm.Run(ctx) }()
	waitUntil(t, time.Second, func() bool {
		_, nacked, _ := spy.snapshot()
		return nacked
	})
	acked, nacked, requeue := spy.snapshot()
	if acked || !nacked || !requeue {
		t.Fatalf("acked=%v nacked=%v requeue=%v", acked, nacked, requeue)
	}
}

func TestAMQPScheduleUsesKindShardRoutingKey(t *testing.T) {
	now := time.Unix(1000, 0)
	ch := &fakeAMQPChannel{}
	a := NewAMQP(ch, AMQPConfig{
		Exchange:   "chunk_delay",
		RoutingKey: "timer.job",
		Shards:     4,
		ShardMode:  ShardHash,
		Kinds:      []string{"answer_timeout"},
	}, WithAMQPClock(func() time.Time { return now }))

	job := Job{Kind: "answer_timeout", Payload: "p1", At: now.Add(time.Second)}
	if err := a.Schedule(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	member := JobKey(job.Kind, job.Payload)
	want := "timer.job." + shardName(job.Kind, shardIndex(ShardHash, 4, member, 0))
	m := ch.lastPublish()
	if m.key != want {
		t.Fatalf("key=%s want=%s", m.key, want)
	}
}

func TestAMQPClaimConsumesKindShardQueues(t *testing.T) {
	body, _ := json.Marshal(amqpMessage{Key: "k1", Kind: "answer_timeout", Payload: "p", At: 1})
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- testDelivery(body, &ackSpy{})
	ch := &fakeAMQPChannel{deliveries: deliveries}
	a := NewAMQP(ch, AMQPConfig{
		Queue:      "timer.job",
		RoutingKey: "timer.job",
		Shards:     2,
		Kinds:      []string{"answer_timeout", "confirm_timeout"},
	})
	got, err := a.Claim(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "k1" {
		t.Fatalf("%+v", got)
	}
	ch.mu.Lock()
	consumed := append([]string(nil), ch.consumed...)
	ch.mu.Unlock()
	want := []string{
		"timer.job.answer_timeout.0",
		"timer.job.answer_timeout.1",
		"timer.job.confirm_timeout.0",
		"timer.job.confirm_timeout.1",
	}
	if len(consumed) != len(want) {
		t.Fatalf("consumed=%v", consumed)
	}
	for i := range want {
		if consumed[i] != want[i] {
			t.Fatalf("consumed=%v want=%v", consumed, want)
		}
	}
}

func TestAMQPClaimRequiresKindsWhenSharded(t *testing.T) {
	a := NewAMQP(&fakeAMQPChannel{}, AMQPConfig{Queue: "q", Shards: 2})
	_, err := a.Claim(context.Background(), 1)
	if !errors.Is(err, ErrAMQPKindsRequired) {
		t.Fatalf("err=%v", err)
	}
}

func TestAMQPScheduleMilliRoutingKey(t *testing.T) {
	now := time.UnixMilli(1_000_003)
	ch := &fakeAMQPChannel{}
	a := NewAMQP(ch, AMQPConfig{
		Exchange:   "chunk_delay",
		RoutingKey: "timer.job",
		Shards:     8,
		ShardMode:  ShardMilli,
		Kinds:      []string{"answer_timeout"},
	}, WithAMQPClock(func() time.Time { return now }))

	job := Job{Kind: "answer_timeout", Payload: "p1", At: now}
	if err := a.Schedule(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	want := "timer.job." + shardName(job.Kind, shardIndex(ShardMilli, 8, "", now.UnixMilli()))
	m := ch.lastPublish()
	if m.key != want {
		t.Fatalf("key=%s want=%s", m.key, want)
	}
}
