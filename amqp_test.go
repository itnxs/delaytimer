package delaytimer

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeAMQPChannel struct {
	mu           sync.Mutex
	pubs         []amqpPublishRec
	deliveries   chan amqp.Delivery
	consumed     []string
	exchanges    []amqpExchangeRec
	queues       []string
	binds        []amqpBindRec
	publishDelay time.Duration
	inFlight     int32
	overlap      int32
	closeN       int
}

type amqpPublishRec struct {
	exchange string
	key      string
	msg      amqp.Publishing
}

func (c *fakeAMQPChannel) PublishWithContext(_ context.Context, exchange, key string, _, _ bool, msg amqp.Publishing) error {
	if n := atomic.AddInt32(&c.inFlight, 1); n > 1 {
		atomic.AddInt32(&c.overlap, 1)
	}
	if d := c.publishDelay; d > 0 {
		time.Sleep(d)
	}
	defer atomic.AddInt32(&c.inFlight, -1)

	c.mu.Lock()
	defer c.mu.Unlock()
	msg.Body = append([]byte(nil), msg.Body...)
	c.pubs = append(c.pubs, amqpPublishRec{exchange: exchange, key: key, msg: msg})
	return nil
}

func (c *fakeAMQPChannel) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeN++
	return nil
}

func (c *fakeAMQPChannel) closes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeN
}

func (c *fakeAMQPChannel) Consume(queue string, _ string, _, _, _, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumed = append(c.consumed, queue)
	return c.deliveries, nil
}

func (c *fakeAMQPChannel) ExchangeDeclare(name, kind string, _, _, _, _ bool, args amqp.Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.exchanges = append(c.exchanges, amqpExchangeRec{name: name, kind: kind, args: args})
	return nil
}

func (c *fakeAMQPChannel) QueueDeclare(name string, _, _, _, _ bool, _ amqp.Table) (amqp.Queue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queues = append(c.queues, name)
	return amqp.Queue{Name: name}, nil
}

func (c *fakeAMQPChannel) QueueBind(name, key, exchange string, _ bool, _ amqp.Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.binds = append(c.binds, amqpBindRec{queue: name, key: key, exchange: exchange})
	return nil
}

type fakeAMQPConn struct {
	mu   sync.Mutex
	n    int
	ch   AMQPChannel
	err  error
	open func() (AMQPChannel, error)
}

func (c *fakeAMQPConn) Channel() (AMQPChannel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	if c.open != nil {
		return c.open()
	}
	if c.err != nil {
		return nil, c.err
	}
	return c.ch, nil
}

func (c *fakeAMQPConn) opens() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

type amqpExchangeRec struct {
	name string
	kind string
	args amqp.Table
}

type amqpBindRec struct {
	queue    string
	key      string
	exchange string
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

func amqpStore(ch AMQPChannel, cfg AMQPConfig, opts ...AMQPOption) *AMQP {
	return newAMQP(&fakeAMQPConn{ch: ch}, cfg, opts...)
}

func TestAMQPOpensChannelLazilyFromConn(t *testing.T) {
	ch := &fakeAMQPChannel{}
	conn := &fakeAMQPConn{ch: ch}
	a := newAMQP(conn, testAMQPConfig())
	if conn.opens() != 0 {
		t.Fatal("Channel should not open at New")
	}
	if err := a.Schedule(context.Background(), Task{Key: "k", Kind: "order", Payload: "{}", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if conn.opens() != 1 {
		t.Fatalf("opens=%d", conn.opens())
	}
	if err := a.Schedule(context.Background(), Task{Key: "k2", Kind: "order", Payload: "{}", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if conn.opens() != 1 {
		t.Fatalf("reuse Channel, opens=%d", conn.opens())
	}
	if len(ch.pubs) != 2 {
		t.Fatalf("pubs=%d", len(ch.pubs))
	}
}

func TestAMQPSeparatesPublishAndConsumeChannels(t *testing.T) {
	var chans []*fakeAMQPChannel
	conn := &fakeAMQPConn{open: func() (AMQPChannel, error) {
		ch := &fakeAMQPChannel{deliveries: make(chan amqp.Delivery)}
		chans = append(chans, ch)
		return ch, nil
	}}
	a := newAMQP(conn, testAMQPConfig())
	if err := a.Schedule(context.Background(), Task{Key: "k", Kind: "order", Payload: "{}", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if len(chans) != 1 {
		t.Fatalf("publish should open 1 channel, got %d", len(chans))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = a.Claim(ctx, 1)
	if len(chans) != 2 {
		t.Fatalf("publish and consume should use separate channels, got %d", len(chans))
	}
	if len(chans[0].pubs) != 1 {
		t.Fatalf("publish channel pubs=%d", len(chans[0].pubs))
	}
	if len(chans[0].consumed) != 0 {
		t.Fatalf("publish channel should not Consume: %v", chans[0].consumed)
	}
	if len(chans[1].consumed) != 1 {
		t.Fatalf("consume channel consumed=%v", chans[1].consumed)
	}
	if len(chans[1].pubs) != 0 {
		t.Fatalf("consume channel should not Publish: %d", len(chans[1].pubs))
	}
}

func TestAMQPConcurrentPublishUsesPool(t *testing.T) {
	var mu sync.Mutex
	var chans []*fakeAMQPChannel
	conn := &fakeAMQPConn{open: func() (AMQPChannel, error) {
		ch := &fakeAMQPChannel{publishDelay: 30 * time.Millisecond}
		mu.Lock()
		chans = append(chans, ch)
		mu.Unlock()
		return ch, nil
	}}
	a := newAMQP(conn, testAMQPConfig(), WithAMQPPublishChannels(4))
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- a.Schedule(context.Background(), Task{Key: "k", Kind: "order", Payload: "{}", At: time.Now()})
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if conn.opens() != 4 {
		t.Fatalf("opens=%d want 4", conn.opens())
	}
	var pubs int
	for _, ch := range chans {
		if atomic.LoadInt32(&ch.overlap) != 0 {
			t.Fatalf("Publish overlapped on same channel")
		}
		ch.mu.Lock()
		pubs += len(ch.pubs)
		ch.mu.Unlock()
	}
	if pubs != 8 {
		t.Fatalf("pubs=%d", pubs)
	}
	if elapsed := time.Since(start); elapsed > 180*time.Millisecond {
		t.Fatalf("elapsed=%s, pool should publish across channels", elapsed)
	}
}

func TestAMQPSerializesConcurrentPublish(t *testing.T) {
	ch := &fakeAMQPChannel{publishDelay: 20 * time.Millisecond}
	a := amqpStore(ch, testAMQPConfig(), WithAMQPPublishChannels(1))
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- a.Schedule(context.Background(), Task{Key: "k", Kind: "order", Payload: "{}", At: time.Now()})
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if atomic.LoadInt32(&ch.overlap) != 0 {
		t.Fatalf("Publish overlapped on same channel: overlap=%d", ch.overlap)
	}
	if len(ch.pubs) != 8 {
		t.Fatalf("pubs=%d", len(ch.pubs))
	}
}

func TestAMQPChannelError(t *testing.T) {
	want := errors.New("channel refused")
	a := newAMQP(&fakeAMQPConn{err: want}, testAMQPConfig())
	if err := a.Schedule(context.Background(), Task{}); !errors.Is(err, want) {
		t.Fatalf("schedule err=%v", err)
	}
}

func TestAMQPReopensChannelAfterConsumeClosed(t *testing.T) {
	closed := make(chan amqp.Delivery)
	close(closed)
	live := make(chan amqp.Delivery, 1)
	okAck := &fakeAcknowledger{}
	live <- amqp.Delivery{
		Acknowledger: okAck,
		Body:         []byte(`{"key":"ok","kind":"order","payload":"{}","at":1}`),
	}
	var n int
	conn := &fakeAMQPConn{open: func() (AMQPChannel, error) {
		n++
		if n == 1 {
			return &fakeAMQPChannel{deliveries: closed}, nil
		}
		return &fakeAMQPChannel{deliveries: live}, nil
	}}
	a := newAMQP(conn, testAMQPConfig())
	if _, err := a.Claim(context.Background(), 1); err == nil {
		t.Fatal("expected closed consume error")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tasks, err := a.Claim(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("Channel opens=%d", n)
	}
	if len(tasks) != 1 || tasks[0].Key != "ok" {
		t.Fatalf("tasks=%+v", tasks)
	}
}

func TestAMQPSchedulePublishesToRoutingKey(t *testing.T) {
	ch := &fakeAMQPChannel{}
	now := time.UnixMilli(1_700_000_000_000)
	a := amqpStore(ch, testAMQPConfig(), WithAMQPClock(func() time.Time { return now }))

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
	if ch.pubs[1].exchange != "" || ch.pubs[1].key != "delay.q" {
		t.Fatalf("due route=%s/%s", ch.pubs[1].exchange, ch.pubs[1].key)
	}
	var msg amqpMessage
	if err := json.Unmarshal(rec.msg.Body, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.Key != task.Key || msg.Kind != task.Kind || msg.Payload != task.Payload {
		t.Fatalf("msg=%+v", msg)
	}
}

func TestAMQPDeclaresTopology(t *testing.T) {
	ch := &fakeAMQPChannel{}
	a := amqpStore(ch, testAMQPConfig())
	if err := a.Schedule(context.Background(), Task{Key: "k", Kind: "order", Payload: "{}", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if len(ch.exchanges) != 1 || ch.exchanges[0].name != "delay.ex" || ch.exchanges[0].kind != "x-delayed-message" {
		t.Fatalf("exchanges=%+v", ch.exchanges)
	}
	if ch.exchanges[0].args["x-delayed-type"] != "direct" {
		t.Fatalf("args=%v", ch.exchanges[0].args)
	}
	if len(ch.queues) != 1 || ch.queues[0] != "delay.q" {
		t.Fatalf("queues=%v", ch.queues)
	}
	if len(ch.binds) != 1 || ch.binds[0] != (amqpBindRec{queue: "delay.q", key: "delay.rk", exchange: "delay.ex"}) {
		t.Fatalf("binds=%+v", ch.binds)
	}
	if err := a.Schedule(context.Background(), Task{Key: "k2", Kind: "order", Payload: "{}", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if len(ch.exchanges) != 1 || len(ch.queues) != 1 || len(ch.binds) != 1 {
		t.Fatal("topology should be declared once")
	}
}

func TestAMQPCancelUnsupported(t *testing.T) {
	a := amqpStore(&fakeAMQPChannel{}, testAMQPConfig())
	if err := a.Cancel(context.Background(), "k"); !errors.Is(err, ErrCancelUnsupported) {
		t.Fatalf("err=%v", err)
	}
}

func TestAMQPNilConnection(t *testing.T) {
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
	a := amqpStore(ch, testAMQPConfig())

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
	a := amqpStore(ch, AMQPConfig{Exchange: "ex", RoutingKey: "rk"})

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
	a := amqpStore(ch, testAMQPConfig())

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
	a := amqpStore(&fakeAMQPChannel{deliveries: deliveries}, testAMQPConfig())
	_, err := a.Claim(context.Background(), 1)
	if !errors.Is(err, ErrConsumeClosed) {
		t.Fatalf("want ErrConsumeClosed, got %v", err)
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
	a := amqpStore(&fakeAMQPChannel{deliveries: deliveries}, testAMQPConfig())
	tasks, err := a.Claim(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Key != "ok" {
		t.Fatalf("tasks=%+v", tasks)
	}
}

func TestAMQPAckFail(t *testing.T) {
	ack := &fakeAcknowledger{}
	task := Task{ack: &amqpDeliveryAck{d: amqp.Delivery{Acknowledger: ack}}}
	a := amqpStore(&fakeAMQPChannel{}, testAMQPConfig())
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
	a := amqpStore(ch, testAMQPConfig(), WithAMQPClock(func() time.Time { return now }))

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

func TestAMQPCloseClosesPublishAndConsumeChannels(t *testing.T) {
	var chans []*fakeAMQPChannel
	conn := &fakeAMQPConn{open: func() (AMQPChannel, error) {
		ch := &fakeAMQPChannel{deliveries: make(chan amqp.Delivery)}
		chans = append(chans, ch)
		return ch, nil
	}}
	a := newAMQP(conn, testAMQPConfig())
	if err := a.Schedule(context.Background(), Task{Key: "k", Kind: "order", Payload: "{}", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = a.Claim(ctx, 1)
	if len(chans) != 2 {
		t.Fatalf("channels=%d", len(chans))
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if chans[0].closes() != 1 {
		t.Fatalf("pub closeN=%d", chans[0].closes())
	}
	if chans[1].closes() != 1 {
		t.Fatalf("sub closeN=%d", chans[1].closes())
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if chans[0].closes() != 1 || chans[1].closes() != 1 {
		t.Fatal("Close should be idempotent")
	}
	if err := a.Schedule(context.Background(), Task{Key: "k2", Kind: "order", Payload: "{}", At: time.Now()}); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("schedule after close=%v", err)
	}
	if _, err := a.Claim(context.Background(), 1); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("claim after close=%v", err)
	}
}

func TestAMQPCloseDoesNotDoubleCloseDroppedConsume(t *testing.T) {
	closed := make(chan amqp.Delivery)
	close(closed)
	var chans []*fakeAMQPChannel
	conn := &fakeAMQPConn{open: func() (AMQPChannel, error) {
		ch := &fakeAMQPChannel{deliveries: closed}
		chans = append(chans, ch)
		return ch, nil
	}}
	a := newAMQP(conn, testAMQPConfig())
	if err := a.Schedule(context.Background(), Task{Key: "k", Kind: "order", Payload: "{}", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	_, _ = a.Claim(context.Background(), 1)
	if len(chans) != 2 {
		t.Fatalf("channels=%d", len(chans))
	}
	if chans[1].closes() != 1 {
		t.Fatalf("drop should close consume channel, closeN=%d", chans[1].closes())
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if chans[0].closes() != 1 {
		t.Fatalf("pub closeN=%d", chans[0].closes())
	}
	if chans[1].closes() != 1 {
		t.Fatalf("consume should not Close twice, closeN=%d", chans[1].closes())
	}
}
