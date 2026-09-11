package delaytimer

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

var _ Store = (*AMQP)(nil)
var _ AMQPChannel = (*amqp.Channel)(nil)

// AMQPChannel 是 amqp091 Channel 的子集，*amqp.Channel 已满足。
// 发布与消费必须使用不同实例：amqp091 Channel 不能跨 goroutine 共用。
type AMQPChannel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Close() error
}

// AMQPConfig AMQP 后端配置。首次 Schedule / Claim 时分别打开发布、消费 Channel，并声明拓扑。
// 发布使用 Exchange + RoutingKey；消费 Queue，为空则回退到 RoutingKey。
type AMQPConfig struct {
	Exchange   string
	RoutingKey string
	Queue      string
}

// AMQP 延迟交换机 + 竞争消费。Cancel 恒为 ErrCancelUnsupported。
// Claim 成功解码后立刻 Ack，避免 unack 堵住消费。
// Close 关闭内部打开的发布 / 消费 Channel；连接仍由调用方关闭。
type AMQP struct {
	clock    Clock
	topology *amqpTopology
	broker   *amqpBroker
}

// AMQPOption AMQP 选项
type AMQPOption func(*AMQP)

// WithAMQPClock 注入时钟
func WithAMQPClock(c Clock) AMQPOption {
	return func(a *AMQP) {
		if c != nil {
			a.clock = c
		}
	}
}

// amqpConn 能从连接打开 Channel。*amqp.Connection 经 NewAMQP 包装后满足。
type amqpConn interface {
	Channel() (AMQPChannel, error)
}

type amqpConnection struct {
	conn *amqp.Connection
}

func (c *amqpConnection) Channel() (AMQPChannel, error) {
	if c == nil || c.conn == nil {
		return nil, ErrPublishFailed
	}
	return c.conn.Channel()
}

// NewAMQP 新建 AMQP 后端。conn 由调用方 Dial / Close；内部自行打开 Channel。
// 发布与消费各用一条 Channel，Publish 串行。Cancel / DelEvent 恒为 ErrCancelUnsupported。
// 首次使用时声明 x-delayed-message 交换机（x-delayed-type=direct）、队列并绑定。
func NewAMQP(conn *amqp.Connection, cfg AMQPConfig, options ...AMQPOption) *AMQP {
	var opener amqpConn
	if conn != nil {
		opener = &amqpConnection{conn: conn}
	}
	return newAMQP(opener, cfg, options...)
}

func newAMQP(conn amqpConn, cfg AMQPConfig, options ...AMQPOption) *AMQP {
	a := &AMQP{
		clock:    time.Now,
		topology: newAMQPTopology(cfg),
		broker:   newAMQPBroker(conn),
	}
	for _, option := range options {
		option(a)
	}
	return a
}

// Schedule 发布延迟消息（header x-delay 毫秒）
func (a *AMQP) Schedule(ctx context.Context, task Task) error {
	cmd := newAMQPPublish(task, a.clock())
	msg, err := cmd.publishing()
	if err != nil {
		return err
	}
	return a.broker.publish(ctx, a.topology, msg)
}

// Cancel AMQP 延迟消息无法从 broker 撤回，恒返回 ErrCancelUnsupported。
func (a *AMQP) Cancel(context.Context, string) error {
	return ErrCancelUnsupported
}

// Claim 从队列竞争消费。Consume 通道关闭后会重新注册，不绑到单次 Claim 的 ctx。
func (a *AMQP) Claim(ctx context.Context, n int) ([]Task, error) {
	if n < 1 {
		n = 1
	}
	src, err := a.broker.source(a.topology)
	if err != nil {
		return nil, err
	}

	var out []Task
	for i := 0; i < n; i++ {
		receipt, err := src.recv(ctx, i == 0)
		if err != nil {
			return nil, err
		}
		if receipt.closed {
			return a.broker.onConsumeClosed(out)
		}
		if receipt.idle || !receipt.ok {
			return out, nil
		}
		task, err := receipt.decode()
		if err != nil {
			_ = receipt.ackInvalid()
			continue
		}
		// 领取后立刻 Ack，避免 Handle / 未知 Kind 失败占着 unack 堵住消费。
		if err := ackClaimed(&task); err != nil {
			if len(out) == 0 {
				return nil, err
			}
			return out, nil
		}
		out = append(out, task)
	}
	return out, nil
}

// Ack 确认消息。Claim 时通常已 Ack，此处为空操作。
func (a *AMQP) Ack(_ context.Context, task Task) error {
	return confirmAMQP(task, func(h ack) error { return h.Ack() })
}

// Fail 重新入队。领取时已 Ack 则重新发布；若仍持有投递则 Nack。
func (a *AMQP) Fail(ctx context.Context, task Task) error {
	if task.ack != nil {
		return confirmAMQP(task, func(h ack) error { return h.Nack(true) })
	}
	task.At = a.clock().Add(failRequeueDelay)
	return a.Schedule(ctx, task)
}

// Release 把未 Ack 消息重新入队。Claim 后已 Ack，调用为空操作。
func (a *AMQP) Release(_ context.Context, task Task) error {
	return confirmAMQP(task, func(h ack) error { return h.Nack(true) })
}

// Close 关闭内部打开的发布 / 消费 Channel。连接仍由调用方 Close。可重复调用。
func (a *AMQP) Close() error {
	if a == nil || a.broker == nil {
		return nil
	}
	return a.broker.shutdown()
}

func confirmAMQP(task Task, fn func(ack) error) error {
	if task.ack == nil {
		return nil
	}
	return fn(task.ack)
}

func ackClaimed(task *Task) error {
	if task == nil || task.ack == nil {
		return nil
	}
	err := task.ack.Ack()
	task.ack = nil
	return err
}

// amqpTopology 路由与队列命名。
type amqpTopology struct {
	exchange   string
	routingKey string
	queue      string
}

func newAMQPTopology(cfg AMQPConfig) *amqpTopology {
	return &amqpTopology{
		exchange:   cfg.Exchange,
		routingKey: cfg.RoutingKey,
		queue:      cfg.Queue,
	}
}

func (t *amqpTopology) route() amqpRoute {
	return amqpRoute{exchange: t.exchange, routingKey: t.routingKey}
}

func (t *amqpTopology) consumeQueue() string {
	if t.queue != "" {
		return t.queue
	}
	return t.routingKey
}

type amqpRoute struct {
	exchange   string
	routingKey string
}

// amqpBroker 发布与消费各用一条 Channel，避免 amqp091 跨 goroutine 共用。
// Publish 在 mu 内串行；Consume 只发生在 Claim 路径。
type amqpBroker struct {
	conn    amqpConn
	mu      sync.Mutex
	stopped bool
	pub     AMQPChannel
	sub     AMQPChannel
	msgCh   <-chan amqp.Delivery
}

func newAMQPBroker(conn amqpConn) *amqpBroker {
	return &amqpBroker{conn: conn}
}

func (b *amqpBroker) openLocked() (AMQPChannel, error) {
	if b.conn == nil {
		return nil, ErrPublishFailed
	}
	ch, err := b.conn.Channel()
	if err != nil {
		return nil, err
	}
	if ch == nil {
		return nil, ErrPublishFailed
	}
	return ch, nil
}

func (b *amqpBroker) declareLocked(ch AMQPChannel, t *amqpTopology) error {
	if err := ch.ExchangeDeclare(t.exchange, "x-delayed-message", true, false, false, false, amqp.Table{
		"x-delayed-type": "direct",
	}); err != nil {
		return err
	}
	queue := t.consumeQueue()
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return err
	}
	return ch.QueueBind(queue, t.routingKey, t.exchange, false, nil)
}

func (b *amqpBroker) ensurePubLocked(t *amqpTopology) error {
	if b.stopped {
		return ErrChannelClosed
	}
	if b.pub != nil {
		return nil
	}
	ch, err := b.openLocked()
	if err != nil {
		return err
	}
	if err := b.declareLocked(ch, t); err != nil {
		_ = ch.Close()
		return err
	}
	b.pub = ch
	return nil
}

func (b *amqpBroker) ensureSubLocked(t *amqpTopology) error {
	if b.stopped {
		return ErrChannelClosed
	}
	if b.sub != nil {
		return nil
	}
	ch, err := b.openLocked()
	if err != nil {
		return err
	}
	if err := b.declareLocked(ch, t); err != nil {
		_ = ch.Close()
		return err
	}
	b.sub = ch
	return nil
}

func (b *amqpBroker) publish(ctx context.Context, t *amqpTopology, msg amqp.Publishing) error {
	if b == nil {
		return ErrPublishFailed
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensurePubLocked(t); err != nil {
		return err
	}
	route := t.route()
	return b.pub.PublishWithContext(ctx, route.exchange, route.routingKey, false, false, msg)
}

func (b *amqpBroker) source(t *amqpTopology) (amqpSource, error) {
	if b == nil {
		return amqpSource{}, ErrPublishFailed
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.msgCh != nil {
		return amqpSource{ch: b.msgCh}, nil
	}
	if err := b.ensureSubLocked(t); err != nil {
		return amqpSource{}, err
	}
	ch, err := b.sub.Consume(t.consumeQueue(), "", false, false, false, false, nil)
	if err != nil {
		return amqpSource{}, err
	}
	if ch == nil {
		return amqpSource{}, errors.WithStack(ErrPublishFailed)
	}
	b.msgCh = ch
	return amqpSource{ch: ch}, nil
}

func (b *amqpBroker) drop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.msgCh = nil
	if b.sub != nil {
		_ = b.sub.Close()
		b.sub = nil
	}
}

func (b *amqpBroker) shutdown() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stopped = true
	b.msgCh = nil
	sub, pub := b.sub, b.pub
	b.sub, b.pub = nil, nil
	var first error
	if sub != nil {
		first = sub.Close()
	}
	if pub != nil && pub != sub {
		if err := pub.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (b *amqpBroker) onConsumeClosed(out []Task) ([]Task, error) {
	b.drop()
	if len(out) > 0 {
		return out, nil
	}
	return nil, errors.New("amqp consume channel closed")
}

// amqpPublish 一次延迟投递。
type amqpPublish struct {
	task  Task
	delay time.Duration
}

func newAMQPPublish(task Task, now time.Time) amqpPublish {
	delay := task.At.Sub(now)
	if delay < 0 {
		delay = 0
	}
	return amqpPublish{task: task, delay: delay}
}

func (p amqpPublish) publishing() (amqp.Publishing, error) {
	body, err := json.Marshal(amqpMessage{
		Key:     p.task.Key,
		Kind:    p.task.Kind,
		Payload: p.task.Payload,
		At:      p.task.At.UnixMilli(),
	})
	if err != nil {
		return amqp.Publishing{}, errors.WithStack(err)
	}
	return amqp.Publishing{
		Headers:      amqp.Table{"x-delay": p.delay.Milliseconds()},
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}, nil
}

// amqpMessage 投递载荷。
type amqpMessage struct {
	Key     string `json:"key"`
	Kind    string `json:"kind"`
	Payload string `json:"payload"`
	At      int64  `json:"at"`
}

func (m amqpMessage) task(h ack) Task {
	return Task{
		Key:     m.Key,
		Kind:    m.Kind,
		Payload: m.Payload,
		At:      time.UnixMilli(m.At),
		ack:     h,
	}
}

type amqpDeliveryAck struct {
	d amqp.Delivery
}

func (a *amqpDeliveryAck) Ack() error {
	return a.d.Ack(false)
}

func (a *amqpDeliveryAck) Nack(requeue bool) error {
	return a.d.Nack(false, requeue)
}

// amqpSource 单条 Consume 通道，负责领取投递。
type amqpSource struct {
	ch <-chan amqp.Delivery
}

type amqpReceipt struct {
	d      amqp.Delivery
	ok     bool
	closed bool
	idle   bool
}

func (r amqpReceipt) decode() (Task, error) {
	var msg amqpMessage
	if err := json.Unmarshal(r.d.Body, &msg); err != nil {
		return Task{}, errors.WithStack(err)
	}
	return msg.task(&amqpDeliveryAck{d: r.d}), nil
}

func (r amqpReceipt) ackInvalid() error {
	return r.d.Ack(false)
}

func (s amqpSource) recv(ctx context.Context, block bool) (amqpReceipt, error) {
	if s.ch == nil {
		return amqpReceipt{idle: true}, nil
	}
	if block {
		select {
		case <-ctx.Done():
			return amqpReceipt{}, ctx.Err()
		case d, ok := <-s.ch:
			if !ok {
				return amqpReceipt{closed: true}, nil
			}
			return amqpReceipt{d: d, ok: true}, nil
		}
	}
	select {
	case d, ok := <-s.ch:
		if !ok {
			return amqpReceipt{closed: true}, nil
		}
		return amqpReceipt{d: d, ok: true}, nil
	default:
		return amqpReceipt{idle: true}, nil
	}
}
