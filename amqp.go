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
type AMQPChannel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
}

// AMQPConfig AMQP 后端配置。交换机 / 队列由调用方声明（x-delayed-message）。
// 发布使用 Exchange + RoutingKey；消费 Queue，为空则回退到 RoutingKey。
type AMQPConfig struct {
	Exchange   string
	RoutingKey string
	Queue      string
}

// AMQP 延迟交换机 + 竞争消费。Cancel 恒为 ErrCancelUnsupported。
// Claim 成功解码后立刻 Ack，避免 unack 堵住消费。
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

// NewAMQP 新建 AMQP 后端。ch 一般为 *amqp.Channel。Cancel / DelEvent 恒为 ErrCancelUnsupported。
// 交换机与队列需由调用方声明（x-delayed-message）。
func NewAMQP(ch AMQPChannel, cfg AMQPConfig, options ...AMQPOption) *AMQP {
	a := &AMQP{
		clock:    time.Now,
		topology: newAMQPTopology(cfg),
		broker:   newAMQPBroker(ch),
	}
	for _, option := range options {
		option(a)
	}
	return a
}

// Schedule 发布延迟消息（header x-delay 毫秒）
func (a *AMQP) Schedule(ctx context.Context, task Task) error {
	if err := a.broker.ensureOpen(); err != nil {
		return err
	}
	cmd := newAMQPPublish(task, a.clock())
	msg, err := cmd.publishing()
	if err != nil {
		return err
	}
	return a.broker.publish(ctx, a.topology.route(), msg)
}

// Cancel AMQP 延迟消息无法从 broker 撤回，恒返回 ErrCancelUnsupported。
func (a *AMQP) Cancel(context.Context, string) error {
	return ErrCancelUnsupported
}

// Claim 从队列竞争消费。Consume 通道关闭后会重新注册，不绑到单次 Claim 的 ctx。
func (a *AMQP) Claim(ctx context.Context, n int) ([]Task, error) {
	if err := a.broker.ensureOpen(); err != nil {
		return nil, err
	}
	if n < 1 {
		n = 1
	}
	src, err := a.broker.source(a.topology.consumeQueue())
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
			return a.broker.closed(out)
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

// amqpBroker 通道适配：发布与 Consume 生命周期。
type amqpBroker struct {
	ch    AMQPChannel
	mu    sync.Mutex
	msgCh <-chan amqp.Delivery
}

func newAMQPBroker(ch AMQPChannel) *amqpBroker {
	return &amqpBroker{ch: ch}
}

func (b *amqpBroker) ensureOpen() error {
	if b == nil || b.ch == nil {
		return ErrPublishFailed
	}
	return nil
}

func (b *amqpBroker) publish(ctx context.Context, route amqpRoute, msg amqp.Publishing) error {
	return b.ch.PublishWithContext(ctx, route.exchange, route.routingKey, false, false, msg)
}

func (b *amqpBroker) source(queue string) (amqpSource, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.msgCh != nil {
		return amqpSource{ch: b.msgCh}, nil
	}
	if err := b.ensureOpen(); err != nil {
		return amqpSource{}, err
	}
	ch, err := b.ch.Consume(queue, "", false, false, false, false, nil)
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
	b.msgCh = nil
	b.mu.Unlock()
}

func (b *amqpBroker) closed(out []Task) ([]Task, error) {
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
