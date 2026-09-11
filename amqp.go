package delaytimer

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

var _ Backend = (*AMQP)(nil)
var _ AMQPChannel = (*amqp.Channel)(nil)

// AMQPChannel 是 amqp091 Channel 的子集，*amqp.Channel 已满足。
type AMQPChannel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
}

// AMQPConfig AMQP 后端配置。交换机 / 队列由调用方声明（x-delayed-message）。
// Shards==1 且未配置 Kinds 时，发布/消费沿用 RoutingKey / Queue（兼容单队列）。
// 否则发布到 `{RoutingKey}.{kind}.{shard}`，消费 `{Queue}.{kind}.{shard}`（Queue 为空则用 RoutingKey）。
// 分片消费必须配置 Kinds。
type AMQPConfig struct {
	Exchange   string
	RoutingKey string
	Queue      string
	Shards     int
	ShardMode  ShardMode
	Kinds      []string // 要 Consume 的 EventName
}

type amqpMessage struct {
	Key     string `json:"key"`
	Kind    string `json:"kind"`
	Payload string `json:"payload"`
	At      int64  `json:"at"`
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

// AMQP 延迟交换机 + 竞争消费，基于 github.com/rabbitmq/amqp091-go。
type AMQP struct {
	ch     AMQPChannel
	cfg    AMQPConfig
	shards int
	mode   ShardMode
	kinds  []string
	clock  Clock
	mu     sync.Mutex
	msgChs []<-chan amqp.Delivery
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

// NewAMQP 新建 AMQP 后端。ch 一般为 *amqp.Channel。Cancel 恒为 ErrCancelUnsupported。
func NewAMQP(ch AMQPChannel, cfg AMQPConfig, opts ...AMQPOption) *AMQP {
	kinds := append([]string(nil), cfg.Kinds...)
	a := &AMQP{
		ch:     ch,
		cfg:    cfg,
		shards: normalizeShards(cfg.Shards),
		mode:   cfg.ShardMode,
		kinds:  kinds,
		clock:  time.Now,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (a *AMQP) compat() bool {
	return a.shards == 1 && len(a.kinds) == 0
}

func (a *AMQP) routingKey(kind string, shard int) string {
	if a.compat() {
		return a.cfg.RoutingKey
	}
	return a.cfg.RoutingKey + "." + shardName(kind, shard)
}

func (a *AMQP) queueName(kind string, shard int) string {
	base := a.cfg.Queue
	if base == "" {
		base = a.cfg.RoutingKey
	}
	if a.compat() {
		return base
	}
	return base + "." + shardName(kind, shard)
}

func (a *AMQP) consumeQueues() []string {
	if a.compat() {
		return []string{a.queueName("", 0)}
	}
	out := make([]string, 0, len(a.kinds)*a.shards)
	for _, kind := range a.kinds {
		for s := 0; s < a.shards; s++ {
			out = append(out, a.queueName(kind, s))
		}
	}
	return out
}

// Schedule 发布延迟消息（header x-delay 毫秒）
func (a *AMQP) Schedule(ctx context.Context, job Job) error {
	if a.ch == nil {
		return ErrPublishFailed
	}
	job = job.withKey()
	delay := job.At.Sub(a.clock())
	if delay < 0 {
		delay = 0
	}
	body, err := json.Marshal(amqpMessage{
		Key:     job.Key,
		Kind:    job.Kind,
		Payload: job.Payload,
		At:      job.At.UnixMilli(),
	})
	if err != nil {
		return errors.WithStack(err)
	}
	shard := shardIndex(a.mode, a.shards, job.Key, job.At.UnixMilli())
	return a.ch.PublishWithContext(ctx, a.cfg.Exchange, a.routingKey(job.Kind, shard), false, false, amqp.Publishing{
		Headers:      amqp.Table{"x-delay": delay.Milliseconds()},
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

// Cancel AMQP 延迟消息无法从 broker 撤回
func (a *AMQP) Cancel(context.Context, string) error {
	return ErrCancelUnsupported
}

// Claim 从队列竞争消费。Consume 通道关闭后会重新注册，不绑到单次 Claim 的 ctx。
func (a *AMQP) Claim(ctx context.Context, n int) ([]Job, error) {
	if a.ch == nil {
		return nil, ErrPublishFailed
	}
	if n < 1 {
		n = 1
	}
	if !a.compat() && len(a.kinds) == 0 {
		return nil, ErrAMQPKindsRequired
	}
	chs, err := a.consume()
	if err != nil {
		return nil, err
	}

	var out []Job
	for i := 0; i < n; i++ {
		d, ok, closed, idle, err := recvDelivery(ctx, chs, i == 0)
		if err != nil {
			return nil, err
		}
		if closed {
			return a.consumeClosed(out)
		}
		if idle || !ok {
			return out, nil
		}
		job, err := decodeAMQPJob(d)
		if err != nil {
			_ = d.Ack(false)
			continue
		}
		out = append(out, job)
	}
	return out, nil
}

func (a *AMQP) consumeClosed(out []Job) ([]Job, error) {
	a.dropConsume()
	if len(out) > 0 {
		return out, nil
	}
	return nil, errors.New("amqp consume channel closed")
}

func (a *AMQP) consume() ([]<-chan amqp.Delivery, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.msgChs != nil {
		return a.msgChs, nil
	}
	if a.ch == nil {
		return nil, ErrPublishFailed
	}
	queues := a.consumeQueues()
	chs := make([]<-chan amqp.Delivery, 0, len(queues))
	for _, q := range queues {
		ch, err := a.ch.Consume(q, "", false, false, false, false, nil)
		if err != nil {
			return nil, err
		}
		if ch == nil {
			return nil, errors.WithStack(ErrPublishFailed)
		}
		chs = append(chs, ch)
	}
	a.msgChs = chs
	return chs, nil
}

func (a *AMQP) dropConsume() {
	a.mu.Lock()
	a.msgChs = nil
	a.mu.Unlock()
}

// Ack 确认消息
func (a *AMQP) Ack(_ context.Context, job Job) error {
	return a.withAck(job, func(h ackHandle) error { return h.Ack() })
}

// Fail 重新入队。Timer FailDiscard 走 Ack，不会调到这里。
func (a *AMQP) Fail(_ context.Context, job Job) error {
	return a.withAck(job, func(h ackHandle) error { return h.Nack(true) })
}

// Release 把未完成消息重新入队，供其他副本领取（K8s 滚动重启）。
func (a *AMQP) Release(_ context.Context, job Job) error {
	return a.withAck(job, func(h ackHandle) error { return h.Nack(true) })
}

func (a *AMQP) withAck(job Job, fn func(ackHandle) error) error {
	if job.ack == nil {
		return nil
	}
	return fn(job.ack)
}

func decodeAMQPJob(d amqp.Delivery) (Job, error) {
	var msg amqpMessage
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		return Job{}, errors.WithStack(err)
	}
	return Job{
		Key:     msg.Key,
		Kind:    msg.Kind,
		Payload: msg.Payload,
		At:      time.UnixMilli(msg.At),
		ack:     &amqpDeliveryAck{d: d},
	}, nil
}

func recvDelivery(ctx context.Context, chs []<-chan amqp.Delivery, block bool) (d amqp.Delivery, ok, closed, idle bool, err error) {
	if len(chs) == 0 {
		return d, false, false, true, nil
	}
	if len(chs) == 1 {
		return recvDelivery1(ctx, chs[0], block)
	}
	return recvDeliveryN(ctx, chs, block)
}

func recvDelivery1(ctx context.Context, ch <-chan amqp.Delivery, block bool) (d amqp.Delivery, ok, closed, idle bool, err error) {
	if block {
		select {
		case <-ctx.Done():
			return d, false, false, false, ctx.Err()
		case d, ok = <-ch:
			if !ok {
				return d, false, true, false, nil
			}
			return d, true, false, false, nil
		}
	}
	select {
	case d, ok = <-ch:
		if !ok {
			return d, false, true, false, nil
		}
		return d, true, false, false, nil
	default:
		return d, false, false, true, nil
	}
}

func recvDeliveryN(ctx context.Context, chs []<-chan amqp.Delivery, block bool) (d amqp.Delivery, ok, closed, idle bool, err error) {
	cases := make([]reflect.SelectCase, 0, len(chs)+2)
	ctxIdx := -1
	if block {
		ctxIdx = len(cases)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
	}
	for _, ch := range chs {
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)})
	}
	defIdx := -1
	if !block {
		defIdx = len(cases)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectDefault})
	}
	chosen, recv, recvOK := reflect.Select(cases)
	if chosen == ctxIdx {
		return d, false, false, false, ctx.Err()
	}
	if chosen == defIdx {
		return d, false, false, true, nil
	}
	if !recvOK {
		return d, false, true, false, nil
	}
	d = recv.Interface().(amqp.Delivery)
	return d, true, false, false, nil
}
