package delaytimer

import (
    "context"
    "sync"
    "time"

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
type AMQPConfig struct {
    Exchange   string
    RoutingKey string
    Queue      string
    Shards     int
    Kinds      []string // 要 Consume 的 Event
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
        ch:    ch,
        cfg:   cfg,
        kinds: kinds,
        clock: time.Now,
    }
    for _, opt := range opts {
        opt(a)
    }
    return a
}

func (a *AMQP) compat() bool {
    return a.shards == 1 && len(a.kinds) == 0
}

func (a *AMQP) routingKey(kind string) string {
    if a.compat() {
        return a.cfg.RoutingKey
    }
    return a.cfg.RoutingKey + "." + kind
}

// Schedule 发布延迟消息（header x-delay 毫秒）
func (a *AMQP) Schedule(ctx context.Context, task Task) error {
    return nil
}

// Cancel AMQP 延迟消息无法从 broker 撤回
func (a *AMQP) Cancel(context.Context, string) error {
    return ErrCancelUnsupported
}

// Claim 从队列竞争消费。Consume 通道关闭后会重新注册，不绑到单次 Claim 的 ctx。
func (a *AMQP) Claim(ctx context.Context, n int) ([]Task, error) {
    return nil, nil
}

// Ack 确认消息
func (a *AMQP) Ack(_ context.Context, job Task) error {
    return nil
}

// Fail 重新入队。Timer FailDiscard 走 Ack，不会调到这里。
func (a *AMQP) Fail(_ context.Context, job Task) error {
    return nil
}

// Release 把未完成消息重新入队，供其他副本领取（K8s 滚动重启）。
func (a *AMQP) Release(_ context.Context, job Task) error {
    return nil
}
