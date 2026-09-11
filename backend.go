package delaytimer

import (
	"context"
	"time"
)

// Clock 当前时间，测试可注入
type Clock func() time.Time

// Job 后端任务，由 EventParams 序列化而来。
// ack 仅 AMQP 使用（broker 确认）。
type Job struct {
	Key     string    // Cancel 身份，通常为 JobKey(Kind, Payload)
	Kind    string    // EventName
	Payload string    // JSON，字段顺序与结构体声明一致
	At      time.Time // 到期时间
	ack     ackHandle
}

type ackHandle interface {
	Ack() error
	Nack(requeue bool) error
}

// JobKey 事件名与 JSON 载荷组成 Cancel 身份，同名同参数才能删掉
func JobKey(kind, payload string) string {
	return kind + ":" + payload
}

const failRequeueDelay = 200 * time.Millisecond

func (j Job) withKey() Job {
	if j.Key == "" {
		j.Key = JobKey(j.Kind, j.Payload)
	}
	return j
}

// Backend 延迟任务存储与领取
type Backend interface {
	// Schedule 写入或按 Key 覆盖
	Schedule(ctx context.Context, job Job) error
	// Cancel 取消尚未完成的任务；不支持时返回 ErrCancelUnsupported
	Cancel(ctx context.Context, key string) error
	// Claim 领取最多 n 条已到期任务；无任务时可阻塞至 ctx 取消
	Claim(ctx context.Context, n int) ([]Job, error)
	// Ack 处理成功，从后端移除
	Ack(ctx context.Context, job Job) error
	// Fail 处理失败。Timer 在 FailRequeue 时调用；FailDiscard 时走 Ack。
	Fail(ctx context.Context, job Job) error
}
