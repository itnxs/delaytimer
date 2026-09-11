package delaytimer

import (
	"context"
	"time"
)

const failRequeueDelay = 200 * time.Millisecond

// Clock 当前时间，测试可注入
type Clock func() time.Time

type (
	// Store 任务存储
	Store interface {
		// Schedule 写入或按 Key 覆盖
		Schedule(ctx context.Context, task Task) error
		// Cancel 取消尚未完成的任务；不支持时返回 ErrCancelUnsupported
		Cancel(ctx context.Context, key string) error
		// Claim 领取最多 num 条已到期任务；无任务时可阻塞至 ctx 取消
		Claim(ctx context.Context, num int) ([]Task, error)
		// Ack 领取后确认。AMQP 在 Claim 内立刻 Ack，避免 unack 堵住消费；
		// Timer 仍会再调一次（已 Ack 则为空操作）。Handle 失败后的重入队 / 抛弃后续统一处理。
		Ack(ctx context.Context, task Task) error
		// Fail 处理失败。Timer 在 FailRequeue 时调用；FailDiscard 时走 Ack。
		Fail(ctx context.Context, task Task) error
	}

	// Task 后端任务
	Task struct {
		Key     string    // Cancel 身份，通常为 TaskName(Kind, Payload)
		Kind    string    // Event
		Payload string    // JSON，字段顺序与结构体声明一致
		At      time.Time // 到期时间
		ack     ack       // ack处理
	}

	// ack  ack处理
	ack interface {
		Ack() error
		Nack(requeue bool) error
	}
)
