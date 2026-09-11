package delaytimer

import (
	"context"
	"strings"
	"time"
)

const failRequeueDelay = 200 * time.Millisecond

// taskKeySep 分隔 Kind 与 Payload。两个 ASCII 单元分隔符，Event 名和 JSON 都不会出现。
// 只按第一处切分。
const taskKeySep = "\x1f\x1f"

func encodeTaskKey(kind, payload string) string {
	return kind + taskKeySep + payload
}

func decodeTaskKey(key string) (kind, payload string) {
	kind, payload, _ = strings.Cut(key, taskKeySep)
	return
}

// Clock 当前时间，测试可注入
type Clock func() time.Time

type (
	// Store 任务存储
	Store interface {
		// Schedule 写入或按 Key 覆盖
		Schedule(ctx context.Context, task Task) error
		// Cancel 取消尚未完成的任务；不支持时返回 ErrCancelUnsupported
		Cancel(ctx context.Context, key string) error
		// Claim 领取最多 num 条已到期任务。无任务时是否阻塞由后端决定：
		// Memory 阻塞至有到期任务或 ctx 取消；Redis 立即返回空；AMQP 等第一条投递。
		Claim(ctx context.Context, num int) ([]Task, error)
		// Ack 领取后确认。Memory / Redis 领取时已删除，为空操作。
		// AMQP 在 Claim 内已 Ack，Timer 再调也是空操作。
		Ack(ctx context.Context, task Task) error
		// Fail Handler 失败后重新入队。Timer 仅在 FailRequeue 时调用。
		Fail(ctx context.Context, task Task) error
		// Close 释放后端。Memory / Redis 为空操作；AMQP 关闭内部打开的 Channel。
		Close() error
	}

	// Task 后端任务
	Task struct {
		Key     string    // Cancel 身份：Event + "\x1f\x1f" + JSON 载荷
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
