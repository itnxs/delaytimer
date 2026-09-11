package delaytimer

import (
    "context"
    "fmt"
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
        // Ack 处理成功，从后端移除
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

func (j Task) withKey() Task {
    if j.Key == "" {
        j.Key = createTaskKey(j.Kind, j.Payload)
    }
    return j
}

// createTaskKey 事件名与JSON载荷组成Cancel身份，同名同参数才能删掉
func createTaskKey(kind, payload string) string {
    return fmt.Sprintf("%s:%s", kind, payload)
}
