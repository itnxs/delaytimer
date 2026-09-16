package delaytimer

import (
	"time"

	"github.com/sirupsen/logrus"
)

const (
	defaultConcurrency = 32
	defaultBatchSize   = 32
	defaultInterval    = 200 * time.Millisecond
)

// FailPolicy Handler 返回错误后的处理。未知 Kind / 解码失败始终抛弃，不受此配置影响。
type FailPolicy int

const (
	// FailDiscard 抛弃任务（默认）
	FailDiscard FailPolicy = iota
	// FailRequeue 约 200ms 后重新入队
	FailRequeue
)

// Option 配置 Timer
type Option func(*Timer)

// WithFailPolicy 仅在 Handler 处理失败后生效：抛弃或重新入队。
func WithFailPolicy(p FailPolicy) Option {
	return func(t *Timer) {
		t.failPolicy = p
	}
}

// WithBus 启用可选 Rx 通道。Timer.SetEvent / DelEvent 只表示进入通道，
// 真正写入 Store 在订阅回调里。默认不启用，此时 SetEvent / DelEvent 同步写 Store。
func WithBus(bus Bus) Option {
	return func(t *Timer) {
		if bus != nil {
			t.bus = bus
		}
	}
}

// WithHandlers 注册到期 Handler
func WithHandlers(handlers ...EventHandler) Option {
	return func(t *Timer) {
		t.registers = append(t.registers, handlers...)
	}
}

// WithLogger 设置 Logger
func WithLogger(logger logrus.FieldLogger) Option {
	return func(t *Timer) {
		if logger != nil {
			t.logger = logger
		}
	}
}

// WithLogLevel 设置当前 logger 级别
func WithLogLevel(level logrus.Level) Option {
	return func(t *Timer) {
		switch x := t.logger.(type) {
		case *logrus.Logger:
			x.SetLevel(level)
		case *logrus.Entry:
			x.Logger.SetLevel(level)
		}
	}
}

// WithConcurrency 同时执行 Handle 的上限
func WithConcurrency(count int) Option {
	return func(t *Timer) {
		if count > 0 {
			t.concurrency = count
		}
	}
}

// WithBatchSize 单次 Claim 条数
func WithBatchSize(size int) Option {
	return func(t *Timer) {
		if size > 0 {
			t.batchSize = size
		}
	}
}

// WithPollInterval Claim 无任务或失败后的等待间隔。Redis Claim 不阻塞，主要靠此项轮询。
func WithPollInterval(duration time.Duration) Option {
	return func(t *Timer) {
		if duration > 0 {
			t.interval = duration
		}
	}
}

// WithHandleTimeout 单条 Handle 最长执行时间。超时后取消传给 Handler 的 ctx，
// 再按 FailPolicy 抛弃或重入队。默认 0 表示不限制。Handler 需响应 ctx 才会停。
func WithHandleTimeout(d time.Duration) Option {
	return func(t *Timer) {
		if d > 0 {
			t.handleTimeout = d
		}
	}
}
