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

// Option 配置 Timer
type Option func(*Timer)

// WithHandlers 注册到期 Handler（消费进程）
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

// WithPollInterval Claim 为空时的等待间隔
func WithPollInterval(duration time.Duration) Option {
	return func(t *Timer) {
		if duration > 0 {
			t.interval = duration
		}
	}
}
