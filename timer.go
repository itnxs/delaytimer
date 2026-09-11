package delaytimer

import (
	"context"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	defaultConcurrency = 32
	defaultBatchSize   = 32
	defaultPoll        = 200 * time.Millisecond
)

// Option 配置 Timer
type Option func(*Timer)

// WithHandlers 注册到期 Handler（消费进程）
func WithHandlers(hs ...EventHandler) Option {
	return func(t *Timer) {
		t.toRegister = append(t.toRegister, hs...)
	}
}

// WithLogger 设置 logrus，可再配合 WithLogLevel
func WithLogger(l logrus.FieldLogger) Option {
	return func(t *Timer) {
		if l != nil {
			t.logger = l
		}
	}
}

// WithLogLevel 设置当前 logger 级别
func WithLogLevel(level logrus.Level) Option {
	return func(t *Timer) {
		applyLogLevel(t.logger, level)
	}
}

// WithConcurrency 同时执行 Handle 的上限
func WithConcurrency(n int) Option {
	return func(t *Timer) {
		if n > 0 {
			t.concurrency = n
		}
	}
}

// WithBatchSize 单次 Claim 条数
func WithBatchSize(n int) Option {
	return func(t *Timer) {
		if n > 0 {
			t.batchSize = n
		}
	}
}

// WithPollInterval Claim 为空时的等待间隔
func WithPollInterval(d time.Duration) Option {
	return func(t *Timer) {
		if d > 0 {
			t.pollInterval = d
		}
	}
}

// FailPolicy 业务失败时的处理
type FailPolicy int

const (
	// FailDiscard 丢弃（默认）：Ack，不放回存储
	FailDiscard FailPolicy = iota
	// FailRequeue 放回存储
	FailRequeue
)

// WithFailPolicy 业务失败（Handle 错误 / panic / 未注册 / 解码失败）的处理。停机 Release 仍归还。
func WithFailPolicy(p FailPolicy) Option {
	return func(t *Timer) {
		t.failPolicy = p
	}
}

func defaultLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetOutput(os.Stderr)
	l.SetLevel(logrus.InfoLevel)
	l.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	return l.WithField("pkg", "delaytimer")
}

func applyLogLevel(logger logrus.FieldLogger, level logrus.Level) {
	switch x := logger.(type) {
	case *logrus.Logger:
		x.SetLevel(level)
	case *logrus.Entry:
		x.Logger.SetLevel(level)
	}
}

// Timer 消费端：Claim 到期任务并调用 Handler。API 进程也可只 Subscribe 不 Run。
type Timer struct {
	backend      Backend
	toRegister   []EventHandler // WithHandlers 暂存，New 时写入 handlers
	handlers     map[EventName]EventHandler
	logger       logrus.FieldLogger
	concurrency  int
	batchSize    int
	pollInterval time.Duration
	failPolicy   FailPolicy
}

// New 创建 Timer。同一 EventName 只能注册一个 Handler；Bind 原型须为指针。
func New(backend Backend, opts ...Option) *Timer {
	if backend == nil {
		panic(ErrNilBackend)
	}
	t := &Timer{
		backend:      backend,
		logger:       defaultLogger(),
		concurrency:  defaultConcurrency,
		batchSize:    defaultBatchSize,
		pollInterval: defaultPoll,
		handlers:     make(map[EventName]EventHandler),
	}
	for _, opt := range opts {
		opt(t)
	}
	t.bindHandlers()
	t.toRegister = nil
	return t
}

func (t *Timer) bindHandlers() {
	for _, h := range t.toRegister {
		if h == nil {
			panic("delaytimer: handler is nil")
		}
		name := h.EventName()
		if name == "" {
			panic(ErrEmptyEventName)
		}
		if _, ok := t.handlers[name]; ok {
			panic(errors.WithMessagef(ErrDuplicateEventName, "%s", name))
		}
		p := h.NewParams()
		if !isPointerParams(p) {
			panic(errors.WithMessagef(ErrNotPointerParams, "%T", p))
		}
		t.handlers[name] = h
	}
}

// Run 从后端领取到期任务并分发。ctx 取消后返回。
func (t *Timer) Run(ctx context.Context) error {
	sem := make(chan struct{}, t.concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	bg := context.WithoutCancel(ctx)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		jobs, err := t.backend.Claim(ctx, t.batchSize)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			t.logger.WithError(err).Error("claim failed")
		}
		if err != nil || len(jobs) == 0 {
			if !sleepCtx(ctx, t.pollInterval) {
				return ctx.Err()
			}
			continue
		}

		for i := 0; i < len(jobs); i++ {
			select {
			case <-ctx.Done():
				for _, job := range jobs[i:] {
					t.releaseJob(bg, job)
				}
				return ctx.Err()
			case sem <- struct{}{}:
				job := jobs[i]
				wg.Add(1)
				go func(job Job) {
					defer wg.Done()
					defer func() { <-sem }()
					t.dispatch(ctx, job)
				}(job)
			}
		}
	}
}

func (t *Timer) dispatch(ctx context.Context, job Job) {
	opCtx := context.WithoutCancel(ctx)
	stopTouch := t.startTouch(opCtx, job)
	defer stopTouch()

	defer func() {
		if r := recover(); r != nil {
			t.recoverLog("handler panic", r, logrus.Fields{"kind": job.Kind})
			t.failJob(opCtx, job)
		}
	}()

	h, ok := t.handlers[EventName(job.Kind)]
	if !ok {
		t.abort(opCtx, job, nil, "undefined event")
		return
	}

	inst := h.NewParams()
	if !isPointerParams(inst) {
		t.abort(opCtx, job, nil, "clone event failed")
		return
	}
	if err := decodeParams(job.Payload, inst); err != nil {
		t.abort(opCtx, job, err, "unmarshal event failed")
		return
	}
	t.logger.WithFields(logrus.Fields{
		"kind": job.Kind,
		"key":  job.Key,
		"at":   job.At,
	}).Info("handle timer event")
	if err := h.Handle(ctx, inst); err != nil {
		if ctx.Err() != nil {
			t.releaseJob(opCtx, job)
			return
		}
		t.abort(opCtx, job, err, "event handle error")
		return
	}
	if err := t.backend.Ack(opCtx, job); err != nil {
		t.logger.WithError(err).WithFields(logrus.Fields{"kind": job.Kind, "key": job.Key}).Error("ack failed")
	}
}

func (t *Timer) recoverLog(msg string, r any, extra logrus.Fields) {
	e := t.logger.WithField("recover", r).WithField("stack", string(debug.Stack()))
	if extra != nil {
		e = e.WithFields(extra)
	}
	e.Error(msg)
}

func (t *Timer) recoverWrap(msg string, extra logrus.Fields) func() {
	return func() {
		if r := recover(); r != nil {
			t.recoverLog(msg, r, extra)
		}
	}
}

func (t *Timer) abort(ctx context.Context, job Job, err error, msg string) {
	log := t.logger.WithFields(logrus.Fields{"kind": job.Kind, "key": job.Key})
	if err != nil {
		log = log.WithError(err)
	}
	log.Error(msg)
	t.failJob(ctx, job)
}

func (t *Timer) failJob(ctx context.Context, job Job) {
	t.safeCall("fail job panic", func() {
		if t.failPolicy == FailRequeue {
			_ = t.backend.Fail(ctx, job)
			return
		}
		_ = t.backend.Ack(ctx, job)
	})
}

type releaser interface {
	Release(ctx context.Context, job Job) error
}

func (t *Timer) releaseJob(ctx context.Context, job Job) {
	t.safeCall("release job panic", func() {
		if r, ok := t.backend.(releaser); ok {
			_ = r.Release(ctx, job)
			return
		}
		_ = t.backend.Fail(ctx, job)
	})
}

func (t *Timer) safeCall(msg string, fn func()) {
	defer t.recoverWrap(msg, nil)()
	fn()
}

type toucher interface {
	Touch(ctx context.Context, job Job) error
	TouchInterval() time.Duration
}

func (t *Timer) startTouch(ctx context.Context, job Job) func() {
	tw, ok := t.backend.(toucher)
	if !ok {
		return func() {}
	}
	interval := tw.TouchInterval()
	if interval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		defer t.recoverWrap("touch panic", logrus.Fields{"key": job.Key})()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = tw.Touch(ctx, job)
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
