package delaytimer

import (
	"context"
	"os"
	"runtime/debug"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// defaultLogger 默认日志
func defaultLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetOutput(os.Stderr)
	l.SetLevel(logrus.InfoLevel)
	return l.WithField("pkg", "timer")
}

// Timer 消费端Timer
type Timer struct {
	concurrency int
	batchSize   int
	store       Store
	registers   []EventHandler
	handlers    map[Event]EventHandler
	logger      logrus.FieldLogger
	interval    time.Duration
}

// New 创建 Timer
func New(store Store, options ...Option) *Timer {
	if store == nil {
		panic(errors.WithStack(ErrNilTaskStore))
	}

	t := &Timer{
		store:       store,
		logger:      defaultLogger(),
		concurrency: defaultConcurrency,
		batchSize:   defaultBatchSize,
		interval:    defaultInterval,
		handlers:    make(map[Event]EventHandler),
	}

	for _, option := range options {
		option(t)
	}

	t.bindHandlers()
	return t
}

// bindHandlers 绑定处理程序
func (t *Timer) bindHandlers() {
	for _, h := range t.registers {
		if h == nil {
			panic(errors.WithStack(ErrNilEventHandler))
		}

		if h.Event().IsNil() {
			panic(errors.WithStack(ErrEmptyEvent))
		}

		name := h.Event()
		if _, ok := t.handlers[name]; ok {
			panic(errors.Wrapf(ErrDuplicateEventName, "%s", name))
		}

		p := h.NewParams()
		if p == nil {
			panic(errors.WithStack(ErrNilParam))
		}

		if !isPointerParams(p) {
			panic(errors.Wrapf(ErrNotPointerParams, "%T", p))
		}

		t.handlers[name] = h
	}
}

// Run 启动服务处理
func (t *Timer) Run(ctx context.Context) error {
	var wg errgroup.Group

	wg.SetLimit(t.concurrency)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		tasks, err := t.store.Claim(ctx, t.batchSize)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			t.logger.WithError(err).Error("claim failed")
		}

		if err != nil || len(tasks) == 0 {
			if !t.sleep(ctx) {
				return ctx.Err()
			}
			continue
		}

		for i := 0; i < len(tasks); i++ {
			task := tasks[i]
			wg.Go(func() error {
				if err = t.dispatch(ctx, task); err != nil {
					t.logger.WithError(err).WithFields(logrus.Fields{
						"kind": task.Kind,
						"key":  task.Key,
					}).Error("task dispatch failed")
				}
				return nil
			})
		}

		if err = wg.Wait(); err != nil {
			return errors.WithStack(err)
		}
	}
}

// dispatch 任务分发处理
func (t *Timer) dispatch(ctx context.Context, task Task) error {
	defer func() {
		if rev := recover(); rev != nil {
			t.logger.WithFields(logrus.Fields{
				"error": rev,
				"stack": string(debug.Stack()),
			}).Error("task dispatch panic")
		}
	}()

	h, ok := t.handlers[Event(task.Kind)]
	if !ok {
		return errors.Wrapf(ErrEmptyEvent, "kind: %s", task.Kind)
	}

	inst := h.NewParams()
	if !isPointerParams(inst) {
		return errors.WithStack(ErrNotPointerParams)
	}

	if err := decodeParams(task.Payload, inst); err != nil {
		return errors.WithStack(ErrUnmarshalParams)
	}

	t.logger.WithFields(logrus.Fields{
		"kind": task.Kind,
		"key":  task.Key,
		"at":   task.At,
	}).Info("handle task")

	if err := h.Handle(ctx, inst); err != nil {
		return err
	}

	if err := t.store.Ack(ctx, task); err != nil {
		t.logger.WithError(err).WithFields(logrus.Fields{
			"kind": task.Kind,
			"key":  task.Key,
		}).Error("task ack failed")
	}

	return nil
}

// sleep 暂停时间
func (t *Timer) sleep(ctx context.Context) bool {
	timer := time.NewTimer(t.interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
