package delaytimer

import (
	"context"
	"os"
	"runtime/debug"
	"sync"
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
	return l.WithField("pkg", "delaytimer")
}

// Timer 唯一启动入口。换 Memory / Redis / AMQP 只换 Store。
type Timer struct {
	concurrency int
	batchSize   int
	store       Store
	bus         Bus
	registers   []EventHandler
	handlers    map[Event]EventHandler
	logger      logrus.FieldLogger
	interval    time.Duration
	mu          sync.Mutex
	cancel      context.CancelFunc
	closed      bool
	running     bool
	done        sync.WaitGroup
	failPolicy  FailPolicy
}

// New 创建 Timer。store 不能为空，否则 panic。
// 默认 FailDiscard、无 Bus；SetEvent / DelEvent 同步写 Store。
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
		failPolicy:  FailDiscard,
		handlers:    make(map[Event]EventHandler),
	}

	for _, option := range options {
		option(t)
	}

	t.bindHandlers()
	if t.bus != nil {
		subscribe(t, t.bus)
	}
	return t
}

// Start 后台领取到期任务并执行 Handler。ctx 取消或 Close 后退出。
// Close 会取消并等待循环结束。重复调用返回 ErrAlreadyRunning。
func (t *Timer) Start(ctx context.Context) error {
	ctx, err := t.begin(ctx)
	if err != nil {
		return err
	}
	t.done.Add(1)
	go func() {
		defer t.done.Done()
		defer t.stop()
		_ = t.loop(ctx)
	}()
	return nil
}

// Close 停止 Start：取消并等待循环退出，然后关闭 Store（AMQP 会关掉内部 Channel）。
// 若启用了 Bus 则关闭其通道。可重复调用。关闭后 SetEvent / DelEvent 返回 ErrChannelClosed。
func (t *Timer) Close() {
	if t == nil {
		return
	}
	t.stop()
	t.done.Wait()
	if t.store != nil {
		if err := t.store.Close(); err != nil {
			t.logger.WithError(err).Error("store close failed")
		}
	}
}

// SetEvent 投递延迟任务。未配置 Bus 时同步写入 Store；
// 传入 WithBus 时只表示进入通道，真正 Schedule 在订阅回调里。
// Close 之后返回 ErrChannelClosed。
func (t *Timer) SetEvent(at time.Time, p Params) error {
	if p == nil {
		return ErrNilParam
	}
	if t.isClosed() {
		return ErrChannelClosed
	}
	if t.bus != nil {
		return t.bus.SetEvent(at, p)
	}
	return t.persistSet(at, p)
}

// DelEvent 取消尚未完成的任务。未配置 Bus 时同步从 Store 取消；
// 传入 WithBus 时只表示进入通道。Close 之后返回 ErrChannelClosed。
func (t *Timer) DelEvent(p Params) error {
	if p == nil {
		return ErrNilParam
	}
	if t.isClosed() {
		return ErrChannelClosed
	}
	if t.bus != nil {
		return t.bus.DelEvent(p)
	}
	return t.persistDel(p)
}

func (t *Timer) persistSet(at time.Time, p Params) error {
	payload, err := encodeParams(p)
	if err != nil {
		return err
	}

	task := t.newTask(at, p, payload)
	if err := t.store.Schedule(context.Background(), task); err != nil {
		return err
	}

	t.logger.WithFields(logrus.Fields{
		"time": task.At,
		"name": task.Kind,
		"key":  task.Key,
	}).Info("set timer event")
	return nil
}

func (t *Timer) persistDel(p Params) error {
	payload, err := encodeParams(p)
	if err != nil {
		return err
	}
	key := t.newTask(time.Now(), p, payload).Key
	if err := t.store.Cancel(context.Background(), key); err != nil {
		return err
	}
	t.logger.WithFields(logrus.Fields{
		"name": p.Event(),
		"key":  key,
	}).Info("del timer event")
	return nil
}

func (t *Timer) isClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func (t *Timer) stop() {
	t.mu.Lock()
	t.closed = true
	cancel := t.cancel
	t.cancel = nil
	t.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if t.bus != nil {
		if ch := t.bus.SetEvents(); ch != nil {
			ch.Close()
		}
		if ch := t.bus.DelEvents(); ch != nil {
			ch.Close()
		}
	}
}

func (t *Timer) begin(parent context.Context) (context.Context, error) {
	ctx, cancel := context.WithCancel(parent)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		cancel()
		return nil, ErrChannelClosed
	}
	if t.running {
		cancel()
		return nil, ErrAlreadyRunning
	}
	t.running = true
	t.cancel = cancel
	return ctx, nil
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
			panic(errors.Wrapf(ErrDuplicateEvent, "%s", name))
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

func (t *Timer) loop(ctx context.Context) error {
	var eg errgroup.Group
	eg.SetLimit(t.concurrency)
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
			eg.Go(func() error {
				if err := t.dispatch(ctx, task); err != nil {
					t.logger.WithError(err).WithFields(logrus.Fields{
						"kind": task.Kind,
						"key":  task.Key,
					}).Error("task dispatch failed")
				}
				return nil
			})
		}

		if err := eg.Wait(); err != nil {
			return errors.WithStack(err)
		}
	}
}

// dispatch 任务分发。Timer 会再调一次 Ack（AMQP Claim 时已 Ack，此处为空操作）。
// 未知 Kind、解码失败直接抛弃；Handle 失败按 FailPolicy 抛弃或重新入队。
func (t *Timer) dispatch(ctx context.Context, task Task) (err error) {
	defer func() {
		if rev := recover(); rev != nil {
			t.logger.WithFields(logrus.Fields{
				"error": rev,
				"stack": string(debug.Stack()),
			}).Error("task dispatch panic")
			err = errors.Errorf("task dispatch panic: %v", rev)
		}
	}()

	if err := t.store.Ack(ctx, task); err != nil {
		return errors.New("task ack failed")
	}

	h, ok := t.handlers[Event(task.Kind)]
	if !ok {
		return errors.Wrapf(ErrUnknownKind, "%s", task.Kind)
	}

	inst := h.NewParams()
	if !isPointerParams(inst) {
		return errors.WithStack(ErrNilParam)
	}

	if err := decodeParams(task.Payload, inst); err != nil {
		return err
	}

	t.logger.WithFields(logrus.Fields{
		"kind": task.Kind,
		"key":  task.Key,
		"at":   task.At,
	}).Info("handle task")

	if err := h.Handle(ctx, inst); err != nil {
		return t.handleFail(ctx, task, err)
	}

	return nil
}

// handleFail 仅处理 Handler 返回的错误。
func (t *Timer) handleFail(ctx context.Context, task Task, cause error) error {
	if t.failPolicy != FailRequeue {
		return cause
	}
	if err := t.store.Fail(ctx, task); err != nil {
		t.logger.WithError(err).WithFields(logrus.Fields{
			"kind": task.Kind,
			"key":  task.Key,
		}).Error("task fail requeue failed")
	}
	return cause
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
