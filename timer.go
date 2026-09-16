package delaytimer

import (
	"context"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/reactivex/rxgo/v2"
	"github.com/sirupsen/logrus"
)

// defaultLogger 默认日志
func defaultLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetOutput(os.Stderr)
	l.SetLevel(logrus.ErrorLevel)
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
	done          sync.WaitGroup
	busDone       sync.WaitGroup
	failPolicy    FailPolicy
	handleTimeout time.Duration
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

// Start 后台领取到期任务并执行 Handler。ctx 取消或 Close 后停止领取；
// 已领取且进入管道的任务会继续处理完。重复调用返回 ErrAlreadyRunning。
func (t *Timer) Start(ctx context.Context) error {
	ctx, err := t.begin(ctx)
	if err != nil {
		return err
	}
	go func() {
		defer t.done.Done()
		defer t.stop()
		t.loop(ctx)
	}()
	return nil
}

// Close 停止领取：唤醒等待中的 Claim，把已拿到的任务处理完，再关闭 Store。
// 若启用了 Bus 则先排空并等待订阅结束。可重复调用。关闭后 SetEvent / DelEvent 返回 ErrChannelClosed。
func (t *Timer) Close() {
	if t == nil {
		return
	}
	t.stop()
	t.done.Wait()
	t.busDone.Wait()
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
		"at":   task.At,
		"kind": task.Kind,
		"key":  task.Key,
	}).Info("set event")
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
		"kind": p.Event(),
		"key":  key,
	}).Info("del event")
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
	t.done.Add(1)
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

// loop 领取任务并交给 RxGo 并发 Handle。发送阻塞形成背压，在途约等于 concurrency。
func (t *Timer) loop(ctx context.Context) {
	workCtx := context.WithoutCancel(ctx)
	ch := make(chan rxgo.Item)
	go func() {
		defer close(ch)
		t.produceClaims(ctx, ch)
	}()
	<-rxgo.FromChannel(ch, rxgo.WithBackPressureStrategy(rxgo.Block)).
		Map(
			func(_ context.Context, i interface{}) (interface{}, error) {
				task, ok := i.(Task)
				if !ok {
					return i, nil
				}
				if err := t.dispatch(workCtx, task); err != nil {
					fields := logrus.Fields{
						"kind": task.Kind,
						"key":  task.Key,
					}
					if errors.Is(err, ErrUnknownKind) || errors.Is(err, ErrUnmarshalParams) {
						t.logger.WithError(err).WithFields(fields).Warn("task skipped")
					} else {
						t.logger.WithError(err).WithFields(fields).Error("task dispatch failed")
					}
				}
				return i, nil
			},
			rxgo.WithPool(t.concurrency),
			rxgo.WithErrorStrategy(rxgo.ContinueOnError),
			rxgo.WithBackPressureStrategy(rxgo.Block),
		).
		ForEach(func(interface{}) {}, func(err error) {
			t.logger.WithError(err).Error("consume stream failed")
		}, func() {})
}

func (t *Timer) produceClaims(ctx context.Context, next chan<- rxgo.Item) {
	for {
		if ctx.Err() != nil {
			return
		}
		tasks, err := t.store.Claim(ctx, t.batchSize)
		for i := range tasks {
			if !rxgo.Of(tasks[i]).SendContext(context.Background(), next) {
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			t.logger.WithError(err).Error("claim failed")
		}
		if err != nil || len(tasks) == 0 {
			if !t.sleep(ctx) {
				return
			}
		}
	}
}

// dispatch 任务分发。Timer 会再调一次 Ack（AMQP Claim 时已 Ack，此处为空操作）。
// 未知 Kind、解码失败直接抛弃；Handle 失败按 FailPolicy 抛弃或重新入队。
func (t *Timer) dispatch(ctx context.Context, task Task) (err error) {
	defer func() {
		if rev := recover(); rev != nil {
			t.logger.WithFields(logrus.Fields{
				"panic": rev,
				"stack": string(debug.Stack()),
			}).Error("task dispatch panic")
			err = errors.Errorf("task dispatch panic: %v", rev)
		}
	}()

	if err := t.store.Ack(ctx, task); err != nil {
		return errors.Wrap(ErrAckFailed, err.Error())
	}

	h, ok := t.handlers[Event(task.Kind)]
	if !ok {
		return errors.Wrapf(ErrUnknownKind, "%s", task.Kind)
	}

	inst := h.NewParams()
	if inst == nil {
		return errors.WithStack(ErrNilParam)
	}
	if !isPointerParams(inst) {
		return errors.Wrapf(ErrNotPointerParams, "%T", inst)
	}

	if err := decodeParams(task.Payload, inst); err != nil {
		return err
	}

	t.logger.WithFields(logrus.Fields{
		"kind": task.Kind,
		"key":  task.Key,
		"at":   task.At,
	}).Info("handle task")

	handleCtx, cancel := t.handleContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	if err := h.Handle(handleCtx, inst); err != nil {
		return t.handleFail(ctx, task, err)
	}

	return nil
}

func (t *Timer) handleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if t.handleTimeout <= 0 {
		return ctx, nil
	}
	return context.WithTimeout(ctx, t.handleTimeout)
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
