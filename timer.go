package delaytimer

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/reactivex/rxgo/v2"
)

// Timer 唯一启动入口。换 Memory / Redis / AMQP 只换 Store。
type Timer struct {
	concurrency   int
	batchSize     int
	store         Store
	bus           Bus
	registers     []EventHandler
	handlers      map[Event]EventHandler
	logger        Logger
	interval      time.Duration
	mu            sync.Mutex
	cancel        context.CancelFunc
	closed        bool
	running       bool
	done          sync.WaitGroup
	busDone       sync.WaitGroup
	failPolicy    FailPolicy
	handleTimeout time.Duration
	onHandleError func(ctx context.Context, p Params, err error)
}

// Must 创建 Timer。store 不能为空，否则 panic。
// 默认 FailDiscard、无 Bus；SetEvent / DelEvent 同步写 Store。
func Must(store Store, options ...Option) *Timer {
	if store == nil {
		panic(ErrNilTaskStore)
	}

	t := &Timer{
		store:       store,
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
			logError(t.logger, context.Background(), "store close failed", err)
		}
	}
}

// SetEvent 投递延迟任务。未配置 Bus 时同步写入 Store；
// 传入 WithBus 时只表示进入通道，真正 Schedule 在订阅回调里。
// Close 之后返回 ErrChannelClosed。
func (t *Timer) SetEvent(ctx context.Context, at time.Time, p Params) error {
	if p == nil {
		return ErrNilParam
	}
	if t.isClosed() {
		return ErrChannelClosed
	}
	if t.bus != nil {
		return t.bus.SetEvent(ctx, at, p)
	}
	return t.persistSet(ctx, at, p)
}

// DelEvent 取消尚未完成的任务。未配置 Bus 时同步从 Store 取消；
// 传入 WithBus 时只表示进入通道。Close 之后返回 ErrChannelClosed。
func (t *Timer) DelEvent(ctx context.Context, p Params) error {
	if p == nil {
		return ErrNilParam
	}
	if t.isClosed() {
		return ErrChannelClosed
	}
	if t.bus != nil {
		return t.bus.DelEvent(ctx, p)
	}
	return t.persistDel(ctx, p)
}

func (t *Timer) persistSet(ctx context.Context, at time.Time, p Params) error {
	payload, err := encodeParams(p)
	if err != nil {
		return err
	}

	task := t.newTask(at, p, payload)
	if err := t.store.Schedule(ctx, task); err != nil {
		return err
	}

	logInfo(t.logger, ctx, "set event",
		"at", task.At,
		"kind", task.Kind,
		"key", task.Key,
	)
	return nil
}

func (t *Timer) persistDel(ctx context.Context, p Params) error {
	payload, err := encodeParams(p)
	if err != nil {
		return err
	}
	key := t.newTask(time.Now(), p, payload).Key
	if err := t.store.Cancel(ctx, key); err != nil {
		return err
	}
	logInfo(t.logger, ctx, "del event",
		"kind", p.Event(),
		"key", key,
	)
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
			panic(ErrNilEventHandler)
		}

		if h.Event().IsNil() {
			panic(ErrEmptyEvent)
		}

		name := h.Event()
		if _, ok := t.handlers[name]; ok {
			panic(fmt.Errorf("duplicate handler event=%s: %w", name, ErrDuplicateEvent))
		}

		p := h.NewParams()
		if p == nil {
			panic(ErrNilParam)
		}

		if !isPointerParams(p) {
			panic(fmt.Errorf("handler params must be pointer (event=%s got=%T): %w", name, p, ErrNotPointerParams))
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
					if errors.Is(err, ErrUnknownKind) || errors.Is(err, ErrUnmarshalParams) {
						logInfo(t.logger, workCtx, "task skipped",
							"error", err,
							"kind", task.Kind,
							"key", task.Key,
						)
					} else {
						logError(t.logger, workCtx, "task dispatch failed", err,
							"kind", task.Kind,
							"key", task.Key,
						)
					}
				}
				return i, nil
			},
			rxgo.WithPool(t.concurrency),
			rxgo.WithErrorStrategy(rxgo.ContinueOnError),
			rxgo.WithBackPressureStrategy(rxgo.Block),
		).
		ForEach(func(interface{}) {}, func(err error) {
			logError(t.logger, workCtx, "consume stream failed", err)
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
			logError(t.logger, ctx, "claim failed", err)
		}
		if err != nil || len(tasks) == 0 {
			if !t.sleep(ctx) {
				return
			}
		}
	}
}

// dispatch 任务分发。Timer 会再调一次 Ack（AMQP Claim 时已 Ack，此处为空操作）。
// 未知 Kind、解码失败直接抛弃；Handle 失败或解码后 panic 按 FailPolicy 抛弃或重新入队。
func (t *Timer) dispatch(ctx context.Context, task Task) (err error) {
	var inst Params
	decoded := false
	defer func() {
		if rev := recover(); rev != nil {
			panicErr := fmt.Errorf("task dispatch panic: %v", rev)
			logError(t.logger, ctx, "task dispatch panic", panicErr,
				"panic", rev,
				"stack", string(debug.Stack()),
			)
			if decoded {
				err = t.handleFail(ctx, inst, task, panicErr)
				return
			}
			err = panicErr
		}
	}()

	if err := t.store.Ack(ctx, task); err != nil {
		return fmt.Errorf("ack task failed (kind=%s key=%s): %v: %w", task.Kind, task.Key, err, ErrAckFailed)
	}

	h, ok := t.handlers[Event(task.Kind)]
	if !ok {
		return fmt.Errorf("unknown task kind=%s key=%s: %w", task.Kind, task.Key, ErrUnknownKind)
	}

	inst = h.NewParams()
	if inst == nil {
		return ErrNilParam
	}
	if !isPointerParams(inst) {
		return fmt.Errorf("new params must be pointer (kind=%s got=%T): %w", task.Kind, inst, ErrNotPointerParams)
	}

	if err := decodeParams(task.Payload, inst); err != nil {
		return err
	}
	decoded = true

	logInfo(t.logger, ctx, "handle task",
		"kind", task.Kind,
		"key", task.Key,
		"at", task.At,
	)

	handleCtx, cancel := t.handleContext(ctx)
	if cancel != nil {
		defer cancel()
	}

	if err := h.Handle(handleCtx, inst); err != nil {
		return t.handleFail(ctx, inst, task, err)
	}

	return nil
}

func (t *Timer) handleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if t.handleTimeout <= 0 {
		return ctx, nil
	}
	return context.WithTimeout(ctx, t.handleTimeout)
}

// handleFail 处理 Handler 失败：返回错误或已解码后的 panic。
func (t *Timer) handleFail(ctx context.Context, p Params, task Task, cause error) error {
	t.notifyHandleError(ctx, p, cause)
	if t.failPolicy != FailRequeue {
		return cause
	}
	if err := t.store.Fail(ctx, task); err != nil {
		logError(t.logger, ctx, "task fail requeue failed", err,
			"kind", task.Kind,
			"key", task.Key,
		)
	}
	return cause
}

func (t *Timer) notifyHandleError(ctx context.Context, p Params, cause error) {
	if t.onHandleError == nil {
		return
	}
	defer func() {
		if rev := recover(); rev != nil {
			panicErr := fmt.Errorf("on handle error callback panic: %v", rev)
			logError(t.logger, ctx, "on handle error callback panic", panicErr,
				"panic", rev,
				"stack", string(debug.Stack()),
			)
		}
	}()
	t.onHandleError(ctx, p, cause)
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
