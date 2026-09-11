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
    return l.WithField("pkg", "delaytime")
}

// Timer 唯一启动入口。换 Memory / Redis / AMQP 只换 Store。
type Timer struct {
    concurrency int
    batchSize   int
    store       Store
    bus         *DefaultBus
    registers   []EventHandler
    handlers    map[Event]EventHandler
    logger      logrus.FieldLogger
    interval    time.Duration
    mu          sync.Mutex
    closed      bool
    cancel      context.CancelFunc
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
    t.bus = NewBus()
    subscribe(t, t.bus)
    return t
}

// SetEvent 同步写入 Store。Close 之后返回 ErrChannelClosed。
func (t *Timer) SetEvent(at time.Time, p Params) error {
    if p == nil {
        return ErrNilParam
    }
    if t.isClosed() {
        return ErrChannelClosed
    }

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

// DelEvent 同步从 Store 取消。Close 之后返回 ErrChannelClosed。
func (t *Timer) DelEvent(p Params) error {
    if p == nil {
        return ErrNilParam
    }
    if t.isClosed() {
        return ErrChannelClosed
    }
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

// Close 停止 Run 并关闭投递通道。可重复调用。
func (t *Timer) Close() {
    if t == nil {
        return
    }

    t.mu.Lock()
    t.closed = true
    cancel := t.cancel
    t.mu.Unlock()

    if cancel != nil {
        cancel()
    }

    if t.bus != nil {
        t.bus.SetEvents().Close()
        t.bus.DelEvents().Close()
    }
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

// Run 启动服务处理。ctx 取消或 Close 后返回。
func (t *Timer) Run(ctx context.Context) error {
    ctx, cancel := context.WithCancel(ctx)

    t.mu.Lock()
    if t.closed {
        t.mu.Unlock()
        cancel()
        return context.Canceled
    }

    t.cancel = cancel
    t.mu.Unlock()
    defer t.Close()
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
                if err := t.dispatch(ctx, task); err != nil {
                    t.logger.WithError(err).WithFields(logrus.Fields{
                        "kind": task.Kind,
                        "key":  task.Key,
                    }).Error("task dispatch failed")
                }
                return nil
            })
        }

        if err := wg.Wait(); err != nil {
            return errors.WithStack(err)
        }
    }
}

// dispatch 任务分发处理
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
