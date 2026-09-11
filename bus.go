package delaytimer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/reactivex/rxgo/v2"
	"github.com/sirupsen/logrus"
)

const (
	// defaultChannelSize 默认事件通道缓冲大小
	defaultChannelSize = 2000
	// defaultTimeout 默认事件发布超时事件
	defaultTimeout = 3 * time.Second
)

var _ Bus = (*DefaultBus)(nil)

// BusOption 配置 Bus
type BusOption func(*busConfig)

type busConfig struct {
	channelSize int
	timeout     time.Duration
}

// WithChannelSize 指定通道缓冲大小
func WithChannelSize(site int) BusOption {
	return func(c *busConfig) {
		c.channelSize = site
	}
}

// WithTimeout 指定事件发布超时时间
func WithTimeout(timeout time.Duration) BusOption {
	return func(c *busConfig) {
		c.timeout = timeout
	}
}

// SetEventParam 总线上的设置请求
type SetEventParam struct {
	Time   time.Time
	Params Params
}

// DelEventParam 总线上的删除请求
type DelEventParam struct {
	Params Params
}

// EventChannel 带背压的投递通道，供 Bus 使用。
type EventChannel struct {
	closed  bool
	timeout time.Duration
	item    chan rxgo.Item
	mu      sync.RWMutex
}

// NewEventChannel 新建通道
func NewEventChannel() *EventChannel {
	return newEventChannel(defaultChannelSize, defaultTimeout)
}

func newEventChannel(size int, timeout time.Duration) *EventChannel {
	if size < 1 {
		size = 1
	}
	return &EventChannel{item: make(chan rxgo.Item, size), timeout: timeout}
}

// Close 关闭通道
func (ec *EventChannel) Close() {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	if !ec.closed {
		close(ec.item)
		ec.closed = true
	}
}

// Observable 转为 RxGo 可观察序列
func (ec *EventChannel) Observable() rxgo.Observable {
	return rxgo.FromChannel(ec.item,
		rxgo.WithContext(context.Background()),
		rxgo.WithBackPressureStrategy(rxgo.Block),
		rxgo.WithErrorStrategy(rxgo.ContinueOnError),
	)
}

// PublishWithContext 带ctx发布
func (ec *EventChannel) PublishWithContext(ctx context.Context, i any) (bool, error) {
	ec.mu.RLock()
	defer ec.mu.RUnlock()
	if ec.closed {
		return false, errors.WithStack(ErrChannelClosed)
	}
	return rxgo.Of(i).SendContext(ctx, ec.item), nil
}

// Publish 发布
func (ec *EventChannel) Publish(i any) error {
	ctx, cancel := context.WithTimeout(context.Background(), ec.timeout)
	defer cancel()
	ok, err := ec.PublishWithContext(ctx, i)
	if err != nil {
		return err
	} else if !ok {
		return errors.WithStack(ErrPublishFailed)
	}
	return nil
}

// Bus API 进程投递 Set / Del
type Bus interface {
	SetEvents() *EventChannel
	DelEvents() *EventChannel
	SetEvent(at time.Time, p Params) error
	DelEvent(p Params) error
}

// DefaultBus 默认总线
type DefaultBus struct {
	setEvents *EventChannel
	delEvents *EventChannel
}

// NewBus 新建 Set / Del 两条通道。缓冲可用 WithChannelSize 指定，默认 5000。
func NewBus(opts ...BusOption) *DefaultBus {
	cfg := busConfig{
		channelSize: defaultChannelSize,
		timeout:     defaultTimeout,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return &DefaultBus{
		setEvents: newEventChannel(cfg.channelSize, cfg.timeout),
		delEvents: newEventChannel(cfg.channelSize, cfg.timeout),
	}
}

// SetEvents 设置事件通道
func (b *DefaultBus) SetEvents() *EventChannel { return b.setEvents }

// DelEvents 删除事件通道
func (b *DefaultBus) DelEvents() *EventChannel { return b.delEvents }

// SetEvent 设置事件
func (b *DefaultBus) SetEvent(at time.Time, p Params) error {
	if p == nil {
		return ErrNilParam
	}
	return b.setEvents.Publish(&SetEventParam{Time: at, Params: p})
}

// DelEvent 删除事件
func (b *DefaultBus) DelEvent(p Params) error {
	if p == nil {
		return ErrNilParam
	}
	return b.delEvents.Publish(&DelEventParam{Params: p})
}

// Subscribe 把总线的 Set / Del 写入 Timer 后端。API 进程调用。
func Subscribe(t *Timer, bus Bus) {
	if t == nil || bus == nil {
		return
	}
	if ch := bus.SetEvents(); ch != nil {
		observe(t, ch.Observable(), t.handleSet, "setEvent")
	}
	if ch := bus.DelEvents(); ch != nil {
		observe(t, ch.Observable(), t.handleDel, "delEvent")
	}
}

func observe(t *Timer, events rxgo.Observable, handle func(any), name string) rxgo.Disposed {
	return events.ForEach(
		func(i interface{}) {
			handle(i)
		},
		func(err error) {
			t.logger.WithError(err).Errorf("observe %s error", name)
		},
		func() {
			t.logger.Infof("observe %s end", name)
		},
	)
}

// newTask 新建任务
func (t *Timer) newTask(at time.Time, p Params, payload string) Task {
	task := Task{
		Kind:    string(p.Event()),
		Payload: payload,
		At:      at,
	}
	task.Key = fmt.Sprintf("%s:%s", task.Kind, task.Payload)
	return task
}

// handleSet set event
func (t *Timer) handleSet(i any) {
	p, ok := i.(*SetEventParam)
	if !ok || p == nil || p.Params == nil {
		t.logger.WithField("value", i).Error("set event param error")
		return
	}

	payload, err := encodeParams(p.Params)
	if err != nil {
		t.logger.WithError(err).WithFields(logrus.Fields{
			"name": p.Params.Event(),
		}).Error("set event marshal error")
		return
	}

	task := t.newTask(p.Time, p.Params, payload)
	if err = t.store.Schedule(context.Background(), task); err != nil {
		t.logger.WithError(err).WithFields(logrus.Fields{
			"kind": task.Kind,
			"key":  task.Key,
		}).Error("schedule failed")
		return
	}

	t.logger.WithFields(logrus.Fields{
		"time": task.At,
		"name": task.Kind,
		"key":  task.Key,
	}).Info("set timer event")
}

// handleDel del event
func (t *Timer) handleDel(i any) {
	p, ok := i.(*DelEventParam)
	if !ok || p == nil || p.Params == nil {
		t.logger.WithField("value", i).Error("del event param error")
		return
	}

	name := p.Params.Event()
	payload, err := encodeParams(p.Params)
	if err != nil {
		t.logger.WithError(err).WithField("name", name).Error("del event marshal error")
		return
	}

	key := t.newTask(time.Now(), p.Params, payload).Key
	if err = t.store.Cancel(context.Background(), key); err != nil {
		t.logger.WithError(err).WithField("key", key).Error("cancel failed")
		return
	}

	t.logger.WithFields(logrus.Fields{
		"name": name,
		"key":  key,
	}).Info("del timer event")
}
