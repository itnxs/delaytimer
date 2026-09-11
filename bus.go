package delaytimer

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/reactivex/rxgo/v2"
	"github.com/sirupsen/logrus"
)

const defaultChannelSize = 2000

// BusOption 配置 Bus
type BusOption func(*busConfig)

type busConfig struct {
	channelSize int
}

// WithChannelSize 指定 Set / Del 通道缓冲。小于 1 时按 1。默认 5000。
func WithChannelSize(n int) BusOption {
	return func(c *busConfig) {
		c.channelSize = n
	}
}

// SetEventParam 总线上的设置请求
type SetEventParam struct {
	Time   time.Time
	Params EventParams
}

// DelEventParam 总线上的删除请求
type DelEventParam struct {
	Params EventParams
}

// EventChannel 带背压的投递通道，供 Bus 使用。
type EventChannel struct {
	ch     chan rxgo.Item
	closed bool
	mu     sync.RWMutex
}

// NewEventChannel 新建通道，容量为默认 5000
func NewEventChannel() *EventChannel {
	return newEventChannel(defaultChannelSize)
}

func newEventChannel(size int) *EventChannel {
	if size < 1 {
		size = 1
	}
	return &EventChannel{ch: make(chan rxgo.Item, size)}
}

// Close 关闭通道，可重复调用
func (ec *EventChannel) Close() {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	if !ec.closed {
		close(ec.ch)
		ec.closed = true
	}
}

// Observable 转为 RxGo 可观察序列
func (ec *EventChannel) Observable() rxgo.Observable {
	return rxgo.FromChannel(ec.ch,
		rxgo.WithContext(context.Background()),
		rxgo.WithBackPressureStrategy(rxgo.Block),
		rxgo.WithErrorStrategy(rxgo.ContinueOnError),
	)
}

// PublishItem 带 ctx 发布；通道已关闭或超时返回 false
func (ec *EventChannel) PublishItem(ctx context.Context, i any) (bool, error) {
	ec.mu.RLock()
	defer ec.mu.RUnlock()
	if ec.closed {
		return false, errors.WithStack(ErrChannelClosed)
	}
	return rxgo.Of(i).SendContext(ctx, ec.ch), nil
}

// Publish 最多等待 3 秒
func (ec *EventChannel) Publish(i any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ok, err := ec.PublishItem(ctx, i)
	if err != nil {
		return err
	}
	if !ok {
		return errors.WithStack(ErrPublishFailed)
	}
	return nil
}

// Bus API 进程投递 Set / Del
type Bus interface {
	SetEvents() *EventChannel
	DelEvents() *EventChannel
	SetEvent(at time.Time, p EventParams) error
	DelEvent(p EventParams) error
}

// DefaultBus 默认总线
type DefaultBus struct {
	setEvents *EventChannel
	delEvents *EventChannel
}

var _ Bus = (*DefaultBus)(nil)

// NewBus 新建 Set / Del 两条通道。缓冲可用 WithChannelSize 指定，默认 5000。
func NewBus(opts ...BusOption) *DefaultBus {
	cfg := busConfig{channelSize: defaultChannelSize}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &DefaultBus{
		setEvents: newEventChannel(cfg.channelSize),
		delEvents: newEventChannel(cfg.channelSize),
	}
}

// SetEvents 设置事件通道
func (b *DefaultBus) SetEvents() *EventChannel { return b.setEvents }

// DelEvents 删除事件通道
func (b *DefaultBus) DelEvents() *EventChannel { return b.delEvents }

// SetEvent 投递设置；本进程不必注册 Handler
func (b *DefaultBus) SetEvent(at time.Time, p EventParams) error {
	if p == nil {
		return ErrNilParam
	}
	return b.setEvents.Publish(&SetEventParam{Time: at, Params: p})
}

// DelEvent 投递删除
func (b *DefaultBus) DelEvent(p EventParams) error {
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
	// ForEach 跑在订阅 goroutine 里同步 handle。发送写入带缓冲的通道，
	// 处理慢时只要缓冲未满，SetEvent / DelEvent 就不会被堵住。Set 与 Del 各一条订阅。
	return events.ForEach(
		func(i interface{}) { handle(i) },
		func(err error) { t.logger.WithError(err).Errorf("observe %s error", name) },
		func() { t.logger.Infof("observe %s end", name) },
	)
}

func (t *Timer) handleSet(i any) {
	defer t.recoverWrap("set event panic", nil)()
	param, ok := i.(*SetEventParam)
	if !ok || param == nil || param.Params == nil {
		t.logger.WithField("value", i).Error("set event param error")
		return
	}
	payload, err := encodeParams(param.Params)
	if err != nil {
		t.logger.WithError(err).WithField("name", param.Params.EventName()).Error("set event marshal error")
		return
	}
	job := jobFromParams(param.Time, param.Params, payload)
	if err := t.backend.Schedule(context.Background(), job); err != nil {
		t.logger.WithError(err).WithFields(logrus.Fields{"kind": job.Kind, "key": job.Key}).Error("schedule failed")
		return
	}
	t.logger.WithFields(logrus.Fields{
		"time": job.At,
		"name": job.Kind,
		"key":  job.Key,
	}).Info("set timer event")
}

func jobFromParams(at time.Time, p EventParams, payload string) Job {
	kind := string(p.EventName())
	return Job{
		Key:     JobKey(kind, payload),
		Kind:    kind,
		Payload: payload,
		At:      at,
	}
}

func (t *Timer) handleDel(i any) {
	defer t.recoverWrap("del event panic", nil)()
	param, ok := i.(*DelEventParam)
	if !ok || param == nil || param.Params == nil {
		t.logger.WithField("value", i).Error("del event param error")
		return
	}
	name := param.Params.EventName()
	payload, err := encodeParams(param.Params)
	if err != nil {
		t.logger.WithError(err).WithField("name", name).Error("del event marshal error")
		return
	}
	key := JobKey(string(name), payload)
	if err := t.backend.Cancel(context.Background(), key); err != nil {
		if errors.Is(err, ErrCancelUnsupported) {
			t.logger.WithField("key", key).Info("cancel unsupported")
			return
		}
		t.logger.WithError(err).WithField("key", key).Error("cancel failed")
		return
	}
	t.logger.WithFields(logrus.Fields{"name": name, "key": key}).Info("del timer event")
}
