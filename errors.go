package delaytimer

import "github.com/pkg/errors"

var (
	// ErrNilEventHandler 事件处理为空
	ErrNilEventHandler = errors.New("event handler is nil")
	// ErrChannelClosed Timer 已 Close，或 Bus 通道已关闭
	ErrChannelClosed = errors.New("event channel closed")
	// ErrPublishFailed 通道已满、发送超时，或 AMQP Channel 为空
	ErrPublishFailed = errors.New("publish failed")
	// ErrCancelUnsupported 当前后端不支持取消
	ErrCancelUnsupported = errors.New("cancel unsupported")
	// ErrEmptyEvent Event 为空
	ErrEmptyEvent = errors.New("event is empty")
	// ErrDuplicateEvent 同一 Event 重复注册 Handler
	ErrDuplicateEvent = errors.New("event name already registered")
	// ErrNilTaskStore 未提供任务存储
	ErrNilTaskStore = errors.New("task store is nil")
	// ErrNilParam SetEvent / DelEvent 参数为空
	ErrNilParam = errors.New("event param is nil")
	// ErrNotPointerParams Bind / NewParams 必须返回指针，才能 JSON 解码
	ErrNotPointerParams = errors.New("params must be a pointer")
	// ErrUnmarshalParams 解码事件参数错误。当前 decode 返回 json 原错误，未必包装为本值。
	ErrUnmarshalParams = errors.New("unmarshal params failed")
	// ErrAlreadyRunning Start 已在执行
	ErrAlreadyRunning = errors.New("timer already running")
)
