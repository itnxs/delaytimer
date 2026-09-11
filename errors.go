package delaytimer

import "github.com/pkg/errors"

var (
	// ErrChannelClosed 事件通道已关闭
	ErrChannelClosed = errors.New("event channel closed")
	// ErrPublishFailed 通道已满、发送超时，或 AMQP Channel 为空
	ErrPublishFailed = errors.New("publish failed")
	// ErrCancelUnsupported 当前后端不支持取消
	ErrCancelUnsupported = errors.New("cancel unsupported")
	// ErrEmptyEventName EventName 为空
	ErrEmptyEventName = errors.New("event name is empty")
	// ErrDuplicateEventName 同一 EventName 重复注册 Handler
	ErrDuplicateEventName = errors.New("event name already registered")
	// ErrNilBackend 未提供后端
	ErrNilBackend = errors.New("backend is nil")
	// ErrAMQPKindsRequired 分片或按 Event 分队列消费时必须配置 Kinds
	ErrAMQPKindsRequired = errors.New("amqp consume requires Kinds")
	// ErrNilParam SetEvent / DelEvent 参数为空
	ErrNilParam = errors.New("event param is nil")
	// ErrNotPointerParams Bind / NewParams 必须返回指针，才能 JSON 解码
	ErrNotPointerParams = errors.New("params must be a pointer")
)
