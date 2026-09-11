package delaytimer

import "github.com/pkg/errors"

var (
    // ErrNilEventHandler 事件处理为空
    ErrNilEventHandler = errors.New("event handler is nil")
    // ErrChannelClosed 事件通道已关闭
    ErrChannelClosed = errors.New("event channel closed")
    // ErrPublishFailed 通道已满、发送超时，或 AMQP Channel 为空
    ErrPublishFailed = errors.New("publish failed")
    // ErrCancelUnsupported 当前后端不支持取消
    ErrCancelUnsupported = errors.New("cancel unsupported")
    // ErrEmptyEvent EventName 为空
    ErrEmptyEvent = errors.New("event is empty")
    // ErrDuplicateEventName 同一 EventName 重复注册 Handler
    ErrDuplicateEventName = errors.New("event name already registered")
    // ErrNilTaskStore 未提供任务存储
    ErrNilTaskStore = errors.New("task store is nil")
    // ErrAMQPKindsRequired 分片或按 Event 分队列消费时必须配置 Kinds
    ErrAMQPKindsRequired = errors.New("amqp consume requires Kinds")
    // ErrNilParam SetEvent / DelEvent 参数为空
    ErrNilParam = errors.New("event param is nil")
    // ErrNotPointerParams Bind / NewParams 必须返回指针，才能 JSON 解码
    ErrNotPointerParams = errors.New("params must be a pointer")
    // ErrUnmarshalParams 解码事件参数错误
    ErrUnmarshalParams = errors.New("unmarshal event params failed")
)
