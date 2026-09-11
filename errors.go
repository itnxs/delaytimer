package delaytimer

import "github.com/pkg/errors"

var (
	// ErrNilEventHandler Bind 或 WithHandlers 的 Handler 为空。
	ErrNilEventHandler = errors.New("event handler is nil")
	// ErrChannelClosed Timer、Bus 或 Store 已关闭。
	ErrChannelClosed = errors.New("already closed")
	// ErrPublishFailed 投递未写入 Store：Bus 通道满或超时；AMQP 连接或 Channel 不可用。
	ErrPublishFailed = errors.New("publish failed")
	// ErrCancelUnsupported Store 不支持取消，当前为 AMQP。
	ErrCancelUnsupported = errors.New("cancel unsupported")
	// ErrEmptyEvent Event 名为空。
	ErrEmptyEvent = errors.New("event is empty")
	// ErrDuplicateEvent 同一 Event 重复注册 Handler。
	ErrDuplicateEvent = errors.New("event name already registered")
	// ErrNilTaskStore New 时 Store 为空。
	ErrNilTaskStore = errors.New("task store is nil")
	// ErrNilParam SetEvent / DelEvent 的 params 为空，或 NewParams 返回空。
	ErrNilParam = errors.New("event param is nil")
	// ErrNotPointerParams Bind / NewParams 必须返回指针，才能 JSON 解码。
	ErrNotPointerParams = errors.New("params must be a pointer")
	// ErrUnmarshalParams 任务载荷无法解码为绑定的参数类型。
	ErrUnmarshalParams = errors.New("unmarshal params failed")
	// ErrAlreadyRunning 重复调用 Start。
	ErrAlreadyRunning = errors.New("timer already running")
	// ErrUnknownKind 未注册的任务 Kind。
	ErrUnknownKind = errors.New("unknown kind")
	// ErrConsumeClosed AMQP 消费 Channel 已关闭。
	ErrConsumeClosed = errors.New("amqp consume channel closed")
	// ErrAckFailed 领取后确认失败。
	ErrAckFailed = errors.New("task ack failed")
	// ErrNilRedis NewRedis 的 cmd 为空。
	ErrNilRedis = errors.New("redis commands is nil")
	// ErrEmptyRedisKey Redis ZSET key 为空。
	ErrEmptyRedisKey = errors.New("redis key is empty")
	// ErrParamsTypeMismatch Handle 收到的 params 与 Bind 注册的类型不一致。
	ErrParamsTypeMismatch = errors.New("params type mismatch")
)
