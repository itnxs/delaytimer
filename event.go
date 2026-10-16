package delaytimer

import (
	"context"
	"fmt"
	"reflect"

	jsoniter "github.com/json-iterator/go"
)

// jsonAPI 字段顺序与结构体声明一致，保证 Cancel 身份稳定
var jsonAPI = jsoniter.ConfigCompatibleWithStandardLibrary

// Event 事件名。与 JSON 载荷组成任务 Key（中间为两个 ASCII 单元分隔符 \x1f\x1f）。
type Event string

// String 转string
func (e Event) String() string {
	return string(e)
}

// IsNil 是否为空
func (e Event) IsNil() bool {
	return e.String() == ""
}

// Params 投递载荷。domain 只实现本接口，不含处理逻辑。
type Params interface {
	Event() Event // Event名称
}

// EventHandler 到期消费处理
type EventHandler interface {
	Event() Event      // Event名称
	NewParams() Params // 解码指针参数
	Handle(ctx context.Context, p Params) error
}

type boundHandler[P Params] struct {
	proto   P
	handler func(context.Context, P) error
}

// Bind 把参数原型和处理函数绑成 Handler。proto 必须是指针，供克隆后 JSON 解码。
func Bind[P Params](proto P, handler func(context.Context, P) error) EventHandler {
	if handler == nil {
		panic(fmt.Errorf("bind handler is nil (handler=%T): %w", handler, ErrNilEventHandler))
	}
	if !isPointerParams(proto) {
		panic(fmt.Errorf("bind proto must be pointer (proto=%T): %w", proto, ErrNotPointerParams))
	}
	if proto.Event().IsNil() {
		panic(ErrEmptyEvent)
	}
	return &boundHandler[P]{proto: proto, handler: handler}
}

// Event 事件名称
func (h *boundHandler[P]) Event() Event {
	return h.proto.Event()
}

// NewParams 复制参数
func (h *boundHandler[P]) NewParams() Params {
	v := reflect.ValueOf(h.proto)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return nil
	}
	p, ok := reflect.New(v.Type().Elem()).Interface().(Params)
	if !ok {
		return nil
	}
	return p
}

// Handle 处理程序
func (h *boundHandler[P]) Handle(ctx context.Context, p Params) error {
	t, ok := p.(P)
	if !ok {
		return fmt.Errorf("handle params type mismatch (want=%T got=%T): %w", h.proto, p, ErrParamsTypeMismatch)
	}
	return h.handler(ctx, t)
}

// isPointerParams 是否是指针参数
func isPointerParams(p Params) bool {
	rv := reflect.ValueOf(p)
	return rv.Kind() == reflect.Ptr && !rv.IsNil()
}

// encodeParams 序列化投递参数
func encodeParams(p Params) (string, error) {
	s, err := jsonAPI.MarshalToString(p)
	if err != nil {
		return "", err
	}
	return s, nil
}

// decodeParams 解码到已克隆的指针参数。失败时包装 ErrUnmarshalParams。
func decodeParams(payload string, p Params) error {
	if err := jsonAPI.UnmarshalFromString(payload, p); err != nil {
		return fmt.Errorf("decode params failed: %v: %w", err, ErrUnmarshalParams)
	}
	return nil
}
