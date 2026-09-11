package delaytimer

import (
	"context"
	"reflect"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

var jsonAPI = jsoniter.ConfigCompatibleWithStandardLibrary // 字段顺序与结构体声明一致，保证 Cancel 身份稳定

// EventName 事件名，与 JSON 载荷一起构成 Cancel 身份。
type EventName string

// EventParams 投递载荷。domain 只实现本接口，不含处理逻辑。
type EventParams interface {
	EventName() EventName
}

// EventHandler 到期处理。消费进程通过 WithHandlers 注册。
type EventHandler interface {
	EventName() EventName
	NewParams() EventParams // 返回可 JSON 解码的指针副本
	Handle(ctx context.Context, p EventParams) error
}

type boundHandler[P EventParams] struct {
	proto P
	fn    func(context.Context, P) error
}

// Bind 把参数原型和处理函数绑成 Handler。proto 必须是指针，供克隆后 JSON 解码。
func Bind[P EventParams](proto P, fn func(context.Context, P) error) EventHandler {
	if fn == nil {
		panic("delaytimer: handler func is nil")
	}
	if !isPointerParams(proto) {
		panic(errors.WithMessagef(ErrNotPointerParams, "%T", proto))
	}
	if proto.EventName() == "" {
		panic(ErrEmptyEventName)
	}
	return &boundHandler[P]{proto: proto, fn: fn}
}

func (h *boundHandler[P]) EventName() EventName {
	return h.proto.EventName()
}

func (h *boundHandler[P]) NewParams() EventParams {
	p, err := cloneParams(h.proto)
	if err != nil {
		panic(err)
	}
	return p
}

func (h *boundHandler[P]) Handle(ctx context.Context, p EventParams) error {
	typed, ok := p.(P)
	if !ok {
		return errors.Errorf("delaytimer: params type mismatch want %T got %T", h.proto, p)
	}
	return h.fn(ctx, typed)
}

func cloneParams(proto EventParams) (EventParams, error) {
	rv := reflect.ValueOf(proto)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return nil, errors.WithMessagef(ErrNotPointerParams, "%T", proto)
	}
	cloned, ok := reflect.New(rv.Type().Elem()).Interface().(EventParams)
	if !ok {
		return nil, errors.WithMessagef(ErrNotPointerParams, "%T", proto)
	}
	return cloned, nil
}

func isPointerParams(p EventParams) bool {
	rv := reflect.ValueOf(p)
	return rv.Kind() == reflect.Ptr && !rv.IsNil()
}

// encodeParams 序列化投递载荷。
func encodeParams(p EventParams) (string, error) {
	s, err := jsonAPI.MarshalToString(p)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return s, nil
}

// decodeParams 解码到已克隆的指针原型。
func decodeParams(payload string, p EventParams) error {
	return errors.WithStack(jsonAPI.UnmarshalFromString(payload, p))
}
