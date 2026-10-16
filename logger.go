package delaytimer

import "context"

// Logger 定义 delaytimer 使用的最小日志接口。
// 未注入 Logger 时，库内部不会输出日志。
type Logger interface {
	Info(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, err error, args ...any)
}

func logInfo(logger Logger, ctx context.Context, msg string, args ...any) {
	if logger == nil {
		return
	}
	logger.Info(ctx, msg, args...)
}

func logError(logger Logger, ctx context.Context, msg string, err error, args ...any) {
	if logger == nil {
		return
	}
	logger.Error(ctx, msg, err, args...)
}
