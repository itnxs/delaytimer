// Package delaytimer 是延迟任务库。
//
// Timer 为唯一入口：SetEvent 投递、DelEvent 取消、Start 后台领取并执行 Handler，Close 取消并等待退出。
// 换 Memory / Redis / AMQP 只换 Store。AMQP 时 Close 还会关掉库打开的 Channel。
// Redis 同一 zsetKey、AMQP 同一 Exchange/Queue 可多进程 Start 竞争领取；Memory 仅单进程。
// 单机用 WithConcurrency / WithBatchSize；Redis Claim 不阻塞，靠 WithPollInterval 轮询。
//
// 默认 SetEvent / DelEvent 同步写入 Store，返回成功即表示已写入（或已取消）。
// 使用 WithBus 时只表示进入内存通道，真正写 Store 在订阅回调里，失败只打日志。
//
// Handler 失败默认抛弃（FailDiscard）；WithFailPolicy(FailRequeue) 时延迟约 200ms 重新入队。
// 未知 Kind、载荷解码失败始终抛弃，不受 FailPolicy 影响。
package delaytimer
