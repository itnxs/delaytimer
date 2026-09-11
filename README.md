# delaytimer

延迟任务库。`Timer` 是唯一入口：投递、取消、到期消费。换后端只换 `Store`。

```go
t := delaytimer.New(store, delaytimer.WithHandlers(delaytimer.Bind(...)))
defer t.Close()
_ = t.Start(ctx)
_ = t.SetEvent(time.Now().Add(time.Minute), params)
_ = t.DelEvent(params)
```

完整可运行示例见 [`example/memory`](example/memory)、[`example/redis`](example/redis)、[`example/amqp`](example/amqp)。

`Start` 在内部起 goroutine；`Close` 会取消并等待退出。

## 投递与取消

未配置 Bus 时，`SetEvent` / `DelEvent` **同步写 Store**：返回 `nil` 表示已 `Schedule` / `Cancel`。`DelEvent` 可以紧跟 `SetEvent`，不需要 Sleep。

`Close` 之后再投递返回 `ErrChannelClosed`。

可选总线：

```go
bus := delaytimer.NewBus() // 默认缓冲 2000，发布超时 3s
t := delaytimer.New(store, delaytimer.WithBus(bus), delaytimer.WithHandlers(...))
_ = t.SetEvent(at, params) // 成功只表示进入通道，不保证已写入 Store
```

## Handler

```go
type OrderTimeout struct {
    OrderID string `json:"order_id"`
}

func (p *OrderTimeout) Event() delaytimer.Event { return "order_timeout" }

h := delaytimer.Bind(&OrderTimeout{}, func(ctx context.Context, p *OrderTimeout) error {
    // 到期处理
    return nil
})
```

`Bind` 的原型必须是指针。同一 `Event` 不能重复注册。

Handler 返回错误时：

| 配置 | 行为 |
|---|---|
| 默认 / `WithFailPolicy(FailDiscard)` | 抛弃，打日志 |
| `WithFailPolicy(FailRequeue)` | 约 200ms 后重新入队 |

未知 Kind、JSON 解码失败：**始终抛弃**，不走 FailPolicy。

## Store

| 后端 | 创建 | Claim | Cancel | 说明 |
|---|---|---|---|---|
| Memory | `NewMemory()` | 无到期任务时阻塞到 ctx 取消 | 支持 | 进程内，不跨进程 |
| Redis | `NewRedis(cmd, zsetKey)` | 立即返回，靠 `WithPollInterval` 轮询 | 支持 | 单 ZSET，ZREM 竞争领取 |
| AMQP | `NewAMQP(conn, AMQPConfig{...})` | 从队列消费；第一条可阻塞 | **不支持**（`ErrCancelUnsupported`） | 内部开 Channel 并声明 x-delayed-message 交换机；领取后立刻 Ack |

Redis 示例（Claim 不阻塞，需要 `WithPollInterval` 轮询）：

```go
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
defer rdb.Close()

t := delaytimer.New(
    delaytimer.NewRedis(rdb, "delaytimer:jobs"),
    delaytimer.WithHandlers(h),
    delaytimer.WithPollInterval(100*time.Millisecond),
)
defer t.Close()
_ = t.Start(ctx)
_ = t.SetEvent(time.Now().Add(time.Minute), params)
_ = t.DelEvent(params)
```

`NewRedis` 的第二个参数是 ZSET 名。多副本用同一个 key 即可竞争领取。

AMQP 示例（首次使用时内部打开 Channel，并声明 x-delayed-message 交换机、队列并绑定；`DelEvent` 不支持）：

```go
conn, err := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
defer conn.Close()

t := delaytimer.New(
    delaytimer.NewAMQP(conn, delaytimer.AMQPConfig{
        Exchange:   "delay.ex",
        RoutingKey: "delay.rk",
        Queue:      "delay.q",
    }),
    delaytimer.WithHandlers(h),
)
defer t.Close()
_ = t.Start(ctx)
_ = t.SetEvent(time.Now().Add(time.Minute), params)
```

需要 RabbitMQ 插件 `rabbitmq_delayed_message_exchange`。`Queue` 为空时消费 `RoutingKey`。

## 任务身份

Cancel 按 Key 匹配。Key = `Event` + 两个 ASCII 单元分隔符 (`\x1f\x1f`) + JSON 载荷。JSON 字段顺序与结构体声明一致。

Redis Claim 用该 Key 还原 Kind / Payload。AMQP 消息体里同时带 Key、Kind、Payload。

## Option

| Option | 默认 | 作用 |
|---|---|---|
| `WithHandlers` | 无 | 注册到期 Handler |
| `WithFailPolicy` | `FailDiscard` | 仅 Handler 失败：抛弃或重入队 |
| `WithBus` | 不启用 | `SetEvent` / `DelEvent` 走内存通道 |
| `WithConcurrency` | 32 | 同时执行 Handle 的上限 |
| `WithBatchSize` | 32 | 单次 Claim 条数 |
| `WithPollInterval` | 200ms | Claim 无任务或失败后的等待。Redis 主要靠此项轮询 |
| `WithLogger` / `WithLogLevel` | stderr Info | 日志 |

Bus 自身：`WithChannelSize`（默认 2000）、`WithTimeout`（默认 3s）。
