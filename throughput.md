# 投递与消费吞吐测试

对照 Memory、Redis、AMQP 三个 Store，分别测 `SetEvent` 写入能力和到期消费能力。测试代码在 `throughput_test.go`（`//go:build throughput`），默认 `go test ./...` 不会跑。

## 怎么跑

```bash
go test -tags throughput -run TestStoreThroughput -count=1 -timeout 10m -v
```

可选环境变量：

| 变量 | 默认 | 说明 |
|---|---|---|
| `THROUGHPUT_N` | `10000` | 每个后端的任务条数 |
| `REDIS_ADDR` | `127.0.0.1:6379` | Redis 地址；Ping 失败则 skip |
| `AMQP_URL` | `amqp://michong:michong@loong-dev-rabbitmq.rabbitmq.svc.cluster.local:5672/` | RabbitMQ；Dial 失败则 skip |

需要 Redis 可连；AMQP 需要插件 `rabbitmq_delayed_message_exchange`。Memory 不依赖外部服务。

## 口径

每个后端同一套流程，互不影响：

1. **投递**：不 `Start`。32 个 goroutine 并发 `SetEvent`，到期时间均为「当前时间 − 1s」（已到期，只进 Store 不立刻消费）。
2. **消费**：投递全部返回后再 `Start`，等到 Handler 执行满 `N` 条。
3. Handler 只做原子计数，不模拟业务耗时。日志丢弃。
4. Timer 参数：`WithConcurrency(64)`、`WithBatchSize(128)`、`WithPollInterval(1ms)`。
5. 每条任务 `ID` 唯一，避免 Redis 同 Key 覆盖。Redis 用一次性 ZSET key，测完删除；AMQP 用一次性 Exchange / Queue / RoutingKey。

ops/s 算法：

- 投递 = `N / 投递耗时`
- 消费 = `实际 Handle 条数 / 消费耗时`（成功时应等于 `N`）

这是库 + 后端的上限参考，不是多次平均，也不是生产 SLA。

## 本次结果

- 时间：2026-09-11
- `N=10000`，投递 workers=32，`GOMAXPROCS=12`
- Redis：本机 Docker `127.0.0.1:6379`（容器 `my-redis`）
- AMQP：`loong-dev-rabbitmq.rabbitmq.svc.cluster.local:5672`
- 三次子测试均 `publish_err=0`

| 后端 | 投递耗时 | 投递 ops/s | 消费耗时 | 消费 ops/s |
|---|---:|---:|---:|---:|
| Memory | 28.0 ms | 356,620 | 34.8 ms | 286,965 |
| Redis | 3,320.5 ms | 3,012 | 243.7 ms | 41,034 |
| AMQP | 17,348.9 ms | 576 | 2,339.3 ms | 4,275 |

原始输出：

```
n=10000 workers=32 gomaxprocs=12
RESULT backend=TestStoreThroughput/memory n=10000 publish_err=0 publish_ms=28.0 publish_ops=356620 consume_ms=34.8 consume_n=10000 consume_ops=286965
RESULT backend=TestStoreThroughput/redis n=10000 publish_err=0 publish_ms=3320.5 publish_ops=3012 consume_ms=243.7 consume_n=10000 consume_ops=41034
RESULT backend=TestStoreThroughput/amqp n=10000 publish_err=0 publish_ms=17348.9 publish_ops=576 consume_ms=2339.3 consume_n=10000 consume_ops=4275
--- PASS: TestStoreThroughput (23.35s)
```

## 怎么读

- **Memory** 无网络、堆内调度，投递和消费都是同机内存量级，只作上限对照，不能外推到多进程。
- **Redis** 每次 `SetEvent` 一次 `ZADD`，投递受 RTT 限制；消费走 Lua 一次 `ZRANGEBYSCORE + ZREM`，按 batch=128 领取，所以消费明显高于投递。
- **AMQP** 发布锁在单 Channel 上串行（amqp091 Channel 不能跨 goroutine），32 个投递 goroutine 会挤在同一把锁上，投递最慢。消费是队列竞争 + 领取即 Ack，仍受 broker 往返限制。

生产若 Handler 有 IO，消费会先打满下游，而不是打满上表数字。加副本、加大 `WithConcurrency` / `WithBatchSize`，或 Redis 缩短 `WithPollInterval`，都可能改变结果；空转副本过多会先打满 Redis。
