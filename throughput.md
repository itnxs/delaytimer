# 投递与消费吞吐测试

对照 Memory、Redis、AMQP 三个 Store，分别测 `SetEvent` 写入能力和到期消费能力。测试代码在 `throughput_test.go`（`//go:build throughput`），默认 `go test ./...` 不会跑。

## 怎么跑

```bash
go test -tags throughput -run TestStoreThroughput -count=1 -timeout 10m -v
```

本机 Docker RabbitMQ（含 `rabbitmq_delayed_message_exchange`）：

```bash
docker run -d --name delaytimer-rabbit -p 5672:5672 -p 15672:15672 \
  heidiks/rabbitmq-delayed-message-exchange:latest

AMQP_URL='amqp://guest:guest@127.0.0.1:5672/' \
  go test -tags throughput -run TestStoreThroughput -count=1 -timeout 10m -v
```

可选环境变量：

| 变量 | 默认 | 说明 |
|---|---|---|
| `THROUGHPUT_N` | `10000` | 每个后端的任务条数 |
| `REDIS_ADDR` | `127.0.0.1:6379` | Redis 地址；Ping 失败则 skip |
| `AMQP_URL` | 见测试代码 | RabbitMQ；Dial 失败则 skip。本机 Docker 用 `amqp://guest:guest@127.0.0.1:5672/` |

需要 Redis 可连；AMQP 需要插件 `rabbitmq_delayed_message_exchange`（未到期任务才走插件）。Memory 不依赖外部服务。

## 口径

每个后端同一套流程，互不影响：

1. **投递**：不 `Start`。32 个 goroutine 并发 `SetEvent`，到期时间均为「当前时间 − 1s」（已到期，只进 Store 不立刻消费）。
2. **消费**：投递全部返回后再 `Start`，等到 Handler 执行满 `N` 条。
3. Handler 只做原子计数，不模拟业务耗时。日志丢弃。
4. Timer 参数：`WithConcurrency(64)`、`WithBatchSize(128)`、`WithPollInterval(1ms)`。
5. AMQP 发布池：`WithAMQPPublishChannels(32)`（与投递 goroutine 数一致；库默认 8）。
6. 每条任务 `ID` 唯一，避免 Redis 同 Key 覆盖。Redis 用一次性 ZSET key，测完删除；AMQP 用一次性 Exchange / Queue / RoutingKey。

ops/s 算法：

- 投递 = `N / 投递耗时`
- 消费 = `实际 Handle 条数 / 消费耗时`（成功时应等于 `N`）

这是库 + 后端的上限参考，不是多次平均，也不是生产 SLA。

## 结果（本机 Docker，2026-09-11）

- `N=10000`，投递 workers=32，`GOMAXPROCS=12`
- Redis：`127.0.0.1:6379`（容器 `my-redis`）
- AMQP：`127.0.0.1:5672`（容器 `delaytimer-rabbit`，镜像 `heidiks/rabbitmq-delayed-message-exchange`，插件已启用）
- `publish_err=0`

| 后端 | 投递耗时 | 投递 ops/s | 消费耗时 | 消费 ops/s |
|---|---:|---:|---:|---:|
| Memory | 34.8 ms | 287,613 | 58.4 ms | 171,295 |
| Redis | 3,462.2 ms | 2,888 | 247.8 ms | 40,353 |
| AMQP | 1,254.6 ms | 7,971 | 2,529.5 ms | 3,953 |

同日对已预热的本机 AMQP 再跑一轮：投递 **27,476 ops/s**（364 ms）、消费 **4,271 ops/s**。

原始输出（上表这一轮，三后端一起跑）：

```
n=10000 workers=32 gomaxprocs=12
RESULT backend=TestStoreThroughput/memory n=10000 publish_err=0 publish_ms=34.8 publish_ops=287613 consume_ms=58.4 consume_n=10000 consume_ops=171295
RESULT backend=TestStoreThroughput/redis n=10000 publish_err=0 publish_ms=3462.2 publish_ops=2888 consume_ms=247.8 consume_n=10000 consume_ops=40353
RESULT backend=TestStoreThroughput/amqp n=10000 publish_err=0 publish_ms=1254.6 publish_ops=7971 consume_ms=2529.5 consume_n=10000 consume_ops=3953
--- PASS: TestStoreThroughput (7.90s)
```

预热后再跑 AMQP：

```
RESULT backend=TestStoreThroughput/amqp n=10000 publish_err=0 publish_ms=364.0 publish_ops=27476 consume_ms=2341.2 consume_n=10000 consume_ops=4271
```

### AMQP 对照

| 环境 | 投递 ops/s | 消费 ops/s |
|---|---:|---:|
| 远程集群，单 Channel + 已到期也走延迟插件 | 576 | 4,275 |
| 远程集群，发布池 + 已到期直投（空闲） | 2,357 | 14,242 |
| 远程集群，发布池 + 已到期直投（忙） | 696 | 4,981 |
| 本机 Docker（冷启动，与 Memory/Redis 同轮） | 7,971 | 3,953 |
| 本机 Docker（AMQP 已预热） | 27,476 | 4,271 |

## 怎么读

- **Memory** 无网络、堆内调度，只作上限对照，不能外推到多进程。
- **Redis** 每次 `SetEvent` 一次 `ZADD`；消费走 Lua 批量领取，所以消费明显高于投递。
- **AMQP** 远程 576 主要是 RTT + 延迟插件 + 单 Channel。本机 Docker 后投递到 **8k～27k**，说明库侧发布池和已到期直投是够用的；远程几百到两千是网络和集群负载。本机消费大约 **4k**，领取仍是单消费 Channel。未到期任务仍走 `x-delayed-message` 插件，会比这组已到期数字慢。

生产若 Handler 有 IO，消费会先打满下游。加副本、加大 `WithConcurrency` / `WithBatchSize`，或 Redis 缩短 `WithPollInterval`，都可能改变结果。
