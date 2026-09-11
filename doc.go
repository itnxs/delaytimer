// Package delaytimer 是可水平扩展的延迟计时器。
//
// domain 实现 EventParams（EventName + json 字段），不含处理逻辑。
// 消费进程用 Bind(原型, Handle函数) 或自行实现 EventHandler 注册。
// 包内用 jsoniter.ConfigCompatibleWithStandardLibrary 编解码，字段顺序与结构体声明一致，保证 Cancel 身份稳定。
// SetEvent / DelEvent 只传 Params；到期后库克隆原型、解码再 Handle(ctx, params)。
// 日志为 logrus.FieldLogger，可用 WithLogger / WithLogLevel 配置，默认 Info 输出到 stderr。
// 消费进程调用 Run，多副本竞争领取，K8s 扩容即提升吞吐。
//
// 三种后端：
//
//   - Memory：进程内堆，支持 Cancel / 同 Key 覆盖，不跨进程。Handle 失败是否重入队由 FailPolicy 决定。
//   - Redis：每种 EventName × shard 一个 ZSET `{prefix:kind:shard}:due`（hash tag）。
//     member=JobKey(kind, payload)，score=到期 Unix 毫秒。已出现的 EventName 记在 `{prefix}:kinds`。
//     同 member 覆盖 score；Cancel 为 ZREM（Hash 定点，Random / Milli 扫该 Event 全部片）。
//     Claim 轮询 kind×shard，用 ZREM 竞争领取（领取即删除），默认用 Redis TIME。
//     Ack 为空操作。Fail 短延迟 ZADD NX 重入队；已有新 Schedule 时不覆盖。
//     客户端用 GoRedisV9 适配 github.com/redis/go-redis/v9。SIGKILL 时已领取任务会丢失，Handle 须幂等。
//   - AMQP：x-delayed-message（header x-delay 毫秒）+ 队列竞争消费；不支持 Cancel，Handle 需幂等。
//     传入 *amqp.Channel（github.com/rabbitmq/amqp091-go）；Consume 通道关闭后会重新注册，不绑到单次 Claim。
//     延迟交换机与队列由调用方声明。Shards==1 且未配 Kinds 时沿用 RoutingKey/Queue；
//     否则发布 `{RoutingKey}.{kind}.{shard}`，消费 `{Queue}.{kind}.{shard}`（分片消费必须配置 Kinds）。
//     停机 Release 会 Nack(requeue=true)；业务失败由 FailPolicy 决定 Ack 或 Fail（Fail 亦 Nack requeue）。
//
// FailPolicy（WithFailPolicy）：
//
//   - FailDiscard（默认）：Handle 错误 / panic / 未注册 / 解码失败时 Ack，不放回存储。
//   - FailRequeue：走后端 Fail。
//   - 停机与 ctx 取消时的 Release 不受 FailPolicy 影响，仍归还未完成任务。
//
// Redis / AMQP 分片（Shards，默认 1）：
//
//   - ShardHash（默认）：fnv32(JobKey)%N，覆盖与 Cancel 定点落同一片。
//   - ShardRandom：写入随机片；Cancel / 再次 Schedule 会扫该 Event 全部片后再写。
//   - ShardMilli：按到期 Unix 毫秒取模；Cancel 没有时间戳，与 Random 一样扫全部片。
//   - Memory 不分片。
//
// API 进程典型用法（不必注册 Handler）：
//
//	bus := delaytimer.NewBus() // 默认通道缓冲 5000；可用 delaytimer.WithChannelSize(n) 指定
//	tm := delaytimer.New(backend)
//	delaytimer.Subscribe(tm, bus)
//	_ = bus.SetEvent(due, &ConfirmTimeoutParams{RoomID: id})
//
// 消费进程：
//
//	tm := delaytimer.New(backend, delaytimer.WithHandlers(
//		delaytimer.Bind(&ConfirmTimeoutParams{}, h.OnConfirmTimeout),
//	))
//	_ = tm.Run(ctx)
//
// Handle 须幂等。K8s 停机：ctx 取消后未分发的任务会 Release 归还后端；
// 正在 Handle 且因 ctx 取消返回的任务同样归还。SIGKILL 时 Redis 已领取任务会丢失，AMQP 靠未 Ack 重投。
// Memory 不跨进程，K8s 重启会丢未完成任务，不要用在多副本消费。
//
// Redis 后端构造：
//
//	client := delaytimer.GoRedisV9{Cmd: rdb} // rdb 为 *redis.Client 或 *redis.ClusterClient
//	backend := delaytimer.NewRedis(client, delaytimer.RedisConfig{Prefix: "dt", Shards: 4})
//
// AMQP 后端构造：
//
//	ch, err := conn.Channel()
//	backend := delaytimer.NewAMQP(ch, delaytimer.AMQPConfig{
//		Exchange: "chunk_delay", RoutingKey: "timer.job", Queue: "timer.job",
//		Shards: 4, Kinds: []string{"confirm_timeout"},
//	})
package delaytimer
