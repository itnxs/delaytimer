package delaytimer

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// RedisCommands 分片 ZSET 所需命令
type RedisCommands interface {
	ZAdd(ctx context.Context, key string, score float64, member string) error
	ZAddNX(ctx context.Context, key string, score float64, member string) (int64, error)
	ZRem(ctx context.Context, key, member string) (int64, error)
	ZScore(ctx context.Context, key, member string) (float64, bool, error)
	ZRangeByScore(ctx context.Context, key, min, max string, count int64) ([]RedisMember, error)
	SAdd(ctx context.Context, key string, members ...string) error
	SMembers(ctx context.Context, key string) ([]string, error)
	TimeMilli(ctx context.Context) (int64, error)
}

// RedisMember 有序集合成员与分数
type RedisMember struct {
	Member string
	Score  float64
}

var (
	_ Backend  = (*Redis)(nil)
	_ releaser = (*Redis)(nil)
)

// RedisConfig Redis 后端配置
type RedisConfig struct {
	Prefix    string
	Shards    int       // 每种 EventName 的分片数，默认 1
	ShardMode ShardMode // Hash（默认）/ Random / Milli
}

// Redis 可水平扩展的 Redis 计时器。
// 每种 EventName × shard 一个 ZSET：`{prefix:kind:shard}:due`，member=JobKey，score=到期 Unix 毫秒。
// Claim 用 ZREM 竞争领取（领取即删除）。
type Redis struct {
	cmd           RedisCommands
	prefix        string
	shards        int
	mode          ShardMode
	clock         Clock
	useServerTime bool
	claimAt       atomic.Uint64
}

// RedisOption Redis 选项
type RedisOption func(*Redis)

// WithRedisClock 注入时钟；同时关闭 Redis TIME，便于单测。
func WithRedisClock(c Clock) RedisOption {
	return func(r *Redis) {
		if c != nil {
			r.clock = c
			r.useServerTime = false
		}
	}
}

// NewRedis 使用 Redis 命令实现。c 一般为 GoRedisV9。
func NewRedis(c RedisCommands, cfg RedisConfig, opts ...RedisOption) *Redis {
	if c == nil {
		panic("delaytimer: redis commands is nil")
	}
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = "delaytimer"
	}
	r := &Redis{
		cmd:           c,
		prefix:        prefix,
		shards:        normalizeShards(cfg.Shards),
		mode:          cfg.ShardMode,
		clock:         time.Now,
		useServerTime: true,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *Redis) kindsKey() string {
	return "{" + r.prefix + "}:kinds"
}

func (r *Redis) dueKey(kind string, shard int) string {
	return "{" + r.prefix + ":" + kind + ":" + strconv.Itoa(shard) + "}:due"
}

func (r *Redis) nowMilli(ctx context.Context) int64 {
	if r.useServerTime {
		if ms, err := r.cmd.TimeMilli(ctx); err == nil {
			return ms
		}
	}
	return r.clock().UnixMilli()
}

func redisMember(job Job) string {
	return JobKey(job.Kind, job.Payload)
}

func splitJobKey(key string) (kind, payload string) {
	kind, payload, _ = strings.Cut(key, ":")
	return
}

func (r *Redis) pickShard(member string, milli int64) int {
	return shardIndex(r.mode, r.shards, member, milli)
}

func (r *Redis) remAllShards(ctx context.Context, kind, member string) error {
	for i := 0; i < r.shards; i++ {
		if _, err := r.cmd.ZRem(ctx, r.dueKey(kind, i), member); err != nil {
			return err
		}
	}
	return nil
}

func (r *Redis) existsAnyShard(ctx context.Context, kind, member string) (bool, error) {
	for i := 0; i < r.shards; i++ {
		_, ok, err := r.cmd.ZScore(ctx, r.dueKey(kind, i), member)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func (r *Redis) addKind(ctx context.Context, kind string) error {
	return r.cmd.SAdd(ctx, r.kindsKey(), kind)
}

func (r *Redis) put(ctx context.Context, job Job, nx bool, score float64) error {
	member := redisMember(job)
	kind := job.Kind
	if err := r.addKind(ctx, kind); err != nil {
		return err
	}
	if !nx && r.mode.scanAll() {
		if err := r.remAllShards(ctx, kind, member); err != nil {
			return err
		}
	}
	if nx && r.mode.scanAll() {
		ok, err := r.existsAnyShard(ctx, kind, member)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	shard := r.pickShard(member, int64(score))
	if nx {
		_, err := r.cmd.ZAddNX(ctx, r.dueKey(kind, shard), score, member)
		return err
	}
	return r.cmd.ZAdd(ctx, r.dueKey(kind, shard), score, member)
}

// Schedule 写入或按 member 覆盖到期时间
func (r *Redis) Schedule(ctx context.Context, job Job) error {
	return r.put(ctx, job, false, float64(job.At.UnixMilli()))
}

// Cancel 删除尚未领取的任务
func (r *Redis) Cancel(ctx context.Context, key string) error {
	kind, _ := splitJobKey(key)
	if r.mode.scanAll() {
		return r.remAllShards(ctx, kind, key)
	}
	_, err := r.cmd.ZRem(ctx, r.dueKey(kind, r.pickShard(key, 0)), key)
	return err
}

// Claim 领取到期任务；ZREM 返回 1 的副本才算领到
func (r *Redis) Claim(ctx context.Context, n int) ([]Job, error) {
	if n < 1 {
		n = 1
	}
	kinds, err := r.cmd.SMembers(ctx, r.kindsKey())
	if err != nil {
		return nil, err
	}
	if len(kinds) == 0 {
		return nil, nil
	}
	nowS := strconv.FormatInt(r.nowMilli(ctx), 10)
	slots := len(kinds) * r.shards
	start := int(r.claimAt.Add(1) - 1)
	out := make([]Job, 0, n)
	for i := 0; i < slots && len(out) < n; i++ {
		idx := (start + i) % slots
		kind := kinds[idx/r.shards]
		shard := idx % r.shards
		remain := n - len(out)
		picked, err := r.claimFrom(ctx, r.dueKey(kind, shard), nowS, remain)
		if err != nil {
			return nil, err
		}
		out = append(out, picked...)
	}
	return out, nil
}

func (r *Redis) claimFrom(ctx context.Context, zkey, nowS string, n int) ([]Job, error) {
	items, err := r.cmd.ZRangeByScore(ctx, zkey, "-inf", nowS, int64(n))
	if err != nil {
		return nil, err
	}
	out := make([]Job, 0, len(items))
	for _, item := range items {
		removed, err := r.cmd.ZRem(ctx, zkey, item.Member)
		if err != nil {
			return nil, err
		}
		if removed != 1 {
			continue
		}
		kind, payload := splitJobKey(item.Member)
		out = append(out, Job{
			Key:     item.Member,
			Kind:    kind,
			Payload: payload,
			At:      time.UnixMilli(int64(item.Score)),
		})
	}
	return out, nil
}

// Ack 领取时已从 ZSET 删除
func (r *Redis) Ack(context.Context, Job) error { return nil }

// Fail 重新入队；若同 member 已被新 Schedule 覆盖则不改写
func (r *Redis) Fail(ctx context.Context, job Job) error {
	at := r.nowMilli(ctx) + failRequeueDelay.Milliseconds()
	return r.put(ctx, job, true, float64(at))
}

// Release 立即重新入队，供其他副本领取（K8s 滚动重启）
func (r *Redis) Release(ctx context.Context, job Job) error {
	return r.put(ctx, job, true, float64(r.nowMilli(ctx)))
}
