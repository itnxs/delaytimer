package delaytimer

import (
    "context"
    "strconv"
    "strings"
    "time"

    "github.com/pkg/errors"
    "github.com/redis/go-redis/v9"
)

var _ Store = (*Redis)(nil)

// redisZSet 单个有序集合所需命令，*redis.Client / ClusterClient 已满足。
type redisZSet interface {
    Time(ctx context.Context) *redis.TimeCmd
    ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd
    ZAddNX(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd
    ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
    ZRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.ZSliceCmd
}

// Redis 单 ZSET 延迟任务存储。应用服务，对外实现 Store。
// Claim 用 ZREM 竞争领取（领取即删除）。
type Redis struct {
    cmd           redisZSet
    key           string
    clock         Clock
    useServerTime bool
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

// NewRedis 使用 go-redis Cmdable。cmd 一般为 *redis.Client / ClusterClient。
func NewRedis(cmd redis.Cmdable, key string, opts ...RedisOption) *Redis {
    if cmd == nil {
        panic(errors.New("redis commands is nil"))
    }
    return newRedis(cmd, key, opts...)
}

func newRedis(cmd redisZSet, key string, opts ...RedisOption) *Redis {
    if cmd == nil {
        panic(errors.New("redis commands is nil"))
    }
    if key == "" {
        panic(errors.New("redis key is empty"))
    }
    r := &Redis{
        cmd:           cmd,
        key:           key,
        clock:         time.Now,
        useServerTime: true,
    }
    for _, opt := range opts {
        opt(r)
    }
    return r
}

// Schedule 写入或按 member 覆盖到期时间
func (r *Redis) Schedule(ctx context.Context, task Task) error {
    return r.put(ctx, task, float64(task.At.UnixMilli()), false)
}

// Cancel 删除尚未领取的任务
func (r *Redis) Cancel(ctx context.Context, key string) error {
    return r.cmd.ZRem(ctx, r.key, key).Err()
}

// Claim 领取到期任务；ZREM 返回 1 的副本才算领到。无任务时立即返回，不阻塞。
func (r *Redis) Claim(ctx context.Context, n int) ([]Task, error) {
    if n < 1 {
        n = 1
    }
    nowS := strconv.FormatInt(r.nowMilli(ctx), 10)
    items, err := r.cmd.ZRangeByScoreWithScores(ctx, r.key, &redis.ZRangeBy{
        Min:   "-inf",
        Max:   nowS,
        Count: int64(n),
    }).Result()
    if err != nil {
        return nil, err
    }
    out := make([]Task, 0, len(items))
    for _, item := range items {
        member, _ := item.Member.(string)
        removed, err := r.cmd.ZRem(ctx, r.key, member).Result()
        if err != nil {
            return nil, err
        }
        if removed != 1 {
            continue
        }
        out = append(out, redisMember(member).task(item.Score))
    }
    return out, nil
}

// Ack 领取时已从 ZSET 删除
func (r *Redis) Ack(context.Context, Task) error { return nil }

// Fail 重新入队；若同 member 已被新 Schedule 覆盖则不改写
func (r *Redis) Fail(ctx context.Context, task Task) error {
    at := float64(r.nowMilli(ctx) + failRequeueDelay.Milliseconds())
    return r.put(ctx, task, at, true)
}

// Release 立即重新入队，供其他副本领取（K8s 滚动重启）
func (r *Redis) Release(ctx context.Context, task Task) error {
    return r.put(ctx, task, float64(r.nowMilli(ctx)), true)
}

func (r *Redis) nowMilli(ctx context.Context) int64 {
    if r.useServerTime {
        if tm, err := r.cmd.Time(ctx).Result(); err == nil {
            return tm.UnixMilli()
        }
    }
    return r.clock().UnixMilli()
}

func (r *Redis) put(ctx context.Context, task Task, score float64, nx bool) error {
    z := redis.Z{Score: score, Member: string(redisMemberOf(task))}
    if nx {
        return r.cmd.ZAddNX(ctx, r.key, z).Err()
    }
    return r.cmd.ZAdd(ctx, r.key, z).Err()
}

// redisMember 有序集合成员，身份为任务 Key（kind:payload）。
type redisMember string

func redisMemberOf(task Task) redisMember {
    return redisMember(task.Key)
}

func (m redisMember) task(score float64) Task {
    kind, payload, _ := strings.Cut(string(m), ":")
    return Task{
        Key:     string(m),
        Kind:    kind,
        Payload: payload,
        At:      time.UnixMilli(int64(score)),
    }
}
