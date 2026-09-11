package delaytimer

import (
	"context"
	"strconv"
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
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
}

// redisClaimLua 一次领取到期 member：ZRANGEBYSCORE + ZREM。
// ARGV[1] 为毫秒时间；空字符串则用 Redis TIME。ARGV[2] 为条数。
const redisClaimLua = `
local key = KEYS[1]
local now = ARGV[1]
local n = tonumber(ARGV[2])
if n == nil or n < 1 then
  n = 1
end
if now == false or now == nil or now == '' then
  local t = redis.call('TIME')
  now = tostring(tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000))
end
local items = redis.call('ZRANGEBYSCORE', key, '-inf', now, 'WITHSCORES', 'LIMIT', 0, n)
if #items == 0 then
  return items
end
local members = {}
for i = 1, #items, 2 do
  members[#members + 1] = items[i]
end
redis.call('ZREM', key, unpack(members))
return items
`

// Redis 单 ZSET 延迟任务。Claim 用一条 Lua 竞争领取，无到期任务时立即返回空，不阻塞。
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

// NewRedis 使用 go-redis Cmdable。cmd 一般为 *redis.Client / ClusterClient，key 为 ZSET 名。
// cmd 为空或 key 为空时 panic。
func NewRedis(cmd redis.Cmdable, key string, opts ...RedisOption) *Redis {
	if cmd == nil {
		panic(ErrNilRedis)
	}
	return newRedis(cmd, key, opts...)
}

func newRedis(cmd redisZSet, key string, opts ...RedisOption) *Redis {
	if cmd == nil {
		panic(ErrNilRedis)
	}
	if key == "" {
		panic(ErrEmptyRedisKey)
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

// Claim 领取到期任务。一条 Lua 完成 ZRANGEBYSCORE + ZREM，无任务时立即返回空。
func (r *Redis) Claim(ctx context.Context, n int) ([]Task, error) {
	if n < 1 {
		n = 1
	}
	var nowArg interface{} = ""
	if !r.useServerTime {
		nowArg = strconv.FormatInt(r.clock().UnixMilli(), 10)
	}
	raw, err := r.cmd.Eval(ctx, redisClaimLua, []string{r.key}, nowArg, n).Result()
	if err != nil {
		return nil, err
	}
	return tasksFromClaimEval(raw)
}

// Ack 领取时已从 ZSET 删除
func (r *Redis) Ack(context.Context, Task) error { return nil }

// Close Redis 后端不关闭传入的客户端
func (r *Redis) Close() error { return nil }

// Fail 重新入队；若同 member 已被新 Schedule 覆盖则不改写
func (r *Redis) Fail(ctx context.Context, task Task) error {
	at := float64(r.nowMilli(ctx) + failRequeueDelay.Milliseconds())
	return r.put(ctx, task, at, true)
}

// Release 立即重新入队，供其他副本领取。同 Key 已被新 Schedule 覆盖则不改写。
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

// redisMember 有序集合成员，身份为任务 Key。
type redisMember string

func redisMemberOf(task Task) redisMember {
	return redisMember(task.Key)
}

func (m redisMember) task(score float64) Task {
	kind, payload := decodeTaskKey(string(m))
	return Task{
		Key:     string(m),
		Kind:    kind,
		Payload: payload,
		At:      time.UnixMilli(int64(score)),
	}
}

func tasksFromClaimEval(raw any) ([]Task, error) {
	if raw == nil {
		return nil, nil
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil, errors.Errorf("redis claim: unexpected type %T", raw)
	}
	out := make([]Task, 0, len(arr)/2)
	for i := 0; i+1 < len(arr); i += 2 {
		member := redisEvalString(arr[i])
		score := redisEvalInt64(arr[i+1])
		out = append(out, redisMember(member).task(float64(score)))
	}
	return out, nil
}

func redisEvalString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return ""
	}
}

func redisEvalInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	case []byte:
		n, _ := strconv.ParseInt(string(x), 10, 64)
		return n
	default:
		return 0
	}
}
