package delaytimer

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

var (
	_ Store                  = (*Redis)(nil)
	_ redis.SortedSetCmdable = (*redis.Client)(nil)
	_ redis.SortedSetCmdable = (*redis.ClusterClient)(nil)
	_ redis.Scripter         = (*redis.Client)(nil)
	_ redis.Scripter         = (*redis.ClusterClient)(nil)
)

// redisStore SortedSet 写入 + Lua Claim。*redis.Client / ClusterClient 已满足。
type redisStore interface {
	redis.SortedSetCmdable
	redis.Scripter
}

// redisClaimLua 一次领取到期 member：ZRANGEBYSCORE + ZREM。
// ARGV[1] 为进程毫秒时间。ARGV[2] 为条数。
const redisClaimLua = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local n = tonumber(ARGV[2])
if n == nil or n < 1 then
  n = 1
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

var redisClaimScript = redis.NewScript(redisClaimLua)

// Redis 单 ZSET 延迟任务。Claim 用一条 Lua 竞争领取，无到期任务时立即返回空，不阻塞。
type Redis struct {
	cmd redisStore
	key string
}

// NewRedis 使用 go-redis Cmdable。cmd 一般为 *redis.Client / ClusterClient，key 为 ZSET 名。
// cmd 为空或 key 为空时 panic。
func NewRedis(cmd redis.Cmdable, key string) *Redis {
	if cmd == nil {
		panic(ErrNilRedis)
	}
	return newRedis(cmd, key)
}

func newRedis(cmd redisStore, key string) *Redis {
	if cmd == nil {
		panic(ErrNilRedis)
	}
	if key == "" {
		panic(ErrEmptyRedisKey)
	}
	return &Redis{cmd: cmd, key: key}
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
// EVAL 不绑入参 ctx：关闭时服务端可能已删除 member，仍要把结果交回调用方排空。
func (r *Redis) Claim(ctx context.Context, n int) ([]Task, error) {
	if n < 1 {
		n = 1
	}
	nowArg := strconv.FormatInt(time.Now().UnixMilli(), 10)
	raw, err := redisClaimScript.Run(context.WithoutCancel(ctx), r.cmd, []string{r.key}, nowArg, n).Result()
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
	at := float64(time.Now().UnixMilli() + failRequeueDelay.Milliseconds())
	return r.put(ctx, task, at, true)
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
		return nil, fmt.Errorf("redis claim result type mismatch (got=%T)", raw)
	}
	output := make([]Task, 0, len(arr)/2)
	for i := 0; i+1 < len(arr); i += 2 {
		member := cast.ToString(arr[i])
		score := cast.ToInt64(arr[i+1])
		output = append(output, redisMember(member).task(float64(score)))
	}
	return output, nil
}
