package delaytimer

import (
	"context"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

var (
	_ RedisCmdable  = (*redis.Client)(nil)
	_ RedisCmdable  = (*redis.ClusterClient)(nil)
	_ RedisCommands = GoRedisV9{}
)

// RedisCmdable go-redis v9 子集：ZSET + SET + TIME
type RedisCmdable interface {
	Time(ctx context.Context) *redis.TimeCmd
	ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd
	ZAddNX(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd
	ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	ZScore(ctx context.Context, key, member string) *redis.FloatCmd
	ZRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.ZSliceCmd
	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
}

// GoRedisV9 适配 github.com/redis/go-redis/v9，实现 RedisCommands
type GoRedisV9 struct {
	Cmd RedisCmdable
}

func (g GoRedisV9) cmdable() (RedisCmdable, error) {
	if g.Cmd == nil {
		return nil, errors.New("delaytimer: redis cmdable is nil")
	}
	return g.Cmd, nil
}

// TimeMilli Redis TIME
func (g GoRedisV9) TimeMilli(ctx context.Context) (int64, error) {
	cmd, err := g.cmdable()
	if err != nil {
		return 0, err
	}
	tm, err := cmd.Time(ctx).Result()
	if err != nil {
		return 0, err
	}
	return tm.UnixMilli(), nil
}

// ZAdd 写入有序集合（已存在则覆盖 score）
func (g GoRedisV9) ZAdd(ctx context.Context, key string, score float64, member string) error {
	cmd, err := g.cmdable()
	if err != nil {
		return err
	}
	return cmd.ZAdd(ctx, key, redis.Z{Score: score, Member: member}).Err()
}

// ZAddNX 仅当 member 不存在时写入
func (g GoRedisV9) ZAddNX(ctx context.Context, key string, score float64, member string) (int64, error) {
	cmd, err := g.cmdable()
	if err != nil {
		return 0, err
	}
	return cmd.ZAddNX(ctx, key, redis.Z{Score: score, Member: member}).Result()
}

// ZRem 删除成员，返回实际删除数量
func (g GoRedisV9) ZRem(ctx context.Context, key, member string) (int64, error) {
	cmd, err := g.cmdable()
	if err != nil {
		return 0, err
	}
	return cmd.ZRem(ctx, key, member).Result()
}

// ZScore 查询成员分数；不存在时 ok=false
func (g GoRedisV9) ZScore(ctx context.Context, key, member string) (float64, bool, error) {
	cmd, err := g.cmdable()
	if err != nil {
		return 0, false, err
	}
	v, err := cmd.ZScore(ctx, key, member).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return v, true, nil
}

// SAdd 记录已出现的 EventName
func (g GoRedisV9) SAdd(ctx context.Context, key string, members ...string) error {
	cmd, err := g.cmdable()
	if err != nil {
		return err
	}
	args := make([]interface{}, len(members))
	for i, m := range members {
		args[i] = m
	}
	return cmd.SAdd(ctx, key, args...).Err()
}

// SMembers 列出集合全部成员
func (g GoRedisV9) SMembers(ctx context.Context, key string) ([]string, error) {
	cmd, err := g.cmdable()
	if err != nil {
		return nil, err
	}
	return cmd.SMembers(ctx, key).Result()
}

// ZRangeByScore 按分数取成员（含 score）
func (g GoRedisV9) ZRangeByScore(ctx context.Context, key, min, max string, count int64) ([]RedisMember, error) {
	cmd, err := g.cmdable()
	if err != nil {
		return nil, err
	}
	zs, err := cmd.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{Min: min, Max: max, Count: count}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]RedisMember, len(zs))
	for i, z := range zs {
		member, _ := z.Member.(string)
		out[i] = RedisMember{Member: member, Score: z.Score}
	}
	return out, nil
}
