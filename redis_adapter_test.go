package delaytimer

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

type ctxCmdSpy struct {
	ctx context.Context
}

func (s *ctxCmdSpy) Time(ctx context.Context) *redis.TimeCmd {
	return redis.NewTimeCmd(ctx)
}

func (s *ctxCmdSpy) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	s.ctx = ctx
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(int64(1))
	return cmd
}

func (s *ctxCmdSpy) ZAddNX(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	return redis.NewIntCmd(ctx)
}

func (s *ctxCmdSpy) ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return redis.NewIntCmd(ctx)
}

func (s *ctxCmdSpy) ZRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.ZSliceCmd {
	return redis.NewZSliceCmd(ctx)
}

func (s *ctxCmdSpy) ZScore(ctx context.Context, key, member string) *redis.FloatCmd {
	return redis.NewFloatCmd(ctx)
}

func (s *ctxCmdSpy) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return redis.NewIntCmd(ctx)
}

func (s *ctxCmdSpy) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	return redis.NewStringSliceCmd(ctx)
}

func TestGoRedisV9ZAddPassesContext(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "from-test")

	spy := &ctxCmdSpy{}
	g := GoRedisV9{Cmd: spy}
	if err := g.ZAdd(ctx, "k", 1, "m"); err != nil {
		t.Fatal(err)
	}
	if spy.ctx.Value(ctxKey{}) != "from-test" {
		t.Fatal("zadd did not receive ctx")
	}
}

func TestGoRedisV9NilCmd(t *testing.T) {
	g := GoRedisV9{}
	if err := g.ZAdd(context.Background(), "k", 1, "m"); err == nil {
		t.Fatal("expected error")
	}
}
