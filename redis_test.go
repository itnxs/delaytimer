package delaytimer

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type memZSet struct {
	mu    sync.Mutex
	key   string
	items map[string]int64
	now   time.Time
}

func newMemZSet(key string, now time.Time) *memZSet {
	return &memZSet{key: key, items: make(map[string]int64), now: now}
}

func (s *memZSet) Time(ctx context.Context) *redis.TimeCmd {
	cmd := redis.NewTimeCmd(ctx, "time")
	s.mu.Lock()
	cmd.SetVal(s.now)
	s.mu.Unlock()
	return cmd
}

func (s *memZSet) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, "zadd")
	s.mu.Lock()
	defer s.mu.Unlock()
	if key != s.key {
		cmd.SetVal(0)
		return cmd
	}
	var n int64
	for _, m := range members {
		member, _ := m.Member.(string)
		if _, ok := s.items[member]; !ok {
			n++
		}
		s.items[member] = int64(m.Score)
	}
	cmd.SetVal(n)
	return cmd
}

func (s *memZSet) ZAddNX(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, "zadd")
	s.mu.Lock()
	defer s.mu.Unlock()
	if key != s.key {
		cmd.SetVal(0)
		return cmd
	}
	var n int64
	for _, m := range members {
		member, _ := m.Member.(string)
		if _, ok := s.items[member]; ok {
			continue
		}
		s.items[member] = int64(m.Score)
		n++
	}
	cmd.SetVal(n)
	return cmd
}

func (s *memZSet) ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, "zrem")
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	if key == s.key {
		for _, m := range members {
			member, _ := m.(string)
			if _, ok := s.items[member]; ok {
				delete(s.items, member)
				n++
			}
		}
	}
	cmd.SetVal(n)
	return cmd
}

func (s *memZSet) ZRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.ZSliceCmd {
	cmd := redis.NewZSliceCmd(ctx, "zrangebyscore")
	s.mu.Lock()
	defer s.mu.Unlock()
	if key != s.key {
		cmd.SetVal(nil)
		return cmd
	}
	max, err := strconv.ParseInt(opt.Max, 10, 64)
	if err != nil {
		cmd.SetErr(err)
		return cmd
	}
	var out []redis.Z
	for member, score := range s.items {
		if score <= max {
			out = append(out, redis.Z{Score: float64(score), Member: member})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score == out[j].Score {
			a, _ := out[i].Member.(string)
			b, _ := out[j].Member.(string)
			return a < b
		}
		return out[i].Score < out[j].Score
	})
	if opt.Count > 0 && int64(len(out)) > opt.Count {
		out = out[:opt.Count]
	}
	cmd.SetVal(out)
	return cmd
}

func testRedis(now time.Time) (*Redis, *memZSet) {
	z := newMemZSet("jobs", now)
	r := newRedis(z, "jobs", WithRedisClock(func() time.Time { return now }))
	return r, z
}

func TestRedisNewPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	newRedis(nil, "jobs")
}

func TestRedisEmptyKeyPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	newRedis(newMemZSet("jobs", time.Now()), "")
}

func TestRedisScheduleCancelClaim(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	r, z := testRedis(now)
	ctx := context.Background()
	due := Task{Key: "order:p1", Kind: "order", Payload: "p1", At: now.Add(-time.Second)}
	later := Task{Key: "order:p2", Kind: "order", Payload: "p2", At: now.Add(time.Hour)}
	if err := r.Schedule(ctx, due); err != nil {
		t.Fatal(err)
	}
	if err := r.Schedule(ctx, later); err != nil {
		t.Fatal(err)
	}
	if err := r.Schedule(ctx, Task{Key: "order:p1", Kind: "order", Payload: "p1", At: now.Add(-2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	z.mu.Lock()
	if z.items["order:p1"] != now.Add(-2*time.Second).UnixMilli() {
		t.Fatalf("score not overwritten: %d", z.items["order:p1"])
	}
	z.mu.Unlock()

	got, err := r.Claim(ctx, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "order:p1" || got[0].Kind != "order" || got[0].Payload != "p1" {
		t.Fatalf("got=%+v", got)
	}
	empty, err := r.Claim(ctx, 1)
	if err != nil || len(empty) != 0 {
		t.Fatalf("no due should return empty immediately: %v %v", empty, err)
	}
	if err := r.Cancel(ctx, "order:p2"); err != nil {
		t.Fatal(err)
	}
	z.mu.Lock()
	_, ok := z.items["order:p2"]
	z.mu.Unlock()
	if ok {
		t.Fatal("cancel should remove member")
	}
	if err := r.Ack(ctx, due); err != nil {
		t.Fatal(err)
	}
}

func TestRedisClaimCompetitive(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	z := newMemZSet("jobs", now)
	clock := func() time.Time { return now }
	r1 := newRedis(z, "jobs", WithRedisClock(clock))
	r2 := newRedis(z, "jobs", WithRedisClock(clock))
	ctx := context.Background()
	if err := r1.Schedule(ctx, Task{Key: "order:p1", Kind: "order", Payload: "p1", At: now.Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}
	var n1, n2 int
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		got, err := r1.Claim(ctx, 1)
		if err != nil {
			t.Error(err)
		}
		n1 = len(got)
	}()
	go func() {
		defer wg.Done()
		got, err := r2.Claim(ctx, 1)
		if err != nil {
			t.Error(err)
		}
		n2 = len(got)
	}()
	wg.Wait()
	if n1+n2 != 1 {
		t.Fatalf("n1=%d n2=%d", n1, n2)
	}
}

func TestRedisFailAndRelease(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	r, z := testRedis(now)
	ctx := context.Background()
	task := Task{Key: "order:p1", Kind: "order", Payload: "p1", At: now.Add(-time.Second)}
	if err := r.Schedule(ctx, task); err != nil {
		t.Fatal(err)
	}
	got, err := r.Claim(ctx, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("claim=%v err=%v", got, err)
	}
	if err := r.Fail(ctx, got[0]); err != nil {
		t.Fatal(err)
	}
	z.mu.Lock()
	failAt := z.items["order:p1"]
	z.mu.Unlock()
	if failAt != now.UnixMilli()+failRequeueDelay.Milliseconds() {
		t.Fatalf("fail score=%d", failAt)
	}
	empty, err := r.Claim(ctx, 1)
	if err != nil || len(empty) != 0 {
		t.Fatalf("not due yet: %v %v", empty, err)
	}

	newer := Task{Key: "order:p1", Kind: "order", Payload: "p1", At: now.Add(time.Hour)}
	if err := r.Schedule(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if err := r.Fail(ctx, got[0]); err != nil {
		t.Fatal(err)
	}
	z.mu.Lock()
	if z.items["order:p1"] != now.Add(time.Hour).UnixMilli() {
		t.Fatalf("fail must not overwrite: %d", z.items["order:p1"])
	}
	z.mu.Unlock()

	claimed, err := r.Claim(ctx, 1)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("future task: %v %v", claimed, err)
	}
	if err := r.Cancel(ctx, "order:p1"); err != nil {
		t.Fatal(err)
	}
	if err := r.Release(ctx, got[0]); err != nil {
		t.Fatal(err)
	}
	got, err = r.Claim(ctx, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("release=%v err=%v", got, err)
	}
}

func TestRedisClaimMinN(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	r, _ := testRedis(now)
	ctx := context.Background()
	if err := r.Schedule(ctx, Task{Key: "order:p1", Kind: "order", Payload: "p1", At: now.Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}
	got, err := r.Claim(ctx, 0)
	if err != nil || len(got) != 1 {
		t.Fatalf("n<1 got=%v err=%v", got, err)
	}
}
