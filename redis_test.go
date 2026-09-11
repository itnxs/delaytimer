package delaytimer

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
)

type frozenClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *frozenClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *frozenClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func testRedis(clk Clock) *Redis {
	return NewRedis(newMemRedis(), RedisConfig{Prefix: "dt"}, WithRedisClock(clk))
}

func TestRedisKeysUseHashTag(t *testing.T) {
	r := NewRedis(newMemRedis(), RedisConfig{Prefix: "dt"})
	if r.kindsKey() != "{dt}:kinds" {
		t.Fatalf("kinds=%s", r.kindsKey())
	}
	if r.dueKey("answer_timeout", 0) != "{dt:answer_timeout:0}:due" {
		t.Fatalf("due=%s", r.dueKey("answer_timeout", 0))
	}
	r = NewRedis(newMemRedis(), RedisConfig{Prefix: "dt", Shards: 4})
	if r.dueKey("k", 3) != "{dt:k:3}:due" {
		t.Fatalf("due=%s", r.dueKey("k", 3))
	}
}

func TestRedisScheduleClaimAck(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	r := testRedis(func() time.Time { return now })
	job := Job{Kind: "answer_timeout", Payload: "p1", At: now.Add(-time.Millisecond)}
	if err := r.Schedule(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	got, err := r.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
	wantKey := JobKey("answer_timeout", "p1")
	if got[0].Key != wantKey || got[0].Kind != "answer_timeout" || got[0].Payload != "p1" {
		t.Fatalf("%+v", got[0])
	}
	if err := r.Ack(context.Background(), got[0]); err != nil {
		t.Fatal(err)
	}

	got, err = r.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("acked job reclaimed: %v", got)
	}
}

func TestRedisCancelBeforeClaim(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	r := testRedis(func() time.Time { return now })
	_ = r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: now})
	if err := r.Cancel(context.Background(), JobKey("kind", "p")); err != nil {
		t.Fatal(err)
	}
	got, err := r.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("canceled job claimed: %v", got)
	}
}

func TestRedisReplaceSameMemberUpdatesScore(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	r := testRedis(func() time.Time { return now })
	_ = r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: now.Add(time.Hour)})
	_ = r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: now})

	got, err := r.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Payload != "p" {
		t.Fatalf("%+v", got)
	}
}

func TestRedisClaimSkipsFuture(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	r := testRedis(func() time.Time { return now })
	_ = r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: now.Add(time.Hour)})
	got, err := r.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("future: %v", got)
	}
}

func TestRedisFailThenReclaimAfterDelay(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	clk := &frozenClock{t: now}
	r := testRedis(clk.now)
	_ = r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: now})
	got, _ := r.Claim(context.Background(), 1)
	if len(got) != 1 {
		t.Fatalf("first claim %v", got)
	}
	if err := r.Fail(context.Background(), got[0]); err != nil {
		t.Fatal(err)
	}

	again, _ := r.Claim(context.Background(), 1)
	if len(again) != 0 {
		t.Fatalf("reclaimed before delay: %v", again)
	}

	clk.set(now.Add(failRequeueDelay + time.Millisecond))
	again, err := r.Claim(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].Key != JobKey("kind", "p") {
		t.Fatalf("want reclaim, got %v", again)
	}
}

func TestRedisFailDoesNotOverwriteRescheduled(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	clk := &frozenClock{t: now}
	r := testRedis(clk.now)
	_ = r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: now})
	got, _ := r.Claim(context.Background(), 1)
	if len(got) != 1 {
		t.Fatalf("claim %v", got)
	}
	later := now.Add(time.Hour)
	if err := r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: later}); err != nil {
		t.Fatal(err)
	}
	if err := r.Fail(context.Background(), got[0]); err != nil {
		t.Fatal(err)
	}

	still, err := r.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(still) != 0 {
		t.Fatalf("future job claimed: %v", still)
	}

	clk.set(later)
	again, err := r.Claim(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].Payload != "p" {
		t.Fatalf("want rescheduled job, got %v", again)
	}
}

func TestRedisReleaseAllowsImmediateReclaim(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	r := testRedis(func() time.Time { return now })
	_ = r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: now})
	got, _ := r.Claim(context.Background(), 1)
	if len(got) != 1 {
		t.Fatalf("claim %v", got)
	}
	if err := r.Release(context.Background(), got[0]); err != nil {
		t.Fatal(err)
	}
	again, err := r.Claim(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].Key != JobKey("kind", "p") {
		t.Fatalf("want reclaim, got %v", again)
	}
}

func TestRedisAckDoesNotDeleteRescheduledJob(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	clk := &frozenClock{t: now}
	r := testRedis(clk.now)
	_ = r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: now})
	got, err := r.Claim(context.Background(), 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("claim %v err=%v", got, err)
	}

	later := now.Add(time.Hour)
	if err := r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: later}); err != nil {
		t.Fatal(err)
	}
	if err := r.Ack(context.Background(), got[0]); err != nil {
		t.Fatal(err)
	}

	still, err := r.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(still) != 0 {
		t.Fatalf("future job claimed: %v", still)
	}

	clk.set(later)
	again, err := r.Claim(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].Payload != "p" {
		t.Fatalf("want rescheduled job, got %v", again)
	}
}

func TestRedisConcurrentClaimDoesNotDuplicate(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	r := testRedis(func() time.Time { return now })
	for i := 0; i < 10; i++ {
		payload := "k" + strconv.Itoa(i)
		_ = r.Schedule(context.Background(), Job{Kind: "kind", Payload: payload, At: now})
	}

	a, _ := r.Claim(context.Background(), 6)
	b, _ := r.Claim(context.Background(), 6)
	seen := map[string]int{}
	for _, j := range append(a, b...) {
		seen[j.Key]++
	}
	if len(seen) != 10 {
		t.Fatalf("got %d unique of %d+%d", len(seen), len(a), len(b))
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("dup %s=%d", k, n)
		}
	}
}

func TestRedisConcurrentClaimCAS(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	r := testRedis(func() time.Time { return now })
	const n = 20
	for i := 0; i < n; i++ {
		payload := "k" + strconv.Itoa(i)
		if err := r.Schedule(context.Background(), Job{Kind: "kind", Payload: payload, At: now}); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var got []Job
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				jobs, err := r.Claim(context.Background(), 5)
				if err != nil {
					t.Error(err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				got = append(got, jobs...)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	seen := map[string]int{}
	for _, j := range got {
		seen[j.Key]++
	}
	if len(seen) != n {
		t.Fatalf("got %d unique of %d", len(seen), len(got))
	}
	for k, c := range seen {
		if c != 1 {
			t.Fatalf("dup %s=%d", k, c)
		}
	}
}

func countZMember(s *memRedis, member string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, z := range s.zsets {
		if _, ok := z[member]; ok {
			n++
		}
	}
	return n
}

func TestRedisHashSameKeyStaysOnOneShard(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	store := newMemRedis()
	r := NewRedis(store, RedisConfig{Prefix: "dt", Shards: 8, ShardMode: ShardHash}, WithRedisClock(func() time.Time { return now }))
	job := Job{Kind: "kind", Payload: "p", At: now}
	member := JobKey(job.Kind, job.Payload)
	for i := 0; i < 5; i++ {
		if err := r.Schedule(context.Background(), job); err != nil {
			t.Fatal(err)
		}
	}
	if n := countZMember(store, member); n != 1 {
		t.Fatalf("member on %d shards", n)
	}
	want := r.dueKey(job.Kind, shardIndex(ShardHash, 8, member, 0))
	store.mu.Lock()
	_, ok := store.zsets[want][member]
	store.mu.Unlock()
	if !ok {
		t.Fatalf("want %s", want)
	}
}

func TestRedisHashCancelHitsHashedShard(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	r := NewRedis(newMemRedis(), RedisConfig{Prefix: "dt", Shards: 8, ShardMode: ShardHash}, WithRedisClock(func() time.Time { return now }))
	_ = r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: now})
	if err := r.Cancel(context.Background(), JobKey("kind", "p")); err != nil {
		t.Fatal(err)
	}
	got, err := r.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("canceled: %v", got)
	}
}

func TestRedisRandomRescheduleLivesOnOneShard(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	store := newMemRedis()
	r := NewRedis(store, RedisConfig{Prefix: "dt", Shards: 8, ShardMode: ShardRandom}, WithRedisClock(func() time.Time { return now }))
	job := Job{Kind: "kind", Payload: "p", At: now}
	member := JobKey(job.Kind, job.Payload)
	for i := 0; i < 20; i++ {
		if err := r.Schedule(context.Background(), job); err != nil {
			t.Fatal(err)
		}
	}
	if n := countZMember(store, member); n != 1 {
		t.Fatalf("member on %d shards", n)
	}
}

func TestRedisRandomCancelScansAllShards(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	r := NewRedis(newMemRedis(), RedisConfig{Prefix: "dt", Shards: 8, ShardMode: ShardRandom}, WithRedisClock(func() time.Time { return now }))
	_ = r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: now})
	if err := r.Cancel(context.Background(), JobKey("kind", "p")); err != nil {
		t.Fatal(err)
	}
	got, err := r.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("canceled: %v", got)
	}
}

func TestRedisClaimMultipleKinds(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	r := testRedis(func() time.Time { return now })
	_ = r.Schedule(context.Background(), Job{Kind: "a", Payload: "p1", At: now})
	_ = r.Schedule(context.Background(), Job{Kind: "b", Payload: "p2", At: now})
	got, err := r.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d %+v", len(got), got)
	}
	seen := map[string]bool{}
	for _, j := range got {
		seen[j.Kind] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("%v", seen)
	}
}

func TestRedisRandomFailDoesNotOverwriteRescheduled(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	clk := &frozenClock{t: now}
	r := NewRedis(newMemRedis(), RedisConfig{Prefix: "dt", Shards: 8, ShardMode: ShardRandom}, WithRedisClock(clk.now))
	_ = r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: now})
	got, _ := r.Claim(context.Background(), 1)
	if len(got) != 1 {
		t.Fatalf("claim %v", got)
	}
	later := now.Add(time.Hour)
	if err := r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: later}); err != nil {
		t.Fatal(err)
	}
	if err := r.Fail(context.Background(), got[0]); err != nil {
		t.Fatal(err)
	}
	still, err := r.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(still) != 0 {
		t.Fatalf("future job claimed: %v", still)
	}
	clk.set(later)
	again, err := r.Claim(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].Payload != "p" {
		t.Fatalf("want rescheduled job, got %v", again)
	}
}

func TestRedisMilliSameAtStaysOnTimeShard(t *testing.T) {
	now := time.UnixMilli(1_000_003)
	store := newMemRedis()
	r := NewRedis(store, RedisConfig{Prefix: "dt", Shards: 8, ShardMode: ShardMilli}, WithRedisClock(func() time.Time { return now }))
	job := Job{Kind: "kind", Payload: "p", At: now}
	member := JobKey(job.Kind, job.Payload)
	for i := 0; i < 5; i++ {
		if err := r.Schedule(context.Background(), job); err != nil {
			t.Fatal(err)
		}
	}
	if n := countZMember(store, member); n != 1 {
		t.Fatalf("member on %d shards", n)
	}
	want := r.dueKey(job.Kind, shardIndex(ShardMilli, 8, member, now.UnixMilli()))
	store.mu.Lock()
	_, ok := store.zsets[want][member]
	store.mu.Unlock()
	if !ok {
		t.Fatalf("want %s", want)
	}
}

func TestRedisMilliRescheduleMovesShard(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	store := newMemRedis()
	r := NewRedis(store, RedisConfig{Prefix: "dt", Shards: 8, ShardMode: ShardMilli}, WithRedisClock(func() time.Time { return now }))
	job := Job{Kind: "kind", Payload: "p", At: now}
	member := JobKey(job.Kind, job.Payload)
	if err := r.Schedule(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	later := now.Add(3 * time.Millisecond)
	if err := r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: later}); err != nil {
		t.Fatal(err)
	}
	if n := countZMember(store, member); n != 1 {
		t.Fatalf("member on %d shards", n)
	}
	want := r.dueKey(job.Kind, shardIndex(ShardMilli, 8, member, later.UnixMilli()))
	store.mu.Lock()
	_, ok := store.zsets[want][member]
	store.mu.Unlock()
	if !ok {
		t.Fatalf("want %s", want)
	}
}

func TestRedisMilliCancelScansAllShards(t *testing.T) {
	now := time.UnixMilli(1_000_003)
	r := NewRedis(newMemRedis(), RedisConfig{Prefix: "dt", Shards: 8, ShardMode: ShardMilli}, WithRedisClock(func() time.Time { return now }))
	_ = r.Schedule(context.Background(), Job{Kind: "kind", Payload: "p", At: now})
	if err := r.Cancel(context.Background(), JobKey("kind", "p")); err != nil {
		t.Fatal(err)
	}
	got, err := r.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("canceled: %v", got)
	}
}
