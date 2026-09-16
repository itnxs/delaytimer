package delaytimer

import (
	"context"
	"os"
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
	evalN int
	zremN int
	timeN int
}

func newMemZSet(key string, now time.Time) *memZSet {
	return &memZSet{key: key, items: make(map[string]int64), now: now}
}

func (s *memZSet) Time(ctx context.Context) *redis.TimeCmd {
	cmd := redis.NewTimeCmd(ctx, "time")
	s.mu.Lock()
	s.timeN++
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
	s.zremN++
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

func (s *memZSet) Eval(ctx context.Context, _ string, keys []string, args ...interface{}) *redis.Cmd {
	cmd := redis.NewCmd(ctx, "eval")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evalN++
	if len(keys) != 1 || keys[0] != s.key {
		cmd.SetVal([]any{})
		return cmd
	}
	nowMilli := s.now.UnixMilli()
	if len(args) > 0 {
		switch x := args[0].(type) {
		case string:
			if x != "" {
				if n, err := strconv.ParseInt(x, 10, 64); err == nil {
					nowMilli = n
				}
			}
		case int64:
			nowMilli = x
		case int:
			nowMilli = int64(x)
		}
	}
	n := 1
	if len(args) > 1 {
		switch x := args[1].(type) {
		case int:
			n = x
		case int64:
			n = int(x)
		case string:
			n, _ = strconv.Atoi(x)
		}
	}
	if n < 1 {
		n = 1
	}
	cmd.SetVal(s.claimDueLocked(nowMilli, n))
	return cmd
}

func (s *memZSet) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return s.Eval(ctx, sha1, keys, args...)
}

func (s *memZSet) EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return s.Eval(ctx, script, keys, args...)
}

func (s *memZSet) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return s.Eval(ctx, sha1, keys, args...)
}

func (s *memZSet) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	cmd := redis.NewBoolSliceCmd(ctx, "script", "exists")
	vals := make([]bool, len(hashes))
	for i := range vals {
		vals[i] = true
	}
	cmd.SetVal(vals)
	return cmd
}

func (s *memZSet) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx, "script", "load")
	cmd.SetVal(redisClaimScript.Hash())
	return cmd
}

func (s *memZSet) claimDueLocked(nowMilli int64, n int) []any {
	type pair struct {
		member string
		score  int64
	}
	var due []pair
	for member, score := range s.items {
		if score <= nowMilli {
			due = append(due, pair{member, score})
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].score == due[j].score {
			return due[i].member < due[j].member
		}
		return due[i].score < due[j].score
	})
	if n > 0 && len(due) > n {
		due = due[:n]
	}
	out := make([]any, 0, len(due)*2)
	for _, p := range due {
		delete(s.items, p.member)
		out = append(out, p.member, p.score)
	}
	return out
}

func testRedis(now time.Time) (*Redis, *memZSet) {
	z := newMemZSet("jobs", now)
	r := newRedis(z, "jobs", WithRedisClock(func() time.Time { return now }))
	return r, z
}

func TestRedisNewPanics(t *testing.T) {
	expectPanicIs(t, ErrNilRedis, func() { newRedis(nil, "jobs") })
}

func TestRedisEmptyKeyPanics(t *testing.T) {
	expectPanicIs(t, ErrEmptyRedisKey, func() {
		newRedis(newMemZSet("jobs", time.Now()), "")
	})
}

func TestRedisScheduleCancelClaim(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	r, z := testRedis(now)
	ctx := context.Background()
	k1 := encodeTaskKey("order", "p1")
	k2 := encodeTaskKey("order", "p2")
	due := Task{Key: k1, Kind: "order", Payload: "p1", At: now.Add(-time.Second)}
	later := Task{Key: k2, Kind: "order", Payload: "p2", At: now.Add(time.Hour)}
	if err := r.Schedule(ctx, due); err != nil {
		t.Fatal(err)
	}
	if err := r.Schedule(ctx, later); err != nil {
		t.Fatal(err)
	}
	if err := r.Schedule(ctx, Task{Key: k1, Kind: "order", Payload: "p1", At: now.Add(-2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	z.mu.Lock()
	if z.items[k1] != now.Add(-2*time.Second).UnixMilli() {
		t.Fatalf("score not overwritten: %d", z.items[k1])
	}
	z.mu.Unlock()

	got, err := r.Claim(ctx, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != k1 || got[0].Kind != "order" || got[0].Payload != "p1" {
		t.Fatalf("got=%+v", got)
	}
	empty, err := r.Claim(ctx, 1)
	if err != nil || len(empty) != 0 {
		t.Fatalf("no due should return empty immediately: %v %v", empty, err)
	}
	if err := r.Cancel(ctx, k2); err != nil {
		t.Fatal(err)
	}
	z.mu.Lock()
	_, ok := z.items[k2]
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
	if err := r1.Schedule(ctx, Task{Key: encodeTaskKey("order", "p1"), Kind: "order", Payload: "p1", At: now.Add(-time.Second)}); err != nil {
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

func TestRedisFail(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	r, z := testRedis(now)
	ctx := context.Background()
	task := Task{Key: encodeTaskKey("order", "p1"), Kind: "order", Payload: "p1", At: now.Add(-time.Second)}
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
	failAt := z.items[encodeTaskKey("order", "p1")]
	z.mu.Unlock()
	if failAt != now.UnixMilli()+failRequeueDelay.Milliseconds() {
		t.Fatalf("fail score=%d", failAt)
	}
	empty, err := r.Claim(ctx, 1)
	if err != nil || len(empty) != 0 {
		t.Fatalf("not due yet: %v %v", empty, err)
	}

	newer := Task{Key: encodeTaskKey("order", "p1"), Kind: "order", Payload: "p1", At: now.Add(time.Hour)}
	if err := r.Schedule(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if err := r.Fail(ctx, got[0]); err != nil {
		t.Fatal(err)
	}
	z.mu.Lock()
	if z.items[encodeTaskKey("order", "p1")] != now.Add(time.Hour).UnixMilli() {
		t.Fatalf("fail must not overwrite: %d", z.items[encodeTaskKey("order", "p1")])
	}
	z.mu.Unlock()

	claimed, err := r.Claim(ctx, 1)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("future task: %v %v", claimed, err)
	}
}

func TestRedisClaimMinN(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	r, _ := testRedis(now)
	ctx := context.Background()
	if err := r.Schedule(ctx, Task{Key: encodeTaskKey("order", "p1"), Kind: "order", Payload: "p1", At: now.Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}
	got, err := r.Claim(ctx, 0)
	if err != nil || len(got) != 1 {
		t.Fatalf("n<1 got=%v err=%v", got, err)
	}
}

func TestRedisClaimRestoresKindContainingColon(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	r, _ := testRedis(now)
	ctx := context.Background()
	kind := "order:timeout"
	payload := `{"id":"1","a":"b"}`
	task := Task{Key: encodeTaskKey(kind, payload), Kind: kind, Payload: payload, At: now.Add(-time.Second)}
	if err := r.Schedule(ctx, task); err != nil {
		t.Fatal(err)
	}
	got, err := r.Claim(ctx, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("claim=%v err=%v", got, err)
	}
	if got[0].Kind != kind || got[0].Payload != payload || got[0].Key != task.Key {
		t.Fatalf("got=%+v", got[0])
	}
}

func TestRedisClaimUsesSingleEval(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	r, z := testRedis(now)
	ctx := context.Background()
	for _, p := range []string{"a", "b", "c"} {
		if err := r.Schedule(ctx, Task{Key: encodeTaskKey("order", p), Kind: "order", Payload: p, At: now.Add(-time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	z.mu.Lock()
	z.evalN, z.zremN, z.timeN = 0, 0, 0
	z.mu.Unlock()
	got, err := r.Claim(ctx, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got=%d", len(got))
	}
	z.mu.Lock()
	evalN, zremN := z.evalN, z.zremN
	z.mu.Unlock()
	if evalN != 1 {
		t.Fatalf("eval=%d want 1", evalN)
	}
	if zremN != 0 {
		t.Fatalf("claim should be one EVAL, zrem=%d", zremN)
	}
}

func TestRedisClaimEvalUsesServerTime(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	z := newMemZSet("jobs", now)
	r := newRedis(z, "jobs")
	ctx := context.Background()
	if err := r.Schedule(ctx, Task{Key: encodeTaskKey("order", "p1"), Kind: "order", Payload: "p1", At: now.Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}
	z.mu.Lock()
	z.evalN, z.timeN, z.zremN = 0, 0, 0
	z.mu.Unlock()
	got, err := r.Claim(ctx, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("got=%v err=%v", got, err)
	}
	z.mu.Lock()
	evalN, timeN := z.evalN, z.timeN
	z.mu.Unlock()
	if evalN != 1 {
		t.Fatalf("eval=%d", evalN)
	}
	if timeN != 0 {
		t.Fatalf("TIME should run inside EVAL, timeN=%d", timeN)
	}
}

func TestRedisConcurrentClaimNoDuplicate(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	r, _ := testRedis(now)
	assertConcurrentClaimNoDuplicate(t, r, 200, 16, now.Add(-time.Second))
}

func TestRedisConcurrentCancelAndClaim(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	r, z := testRedis(now)
	assertConcurrentCancelAndClaim(t, r, 300, now.Add(-time.Second), func() int64 {
		z.mu.Lock()
		defer z.mu.Unlock()
		return int64(len(z.items))
	})
}

func TestRedisLiveConcurrentClaimNoDuplicate(t *testing.T) {
	rdb, store, zkey := liveRedisStore(t)
	assertConcurrentClaimNoDuplicate(t, store, 200, 16, time.Now().Add(-time.Second))
	leftover, err := rdb.ZCard(context.Background(), zkey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if leftover != 0 {
		t.Fatalf("zcard=%d want 0", leftover)
	}
}

func TestRedisLiveConcurrentCancelAndClaim(t *testing.T) {
	rdb, store, zkey := liveRedisStore(t)
	assertConcurrentCancelAndClaim(t, store, 300, time.Now().Add(-time.Second), func() int64 {
		n, err := rdb.ZCard(context.Background(), zkey).Result()
		if err != nil {
			t.Fatal(err)
		}
		return n
	})
}

func liveRedisStore(t *testing.T) (*redis.Client, *Redis, string) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skip(err)
	}
	key := "delaytimer:conc:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	t.Cleanup(func() {
		_ = rdb.Del(context.Background(), key).Err()
		_ = rdb.Close()
	})
	return rdb, NewRedis(rdb, key), key
}

func assertConcurrentClaimNoDuplicate(t *testing.T, store *Redis, n, workers int, at time.Time) {
	t.Helper()
	ctx := context.Background()
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		p := strconv.Itoa(i)
		keys[i] = encodeTaskKey("order", p)
		if err := store.Schedule(ctx, Task{Key: keys[i], Kind: "order", Payload: p, At: at}); err != nil {
			t.Fatal(err)
		}
	}
	counts := drainClaims(t, ctx, store, workers, 8)
	if len(counts) != n {
		t.Fatalf("unique=%d want %d", len(counts), n)
	}
	for _, k := range keys {
		if c := counts[k]; c != 1 {
			t.Fatalf("key %q claimed %d times", k, c)
		}
	}
}

func assertConcurrentCancelAndClaim(t *testing.T, store *Redis, n int, at time.Time, leftover func() int64) {
	t.Helper()
	ctx := context.Background()
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		p := strconv.Itoa(i)
		keys[i] = encodeTaskKey("order", p)
		if err := store.Schedule(ctx, Task{Key: keys[i], Kind: "order", Payload: p, At: at}); err != nil {
			t.Fatal(err)
		}
	}
	cancelKeys := keys[:n/2]
	mustClaim := keys[n/2:]

	var mu sync.Mutex
	counts := make(map[string]int, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			collectClaims(t, ctx, store, 8, counts, &mu)
		}()
	}
	cancelCh := make(chan string, len(cancelKeys))
	for _, k := range cancelKeys {
		cancelCh <- k
	}
	close(cancelCh)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for k := range cancelCh {
				if err := store.Cancel(ctx, k); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	collectClaims(t, ctx, store, 8, counts, &mu)

	if n := leftover(); n != 0 {
		t.Fatalf("leftover=%d want 0", n)
	}
	for _, k := range mustClaim {
		if c := counts[k]; c != 1 {
			t.Fatalf("uncanceled key %q claimed %d times", k, c)
		}
	}
	for _, k := range cancelKeys {
		if c := counts[k]; c > 1 {
			t.Fatalf("canceled key %q claimed %d times", k, c)
		}
	}
	for k, c := range counts {
		if c != 1 {
			t.Fatalf("key %q claimed %d times", k, c)
		}
	}
}

func drainClaims(t *testing.T, ctx context.Context, store *Redis, workers, batch int) map[string]int {
	t.Helper()
	var mu sync.Mutex
	counts := make(map[string]int)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			collectClaims(t, ctx, store, batch, counts, &mu)
		}()
	}
	wg.Wait()
	return counts
}

func collectClaims(t *testing.T, ctx context.Context, store *Redis, batch int, counts map[string]int, mu *sync.Mutex) {
	t.Helper()
	for {
		got, err := store.Claim(ctx, batch)
		if err != nil {
			t.Error(err)
			return
		}
		if len(got) == 0 {
			return
		}
		mu.Lock()
		for _, task := range got {
			counts[task.Key]++
		}
		mu.Unlock()
	}
}
