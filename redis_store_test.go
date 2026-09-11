package delaytimer

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"
)

// memRedis 用内存结构模拟 Redis ZSET / SET，供单测
type memRedis struct {
	mu    sync.Mutex
	zsets map[string]map[string]int64
	sets  map[string]map[string]struct{}
}

func newMemRedis() *memRedis {
	return &memRedis{
		zsets: make(map[string]map[string]int64),
		sets:  make(map[string]map[string]struct{}),
	}
}

var _ RedisCommands = (*memRedis)(nil)

func (s *memRedis) zset(key string) map[string]int64 {
	m := s.zsets[key]
	if m == nil {
		m = make(map[string]int64)
		s.zsets[key] = m
	}
	return m
}

func (s *memRedis) TimeMilli(context.Context) (int64, error) {
	return time.Now().UnixMilli(), nil
}

func (s *memRedis) ZAdd(_ context.Context, key string, score float64, member string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.zset(key)[member] = int64(score)
	return nil
}

func (s *memRedis) ZAddNX(_ context.Context, key string, score float64, member string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	z := s.zset(key)
	if _, ok := z[member]; ok {
		return 0, nil
	}
	z[member] = int64(score)
	return 1, nil
}

func (s *memRedis) ZRem(_ context.Context, key, member string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.zset(key)[member]; !ok {
		return 0, nil
	}
	delete(s.zset(key), member)
	return 1, nil
}

func (s *memRedis) ZScore(_ context.Context, key, member string) (float64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	z, ok := s.zsets[key]
	if !ok {
		return 0, false, nil
	}
	sc, ok := z[member]
	if !ok {
		return 0, false, nil
	}
	return float64(sc), true, nil
}

func (s *memRedis) SAdd(_ context.Context, key string, members ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := s.sets[key]
	if set == nil {
		set = make(map[string]struct{})
		s.sets[key] = set
	}
	for _, m := range members {
		set[m] = struct{}{}
	}
	return nil
}

func (s *memRedis) SMembers(_ context.Context, key string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := s.sets[key]
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	sort.Strings(out)
	return out, nil
}

func (s *memRedis) ZRangeByScore(_ context.Context, key, _, max string, count int64) ([]RedisMember, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	maxScore := int64(^uint64(0) >> 1)
	if max != "+inf" {
		maxScore = asInt64(max)
	}
	return s.rangeByScoreLocked(key, maxScore, int(count)), nil
}

func (s *memRedis) rangeByScoreLocked(key string, max int64, n int) []RedisMember {
	type kv struct {
		member string
		score  int64
	}
	var items []kv
	for member, score := range s.zset(key) {
		if score <= max {
			items = append(items, kv{member, score})
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].score == items[j].score {
			return items[i].member < items[j].member
		}
		return items[i].score < items[j].score
	})
	if n >= 0 && len(items) > n {
		items = items[:n]
	}
	out := make([]RedisMember, len(items))
	for i, item := range items {
		out[i] = RedisMember{Member: item.member, Score: float64(item.score)}
	}
	return out
}

func asInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	default:
		return 0
	}
}
