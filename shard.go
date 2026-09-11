package delaytimer

import (
	"hash/fnv"
	"math/rand"
	"strconv"
)

// ShardMode 分片算法
type ShardMode int

const (
	// ShardHash 按 JobKey hash 取模，同 Key 固定落同一片（默认）
	ShardHash ShardMode = iota
	// ShardRandom 写入时随机选片；Cancel / 再次 Schedule 需扫该 Event 下全部片
	ShardRandom
	// ShardMilli 按到期 Unix 毫秒取模；Cancel / 再次 Schedule 需扫该 Event 下全部片
	ShardMilli
)

func normalizeShards(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

func (m ShardMode) scanAll() bool {
	return m == ShardRandom || m == ShardMilli
}

func shardIndex(mode ShardMode, shards int, key string, milli int64) int {
	n := normalizeShards(shards)
	if n == 1 {
		return 0
	}
	switch mode {
	case ShardRandom:
		return rand.Intn(n)
	case ShardMilli:
		mod := milli % int64(n)
		if mod < 0 {
			mod += int64(n)
		}
		return int(mod)
	default:
		h := fnv.New32a()
		_, _ = h.Write([]byte(key))
		return int(h.Sum32() % uint32(n))
	}
}

func shardName(kind string, shard int) string {
	return kind + "." + strconv.Itoa(shard)
}
