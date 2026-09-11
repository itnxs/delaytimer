package delaytimer

import "testing"

func TestShardIndexHashStable(t *testing.T) {
	key := JobKey("kind", "payload")
	a := shardIndex(ShardHash, 8, key, 0)
	b := shardIndex(ShardHash, 8, key, 0)
	if a != b || a < 0 || a >= 8 {
		t.Fatalf("a=%d b=%d", a, b)
	}
	if shardIndex(ShardHash, 1, key, 0) != 0 {
		t.Fatal("single shard")
	}
}

func TestShardIndexMilliModulo(t *testing.T) {
	if got := shardIndex(ShardMilli, 8, "k", 1_000_003); got != 3 {
		t.Fatalf("got=%d", got)
	}
	if got := shardIndex(ShardMilli, 8, "k", -1); got != 7 {
		t.Fatalf("neg=%d", got)
	}
	if shardIndex(ShardMilli, 1, "k", 99) != 0 {
		t.Fatal("single shard")
	}
}

func TestShardName(t *testing.T) {
	if got := shardName("answer_timeout", 2); got != "answer_timeout.2" {
		t.Fatalf("got=%s", got)
	}
}
