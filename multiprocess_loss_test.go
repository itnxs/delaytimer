//go:build throughput

package delaytimer

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

func TestStoreMultiProcessLoss(t *testing.T) {
	switch os.Getenv("LOSS_ROLE") {
	case "producer":
		lossProducer(t)
		return
	case "consumer":
		lossConsumer(t)
		return
	}

	n := 10_000
	if v := os.Getenv("LOSS_N"); v != "" {
		fmt.Sscanf(v, "%d", &n)
	}
	producers, consumers := 4, 4
	t.Run("memory", func(t *testing.T) { runMemorySingleProcessLoss(t, n) })
	t.Run("redis", func(t *testing.T) { runMultiProcessLoss(t, "redis", n, producers, consumers) })
	t.Run("amqp", func(t *testing.T) { runMultiProcessLoss(t, "amqp", n, producers, consumers) })
}

func runMemorySingleProcessLoss(t *testing.T, n int) {
	t.Helper()
	var (
		mu      sync.Mutex
		cons    = make(map[string]int, n)
		hits    atomic.Int64
		pubErr  atomic.Int64
	)
	timer := New(NewMemory(),
		WithHandlers(Bind(&benchParams{}, func(_ context.Context, p *benchParams) error {
			hits.Add(1)
			mu.Lock()
			cons[p.ID]++
			mu.Unlock()
			return nil
		})),
		WithLogger(discardLogger()),
		WithConcurrency(32),
		WithBatchSize(64),
		WithPollInterval(time.Millisecond),
	)
	t.Cleanup(timer.Close)
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	at := time.Now().Add(-time.Second)
	var wg sync.WaitGroup
	ch := make(chan int, n)
	for i := 0; i < n; i++ {
		ch <- i
	}
	close(ch)
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range ch {
				if err := timer.SetEvent(at, &benchParams{ID: strconv.Itoa(i)}); err != nil {
					pubErr.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if pubErr.Load() != 0 {
		t.Fatalf("publish errors %d", pubErr.Load())
	}

	deadline := time.Now().Add(2 * time.Minute)
	for {
		mu.Lock()
		got := len(cons)
		mu.Unlock()
		if got >= n {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	var missing, extra, dup int
	for i := 0; i < n; i++ {
		id := strconv.Itoa(i)
		c := cons[id]
		if c == 0 {
			missing++
		}
		if c > 1 {
			dup += c - 1
		}
	}
	for id := range cons {
		if _, err := strconv.Atoi(id); err != nil {
			extra++
			continue
		}
		k, _ := strconv.Atoi(id)
		if k < 0 || k >= n {
			extra++
		}
	}
	left := 0
	if missing > 0 {
		left = missing
	}
	fmt.Printf("LOSS backend=memory processes=prod:1/cons:1 want=%d published=%d consumed_unique=%d handle_hits=%d missing=%d extra=%d dup=%d leftover=%d\n",
		n, n-int(pubErr.Load()), len(cons), hits.Load(), missing, extra, dup, left)
	if missing > 0 || extra > 0 || dup > 0 {
		t.Fatalf("data loss: missing=%d extra=%d dup=%d", missing, extra, dup)
	}
}

func runMultiProcessLoss(t *testing.T, backend string, n, producers, consumers int) {
	t.Helper()
	rdb, addr := lossRedis(t)
	ctx := context.Background()
	run := fmt.Sprintf("%d", time.Now().UnixNano())
	keys := lossKeys(run)
	t.Cleanup(func() {
		_ = rdb.Del(ctx, keys.zset, keys.pub, keys.cons, keys.hits).Err()
		_ = rdb.Close()
	})

	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@127.0.0.1:5672/"
	}
	if backend == "amqp" {
		conn, err := amqp.DialConfig(amqpURL, amqp.Config{Dial: amqp.DefaultDial(3 * time.Second)})
		if err != nil {
			t.Skip(err)
		}
		_ = conn.Close()
	}

	envBase := append(os.Environ(),
		"LOSS_BACKEND="+backend,
		"LOSS_RUN="+run,
		"LOSS_ZSET="+keys.zset,
		"LOSS_PUB="+keys.pub,
		"LOSS_CONS="+keys.cons,
		"LOSS_HITS="+keys.hits,
		"LOSS_AMQP="+keys.amqp,
		"REDIS_ADDR="+addr,
		"AMQP_URL="+amqpURL,
	)

	cons := make([]*exec.Cmd, consumers)
	for i := 0; i < consumers; i++ {
		cmd := lossCmd(envBase, "consumer")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		cons[i] = cmd
	}
	t.Cleanup(func() { lossStop(cons) })
	time.Sleep(400 * time.Millisecond)

	chunk := n / producers
	pubs := make([]*exec.Cmd, producers)
	for i := 0; i < producers; i++ {
		start := i * chunk
		count := chunk
		if i == producers-1 {
			count = n - start
		}
		cmd := lossCmd(append(append([]string{}, envBase...),
			"LOSS_START="+strconv.Itoa(start),
			"LOSS_COUNT="+strconv.Itoa(count),
		), "producer")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		pubs[i] = cmd
	}
	for _, cmd := range pubs {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("producer: %v", err)
		}
	}

	deadline := time.Now().Add(2 * time.Minute)
	for {
		pubN, _ := rdb.SCard(ctx, keys.pub).Result()
		consN, _ := rdb.SCard(ctx, keys.cons).Result()
		if pubN >= int64(n) && consN >= pubN {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	lossStop(cons)

	pubN, _ := rdb.SCard(ctx, keys.pub).Result()
	consN, _ := rdb.SCard(ctx, keys.cons).Result()
	hits, _ := rdb.Get(ctx, keys.hits).Int64()
	missing, _ := rdb.SDiff(ctx, keys.pub, keys.cons).Result()
	extra, _ := rdb.SDiff(ctx, keys.cons, keys.pub).Result()
	dup := hits - consN
	if dup < 0 {
		dup = 0
	}
	left := lossLeftover(t, backend, rdb, keys, amqpURL)

	fmt.Printf("LOSS backend=%s processes=prod:%d/cons:%d want=%d published=%d consumed_unique=%d handle_hits=%d missing=%d extra=%d dup=%d leftover=%d\n",
		backend, producers, consumers, n, pubN, consN, hits, len(missing), len(extra), dup, left)
	if len(missing) > 0 && len(missing) <= 10 {
		fmt.Printf("LOSS missing ids=%v\n", missing)
	} else if len(missing) > 10 {
		fmt.Printf("LOSS missing ids(sample)=%v\n", missing[:10])
	}
	if pubN != int64(n) {
		t.Fatalf("published %d want %d", pubN, n)
	}
	if len(missing) > 0 || left > 0 {
		t.Fatalf("data loss: missing=%d leftover=%d", len(missing), left)
	}
	if len(extra) > 0 || dup > 0 {
		t.Fatalf("unexpected extra=%d dup=%d", len(extra), dup)
	}
}

type lossKeySet struct {
	zset, pub, cons, hits, amqp string
}

func lossKeys(run string) lossKeySet {
	p := "delaytimer:loss:" + run
	return lossKeySet{
		zset: p + ":jobs",
		pub:  p + ":pub",
		cons: p + ":cons",
		hits: p + ":hits",
		amqp: "delaytimer.loss." + run,
	}
}

func lossRedis(t *testing.T) (*redis.Client, string) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		_ = rdb.Close()
		t.Skip(err)
	}
	return rdb, addr
}

func lossCmd(env []string, role string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run=^TestStoreMultiProcessLoss$", "-test.count=1")
	cmd.Env = append(env, "LOSS_ROLE="+role)
	return cmd
}

func lossStop(cmds []*exec.Cmd) {
	for _, cmd := range cmds {
		if cmd == nil || cmd.Process == nil {
			continue
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
	}
	deadline := time.Now().Add(5 * time.Second)
	for _, cmd := range cmds {
		if cmd == nil || cmd.Process == nil {
			continue
		}
		done := make(chan struct{})
		go func(c *exec.Cmd) {
			_ = c.Wait()
			close(done)
		}(cmd)
		select {
		case <-done:
		case <-time.After(time.Until(deadline)):
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}
}

func lossProducer(t *testing.T) {
	t.Helper()
	start, _ := strconv.Atoi(os.Getenv("LOSS_START"))
	count, _ := strconv.Atoi(os.Getenv("LOSS_COUNT"))
	rdb, timer := lossTimer(t, false)
	defer rdb.Close()
	defer timer.Close()
	ctx := context.Background()
	at := time.Now().Add(-time.Second)
	pubKey := os.Getenv("LOSS_PUB")
	var errN int
	for i := 0; i < count; i++ {
		id := strconv.Itoa(start + i)
		if err := timer.SetEvent(at, &benchParams{ID: id}); err != nil {
			errN++
			continue
		}
		if err := rdb.SAdd(ctx, pubKey, id).Err(); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Printf("producer backend=%s start=%d count=%d err=%d\n", os.Getenv("LOSS_BACKEND"), start, count, errN)
	if errN > 0 {
		t.Fatalf("publish errors %d", errN)
	}
}

func lossConsumer(t *testing.T) {
	t.Helper()
	rdb, timer := lossTimer(t, true)
	defer rdb.Close()
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("consumer backend=%s pid=%d started\n", os.Getenv("LOSS_BACKEND"), os.Getpid())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, os.Interrupt)
	<-ch
	timer.Close()
}

func lossTimer(t *testing.T, consume bool) (*redis.Client, *Timer) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	backend := os.Getenv("LOSS_BACKEND")
	var store Store
	switch backend {
	case "redis":
		store = NewRedis(rdb, os.Getenv("LOSS_ZSET"))
	case "amqp":
		url := os.Getenv("AMQP_URL")
		conn, err := amqp.Dial(url)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		name := os.Getenv("LOSS_AMQP")
		store = NewAMQP(conn, AMQPConfig{Exchange: name, RoutingKey: name, Queue: name}, WithAMQPPublishChannels(8))
	default:
		t.Fatalf("backend=%s", backend)
	}
	opts := []Option{
		WithLogger(discardLogger()),
		WithConcurrency(32),
		WithBatchSize(64),
		WithPollInterval(time.Millisecond),
	}
	if consume {
		cons, hits := os.Getenv("LOSS_CONS"), os.Getenv("LOSS_HITS")
		opts = append(opts, WithHandlers(Bind(&benchParams{}, func(_ context.Context, p *benchParams) error {
			pipe := rdb.Pipeline()
			pipe.SAdd(context.Background(), cons, p.ID)
			pipe.Incr(context.Background(), hits)
			_, err := pipe.Exec(context.Background())
			return err
		})))
	}
	return rdb, New(store, opts...)
}

func lossLeftover(t *testing.T, backend string, rdb *redis.Client, keys lossKeySet, amqpURL string) int {
	t.Helper()
	ctx := context.Background()
	switch backend {
	case "redis":
		n, err := rdb.ZCard(ctx, keys.zset).Result()
		if err != nil {
			t.Fatal(err)
		}
		return int(n)
	case "amqp":
		conn, err := amqp.Dial(amqpURL)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		defer ch.Close()
		q, err := ch.QueueInspect(keys.amqp)
		if err != nil {
			return 0
		}
		return q.Messages
	}
	return 0
}
