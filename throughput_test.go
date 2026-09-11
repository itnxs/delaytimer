//go:build throughput

package delaytimer

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type benchParams struct {
	ID string `json:"id"`
}

func (p *benchParams) Event() Event { return "bench" }

type throughputResult struct {
	backend   string
	n          int
	publishDur time.Duration
	publishErr int
	consumeDur time.Duration
	consumed   int64
}

func TestStoreThroughput(t *testing.T) {
	n := 10_000
	if v := os.Getenv("THROUGHPUT_N"); v != "" {
		fmt.Sscanf(v, "%d", &n)
	}
	workers := 32
	fmt.Printf("n=%d workers=%d gomaxprocs=%d\n", n, workers, runtime.GOMAXPROCS(0))

	t.Run("memory", func(t *testing.T) {
		reportThroughput(t, runThroughput(t, n, workers, func() (Store, func()) {
			return NewMemory(), func() {}
		}))
	})

	t.Run("redis", func(t *testing.T) {
		addr := os.Getenv("REDIS_ADDR")
		if addr == "" {
			addr = "127.0.0.1:6379"
		}
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		ctx := context.Background()
		if err := rdb.Ping(ctx).Err(); err != nil {
			t.Skip(err)
		}
		key := fmt.Sprintf("delaytimer:throughput:%d", time.Now().UnixNano())
		t.Cleanup(func() {
			_ = rdb.Del(ctx, key).Err()
			_ = rdb.Close()
		})
		reportThroughput(t, runThroughput(t, n, workers, func() (Store, func()) {
			return NewRedis(rdb, key), func() { _ = rdb.Del(ctx, key).Err() }
		}))
	})

	t.Run("amqp", func(t *testing.T) {
		url := os.Getenv("AMQP_URL")
		if url == "" {
			url = "amqp://michong:michong@loong-dev-rabbitmq.rabbitmq.svc.cluster.local:5672/"
		}
		conn, err := amqp.DialConfig(url, amqp.Config{Dial: amqp.DefaultDial(3 * time.Second)})
		if err != nil {
			t.Skip(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		name := fmt.Sprintf("delaytimer.throughput.%d", time.Now().UnixNano())
		reportThroughput(t, runThroughput(t, n, workers, func() (Store, func()) {
			return NewAMQP(conn, AMQPConfig{Exchange: name, RoutingKey: name, Queue: name}), func() {}
		}))
	})
}

func runThroughput(t *testing.T, n, workers int, newStore func() (Store, func())) throughputResult {
	t.Helper()
	store, cleanup := newStore()
	t.Cleanup(cleanup)

	var handled atomic.Int64
	timer := New(store,
		WithHandlers(Bind(&benchParams{}, func(context.Context, *benchParams) error {
			handled.Add(1)
			return nil
		})),
		WithLogger(discardLogger()),
		WithConcurrency(64),
		WithBatchSize(128),
		WithPollInterval(time.Millisecond),
	)
	t.Cleanup(timer.Close)

	startPub := time.Now()
	pubErr := publishN(timer, "run", n, workers)
	pubDur := time.Since(startPub)
	if pubErr == n {
		t.Fatalf("all %d publishes failed", n)
	}

	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	want := int64(n)
	timeout := 3 * time.Minute
	deadline := time.Now().Add(timeout)
	startCons := time.Now()
	for handled.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("consume timeout: got %d want %d", handled.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
	consDur := time.Since(startCons)

	return throughputResult{
		backend:   t.Name(),
		n:          n,
		publishDur: pubDur,
		publishErr: pubErr,
		consumeDur: consDur,
		consumed:   handled.Load(),
	}
}

func publishN(timer *Timer, prefix string, n, workers int) int {
	if workers < 1 {
		workers = 1
	}
	if workers > n {
		workers = n
	}
	at := time.Now().Add(-time.Second)
	var errN atomic.Int64
	var wg sync.WaitGroup
	ch := make(chan int, n)
	for i := 0; i < n; i++ {
		ch <- i
	}
	close(ch)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range ch {
				p := &benchParams{ID: fmt.Sprintf("%s-%d", prefix, i)}
				if err := timer.SetEvent(at, p); err != nil {
					errN.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	return int(errN.Load())
}

func reportThroughput(t *testing.T, r throughputResult) {
	t.Helper()
	pubRPS := float64(r.n) / r.publishDur.Seconds()
	consRPS := float64(r.consumed) / r.consumeDur.Seconds()
	fmt.Printf("RESULT backend=%s n=%d publish_err=%d publish_ms=%.1f publish_ops=%.0f consume_ms=%.1f consume_n=%d consume_ops=%.0f\n",
		r.backend, r.n, r.publishErr, r.publishDur.Seconds()*1000, pubRPS, r.consumeDur.Seconds()*1000, r.consumed, consRPS)
}

func discardLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	l.SetLevel(logrus.ErrorLevel)
	return l
}
