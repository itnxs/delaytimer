package delaytimer

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestNewNilStore(t *testing.T) {
	expectPanicIs(t, ErrNilTaskStore, func() { New(nil) })
}

func TestBindHandlersPanics(t *testing.T) {
	store := &fakeStore{}
	t.Run("nilHandler", func(t *testing.T) {
		expectPanicIs(t, ErrNilEventHandler, func() {
			New(store, WithHandlers(nil), WithLogger(silentLogger()))
		})
	})
	t.Run("emptyEvent", func(t *testing.T) {
		expectPanicIs(t, ErrEmptyEvent, func() {
			New(store, WithHandlers(emptyEventHandler{}), WithLogger(silentLogger()))
		})
	})
	t.Run("duplicate", func(t *testing.T) {
		h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error { return nil })
		expectPanicIs(t, ErrDuplicateEvent, func() {
			New(store, WithHandlers(h, h), WithLogger(silentLogger()))
		})
	})
}

type emptyEventHandler struct{}

func (emptyEventHandler) Event() Event                         { return "" }
func (emptyEventHandler) NewParams() Params                    { return &sampleParams{} }
func (emptyEventHandler) Handle(context.Context, Params) error { return nil }

func TestTimerRunAcksOnSuccess(t *testing.T) {
	task := sampleTaskAt(time.Unix(1, 0))
	store := &fakeStore{claims: []Task{task}}
	handled := make(chan struct{}, 1)
	h := Bind(&sampleParams{}, func(_ context.Context, p *sampleParams) error {
		if p.ID != "1" {
			t.Errorf("id=%s", p.ID)
		}
		handled <- struct{}{}
		return nil
	})
	timer := New(store, WithHandlers(h), WithLogger(silentLogger()), WithPollInterval(time.Millisecond), WithConcurrency(1))
	t.Cleanup(timer.Close)
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatal("handler not called")
	}
	waitUntil(t, time.Second, func() bool {
		_, _, ackN, failN := store.snapshot()
		return ackN == 1 && failN == 0
	})
}

func TestTimerRunHandleErrorDoesNotFail(t *testing.T) {
	task := sampleTaskAt(time.Unix(1, 0))
	store := &fakeStore{claims: []Task{task}}
	handled := make(chan struct{}, 1)
	h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error {
		handled <- struct{}{}
		return errors.New("boom")
	})
	timer := New(store, WithHandlers(h), WithLogger(silentLogger()), WithPollInterval(time.Millisecond))
	t.Cleanup(timer.Close)
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatal("handler not called")
	}
	time.Sleep(50 * time.Millisecond)
	_, _, ackN, failN := store.snapshot()
	if ackN != 1 || failN != 0 {
		t.Fatalf("default FailDiscard: ack=%d fail=%d", ackN, failN)
	}
}

func TestTimerRunHandleErrorRequeue(t *testing.T) {
	task := sampleTaskAt(time.Unix(1, 0))
	store := &fakeStore{claims: []Task{task}}
	handled := make(chan struct{}, 1)
	h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error {
		handled <- struct{}{}
		return errors.New("boom")
	})
	timer := New(store, WithHandlers(h), WithFailPolicy(FailRequeue), WithLogger(silentLogger()), WithPollInterval(time.Millisecond))
	t.Cleanup(timer.Close)
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatal("handler not called")
	}
	waitUntil(t, time.Second, func() bool {
		_, _, _, failN := store.snapshot()
		return failN == 1
	})
	_, _, ackN, failN := store.snapshot()
	if ackN != 1 || failN != 1 {
		t.Fatalf("handle error should ack then Fail: ack=%d fail=%d", ackN, failN)
	}
}

func TestTimerFailRequeueSkipsUnknownKindAndBadPayload(t *testing.T) {
	h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error {
		t.Fatal("handler should not run")
		return nil
	})
	store := &fakeStore{claims: []Task{
		{Key: "x", Kind: "missing", Payload: "{}", At: time.Unix(1, 0)},
		{Key: encodeTaskKey("sample", "x"), Kind: "sample", Payload: "not-json", At: time.Unix(2, 0)},
	}}
	timer := New(store, WithHandlers(h), WithFailPolicy(FailRequeue), WithLogger(silentLogger()), WithPollInterval(time.Millisecond))
	t.Cleanup(timer.Close)
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool {
		_, _, ackN, _ := store.snapshot()
		return ackN == 2
	})
	time.Sleep(50 * time.Millisecond)
	_, _, _, failN := store.snapshot()
	if failN != 0 {
		t.Fatalf("unknown kind / decode fail must not Fail: fail=%d", failN)
	}
}

func TestTimerRunUnknownKind(t *testing.T) {
	store := &fakeStore{claims: []Task{{Key: "x", Kind: "missing", Payload: "{}", At: time.Unix(1, 0)}}}
	timer := New(store, WithLogger(silentLogger()), WithPollInterval(time.Millisecond))
	t.Cleanup(timer.Close)
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return len(store.claims) == 0
	})
	_, _, ackN, failN := store.snapshot()
	if ackN != 1 || failN != 0 {
		t.Fatalf("unknown kind should ack and discard: ack=%d fail=%d", ackN, failN)
	}
}

func TestTimerRunBadPayloadDiscarded(t *testing.T) {
	handled := make(chan struct{}, 1)
	h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error {
		handled <- struct{}{}
		return nil
	})
	store := &fakeStore{claims: []Task{{Key: encodeTaskKey("sample", "x"), Kind: "sample", Payload: "not-json", At: time.Unix(1, 0)}}}
	timer := New(store, WithHandlers(h), WithLogger(silentLogger()), WithPollInterval(time.Millisecond))
	t.Cleanup(timer.Close)
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool {
		_, _, ackN, _ := store.snapshot()
		return ackN == 1
	})
	select {
	case <-handled:
		t.Fatal("handler should not run on bad payload")
	case <-time.After(50 * time.Millisecond):
	}
	_, _, _, failN := store.snapshot()
	if failN != 0 {
		t.Fatalf("fail=%d", failN)
	}
}

func TestTimerAMQPAckOnUnknownKind(t *testing.T) {
	ack := &fakeAcknowledger{}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{
		Acknowledger: ack,
		Body:         []byte(`{"key":"x","kind":"missing","payload":"{}","at":1}`),
	}
	store := amqpStore(&fakeAMQPChannel{deliveries: deliveries}, testAMQPConfig())
	timer := New(store, WithLogger(silentLogger()), WithPollInterval(time.Millisecond))
	t.Cleanup(timer.Close)
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 2*time.Second, func() bool {
		ack.mu.Lock()
		defer ack.mu.Unlock()
		return ack.acked
	})
	ack.mu.Lock()
	defer ack.mu.Unlock()
	if ack.nacked {
		t.Fatal("unknown kind should ack, not nack")
	}
}

func TestTimerSetEventAndDelEvent(t *testing.T) {
	handled := make(chan *sampleParams, 4)
	h := Bind(&sampleParams{}, func(_ context.Context, p *sampleParams) error {
		handled <- p
		return nil
	})
	timer := New(NewMemory(), WithHandlers(h), WithLogger(silentLogger()), WithPollInterval(10*time.Millisecond))
	t.Cleanup(timer.Close)
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := timer.SetEvent(time.Now().Add(-time.Second), &sampleParams{ID: "ok"}); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-handled:
		if p.ID != "ok" {
			t.Fatalf("id=%s", p.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("due event was not handled")
	}

	skip := &sampleParams{ID: "skip"}
	if err := timer.SetEvent(time.Now().Add(time.Hour), skip); err != nil {
		t.Fatal(err)
	}
	if err := timer.DelEvent(skip); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-handled:
		t.Fatalf("canceled event should not run, got=%s", p.ID)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestTimerSetEventNilParams(t *testing.T) {
	timer := New(&fakeStore{}, WithLogger(silentLogger()))
	t.Cleanup(timer.Close)
	if err := timer.SetEvent(time.Now(), nil); !errors.Is(err, ErrNilParam) {
		t.Fatalf("set=%v", err)
	}
	if err := timer.DelEvent(nil); !errors.Is(err, ErrNilParam) {
		t.Fatalf("del=%v", err)
	}
}

func TestTimerSetEventWritesStoreImmediately(t *testing.T) {
	store := &fakeStore{}
	timer := New(store, WithLogger(silentLogger()))
	t.Cleanup(timer.Close)
	at := time.Unix(1000, 0)
	p := &sampleParams{ID: "1"}
	if err := timer.SetEvent(at, p); err != nil {
		t.Fatal(err)
	}
	scheduled, _, _, _ := store.snapshot()
	if len(scheduled) != 1 {
		t.Fatalf("expected sync schedule, got %d", len(scheduled))
	}
	payload, err := encodeParams(p)
	if err != nil {
		t.Fatal(err)
	}
	wantKey := encodeTaskKey("sample", payload)
	if scheduled[0].Key != wantKey || scheduled[0].At != at {
		t.Fatalf("task=%+v", scheduled[0])
	}
	if err := timer.DelEvent(p); err != nil {
		t.Fatal(err)
	}
	_, canceled, _, _ := store.snapshot()
	if len(canceled) != 1 || canceled[0] != wantKey {
		t.Fatalf("canceled=%v", canceled)
	}
}

func TestTimerCloseStopsStart(t *testing.T) {
	timer := New(NewMemory(), WithLogger(silentLogger()), WithPollInterval(time.Millisecond))
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		timer.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close should stop Start and return")
	}
}

func TestTimerStartCloseWaits(t *testing.T) {
	timer := New(NewMemory(), WithLogger(silentLogger()), WithPollInterval(time.Millisecond))
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := timer.Start(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second start=%v", err)
	}
	done := make(chan struct{})
	go func() {
		timer.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close should cancel and wait")
	}
	if err := timer.Start(context.Background()); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("start after close=%v", err)
	}
}

func TestTimerStartHandlesEvent(t *testing.T) {
	handled := make(chan *sampleParams, 1)
	h := Bind(&sampleParams{}, func(_ context.Context, p *sampleParams) error {
		handled <- p
		return nil
	})
	timer := New(NewMemory(), WithHandlers(h), WithLogger(silentLogger()), WithPollInterval(10*time.Millisecond))
	defer timer.Close()
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := timer.SetEvent(time.Now().Add(-time.Second), &sampleParams{ID: "ok"}); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-handled:
		if p.ID != "ok" {
			t.Fatalf("id=%s", p.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("due event was not handled")
	}
}

func TestTimerSetEventAfterClose(t *testing.T) {
	timer := New(&fakeStore{}, WithLogger(silentLogger()))
	timer.Close()
	if err := timer.SetEvent(time.Now(), &sampleParams{ID: "1"}); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("set=%v", err)
	}
	if err := timer.DelEvent(&sampleParams{ID: "1"}); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("del=%v", err)
	}
}

func TestTimerCloseClosesAMQPChannels(t *testing.T) {
	var chans []*fakeAMQPChannel
	conn := &fakeAMQPConn{open: func() (AMQPChannel, error) {
		ch := &fakeAMQPChannel{deliveries: make(chan amqp.Delivery)}
		chans = append(chans, ch)
		return ch, nil
	}}
	store := newAMQP(conn, testAMQPConfig())
	timer := New(store, WithLogger(silentLogger()), WithPollInterval(time.Millisecond))
	if err := timer.SetEvent(time.Now().Add(time.Hour), &sampleParams{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool { return conn.opens() >= 2 })
	timer.Close()
	if len(chans) < 2 {
		t.Fatalf("channels=%d", len(chans))
	}
	if chans[0].closes() != 1 {
		t.Fatalf("pub closeN=%d", chans[0].closes())
	}
	if chans[1].closes() != 1 {
		t.Fatalf("sub closeN=%d", chans[1].closes())
	}
}

func TestDispatchAckFailed(t *testing.T) {
	store := &fakeStore{ackErr: errors.New("ack boom")}
	timer := New(store, WithLogger(silentLogger()))
	t.Cleanup(timer.Close)
	err := timer.dispatch(context.Background(), sampleTaskAt(time.Unix(1, 0)))
	if !errors.Is(err, ErrAckFailed) {
		t.Fatalf("want ErrAckFailed, got %v", err)
	}
}

func TestDispatchPanicReturnsError(t *testing.T) {
	h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error {
		panic("boom")
	})
	timer := New(&fakeStore{}, WithHandlers(h), WithLogger(silentLogger()))
	t.Cleanup(timer.Close)
	err := timer.dispatch(context.Background(), sampleTaskAt(time.Unix(1, 0)))
	if err == nil {
		t.Fatal("panic should return error")
	}
}

func TestTimerRunConcurrentDispatch(t *testing.T) {
	now := time.Unix(1, 0)
	claims := make([]Task, 8)
	for i := range claims {
		p := &sampleParams{ID: string(rune('a' + i))}
		payload, err := encodeParams(p)
		if err != nil {
			t.Fatal(err)
		}
		claims[i] = Task{Key: encodeTaskKey("sample", payload), Kind: "sample", Payload: payload, At: now}
	}
	store := &fakeStore{claims: claims}
	h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error { return nil })
	timer := New(store, WithHandlers(h), WithLogger(silentLogger()), WithPollInterval(time.Millisecond), WithConcurrency(8), WithBatchSize(8))
	t.Cleanup(timer.Close)
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 2*time.Second, func() bool {
		_, _, ackN, _ := store.snapshot()
		return ackN == 8
	})
}

func TestTimerClaimsWhileHandlersRun(t *testing.T) {
	block := make(chan struct{})
	h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error {
		<-block
		return nil
	})
	store := &fakeStore{claims: []Task{sampleTaskAt(time.Unix(1, 0)), sampleTaskAt(time.Unix(2, 0))}}
	timer := New(store, WithHandlers(h), WithLogger(silentLogger()), WithPollInterval(time.Millisecond), WithConcurrency(2), WithBatchSize(1))
	t.Cleanup(func() {
		close(block)
		timer.Close()
	})
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 2*time.Second, func() bool { return store.claimCount() >= 2 })
}

func TestTimerCloseWaitsForAsyncDispatch(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error {
		close(started)
		time.Sleep(40 * time.Millisecond)
		close(finished)
		return nil
	})
	store := &fakeStore{claims: []Task{sampleTaskAt(time.Unix(1, 0))}}
	timer := New(store, WithHandlers(h), WithLogger(silentLogger()), WithPollInterval(time.Millisecond), WithConcurrency(1), WithBatchSize(1))
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-started
	timer.Close()
	select {
	case <-finished:
	default:
		t.Fatal("Close returned before handler finished")
	}
}

// claimAfterCancelStore 模拟 Redis：EVAL 在 ctx 取消后仍返回已 ZREM 的任务。
type claimAfterCancelStore struct {
	fakeStore
	entered chan struct{}
}

func (s *claimAfterCancelStore) Claim(ctx context.Context, n int) ([]Task, error) {
	s.mu.Lock()
	s.claimCalls++
	s.mu.Unlock()
	select {
	case <-s.entered:
	default:
		close(s.entered)
	}
	<-ctx.Done()
	return s.fakeStore.Claim(context.Background(), n)
}

func TestTimerCloseDrainsClaimedTasks(t *testing.T) {
	store := &claimAfterCancelStore{
		fakeStore: fakeStore{claims: []Task{sampleTaskAt(time.Unix(1, 0))}},
		entered:   make(chan struct{}),
	}
	handled := make(chan struct{}, 1)
	h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error {
		handled <- struct{}{}
		return nil
	})
	timer := New(store, WithHandlers(h), WithLogger(silentLogger()), WithPollInterval(time.Millisecond), WithConcurrency(1), WithBatchSize(1))
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-store.entered
	timer.Close()
	select {
	case <-handled:
	default:
		t.Fatal("Close dropped a task that Claim already returned")
	}
}

func TestTimerCloseFailRequeueIgnoresCancel(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	store := &failIfCanceledStore{fakeStore: fakeStore{claims: []Task{sampleTaskAt(time.Unix(1, 0))}}}
	h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error {
		close(started)
		<-release
		return errors.New("boom")
	})
	timer := New(store, WithHandlers(h), WithFailPolicy(FailRequeue), WithLogger(silentLogger()), WithPollInterval(time.Millisecond), WithConcurrency(1), WithBatchSize(1))
	if err := timer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-started
	done := make(chan struct{})
	go func() {
		timer.Close()
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	close(release)
	<-done
	_, _, _, failN := store.snapshot()
	if store.canceledFail > 0 {
		t.Fatalf("Fail saw canceled ctx %d times", store.canceledFail)
	}
	if failN != 1 {
		t.Fatalf("fail=%d want 1", failN)
	}
}

type failIfCanceledStore struct {
	fakeStore
	canceledFail int
}

func (s *failIfCanceledStore) Fail(ctx context.Context, task Task) error {
	if ctx.Err() != nil {
		s.mu.Lock()
		s.canceledFail++
		s.mu.Unlock()
		return ctx.Err()
	}
	return s.fakeStore.Fail(ctx, task)
}

func TestDispatchHandleTimeoutDiscard(t *testing.T) {
	store := &fakeStore{}
	timedOut := make(chan struct{}, 1)
	h := Bind(&sampleParams{}, func(ctx context.Context, _ *sampleParams) error {
		<-ctx.Done()
		timedOut <- struct{}{}
		return ctx.Err()
	})
	timer := New(store, WithHandlers(h), WithHandleTimeout(20*time.Millisecond), WithLogger(silentLogger()))
	t.Cleanup(timer.Close)
	err := timer.dispatch(context.Background(), sampleTaskAt(time.Unix(1, 0)))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	select {
	case <-timedOut:
	default:
		t.Fatal("handler should see timeout")
	}
	_, _, _, failN := store.snapshot()
	if failN != 0 {
		t.Fatalf("FailDiscard failN=%d", failN)
	}
}

func TestDispatchHandleTimeoutRequeue(t *testing.T) {
	store := &failIfCanceledStore{}
	h := Bind(&sampleParams{}, func(ctx context.Context, _ *sampleParams) error {
		<-ctx.Done()
		return ctx.Err()
	})
	timer := New(store, WithHandlers(h), WithHandleTimeout(20*time.Millisecond), WithFailPolicy(FailRequeue), WithLogger(silentLogger()))
	t.Cleanup(timer.Close)
	err := timer.dispatch(context.Background(), sampleTaskAt(time.Unix(1, 0)))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	if store.canceledFail > 0 {
		t.Fatal("Fail must not use the timed-out handle ctx")
	}
	_, _, _, failN := store.snapshot()
	if failN != 1 {
		t.Fatalf("fail=%d want 1", failN)
	}
}

func TestDispatchHandleTimeoutNotFired(t *testing.T) {
	store := &fakeStore{}
	h := Bind(&sampleParams{}, func(context.Context, *sampleParams) error { return nil })
	timer := New(store, WithHandlers(h), WithHandleTimeout(time.Second), WithLogger(silentLogger()))
	t.Cleanup(timer.Close)
	if err := timer.dispatch(context.Background(), sampleTaskAt(time.Unix(1, 0))); err != nil {
		t.Fatal(err)
	}
	_, _, _, failN := store.snapshot()
	if failN != 0 {
		t.Fatalf("fail=%d", failN)
	}
}
