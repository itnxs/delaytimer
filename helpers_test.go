package delaytimer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

)

type sampleParams struct {
	ID string `json:"id"`
}

func (p *sampleParams) Event() Event { return "sample" }

type orderedParams struct {
	A string `json:"a"`
	B string `json:"b"`
}

func (p *orderedParams) Event() Event { return "ordered" }

type emptyEventParams struct{}

func (p *emptyEventParams) Event() Event { return "" }

type fakeStore struct {
	mu           sync.Mutex
	scheduled    []Task
	canceled     []string
	scheduleCtxs []context.Context
	cancelCtxs   []context.Context
	claims       []Task
	ackN         int
	failN        int
	claimErr     error
	ackErr       error
	claimCalls   int
}

func (s *fakeStore) Schedule(ctx context.Context, task Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduled = append(s.scheduled, task)
	s.scheduleCtxs = append(s.scheduleCtxs, ctx)
	return nil
}

func (s *fakeStore) Cancel(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.canceled = append(s.canceled, key)
	s.cancelCtxs = append(s.cancelCtxs, ctx)
	return nil
}

func (s *fakeStore) Claim(_ context.Context, n int) ([]Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimCalls++
	if s.claimErr != nil {
		return nil, s.claimErr
	}
	if n < 1 {
		n = 1
	}
	if len(s.claims) == 0 {
		return nil, nil
	}
	if n > len(s.claims) {
		n = len(s.claims)
	}
	out := append([]Task(nil), s.claims[:n]...)
	s.claims = s.claims[n:]
	return out, nil
}

func (s *fakeStore) Ack(context.Context, Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ackN++
	return s.ackErr
}

func (s *fakeStore) Fail(context.Context, Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failN++
	return nil
}

func (s *fakeStore) Close() error { return nil }

func (s *fakeStore) snapshot() (scheduled []Task, canceled []string, ackN, failN int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Task(nil), s.scheduled...), append([]string(nil), s.canceled...), s.ackN, s.failN
}

func (s *fakeStore) claimCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimCalls
}

func (s *fakeStore) lastScheduleCtx() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.scheduleCtxs) == 0 {
		return nil
	}
	return s.scheduleCtxs[len(s.scheduleCtxs)-1]
}

func (s *fakeStore) lastCancelCtx() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cancelCtxs) == 0 {
		return nil
	}
	return s.cancelCtxs[len(s.cancelCtxs)-1]
}

type noopLogger struct{}

func (noopLogger) Info(context.Context, string, ...any)         {}
func (noopLogger) Error(context.Context, string, error, ...any) {}

func silentLogger() Logger { return noopLogger{} }

func sampleTaskAt(at time.Time) Task {
	payload, err := encodeParams(&sampleParams{ID: "1"})
	if err != nil {
		panic(err)
	}
	return Task{
		Key:     encodeTaskKey("sample", payload),
		Kind:    "sample",
		Payload: payload,
		At:      at,
	}
}

func expectPanicIs(t *testing.T, target error, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic")
		}
		err, ok := r.(error)
		if !ok || !errors.Is(err, target) {
			t.Fatalf("panic %v", r)
		}
	}()
	fn()
}

func waitUntil(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout")
}
