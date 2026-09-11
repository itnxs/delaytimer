package delaytimer

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	mu        sync.Mutex
	scheduled []Task
	canceled  []string
	claims    []Task
	ackN      int
	failN     int
	claimErr  error
}

func (s *fakeStore) Schedule(_ context.Context, task Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduled = append(s.scheduled, task)
	return nil
}

func (s *fakeStore) Cancel(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.canceled = append(s.canceled, key)
	return nil
}

func (s *fakeStore) Claim(_ context.Context, n int) ([]Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	return nil
}

func (s *fakeStore) Fail(context.Context, Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failN++
	return nil
}

func (s *fakeStore) snapshot() (scheduled []Task, canceled []string, ackN, failN int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Task(nil), s.scheduled...), append([]string(nil), s.canceled...), s.ackN, s.failN
}

func silentLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

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
