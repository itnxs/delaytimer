package delaytimer

import (
	"context"
	"github.com/pkg/errors"
	"sync"
	"testing"
	"time"
)

type eventSink struct {
	mu    sync.Mutex
	plays []string
	err   error
}

func (s *eventSink) add(play string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plays = append(s.plays, play)
	return s.err
}

func (s *eventSink) last() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.plays) == 0 {
		return ""
	}
	return s.plays[len(s.plays)-1]
}

func (s *eventSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.plays)
}

var sink eventSink

func resetSink() {
	sink = eventSink{}
}

type stubEvent struct {
	name string
}

func (e *stubEvent) EventName() EventName { return EventName(e.name) }

type timeoutEvent struct {
	Play string `json:"play"`
}

func (e *timeoutEvent) EventName() EventName { return "answer_timeout" }

type badJSONEvent struct {
	Ch chan int `json:"ch"`
}

func (e *badJSONEvent) EventName() EventName { return "bad_json" }

type orderEvent struct {
	Z string `json:"z"`
	A string `json:"a"`
}

func (e *orderEvent) EventName() EventName { return "order" }

type blockEvent struct{}

func (e *blockEvent) EventName() EventName { return "block" }

type valueEvent struct{}

func (valueEvent) EventName() EventName { return "value" }

func nopHandler(name EventName) EventHandler {
	return Bind(&stubEvent{name: string(name)}, func(context.Context, *stubEvent) error { return nil })
}

func timeoutHandler() EventHandler {
	return Bind(&timeoutEvent{}, func(_ context.Context, p *timeoutEvent) error {
		return sink.add(p.Play)
	})
}

func failHandler() EventHandler {
	return Bind(&timeoutEvent{}, func(context.Context, *timeoutEvent) error {
		return errors.New("fail")
	})
}

func blockHandler() EventHandler {
	return Bind(&blockEvent{}, func(context.Context, *blockEvent) error {
		g := currentBlockGate()
		select {
		case g.started <- struct{}{}:
		default:
		}
		<-g.hold
		return nil
	})
}

type blockGate struct {
	started chan struct{}
	hold    chan struct{}
}

var blockGateMu sync.Mutex
var activeBlockGate *blockGate

func currentBlockGate() *blockGate {
	blockGateMu.Lock()
	defer blockGateMu.Unlock()
	return activeBlockGate
}

func withBlockGate() *blockGate {
	g := &blockGate{
		started: make(chan struct{}, 1),
		hold:    make(chan struct{}),
	}
	blockGateMu.Lock()
	activeBlockGate = g
	blockGateMu.Unlock()
	return g
}

type fakeBackend struct {
	mu        sync.Mutex
	scheduled []Job
	canceled  []string
	toClaim   []Job
	acked     []Job
	failed    []Job
	cancelErr error
}

func (f *fakeBackend) Schedule(_ context.Context, job Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scheduled = append(f.scheduled, job)
	return nil
}

func (f *fakeBackend) Cancel(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cancelErr != nil {
		return f.cancelErr
	}
	f.canceled = append(f.canceled, key)
	return nil
}

func (f *fakeBackend) Claim(_ context.Context, n int) ([]Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.toClaim) == 0 {
		return nil, nil
	}
	if n > len(f.toClaim) {
		n = len(f.toClaim)
	}
	out := f.toClaim[:n]
	f.toClaim = f.toClaim[n:]
	return out, nil
}

func (f *fakeBackend) Ack(_ context.Context, job Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, job)
	return nil
}

func (f *fakeBackend) Fail(_ context.Context, job Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, job)
	return nil
}

func (f *fakeBackend) scheduledJobs() []Job {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Job, len(f.scheduled))
	copy(out, f.scheduled)
	return out
}

func (f *fakeBackend) canceledKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.canceled))
	copy(out, f.canceled)
	return out
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
