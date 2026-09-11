package delaytimer

import (
	"context"
	"testing"
	"time"
)

func TestMemoryScheduleAndClaimDueJob(t *testing.T) {
	now := time.Unix(1000, 0)
	m := NewMemory(WithMemoryClock(func() time.Time { return now }))

	job := Job{Key: "answer_timeout:p1", Kind: "answer_timeout", Payload: "p1", At: now.Add(-time.Second)}
	if err := m.Schedule(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	got, err := m.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
	if got[0].Key != job.Key || got[0].Kind != job.Kind || got[0].Payload != job.Payload {
		t.Fatalf("got %+v", got[0])
	}
}

func TestMemoryReplaceSameKey(t *testing.T) {
	now := time.Unix(1000, 0)
	m := NewMemory(WithMemoryClock(func() time.Time { return now }))

	job := Job{Key: "k", Kind: "kind", Payload: "old", At: now.Add(-time.Second)}
	_ = m.Schedule(context.Background(), job)
	job.Payload = "new"
	job.At = now.Add(-time.Millisecond)
	_ = m.Schedule(context.Background(), job)

	got, err := m.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
	if got[0].Payload != "new" {
		t.Fatalf("payload %s", got[0].Payload)
	}
}

func TestMemoryCancel(t *testing.T) {
	now := time.Unix(1000, 0)
	m := NewMemory(WithMemoryClock(func() time.Time { return now }))

	job := Job{Key: "k", Kind: "kind", Payload: "p", At: now.Add(-time.Second)}
	_ = m.Schedule(context.Background(), job)
	if err := m.Cancel(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	got, err := m.Claim(ctx, 8)
	if err != nil && ctx.Err() == nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("claimed %v", got)
	}
}

func TestMemoryClaimIgnoresFutureJobs(t *testing.T) {
	now := time.Unix(1000, 0)
	m := NewMemory(WithMemoryClock(func() time.Time { return now }))

	_ = m.Schedule(context.Background(), Job{Key: "k", Kind: "kind", Payload: "p", At: now.Add(time.Hour)})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	got, err := m.Claim(ctx, 8)
	if len(got) != 0 {
		t.Fatalf("claimed future job %v err=%v", got, err)
	}
}

func TestMemoryFailReschedulesJob(t *testing.T) {
	m := NewMemory()
	job := Job{Key: "k", Kind: "kind", Payload: "p", At: time.Now().Add(-time.Second)}
	_ = m.Schedule(context.Background(), job)
	got, _ := m.Claim(context.Background(), 1)
	if len(got) != 1 {
		t.Fatalf("claim %v", got)
	}
	if err := m.Fail(context.Background(), got[0]); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	again, err := m.Claim(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].Key != "k" {
		t.Fatalf("want requeued job, got %v", again)
	}
}

func TestMemoryFailDoesNotOverwriteNewerSchedule(t *testing.T) {
	now := time.Unix(1000, 0)
	m := NewMemory(WithMemoryClock(func() time.Time { return now }))

	_ = m.Schedule(context.Background(), Job{Key: "k", Kind: "kind", Payload: "old", At: now})
	got, _ := m.Claim(context.Background(), 1)
	_ = m.Schedule(context.Background(), Job{Key: "k", Kind: "kind", Payload: "new", At: now.Add(time.Hour)})
	if err := m.Fail(context.Background(), got[0]); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	again, _ := m.Claim(ctx, 1)
	if len(again) != 0 {
		t.Fatalf("future job claimed: %v", again)
	}
	m.mu.Lock()
	payload := m.byKey["k"].job.Payload
	m.mu.Unlock()
	if payload != "new" {
		t.Fatalf("payload %s", payload)
	}
}

func TestMemoryAckIsNoopAfterClaim(t *testing.T) {
	now := time.Unix(1000, 0)
	m := NewMemory(WithMemoryClock(func() time.Time { return now }))
	job := Job{Key: "k", Kind: "kind", Payload: "p", At: now}
	_ = m.Schedule(context.Background(), job)
	got, _ := m.Claim(context.Background(), 1)
	if err := m.Ack(context.Background(), got[0]); err != nil {
		t.Fatal(err)
	}
}

func TestJobKey(t *testing.T) {
	if JobKey("kind", "payload") != "kind:payload" {
		t.Fatalf("got %s", JobKey("kind", "payload"))
	}
}
