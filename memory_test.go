package delaytimer

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
)

func TestMemoryScheduleAndClaimDue(t *testing.T) {
	now := time.Unix(1000, 0)
	m := NewMemory(WithMemoryClock(func() time.Time { return now }))
	task := Task{Key: "k1", Kind: "order", Payload: "p1", At: now.Add(-time.Second)}
	if err := m.Schedule(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	got, err := m.Claim(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "k1" || got[0].Payload != "p1" {
		t.Fatalf("got=%+v", got)
	}
	if err := m.Ack(context.Background(), got[0]); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryClaimNotDueBlocksUntilCancel(t *testing.T) {
	now := time.Unix(1000, 0)
	m := NewMemory(WithMemoryClock(func() time.Time { return now }))
	if err := m.Schedule(context.Background(), Task{Key: "k", Kind: "x", Payload: "p", At: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err := m.Claim(ctx, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
}

func TestMemoryClaimWhenDueBetweenClockReads(t *testing.T) {
	at := time.Unix(10, 0)
	var n int
	m := NewMemory(WithMemoryClock(func() time.Time {
		n++
		if n == 1 {
			return at.Add(-time.Nanosecond)
		}
		return at
	}))
	if err := m.Schedule(context.Background(), Task{Key: "k", Kind: "x", Payload: "p", At: at}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	got, err := m.Claim(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "k" {
		t.Fatalf("got=%+v", got)
	}
}

func TestMemoryClaimWakesWhenTaskBecomesDue(t *testing.T) {
	m := NewMemory()
	at := time.Now().Add(25 * time.Millisecond)
	if err := m.Schedule(context.Background(), Task{Key: "k", Kind: "x", Payload: "p", At: at}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := m.Claim(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "k" {
		t.Fatalf("got=%+v", got)
	}
}

func TestMemoryScheduleOverwrite(t *testing.T) {
	now := time.Unix(1000, 0)
	m := NewMemory(WithMemoryClock(func() time.Time { return now }))
	ctx := context.Background()
	if err := m.Schedule(ctx, Task{Key: "k", Kind: "x", Payload: "old", At: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := m.Schedule(ctx, Task{Key: "k", Kind: "x", Payload: "new", At: now.Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}
	got, err := m.Claim(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Payload != "new" {
		t.Fatalf("got=%+v", got)
	}
}

func TestMemoryCancel(t *testing.T) {
	now := time.Unix(1000, 0)
	m := NewMemory(WithMemoryClock(func() time.Time { return now }))
	ctx := context.Background()
	if err := m.Schedule(ctx, Task{Key: "k", Kind: "x", Payload: "p", At: now.Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err := m.Cancel(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()
	_, err := m.Claim(cctx, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
}

func TestMemoryClaimBatchAndMinN(t *testing.T) {
	now := time.Unix(1000, 0)
	m := NewMemory(WithMemoryClock(func() time.Time { return now }))
	ctx := context.Background()
	for i, key := range []string{"a", "b", "c"} {
		if err := m.Schedule(ctx, Task{Key: key, Kind: "x", Payload: key, At: now.Add(-time.Duration(i) * time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := m.Claim(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d", len(got))
	}
	if err := m.Schedule(ctx, Task{Key: "d", Kind: "x", Payload: "d", At: now.Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}
	got, err = m.Claim(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("n<1 should claim 1, got=%d", len(got))
	}
}

func TestMemoryFailRequeueAndNoOverwrite(t *testing.T) {
	now := time.Unix(1000, 0)
	clockNow := now
	m := NewMemory(WithMemoryClock(func() time.Time { return clockNow }))
	ctx := context.Background()
	task := Task{Key: "k", Kind: "x", Payload: "old", At: now.Add(-time.Second)}
	if err := m.Schedule(ctx, task); err != nil {
		t.Fatal(err)
	}
	got, err := m.Claim(ctx, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("claim=%v err=%v", got, err)
	}
	if err := m.Fail(ctx, got[0]); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	_, err = m.Claim(cctx, 1)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("should not be due yet, err=%v", err)
	}

	clockNow = now.Add(failRequeueDelay)
	got, err = m.Claim(ctx, 1)
	if err != nil || len(got) != 1 || got[0].Payload != "old" {
		t.Fatalf("requeue=%+v err=%v", got, err)
	}

	if err := m.Schedule(ctx, Task{Key: "k", Kind: "x", Payload: "new", At: clockNow.Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err := m.Fail(ctx, got[0]); err != nil {
		t.Fatal(err)
	}
	got, err = m.Claim(ctx, 1)
	if err != nil || len(got) != 1 || got[0].Payload != "new" {
		t.Fatalf("no overwrite=%+v err=%v", got, err)
	}
}
