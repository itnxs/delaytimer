package delaytimer

import (
	"context"
	"errors"
	"testing"
	"time"

)

func TestMemoryScheduleAndClaimDue(t *testing.T) {
	m := NewMemory()
	task := Task{Key: "k1", Kind: "order", Payload: "p1", At: time.Now().Add(-time.Second)}
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
	m := NewMemory()
	if err := m.Schedule(context.Background(), Task{Key: "k", Kind: "x", Payload: "p", At: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err := m.Claim(ctx, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
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
	m := NewMemory()
	ctx := context.Background()
	if err := m.Schedule(ctx, Task{Key: "k", Kind: "x", Payload: "old", At: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := m.Schedule(ctx, Task{Key: "k", Kind: "x", Payload: "new", At: time.Now().Add(-time.Second)}); err != nil {
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
	m := NewMemory()
	ctx := context.Background()
	if err := m.Schedule(ctx, Task{Key: "k", Kind: "x", Payload: "p", At: time.Now().Add(-time.Second)}); err != nil {
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
	m := NewMemory()
	ctx := context.Background()
	for i, key := range []string{"a", "b", "c"} {
		if err := m.Schedule(ctx, Task{Key: key, Kind: "x", Payload: key, At: time.Now().Add(-time.Duration(i+1) * time.Second)}); err != nil {
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
	if err := m.Schedule(ctx, Task{Key: "d", Kind: "x", Payload: "d", At: time.Now().Add(-time.Second)}); err != nil {
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
	m := NewMemory()
	ctx := context.Background()
	task := Task{Key: "k", Kind: "x", Payload: "old", At: time.Now().Add(-time.Second)}
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

	wait, cancelWait := context.WithTimeout(ctx, failRequeueDelay+50*time.Millisecond)
	got, err = m.Claim(wait, 1)
	cancelWait()
	if err != nil || len(got) != 1 || got[0].Payload != "old" {
		t.Fatalf("requeue=%+v err=%v", got, err)
	}

	if err := m.Schedule(ctx, Task{Key: "k", Kind: "x", Payload: "new", At: time.Now().Add(-time.Second)}); err != nil {
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
