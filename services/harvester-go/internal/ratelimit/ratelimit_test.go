package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTokenBucketRateLimitAndCancel(t *testing.T) {
	b := NewIntervalLimiter(40*time.Millisecond, 1)
	if !b.Allow() {
		t.Fatal("expected initial burst token to be available")
	}
	if b.Allow() {
		t.Fatal("expected second immediate Allow to return false")
	}

	start := time.Now()
	if err := b.Wait(context.Background()); err != nil {
		t.Fatalf("Wait failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("expected wait >= 20ms, got %v", elapsed)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	bSlow := NewIntervalLimiter(time.Second, 1)
	_ = bSlow.Allow()
	if err := bSlow.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestBudgetTrackerDailyLimits(t *testing.T) {
	tracker := NewBudgetTracker(nil)
	capVal := 3
	ctx := context.Background()

	rem, capped, err := tracker.Consume(ctx, "openalex", 1, &capVal)
	if err != nil || !capped || rem != 2 {
		t.Fatalf("unexpected result: rem=%d capped=%v err=%v", rem, capped, err)
	}
	rem, capped, err = tracker.Consume(ctx, "openalex", 2, &capVal)
	if err != nil || !capped || rem != 0 {
		t.Fatalf("unexpected result: rem=%d capped=%v err=%v", rem, capped, err)
	}
	_, _, err = tracker.Consume(ctx, "openalex", 1, &capVal)
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("expected ErrBudgetExhausted, got %v", err)
	}

	// Sync resets/updates budget from API /rate-limit snapshot
	if err := tracker.Sync(ctx, "openalex", 10, 100); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	rem, capped, err = tracker.Check(ctx, "openalex")
	if err != nil || !capped || rem != 90 {
		t.Fatalf("expected remaining 90, got rem=%d capped=%v err=%v", rem, capped, err)
	}
}
