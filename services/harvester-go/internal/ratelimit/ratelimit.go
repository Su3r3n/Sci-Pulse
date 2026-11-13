package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrBudgetExhausted is returned when a source's daily API budget has been spent.
var ErrBudgetExhausted = errors.New("daily API budget exhausted")

// TokenBucket implements a context-aware token bucket rate limiter.
type TokenBucket struct {
	mu       sync.Mutex
	capacity float64
	tokens   float64
	rate     float64 // tokens per second
	last     time.Time
	now      func() time.Time
}

// NewTokenBucket creates a token bucket refilling at ratePerSec with burst capacity.
// If ratePerSec <= 0, the bucket never blocks.
func NewTokenBucket(ratePerSec float64, burst int) *TokenBucket {
	if burst < 1 {
		burst = 1
	}
	nowFn := time.Now
	return &TokenBucket{
		capacity: float64(burst),
		tokens:   float64(burst),
		rate:     ratePerSec,
		last:     nowFn(),
		now:      nowFn,
	}
}

// NewIntervalLimiter creates a token bucket enforcing at least interval between requests.
func NewIntervalLimiter(interval time.Duration, burst int) *TokenBucket {
	if interval <= 0 {
		return NewTokenBucket(0, burst)
	}
	return NewTokenBucket(1.0/interval.Seconds(), burst)
}

func (b *TokenBucket) refillLocked(now time.Time) {
	if b.rate <= 0 {
		b.tokens = b.capacity
		b.last = now
		return
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.last = now
	}
}

// Allow consumes one token if immediately available.
func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rate <= 0 {
		return true
	}
	b.refillLocked(b.now())
	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true
	}
	return false
}

// Wait blocks until a token is available or ctx is cancelled.
func (b *TokenBucket) Wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		b.mu.Lock()
		if b.rate <= 0 {
			b.mu.Unlock()
			return nil
		}
		b.refillLocked(b.now())
		if b.tokens >= 1.0 {
			b.tokens -= 1.0
			b.mu.Unlock()
			return nil
		}
		needed := 1.0 - b.tokens
		waitDur := time.Duration((needed / b.rate) * float64(time.Second))
		if waitDur < time.Millisecond {
			waitDur = time.Millisecond
		}
		b.mu.Unlock()

		timer := time.NewTimer(waitDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// SourceLimiters manages per-source token buckets.
type SourceLimiters struct {
	mu      sync.RWMutex
	buckets map[string]*TokenBucket
}

// NewSourceLimiters creates an empty per-source limiter registry.
func NewSourceLimiters() *SourceLimiters {
	return &SourceLimiters{
		buckets: make(map[string]*TokenBucket),
	}
}

// Set registers or replaces the limiter for a source.
func (s *SourceLimiters) Set(source string, bucket *TokenBucket) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buckets[source] = bucket
}

// Wait waits on the registered limiter for source, or returns immediately if none is registered.
func (s *SourceLimiters) Wait(ctx context.Context, source string) error {
	s.mu.RLock()
	b := s.buckets[source]
	s.mu.RUnlock()
	if b == nil {
		return ctx.Err()
	}
	return b.Wait(ctx)
}

// BudgetStore persists daily source budget counters.
type BudgetStore interface {
	GetBudget(ctx context.Context, source string, day time.Time) (spent int, budget *int, err error)
	AddBudgetSpent(ctx context.Context, source string, day time.Time, delta int, budget *int) (spent int, remaining *int, err error)
	SetBudgetSnapshot(ctx context.Context, source string, day time.Time, spent int, budget int) error
}

// BudgetTracker tracks daily API call budgets per source.
type BudgetTracker struct {
	mu       sync.Mutex
	store    BudgetStore
	fallback map[string]*dailyBudget
	now      func() time.Time
}

type dailyBudget struct {
	day    string
	spent  int
	budget *int
}

// NewBudgetTracker creates a daily budget tracker optionally backed by BudgetStore.
func NewBudgetTracker(store BudgetStore) *BudgetTracker {
	return &BudgetTracker{
		store:    store,
		fallback: make(map[string]*dailyBudget),
		now:      func() time.Time { return time.Now().UTC() },
	}
}

// Sync updates the known daily budget and spent count (e.g. from OpenAlex /rate-limit).
func (t *BudgetTracker) Sync(ctx context.Context, source string, spent int, totalBudget int) error {
	day := t.today()
	if t.store != nil {
		return t.store.SetBudgetSnapshot(ctx, source, day, spent, totalBudget)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	b := totalBudget
	t.fallback[source] = &dailyBudget{
		day:    day.Format("2006-01-02"),
		spent:  spent,
		budget: &b,
	}
	return nil
}

// Check verifies whether the source still has remaining daily budget.
// If no budget cap is configured, remaining is -1 and capped is false.
func (t *BudgetTracker) Check(ctx context.Context, source string) (remaining int, capped bool, err error) {
	day := t.today()
	if t.store != nil {
		spent, budget, err := t.store.GetBudget(ctx, source, day)
		if err != nil {
			return 0, false, fmt.Errorf("get budget: %w", err)
		}
		if budget == nil {
			return -1, false, nil
		}
		rem := *budget - spent
		if rem <= 0 {
			return 0, true, ErrBudgetExhausted
		}
		return rem, true, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	entry := t.getFallbackLocked(source, day)
	if entry.budget == nil {
		return -1, false, nil
	}
	rem := *entry.budget - entry.spent
	if rem <= 0 {
		return 0, true, ErrBudgetExhausted
	}
	return rem, true, nil
}

// Consume increments daily spent count and returns remaining budget (or -1 if uncapped).
func (t *BudgetTracker) Consume(ctx context.Context, source string, delta int, defaultBudget *int) (remaining int, capped bool, err error) {
	if delta <= 0 {
		delta = 1
	}
	day := t.today()
	if t.store != nil {
		_, rem, err := t.store.AddBudgetSpent(ctx, source, day, delta, defaultBudget)
		if err != nil {
			return 0, false, fmt.Errorf("add budget spent: %w", err)
		}
		if rem == nil {
			return -1, false, nil
		}
		if *rem < 0 {
			return 0, true, ErrBudgetExhausted
		}
		return *rem, true, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	entry := t.getFallbackLocked(source, day)
	if entry.budget == nil && defaultBudget != nil {
		b := *defaultBudget
		entry.budget = &b
	}
	if entry.budget != nil && entry.spent+delta > *entry.budget {
		return 0, true, ErrBudgetExhausted
	}
	entry.spent += delta
	if entry.budget == nil {
		return -1, false, nil
	}
	return *entry.budget - entry.spent, true, nil
}

func (t *BudgetTracker) getFallbackLocked(source string, day time.Time) *dailyBudget {
	dayStr := day.Format("2006-01-02")
	entry, ok := t.fallback[source]
	if !ok || entry.day != dayStr {
		var budgetCopy *int
		if ok && entry.budget != nil {
			b := *entry.budget
			budgetCopy = &b
		}
		entry = &dailyBudget{day: dayStr, budget: budgetCopy}
		t.fallback[source] = entry
	}
	return entry
}

func (t *BudgetTracker) today() time.Time {
	now := t.now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
}
