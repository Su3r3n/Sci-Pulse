package state

import (
	"context"
	"sync"
	"time"
)

type cursorKey struct {
	source string
	scope  string
}

type budgetKey struct {
	source string
	day    string
}

type budgetRow struct {
	spent  int
	budget *int
}

// MemoryStore is an in-memory Store implementation for unit tests and offline mode.
type MemoryStore struct {
	mu      sync.RWMutex
	cursors map[cursorKey]Cursor
	etags   map[string]ETagEntry
	budgets map[budgetKey]budgetRow
}

// NewMemoryStore creates an initialized MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		cursors: make(map[cursorKey]Cursor),
		etags:   make(map[string]ETagEntry),
		budgets: make(map[budgetKey]budgetRow),
	}
}

func (m *MemoryStore) Migrate(context.Context) error { return nil }
func (m *MemoryStore) Ping(context.Context) error    { return nil }

func (m *MemoryStore) GetCursor(_ context.Context, source, scope string) (Cursor, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.cursors[cursorKey{source: source, scope: scope}]
	return c, ok, nil
}

func (m *MemoryStore) SaveCursor(_ context.Context, cursor Cursor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	if cursor.UpdatedAt.IsZero() {
		cursor.UpdatedAt = now
	}
	if cursor.LastSuccessAt == nil {
		cursor.LastSuccessAt = &now
	}
	cursor.LastError = nil
	m.cursors[cursorKey{source: cursor.Source, scope: cursor.Scope}] = cursor
	return nil
}

func (m *MemoryStore) RecordError(_ context.Context, source, scope, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := cursorKey{source: source, scope: scope}
	cur := m.cursors[key]
	cur.Source = source
	cur.Scope = scope
	cur.LastError = &errMsg
	cur.UpdatedAt = time.Now().UTC()
	m.cursors[key] = cur
	return nil
}

func (m *MemoryStore) GetETag(_ context.Context, url string) (ETagEntry, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.etags[url]
	return e, ok, nil
}

func (m *MemoryStore) SaveETag(_ context.Context, entry ETagEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry.FetchedAt.IsZero() {
		entry.FetchedAt = time.Now().UTC()
	}
	m.etags[entry.URL] = entry
	return nil
}

func (m *MemoryStore) GetBudget(_ context.Context, source string, day time.Time) (int, *int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	row, ok := m.budgets[budgetKey{source: source, day: day.UTC().Format("2006-01-02")}]
	if !ok {
		return 0, nil, nil
	}
	var b *int
	if row.budget != nil {
		v := *row.budget
		b = &v
	}
	return row.spent, b, nil
}

func (m *MemoryStore) AddBudgetSpent(_ context.Context, source string, day time.Time, delta int, budget *int) (int, *int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := budgetKey{source: source, day: day.UTC().Format("2006-01-02")}
	row := m.budgets[k]
	if row.budget == nil && budget != nil {
		v := *budget
		row.budget = &v
	}
	row.spent += delta
	m.budgets[k] = row
	if row.budget == nil {
		return row.spent, nil, nil
	}
	rem := *row.budget - row.spent
	return row.spent, &rem, nil
}

func (m *MemoryStore) SetBudgetSnapshot(_ context.Context, source string, day time.Time, spent int, budget int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := budgetKey{source: source, day: day.UTC().Format("2006-01-02")}
	b := budget
	m.budgets[k] = budgetRow{spent: spent, budget: &b}
	return nil
}
