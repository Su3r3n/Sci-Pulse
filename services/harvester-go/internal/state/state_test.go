package state

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStoreCursorsETagsBudget(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()

	if _, ok, err := st.GetCursor(ctx, "openalex", "default"); err != nil || ok {
		t.Fatalf("expected no cursor, got ok=%v err=%v", ok, err)
	}

	if err := st.RecordError(ctx, "openalex", "default", "temporary 503"); err != nil {
		t.Fatalf("RecordError failed: %v", err)
	}
	cur, ok, err := st.GetCursor(ctx, "openalex", "default")
	if err != nil || !ok || cur.CursorValue != "" || cur.LastError == nil || *cur.LastError != "temporary 503" {
		t.Fatalf("unexpected cursor after RecordError: %+v", cur)
	}

	if err := st.SaveCursor(ctx, Cursor{Source: "openalex", Scope: "default", CursorValue: "2026-11-10"}); err != nil {
		t.Fatalf("SaveCursor failed: %v", err)
	}
	cur, ok, err = st.GetCursor(ctx, "openalex", "default")
	if err != nil || !ok || cur.CursorValue != "2026-11-10" || cur.LastError != nil || cur.LastSuccessAt == nil {
		t.Fatalf("unexpected cursor after SaveCursor: %+v", cur)
	}

	if err := st.SaveETag(ctx, ETagEntry{URL: "https://api.github.com/repos/rucio/rucio", ETag: `"abc"`, LastStatus: 200}); err != nil {
		t.Fatalf("SaveETag failed: %v", err)
	}
	etag, ok, err := st.GetETag(ctx, "https://api.github.com/repos/rucio/rucio")
	if err != nil || !ok || etag.ETag != `"abc"` || etag.LastStatus != 200 {
		t.Fatalf("unexpected etag: %+v", etag)
	}

	day := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	capVal := 100
	spent, rem, err := st.AddBudgetSpent(ctx, "openalex", day, 5, &capVal)
	if err != nil || spent != 5 || rem == nil || *rem != 95 {
		t.Fatalf("unexpected budget after add: spent=%d rem=%v err=%v", spent, rem, err)
	}
}
