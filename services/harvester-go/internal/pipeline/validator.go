package pipeline

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var doiRegex = regexp.MustCompile(`^10\.\d{4,9}/\S+$`)

// ValidatePaper validates a mapped Paper against §3.3 and §8.3 rules.
// Returns (reasonCode, err) when invalid.
func ValidatePaper(p Paper, now time.Time) (string, error) {
	if strings.TrimSpace(p.SourceID) == "" {
		return ReasonMissingSourceID, fmt.Errorf("source_id is required")
	}
	if strings.TrimSpace(p.Title) == "" {
		return ReasonMissingTitle, fmt.Errorf("title is required")
	}
	if p.PublishedAt.IsZero() {
		return ReasonMissingPublishedAt, fmt.Errorf("published_at is required")
	}
	if p.PublishedAt.After(now.UTC().Add(24 * time.Hour)) {
		return ReasonFuturePublishedAt, fmt.Errorf("published_at %s is in the future", p.PublishedAt.Format(time.RFC3339))
	}
	if p.DOI != nil && *p.DOI != "" {
		if !doiRegex.MatchString(*p.DOI) {
			return ReasonBadDOI, fmt.Errorf("invalid DOI %q", *p.DOI)
		}
	}
	if len(p.Authors) == 0 {
		return ReasonNoAuthors, fmt.Errorf("at least one author is required")
	}
	return "", nil
}
