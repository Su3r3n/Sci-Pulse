package pipeline

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/scipulse/scipulse/services/harvester-go/internal/source"
)

var githubURLRegex = regexp.MustCompile(`https?://(?:www\.)?github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[^\s)<>"',;\]}]*)?`)

// NormalizeDOI strips URL/scheme prefixes and lowercases the DOI.
func NormalizeDOI(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	lower := strings.ToLower(s)
	for _, prefix := range []string{
		"https://doi.org/",
		"http://doi.org/",
		"https://dx.doi.org/",
		"http://dx.doi.org/",
		"doi:",
	} {
		if strings.HasPrefix(lower, prefix) {
			s = s[len(prefix):]
			break
		}
	}
	return strings.ToLower(strings.TrimSpace(s))
}

// ExtractGitHubLinks extracts unique GitHub URLs from text fields with kind="code".
func ExtractGitHubLinks(texts ...string) []Link {
	seen := make(map[string]struct{})
	var links []Link
	for _, txt := range texts {
		if txt == "" {
			continue
		}
		matches := githubURLRegex.FindAllString(txt, -1)
		for _, m := range matches {
			cleaned := strings.TrimRight(m, ".,;:!?)]}>\"'")
			if cleaned == "" {
				continue
			}
			if _, ok := seen[cleaned]; ok {
				continue
			}
			seen[cleaned] = struct{}{}
			links = append(links, Link{URL: cleaned, Kind: "code"})
		}
	}
	return links
}

// MapRecord converts a RawRecord into a Paper contract struct.
// Returns (paper, stage, reasonCode, err).
func MapRecord(raw source.RawRecord, now time.Time) (Paper, string, string, error) {
	if raw.ParseError != nil {
		return Paper{}, StageParse, ReasonParseError, raw.ParseError
	}
	if len(strings.TrimSpace(string(raw.Body))) == 0 {
		return Paper{}, StageParse, ReasonParseError, fmt.Errorf("empty raw record body")
	}

	switch raw.Source {
	case "openalex":
		return mapOpenAlex(raw, now)
	case "arxiv":
		return mapArxiv(raw, now)
	default:
		return Paper{}, StageMap, ReasonParseError, fmt.Errorf("unsupported source %q", raw.Source)
	}
}

type openAlexWork struct {
	ID                    string           `json:"id"`
	DOI                   string           `json:"doi"`
	Title                 string           `json:"title"`
	Abstract              string           `json:"abstract"`
	AbstractInvertedIndex map[string][]int `json:"abstract_inverted_index"`
	PublicationDate       string           `json:"publication_date"`
	UpdatedDate           string           `json:"updated_date"`
	CitedByCount          *int             `json:"cited_by_count"`
	IDs                   struct {
		DOI   string `json:"doi"`
		Arxiv string `json:"arxiv"`
	} `json:"ids"`
	Authorships []struct {
		Author struct {
			DisplayName string `json:"display_name"`
			ORCID       string `json:"orcid"`
		} `json:"author"`
		Institutions []struct {
			DisplayName string `json:"display_name"`
		} `json:"institutions"`
		RawAffiliationStrings []string `json:"raw_affiliation_strings"`
	} `json:"authorships"`
	Topics []struct {
		DisplayName string `json:"display_name"`
	} `json:"topics"`
	Concepts []struct {
		DisplayName string `json:"display_name"`
	} `json:"concepts"`
	PrimaryLocation *openAlexLocation  `json:"primary_location"`
	Locations       []openAlexLocation `json:"locations"`
	OpenAccess      *struct {
		OAURL string `json:"oa_url"`
	} `json:"open_access"`
}

type openAlexLocation struct {
	LandingPageURL string `json:"landing_page_url"`
	PDFURL         string `json:"pdf_url"`
}

func mapOpenAlex(raw source.RawRecord, now time.Time) (Paper, string, string, error) {
	var w openAlexWork
	if err := json.Unmarshal(raw.Body, &w); err != nil {
		return Paper{}, StageParse, ReasonParseError, fmt.Errorf("invalid openalex json: %w", err)
	}

	sourceID := strings.TrimSpace(w.ID)
	sourceID = strings.TrimPrefix(sourceID, "https://openalex.org/")
	sourceID = strings.TrimPrefix(sourceID, "http://openalex.org/")

	doiStr := NormalizeDOI(w.DOI)
	if doiStr == "" {
		doiStr = NormalizeDOI(w.IDs.DOI)
	}
	var doiPtr *string
	if doiStr != "" {
		doiPtr = &doiStr
	}

	var arxivPtr *string
	if ax := normalizeArxivID(w.IDs.Arxiv); ax != "" {
		arxivPtr = &ax
	}

	abstractStr := strings.TrimSpace(w.Abstract)
	if abstractStr == "" && len(w.AbstractInvertedIndex) > 0 {
		abstractStr = reconstructInvertedAbstract(w.AbstractInvertedIndex)
	}
	var abstractPtr *string
	if abstractStr != "" {
		abstractPtr = &abstractStr
	}

	authors := make([]Author, 0, len(w.Authorships))
	for _, a := range w.Authorships {
		name := strings.TrimSpace(a.Author.DisplayName)
		if name == "" {
			continue
		}
		var orcidPtr *string
		if o := strings.TrimSpace(a.Author.ORCID); o != "" {
			o = strings.TrimPrefix(o, "https://orcid.org/")
			o = strings.TrimPrefix(o, "http://orcid.org/")
			orcidPtr = &o
		}
		affs := make([]string, 0, len(a.Institutions)+len(a.RawAffiliationStrings))
		affSeen := make(map[string]struct{})
		for _, inst := range a.Institutions {
			n := strings.TrimSpace(inst.DisplayName)
			if n != "" {
				if _, ok := affSeen[n]; !ok {
					affSeen[n] = struct{}{}
					affs = append(affs, n)
				}
			}
		}
		for _, rawAff := range a.RawAffiliationStrings {
			n := strings.TrimSpace(rawAff)
			if n != "" {
				if _, ok := affSeen[n]; !ok {
					affSeen[n] = struct{}{}
					affs = append(affs, n)
				}
			}
		}
		authors = append(authors, Author{
			Name:         name,
			ORCID:        orcidPtr,
			Affiliations: affs,
		})
	}

	topics := make([]string, 0, len(w.Topics))
	topicSeen := make(map[string]struct{})
	for _, tp := range w.Topics {
		n := strings.TrimSpace(tp.DisplayName)
		if n != "" {
			if _, ok := topicSeen[n]; !ok {
				topicSeen[n] = struct{}{}
				topics = append(topics, n)
			}
		}
	}
	if len(topics) == 0 {
		for _, c := range w.Concepts {
			n := strings.TrimSpace(c.DisplayName)
			if n != "" {
				if _, ok := topicSeen[n]; !ok {
					topicSeen[n] = struct{}{}
					topics = append(topics, n)
				}
			}
		}
	}

	publishedAt, _ := parseFlexibleTime(w.PublicationDate)
	updatedAt, okUpd := parseFlexibleTime(w.UpdatedDate)
	if !okUpd {
		updatedAt = publishedAt
	}

	links := make([]Link, 0)
	addLink := func(rawURL, kind string) {
		u := strings.TrimSpace(rawURL)
		if u == "" {
			return
		}
		if githubURLRegex.MatchString(u) {
			kind = "code"
		}
		links = append(links, Link{URL: u, Kind: kind})
	}
	if w.PrimaryLocation != nil {
		addLink(w.PrimaryLocation.LandingPageURL, "landing")
		addLink(w.PrimaryLocation.PDFURL, "pdf")
	}
	for _, loc := range w.Locations {
		addLink(loc.LandingPageURL, "landing")
		addLink(loc.PDFURL, "pdf")
	}
	if w.OpenAccess != nil {
		addLink(w.OpenAccess.OAURL, "pdf")
	}
	links = append(links, ExtractGitHubLinks(w.Title, abstractStr)...)
	links = dedupeLinks(links)

	return Paper{
		Source:       "openalex",
		SourceID:     sourceID,
		DOI:          doiPtr,
		ArxivID:      arxivPtr,
		Title:        strings.TrimSpace(w.Title),
		Abstract:     abstractPtr,
		Comments:     nil,
		JournalRef:   nil,
		Authors:      authors,
		Topics:       topics,
		PublishedAt:  publishedAt,
		UpdatedAt:    updatedAt,
		CitedByCount: w.CitedByCount,
		Links:        links,
		IngestedAt:   now.UTC(),
	}, "", "", nil
}

type arxivOAIRecord struct {
	XMLName xml.Name `xml:"record"`
	Header  struct {
		Identifier string `xml:"identifier"`
		Datestamp  string `xml:"datestamp"`
	} `xml:"header"`
	Metadata struct {
		Arxiv *arxivMetadata `xml:"arXiv"`
	} `xml:"metadata"`
}

type arxivMetadata struct {
	ID         string `xml:"id"`
	Created    string `xml:"created"`
	Updated    string `xml:"updated"`
	Title      string `xml:"title"`
	Abstract   string `xml:"abstract"`
	Comments   string `xml:"comments"`
	JournalRef string `xml:"journal-ref"`
	DOI        string `xml:"doi"`
	Categories string `xml:"categories"`
	Authors    struct {
		Author []struct {
			Keyname     string   `xml:"keyname"`
			Forenames   string   `xml:"forenames"`
			Suffix      string   `xml:"suffix"`
			Name        string   `xml:"name"`
			Affiliation []string `xml:"affiliation"`
		} `xml:"author"`
	} `xml:"authors"`
}

func mapArxiv(raw source.RawRecord, now time.Time) (Paper, string, string, error) {
	var rec arxivOAIRecord
	if err := xml.Unmarshal(raw.Body, &rec); err != nil {
		return Paper{}, StageParse, ReasonParseError, fmt.Errorf("invalid arxiv xml: %w", err)
	}
	if rec.Metadata.Arxiv == nil {
		return Paper{}, StageParse, ReasonParseError, fmt.Errorf("missing <metadata><arXiv> block")
	}
	m := rec.Metadata.Arxiv

	arxivID := normalizeArxivID(m.ID)
	sourceID := arxivID
	if sourceID == "" {
		sourceID = strings.TrimPrefix(strings.TrimSpace(rec.Header.Identifier), "oai:arXiv.org:")
		arxivID = sourceID
	}
	var arxivPtr *string
	if arxivID != "" {
		arxivPtr = &arxivID
	}

	doiStr := NormalizeDOI(m.DOI)
	var doiPtr *string
	if doiStr != "" {
		doiPtr = &doiStr
	}

	title := collapseWhitespace(m.Title)
	abstractStr := collapseWhitespace(m.Abstract)
	var abstractPtr *string
	if abstractStr != "" {
		abstractPtr = &abstractStr
	}

	commentsStr := collapseWhitespace(m.Comments)
	var commentsPtr *string
	if commentsStr != "" {
		commentsPtr = &commentsStr
	}

	journalRefStr := collapseWhitespace(m.JournalRef)
	var journalRefPtr *string
	if journalRefStr != "" {
		journalRefPtr = &journalRefStr
	}

	authors := make([]Author, 0, len(m.Authors.Author))
	for _, a := range m.Authors.Author {
		name := collapseWhitespace(a.Name)
		if name == "" {
			parts := make([]string, 0, 3)
			if f := collapseWhitespace(a.Forenames); f != "" {
				parts = append(parts, f)
			}
			if k := collapseWhitespace(a.Keyname); k != "" {
				parts = append(parts, k)
			}
			if s := collapseWhitespace(a.Suffix); s != "" {
				parts = append(parts, s)
			}
			name = strings.Join(parts, " ")
		}
		if name == "" {
			continue
		}
		affs := make([]string, 0, len(a.Affiliation))
		for _, af := range a.Affiliation {
			if cleaned := collapseWhitespace(af); cleaned != "" {
				affs = append(affs, cleaned)
			}
		}
		authors = append(authors, Author{
			Name:         name,
			ORCID:        nil,
			Affiliations: affs,
		})
	}

	topics := make([]string, 0)
	for _, cat := range strings.Fields(m.Categories) {
		if cat != "" {
			topics = append(topics, cat)
		}
	}

	publishedAt, _ := parseFlexibleTime(m.Created)
	if publishedAt.IsZero() {
		publishedAt, _ = parseFlexibleTime(rec.Header.Datestamp)
	}
	updatedAt, okUpd := parseFlexibleTime(m.Updated)
	if !okUpd {
		updatedAt, okUpd = parseFlexibleTime(rec.Header.Datestamp)
		if !okUpd {
			updatedAt = publishedAt
		}
	}

	links := make([]Link, 0, 4)
	if arxivID != "" {
		links = append(links,
			Link{URL: "https://arxiv.org/abs/" + arxivID, Kind: "landing"},
			Link{URL: "https://arxiv.org/pdf/" + arxivID, Kind: "pdf"},
		)
	}
	links = append(links, ExtractGitHubLinks(title, abstractStr, commentsStr, journalRefStr)...)
	links = dedupeLinks(links)

	return Paper{
		Source:       "arxiv",
		SourceID:     sourceID,
		DOI:          doiPtr,
		ArxivID:      arxivPtr,
		Title:        title,
		Abstract:     abstractPtr,
		Comments:     commentsPtr,
		JournalRef:   journalRefPtr,
		Authors:      authors,
		Topics:       topics,
		PublishedAt:  publishedAt,
		UpdatedAt:    updatedAt,
		CitedByCount: nil,
		Links:        links,
		IngestedAt:   now.UTC(),
	}, "", "", nil
}

func reconstructInvertedAbstract(idx map[string][]int) string {
	type wordPos struct {
		pos  int
		word string
	}
	var words []wordPos
	for word, positions := range idx {
		for _, p := range positions {
			words = append(words, wordPos{pos: p, word: word})
		}
	}
	if len(words) == 0 {
		return ""
	}
	sort.Slice(words, func(i, j int) bool {
		return words[i].pos < words[j].pos
	})
	out := make([]string, len(words))
	for i, wp := range words {
		out[i] = wp.word
	}
	return strings.Join(out, " ")
}

func normalizeArxivID(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	for _, prefix := range []string{
		"https://arxiv.org/abs/",
		"http://arxiv.org/abs/",
		"arXiv:",
		"arxiv:",
	} {
		if strings.HasPrefix(s, prefix) {
			s = s[len(prefix):]
			break
		}
	}
	return strings.TrimSpace(s)
}

func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func parseFlexibleTime(raw string) (time.Time, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return time.Time{}, false
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func dedupeLinks(links []Link) []Link {
	if len(links) == 0 {
		return []Link{}
	}
	seen := make(map[string]struct{}, len(links))
	out := make([]Link, 0, len(links))
	for _, l := range links {
		key := l.Kind + "\x00" + l.URL
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, l)
	}
	return out
}
