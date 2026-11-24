package domain

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"replicationlag/internal/apperr"
)

// Interval is an inclusive GTID transaction interval.
type Interval struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

func (i Interval) String() string {
	if i.Start == i.End {
		return strconv.FormatUint(i.Start, 10)
	}
	return fmt.Sprintf("%d-%d", i.Start, i.End)
}

// SourceSet is the normalized interval set for one GTID source UUID.
type SourceSet struct {
	Source    string     `json:"source"`
	Intervals []Interval `json:"intervals"`
}

// GTIDSet is a canonical collection of source interval sets.
type GTIDSet struct {
	Sources []SourceSet `json:"sources"`
}

// GTIDParseOptions controls the explicitly allowed historical syntaxes.
type GTIDParseOptions struct {
	AllowTildeRanges        bool
	AllowIntegerShorthand   bool
	DefaultSource           string
	AllowColonRangeList     bool
	AllowCommaRangeList     bool
	AllowLegacySourceLabels bool
}

var (
	uuidHyphenRE = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	uuidPlainRE  = regexp.MustCompile(`(?i)^[0-9a-f]{32}$`)
	rangeRE      = regexp.MustCompile(`^\s*([0-9]+)(?:\s*[-~]\s*([0-9]+))?\s*$`)
	sourceRE     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
)

// NormalizeSource normalizes UUID source labels to lower-case hyphenated form.
func NormalizeSource(source string) (string, bool) {
	s := strings.TrimSpace(source)
	if uuidHyphenRE.MatchString(s) {
		return strings.ToLower(s), true
	}
	if uuidPlainRE.MatchString(s) {
		s = strings.ToLower(s)
		return s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:32], true
	}
	return "", false
}

// ParseGTID parses the canonical syntax. It intentionally rejects legacy
// shorthand; callers must opt into compatibility syntax explicitly.
func ParseGTID(raw string) (GTIDSet, error) {
	return ParseGTIDWithOptions(raw, GTIDParseOptions{AllowColonRangeList: true, AllowCommaRangeList: true})
}

// ParseGTIDWithOptions parses GTID text and normalizes all source interval
// lists by sorting, merging overlaps and merging adjacent intervals.
func ParseGTIDWithOptions(raw string, opts GTIDParseOptions) (GTIDSet, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return GTIDSet{}, fmt.Errorf("GTID set is empty")
	}
	if opts.AllowTildeRanges {
		text = strings.ReplaceAll(text, "~", "-")
	}
	if strings.Contains(text, "~") {
		return GTIDSet{}, fmt.Errorf("tilde ranges are not enabled")
	}

	parts := splitTopLevelComma(text)
	if len(parts) == 0 {
		return GTIDSet{}, fmt.Errorf("GTID set is empty")
	}

	bySource := map[string][]Interval{}
	var currentSource string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return GTIDSet{}, fmt.Errorf("empty GTID component")
		}
		source, ranges, hasSource, err := splitSourceRanges(part, opts)
		if err != nil {
			return GTIDSet{}, err
		}
		if hasSource {
			currentSource = source
		} else {
			if currentSource == "" {
				if !opts.AllowIntegerShorthand {
					return GTIDSet{}, fmt.Errorf("range %q has no source identifier", part)
				}
				src, ok := NormalizeSource(opts.DefaultSource)
				if !ok {
					return GTIDSet{}, fmt.Errorf("integer interval shorthand requires a valid default GTID source")
				}
				currentSource = src
			}
			ranges = []string{part}
		}

		for _, rangeText := range ranges {
			interval, err := parseInterval(rangeText)
			if err != nil {
				return GTIDSet{}, err
			}
			bySource[currentSource] = append(bySource[currentSource], interval)
		}
	}

	sources := make([]SourceSet, 0, len(bySource))
	for source, intervals := range bySource {
		sources = append(sources, SourceSet{Source: source, Intervals: NormalizeIntervals(intervals)})
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].Source < sources[j].Source })
	return GTIDSet{Sources: sources}, nil
}

func splitTopLevelComma(text string) []string {
	// GTID syntax has no nested delimiters. A comma either starts a new UUID
	// item or continues the current source's range list.
	return strings.Split(text, ",")
}

func splitSourceRanges(part string, opts GTIDParseOptions) (string, []string, bool, error) {
	colon := strings.IndexByte(part, ':')
	if colon < 0 {
		return "", nil, false, nil
	}
	label := strings.TrimSpace(part[:colon])
	if looksLikeRange(label) {
		return "", nil, false, nil
	}
	source, ok := NormalizeSource(label)
	if !ok {
		if !opts.AllowLegacySourceLabels || !sourceRE.MatchString(label) {
			return "", nil, false, fmt.Errorf("invalid GTID source %q", label)
		}
		source = strings.ToLower(label)
	}
	rest := strings.TrimSpace(part[colon+1:])
	if rest == "" {
		return "", nil, false, fmt.Errorf("source %q has no interval list", source)
	}
	var ranges []string
	if opts.AllowColonRangeList {
		ranges = strings.Split(rest, ":")
	} else {
		ranges = []string{rest}
	}
	if !opts.AllowCommaRangeList {
		for _, r := range ranges {
			if strings.Contains(r, ",") {
				return "", nil, false, fmt.Errorf("comma range lists are not enabled")
			}
		}
	}
	return source, ranges, true, nil
}

func looksLikeRange(s string) bool {
	return rangeRE.MatchString(s)
}

func parseInterval(text string) (Interval, error) {
	match := rangeRE.FindStringSubmatch(text)
	if match == nil {
		return Interval{}, fmt.Errorf("invalid interval %q", strings.TrimSpace(text))
	}
	start, err := strconv.ParseUint(match[1], 10, 64)
	if err != nil || start == 0 {
		return Interval{}, fmt.Errorf("invalid interval start %q", match[1])
	}
	end := start
	if match[2] != "" {
		end, err = strconv.ParseUint(match[2], 10, 64)
		if err != nil {
			return Interval{}, fmt.Errorf("invalid interval end %q", match[2])
		}
	}
	if end < start {
		return Interval{}, fmt.Errorf("interval end %d precedes start %d", end, start)
	}
	return Interval{Start: start, End: end}, nil
}

// NormalizeIntervals sorts and merges overlapping and adjacent intervals.
func NormalizeIntervals(in []Interval) []Interval {
	if len(in) == 0 {
		return []Interval{}
	}
	out := append([]Interval(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Start == out[j].Start {
			return out[i].End < out[j].End
		}
		return out[i].Start < out[j].Start
	})
	merged := make([]Interval, 0, len(out))
	for _, iv := range out {
		if len(merged) == 0 {
			merged = append(merged, iv)
			continue
		}
		last := &merged[len(merged)-1]
		adjacent := last.End != ^uint64(0) && iv.Start == last.End+1
		if iv.Start <= last.End || adjacent {
			if iv.End > last.End {
				last.End = iv.End
			}
			continue
		}
		merged = append(merged, iv)
	}
	return merged
}

// String returns canonical source:ranges syntax.
func (s GTIDSet) String() string {
	parts := make([]string, 0, len(s.Sources))
	for _, src := range s.Sources {
		intervals := make([]string, 0, len(src.Intervals))
		for _, iv := range src.Intervals {
			intervals = append(intervals, iv.String())
		}
		parts = append(parts, src.Source+":"+strings.Join(intervals, ":"))
	}
	return strings.Join(parts, ",")
}

func (s GTIDSet) IsEmpty() bool {
	for _, src := range s.Sources {
		if len(src.Intervals) > 0 {
			return false
		}
	}
	return true
}

func (s GTIDSet) Clone() GTIDSet {
	out := GTIDSet{Sources: make([]SourceSet, len(s.Sources))}
	for i, src := range s.Sources {
		out.Sources[i] = SourceSet{Source: src.Source, Intervals: append([]Interval(nil), src.Intervals...)}
	}
	return out
}

func (s GTIDSet) Equal(other GTIDSet) bool {
	a, b := s.Normalized(), other.Normalized()
	if len(a.Sources) != len(b.Sources) {
		return false
	}
	for i := range a.Sources {
		if a.Sources[i].Source != b.Sources[i].Source || len(a.Sources[i].Intervals) != len(b.Sources[i].Intervals) {
			return false
		}
		for j := range a.Sources[i].Intervals {
			if a.Sources[i].Intervals[j] != b.Sources[i].Intervals[j] {
				return false
			}
		}
	}
	return true
}

// Normalized returns a sorted canonical copy.
func (s GTIDSet) Normalized() GTIDSet {
	bySource := map[string][]Interval{}
	for _, src := range s.Sources {
		if len(src.Intervals) == 0 {
			continue
		}
		bySource[src.Source] = append(bySource[src.Source], src.Intervals...)
	}
	out := GTIDSet{Sources: make([]SourceSet, 0, len(bySource))}
	for source, intervals := range bySource {
		out.Sources = append(out.Sources, SourceSet{Source: source, Intervals: NormalizeIntervals(intervals)})
	}
	sort.Slice(out.Sources, func(i, j int) bool { return out.Sources[i].Source < out.Sources[j].Source })
	return out
}

// Union combines two normalized GTID sets.
func (s GTIDSet) Union(other GTIDSet) GTIDSet {
	bySource := map[string][]Interval{}
	for _, set := range []GTIDSet{s.Normalized(), other.Normalized()} {
		for _, src := range set.Sources {
			bySource[src.Source] = append(bySource[src.Source], src.Intervals...)
		}
	}
	out := GTIDSet{Sources: make([]SourceSet, 0, len(bySource))}
	for source, intervals := range bySource {
		out.Sources = append(out.Sources, SourceSet{Source: source, Intervals: NormalizeIntervals(intervals)})
	}
	sort.Slice(out.Sources, func(i, j int) bool { return out.Sources[i].Source < out.Sources[j].Source })
	return out
}

// IsSuperset reports whether every transaction in other is in s.
func (s GTIDSet) IsSuperset(other GTIDSet) bool {
	return other.Difference(s).IsEmpty()
}

// Difference returns intervals in s that are not present in other.
func (s GTIDSet) Difference(other GTIDSet) GTIDSet {
	a := s.Normalized()
	b := other.Normalized()
	bBySource := map[string][]Interval{}
	for _, src := range b.Sources {
		bBySource[src.Source] = src.Intervals
	}
	out := GTIDSet{}
	for _, src := range a.Sources {
		remaining := subtractIntervals(src.Intervals, bBySource[src.Source])
		if len(remaining) > 0 {
			out.Sources = append(out.Sources, SourceSet{Source: src.Source, Intervals: remaining})
		}
	}
	return out.Normalized()
}

func subtractIntervals(a, b []Interval) []Interval {
	if len(b) == 0 {
		return append([]Interval(nil), a...)
	}
	out := make([]Interval, 0, len(a))
	for _, iv := range a {
		curStart, curEnd := iv.Start, iv.End
		alive := true
		for _, cut := range b {
			if cut.End < curStart {
				continue
			}
			if cut.Start > curEnd {
				break
			}
			if cut.Start > curStart {
				out = append(out, Interval{Start: curStart, End: cut.Start - 1})
			}
			if cut.End >= curEnd {
				alive = false
				break
			}
			curStart = cut.End + 1
		}
		if alive {
			out = append(out, Interval{Start: curStart, End: curEnd})
		}
	}
	return NormalizeIntervals(out)
}

// Cardinality returns the number of GTID transactions, saturated on overflow.
func (s GTIDSet) Cardinality() uint64 {
	var total uint64
	for _, src := range s.Normalized().Sources {
		for _, iv := range src.Intervals {
			width := iv.End - iv.Start + 1
			if ^uint64(0)-total < width {
				return ^uint64(0)
			}
			total += width
		}
	}
	return total
}

// IntervalCount returns the number of normalized intervals.
func (s GTIDSet) IntervalCount() int {
	count := 0
	for _, src := range s.Normalized().Sources {
		count += len(src.Intervals)
	}
	return count
}

// Intersect returns the common transactions.
func (s GTIDSet) Intersect(other GTIDSet) GTIDSet {
	return s.Difference(s.Difference(other))
}

// Validate checks structural invariants of a GTID set.
func (s GTIDSet) Validate() error {
	seen := map[string]bool{}
	for _, src := range s.Sources {
		normalized, ok := NormalizeSource(src.Source)
		if !ok || normalized != src.Source {
			return fmt.Errorf("invalid source %q", src.Source)
		}
		if seen[src.Source] {
			return fmt.Errorf("duplicate source %q", src.Source)
		}
		seen[src.Source] = true
		if len(src.Intervals) == 0 {
			return fmt.Errorf("source %q has no intervals", src.Source)
		}
		prevEnd := uint64(0)
		for i, iv := range src.Intervals {
			if iv.Start == 0 || iv.End < iv.Start {
				return fmt.Errorf("invalid interval %s", iv.String())
			}
			if i > 0 && (iv.Start <= prevEnd || (prevEnd != ^uint64(0) && iv.Start == prevEnd+1)) {
				return fmt.Errorf("intervals are not normalized")
			}
			prevEnd = iv.End
		}
	}
	if len(s.Sources) == 0 {
		return fmt.Errorf("GTID set is empty")
	}
	return nil
}

// GTIDInvalidError creates the required observation error context.
func GTIDInvalidError(replicaID string, sampledAt int64, field string, err error) *apperr.Error {
	return apperr.New(apperr.CodeObservationInvalid, "observation field is invalid", map[string]any{
		"replica_id":    replicaID,
		"sampled_at_ms": sampledAt,
		"field":         field,
		"detail":        err.Error(),
	})
}
