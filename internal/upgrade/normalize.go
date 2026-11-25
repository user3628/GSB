package upgrade

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"dialogalign/internal/domain"
)

var integerPattern = regexp.MustCompile(`^[+-]?[0-9]+$`)

var datePatterns = []struct {
	re     *regexp.Regexp
	layout string
}{
	{regexp.MustCompile(`[0-9]{4}-[0-9]{2}-[0-9]{2}`), "2006-01-02"},
	{regexp.MustCompile(`[0-9]{4}/[0-9]{2}/[0-9]{2}`), "2006/01/02"},
	{regexp.MustCompile(`[0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日`), "2006年1月2日"},
}

// NormalizeValue converts free text into the canonical representation for the
// declared value kind. Bounds are deliberately not enforced here: alignment
// reports out-of-range values without discarding their provenance.
func NormalizeValue(raw string, kind domain.ValueKind, cfg domain.NormalizationConfig) (domain.SlotValue, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return domain.SlotValue{}, fmt.Errorf("empty value for %s", kind)
	}
	switch kind {
	case domain.KindInteger:
		compact := strings.ReplaceAll(raw, ",", "")
		compact = strings.ReplaceAll(compact, " ", "")
		if !integerPattern.MatchString(compact) {
			return domain.SlotValue{}, fmt.Errorf("%q is not an integer", raw)
		}
		n, err := strconv.ParseInt(compact, 10, 64)
		if err != nil {
			return domain.SlotValue{}, fmt.Errorf("%q is outside int64", raw)
		}
		return domain.SlotValue{Kind: kind, Canonical: strconv.FormatInt(n, 10), Raw: raw}, nil
	case domain.KindEnum:
		value := strings.TrimSpace(raw)
		if cfg.EnumCaseFold {
			value = strings.ToLower(value)
		}
		if alias, ok := cfg.EnumAliases[value]; ok {
			value = alias
		}
		value = strings.Join(strings.Fields(value), " ")
		if value == "" {
			return domain.SlotValue{}, fmt.Errorf("empty enum value")
		}
		return domain.SlotValue{Kind: kind, Canonical: value, Raw: raw}, nil
	case domain.KindDateRange:
		start, end, err := parseDateRange(raw, cfg)
		if err != nil {
			return domain.SlotValue{}, err
		}
		return domain.SlotValue{Kind: kind, Canonical: start + "/" + end, Raw: raw}, nil
	default:
		return domain.SlotValue{}, fmt.Errorf("unsupported value kind %q", kind)
	}
}

func parseDateRange(raw string, cfg domain.NormalizationConfig) (string, string, error) {
	dateStrs := extractDates(raw)
	if len(dateStrs) < 2 {
		return "", "", fmt.Errorf("%q is not a two-end date range", raw)
	}
	start, err := normalizeDate(dateStrs[0], cfg)
	if err != nil {
		return "", "", err
	}
	end, err := normalizeDate(dateStrs[1], cfg)
	if err != nil {
		return "", "", err
	}
	if start > end {
		return "", "", fmt.Errorf("date range starts after it ends: %s/%s", start, end)
	}
	return start, end, nil
}

func extractDates(raw string) []string {
	var out []string
	rest := raw
	for {
		bestStart, bestEnd := -1, -1
		for _, p := range datePatterns {
			loc := p.re.FindStringIndex(rest)
			if loc != nil && (bestStart == -1 || loc[0] < bestStart || (loc[0] == bestStart && loc[1] > bestEnd)) {
				bestStart, bestEnd = loc[0], loc[1]
			}
		}
		if bestStart < 0 {
			break
		}
		out = append(out, rest[bestStart:bestEnd])
		rest = rest[bestEnd:]
	}
	return out
}

func normalizeDate(raw string, cfg domain.NormalizationConfig) (string, error) {
	for _, layout := range cfg.DateLayouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.Format("2006-01-02"), nil
		}
	}
	for _, p := range datePatterns {
		if p.re.MatchString(raw) {
			if t, err := time.Parse(p.layout, raw); err == nil {
				return t.Format("2006-01-02"), nil
			}
		}
	}
	return "", fmt.Errorf("unsupported date %q", raw)
}

// ValueCompatible reports whether a canonical value lies in an inclusive role
// range. It intentionally has no side effects so it can be used while deciding
// legacy candidates.
func ValueCompatible(value domain.SlotValue, r domain.ValueRange) bool {
	if value.Kind != r.Kind {
		return false
	}
	switch r.Kind {
	case domain.KindInteger:
		n, err := strconv.ParseInt(value.Canonical, 10, 64)
		if err != nil {
			return false
		}
		if r.Min != nil && n < *r.Min {
			return false
		}
		if r.Max != nil && n > *r.Max {
			return false
		}
		return true
	case domain.KindEnum:
		for _, allowed := range r.Enum {
			if value.Canonical == allowed {
				return true
			}
		}
		return false
	case domain.KindDateRange:
		parts := strings.Split(value.Canonical, "/")
		if len(parts) != 2 {
			return false
		}
		if r.DateMin != "" && parts[0] < r.DateMin {
			return false
		}
		if r.DateMax != "" && parts[1] > r.DateMax {
			return false
		}
		return true
	default:
		return false
	}
}

func RangeCompatible(declared, mapped domain.ValueRange) bool {
	if declared.Kind != mapped.Kind {
		return false
	}
	switch declared.Kind {
	case domain.KindInteger:
		if mapped.Min != nil && (declared.Min == nil || *declared.Min < *mapped.Min) {
			return false
		}
		if mapped.Max != nil && (declared.Max == nil || *declared.Max > *mapped.Max) {
			return false
		}
		return true
	case domain.KindEnum:
		allowed := make(map[string]struct{}, len(mapped.Enum))
		for _, v := range mapped.Enum {
			allowed[v] = struct{}{}
		}
		for _, v := range declared.Enum {
			if _, ok := allowed[v]; !ok {
				return false
			}
		}
		return len(declared.Enum) > 0
	case domain.KindDateRange:
		if mapped.DateMin != "" && (declared.DateMin == "" || declared.DateMin < mapped.DateMin) {
			return false
		}
		if mapped.DateMax != "" && (declared.DateMax == "" || declared.DateMax > mapped.DateMax) {
			return false
		}
		return declared.DateMin != "" && declared.DateMax != ""
	default:
		return false
	}
}
