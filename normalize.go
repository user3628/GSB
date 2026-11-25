package prefixcache

import (
	"bytes"
	"encoding/json"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
	"golang.org/x/text/width"
)

// NormalizeText applies the fixed normalization sequence used by the cache:
// NFKC, compatibility-width folding, lowercasing, and Unicode whitespace
// collapse to one ASCII space.
func NormalizeText(text string) string {
	text = norm.NFKC.String(text)
	text = width.Fold.String(text)
	text = strings.ToLower(text)

	runes := []rune(text)
	out := make([]rune, 0, len(runes))
	pendingSpace := false
	for _, r := range runes {
		if unicode.IsSpace(r) {
			if len(out) > 0 {
				pendingSpace = true
			}
			continue
		}
		if pendingSpace {
			out = append(out, ' ')
			pendingSpace = false
		}
		out = append(out, r)
	}
	return string(out)
}

// CanonicalRequestPlanJSON returns a stable serialized form for byte-level
// determinism checks.
func CanonicalRequestPlanJSON(plan RequestPlan) ([]byte, error) {
	return marshalDeterministic(plan)
}

func marshalDeterministic(v any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
