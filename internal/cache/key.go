package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"cdn-edge-consistency/internal/model"
)

type KeyRules struct {
	CollapseSlashes  bool `json:"collapse_slashes"`
	MergeDotSegments bool `json:"merge_dot_segments"`
	UppercasePercent bool `json:"uppercase_percent"`
	SortQuery        bool `json:"sort_query"`
}

type VaryValue struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Identity struct {
	Key        string      `json:"cache_key"`
	NormPath   string      `json:"normalized_path"`
	NormQuery  string      `json:"normalized_query"`
	Vary       []VaryValue `json:"vary"`
	DisplayKey string      `json:"display_key"`
}

func invalidKey(format string, args ...any) error {
	return model.NewBiz(model.CodeKeyInvalid, fmt.Sprintf(format, args...))
}

// NormalizePath canonicalizes the escaped URL path. Repeated slashes and dot
// segments are merged, percent triplets are uppercased, and percent-encoded
// unreserved octets are decoded. Reserved octets stay encoded (notably %2F),
// so an encoded slash cannot silently become a path separator.
func NormalizePath(raw string, rules KeyRules) (string, error) {
	if raw == "" {
		raw = "/"
	}
	if !strings.HasPrefix(raw, "/") {
		return "", invalidKey("path must begin with /")
	}
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if b <= 0x20 || b == 0x7f {
			return "", invalidKey("path contains an unescaped control or space at byte %d", i)
		}
		if b == '%' {
			if i+2 >= len(raw) {
				return "", invalidKey("truncated percent escape at byte %d", i)
			}
			if !isHex(raw[i+1]) || !isHex(raw[i+2]) {
				return "", invalidKey("invalid percent escape at byte %d", i)
			}
		}
	}
	normalized := normalizePercentEscapes(raw, rules.UppercasePercent)
	if !rules.MergeDotSegments && !rules.CollapseSlashes {
		return normalized, nil
	}
	lastSeg := normalized[strings.LastIndex(normalized, "/")+1:]
	trailingSlash := strings.HasSuffix(normalized, "/") || (rules.MergeDotSegments && (lastSeg == "." || lastSeg == ".."))
	segments := strings.Split(strings.TrimPrefix(normalized, "/"), "/")
	out := make([]string, 0, len(segments))
	for _, seg := range segments {
		if rules.MergeDotSegments {
			switch seg {
			case ".":
				continue
			case "..":
				if len(out) > 0 {
					out = out[:len(out)-1]
				}
				continue
			}
		}
		if seg == "" && rules.CollapseSlashes {
			continue
		}
		out = append(out, seg)
	}
	result := "/" + strings.Join(out, "/")
	if result == "" {
		result = "/"
	}
	if trailingSlash && result != "/" && !strings.HasSuffix(result, "/") {
		result += "/"
	}
	return result, nil
}

func normalizePercentEscapes(raw string, uppercase bool) string {
	var b strings.Builder
	b.Grow(len(raw))
	for i := 0; i < len(raw); {
		if raw[i] != '%' {
			b.WriteByte(raw[i])
			i++
			continue
		}
		h := raw[i+1 : i+3]
		v := fromHex(h[0])<<4 | fromHex(h[1])
		if isUnreserved(v) {
			b.WriteByte(v)
		} else {
			b.WriteByte('%')
			if uppercase {
				b.WriteString(strings.ToUpper(h))
			} else {
				b.WriteString(strings.ToLower(h))
			}
		}
		i += 3
	}
	return b.String()
}

func NormalizeQuery(raw string, rules KeyRules) (string, error) {
	if raw == "" {
		return "", nil
	}
	type pair struct{ k, v string }
	pairs := make([]pair, 0, strings.Count(raw, "&")+1)
	for _, part := range strings.Split(raw, "&") {
		if part == "" {
			continue
		}
		k, v, hasEq := strings.Cut(part, "=")
		if !hasEq {
			v = ""
		}
		dk, err := decodeQueryComponent(k)
		if err != nil {
			return "", invalidKey("invalid query key: %v", err)
		}
		dv, err := decodeQueryComponent(v)
		if err != nil {
			return "", invalidKey("invalid query value: %v", err)
		}
		pairs = append(pairs, pair{k: queryEscape(dk), v: queryEscape(dv)})
	}
	if rules.SortQuery {
		sort.SliceStable(pairs, func(i, j int) bool {
			if pairs[i].k != pairs[j].k {
				return pairs[i].k < pairs[j].k
			}
			return pairs[i].v < pairs[j].v
		})
	}
	parts := make([]string, 0, len(pairs))
	for _, p := range pairs {
		parts = append(parts, p.k+"="+p.v)
	}
	return strings.Join(parts, "&"), nil
}

func decodeQueryComponent(s string) (string, error) {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		switch s[i] {
		case '+':
			b.WriteByte(' ')
			i++
		case '%':
			if i+2 >= len(s) || !isHex(s[i+1]) || !isHex(s[i+2]) {
				return "", fmt.Errorf("invalid percent escape")
			}
			b.WriteByte(fromHex(s[i+1])<<4 | fromHex(s[i+2]))
			i += 3
		default:
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String(), nil
}

func queryEscape(s string) string {
	const hexChars = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexChars[c>>4])
		b.WriteByte(hexChars[c&0x0f])
	}
	return b.String()
}

func BuildIdentity(rawPath, rawQuery string, headers http.Header, vary []string, rules KeyRules) (Identity, error) {
	np, err := NormalizePath(rawPath, rules)
	if err != nil {
		return Identity{}, err
	}
	nq, err := NormalizeQuery(rawQuery, rules)
	if err != nil {
		return Identity{}, err
	}
	vary = canonicalVaryNames(vary)
	values := make([]VaryValue, 0, len(vary))
	for _, name := range vary {
		values = append(values, VaryValue{Name: name, Value: canonicalHeaderValue(headers.Values(name))})
	}
	var canonical strings.Builder
	canonical.WriteString("cdn-cache-key/v1\npath=")
	canonical.WriteString(np)
	canonical.WriteString("\nquery=")
	canonical.WriteString(nq)
	canonical.WriteByte('\n')
	for _, v := range values {
		canonical.WriteString("vary:")
		canonical.WriteString(v.Name)
		canonical.WriteByte('=')
		canonical.WriteString(v.Value)
		canonical.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(canonical.String()))
	key := hex.EncodeToString(sum[:])
	display := np
	if nq != "" {
		display += "?" + nq
	}
	if len(values) > 0 {
		display += "#vary{"
		for i, v := range values {
			if i > 0 {
				display += ","
			}
			display += v.Name + "=" + v.Value
		}
		display += "}"
	}
	return Identity{Key: key, NormPath: np, NormQuery: nq, Vary: values, DisplayKey: display}, nil
}

func canonicalVaryNames(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, name := range in {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func canonicalHeaderValue(values []string) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		for _, p := range strings.Split(value, ",") {
			parts = append(parts, strings.TrimSpace(p))
		}
	}
	return strings.Join(parts, ",")
}

func isUnreserved(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' || c == '~'
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func fromHex(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	default:
		return c - 'A' + 10
	}
}
