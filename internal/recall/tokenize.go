package recall

import (
	"hash/fnv"
	"sort"
	"strings"
	"unicode"

	"localsearch/multirecall/internal/model"
)

func tokenize(text string) []string {
	seen := make(map[string]struct{})
	var token []rune
	flush := func() {
		if len(token) == 0 {
			return
		}
		value := strings.ToLower(string(token))
		seen[value] = struct{}{}
		token = token[:0]
	}
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			token = append(token, r)
			continue
		}
		flush()
	}
	flush()
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func IsEmptyQuery(text string) bool {
	return len(tokenize(text)) == 0
}

func overlap(left, right []string) []string {
	i, j := 0, 0
	out := make([]string, 0, min(len(left), len(right)))
	for i < len(left) && j < len(right) {
		switch {
		case left[i] == right[j]:
			out = append(out, left[i])
			i++
			j++
		case left[i] < right[j]:
			i++
		default:
			j++
		}
	}
	return out
}

func QueryVector(query model.Query, dimension int) []float64 {
	hash := fnv.New64a()
	vector := make([]float64, dimension)
	add := func(token string, weight float64) {
		if token == "" {
			return
		}
		hash.Reset()
		_, _ = hash.Write([]byte(token))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(query.Locale))
		sum := hash.Sum64()
		idx := int(sum % uint64(dimension))
		sign := 1.0
		if (sum>>63)&1 == 1 {
			sign = -1
		}
		vector[idx] += sign * weight
	}
	for _, term := range tokenize(query.Text) {
		add(term, 1.0)
	}
	for _, category := range tokenize(strings.Join(query.ClickedCategories, " ")) {
		add(category, 0.5)
	}
	add("locale:"+strings.ToLower(query.Locale), 0.25)
	return vector
}
