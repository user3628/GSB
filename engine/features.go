package engine

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

const MissingBucket = 0

func ValidateSample(in SampleInput) error {
	if in.ID == "" {
		return newError(CodeConfigInvalid, "sample id is required")
	}
	if in.Label != 0 && in.Label != 1 {
		return &Error{Code: CodeLabelInvalid, Message: fmt.Sprintf("label must be 0 or 1 for sample %q, got %d", in.ID, in.Label)}
	}
	return nil
}

// NormalizeText applies NFKC, Unicode lowercase and whitespace folding.
func NormalizeText(text string) string {
	text = norm.NFKC.String(text)
	text = strings.ToLower(text)
	return strings.Join(strings.Fields(text), " ")
}

func isHan(r rune) bool {
	return unicode.Is(unicode.Han, r)
}

func isLatinLetter(r rune) bool {
	return unicode.IsLetter(r) && unicode.Is(unicode.Latin, r)
}

// Tokenize returns namespaced tokens: h: unigram, b: adjacent Han bigram,
// l: Latin run, f: supplemental categorical field, m: explicit missing value.
func Tokenize(normalized string) []string {
	runes := []rune(normalized)
	var tokens []string
	var han []rune
	var latin []rune

	flushHan := func() {
		if len(han) == 0 {
			return
		}
		for i, r := range han {
			tokens = append(tokens, "h:"+string(r))
			if i+1 < len(han) {
				tokens = append(tokens, "b:"+string(han[i:i+2]))
			}
		}
		han = nil
	}
	flushLatin := func() {
		if len(latin) == 0 {
			return
		}
		tokens = append(tokens, "l:"+string(latin))
		latin = nil
	}
	flushAll := func() {
		flushLatin()
		flushHan()
	}

	for _, r := range runes {
		switch {
		case isHan(r):
			flushLatin()
			han = append(han, r)
		case isLatinLetter(r):
			flushHan()
			latin = append(latin, r)
		default:
			flushAll()
		}
	}
	flushAll()
	return tokens
}

func stableHash(token string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(token))
	return h.Sum32()
}

func Featurize(in SampleInput, cfg Config) (*FeatureVector, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.Normalize()
	normalized := NormalizeText(in.Text)
	tokens := Tokenize(normalized)

	type counted struct {
		value  int
		tokens map[string]struct{}
	}
	buckets := make(map[int]*counted, cfg.Dim)
	getBucket := func(dim int) *counted {
		b := buckets[dim]
		if b == nil {
			b = &counted{tokens: make(map[string]struct{})}
			buckets[dim] = b
		}
		return b
	}

	collisions := 0
	noteToken := func(token string, dim int) {
		b := getBucket(dim)
		if _, seen := b.tokens[token]; !seen {
			if len(b.tokens) > 0 {
				collisions++
			}
			b.tokens[token] = struct{}{}
		}
		b.value++
	}

	missingCount := 0
	for _, field := range cfg.MissingFields {
		value := ""
		if field == "text" {
			value = normalized
		} else if in.Fields != nil {
			value = NormalizeText(in.Fields[field])
		}
		if value == "" {
			missingCount++
			noteToken("m:"+field, MissingBucket)
		}
	}

	for _, token := range tokens {
		dim := 1 + int(stableHash(token)%uint32(cfg.Dim-1))
		noteToken(token, dim)
	}

	if in.Fields != nil {
		keys := make([]string, 0, len(in.Fields))
		for k := range in.Fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if key == "" {
				continue
			}
			value := NormalizeText(in.Fields[key])
			if value == "" {
				continue
			}
			token := fmt.Sprintf("f:%s=%s", key, value)
			dim := 1 + int(stableHash(token)%uint32(cfg.Dim-1))
			noteToken(token, dim)
		}
	}

	dims := make([]int, 0, len(buckets))
	for dim := range buckets {
		dims = append(dims, dim)
	}
	sort.Ints(dims)
	features := make([]Feature, 0, len(dims))
	for _, dim := range dims {
		features = append(features, Feature{Dim: dim, Value: buckets[dim].value})
	}

	fv := &FeatureVector{
		SampleID:       in.ID,
		Dim:            cfg.Dim,
		MissingBucket:  MissingBucket,
		Features:       features,
		MissingCount:   missingCount,
		CollisionCount: collisions,
	}
	digest, _, err := DigestCanonical(struct {
		SampleID  string    `json:"sample_id"`
		Dim       int       `json:"dim"`
		Features  []Feature `json:"features"`
		Missing   int       `json:"missing_count"`
		Collision int       `json:"collision_count"`
	}{fv.SampleID, fv.Dim, fv.Features, fv.MissingCount, fv.CollisionCount})
	if err != nil {
		return nil, err
	}
	fv.Digest = digest
	return fv, nil
}

func UnitOfDim(dim, units int) int {
	return dim % units
}

func UnitDims(unit, dimCount, units int) []int {
	var dims []int
	for d := 0; d < dimCount; d++ {
		if UnitOfDim(d, units) == unit {
			dims = append(dims, d)
		}
	}
	return dims
}

// ScaleForDim returns an explicit per-dimension fixed-point scale. Higher
// specialization unit ids receive coarser scales, amplifying quant error.
func ScaleForDim(dim int, cfg Config) int64 {
	unit := UnitOfDim(dim, cfg.Units)
	amplifier := int64(1 + 3*unit)
	scale := cfg.BaseScale / amplifier
	if scale < 1 {
		scale = 1
	}
	return scale
}

func ValidateFeatureVector(fv *FeatureVector, cfg Config) error {
	if fv == nil {
		return newError(CodeFeatureDimInvalid, "nil feature vector")
	}
	for _, f := range fv.Features {
		if f.Dim < 0 || f.Dim >= cfg.Dim {
			d := f.Dim
			return &Error{Code: CodeFeatureDimInvalid, Dim: &d, Message: fmt.Sprintf("feature dimension %d outside [0,%d)", f.Dim, cfg.Dim)}
		}
		if f.Value == 0 {
			d := f.Dim
			return &Error{Code: CodeFeatureDimInvalid, Dim: &d, Message: fmt.Sprintf("feature dimension %d has zero value", f.Dim)}
		}
	}
	return nil
}
