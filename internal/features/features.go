package features

import (
	"hash/fnv"
	"image"
	"math"
	"sort"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"imretrace/internal/model"
)

type TermCount struct {
	Term  string `json:"term"`
	Count int    `json:"count"`
}

type FeatureSet struct {
	ItemID            string      `json:"item_id"`
	ShardID           string      `json:"shard_id"`
	Stage             string      `json:"stage"`
	InputFingerprint  string      `json:"input_fingerprint"`
	PolicyVersion     int         `json:"policy_version"`
	Grid              int         `json:"grid"`
	VisualDim         int         `json:"visual_dim"`
	TextDim           int         `json:"text_dim"`
	Visual            []float64   `json:"visual"`
	Text              []float64   `json:"text"`
	Terms             []TermCount `json:"terms"`
	NormalizedCaption string      `json:"normalized_caption"`
	Digest            string      `json:"digest"`
}

func round9(v float64) float64 {
	return math.Round(v*1e9) / 1e9
}

func roundSlice(values []float64) []float64 {
	out := make([]float64, len(values))
	for i, v := range values {
		out[i] = round9(v)
	}
	return out
}

func NormalizeText(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r == '\u3000' {
			r = ' '
		}
		if r >= 0xFF01 && r <= 0xFF5E {
			r = r - 0xFEE0
		}
		b.WriteRune(r)
	}
	nfkc := norm.NFKC.String(b.String())
	fields := strings.FieldsFunc(nfkc, func(r rune) bool { return unicode.IsSpace(r) })
	return strings.Join(fields, " ")
}

func isCJK(r rune) bool {
	return unicode.Is(unicode.Han, r) || unicode.Is(unicode.Hiragana, r) ||
		unicode.Is(unicode.Katakana, r) || unicode.Is(unicode.Hangul, r)
}

func Tokenize(normalized string) ([]string, []TermCount) {
	var tokens []string
	var latin []rune
	var cjk []rune

	flushLatin := func() {
		if len(latin) > 0 {
			tokens = append(tokens, string(latin))
			latin = nil
		}
	}
	flushCJK := func() {
		if len(cjk) == 0 {
			return
		}
		for _, r := range cjk {
			tokens = append(tokens, string(r))
		}
		for i := 0; i+1 < len(cjk); i++ {
			tokens = append(tokens, string(cjk[i:i+2]))
		}
		cjk = nil
	}
	for _, r0 := range normalized {
		r := unicode.ToLower(r0)
		switch {
		case isCJK(r):
			flushLatin()
			cjk = append(cjk, r)
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			flushCJK()
			latin = append(latin, r)
		default:
			flushLatin()
			flushCJK()
		}
	}
	flushLatin()
	flushCJK()

	counts := map[string]int{}
	for _, token := range tokens {
		counts[token]++
	}
	terms := make([]TermCount, 0, len(counts))
	for term, count := range counts {
		terms = append(terms, TermCount{Term: term, Count: count})
	}
	sort.Slice(terms, func(i, j int) bool { return terms[i].Term < terms[j].Term })
	return tokens, terms
}

func hashIndex(term string) (int, float64) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(term))
	sum := h.Sum64()
	idx := int(sum % uint64(model.TextDim))
	sign := 1.0
	if (sum>>63)&1 == 1 {
		sign = -1
	}
	return idx, sign
}

func TextVector(normalized string, terms []TermCount) []float64 {
	vec := make([]float64, model.TextDim)
	for _, tc := range terms {
		idx, sign := hashIndex("term:" + tc.Term)
		vec[idx] += sign * (1 + math.Log(float64(tc.Count)))
	}
	lowered := []rune(strings.ToLower(normalized))
	for n := 2; n <= 3; n++ {
		for i := 0; i+n <= len(lowered); i++ {
			gram := string(lowered[i : i+n])
			idx, sign := hashIndex("ngram:" + gram)
			vec[idx] += sign * 0.5
		}
	}
	norm := 0.0
	for _, v := range vec {
		norm += v * v
	}
	if norm > 0 {
		norm = math.Sqrt(norm)
		for i := range vec {
			vec[i] = round9(vec[i] / norm)
		}
	}
	return vec
}

func ExtractText(itemID, caption string) (string, []TermCount, []float64, error) {
	normalized := NormalizeText(caption)
	if normalized == "" {
		return "", nil, nil, model.NewBiz(model.CodeFeatureInvalid, model.StageFeature, itemID, "caption is empty after NFKC and whitespace folding")
	}
	_, terms := Tokenize(normalized)
	vec := TextVector(normalized, terms)
	if allZero(vec) {
		return "", nil, nil, model.NewBiz(model.CodeFeatureInvalid, model.StageFeature, itemID, "text descriptor is all zero")
	}
	return normalized, terms, vec, nil
}

type pixel struct{ r, g, b float64 }

func ExtractVisual(img image.Image, itemID string, grid int) ([]float64, error) {
	if grid <= 0 {
		return nil, model.NewBiz(model.CodeFeatureInvalid, model.StageFeature, itemID, "grid must be positive, got %d", grid)
	}
	if img == nil || img.Bounds().Empty() {
		return nil, model.NewBiz(model.CodeFeatureInvalid, model.StageFeature, itemID, "image bounds are empty")
	}
	bounds := img.Bounds()
	w, h := bounds.Dx(), bounds.Dy()
	first := img.At(bounds.Min.X, bounds.Min.Y)
	fr, fg, fb, fa := first.RGBA()
	blank := true
	pixels := make([][][4]float64, h)
	lum := make([][]float64, h)
	for y := 0; y < h; y++ {
		pixels[y] = make([][4]float64, w)
		lum[y] = make([]float64, w)
		for x := 0; x < w; x++ {
			r, g, b, a := img.At(bounds.Min.X+x, bounds.Min.Y+y).RGBA()
			rr, gg, bb, aa := float64(r>>8), float64(g>>8), float64(b>>8), float64(a>>8)
			pixels[y][x] = [4]float64{rr, gg, bb, aa}
			lum[y][x] = 0.2126*rr + 0.7152*gg + 0.0722*bb
			if r != fr || g != fg || b != fb || a != fa {
				blank = false
			}
		}
	}
	if blank {
		return nil, model.NewBiz(model.CodeFeatureInvalid, model.StageFeature, itemID, "image is blank or uniformly constant")
	}

	dim := model.VisualDim(grid)
	out := make([]float64, 0, dim)
	for cy := 0; cy < grid; cy++ {
		y0 := cy * h / grid
		y1 := (cy + 1) * h / grid
		for cx := 0; cx < grid; cx++ {
			x0 := cx * w / grid
			x1 := (cx + 1) * w / grid
			var sr, sg, sb float64
			count := float64((x1 - x0) * (y1 - y0))
			for y := y0; y < y1; y++ {
				for x := x0; x < x1; x++ {
					p := pixels[y][x]
					sr += p[0]
					sg += p[1]
					sb += p[2]
				}
			}
			mr, mg, mb := sr/count, sg/count, sb/count
			var vr, vg, vb float64
			hist := make([]float64, model.GradientBins)
			var gradTotal float64
			for y := y0; y < y1; y++ {
				for x := x0; x < x1; x++ {
					p := pixels[y][x]
					dr, dg, db := p[0]-mr, p[1]-mg, p[2]-mb
					vr += dr * dr
					vg += dg * dg
					vb += db * db
					if x+1 < x1 && y+1 < y1 {
						gx := lum[y][x+1] - lum[y][x]
						gy := lum[y+1][x] - lum[y][x]
						mag := math.Hypot(gx, gy)
						angle := math.Atan2(gy, gx)
						if angle < 0 {
							angle += 2 * math.Pi
						}
						bin := int(angle / (2 * math.Pi) * model.GradientBins)
						if bin >= model.GradientBins {
							bin = model.GradientBins - 1
						}
						hist[bin] += mag
						gradTotal += mag
					}
				}
			}
			out = append(out, mr/255.0, mg/255.0, mb/255.0)
			out = append(out, math.Sqrt(vr/count)/128.0, math.Sqrt(vg/count)/128.0, math.Sqrt(vb/count)/128.0)
			if gradTotal > 0 {
				for i := range hist {
					hist[i] /= gradTotal
				}
			}
			out = append(out, hist...)
		}
	}
	out = roundSlice(out)
	if len(out) != dim {
		return nil, model.DimError(model.CodeFeatureInvalid, model.StageFeature, itemID, len(out), "visual descriptor dim %d expected %d", len(out), dim)
	}
	if allZero(out) {
		return nil, model.NewBiz(model.CodeFeatureInvalid, model.StageFeature, itemID, "visual descriptor is all zero")
	}
	return out, nil
}

func allZero(v []float64) bool {
	for _, x := range v {
		if x != 0 {
			return false
		}
	}
	return true
}

func (f *FeatureSet) RefreshDigest() string {
	f.Digest = ""
	f.Digest = model.HashBytes("features-v1:", model.MustCanonical(f))
	return f.Digest
}
