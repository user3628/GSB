package engine

import (
	"fmt"
	"math"
	"sort"

	"ocrlab/internal/canonical"
	"ocrlab/internal/config"
	"ocrlab/internal/model"
)

type GlyphTemplate struct {
	Char   string `json:"char"`
	Hash   string `json:"hash"`
	Vector []byte `json:"vector"`
}

type TemplateLibrary struct {
	Templates []GlyphTemplate `json:"templates"`
	CharSet   []string        `json:"character_set"`
	Digest    string          `json:"digest"`
}

type RecognitionBuild struct {
	Results []model.RecognitionResult
	Errors  []model.RecognitionError
	Library TemplateLibrary
}

func BuildTemplateLibrary(binary BinaryImage, components []Component, spec model.ImageSpec, cfg config.RecognitionConfig, emptyProbe bool) (TemplateLibrary, error) {
	if emptyProbe {
		return finalizeLibrary(nil, spec), nil
	}
	templates := make([]GlyphTemplate, 0, 32)
	seen := make(map[string]struct{})
	for _, truth := range spec.Truth {
		chars := []rune(truth.Text)
		if len(chars) == 0 {
			continue
		}
		x0, y0, x1, y1 := normalizedPixelRect(truth.Box, binary.Width, binary.Height)
		inside := componentsInRect(components, x0, y0, x1, y1)
		if len(inside) == len(chars) {
			for i, character := range chars {
				vector, ok := normalizeComponent(binary, inside[i], cfg.NormWidth, cfg.NormHeight)
				if !ok {
					continue
				}
				template := GlyphTemplate{Char: string(character), Hash: canonical.DigestBytes(vector), Vector: vector}
				key := template.Char + ":" + template.Hash
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				templates = append(templates, template)
			}
			continue
		}
		// A segmenter can merge two narrow glyphs. The fallback partitions the
		// declared truth box into deterministic character cells, preserving the
		// truth-only template contract instead of guessing missing labels.
		for i, character := range chars {
			cellX0 := x0 + (x1-x0)*i/len(chars)
			cellX1 := x0 + (x1-x0)*(i+1)/len(chars)
			vector, ok := normalizeBinaryRect(binary, cellX0, y0, cellX1, y1, cfg.NormWidth, cfg.NormHeight)
			if !ok {
				continue
			}
			template := GlyphTemplate{Char: string(character), Hash: canonical.DigestBytes(vector), Vector: vector}
			key := template.Char + ":" + template.Hash
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			templates = append(templates, template)
		}
	}
	return finalizeLibrary(templates, spec), nil
}

func finalizeLibrary(templates []GlyphTemplate, spec model.ImageSpec) TemplateLibrary {
	sort.SliceStable(templates, func(i, j int) bool {
		if templates[i].Char != templates[j].Char {
			return templates[i].Char < templates[j].Char
		}
		return templates[i].Hash < templates[j].Hash
	})
	chars := make([]string, 0, len(spec.Truth))
	seen := make(map[string]struct{})
	for _, truth := range spec.Truth {
		for _, character := range []rune(truth.Text) {
			value := string(character)
			if _, exists := seen[value]; exists {
				continue
			}
			seen[value] = struct{}{}
			chars = append(chars, value)
		}
	}
	sort.Strings(chars)
	digest, _ := canonical.Digest(struct {
		Templates []GlyphTemplate `json:"templates"`
		CharSet   []string        `json:"character_set"`
	}{Templates: templates, CharSet: chars})
	return TemplateLibrary{Templates: templates, CharSet: chars, Digest: digest}
}

func Recognize(binary BinaryImage, components []Component, detection model.DetectionProduct, spec model.ImageSpec, cfg config.RecognitionConfig, library TemplateLibrary, strategy string) RecognitionBuild {
	results := make([]model.RecognitionResult, 0, len(detection.Candidates))
	errors := make([]model.RecognitionError, 0)
	exactTemplates := make(map[string]string, len(library.Templates))
	for _, template := range library.Templates {
		if _, exists := exactTemplates[template.Hash]; !exists {
			exactTemplates[template.Hash] = template.Char
		}
	}
	for _, candidate := range detection.Candidates {
		result := model.RecognitionResult{
			CandidateIndex: candidate.Index,
			Box:            candidate.Box,
			Status:         "error",
			TemplateCount:  len(library.Templates),
		}
		x0, y0, x1, y1 := normalizedPixelRect(candidate.Box, binary.Width, binary.Height)
		width, height := x1-x0, y1-y0
		if len(library.Templates) == 0 {
			miss := model.RecognitionError{
				Code: "E_TEMPLATE_MISS", Message: "template library is empty",
				ImageID: spec.ImageID, CandidateIndex: candidate.Index,
			}
			result.ErrorCode = miss.Code
			results = append(results, result)
			errors = append(errors, miss)
			continue
		}
		if width < cfg.MinGlyphWidth || height < cfg.MinGlyphHeight {
			miss := model.RecognitionError{
				Code: "E_TEMPLATE_MISS", Message: fmt.Sprintf("candidate pixel size %dx%d is below the configured minimum", width, height),
				ImageID: spec.ImageID, CandidateIndex: candidate.Index,
			}
			result.ErrorCode = miss.Code
			results = append(results, result)
			errors = append(errors, miss)
			continue
		}
		glyphs := componentsInRect(components, x0, y0, x1, y1)
		if len(glyphs) == 0 {
			miss := model.RecognitionError{
				Code: "E_TEMPLATE_MISS", Message: "candidate box contains no complete glyph component",
				ImageID: spec.ImageID, CandidateIndex: candidate.Index,
			}
			result.ErrorCode = miss.Code
			results = append(results, result)
			errors = append(errors, miss)
			continue
		}
		text := make([]rune, 0, len(glyphs))
		totalDistance := 0.0
		for _, glyph := range glyphs {
			vector, ok := normalizeComponent(binary, glyph, cfg.NormWidth, cfg.NormHeight)
			if !ok {
				continue
			}
			best := -1
			bestDistance := math.Inf(1)
			if strategy == "r-hash-light" {
				if character, exists := exactTemplates[canonical.DigestBytes(vector)]; exists {
					text = append(text, []rune(character)...)
					continue
				}
			}
			for i := range library.Templates {
				distance := hamming(vector, library.Templates[i].Vector)
				if strategy == "r-hash-dense" {
					// Dense mode adds a cell-density descriptor to the exact
					// normalized-hash distance. Balanced mode uses only Hamming
					// distance; light mode takes the exact-hash fast path above.
					densityDistance := cellDensityDistance(vector, library.Templates[i].Vector, cfg.NormWidth, cfg.Grid)
					distance = 0.7*distance + 0.3*densityDistance
				}
				if distance < bestDistance {
					bestDistance = distance
					best = i
				}
			}
			if best < 0 {
				continue
			}
			text = append(text, []rune(library.Templates[best].Char)...)
			totalDistance += bestDistance
		}
		if len(text) == 0 {
			miss := model.RecognitionError{
				Code: "E_TEMPLATE_MISS", Message: "no candidate glyph matched an available template",
				ImageID: spec.ImageID, CandidateIndex: candidate.Index,
			}
			result.ErrorCode = miss.Code
			results = append(results, result)
			errors = append(errors, miss)
			continue
		}
		distance := totalDistance / float64(len(text))
		result.Status = "recognized"
		result.Text = string(text)
		result.MatchDistance = round6(distance)
		result.GlyphCount = len(text)
		result.Confidence = round6(1 / (1 + distance))
		result.WithinThreshold = distance <= cfg.MaxDistance
		results = append(results, result)
	}
	return RecognitionBuild{Results: results, Errors: errors, Library: library}
}

func normalizedPixelRect(rect model.Rect, width, height int) (int, int, int, int) {
	x0 := int(math.Round(rect.X * float64(width)))
	y0 := int(math.Round(rect.Y * float64(height)))
	x1 := int(math.Round((rect.X + rect.W) * float64(width)))
	y1 := int(math.Round((rect.Y + rect.H) * float64(height)))
	x0 = maxInt(0, minInt(width, x0))
	y0 = maxInt(0, minInt(height, y0))
	x1 = maxInt(0, minInt(width, x1))
	y1 = maxInt(0, minInt(height, y1))
	return x0, y0, x1, y1
}

func componentsInRect(components []Component, x0, y0, x1, y1 int) []Component {
	result := make([]Component, 0, 8)
	for _, component := range components {
		cx, cy := component.CenterX(), component.CenterY()
		if cx >= float64(x0) && cx <= float64(x1) && cy >= float64(y0) && cy <= float64(y1) {
			result = append(result, component)
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].X0 != result[j].X0 {
			return result[i].X0 < result[j].X0
		}
		return result[i].Y0 < result[j].Y0
	})
	return result
}

func normalizeComponent(binary BinaryImage, component Component, normWidth, normHeight int) ([]byte, bool) {
	return normalizeBinaryRect(binary, component.X0, component.Y0, component.X1, component.Y1, normWidth, normHeight)
}

func normalizeBinaryRect(binary BinaryImage, x0, y0, x1, y1, normWidth, normHeight int) ([]byte, bool) {
	if x1 <= x0 || y1 <= y0 || normWidth < 1 || normHeight < 1 {
		return nil, false
	}
	vector := make([]byte, normWidth*normHeight)
	ink := 0
	for ny := 0; ny < normHeight; ny++ {
		sy := y0 + (2*ny+1)*(y1-y0)/(2*normHeight)
		if sy >= binary.Height {
			sy = binary.Height - 1
		}
		for nx := 0; nx < normWidth; nx++ {
			sx := x0 + (2*nx+1)*(x1-x0)/(2*normWidth)
			if sx >= binary.Width {
				sx = binary.Width - 1
			}
			if sx >= 0 && sy >= 0 && binary.Ink[sy*binary.Width+sx] {
				vector[ny*normWidth+nx] = 1
				ink++
			}
		}
	}
	if ink == 0 {
		return nil, false
	}
	return vector, true
}

func hamming(a, b []byte) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 1
	}
	different := 0
	for i := range a {
		if a[i] != b[i] {
			different++
		}
	}
	return float64(different) / float64(len(a))
}

func cellDensityDistance(a, b []byte, width, grid int) float64 {
	if len(a) != len(b) || width <= 0 || grid <= 0 {
		return 1
	}
	height := len(a) / width
	if height <= 0 {
		return 1
	}
	total := 0.0
	cells := 0
	for gy := 0; gy < grid; gy++ {
		for gx := 0; gx < grid; gx++ {
			x0 := gx * width / grid
			x1 := (gx + 1) * width / grid
			y0 := gy * height / grid
			y1 := (gy + 1) * height / grid
			sumA, sumB, count := 0, 0, 0
			for y := y0; y < y1; y++ {
				for x := x0; x < x1; x++ {
					sumA += int(a[y*width+x])
					sumB += int(b[y*width+x])
					count++
				}
			}
			if count == 0 {
				continue
			}
			densityA := float64(sumA) / float64(count)
			densityB := float64(sumB) / float64(count)
			total += math.Abs(densityA - densityB)
			cells++
		}
	}
	if cells == 0 {
		return 1
	}
	return total / float64(cells)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
