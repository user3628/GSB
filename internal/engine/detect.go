package engine

import (
	"fmt"
	"image"
	"math"
	"sort"

	"ocrlab/internal/model"
)

type DetectionResult struct {
	Candidates []model.CandidateBox
	Errors     []model.DetectionError
	Binary     BinaryImage
	Components []Component
}

func Detect(img image.Image, spec model.ImageSpec, threshold int, invert bool, minPixels int, mergeGapRatio, lineTolerance float64, strategy string, probeInvalid bool) DetectionResult {
	binary := Binarize(img, threshold, invert, strategy)
	components := Segment(binary, strategy, minPixels)
	lines := groupLines(components, lineTolerance)
	candidates := make([]model.CandidateBox, 0, len(lines))
	errors := make([]model.DetectionError, 0)
	rawIndex := 0
	for _, line := range lines {
		groups := splitLine(line, mergeGapRatio)
		for _, group := range groups {
			if len(group) == 0 {
				continue
			}
			box := unionComponent(group)
			rawIndex++
			index := rawIndex
			rect := model.Rect{
				X: round6(float64(box.X0) / float64(binary.Width)),
				Y: round6(float64(box.Y0) / float64(binary.Height)),
				W: round6(float64(box.Width()) / float64(binary.Width)),
				H: round6(float64(box.Height()) / float64(binary.Height)),
			}
			if reason := ValidateNormalizedBox(rect); reason != "" {
				errors = append(errors, model.DetectionError{
					Code: "E_BOX_INVALID", Message: reason, ImageID: spec.ImageID, BoxIndex: index,
				})
				continue
			}
			ink := 0
			for _, component := range group {
				ink += component.Pixels
			}
			area := float64(box.Width() * box.Height())
			density := float64(ink) / area
			confidence := 0.55 + 0.25*math.Min(1, float64(len(group))/3) + 0.20*math.Min(1, density/0.35)
			candidates = append(candidates, model.CandidateBox{
				Index:          index,
				Box:            rect,
				Confidence:     round6(math.Min(1, confidence)),
				SourceStrategy: fmt.Sprintf("%s/cc/line_projection", strategy),
				ComponentCount: len(group),
				InkPixels:      ink,
			})
		}
	}
	if probeInvalid {
		invalidIndex := rawIndex + 1
		errors = append(errors, model.DetectionError{
			Code: "E_BOX_INVALID", Message: "diagnostic probe: normalized box extends beyond image bounds",
			ImageID: spec.ImageID, BoxIndex: invalidIndex,
		})
	}
	return DetectionResult{Candidates: candidates, Errors: errors, Binary: binary, Components: components}
}

func ValidateNormalizedBox(rect model.Rect) string {
	const epsilon = 1e-9
	if math.IsNaN(rect.X) || math.IsNaN(rect.Y) || math.IsNaN(rect.W) || math.IsNaN(rect.H) ||
		math.IsInf(rect.X, 0) || math.IsInf(rect.Y, 0) || math.IsInf(rect.W, 0) || math.IsInf(rect.H, 0) {
		return "box contains a non-finite coordinate"
	}
	if rect.W <= epsilon || rect.H <= epsilon {
		return "box width or height is zero"
	}
	if rect.X < -epsilon || rect.Y < -epsilon || rect.X+rect.W > 1+epsilon || rect.Y+rect.H > 1+epsilon {
		return "box extends beyond normalized image bounds"
	}
	return ""
}

type lineGroup struct {
	components []Component
	top        float64
	bottom     float64
	center     float64
	height     float64
}

func groupLines(components []Component, tolerance float64) [][]Component {
	if len(components) == 0 {
		return nil
	}
	sorted := append([]Component(nil), components...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Y0 != sorted[j].Y0 {
			return sorted[i].Y0 < sorted[j].Y0
		}
		return sorted[i].X0 < sorted[j].X0
	})
	lines := make([]lineGroup, 0, 8)
	for _, component := range sorted {
		best := -1
		bestDistance := math.Inf(1)
		componentCenter := component.CenterY()
		componentHeight := float64(component.Height())
		for i := range lines {
			overlap := math.Min(lines[i].bottom, float64(component.Y1)) - math.Max(lines[i].top, float64(component.Y0))
			minHeight := math.Min(lines[i].height, componentHeight)
			centerDistance := math.Abs(lines[i].center - componentCenter)
			if (overlap > 0 && overlap >= 0.45*minHeight) || centerDistance <= tolerance*math.Max(lines[i].height, componentHeight) {
				if centerDistance < bestDistance {
					best = i
					bestDistance = centerDistance
				}
			}
		}
		if best < 0 {
			lines = append(lines, lineGroup{
				components: []Component{component},
				top:        float64(component.Y0),
				bottom:     float64(component.Y1),
				center:     componentCenter,
				height:     componentHeight,
			})
			continue
		}
		line := &lines[best]
		line.components = append(line.components, component)
		line.top = math.Min(line.top, float64(component.Y0))
		line.bottom = math.Max(line.bottom, float64(component.Y1))
		line.center = (line.top + line.bottom) / 2
		line.height = line.bottom - line.top
	}
	sort.SliceStable(lines, func(i, j int) bool {
		if math.Abs(lines[i].top-lines[j].top) > 0.5 {
			return lines[i].top < lines[j].top
		}
		leftI, leftJ := minX(lines[i].components), minX(lines[j].components)
		return leftI < leftJ
	})
	result := make([][]Component, 0, len(lines))
	for _, line := range lines {
		sort.SliceStable(line.components, func(i, j int) bool {
			if line.components[i].X0 != line.components[j].X0 {
				return line.components[i].X0 < line.components[j].X0
			}
			return line.components[i].Y0 < line.components[j].Y0
		})
		result = append(result, line.components)
	}
	return result
}

func splitLine(line []Component, gapRatio float64) [][]Component {
	if len(line) == 0 {
		return nil
	}
	widths := make([]int, len(line))
	for i, component := range line {
		widths[i] = component.Width()
	}
	sort.Ints(widths)
	median := float64(widths[len(widths)/2])
	threshold := gapRatio * median
	groups := make([][]Component, 0, 1)
	current := []Component{line[0]}
	currentRight := line[0].X1
	for _, component := range line[1:] {
		gap := float64(component.X0 - currentRight)
		if gap > threshold {
			groups = append(groups, current)
			current = nil
		}
		current = append(current, component)
		if component.X1 > currentRight {
			currentRight = component.X1
		}
	}
	groups = append(groups, current)
	return groups
}

func unionComponent(group []Component) Component {
	union := Component{X0: group[0].X0, Y0: group[0].Y0, X1: group[0].X1, Y1: group[0].Y1}
	for _, component := range group[1:] {
		if component.X0 < union.X0 {
			union.X0 = component.X0
		}
		if component.Y0 < union.Y0 {
			union.Y0 = component.Y0
		}
		if component.X1 > union.X1 {
			union.X1 = component.X1
		}
		if component.Y1 > union.Y1 {
			union.Y1 = component.Y1
		}
	}
	for _, component := range group {
		union.Pixels += component.Pixels
	}
	return union
}

func minX(components []Component) int {
	value := components[0].X0
	for _, component := range components[1:] {
		if component.X0 < value {
			value = component.X0
		}
	}
	return value
}

func round6(value float64) float64 {
	return math.Round(value*1e6) / 1e6
}
