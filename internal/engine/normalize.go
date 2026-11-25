package engine

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"layoutread/internal/model"
)

const coordinateEpsilon = 1e-9

type pixelBox struct {
	Index      int
	X          int
	Y          int
	W          int
	H          int
	FontTier   int
	Text       string
	Lang       string
	ColumnHint int
	Kind       string
}

func (b pixelBox) right() int  { return b.X + b.W }
func (b pixelBox) bottom() int { return b.Y + b.H }

// Normalize converts normalized coordinates to deterministic pixel coordinates,
// classifies marginal text, and merges adjacent boxes of the same class.
func Normalize(doc model.Document, strategy model.Strategy) (*model.NormalizedArtifact, error) {
	if err := doc.Validate(); err != nil {
		return nil, err
	}
	if err := strategy.Validate(); err != nil {
		return nil, err
	}
	pageW := int(math.Round(doc.Page.Width))
	pageH := int(math.Round(doc.Page.Height))
	contentLeft := int(math.Round(strategy.MarginXPX))
	contentRight := pageW - contentLeft
	if pageW <= 0 || pageH <= 0 || contentLeft < 0 || contentRight <= contentLeft {
		return nil, model.NewError(model.CodeStrategyInvalid, "page margins leave no effective content width", doc.DocID)
	}

	boxes := make([]pixelBox, 0, len(doc.Boxes))
	diagnostics := make([]model.Diagnostic, 0)
	invalid := make([]model.Diagnostic, 0)
	for _, box := range doc.Boxes {
		boxDiag, boxErr := validateAndConvertBox(doc, box, pageW, pageH)
		if boxErr != nil {
			invalid = append(invalid, *boxErr)
			continue
		}
		boxDiag.Kind = classifyKind(boxDiag, pageH, strategy)
		if boxDiag.Kind != model.KindBody {
			diagnostics = append(diagnostics, model.Diagnostic{
				Code:        "MARGINAL_TEXT_CLASSIFIED",
				Stage:       model.StageNormalize,
				DocID:       doc.DocID,
				BoxIndex:    model.IndexPtr(box.Index),
				Message:     fmt.Sprintf("box %d classified as %s", box.Index, boxDiag.Kind),
				Recoverable: true,
			})
		}
		boxes = append(boxes, boxDiag)
	}
	if len(invalid) > 0 {
		first := invalid[0]
		return &model.NormalizedArtifact{
			DocID:        doc.DocID,
			Page:         doc.Page,
			ContentLeft:  contentLeft,
			ContentRight: contentRight,
			Diagnostics:  append(diagnostics, invalid...),
		}, &model.Error{
			Code:        model.CodeBoxInvalid,
			Message:     "one or more boxes have invalid coordinates or dimensions",
			DocID:       doc.DocID,
			BoxIndex:    first.BoxIndex,
			Stage:       model.StageNormalize,
			Diagnostics: append(diagnostics, invalid...),
		}
	}

	groups := mergeBoxGroups(boxes, strategy)
	merged := make([]model.MergedBox, 0, len(groups))
	for _, group := range groups {
		result, groupDiagnostics := mergeGroup(doc.DocID, group)
		merged = append(merged, result)
		diagnostics = append(diagnostics, groupDiagnostics...)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Y != merged[j].Y {
			return merged[i].Y < merged[j].Y
		}
		if merged[i].X != merged[j].X {
			return merged[i].X < merged[j].X
		}
		return merged[i].BlockID < merged[j].BlockID
	})

	return &model.NormalizedArtifact{
		DocID:        doc.DocID,
		Page:         doc.Page,
		ContentLeft:  contentLeft,
		ContentRight: contentRight,
		Boxes:        merged,
		Diagnostics:  diagnostics,
	}, nil
}

func validateAndConvertBox(doc model.Document, box model.TextBox, pageW, pageH int) (pixelBox, *model.Diagnostic) {
	invalid := func(message string) (pixelBox, *model.Diagnostic) {
		return pixelBox{}, &model.Diagnostic{
			Code:        model.CodeBoxInvalid,
			Stage:       model.StageNormalize,
			DocID:       doc.DocID,
			BoxIndex:    model.IndexPtr(box.Index),
			Message:     message,
			Recoverable: true,
		}
	}
	if !finite(box.X) || !finite(box.Y) || !finite(box.W) || !finite(box.H) {
		return invalid("box coordinates must be finite")
	}
	if box.W <= 0 || box.H <= 0 {
		return invalid("box width and height must be positive")
	}
	if box.X < 0-coordinateEpsilon || box.Y < 0-coordinateEpsilon ||
		box.X+box.W > 1+coordinateEpsilon || box.Y+box.H > 1+coordinateEpsilon {
		return invalid("box normalized coordinates are outside [0,1]")
	}
	x0 := int(math.Round(box.X * float64(pageW)))
	x1 := int(math.Round((box.X + box.W) * float64(pageW)))
	y0 := int(math.Round(box.Y * float64(pageH)))
	y1 := int(math.Round((box.Y + box.H) * float64(pageH)))
	if x0 < 0 {
		x0 = 0
	}
	if y0 < 0 {
		y0 = 0
	}
	if x1 > pageW {
		x1 = pageW
	}
	if y1 > pageH {
		y1 = pageH
	}
	if x1 <= x0 || y1 <= y0 {
		return invalid("box has zero pixel width or height")
	}
	return pixelBox{
		Index:      box.Index,
		X:          x0,
		Y:          y0,
		W:          x1 - x0,
		H:          y1 - y0,
		FontTier:   box.FontTier,
		Text:       box.Text,
		Lang:       box.Lang,
		ColumnHint: box.ColumnHint,
	}, nil
}

func classifyKind(box pixelBox, pageH int, strategy model.Strategy) string {
	if box.FontTier > strategy.HeaderFooterMaxFontTier {
		return model.KindBody
	}
	margin := int(math.Round(strategy.MarginYPX))
	topDistance := box.Y
	bottomDistance := pageH - box.bottom()
	inHeader := topDistance <= margin
	inFooter := bottomDistance <= margin
	if inHeader && inFooter {
		if topDistance <= bottomDistance {
			return model.KindHeader
		}
		return model.KindFooter
	}
	if inHeader {
		return model.KindHeader
	}
	if inFooter {
		return model.KindFooter
	}
	return model.KindBody
}

type disjointSet struct {
	parent []int
	rank   []int
}

func newDisjointSet(n int) *disjointSet {
	parent := make([]int, n)
	rank := make([]int, n)
	for i := range parent {
		parent[i] = i
	}
	return &disjointSet{parent: parent, rank: rank}
}

func (d *disjointSet) find(x int) int {
	if d.parent[x] != x {
		d.parent[x] = d.find(d.parent[x])
	}
	return d.parent[x]
}

func (d *disjointSet) union(a, b int) {
	ra, rb := d.find(a), d.find(b)
	if ra == rb {
		return
	}
	if d.rank[ra] < d.rank[rb] {
		ra, rb = rb, ra
	}
	d.parent[rb] = ra
	if d.rank[ra] == d.rank[rb] {
		d.rank[ra]++
	}
}

func mergeBoxGroups(boxes []pixelBox, strategy model.Strategy) [][]pixelBox {
	if len(boxes) == 0 {
		return nil
	}
	sorted := append([]pixelBox(nil), boxes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Index < sorted[j].Index })
	dsu := newDisjointSet(len(sorted))
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if shouldMerge(sorted[i], sorted[j], strategy.MergeGapPX) {
				dsu.union(i, j)
			}
		}
	}
	groupIndex := make(map[int]int)
	groups := make([][]pixelBox, 0)
	for i, box := range sorted {
		root := dsu.find(i)
		idx, ok := groupIndex[root]
		if !ok {
			idx = len(groups)
			groupIndex[root] = idx
			groups = append(groups, nil)
		}
		groups[idx] = append(groups[idx], box)
	}
	sort.Slice(groups, func(i, j int) bool {
		minI := groups[i][0].Index
		minJ := groups[j][0].Index
		for _, box := range groups[i] {
			if box.Index < minI {
				minI = box.Index
			}
		}
		for _, box := range groups[j] {
			if box.Index < minJ {
				minJ = box.Index
			}
		}
		return minI < minJ
	})
	return groups
}

func shouldMerge(a, b pixelBox, threshold float64) bool {
	if a.Kind != b.Kind {
		return false
	}
	if a.ColumnHint == -1 || b.ColumnHint == -1 {
		return false
	}
	if a.ColumnHint >= 0 && b.ColumnHint >= 0 && a.ColumnHint != b.ColumnHint {
		return false
	}
	xGap := maxInt(0, maxInt(a.X, b.X)-minInt(a.right(), b.right()))
	yGap := maxInt(0, maxInt(a.Y, b.Y)-minInt(a.bottom(), b.bottom()))
	verticalOverlap := minInt(a.bottom(), b.bottom()) - maxInt(a.Y, b.Y)
	horizontalOverlap := minInt(a.right(), b.right()) - maxInt(a.X, b.X)
	horizontalAdjacent := verticalOverlap > 0 && verticalOverlap*2 >= minInt(a.H, b.H) && float64(xGap) < threshold
	verticalAdjacent := horizontalOverlap > 0 && horizontalOverlap*2 >= minInt(a.W, b.W) && float64(yGap) < threshold
	return horizontalAdjacent || verticalAdjacent
}

func mergeGroup(docID string, group []pixelBox) (model.MergedBox, []model.Diagnostic) {
	sort.Slice(group, func(i, j int) bool {
		if group[i].Y != group[j].Y {
			return group[i].Y < group[j].Y
		}
		if group[i].X != group[j].X {
			return group[i].X < group[j].X
		}
		return group[i].Index < group[j].Index
	})
	minX, minY := group[0].X, group[0].Y
	maxX, maxY := group[0].right(), group[0].bottom()
	sourceIndices := make([]int, 0, len(group))
	parts := make([]string, 0, len(group))
	langs := make([]string, 0, len(group))
	seenLang := map[string]struct{}{}
	allHintsEqual := true
	hint := group[0].ColumnHint
	fontTier := group[0].FontTier
	for _, box := range group {
		minX = minInt(minX, box.X)
		minY = minInt(minY, box.Y)
		maxX = maxInt(maxX, box.right())
		maxY = maxInt(maxY, box.bottom())
		sourceIndices = append(sourceIndices, box.Index)
		if strings.TrimSpace(box.Text) != "" {
			parts = append(parts, strings.TrimSpace(box.Text))
		}
		if _, ok := seenLang[box.Lang]; !ok {
			seenLang[box.Lang] = struct{}{}
			langs = append(langs, box.Lang)
		}
		if box.ColumnHint != hint {
			allHintsEqual = false
		}
		if box.FontTier > fontTier {
			fontTier = box.FontTier
		}
	}
	sort.Ints(sourceIndices)
	if !allHintsEqual {
		hint = -2
	}
	diagnostics := make([]model.Diagnostic, 0, 1)
	if len(group) > 1 {
		diagnostics = append(diagnostics, model.Diagnostic{
			Code:        "ADJACENT_BOXES_MERGED",
			Stage:       model.StageNormalize,
			DocID:       docID,
			Message:     "merged source boxes " + joinInts(sourceIndices),
			Recoverable: true,
		})
	}
	return model.MergedBox{
		BlockID:       fmt.Sprintf("%s-b%s", docID, leftPad(sourceIndices[0])),
		SourceIndices: sourceIndices,
		X:             minX,
		Y:             minY,
		W:             maxX - minX,
		H:             maxY - minY,
		FontTier:      fontTier,
		Text:          strings.Join(parts, " "),
		Lang:          strings.Join(langs, "+"),
		Kind:          group[0].Kind,
		ColumnHint:    hint,
		Column:        -1,
	}, diagnostics
}

func leftPad(n int) string {
	s := strconv.Itoa(n)
	for len(s) < 4 {
		s = "0" + s
	}
	return s
}

func joinInts(values []int) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = strconv.Itoa(value)
	}
	return strings.Join(parts, ",")
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
