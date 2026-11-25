package engine

import (
	"fmt"
	"sort"

	"layoutread/internal/model"
)

// GenerateOrder places headers first, footers last, and body blocks into
// vertical bands separated by full-span blocks. Within each band, columns are
// read left-to-right and blocks within a column top-to-bottom.
func GenerateOrder(columns *model.ColumnArtifact, strategy model.Strategy) (*model.OrderArtifact, error) {
	if columns == nil {
		return nil, model.NewError(model.CodeInternal, "column artifact is nil", "")
	}
	if err := strategy.Validate(); err != nil {
		return nil, err
	}
	diagnostics := append([]model.Diagnostic(nil), columns.Diagnostics...)
	headers := make([]model.MergedBox, 0)
	footers := make([]model.MergedBox, 0)
	bodySpans := make([]model.MergedBox, 0)
	bodyNormal := make([]model.MergedBox, 0)
	for _, box := range columns.Boxes {
		switch box.Kind {
		case model.KindHeader:
			headers = append(headers, box)
		case model.KindFooter:
			footers = append(footers, box)
		default:
			if box.FullSpan || box.Column < 0 {
				bodySpans = append(bodySpans, box)
			} else {
				bodyNormal = append(bodyNormal, box)
			}
		}
	}
	if len(columns.Boxes) == 0 {
		err := model.NewError(model.CodeOrderConflict, "document has no blocks to order", columns.DocID)
		err.Stage = model.StageOrder
		err.Diagnostics = diagnostics
		return &model.OrderArtifact{DocID: columns.DocID, Diagnostics: diagnostics}, err
	}
	if err := validateUniqueBoxes(columns); err != nil {
		return &model.OrderArtifact{DocID: columns.DocID, Diagnostics: diagnostics}, err
	}

	lessBox := func(a, b model.MergedBox) bool {
		if a.Y != b.Y {
			return a.Y < b.Y
		}
		if a.X != b.X {
			return a.X < b.X
		}
		return a.BlockID < b.BlockID
	}
	sort.Slice(headers, func(i, j int) bool { return lessBox(headers[i], headers[j]) })
	sort.Slice(footers, func(i, j int) bool { return lessBox(footers[i], footers[j]) })
	sort.Slice(bodySpans, func(i, j int) bool { return lessBox(bodySpans[i], bodySpans[j]) })

	type bandBlock struct {
		box     model.MergedBox
		segment int
	}
	bandBlocks := make([]bandBlock, 0, len(bodyNormal))
	for _, box := range bodyNormal {
		segment := 0
		for _, span := range bodySpans {
			if span.Bottom() <= box.Y {
				segment++
				continue
			}
			if box.Bottom() <= span.Y {
				break
			}
			// The box vertically intersects the full-span block.
			if strategy.CrossColumnPriority == model.PrioritySpanFirst {
				segment++
			}
		}
		bandBlocks = append(bandBlocks, bandBlock{box: box, segment: segment})
	}
	sort.Slice(bandBlocks, func(i, j int) bool {
		a, b := bandBlocks[i], bandBlocks[j]
		if a.segment != b.segment {
			return a.segment < b.segment
		}
		if a.box.Column != b.box.Column {
			return a.box.Column < b.box.Column
		}
		if a.box.Y != b.box.Y {
			return a.box.Y < b.box.Y
		}
		if a.box.X != b.box.X {
			return a.box.X < b.box.X
		}
		return a.box.BlockID < b.box.BlockID
	})

	desired := make([]model.MergedBox, 0, len(columns.Boxes))
	desired = append(desired, headers...)
	position := 0
	for segment := 0; segment <= len(bodySpans); segment++ {
		for position < len(bandBlocks) && bandBlocks[position].segment == segment {
			desired = append(desired, bandBlocks[position].box)
			position++
		}
		if segment < len(bodySpans) {
			desired = append(desired, bodySpans[segment])
		}
	}
	// Overlapping full spans can produce a segment beyond len(bodySpans); keep
	// any remaining deterministic blocks rather than dropping them.
	for position < len(bandBlocks) {
		desired = append(desired, bandBlocks[position].box)
		position++
	}
	desired = append(desired, footers...)

	ordered, conflict := acyclicSequence(desired)
	if conflict != nil {
		pair := conflictPair(columns.DocID, conflict[0], conflict[1])
		err := &model.Error{
			Code:        model.CodeOrderConflict,
			Message:     "reading-order precedence graph contains a cycle",
			DocID:       columns.DocID,
			Stage:       model.StageOrder,
			Conflict:    []model.BoxPair{pair},
			Diagnostics: diagnostics,
		}
		return &model.OrderArtifact{DocID: columns.DocID, Diagnostics: diagnostics}, err
	}

	orderedBlocks := make([]model.OrderedBlock, len(ordered))
	for i, box := range ordered {
		orderedBlocks[i] = model.OrderedBlock{MergedBox: box, Order: i}
	}
	confidences := calculateConfidences(orderedBlocks, columns.Page, strategy)
	if len(confidences) > 0 {
		allZero := true
		for _, confidence := range confidences {
			if confidence.Confidence != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			pair := conflictPair(columns.DocID, orderedBlocks[0].MergedBox, orderedBlocks[1].MergedBox)
			err := &model.Error{
				Code:        model.CodeOrderConflict,
				Message:     "all adjacent reading-order confidences are zero",
				DocID:       columns.DocID,
				Stage:       model.StageOrder,
				Conflict:    []model.BoxPair{pair},
				Diagnostics: diagnostics,
			}
			return &model.OrderArtifact{
				DocID:       columns.DocID,
				Blocks:      orderedBlocks,
				Confidences: confidences,
				Diagnostics: diagnostics,
			}, err
		}
	}
	for _, span := range bodySpans {
		diagnostics = append(diagnostics, model.Diagnostic{
			Code:        "FULL_SPAN_BLOCK_ORDERED",
			Stage:       model.StageOrder,
			DocID:       columns.DocID,
			BlockID:     span.BlockID,
			Message:     fmt.Sprintf("full-span block positioned by %s priority", strategy.CrossColumnPriority),
			Recoverable: true,
		})
	}
	return &model.OrderArtifact{
		DocID:       columns.DocID,
		Blocks:      orderedBlocks,
		Confidences: confidences,
		Diagnostics: diagnostics,
	}, nil
}

func validateUniqueBoxes(columns *model.ColumnArtifact) error {
	seenBlocks := map[string]struct{}{}
	seenSources := map[int]string{}
	for _, box := range columns.Boxes {
		if _, ok := seenBlocks[box.BlockID]; ok {
			diag := model.Diagnostic{
				Code: model.CodeOrderConflict, Stage: model.StageOrder, DocID: columns.DocID,
				BlockID: box.BlockID, Message: "duplicate block id", Recoverable: true,
			}
			return &model.Error{
				Code: model.CodeOrderConflict, Message: "duplicate block id", DocID: columns.DocID,
				Stage: model.StageOrder, Diagnostics: []model.Diagnostic{diag},
			}
		}
		seenBlocks[box.BlockID] = struct{}{}
		for _, source := range box.SourceIndices {
			if other, ok := seenSources[source]; ok {
				pair := model.BoxPair{DocID: columns.DocID, BoxA: source, BoxB: source}
				diag := model.Diagnostic{
					Code: model.CodeOrderConflict, Stage: model.StageOrder, DocID: columns.DocID,
					BoxIndex: model.IndexPtr(source), Message: "source box appears in both " + other + " and " + box.BlockID,
					Recoverable: true,
				}
				return &model.Error{
					Code: model.CodeOrderConflict, Message: "source box would be ordered more than once", DocID: columns.DocID,
					BoxIndex: model.IndexPtr(source), Stage: model.StageOrder, Conflict: []model.BoxPair{pair},
					Diagnostics: []model.Diagnostic{diag},
				}
			}
			seenSources[source] = box.BlockID
		}
	}
	return nil
}

// acyclicSequence validates the desired sequence using a precedence graph in
// which every item must precede each later item. A malformed artifact that
// introduces contradictory keys is reported instead of silently ordered.
func acyclicSequence(desired []model.MergedBox) ([]model.MergedBox, []model.MergedBox) {
	n := len(desired)
	seen := make(map[string]int, n)
	for i, box := range desired {
		if previous, exists := seen[box.BlockID]; exists {
			return nil, []model.MergedBox{desired[previous], box}
		}
		seen[box.BlockID] = i
	}
	indegree := make([]int, n)
	adjacency := make([][]int, n)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			adjacency[i] = append(adjacency[i], j)
			indegree[j]++
		}
	}
	ready := make([]int, 0)
	for i, degree := range indegree {
		if degree == 0 {
			ready = append(ready, i)
		}
	}
	ordered := make([]model.MergedBox, 0, n)
	for len(ready) > 0 {
		sort.Ints(ready)
		current := ready[0]
		ready = ready[1:]
		ordered = append(ordered, desired[current])
		for _, next := range adjacency[current] {
			indegree[next]--
			if indegree[next] == 0 {
				ready = append(ready, next)
			}
		}
	}
	if len(ordered) != n {
		remaining := make([]model.MergedBox, 0)
		for i, degree := range indegree {
			if degree > 0 {
				remaining = append(remaining, desired[i])
			}
		}
		if len(remaining) == 1 {
			remaining = append(remaining, desired[0])
		}
		if len(remaining) >= 2 {
			return nil, remaining[:2]
		}
		return nil, []model.MergedBox{desired[0], desired[0]}
	}
	return ordered, nil
}

func calculateConfidences(blocks []model.OrderedBlock, page model.Page, strategy model.Strategy) []model.OrderConfidence {
	result := make([]model.OrderConfidence, 0, maxInt(0, len(blocks)-1))
	for i := 0; i+1 < len(blocks); i++ {
		from, to := blocks[i], blocks[i+1]
		gap := to.Y - from.Bottom()
		sameColumn := from.Column >= 0 && to.Column >= 0 && from.Column == to.Column
		columnAffinity := 0.65
		if sameColumn {
			columnAffinity = 1
		} else if from.Column >= 0 && to.Column >= 0 {
			columnAffinity = 0.4
		} else if (from.Kind == model.KindHeader && to.Kind != model.KindHeader) ||
			(from.Kind != model.KindFooter && to.Kind == model.KindFooter) {
			columnAffinity = 0.8
		}
		verticalScore := 0.0
		if gap > 0 {
			verticalScore = model.Clamp01(float64(gap) / (page.Height * 0.08))
		}
		confidence := strategy.ConfidenceVerticalWeight*verticalScore + strategy.ConfidenceColumnWeight*columnAffinity
		result = append(result, model.OrderConfidence{
			FromOrder:      from.Order,
			ToOrder:        to.Order,
			FromBlockID:    from.BlockID,
			ToBlockID:      to.BlockID,
			VerticalGapPX:  gap,
			SameColumn:     sameColumn,
			ColumnAffinity: model.Round6(columnAffinity),
			VerticalScore:  model.Round6(verticalScore),
			ColumnScore:    model.Round6(strategy.ConfidenceColumnWeight * columnAffinity),
			Confidence:     model.Round6(confidence),
		})
	}
	return result
}

func conflictPair(docID string, a, b model.MergedBox) model.BoxPair {
	boxA, boxB := 0, 0
	if len(a.SourceIndices) > 0 {
		boxA = a.SourceIndices[0]
	}
	if len(b.SourceIndices) > 0 {
		boxB = b.SourceIndices[0]
	}
	return model.BoxPair{DocID: docID, BoxA: boxA, BoxB: boxB}
}
