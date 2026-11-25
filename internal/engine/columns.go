package engine

import (
	"fmt"
	"math"
	"sort"

	"layoutread/internal/model"
)

type startCluster struct {
	MinStart int
	MaxStart int
	MinBin   int
	MaxBin   int
	Support  int
	MaxRight int
	BoxPos   []int
}

// InferColumns uses a histogram of box left edges plus each cluster's width
// extent. It never snaps an unassignable box into a neighboring column.
func InferColumns(normalized *model.NormalizedArtifact, strategy model.Strategy) (*model.ColumnArtifact, error) {
	if normalized == nil {
		return nil, model.NewError(model.CodeInternal, "normalized artifact is nil", "")
	}
	if err := strategy.Validate(); err != nil {
		return nil, err
	}
	pageW := int(math.Round(normalized.Page.Width))
	contentLeft := normalized.ContentLeft
	contentRight := normalized.ContentRight
	if contentLeft < 0 || contentRight > pageW || contentRight <= contentLeft {
		return nil, model.NewError(model.CodeColumnUnassigned, "declared effective width is invalid", normalized.DocID)
	}
	contentWidth := contentRight - contentLeft
	boxes := append([]model.MergedBox(nil), normalized.Boxes...)
	diagnostics := append([]model.Diagnostic(nil), normalized.Diagnostics...)
	fullSpanIDs := make([]string, 0)
	candidatePositions := make([]int, 0)
	spanThreshold := int(math.Ceil(strategy.SpanWidthRatio * float64(contentWidth)))

	for i := range boxes {
		box := &boxes[i]
		if box.Kind == model.KindHeader || box.Kind == model.KindFooter {
			box.FullSpan = true
			box.Column = -1
			fullSpanIDs = append(fullSpanIDs, box.BlockID)
			continue
		}
		if box.W >= spanThreshold {
			box.FullSpan = true
			box.Column = -1
			fullSpanIDs = append(fullSpanIDs, box.BlockID)
			continue
		}
		if box.X < contentLeft || box.Right() > contentRight {
			return columnError(normalized, *box, "box lies outside the declared effective page width", diagnostics)
		}
		candidatePositions = append(candidatePositions, i)
	}
	sort.Strings(fullSpanIDs)

	if len(candidatePositions) == 0 {
		hasBodySpan := false
		for _, box := range boxes {
			if box.Kind == model.KindBody && box.FullSpan {
				hasBodySpan = true
				break
			}
		}
		if !hasBodySpan {
			err := model.NewError(model.CodeColumnUnassigned, "no content column could be inferred", normalized.DocID)
			err.Stage = model.StageColumns
			err.Diagnostics = diagnostics
			return &model.ColumnArtifact{
				DocID:        normalized.DocID,
				Page:         normalized.Page,
				ContentLeft:  contentLeft,
				ContentRight: contentRight,
				Boxes:        boxes,
				Diagnostics:  diagnostics,
			}, err
		}
		// A page containing only full-span body text still has one visual column.
		columns := []model.Column{{Index: 0, X0: contentLeft, X1: contentRight}}
		return &model.ColumnArtifact{
			DocID:            normalized.DocID,
			Page:             normalized.Page,
			ContentLeft:      contentLeft,
			ContentRight:     contentRight,
			Columns:          columns,
			Boxes:            boxes,
			FullSpanBlockIDs: fullSpanIDs,
			Diagnostics:      diagnostics,
		}, nil
	}

	binWidth := int(math.Round(strategy.ColumnHistogramBinPX))
	if binWidth < 1 {
		binWidth = 1
	}
	binCount := (contentWidth + binWidth - 1) / binWidth
	if binCount < 1 {
		binCount = 1
	}
	hist := make([]int, binCount)
	boxBin := make(map[int]int, len(candidatePositions))
	for _, pos := range candidatePositions {
		bin := (boxes[pos].X - contentLeft) / binWidth
		if bin < 0 {
			bin = 0
		}
		if bin >= binCount {
			bin = binCount - 1
		}
		hist[bin]++
		boxBin[pos] = bin
	}

	occupied := make([]int, 0)
	for bin, count := range hist {
		if count > 0 {
			occupied = append(occupied, bin)
		}
	}
	if len(occupied) == 0 {
		err := model.NewError(model.CodeColumnUnassigned, "column histogram has no occupied bins", normalized.DocID)
		err.Stage = model.StageColumns
		err.Diagnostics = diagnostics
		return nil, err
	}
	clusterGap := strategy.ColumnGapPX
	if clusterGap < float64(binWidth*2) {
		clusterGap = float64(binWidth * 2)
	}
	if suggested := float64(contentWidth) * 0.06; suggested > clusterGap {
		clusterGap = suggested
	}

	type binGroup struct {
		minBin int
		maxBin int
	}
	groups := make([]binGroup, 0)
	current := binGroup{minBin: occupied[0], maxBin: occupied[0]}
	for _, bin := range occupied[1:] {
		gap := float64(bin-current.maxBin-1) * float64(binWidth)
		if gap <= clusterGap {
			current.maxBin = bin
			continue
		}
		groups = append(groups, current)
		current = binGroup{minBin: bin, maxBin: bin}
	}
	groups = append(groups, current)

	clusters := make([]startCluster, len(groups))
	for i, group := range groups {
		clusters[i].MinBin = group.minBin
		clusters[i].MaxBin = group.maxBin
		clusters[i].MinStart = contentLeft + group.minBin*binWidth
		clusters[i].MaxStart = contentLeft + group.maxBin*binWidth
		clusters[i].MaxRight = clusters[i].MaxStart
	}
	for _, pos := range candidatePositions {
		clusterIndex := -1
		for i := range clusters {
			if boxBin[pos] >= clusters[i].MinBin && boxBin[pos] <= clusters[i].MaxBin {
				clusterIndex = i
				break
			}
		}
		if clusterIndex < 0 {
			return columnError(normalized, boxes[pos], "box start could not be grouped into a column", diagnostics)
		}
		cluster := &clusters[clusterIndex]
		cluster.Support++
		cluster.BoxPos = append(cluster.BoxPos, pos)
		cluster.MaxRight = maxInt(cluster.MaxRight, boxes[pos].Right())
		if boxes[pos].X < cluster.MinStart {
			cluster.MinStart = boxes[pos].X
		}
	}

	// Very low-support clusters are folded into their nearest geometric neighbor
	// only when they are close enough to be typographic outliers, never across a
	// genuine column gutter.
	filtered := make([]startCluster, 0, len(clusters))
	for _, cluster := range clusters {
		if cluster.Support < strategy.ColumnMinSupport && len(clusters) > 1 {
			nearest := -1
			nearestDistance := int(^uint(0) >> 1)
			for i := range clusters {
				if clusters[i].MinBin == cluster.MinBin && clusters[i].MaxBin == cluster.MaxBin {
					continue
				}
				distance := absInt(clusters[i].MinStart - cluster.MinStart)
				if distance < nearestDistance {
					nearest = i
					nearestDistance = distance
				}
			}
			if nearest >= 0 && float64(nearestDistance) <= clusterGap {
				clusters[nearest].Support += cluster.Support
				clusters[nearest].BoxPos = append(clusters[nearest].BoxPos, cluster.BoxPos...)
				clusters[nearest].MaxRight = maxInt(clusters[nearest].MaxRight, cluster.MaxRight)
				clusters[nearest].MinStart = minInt(clusters[nearest].MinStart, cluster.MinStart)
				clusters[nearest].MaxStart = maxInt(clusters[nearest].MaxStart, cluster.MaxStart)
				continue
			}
		}
		filtered = append(filtered, cluster)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].MinStart != filtered[j].MinStart {
			return filtered[i].MinStart < filtered[j].MinStart
		}
		return filtered[i].Support > filtered[j].Support
	})
	if len(filtered) == 0 {
		return columnError(normalized, boxes[candidatePositions[0]], "no column has sufficient support", diagnostics)
	}

	columns := make([]model.Column, len(filtered))
	for i := range filtered {
		columns[i].Index = i
		switch {
		case i == 0:
			columns[i].X0 = contentLeft
		default:
			boundary := (filtered[i-1].MaxRight + filtered[i].MinStart) / 2
			if boundary <= columns[i-1].X0 {
				boundary = filtered[i].MinStart
			}
			columns[i].X0 = boundary
		}
		if i == len(filtered)-1 {
			columns[i].X1 = contentRight
		}
	}
	for i := 0; i+1 < len(columns); i++ {
		columns[i].X1 = columns[i+1].X0
	}
	if err := validateColumns(columns, contentLeft, contentRight, normalized.DocID); err != nil {
		return nil, err
	}

	assignedCount := make([]int, len(columns))
	for i := range boxes {
		box := &boxes[i]
		if box.FullSpan {
			continue
		}
		found := -1
		for colIndex, column := range columns {
			if box.X >= column.X0 && box.Right() <= column.X1 {
				found = colIndex
				break
			}
		}
		if found < 0 {
			return columnError(normalized, *box, "box crosses or cannot be assigned to a non-overlapping column", diagnostics)
		}
		box.Column = columns[found].Index
		assignedCount[found]++
		if box.ColumnHint >= 0 && box.ColumnHint != box.Column {
			diagnostics = append(diagnostics, model.Diagnostic{
				Code:        "COLUMN_HINT_OVERRIDDEN",
				Stage:       model.StageColumns,
				DocID:       normalized.DocID,
				BoxIndex:    model.IndexPtr(box.SourceIndices[0]),
				BlockID:     box.BlockID,
				Message:     fmt.Sprintf("geometry assigned column %d instead of hint %d", box.Column, box.ColumnHint),
				Recoverable: true,
			})
		}
	}
	for i := range columns {
		columns[i].BoxCount = assignedCount[i]
	}

	return &model.ColumnArtifact{
		DocID:            normalized.DocID,
		Page:             normalized.Page,
		ContentLeft:      contentLeft,
		ContentRight:     contentRight,
		Columns:          columns,
		Boxes:            boxes,
		FullSpanBlockIDs: fullSpanIDs,
		Diagnostics:      diagnostics,
	}, nil
}

func validateColumns(columns []model.Column, contentLeft, contentRight int, docID string) error {
	if len(columns) == 0 {
		return model.NewError(model.CodeColumnUnassigned, "column count is zero", docID)
	}
	if columns[0].X0 != contentLeft {
		return model.NewError(model.CodeColumnUnassigned, "columns do not cover the left edge of the effective width", docID)
	}
	if columns[len(columns)-1].X1 != contentRight {
		return model.NewError(model.CodeColumnUnassigned, "columns do not cover the right edge of the effective width", docID)
	}
	for i, column := range columns {
		if column.Index != i || column.X1 <= column.X0 {
			return model.NewError(model.CodeColumnUnassigned, "column interval is empty or out of order", docID)
		}
		if i > 0 && columns[i-1].X1 != column.X0 {
			return model.NewError(model.CodeColumnUnassigned, "column intervals overlap or leave a gap", docID)
		}
	}
	return nil
}

func columnError(normalized *model.NormalizedArtifact, box model.MergedBox, message string, diagnostics []model.Diagnostic) (*model.ColumnArtifact, error) {
	boxIndex := 0
	if len(box.SourceIndices) > 0 {
		boxIndex = box.SourceIndices[0]
	}
	diag := model.Diagnostic{
		Code:        model.CodeColumnUnassigned,
		Stage:       model.StageColumns,
		DocID:       normalized.DocID,
		BoxIndex:    model.IndexPtr(boxIndex),
		BlockID:     box.BlockID,
		Message:     message,
		Recoverable: true,
	}
	diagnostics = append(diagnostics, diag)
	err := &model.Error{
		Code:        model.CodeColumnUnassigned,
		Message:     message,
		DocID:       normalized.DocID,
		BoxIndex:    model.IndexPtr(boxIndex),
		Stage:       model.StageColumns,
		Diagnostics: diagnostics,
	}
	return &model.ColumnArtifact{
		DocID:        normalized.DocID,
		Page:         normalized.Page,
		ContentLeft:  normalized.ContentLeft,
		ContentRight: normalized.ContentRight,
		Boxes:        append([]model.MergedBox(nil), normalized.Boxes...),
		Diagnostics:  diagnostics,
	}, err
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
