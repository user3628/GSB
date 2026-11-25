package engine

import (
	"sort"
	"unicode/utf8"

	"layoutread/internal/model"
)

func Summarize(columns *model.ColumnArtifact, order *model.OrderArtifact) (*model.SummaryArtifact, error) {
	if columns == nil || order == nil {
		return nil, model.NewError(model.CodeInternal, "summary inputs are nil", "")
	}
	if len(order.Blocks) == 0 {
		err := model.NewError(model.CodeOrderConflict, "cannot summarize an empty order", columns.DocID)
		err.Stage = model.StageSummary
		return nil, err
	}
	stats := make([]model.ColumnStat, len(columns.Columns))
	for i, column := range columns.Columns {
		stat := model.ColumnStat{
			ColumnIndex: column.Index,
			X0:          column.X0,
			X1:          column.X1,
			Width:       column.X1 - column.X0,
		}
		var occupiedArea int64
		for _, box := range columns.Boxes {
			if box.Column != column.Index {
				continue
			}
			stat.BoxCount++
			stat.CharCount += utf8.RuneCountInString(box.Text)
			occupiedArea += int64(box.W) * int64(box.H)
		}
		columnArea := int64(stat.Width) * int64(columns.Page.Height)
		if columnArea > 0 {
			stat.AreaRatio = model.Round6(float64(occupiedArea) / float64(columnArea))
		}
		stats[i] = stat
	}
	sequence := make([]model.BlockSequenceItem, len(order.Blocks))
	for i, block := range order.Blocks {
		sequence[i] = model.BlockSequenceItem{
			Order:         i,
			BlockID:       block.BlockID,
			Column:        block.Column,
			Kind:          block.Kind,
			FullSpan:      block.FullSpan,
			X:             block.X,
			Y:             block.Y,
			W:             block.W,
			H:             block.H,
			Text:          block.Text,
			Lang:          block.Lang,
			FontTier:      block.FontTier,
			SourceIndices: append([]int(nil), block.SourceIndices...),
		}
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].ColumnIndex < stats[j].ColumnIndex })
	diagnostics := append([]model.Diagnostic(nil), order.Diagnostics...)
	return &model.SummaryArtifact{
		DocID:            columns.DocID,
		Page:             columns.Page,
		BlockSequence:    sequence,
		Columns:          stats,
		Diagnostics:      diagnostics,
		OrderFingerprint: order.Fingerprint(),
	}, nil
}

func BuildDocumentResult(doc model.Document, strategy model.Strategy, stages []model.StageFingerprint, order *model.OrderArtifact, summary *model.SummaryArtifact) model.DocumentResult {
	result := model.DocumentResult{
		DocID:               doc.DocID,
		ContentFingerprint:  doc.ContentFingerprint(),
		StrategyVersion:     strategy.Version,
		StrategyFingerprint: strategy.Fingerprint(),
		StageFingerprints:   stages,
		Blocks:              append([]model.OrderedBlock(nil), order.Blocks...),
		Columns:             make([]model.Column, 0),
		ColumnStats:         append([]model.ColumnStat(nil), summary.Columns...),
		Confidences:         append([]model.OrderConfidence(nil), order.Confidences...),
		Diagnostics:         append([]model.Diagnostic(nil), summary.Diagnostics...),
	}
	// Recover the canonical column intervals from the summary's per-column stats.
	// They are reconstructed here without retaining a separate artifact.
	for _, stat := range summary.Columns {
		result.Columns = append(result.Columns, model.Column{
			Index:    stat.ColumnIndex,
			X0:       stat.X0,
			X1:       stat.X1,
			BoxCount: stat.BoxCount,
		})
	}
	result.ResultFingerprint = model.ResultFingerprint(result)
	return result
}
