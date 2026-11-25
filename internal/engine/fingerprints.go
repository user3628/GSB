package engine

import "layoutread/internal/model"

type normalizePolicy struct {
	MergeGapPX              float64 `json:"merge_gap_px"`
	MarginXPX               float64 `json:"margin_x_px"`
	MarginYPX               float64 `json:"margin_y_px"`
	HeaderFooterMaxFontTier int     `json:"header_footer_max_font_tier"`
}

type columnPolicy struct {
	ColumnHistogramBinPX float64 `json:"column_histogram_bin_px"`
	ColumnGapPX          float64 `json:"column_gap_px"`
	ColumnMinSupport     int     `json:"column_min_support"`
	SpanWidthRatio       float64 `json:"span_width_ratio"`
}

type orderPolicy struct {
	CrossColumnPriority      string  `json:"cross_column_priority"`
	ConfidenceVerticalWeight float64 `json:"confidence_vertical_weight"`
	ConfidenceColumnWeight   float64 `json:"confidence_column_weight"`
}

func NormalizeInputFingerprint(doc model.Document, strategy model.Strategy) string {
	return model.Digest(struct {
		Document model.Document  `json:"document"`
		Policy   normalizePolicy `json:"policy"`
	}{
		Document: doc,
		Policy: normalizePolicy{
			MergeGapPX:              strategy.MergeGapPX,
			MarginXPX:               strategy.MarginXPX,
			MarginYPX:               strategy.MarginYPX,
			HeaderFooterMaxFontTier: strategy.HeaderFooterMaxFontTier,
		},
	})
}

func ColumnInputFingerprint(normalized *model.NormalizedArtifact, strategy model.Strategy) string {
	return model.Digest(struct {
		Normalized string       `json:"normalized"`
		Policy     columnPolicy `json:"policy"`
	}{
		Normalized: normalized.Fingerprint(),
		Policy: columnPolicy{
			ColumnHistogramBinPX: strategy.ColumnHistogramBinPX,
			ColumnGapPX:          strategy.ColumnGapPX,
			ColumnMinSupport:     strategy.ColumnMinSupport,
			SpanWidthRatio:       strategy.SpanWidthRatio,
		},
	})
}

func OrderInputFingerprint(columns *model.ColumnArtifact, strategy model.Strategy) string {
	return model.Digest(struct {
		Columns string      `json:"columns"`
		Policy  orderPolicy `json:"policy"`
	}{
		Columns: columns.Fingerprint(),
		Policy: orderPolicy{
			CrossColumnPriority:      strategy.CrossColumnPriority,
			ConfidenceVerticalWeight: strategy.ConfidenceVerticalWeight,
			ConfidenceColumnWeight:   strategy.ConfidenceColumnWeight,
		},
	})
}

func SummaryInputFingerprint(order *model.OrderArtifact) string {
	return model.Digest(struct {
		Order string `json:"order"`
	}{Order: order.Fingerprint()})
}

type FullArtifacts struct {
	Normalized *model.NormalizedArtifact
	Columns    *model.ColumnArtifact
	Order      *model.OrderArtifact
	Summary    *model.SummaryArtifact
	Stages     []model.StageFingerprint
}

// AnalyzeFull runs the pipeline without persistence. It is used by tests and by
// the export verifier; the service uses the same stage inputs for incremental runs.
func AnalyzeFull(doc model.Document, strategy model.Strategy) (FullArtifacts, model.DocumentResult, error) {
	result := model.DocumentResult{}
	normalizeFP := NormalizeInputFingerprint(doc, strategy)
	normalized, err := Normalize(doc, strategy)
	if err != nil {
		return FullArtifacts{}, result, err
	}
	columnFP := ColumnInputFingerprint(normalized, strategy)
	columns, err := InferColumns(normalized, strategy)
	if err != nil {
		return FullArtifacts{Normalized: normalized}, result, err
	}
	orderFP := OrderInputFingerprint(columns, strategy)
	order, err := GenerateOrder(columns, strategy)
	if err != nil {
		return FullArtifacts{Normalized: normalized, Columns: columns}, result, err
	}
	summaryFP := SummaryInputFingerprint(order)
	summary, err := Summarize(columns, order)
	if err != nil {
		return FullArtifacts{Normalized: normalized, Columns: columns, Order: order}, result, err
	}
	stages := []model.StageFingerprint{
		{Stage: model.StageNormalize, InputFingerprint: normalizeFP, ArtifactFingerprint: normalized.Fingerprint()},
		{Stage: model.StageColumns, InputFingerprint: columnFP, ArtifactFingerprint: columns.Fingerprint()},
		{Stage: model.StageOrder, InputFingerprint: orderFP, ArtifactFingerprint: order.Fingerprint()},
		{Stage: model.StageSummary, InputFingerprint: summaryFP, ArtifactFingerprint: summary.Fingerprint()},
	}
	result = BuildDocumentResult(doc, strategy, stages, order, summary)
	return FullArtifacts{Normalized: normalized, Columns: columns, Order: order, Summary: summary, Stages: stages}, result, nil
}
