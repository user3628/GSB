package recall

import (
	"multirecall/internal/configx"
	"multirecall/internal/model"
	"multirecall/internal/textnorm"
)

const (
	ScaleLexical   int64 = 1_000_000_000
	ScaleVector    int64 = 1_000_000_000
	ScaleNormal    int64 = 1_000_000_000
	ScaleWeight    int64 = 1_000_000_000
	ScaleFusion    int64 = 1_000_000_000
	StageBudget          = "budget"
	StageNormalize       = "normalize"
	StageLexical         = "lexical"
	StageCategory        = "category"
	StageVector          = "vector"
	StageFuse            = "fuse"
	StageAudit           = "audit"
)

var StageOrder = []string{StageBudget, StageNormalize, StageLexical, StageCategory, StageVector, StageFuse, StageAudit}

type RouteBudget struct {
	Route                   string `json:"route"`
	RequestedDepth          int    `json:"requested_depth"`
	Permille                int    `json:"permille"`
	PermilleEvaluationTotal int    `json:"permille_evaluation_total"`
	PermilleDepth           int    `json:"permille_depth"`
	EffectiveDepth          int    `json:"effective_depth"`
	PlannedEvaluations      int    `json:"planned_evaluations"`
	EstimatedMicroseconds   int64  `json:"estimated_microseconds"`
	Enabled                 bool   `json:"enabled"`
}

type BudgetReduction struct {
	Route      string `json:"route"`
	Kind       string `json:"kind"`
	FromDepth  int    `json:"from_depth"`
	ToDepth    int    `json:"to_depth"`
	Eliminated bool   `json:"eliminated"`
	Reason     string `json:"reason"`
}

type BudgetPlan struct {
	QueryCount            int               `json:"query_count"`
	GlobalLimit           int               `json:"global_evaluation_limit"`
	EstimatedTimeBudgetUS int64             `json:"estimated_time_budget_us"`
	Concurrency           int               `json:"concurrency"`
	Routes                []RouteBudget     `json:"routes"`
	Reductions            []BudgetReduction `json:"reductions"`
	PlannedEvaluations    int               `json:"planned_evaluations"`
	EstimatedTimeUS       int64             `json:"estimated_time_us"`
	ConsumedEvaluations   int               `json:"consumed_evaluations"`
}

type TermHit struct {
	Term              string  `json:"term"`
	Kind              string  `json:"kind"`
	DocumentFrequency int     `json:"document_frequency"`
	IDF               float64 `json:"idf"`
	IDFFP             int64   `json:"idf_fp"`
	TermFrequency     int     `json:"term_frequency"`
	SaturationFP      int64   `json:"saturation_fp"`
	ContributionFP    int64   `json:"contribution_fp"`
	Contribution      float64 `json:"contribution"`
}

type LexicalEvidence struct {
	IndexedTerms int       `json:"indexed_terms"`
	IndexedDocs  int       `json:"indexed_docs"`
	QueryTerms   int       `json:"query_terms"`
	MatchedTerms int       `json:"matched_terms"`
	TermHits     []TermHit `json:"term_hits"`
	ScoreFP      int64     `json:"score_fp"`
	Score        float64   `json:"score"`
	ErrorBound   float64   `json:"error_bound"`
	FixedScale   int64     `json:"fixed_scale"`
	Formula      string    `json:"formula"`
}

type CategoryMatch struct {
	HistoryPath   string   `json:"history_path"`
	HistoryPrefix []string `json:"history_prefix"`
	ItemPrefix    []string `json:"item_prefix"`
	PrefixDepth   int      `json:"prefix_depth"`
	RecencyWeight int64    `json:"recency_weight"`
	Contribution  int64    `json:"contribution"`
}

type CategoryEvidence struct {
	Matches     []CategoryMatch `json:"matches"`
	PrefixCount int             `json:"prefix_count"`
	Score       int64           `json:"score"`
	ErrorBound  float64         `json:"error_bound"`
	Formula     string          `json:"formula"`
}

type VectorEvidence struct {
	Dimension         int     `json:"dimension"`
	ExpectedDimension int     `json:"expected_dimension"`
	Scale             float64 `json:"scale"`
	DotFP             int64   `json:"dot_fp"`
	ItemNormFP        float64 `json:"item_norm_fp"`
	QueryNormFP       float64 `json:"query_norm_fp"`
	CosineReference   float64 `json:"cosine_reference"`
	CosineFixed       float64 `json:"cosine_fixed"`
	ScoreFP           int64   `json:"score_fp"`
	ErrorBound        float64 `json:"error_bound"`
	QueryVectorDigest string  `json:"query_vector_digest"`
	Formula           string  `json:"formula"`
}

type Candidate struct {
	ItemID               string            `json:"item_id"`
	Route                string            `json:"route"`
	Rank                 int               `json:"rank"`
	DomainScore          float64           `json:"domain_score"`
	DomainScoreFP        int64             `json:"domain_score_fp"`
	NormalizedScore      float64           `json:"normalized_score"`
	NormalizedScoreFP    int64             `json:"normalized_score_fp"`
	RawErrorBound        float64           `json:"raw_error_bound"`
	NormalizedErrorBound float64           `json:"normalized_error_bound"`
	Lexical              *LexicalEvidence  `json:"lexical,omitempty"`
	Category             *CategoryEvidence `json:"category,omitempty"`
	Vector               *VectorEvidence   `json:"vector,omitempty"`
}

type RouteResult struct {
	Route          string      `json:"route"`
	Enabled        bool        `json:"enabled"`
	RequestedDepth int         `json:"requested_depth"`
	EffectiveDepth int         `json:"effective_depth"`
	Returned       int         `json:"returned"`
	Candidates     []Candidate `json:"candidates"`
}

type RouteContribution struct {
	Route           string  `json:"route"`
	SourceRank      int     `json:"source_rank"`
	RouteScore      float64 `json:"route_score"`
	RouteScoreFP    int64   `json:"route_score_fp"`
	NormalizedScore float64 `json:"normalized_score"`
	Weight          float64 `json:"weight"`
	WeightFP        int64   `json:"weight_fp"`
	Contribution    float64 `json:"contribution"`
	ContributionFP  int64   `json:"contribution_fp"`
	LogContribution float64 `json:"log_contribution"`
	Formula         string  `json:"formula"`
}

type RouteErrorBound struct {
	Route             string  `json:"route"`
	QuantizationBound float64 `json:"quantization_bound"`
	PropagationBound  float64 `json:"propagation_bound"`
}

type StabilityInfo struct {
	AdjacentItemID    string  `json:"adjacent_item_id,omitempty"`
	ScoreGap          float64 `json:"score_gap"`
	ErrorBoundary     float64 `json:"error_boundary"`
	Unstable          bool    `json:"unstable"`
	CrossingMagnitude float64 `json:"crossing_magnitude"`
	Reason            string  `json:"reason"`
}

type HitNumeric struct {
	ItemID               string           `json:"item_id"`
	FusionReference      float64          `json:"fusion_reference"`
	FusionImplementation float64          `json:"fusion_implementation"`
	AbsoluteDeviation    float64          `json:"absolute_deviation"`
	ErrorBound           float64          `json:"error_bound"`
	WithinBound          bool             `json:"within_bound"`
	RouteDeviations      []RouteDeviation `json:"route_deviations"`
}

type Hit struct {
	Rank                   int                 `json:"rank"`
	ItemID                 string              `json:"item_id"`
	FusionScore            float64             `json:"fusion_score"`
	FusionScoreFP          int64               `json:"fusion_score_fp"`
	FusionErrorBound       float64             `json:"fusion_error_bound"`
	EffectiveDecimalDigits int                 `json:"effective_decimal_digits"`
	Contributions          []RouteContribution `json:"contributions"`
	RouteErrorBounds       []RouteErrorBound   `json:"route_error_bounds"`
	Stability              *StabilityInfo      `json:"stability,omitempty"`
	Numeric                *HitNumeric         `json:"numeric,omitempty"`
	Derivation             []string            `json:"derivation"`
}

type TruncationRecord struct {
	RequestedTopK      int    `json:"requested_top_k"`
	MaxCandidates      int    `json:"max_candidates"`
	MergedCandidates   int    `json:"merged_candidates"`
	EffectiveLimit     int    `json:"effective_limit"`
	ReturnedHits       int    `json:"returned_hits"`
	Truncated          bool   `json:"truncated"`
	ClampedToCandidate bool   `json:"clamped_to_candidate_count"`
	Reason             string `json:"reason,omitempty"`
}

type RouteDeviation struct {
	ItemID            string  `json:"item_id"`
	Route             string  `json:"route"`
	Implementation    float64 `json:"implementation"`
	Reference         float64 `json:"reference"`
	AbsoluteDeviation float64 `json:"absolute_deviation"`
	ErrorBound        float64 `json:"error_bound"`
	WithinBound       bool    `json:"within_bound"`
}

type SamplingCheck struct {
	Position             int    `json:"position"`
	ItemID               string `json:"item_id"`
	Digits               int    `json:"digits"`
	ImplementationDigits string `json:"implementation_digits"`
	ReferenceDigits      string `json:"reference_digits"`
	DigitByDigitMatch    bool   `json:"digit_by_digit_match"`
}

type NumericAudit struct {
	ReferencePrecisionBits int             `json:"reference_precision_bits"`
	Tolerance              float64         `json:"tolerance"`
	MaxDeviation           float64         `json:"max_deviation"`
	MaxDeviationRoute      string          `json:"max_deviation_route"`
	MaxDeviationItemID     string          `json:"max_deviation_item_id"`
	Passed                 bool            `json:"passed"`
	SamplingChecks         []SamplingCheck `json:"sampling_checks"`
	ReferenceDigest        string          `json:"reference_digest"`
}

type QueryMetrics struct {
	CandidateEvaluations  int   `json:"candidate_evaluations"`
	LexicalReturned       int   `json:"lexical_returned"`
	CategoryReturned      int   `json:"category_returned"`
	VectorReturned        int   `json:"vector_returned"`
	MergedItems           int   `json:"merged_items"`
	EstimatedMicroseconds int64 `json:"estimated_microseconds"`
}

type QueryReport struct {
	Query            model.Query       `json:"query"`
	InputFingerprint string            `json:"input_fingerprint"`
	ConfigVersion    int64             `json:"config_version"`
	ConfigDigest     string            `json:"config_digest"`
	Budget           *BudgetPlan       `json:"budget,omitempty"`
	Normalization    *textnorm.Result  `json:"normalization,omitempty"`
	Lexical          *RouteResult      `json:"lexical,omitempty"`
	Category         *RouteResult      `json:"category,omitempty"`
	Vector           *RouteResult      `json:"vector,omitempty"`
	Hits             []Hit             `json:"hits"`
	Truncation       *TruncationRecord `json:"truncation,omitempty"`
	Audit            *NumericAudit     `json:"audit,omitempty"`
	Metrics          QueryMetrics      `json:"metrics"`
	Error            *model.APIError   `json:"error,omitempty"`
}

type Report struct {
	Schema        string           `json:"schema"`
	DatasetSeed   int64            `json:"dataset_seed"`
	DatasetDigest string           `json:"dataset_digest"`
	Config        configx.Snapshot `json:"config"`
	QueryCount    int              `json:"query_count"`
	Queries       []QueryReport    `json:"queries"`
	ResultDigest  string           `json:"result_digest"`
}

func ReportDigest(queries []QueryReport) string {
	return model.Digest(queries)
}
