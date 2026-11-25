package model

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
)

const (
	RouteLexical  = "lexical"
	RouteCategory = "category"
	RouteVector   = "vector"

	StageRecall     = "recall"
	StageFusion     = "fusion"
	StageEvaluation = "evaluation"
)

var RouteOrder = []string{RouteLexical, RouteCategory, RouteVector}

type Item struct {
	ItemID   string    `json:"item_id"`
	Category string    `json:"category"`
	Vector   []float64 `json:"vector"`
	Title    string    `json:"title"`
}

type Query struct {
	QueryID           string   `json:"query_id"`
	Text              string   `json:"query"`
	Locale            string   `json:"locale"`
	ClickedCategories []string `json:"clicked_categories"`
}

type Judgment struct {
	QueryID       string `json:"query_id"`
	ItemID        string `json:"item_id"`
	Relevance     int    `json:"relevance"`
	ReferenceRank int    `json:"reference_rank"`
}

type RouteConfig struct {
	CandidateDepth  int `json:"candidate_depth"`
	EvaluationQuota int `json:"evaluation_quota"`
}

type RoutesConfig struct {
	Lexical  RouteConfig `json:"lexical"`
	Category RouteConfig `json:"category"`
	Vector   RouteConfig `json:"vector"`
}

func (r RoutesConfig) Get(route string) RouteConfig {
	switch route {
	case RouteLexical:
		return r.Lexical
	case RouteCategory:
		return r.Category
	case RouteVector:
		return r.Vector
	default:
		return RouteConfig{}
	}
}

func (r *RoutesConfig) Set(route string, value RouteConfig) {
	switch route {
	case RouteLexical:
		r.Lexical = value
	case RouteCategory:
		r.Category = value
	case RouteVector:
		r.Vector = value
	}
}

type RouteWeights struct {
	Lexical  float64 `json:"lexical"`
	Category float64 `json:"category"`
	Vector   float64 `json:"vector"`
}

func (w RouteWeights) Get(route string) float64 {
	switch route {
	case RouteLexical:
		return w.Lexical
	case RouteCategory:
		return w.Category
	case RouteVector:
		return w.Vector
	default:
		return 0
	}
}

type FusionConfig struct {
	Strategy       string       `json:"strategy"`
	Weights        RouteWeights `json:"weights"`
	RRFK           float64      `json:"rrf_k"`
	TopK           int          `json:"top_k"`
	DedupeByItemID bool         `json:"dedupe_by_item_id"`
}

type BudgetConfig struct {
	GlobalCandidateEvaluations int     `json:"global_candidate_evaluations"`
	PerRoundTimeBudgetMS       int     `json:"per_round_time_budget_ms"`
	EstimatedCandidateMicros   float64 `json:"estimated_candidate_micros"`
}

type EvaluationConfig struct {
	FusedTopK int `json:"fused_top_k"`
}

type ExecutionConfig struct {
	MaxConcurrentQueries int `json:"max_concurrent_queries"`
	WorkerProcesses      int `json:"worker_processes"`
}

type Config struct {
	Version         string           `json:"version"`
	Seed            int64            `json:"seed"`
	VectorDimension int              `json:"vector_dim"`
	Routes          RoutesConfig     `json:"routes"`
	Fusion          FusionConfig     `json:"fusion"`
	Budget          BudgetConfig     `json:"budget"`
	Evaluation      EvaluationConfig `json:"evaluation"`
	Execution       ExecutionConfig  `json:"execution"`
}

func (c Config) Normalize() Config {
	out := c
	out.Version = strings.TrimSpace(out.Version)
	out.Fusion.Strategy = strings.ToLower(strings.TrimSpace(out.Fusion.Strategy))
	if out.Fusion.Strategy == "reciprocal_rank" || out.Fusion.Strategy == "rrf" {
		out.Fusion.Strategy = "rrf"
	}
	if out.Fusion.Strategy == "" {
		out.Fusion.Strategy = "weighted"
	}
	if out.Fusion.RRFK == 0 {
		out.Fusion.RRFK = 60
	}
	if out.Evaluation.FusedTopK == 0 {
		out.Evaluation.FusedTopK = 10
	}
	if out.Execution.MaxConcurrentQueries == 0 {
		out.Execution.MaxConcurrentQueries = 1
	}
	if out.Execution.WorkerProcesses == 0 {
		out.Execution.WorkerProcesses = out.Execution.MaxConcurrentQueries
	}
	if out.Budget.EstimatedCandidateMicros == 0 {
		out.Budget.EstimatedCandidateMicros = 25
	}
	return out
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Version) == "" {
		return NewError(CodeInvalidConfig, "config version must not be empty", "")
	}
	if c.VectorDimension <= 0 {
		return NewError(CodeInvalidConfig, "vector_dim must be positive", "")
	}
	if c.Fusion.Strategy != "weighted" && c.Fusion.Strategy != "rrf" {
		return NewError(CodeInvalidConfig, "fusion.strategy must be weighted or rrf", "")
	}
	for _, route := range RouteOrder {
		rc := c.Routes.Get(route)
		if rc.CandidateDepth < 0 || rc.EvaluationQuota < 0 {
			return NewError(CodeInvalidConfig, fmt.Sprintf("route %s depth and quota must be non-negative", route), "")
		}
		weight := c.Fusion.Weights.Get(route)
		if math.IsNaN(weight) || math.IsInf(weight, 0) {
			return NewError(CodeInvalidWeight, fmt.Sprintf("weight for %s is not finite", route), "")
		}
		if weight < 0 {
			return NewError(CodeInvalidWeight, fmt.Sprintf("weight for %s must be non-negative", route), "")
		}
	}
	if c.Fusion.Weights.Lexical+c.Fusion.Weights.Category+c.Fusion.Weights.Vector == 0 {
		return NewError(CodeInvalidWeight, "at least one fusion weight must be positive", "")
	}
	if c.Fusion.TopK <= 0 || c.Evaluation.FusedTopK <= 0 {
		return NewError(CodeInvalidConfig, "fusion.top_k and evaluation.fused_top_k must be positive", "")
	}
	if !c.Fusion.DedupeByItemID {
		return NewError(CodeInvalidConfig, "fusion.dedupe_by_item_id must be true", "")
	}
	if c.Budget.GlobalCandidateEvaluations < 0 || c.Budget.PerRoundTimeBudgetMS <= 0 {
		return NewError(CodeInvalidConfig, "global budget must be non-negative and per-round time budget must be positive", "")
	}
	if c.Budget.EstimatedCandidateMicros <= 0 {
		return NewError(CodeInvalidConfig, "estimated_candidate_micros must be positive", "")
	}
	if c.Execution.MaxConcurrentQueries <= 0 || c.Execution.WorkerProcesses <= 0 {
		return NewError(CodeInvalidConfig, "execution concurrency and worker processes must be positive", "")
	}
	return nil
}

type LexicalComponents struct {
	MatchedTerms    []string `json:"matched_terms"`
	QueryTermCount  int      `json:"query_term_count"`
	TitleTermCount  int      `json:"title_term_count"`
	OverlapCount    int      `json:"overlap_count"`
	DiceCoefficient float64  `json:"dice_coefficient"`
}

type CategoryComponents struct {
	MatchedCategories []string `json:"matched_categories"`
	ClickCount        int      `json:"click_count"`
	TotalClicks       int      `json:"total_clicks"`
	CategoryItemCount int      `json:"category_item_count"`
	Affinity          float64  `json:"affinity"`
}

type VectorComponents struct {
	CosineSimilarity float64 `json:"cosine_similarity"`
	QueryNorm        float64 `json:"query_norm"`
	ItemNorm         float64 `json:"item_norm"`
}

type ScoreComponents struct {
	Lexical  *LexicalComponents  `json:"lexical,omitempty"`
	Category *CategoryComponents `json:"category,omitempty"`
	Vector   *VectorComponents   `json:"vector,omitempty"`
}

type Candidate struct {
	ItemID     string          `json:"item_id"`
	Score      float64         `json:"score"`
	Components ScoreComponents `json:"components"`
}

type RouteResult struct {
	Route          string      `json:"route"`
	Enabled        bool        `json:"enabled"`
	RequestedDepth int         `json:"requested_depth"`
	AppliedDepth   int         `json:"applied_depth"`
	EvaluatedCount int         `json:"evaluated_count"`
	Candidates     []Candidate `json:"candidates"`
	DisabledReason string      `json:"disabled_reason,omitempty"`
}

type RecallBundle struct {
	QueryID string        `json:"query_id"`
	Routes  []RouteResult `json:"routes"`
}

type SourceContribution struct {
	Route        string  `json:"route"`
	Rank         int     `json:"rank"`
	RouteScore   float64 `json:"route_score"`
	Contribution float64 `json:"contribution"`
}

type FusedItem struct {
	ItemID  string               `json:"item_id"`
	Score   float64              `json:"score"`
	Sources []SourceContribution `json:"sources"`
}

type RouteDetail struct {
	Route          string   `json:"route"`
	CandidateCount int      `json:"candidate_count"`
	ItemIDs        []string `json:"item_ids"`
}

type FusionResult struct {
	QueryID               string        `json:"query_id"`
	Strategy              string        `json:"strategy"`
	RequestedTopK         int           `json:"requested_top_k"`
	CandidateCount        int           `json:"candidate_count"`
	ReturnedCount         int           `json:"returned_count"`
	TopKExceedsCandidates bool          `json:"top_k_exceeds_candidates"`
	Truncated             bool          `json:"truncated"`
	TruncationReason      string        `json:"truncation_reason,omitempty"`
	RouteDetails          []RouteDetail `json:"route_details"`
	Results               []FusedItem   `json:"results"`
}

type RouteHit struct {
	Route    string `json:"route"`
	HitCount int    `json:"hit_count"`
}

type Metric struct {
	Applicable          bool    `json:"applicable"`
	Value               float64 `json:"value"`
	Numerator           int     `json:"numerator"`
	Denominator         int     `json:"denominator"`
	NotApplicableReason string  `json:"not_applicable_reason,omitempty"`
}

type EvaluationResult struct {
	QueryID              string     `json:"query_id"`
	RouteHits            []RouteHit `json:"route_hits"`
	FusedTop10Overlap    Metric     `json:"fused_top10_overlap"`
	CandidateCoverage    Metric     `json:"candidate_coverage"`
	AverageRankDeviation Metric     `json:"average_rank_deviation"`
	RelevantItemCount    int        `json:"relevant_item_count"`
}

type QueryResult struct {
	QueryID    string           `json:"query_id"`
	Recall     RecallBundle     `json:"recall"`
	Fusion     FusionResult     `json:"fusion"`
	Evaluation EvaluationResult `json:"evaluation"`
}

type RouteDecision struct {
	Route                string `json:"route"`
	Enabled              bool   `json:"enabled"`
	ConfiguredDepth      int    `json:"configured_depth"`
	AppliedDepth         int    `json:"applied_depth"`
	PredictedEvaluations int    `json:"predicted_evaluations"`
	RemainingRouteQuota  int    `json:"remaining_route_quota"`
	Reason               string `json:"reason"`
	EliminatedReason     string `json:"eliminated_reason,omitempty"`
}

type BudgetDecision struct {
	Round                int             `json:"round"`
	QueryID              string          `json:"query_id"`
	Routes               []RouteDecision `json:"routes"`
	PredictedEvaluations int             `json:"predicted_evaluations"`
	BudgetAtDecision     int             `json:"budget_at_decision"`
	RemainingQueries     int             `json:"remaining_queries"`
	Processed            bool            `json:"processed"`
	Reason               string          `json:"reason"`
}

type UnprocessedQuery struct {
	QueryID string `json:"query_id"`
	Reason  string `json:"reason"`
}

type AggregateMetric struct {
	ApplicableQueries    int     `json:"applicable_queries"`
	NotApplicableQueries int     `json:"not_applicable_queries"`
	Mean                 float64 `json:"mean"`
	NumeratorSum         int     `json:"numerator_sum"`
	DenominatorSum       int     `json:"denominator_sum"`
}

type RouteAggregate struct {
	Route          string `json:"route"`
	CandidateCount int    `json:"candidate_count"`
	HitCount       int    `json:"hit_count"`
	EnabledQueries int    `json:"enabled_queries"`
}

type Summary struct {
	ConfigVersion           string           `json:"config_version"`
	ConfigFingerprint       string           `json:"config_fingerprint"`
	Seed                    int64            `json:"seed"`
	DataFingerprint         string           `json:"data_fingerprint"`
	QueryCount              int              `json:"query_count"`
	ProcessedCount          int              `json:"processed_count"`
	UnprocessedCount        int              `json:"unprocessed_count"`
	PredictedEvaluations    int              `json:"predicted_evaluations"`
	FusionTopK              int              `json:"fusion_top_k"`
	EvaluationTopK          int              `json:"evaluation_top_k"`
	Routes                  []RouteAggregate `json:"routes"`
	FusedTop10Overlap       AggregateMetric  `json:"fused_top10_overlap"`
	CandidateCoverage       AggregateMetric  `json:"candidate_coverage"`
	AverageRankDeviation    AggregateMetric  `json:"average_rank_deviation"`
	NotApplicableCounts     map[string]int   `json:"not_applicable_counts"`
	UnprocessedReasonCounts map[string]int   `json:"unprocessed_reason_counts"`
	Stopped                 bool             `json:"stopped"`
	SummaryFingerprint      string           `json:"summary_fingerprint"`
}

type RunOutput struct {
	RunID             string             `json:"run_id"`
	ConfigVersion     string             `json:"config_version"`
	ConfigFingerprint string             `json:"config_fingerprint"`
	Seed              int64              `json:"seed"`
	Results           []QueryResult      `json:"results"`
	BudgetDecisions   []BudgetDecision   `json:"budget_decisions"`
	Unprocessed       []UnprocessedQuery `json:"unprocessed"`
	Summary           Summary            `json:"summary"`
	Stopped           bool               `json:"stopped"`
	ReusedCheckpoints int                `json:"-"`
}

func (r *RecallBundle) Sort() {
	sort.Slice(r.Routes, func(i, j int) bool { return RouteIndex(r.Routes[i].Route) < RouteIndex(r.Routes[j].Route) })
	for i := range r.Routes {
		SortCandidates(r.Routes[i].Candidates)
	}
}

func SortCandidates(c []Candidate) {
	sort.SliceStable(c, func(i, j int) bool {
		if c[i].Score == c[j].Score {
			return c[i].ItemID < c[j].ItemID
		}
		return c[i].Score > c[j].Score
	})
}

func CanonicalJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}

func RouteIndex(route string) int {
	for i, r := range RouteOrder {
		if r == route {
			return i
		}
	}
	return -1
}

func StageSequence(stage string) int {
	switch stage {
	case StageRecall:
		return 1
	case StageFusion:
		return 2
	case StageEvaluation:
		return 3
	default:
		return 0
	}
}

func NextStage(stage string) string {
	switch stage {
	case StageRecall:
		return StageFusion
	case StageFusion:
		return StageEvaluation
	default:
		return ""
	}
}
