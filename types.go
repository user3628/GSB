package rewardpack

import "encoding/json"

// Stage names are part of the public checkpoint contract.
const (
	StageNormalize = "normalize"
	StageScore     = "score"
	StageFeatures  = "features"
	StageBudget    = "budget"
	StageReport    = "report"
)

// StageOrder is the only legal checkpoint publication order.
var StageOrder = []string{
	StageNormalize,
	StageScore,
	StageFeatures,
	StageBudget,
	StageReport,
}

// StageIndex returns the zero-based position of stage, or -1.
func StageIndex(stage string) int {
	for i, candidate := range StageOrder {
		if candidate == stage {
			return i
		}
	}
	return -1
}

// Annotation is one historical preference annotation. Consistency is kept as
// float64 because input may deliberately be outside [0,1].
type Annotation struct {
	PairID      string  `json:"pair_id"`
	Prompt      string  `json:"prompt"`
	Chosen      string  `json:"chosen"`
	Rejected    string  `json:"rejected"`
	Source      string  `json:"source"`
	Lang        string  `json:"lang"`
	Consistency float64 `json:"consistency"`
}

// Token records a half-open rune span in the normalized text.
type Token struct {
	Text      string `json:"text"`
	Start     int    `json:"start"`
	End       int    `json:"end"`
	ByteStart int    `json:"byte_start"`
	ByteEnd   int    `json:"byte_end"`
}

// TermFrequency is a deterministic term-set entry.
type TermFrequency struct {
	Term  string `json:"term"`
	Count int    `json:"count"`
}

// TextFeatures is the normalized representation of one text field.
type TextFeatures struct {
	Normalized string          `json:"normalized"`
	Tokens     []Token         `json:"tokens"`
	Terms      []TermFrequency `json:"terms"`
	TokenCount int             `json:"token_count"`
	CharCount  int             `json:"char_count"`
	Language   string          `json:"language"`
}

// NormalizedPair is the output of the normalization stage.
type NormalizedPair struct {
	PairID      string       `json:"pair_id"`
	Source      string       `json:"source"`
	Lang        string       `json:"lang"`
	Prompt      TextFeatures `json:"prompt"`
	Chosen      TextFeatures `json:"chosen"`
	Rejected    TextFeatures `json:"rejected"`
	TokenCost   int          `json:"token_cost"`
	RawDigest   string       `json:"raw_digest"`
	InputDigest string       `json:"input_digest"`
	Digest      string       `json:"digest"`
}

// Factor is one interpretable multiplicative term in log space.
type Factor struct {
	Name            string  `json:"name"`
	Value           float64 `json:"value"`
	LogContribution float64 `json:"log_contribution"`
	Fallback        bool    `json:"fallback"`
	FallbackReason  string  `json:"fallback_reason,omitempty"`
}

// ScoreResult is the output of the factorized scoring stage.
type ScoreResult struct {
	PairID          string   `json:"pair_id"`
	Source          string   `json:"source"`
	Lang            string   `json:"lang"`
	Factors         []Factor `json:"factors"`
	LogStrength     float64  `json:"log_strength"`
	Strength        float64  `json:"strength"`
	StrengthText    string   `json:"strength_text"`
	EffectiveDigits int      `json:"effective_digits"`
	TokenCost       int      `json:"token_cost"`
	InputDigest     string   `json:"input_digest"`
	Digest          string   `json:"digest"`
}

// QuantizedDimension records every value needed to audit fixed-point error.
type QuantizedDimension struct {
	Index         int     `json:"index"`
	Scale         float64 `json:"scale"`
	Quantized     int64   `json:"quantized"`
	ErrorBound    float64 `json:"error_bound"`
	ObservedError float64 `json:"observed_error"`
	Reference     string  `json:"reference"`
}

// FeatureResult is the output of projection, unit normalization and
// quantization. Reference values are produced with 256-bit big.Float math.
type FeatureResult struct {
	PairID           string               `json:"pair_id"`
	Source           string               `json:"source"`
	Lang             string               `json:"lang"`
	InputDimension   int                  `json:"input_dimension"`
	OutputDimension  int                  `json:"output_dimension"`
	QuantizationBits int                  `json:"quantization_bits"`
	Dimensions       []QuantizedDimension `json:"dimensions"`
	InputDigest      string               `json:"input_digest"`
	Digest           string               `json:"digest"`
}

// RetainedPair is a pair kept by the budget arbiter.
type RetainedPair struct {
	PairID    string  `json:"pair_id"`
	Source    string  `json:"source"`
	Lang      string  `json:"lang"`
	Strength  float64 `json:"strength"`
	TokenCost int     `json:"token_cost"`
	Rank      int     `json:"rank"`
	Purpose   string  `json:"purpose"`
}

// EliminatedPair records why a candidate was not retained.
type EliminatedPair struct {
	PairID    string  `json:"pair_id"`
	Source    string  `json:"source"`
	Lang      string  `json:"lang"`
	Strength  float64 `json:"strength"`
	TokenCost int     `json:"token_cost"`
	Reason    string  `json:"reason"`
	Detail    string  `json:"detail"`
}

// SourceLayer is emitted for every configured or observed source, including
// empty sources. Empty layers are never silently omitted.
type SourceLayer struct {
	Source          string `json:"source"`
	AvailablePairs  int    `json:"available_pairs"`
	ConfiguredQuota int    `json:"configured_quota"`
	EffectiveQuota  int    `json:"effective_quota"`
	KeptPairs       int    `json:"kept_pairs"`
	UsedTokens      int    `json:"used_tokens"`
	Status          string `json:"status"`
	Note            string `json:"note"`
}

// LanguageCoverage reports required language minima.
type LanguageCoverage struct {
	Language string `json:"language"`
	Required int    `json:"required"`
	Kept     int    `json:"kept"`
	Met      bool   `json:"met"`
	Note     string `json:"note,omitempty"`
}

// BudgetResult is the output of the deterministic retention stage.
type BudgetResult struct {
	GlobalTokens     int                `json:"global_tokens"`
	UsedTokens       int                `json:"used_tokens"`
	MinRetentionUnit int                `json:"min_retention_unit"`
	Retained         []RetainedPair     `json:"retained"`
	Eliminated       []EliminatedPair   `json:"eliminated"`
	SourceLayers     []SourceLayer      `json:"source_layers"`
	LanguageCoverage []LanguageCoverage `json:"language_coverage"`
	InputDigest      string             `json:"input_digest"`
	Digest           string             `json:"digest"`
}

// ReportPair is one auditable per-pair attribution row.
type ReportPair struct {
	PairID        string   `json:"pair_id"`
	Source        string   `json:"source"`
	Lang          string   `json:"lang"`
	Strength      float64  `json:"strength"`
	TokenCost     int      `json:"token_cost"`
	Factors       []Factor `json:"factors"`
	FeatureDigest string   `json:"feature_digest"`
	Retained      bool     `json:"retained"`
	Decision      string   `json:"decision"`
}

// Report is the final human-auditable artifact.
type Report struct {
	ConfigVersion    int                `json:"config_version"`
	ConfigDigest     string             `json:"config_digest"`
	GlobalTokens     int                `json:"global_tokens"`
	UsedTokens       int                `json:"used_tokens"`
	RetainedPairs    int                `json:"retained_pairs"`
	EliminatedPairs  int                `json:"eliminated_pairs"`
	Pairs            []ReportPair       `json:"pairs"`
	SourceLayers     []SourceLayer      `json:"source_layers"`
	LanguageCoverage []LanguageCoverage `json:"language_coverage"`
	UnmetLanguages   []string           `json:"unmet_languages"`
	InputDigest      string             `json:"input_digest"`
	Digest           string             `json:"digest"`
}

// ArtifactBundle is the canonical byte-comparable result set. It deliberately
// excludes checkpoint sequence numbers so incremental and full builds can be
// compared independently of publication history.
type ArtifactBundle struct {
	ConfigVersion int              `json:"config_version"`
	ConfigDigest  string           `json:"config_digest"`
	Normalized    []NormalizedPair `json:"normalized"`
	Scores        []ScoreResult    `json:"scores"`
	Features      []FeatureResult  `json:"features"`
	Budget        BudgetResult     `json:"budget"`
	Report        Report           `json:"report"`
}

// CanonicalBytes returns compact, stable JSON.
func CanonicalBytes(v any) ([]byte, error) {
	return json.Marshal(v)
}

// CanonicalString returns compact, stable JSON as a string.
func CanonicalString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}
