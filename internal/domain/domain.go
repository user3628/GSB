package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
)

const (
	StageNormalize = "normalize"
	StageIntent    = "intent"
	StagePriority  = "priority"
	StageVerify    = "verify"
)

var Stages = []string{StageNormalize, StageIntent, StagePriority, StageVerify}

const (
	CodeOK                 = "OK"
	CodeAdvanced           = "PIPELINE_ADVANCED"
	CodeStageCompleted     = "STAGE_COMPLETED"
	CodeStopRequested      = "STOP_REQUESTED"
	CodeRunCompleted       = "RUN_COMPLETED"
	CodeShardReplaced      = "SHARD_REPLACED"
	CodeSnapshotReplaced   = "SNAPSHOT_REPLACED"
	CodeConfigUpdated      = "CONFIG_UPDATED"
	CodeConfigRolledBack   = "CONFIG_ROLLED_BACK"
	CodeIncrementalRebuilt = "INCREMENTAL_REBUILT"
	CodeReferenceMatched   = "REFERENCE_MATCHED"
	CodeReferenceCompared  = "REFERENCE_COMPARED"
	ErrEmptyText           = "E_EMPTY_TEXT"
	ErrNonFiniteScore      = "E_NON_FINITE_SCORE"
	ErrInvalidSmoothing    = "E_INVALID_SMOOTHING"
	ErrInvalidLabel        = "E_INVALID_LABEL"
	ErrStaleCheckpoint     = "E_STALE_CHECKPOINT"
	ErrIllegalStage        = "E_ILLEGAL_STAGE"
	ErrInvalidConfig       = "E_INVALID_CONFIG"
	ErrShardMismatch       = "E_SHARD_DECLARATION_MISMATCH"
	ErrNotFound            = "E_NOT_FOUND"
)

type BizError struct {
	Code               string  `json:"code"`
	Message            string  `json:"message"`
	TicketID           string  `json:"ticket_id,omitempty"`
	Intent             string  `json:"intent,omitempty"`
	ConfigVersion      int64   `json:"config_version"`
	VersionFingerprint string  `json:"version_fingerprint,omitempty"`
	Value              float64 `json:"value,omitempty"`
}

func (e *BizError) Error() string {
	if e == nil {
		return ""
	}
	if e.TicketID != "" {
		return fmt.Sprintf("%s ticket=%s v=%d: %s", e.Code, e.TicketID, e.ConfigVersion, e.Message)
	}
	return fmt.Sprintf("%s v=%d: %s", e.Code, e.ConfigVersion, e.Message)
}

func NewBizError(code, ticketID, intent string, version int64, format string, args ...any) *BizError {
	return &BizError{
		Code:          code,
		Message:       fmt.Sprintf(format, args...),
		TicketID:      ticketID,
		Intent:        intent,
		ConfigVersion: version,
	}
}

type Ticket struct {
	TicketID      string   `json:"ticket_id"`
	Text          string   `json:"text"`
	Channel       string   `json:"channel"`
	Language      string   `json:"language"`
	Intent        string   `json:"intent"`
	Priority      *int     `json:"priority"`
	ConfidencePct *float64 `json:"confidence_pct"`
}

func (t Ticket) ContentFingerprint() string {
	return FingerprintHex("ticket-content/v1", t)
}

type ShardManifest struct {
	Path          string `json:"path"`
	Tier          string `json:"tier"`
	Rows          int    `json:"rows"`
	Columns       int    `json:"columns"`
	DeclaredRows  int    `json:"declared_rows"`
	DeclaredCols  int    `json:"declared_columns"`
	SHA256        string `json:"sha256"`
	FirstTicketID string `json:"first_ticket_id,omitempty"`
	LastTicketID  string `json:"last_ticket_id,omitempty"`
}

type TierManifest struct {
	Tier        string          `json:"tier"`
	Seed        int64           `json:"seed"`
	RowBand     string          `json:"row_band"`
	Rows        int             `json:"rows"`
	IntentCount int             `json:"intent_count"`
	Columns     []string        `json:"columns"`
	PolicyPath  string          `json:"policy_path"`
	Shards      []ShardManifest `json:"shards"`
}

type Manifest struct {
	SchemaVersion string                  `json:"schema_version"`
	Generator     string                  `json:"generator"`
	Seed          int64                   `json:"seed"`
	Columns       []string                `json:"columns"`
	Tiers         map[string]TierManifest `json:"tiers"`
	TierOrder     []string                `json:"tier_order"`
}

type ScaleObservation struct {
	Tier                string  `json:"tier"`
	RowBand             string  `json:"row_band"`
	DeclaredTickets     int     `json:"declared_tickets"`
	ActualTickets       int     `json:"actual_tickets"`
	IntentCount         int     `json:"intent_count"`
	ShardCount          int     `json:"shard_count"`
	HashDimensions      int     `json:"hash_dimensions"`
	RoundBudget         int     `json:"round_budget"`
	ObservedUniqueTerms int     `json:"observed_unique_terms"`
	ObservedTotalTerms  int     `json:"observed_total_terms"`
	AvgTermsPerTicket   float64 `json:"avg_terms_per_ticket"`
}

type PriorityThresholds struct {
	P1 float64 `json:"p1"`
	P2 float64 `json:"p2"`
	P3 float64 `json:"p3"`
}

type Policy struct {
	SchemaVersion             string                        `json:"schema_version"`
	Version                   int64                         `json:"version"`
	Tier                      string                        `json:"tier"`
	MonotonicVersion          int64                         `json:"monotonic_version"`
	IntentNames               []string                      `json:"intent_names"`
	IntentVocabWeights        map[string]map[string]float64 `json:"intent_vocab_weights"`
	IntentBias                map[string]float64            `json:"intent_bias"`
	IntentSeverity            map[string]float64            `json:"intent_severity"`
	PriorityThresholds        PriorityThresholds            `json:"priority_thresholds"`
	SmoothingAlpha            float64                       `json:"smoothing_alpha"`
	HashDimensions            int                           `json:"hash_dimensions"`
	RoundBudget               int                           `json:"round_budget"`
	VocabTopK                 int                           `json:"vocab_top_k"`
	MissingConfidenceFallback float64                       `json:"missing_confidence_fallback_pct"`
	Scale                     ScaleObservation              `json:"scale"`
	ChangeCode                string                        `json:"change_code,omitempty"`
	ChangeNote                string                        `json:"change_note,omitempty"`
	BasedOnVersion            int64                         `json:"based_on_version,omitempty"`
	RollbackOfVersion         int64                         `json:"rollback_of_version,omitempty"`
}

func (p Policy) EffectiveVersion() int64 {
	if p.MonotonicVersion > 0 {
		return p.MonotonicVersion
	}
	return p.Version
}

func (p Policy) Fingerprint() string {
	return FingerprintHex("policy/v1", p)
}

func (p Policy) SectionFingerprint(section string) string {
	switch section {
	case "normalize":
		return FingerprintHex("policy-section/normalize/v1", "nfkc+whitespace+cjk-bigram+latin-alnum")
	case "vocab_weights":
		return FingerprintHex("policy-section/vocab/v1", p.IntentVocabWeights)
	case "smoothing":
		return FingerprintHex("policy-section/smoothing/v1", p.SmoothingAlpha)
	case "hash":
		return FingerprintHex("policy-section/hash/v1", p.HashDimensions)
	case "budget":
		return FingerprintHex("policy-section/budget/v1", p.RoundBudget, p.VocabTopK)
	case "priority":
		return FingerprintHex("policy-section/priority/v1", p.PriorityThresholds, p.MissingConfidenceFallback, p.IntentSeverity)
	default:
		return FingerprintHex("policy-section/unknown/v1", section)
	}
}

type Token struct {
	Text      string `json:"text"`
	Kind      string `json:"kind"`
	Start     int    `json:"start"`
	End       int    `json:"end"`
	ByteStart int    `json:"byte_start"`
	ByteEnd   int    `json:"byte_end"`
	Channel   string `json:"channel"`
}

type NormalizeArtifact struct {
	TicketID   string    `json:"ticket_id"`
	Channel    string    `json:"channel"`
	Language   string    `json:"language"`
	Normalized string    `json:"normalized"`
	Tokens     []Token   `json:"tokens"`
	TokenCount int       `json:"token_count"`
	ErrorCode  string    `json:"error_code,omitempty"`
	Error      *BizError `json:"error,omitempty"`
	Digest     string    `json:"digest"`
}

type Contribution struct {
	Token           string  `json:"token"`
	Kind            string  `json:"kind"`
	Source          string  `json:"source"`
	Bucket          int     `json:"bucket,omitempty"`
	Occurrences     int     `json:"occurrences"`
	ConditionalP    float64 `json:"conditional_p"`
	LogConditional  float64 `json:"log_conditional"`
	VocabWeight     float64 `json:"vocab_weight"`
	LogWeight       float64 `json:"log_weight"`
	LogContribution float64 `json:"log_contribution"`
}

type IntentCandidate struct {
	Rank                int            `json:"rank"`
	Intent              string         `json:"intent"`
	Score               float64        `json:"score"`
	Posterior           float64        `json:"posterior"`
	LogPrior            float64        `json:"log_prior"`
	LogLikelihood       float64        `json:"log_likelihood"`
	LogBias             float64        `json:"log_bias"`
	Contributions       []Contribution `json:"contributions,omitempty"`
	FullContributions   bool           `json:"full_contributions"`
	EstimatedOperations int            `json:"estimated_operations"`
}

type IntentArtifact struct {
	TicketID         string            `json:"ticket_id"`
	Strategy         string            `json:"strategy"`
	ModelFingerprint string            `json:"model_fingerprint"`
	VocabFingerprint string            `json:"vocab_fingerprint"`
	Candidates       []IntentCandidate `json:"candidates"`
	TopIntent        string            `json:"top_intent"`
	TopScore         float64           `json:"top_score"`
	TopPosterior     float64           `json:"top_posterior"`
	UnknownTokens    []string          `json:"unknown_tokens"`
	TruncatedTokens  []string          `json:"truncated_tokens,omitempty"`
	HashBucketsUsed  []int             `json:"hash_buckets_used,omitempty"`
	ErrorCode        string            `json:"error_code,omitempty"`
	Digest           string            `json:"digest"`
}

type PriorityArtifact struct {
	TicketID             string   `json:"ticket_id"`
	TopIntent            string   `json:"top_intent"`
	PredictedPriority    *int     `json:"predicted_priority"`
	CombinedScore        float64  `json:"combined_score"`
	SeverityScore        float64  `json:"severity_score"`
	ModelConfidence      float64  `json:"model_confidence"`
	AnnotationConfidence *float64 `json:"annotation_confidence_pct"`
	AppliedConfidencePct float64  `json:"applied_confidence_pct"`
	DegradationSources   []string `json:"degradation_sources"`
	ErrorCode            string   `json:"error_code,omitempty"`
	Digest               string   `json:"digest"`
}

type VerificationDetail struct {
	TicketID               string   `json:"ticket_id"`
	Tier                   string   `json:"tier"`
	Shard                  string   `json:"shard"`
	Channel                string   `json:"channel"`
	Language               string   `json:"language"`
	AnnotatedIntent        string   `json:"annotated_intent"`
	PredictedIntent        string   `json:"predicted_intent"`
	IntentHit              bool     `json:"intent_hit"`
	NormalizationErrorCode string   `json:"normalization_error_code,omitempty"`
	TopScore               float64  `json:"top_score"`
	TopPosterior           float64  `json:"top_posterior"`
	CombinedScore          float64  `json:"combined_score"`
	AnnotatedPriority      *int     `json:"annotated_priority"`
	PredictedPriority      *int     `json:"predicted_priority"`
	Comparison             string   `json:"comparison"`
	Delta                  int      `json:"delta,omitempty"`
	DegradationSources     []string `json:"degradation_sources"`
	ErrorCode              string   `json:"error_code,omitempty"`
}

type StrategyCandidate struct {
	Strategy           string   `json:"strategy"`
	Selected           bool     `json:"selected"`
	EstimatedOps       int64    `json:"estimated_ops"`
	EstimatedBuckets   int      `json:"estimated_buckets,omitempty"`
	FitsBudget         bool     `json:"fits_budget"`
	Reason             string   `json:"reason"`
	EliminationReasons []string `json:"elimination_reasons,omitempty"`
}

type StrategyDecision struct {
	Tier              string              `json:"tier"`
	SelectedStrategy  string              `json:"selected_strategy"`
	Reason            string              `json:"reason"`
	Scale             ScaleObservation    `json:"scale"`
	Candidates        []StrategyCandidate `json:"candidates"`
	BudgetUtilization float64             `json:"budget_utilization"`
}

type ReferenceComparison struct {
	ReferenceFingerprint  string             `json:"reference_fingerprint"`
	EngineFingerprint     string             `json:"engine_fingerprint"`
	ExactScoreTickets     int                `json:"exact_score_tickets"`
	ComparedTickets       int                `json:"compared_tickets"`
	IntentTop1Matches     int                `json:"intent_top1_matches"`
	PriorityMatches       int                `json:"priority_matches"`
	MaxScoreAbsDiff       float64            `json:"max_score_abs_diff"`
	MeanScoreAbsDiff      float64            `json:"mean_score_abs_diff"`
	MaxCenteredScoreDiff  float64            `json:"max_centered_score_diff"`
	MeanCenteredScoreDiff float64            `json:"mean_centered_score_diff"`
	MaxPosteriorDiff      float64            `json:"max_posterior_diff"`
	Pass                  bool               `json:"pass"`
	Thresholds            map[string]float64 `json:"thresholds"`
	Note                  string             `json:"note"`
}

type Report struct {
	SchemaVersion       string               `json:"schema_version"`
	Product             string               `json:"product"`
	Tier                string               `json:"tier"`
	PolicyVersion       int64                `json:"policy_version"`
	PolicyFingerprint   string               `json:"policy_fingerprint"`
	ManifestFingerprint string               `json:"manifest_fingerprint"`
	Strategy            StrategyDecision     `json:"strategy"`
	Totals              map[string]int       `json:"totals"`
	ComparisonCounts    map[string]int       `json:"comparison_counts"`
	IntentLabelAccuracy float64              `json:"intent_label_accuracy"`
	PriorityAccuracy    float64              `json:"priority_accuracy"`
	DegradedCount       int                  `json:"degraded_count"`
	InvalidCount        int                  `json:"invalid_count"`
	Details             []VerificationDetail `json:"details"`
	Reference           ReferenceComparison  `json:"reference"`
	ReportFingerprint   string               `json:"report_fingerprint"`
}

func CanonicalBytes(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func FingerprintHex(prefix string, parts ...any) string {
	h := sha256.New()
	_, _ = h.Write([]byte(prefix))
	_, _ = h.Write([]byte{0})
	for _, part := range parts {
		switch x := part.(type) {
		case string:
			_, _ = h.Write([]byte(x))
		case []byte:
			_, _ = h.Write(x)
		default:
			b, err := json.Marshal(x)
			if err != nil {
				_, _ = h.Write([]byte("__non_canonical_value__:"))
				_, _ = h.Write([]byte(fmt.Sprintf("%#v", x)))
			} else {
				_, _ = h.Write(b)
			}
		}
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func DigestArtifact(kind string, v any) string {
	return FingerprintHex("artifact/"+kind+"/v1", v)
}

func SortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func FiniteOrNaN(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}
