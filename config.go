package rewardpack

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the complete versioned feature configuration.
type Config struct {
	Version       int                 `yaml:"version" json:"version"`
	Normalization NormalizationConfig `yaml:"normalization" json:"normalization"`
	Scoring       ScoringConfig       `yaml:"scoring" json:"scoring"`
	Features      FeaturesConfig      `yaml:"features" json:"features"`
	Budget        BudgetConfig        `yaml:"budget" json:"budget"`
}

type NormalizationConfig struct {
	NFKC               bool `yaml:"nfkc" json:"nfkc"`
	CollapseWhitespace bool `yaml:"collapse_whitespace" json:"collapse_whitespace"`
	Lowercase          bool `yaml:"lowercase" json:"lowercase"`
}

type FactorFloors struct {
	Length      float64 `yaml:"length" json:"length"`
	Overlap     float64 `yaml:"overlap" json:"overlap"`
	Consistency float64 `yaml:"consistency" json:"consistency"`
}

type ScoringConfig struct {
	TermWeights               map[string]float64 `yaml:"term_weights" json:"term_weights"`
	DefaultTermWeight         float64            `yaml:"default_term_weight" json:"default_term_weight"`
	LengthPenalty             float64            `yaml:"length_penalty" json:"length_penalty"`
	OverlapScale              float64            `yaml:"overlap_scale" json:"overlap_scale"`
	FactorFloors              FactorFloors       `yaml:"factor_floors" json:"factor_floors"`
	StrengthSignificantDigits int                `yaml:"strength_significant_digits" json:"strength_significant_digits"`
}

type FeaturesConfig struct {
	Dimension        int         `yaml:"dimension" json:"dimension"`
	Projection       [][]float64 `yaml:"projection" json:"projection"`
	QuantizationBits int         `yaml:"quantization_bits" json:"quantization_bits"`
}

type BudgetConfig struct {
	GlobalTokens       int            `yaml:"global_tokens" json:"global_tokens"`
	MinRetentionUnit   int            `yaml:"min_retention_unit" json:"min_retention_unit"`
	DefaultSourceQuota int            `yaml:"default_source_quota" json:"default_source_quota"`
	SourceQuotas       map[string]int `yaml:"source_quotas" json:"source_quotas"`
	RequiredLanguages  []string       `yaml:"required_languages" json:"required_languages"`
	LanguageMinimums   map[string]int `yaml:"language_minimums" json:"language_minimums"`
}

// DefaultConfig supplies safe normalization and scoring defaults. Projection,
// dimensions and budgets remain explicit so a missing matrix cannot silently
// produce an unauditable feature space.
func DefaultConfig() Config {
	return Config{
		Version: 1,
		Normalization: NormalizationConfig{
			NFKC: true, CollapseWhitespace: true, Lowercase: true,
		},
		Scoring: ScoringConfig{
			TermWeights:               map[string]float64{},
			DefaultTermWeight:         1,
			LengthPenalty:             0.20,
			OverlapScale:              1,
			FactorFloors:              FactorFloors{Length: 1e-4, Overlap: 1e-4, Consistency: 1e-4},
			StrengthSignificantDigits: 8,
		},
		Features: FeaturesConfig{QuantizationBits: 8},
		Budget: BudgetConfig{
			GlobalTokens:       100,
			MinRetentionUnit:   1,
			DefaultSourceQuota: 100,
			SourceQuotas:       map[string]int{},
			RequiredLanguages:  []string{},
			LanguageMinimums:   map[string]int{},
		},
	}
}

// LoadConfig reads YAML and applies explicit defaults before strict decoding.
func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, errorWith(CodeInvalidConfig, fmt.Errorf("read config: %w", err))
	}
	return ParseConfig(b)
}

// ParseConfig parses one YAML configuration document.
func ParseConfig(b []byte) (Config, error) {
	cfg := DefaultConfig()
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, &BusinessError{Code: CodeInvalidConfig, Field: "config", Message: err.Error()}
	}
	if cfg.Scoring.TermWeights == nil {
		cfg.Scoring.TermWeights = map[string]float64{}
	}
	if cfg.Budget.SourceQuotas == nil {
		cfg.Budget.SourceQuotas = map[string]int{}
	}
	if cfg.Budget.LanguageMinimums == nil {
		cfg.Budget.LanguageMinimums = map[string]int{}
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks semantic constraints that do not belong to a later stage.
// Projection shape and finite values are validated by BuildFeatures so callers
// receive E_DIM_MISMATCH with row/column context.
func (c Config) Validate() error {
	if c.Version < 1 {
		return &BusinessError{Code: CodeInvalidConfig, Field: "version", Message: "version must be >= 1"}
	}
	if !c.Normalization.NFKC {
		return &BusinessError{Code: CodeInvalidConfig, Field: "normalization.nfkc", Message: "NFKC normalization is mandatory"}
	}
	if !c.Normalization.CollapseWhitespace {
		return &BusinessError{Code: CodeInvalidConfig, Field: "normalization.collapse_whitespace", Message: "whitespace collapse is mandatory"}
	}
	if !c.Normalization.Lowercase {
		return &BusinessError{Code: CodeInvalidConfig, Field: "normalization.lowercase", Message: "lowercase normalization is mandatory"}
	}
	if !finite(c.Scoring.DefaultTermWeight) || c.Scoring.DefaultTermWeight < 0 {
		return &BusinessError{Code: CodeInvalidConfig, Field: "scoring.default_term_weight", Message: "must be finite and >= 0"}
	}
	for term, weight := range c.Scoring.TermWeights {
		if strings.TrimSpace(term) == "" {
			return &BusinessError{Code: CodeInvalidConfig, Field: "scoring.term_weights", Message: "term keys must not be empty"}
		}
		if !finite(weight) || weight < 0 {
			return &BusinessError{Code: CodeInvalidConfig, Field: "scoring.term_weights." + term, Message: "weight must be finite and >= 0"}
		}
	}
	if !finite(c.Scoring.LengthPenalty) || c.Scoring.LengthPenalty < 0 {
		return &BusinessError{Code: CodeInvalidConfig, Field: "scoring.length_penalty", Message: "must be finite and >= 0"}
	}
	if !finite(c.Scoring.OverlapScale) || c.Scoring.OverlapScale < 0 {
		return &BusinessError{Code: CodeInvalidConfig, Field: "scoring.overlap_scale", Message: "must be finite and >= 0"}
	}
	for name, floor := range map[string]float64{
		"length":      c.Scoring.FactorFloors.Length,
		"overlap":     c.Scoring.FactorFloors.Overlap,
		"consistency": c.Scoring.FactorFloors.Consistency,
	} {
		if !finite(floor) || floor <= 0 || floor > 1 {
			return &BusinessError{Code: CodeInvalidConfig, Field: "scoring.factor_floors." + name, Message: "must be finite and in (0,1]"}
		}
	}
	if c.Scoring.StrengthSignificantDigits < 1 || c.Scoring.StrengthSignificantDigits > 15 {
		return &BusinessError{Code: CodeInvalidConfig, Field: "scoring.strength_significant_digits", Message: "must be in [1,15]"}
	}
	if c.Features.Dimension < 1 {
		return &BusinessError{Code: CodeInvalidConfig, Field: "features.dimension", Message: "must be >= 1"}
	}
	if len(c.Features.Projection) == 0 {
		return &BusinessError{Code: CodeInvalidConfig, Field: "features.projection", Message: "must contain at least one row"}
	}
	if c.Features.QuantizationBits < 2 || c.Features.QuantizationBits > 53 {
		return &BusinessError{Code: CodeInvalidConfig, Field: "features.quantization_bits", Message: "must be in [2,53]"}
	}
	if c.Budget.GlobalTokens < 0 {
		return &BusinessError{Code: CodeInvalidConfig, Field: "budget.global_tokens", Message: "must be >= 0"}
	}
	if c.Budget.MinRetentionUnit < 1 {
		return &BusinessError{Code: CodeInvalidConfig, Field: "budget.min_retention_unit", Message: "must be >= 1"}
	}
	if c.Budget.DefaultSourceQuota < 0 {
		return &BusinessError{Code: CodeInvalidConfig, Field: "budget.default_source_quota", Message: "must be >= 0"}
	}
	for source, quota := range c.Budget.SourceQuotas {
		if strings.TrimSpace(source) == "" {
			return &BusinessError{Code: CodeInvalidConfig, Field: "budget.source_quotas", Message: "source keys must not be empty"}
		}
		if quota < 0 {
			return &BusinessError{Code: CodeInvalidConfig, Field: "budget.source_quotas." + source, Message: "quota must be >= 0"}
		}
	}
	for language, minimum := range c.Budget.LanguageMinimums {
		if language == "" || minimum < 0 {
			return &BusinessError{Code: CodeInvalidConfig, Field: "budget.language_minimums." + language, Message: "language must be non-empty and minimum >= 0"}
		}
	}
	return nil
}

// Digest returns a stable digest over all configuration contents.
func (c Config) Digest() string { return digestJSON(c) }

func (c Config) NormalizationDigest() string { return digestJSON(c.Normalization) }
func (c Config) ScoringDigest() string       { return digestJSON(c.Scoring) }
func (c Config) FeaturesDigest() string      { return digestJSON(c.Features) }
func (c Config) BudgetDigest() string        { return digestJSON(c.Budget) }

// EarliestChangedStage compares config sections and returns the first affected
// stage. An empty string means the configuration did not change.
func EarliestChangedStage(old, next Config) string {
	switch {
	case old.NormalizationDigest() != next.NormalizationDigest():
		return StageNormalize
	case old.ScoringDigest() != next.ScoringDigest():
		return StageScore
	case old.FeaturesDigest() != next.FeaturesDigest():
		return StageFeatures
	case old.BudgetDigest() != next.BudgetDigest():
		return StageBudget
	case old.Digest() != next.Digest():
		return StageBudget
	default:
		return ""
	}
}

// CanonicalJSON returns the stable JSON encoding used by snapshots.
func (c Config) CanonicalJSON() ([]byte, error) {
	return json.Marshal(c)
}

func sortedStringKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func clamp(v, lo, hi float64) float64 {
	if math.IsNaN(v) {
		return lo
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
