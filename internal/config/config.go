package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"ocrlab/internal/canonical"
	"ocrlab/internal/model"
)

type DetectionConfig struct {
	Threshold           int     `json:"threshold"` // 0 selects Otsu; >0 is an explicit dark-pixel cutoff.
	Invert              bool    `json:"invert"`
	MinComponentPixels  int     `json:"min_component_pixels"`
	MergeGapRatio       float64 `json:"merge_gap_ratio"`
	LineCenterTolerance float64 `json:"line_center_tolerance"`
}

type RecognitionConfig struct {
	NormWidth      int     `json:"norm_width"`
	NormHeight     int     `json:"norm_height"`
	Grid           int     `json:"grid"`
	MaxDistance    float64 `json:"max_distance"`
	MinGlyphWidth  int     `json:"min_glyph_width"`
	MinGlyphHeight int     `json:"min_glyph_height"`
}

type StrategyTier struct {
	Code        string  `json:"code"`
	Quality     float64 `json:"quality"`
	BaseCost    int     `json:"base_cost"`
	PerCharCost int     `json:"per_char_cost"`
	PerBoxCost  int     `json:"per_box_cost"`
	Heavy       bool    `json:"heavy"`
}

type StrategyConfig struct {
	SmallMaxShards   int            `json:"small_max_shards"`
	MediumMaxShards  int            `json:"medium_max_shards"`
	SmallForbidHeavy bool           `json:"small_forbid_heavy"`
	QualityWeight    float64        `json:"quality_weight"`
	CostWeight       float64        `json:"cost_weight"`
	DetectionTiers   []StrategyTier `json:"detection_tiers"`
	RecognitionTiers []StrategyTier `json:"recognition_tiers"`
}

type QueryConfig struct {
	LatencyBudgetMS int  `json:"latency_budget_ms"`
	CacheEnabled    bool `json:"cache_enabled"`
	MaxBudgetMS     int  `json:"max_budget_ms"`
}

type DiagnosticsConfig struct {
	ProbeInvalidBox     bool `json:"probe_invalid_box,omitempty"`
	ProbeEmptyTemplates bool `json:"probe_empty_templates,omitempty"`
}

type Config struct {
	SchemaVersion      string            `json:"schema_version"`
	ComputeBudgetUnits int               `json:"compute_budget_units"`
	Detection          DetectionConfig   `json:"detection"`
	Recognition        RecognitionConfig `json:"recognition"`
	Strategy           StrategyConfig    `json:"strategy"`
	Query              QueryConfig       `json:"query"`
	Diagnostics        DiagnosticsConfig `json:"diagnostics"`
}

func Default() Config {
	return Config{
		SchemaVersion:      model.SchemaVersion,
		ComputeBudgetUnits: 250000,
		Detection: DetectionConfig{
			Threshold:           0,
			Invert:              false,
			MinComponentPixels:  3,
			MergeGapRatio:       2.80,
			LineCenterTolerance: 0.65,
		},
		Recognition: RecognitionConfig{
			NormWidth:      12,
			NormHeight:     16,
			Grid:           4,
			MaxDistance:    0.70,
			MinGlyphWidth:  2,
			MinGlyphHeight: 2,
		},
		Strategy: StrategyConfig{
			SmallMaxShards:   6,
			MediumMaxShards:  24,
			SmallForbidHeavy: true,
			QualityWeight:    100,
			CostWeight:       8,
			DetectionTiers: []StrategyTier{
				{Code: "d-light", Quality: 0.80, BaseCost: 8, PerCharCost: 1},
				{Code: "d-balanced", Quality: 0.93, BaseCost: 18, PerCharCost: 2},
				{Code: "d-heavy", Quality: 0.98, BaseCost: 48, PerCharCost: 5, Heavy: true},
			},
			RecognitionTiers: []StrategyTier{
				{Code: "r-hash-light", Quality: 0.82, BaseCost: 6, PerBoxCost: 2},
				{Code: "r-hash-balanced", Quality: 0.94, BaseCost: 16, PerBoxCost: 5},
				{Code: "r-hash-dense", Quality: 0.99, BaseCost: 42, PerBoxCost: 12, Heavy: true},
			},
		},
		Query: QueryConfig{
			LatencyBudgetMS: 250,
			CacheEnabled:    true,
			MaxBudgetMS:     5000,
		},
	}
}

func (c Config) Validate() error {
	if c.ComputeBudgetUnits <= 0 {
		return fmt.Errorf("compute_budget_units must be positive")
	}
	if c.Detection.MinComponentPixels < 1 {
		return fmt.Errorf("detection.min_component_pixels must be positive")
	}
	if c.Detection.MergeGapRatio <= 0 || c.Detection.LineCenterTolerance <= 0 {
		return fmt.Errorf("detection merge and line tolerance must be positive")
	}
	if c.Recognition.NormWidth < 2 || c.Recognition.NormHeight < 2 || c.Recognition.Grid < 1 {
		return fmt.Errorf("recognition normalization dimensions must be positive")
	}
	if c.Recognition.Grid*c.Recognition.Grid > c.Recognition.NormWidth*c.Recognition.NormHeight*4 {
		return fmt.Errorf("recognition grid is too fine for normalized glyph size")
	}
	if len(c.Strategy.DetectionTiers) != 3 || len(c.Strategy.RecognitionTiers) != 3 {
		return fmt.Errorf("exactly three detection and recognition strategy tiers are required")
	}
	if c.Strategy.SmallMaxShards >= c.Strategy.MediumMaxShards {
		return fmt.Errorf("small_max_shards must be less than medium_max_shards")
	}
	if c.Query.LatencyBudgetMS < 0 || c.Query.MaxBudgetMS < c.Query.LatencyBudgetMS {
		return fmt.Errorf("invalid latency budget configuration")
	}
	return nil
}

func (c Config) Version() string {
	digest, err := canonical.Digest(c)
	if err != nil {
		return "cfg-error"
	}
	return "cfg-" + digest[:16]
}

func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return Config{}, err
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func Save(path string, c Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
