// Package inject defines the external, parameter-driven fault injection
// configuration. All five injection classes (escape link, link cycle, depth
// overflow, dangling link, snapshot version gap) are selected purely through
// CLI flags or a JSON config file; no source modification is involved.
package inject

import (
	"encoding/json"
	"errors"
	"os"

	"pathcourt/internal/sample"
)

// Config is the injectable run configuration.
type Config struct {
	// Scenario selects the generated fixture set.
	Scenario sample.Scenario `json:"scenario"`
	// MaxJumps overrides the resolver reparse jump cap (depth injection).
	MaxJumps int `json:"max_jumps,omitempty"`
	// MaxConcurrent bounds concurrent sessions.
	MaxConcurrent int `json:"max_concurrent,omitempty"`
	// MaxPathsPerSession sets the per-session path budget.
	MaxPathsPerSession int `json:"max_paths_per_session,omitempty"`
	// ForceSnapshotGap makes the next snapshot commit jump to this version.
	ForceSnapshotGap int64 `json:"force_snapshot_gap,omitempty"`
	// ExtraPaths appends manifest paths (for budget injection tests).
	ExtraPaths []string `json:"extra_paths,omitempty"`
	// ExtraManifestID receives the extra paths when provided.
	ExtraManifestID string `json:"extra_manifest_id,omitempty"`
}

// Defaults returns the baseline configuration.
func Defaults() Config {
	return Config{
		Scenario:           sample.ScenarioBaseline,
		MaxJumps:           16,
		MaxConcurrent:      2,
		MaxPathsPerSession: 256,
	}
}

// LoadFile reads and validates a JSON injection config.
func LoadFile(path string) (Config, error) {
	cfg := Defaults()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	return cfg.Validate()
}

// Validate normalizes and validates the configuration.
func (c Config) Validate() (Config, error) {
	out := c
	if out.Scenario == "" {
		out.Scenario = sample.ScenarioBaseline
	}
	if _, err := sample.ParseScenario(string(out.Scenario)); err != nil {
		return out, err
	}
	if out.MaxJumps == 0 {
		out.MaxJumps = 16
	}
	if out.MaxConcurrent == 0 {
		out.MaxConcurrent = 2
	}
	if out.MaxPathsPerSession == 0 {
		out.MaxPathsPerSession = 256
	}
	if out.MaxJumps < 0 {
		return out, errors.New("max_jumps must be positive")
	}
	return out, nil
}
