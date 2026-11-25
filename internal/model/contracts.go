package model

import (
	"strings"
	"time"
)

const (
	LossScalePrecisionFloat32 = "float32"
	LossScalePrecisionFixed64 = "fixed64"
)

type StrategySnapshot struct {
	Version               int64   `json:"version"`
	GeneratedAt           string  `json:"generated_at"`
	GPUCapBytes           int64   `json:"gpu_cap_bytes"`
	WriteBudgetBytes      int64   `json:"write_budget_bytes"`
	TimeBudgetMillis      int64   `json:"time_budget_ms"`
	LossScaleInitialFixed int64   `json:"loss_scale_initial_fixed"`
	LossScaleGrowth       float64 `json:"loss_scale_growth"`
	MaxAbsErrorFixed      int64   `json:"max_abs_error_fixed"`
	MaxRelErrorPPM        int64   `json:"max_rel_error_ppm"`
	Checksum              string  `json:"checksum,omitempty"`
}

func (s StrategySnapshot) WithoutChecksum() StrategySnapshot {
	s.Checksum = ""
	return s
}

func (s StrategySnapshot) ComputedChecksum() string {
	raw, _ := CanonicalJSON(s.WithoutChecksum())
	return SHA256Hex(raw)
}

func ValidateStrategy(s StrategySnapshot) *BizError {
	if s.Version <= 0 {
		return NewError(CodeSpecInvalid, "strategy version must be positive", map[string]any{"field": "version", "value": s.Version})
	}
	if s.GeneratedAt == "" {
		return NewError(CodeSpecInvalid, "generated_at is required", map[string]any{"field": "generated_at"})
	}
	if _, err := time.Parse(time.RFC3339Nano, s.GeneratedAt); err != nil {
		return NewError(CodeSpecInvalid, "generated_at must be RFC3339Nano", map[string]any{"field": "generated_at", "value": s.GeneratedAt})
	}
	if s.GPUCapBytes <= 0 {
		return NewError(CodeSpecInvalid, "gpu_cap_bytes must be positive", map[string]any{"field": "gpu_cap_bytes", "value": s.GPUCapBytes})
	}
	if s.WriteBudgetBytes < 0 {
		return NewError(CodeSpecInvalid, "write_budget_bytes must not be negative", map[string]any{"field": "write_budget_bytes", "value": s.WriteBudgetBytes})
	}
	if s.TimeBudgetMillis < 0 {
		return NewError(CodeSpecInvalid, "time_budget_ms must not be negative", map[string]any{"field": "time_budget_ms", "value": s.TimeBudgetMillis})
	}
	if s.LossScaleInitialFixed <= 0 {
		return NewError(CodeSpecInvalid, "loss_scale_initial_fixed must be positive", map[string]any{"field": "loss_scale_initial_fixed", "value": s.LossScaleInitialFixed})
	}
	if s.LossScaleGrowth < 1 {
		return NewError(CodeSpecInvalid, "loss_scale_growth must be at least 1", map[string]any{"field": "loss_scale_growth", "value": s.LossScaleGrowth})
	}
	if s.MaxAbsErrorFixed <= 0 {
		return NewError(CodeSpecInvalid, "max_abs_error_fixed must be positive", map[string]any{"field": "max_abs_error_fixed", "value": s.MaxAbsErrorFixed})
	}
	if s.MaxRelErrorPPM <= 0 {
		return NewError(CodeSpecInvalid, "max_rel_error_ppm must be positive", map[string]any{"field": "max_rel_error_ppm", "value": s.MaxRelErrorPPM})
	}
	return nil
}

type IncrementalPatch struct {
	ShardID           string    `json:"shard_id"`
	Precision         Precision `json:"precision,omitempty"`
	SizeBytes         int64     `json:"size_bytes,omitempty"`
	Digest            string    `json:"digest,omitempty"`
	Pool              Pool      `json:"pool,omitempty"`
	LastAccess        int64     `json:"last_access,omitempty"`
	SourceDigest      string    `json:"source_digest,omitempty"`
	MomentumDigest    string    `json:"momentum_digest,omitempty"`
	Priority          int64     `json:"priority,omitempty"`
	ClearSourceDigest bool      `json:"clear_source_digest,omitempty"`
}

func ApplyPatch(base Shard, patch IncrementalPatch) Shard {
	if patch.Precision != "" {
		base.Precision = patch.Precision
	}
	if patch.SizeBytes != 0 {
		base.SizeBytes = patch.SizeBytes
	}
	if patch.Digest != "" {
		base.Digest = patch.Digest
	}
	if patch.Pool != "" {
		base.Pool = patch.Pool
	}
	if patch.LastAccess != 0 {
		base.LastAccess = patch.LastAccess
	}
	if patch.ClearSourceDigest {
		base.SourceDigest = ""
	} else if patch.SourceDigest != "" {
		base.SourceDigest = patch.SourceDigest
	}
	if patch.MomentumDigest != "" {
		base.MomentumDigest = patch.MomentumDigest
	}
	if patch.Priority != 0 {
		base.Priority = patch.Priority
	}
	return base
}

type ImportRequest struct {
	CommitID string `json:"commit_id"`
	Format   string `json:"format"`
	Version  *int   `json:"version,omitempty"`
	Data     []byte `json:"-"`
}

type ImportReport struct {
	CommitID       string           `json:"commit_id"`
	Format         string           `json:"format"`
	FormatVersion  int              `json:"format_version"`
	Verdict        string           `json:"verdict"`
	Details        []map[string]any `json:"details"`
	RecordCount    int              `json:"record_count"`
	DatasetVersion int64            `json:"dataset_version"`
}

func NormalizeFormat(format string) string {
	return strings.ToLower(strings.TrimSpace(format))
}
