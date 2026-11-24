package domain

import "fmt"

type CleaningPolicy struct {
	EmptyTextAction    string   `json:"empty_text_action"`
	OverlongTextAction string   `json:"overlong_text_action"`
	MaxTextLength      int      `json:"max_text_length"`
	InvalidLabelAction string   `json:"invalid_label_action"`
	AllowedLabels      []string `json:"allowed_labels"`
}

type FilterPolicy struct {
	MinTimestampMS int64 `json:"min_timestamp_ms"`
	MaxTimestampMS int64 `json:"max_timestamp_ms"`
	MinTextLength  int   `json:"min_text_length"`
}

type ShardingPolicy struct {
	Count               int     `json:"count"`
	HashAlgorithm       string  `json:"hash_algorithm"`
	Salt                string  `json:"salt"`
	RecordSkewThreshold float64 `json:"record_skew_threshold"`
	ByteSkewThreshold   float64 `json:"byte_skew_threshold"`
}

type NormalizationPolicy struct {
	TextLengthMean float64 `json:"text_length_mean"`
	TextLengthStd  float64 `json:"text_length_std"`
	FeatureDim     int     `json:"feature_dim"`
	EpochMS        int64   `json:"epoch_ms"`
}

type Policy struct {
	Version     int64               `json:"version"`
	GeneratedAt string              `json:"generated_at"`
	Cleaning    CleaningPolicy      `json:"cleaning"`
	Filter      FilterPolicy        `json:"filter"`
	Sharding    ShardingPolicy      `json:"sharding"`
	Normalize   NormalizationPolicy `json:"normalization"`
	Seed        int64               `json:"seed"`
	SampleRatio float64             `json:"sample_ratio"`
	Digest      string              `json:"digest"`
}

type PolicyPatch struct {
	BaseVersion *int64               `json:"base_version,omitempty"`
	Version     int64                `json:"version,omitempty"`
	GeneratedAt *string              `json:"generated_at,omitempty"`
	Cleaning    *CleaningPolicy      `json:"cleaning,omitempty"`
	Filter      *FilterPolicy        `json:"filter,omitempty"`
	Sharding    *ShardingPolicy      `json:"sharding,omitempty"`
	Normalize   *NormalizationPolicy `json:"normalization,omitempty"`
	Seed        *int64               `json:"seed,omitempty"`
	SampleRatio *float64             `json:"sample_ratio,omitempty"`
}

func DefaultPolicy() Policy {
	p := Policy{
		Version:     1,
		GeneratedAt: "2026-01-01T00:00:00Z",
		Cleaning: CleaningPolicy{
			EmptyTextAction:    "keep_null_mask",
			OverlongTextAction: "truncate",
			MaxTextLength:      256,
			InvalidLabelAction: "reject_batch",
			AllowedLabels:      []string{"alpha", "beta", "gamma"},
		},
		Filter: FilterPolicy{
			MinTimestampMS: 1767225600000,
			MaxTimestampMS: 1767312000000,
			MinTextLength:  0,
		},
		Sharding: ShardingPolicy{
			Count:               4,
			HashAlgorithm:       "fnv1a64",
			Salt:                "fixed-shard-salt-v1",
			RecordSkewThreshold: 0.35,
			ByteSkewThreshold:   0.50,
		},
		Normalize: NormalizationPolicy{
			TextLengthMean: 48,
			TextLengthStd:  24,
			FeatureDim:     4,
			EpochMS:        1767225600000,
		},
		Seed:        20261125,
		SampleRatio: 0.75,
	}
	p.Digest = p.ComputeDigest()
	return p
}

func (p Policy) WithoutDigest() Policy {
	p.Digest = ""
	return p
}

func (p Policy) ComputeDigest() string {
	return DigestValue(p.WithoutDigest())
}

func (p Policy) Validate() error {
	if p.Version <= 0 {
		return NewError(CodeRecordInvalid, "policy version must be positive")
	}
	if p.GeneratedAt == "" {
		return NewError(CodeRecordInvalid, "policy generated_at is required")
	}
	if p.SampleRatio < 0 || p.SampleRatio > 1 {
		return NewError(CodeSampleRatioInvalid,
			fmt.Sprintf("sample_ratio must be in [0,1], got %.4f", p.SampleRatio),
			map[string]any{"sample_ratio": p.SampleRatio})
	}
	if p.Sharding.Count <= 0 || p.Sharding.Count > 256 {
		return NewError(CodeRecordInvalid, "sharding.count must be between 1 and 256")
	}
	if p.Cleaning.MaxTextLength <= 0 {
		return NewError(CodeRecordInvalid, "cleaning.max_text_length must be positive")
	}
	if p.Normalize.FeatureDim != 4 {
		return NewError(CodeRecordInvalid, "normalization.feature_dim must equal 4 in this build")
	}
	if p.Normalize.TextLengthStd == 0 {
		return NewError(CodeRecordInvalid, "normalization.text_length_std must be non-zero")
	}
	if !validAction(p.Cleaning.EmptyTextAction, "keep_null_mask", "drop") {
		return NewError(CodeRecordInvalid, "unsupported empty_text_action")
	}
	if !validAction(p.Cleaning.OverlongTextAction, "truncate", "drop") {
		return NewError(CodeRecordInvalid, "unsupported overlong_text_action")
	}
	if !validAction(p.Cleaning.InvalidLabelAction, "reject_batch", "drop") {
		return NewError(CodeRecordInvalid, "unsupported invalid_label_action")
	}
	if p.Digest == "" || p.Digest != p.ComputeDigest() {
		return NewError(CodePolicyDigestMismatch, "policy digest does not match canonical payload",
			map[string]any{"expected_digest": p.ComputeDigest(), "submitted_digest": p.Digest})
	}
	return nil
}

func validAction(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func (patch PolicyPatch) Apply(base Policy) Policy {
	next := base
	next.Digest = ""
	if patch.Version > 0 {
		next.Version = patch.Version
	} else {
		next.Version = base.Version + 1
	}
	if patch.GeneratedAt != nil {
		next.GeneratedAt = *patch.GeneratedAt
	}
	if patch.Cleaning != nil {
		next.Cleaning = *patch.Cleaning
	}
	if patch.Filter != nil {
		next.Filter = *patch.Filter
	}
	if patch.Sharding != nil {
		next.Sharding = *patch.Sharding
	}
	if patch.Normalize != nil {
		next.Normalize = *patch.Normalize
	}
	if patch.Seed != nil {
		next.Seed = *patch.Seed
	}
	if patch.SampleRatio != nil {
		next.SampleRatio = *patch.SampleRatio
	}
	next.Digest = next.ComputeDigest()
	return next
}
