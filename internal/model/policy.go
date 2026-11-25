package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
)

const (
	StageFeature      = "feature_extract"
	StageProjectQuant = "project_quantize"
	StageIndexBuild   = "index_build"
	StageRecallVerify = "recall_verify"
	TextDim           = 512
	GradientBins      = 8
	CellDescriptorDim = 6 + GradientBins
)

var StageNames = []string{StageFeature, StageProjectQuant, StageIndexBuild, StageRecallVerify}

func StageSeq(stage string) int {
	for i, name := range StageNames {
		if name == stage {
			return i + 1
		}
	}
	return 0
}

func VisualDim(grid int) int {
	if grid <= 0 {
		return 0
	}
	return grid * grid * CellDescriptorDim
}

type Weights struct {
	Cosine float64 `json:"cosine"`
	Term   float64 `json:"term"`
}

func (w Weights) Sum() float64 { return w.Cosine + w.Term }

type Policy struct {
	Version              int     `json:"version"`
	Kind                 string  `json:"kind"`
	ContentHash          string  `json:"content_hash"`
	Grid                 int     `json:"grid"`
	VisualDim            int     `json:"visual_dim"`
	TextDim              int     `json:"text_dim"`
	ProjectionDim        int     `json:"projection_dim"`
	ProjectionSeed       int64   `json:"projection_seed"`
	QuantBits            int     `json:"quant_bits"`
	QuantGranularity     string  `json:"quant_granularity"`
	FusionWeights        Weights `json:"fusion_weights"`
	CandidateDepth       int     `json:"candidate_depth"`
	NumericAbsTolerance  float64 `json:"numeric_abs_tolerance"`
	NumericRelTolerance  float64 `json:"numeric_rel_tolerance"`
	RelativeScoreFloor   float64 `json:"relative_score_floor"`
	MinAcceptableVersion int     `json:"min_acceptable_version"`
}

type Patch struct {
	BaseVersion    *int     `json:"base_version,omitempty"`
	QuantBits      *int     `json:"quant_bits,omitempty"`
	Grid           *int     `json:"grid,omitempty"`
	FusionWeights  *Weights `json:"fusion_weights,omitempty"`
	CandidateDepth *int     `json:"candidate_depth,omitempty"`
}

func DefaultPolicy() Policy {
	p := Policy{
		Version:              1,
		Kind:                 "full",
		Grid:                 3,
		TextDim:              TextDim,
		ProjectionDim:        24,
		ProjectionSeed:       0x295295,
		QuantBits:            8,
		QuantGranularity:     "dim",
		FusionWeights:        Weights{Cosine: 0.72, Term: 0.28},
		CandidateDepth:       5,
		NumericAbsTolerance:  0.18,
		NumericRelTolerance:  1.25,
		RelativeScoreFloor:   0.10,
		MinAcceptableVersion: 1,
	}
	p.VisualDim = VisualDim(p.Grid)
	p.ContentHash = p.ComputeHash()
	return p
}

func CanonicalBytes(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

func MustCanonical(v any) []byte {
	b, err := CanonicalBytes(v)
	if err != nil {
		panic(err)
	}
	return b
}

func HashBytes(prefix string, b []byte) string {
	sum := sha256.Sum256(append([]byte(prefix), b...))
	return hex.EncodeToString(sum[:])
}

func CanonicalHash(prefix string, v any) (string, []byte) {
	b := MustCanonical(v)
	return HashBytes(prefix, b), b
}

func (p Policy) hashPayload() Policy {
	cp := p
	cp.ContentHash = ""
	return cp
}

func (p Policy) ComputeHash() string {
	return HashBytes("policy-v1:", MustCanonical(p.hashPayload()))
}

func (p *Policy) Refresh() {
	p.VisualDim = VisualDim(p.Grid)
	if p.TextDim == 0 {
		p.TextDim = TextDim
	}
	p.ContentHash = p.ComputeHash()
}

func (p Policy) Validate() error {
	if p.Version < 1 {
		return NewBiz(CodePolicyInvalid, "policy", "", "version must be >= 1")
	}
	if p.Grid <= 0 {
		return NewBiz(CodeFeatureInvalid, StageFeature, "", "policy grid must be positive")
	}
	if p.VisualDim != VisualDim(p.Grid) {
		return NewBiz(CodePolicyInvalid, "policy", "", "visual_dim %d does not match grid %d", p.VisualDim, p.Grid)
	}
	if p.TextDim != TextDim {
		return NewBiz(CodePolicyInvalid, "policy", "", "text_dim must be %d", TextDim)
	}
	if p.ProjectionDim <= 0 {
		return NewBiz(CodePolicyInvalid, "policy", "", "projection_dim must be positive")
	}
	if p.QuantBits != 8 && p.QuantBits != 4 {
		return NewBiz(CodePolicyInvalid, "policy", "", "quant_bits must be 8 or 4")
	}
	if p.QuantGranularity != "dim" && p.QuantGranularity != "vector" {
		return NewBiz(CodePolicyInvalid, "policy", "", "quant_granularity must be dim or vector")
	}
	if p.CandidateDepth <= 0 {
		return NewBiz(CodePolicyInvalid, "policy", "", "candidate_depth must be positive")
	}
	if math.IsNaN(p.FusionWeights.Cosine) || math.IsNaN(p.FusionWeights.Term) || p.FusionWeights.Cosine < 0 || p.FusionWeights.Term < 0 {
		return NewBiz(CodePolicyInvalid, "policy", "", "fusion weights must be finite and non-negative")
	}
	if p.NumericAbsTolerance <= 0 || p.NumericRelTolerance <= 0 {
		return NewBiz(CodePolicyInvalid, "policy", "", "numeric tolerances must be positive")
	}
	return nil
}

func (p Policy) ApplyPatch(patch Patch) (Policy, error) {
	changed := 0
	if patch.QuantBits != nil {
		changed++
	}
	if patch.Grid != nil {
		changed++
	}
	if patch.FusionWeights != nil {
		changed++
	}
	if patch.CandidateDepth != nil {
		changed++
	}
	if changed != 1 {
		return Policy{}, NewBiz(CodePolicyInvalid, "policy_patch", "", "exactly one adjustable field is required, got %d", changed)
	}
	if patch.BaseVersion != nil && *patch.BaseVersion != p.Version {
		biz := NewBiz(CodeStaleVersion, "policy_patch", "", "patch base_version %d does not match current version %d", *patch.BaseVersion, p.Version)
		biz.CurrentVersion = p.Version
		biz.MinAcceptableVersion = p.MinAcceptableVersion
		biz.Details = map[string]any{"base_version": *patch.BaseVersion}
		return Policy{}, biz
	}
	next := p
	next.ContentHash = ""
	next.Kind = "incremental"
	next.Version = p.Version + 1
	if patch.QuantBits != nil {
		next.QuantBits = *patch.QuantBits
	}
	if patch.Grid != nil {
		next.Grid = *patch.Grid
	}
	if patch.FusionWeights != nil {
		next.FusionWeights = *patch.FusionWeights
	}
	if patch.CandidateDepth != nil {
		next.CandidateDepth = *patch.CandidateDepth
	}
	next.Refresh()
	if err := next.Validate(); err != nil {
		return Policy{}, err
	}
	return next, nil
}

// AcceptIncomingPolicy identifies idempotent replay, monotonic advance, full
// replacement, and rollback by version plus content digest.
func AcceptIncomingPolicy(current *Policy, incoming Policy) (Policy, bool, error) {
	incoming.Refresh()
	if err := incoming.Validate(); err != nil {
		return Policy{}, false, err
	}
	if current == nil {
		if incoming.MinAcceptableVersion == 0 {
			incoming.MinAcceptableVersion = incoming.Version
		}
		incoming.Refresh()
		return incoming, true, nil
	}
	if incoming.Version < current.Version {
		biz := NewBiz(CodeStaleVersion, "policy_replace", "", "policy version %d is a rollback; current is %d", incoming.Version, current.Version)
		biz.CurrentVersion = current.Version
		biz.MinAcceptableVersion = current.MinAcceptableVersion
		biz.Details = map[string]any{"incoming_hash": incoming.ContentHash, "current_hash": current.ContentHash}
		return Policy{}, false, biz
	}
	if incoming.Version == current.Version {
		if incoming.ContentHash == current.ContentHash {
			return *current, false, nil
		}
		biz := NewBiz(CodeStaleVersion, "policy_replace", "", "version %d already has a different active content hash", incoming.Version)
		biz.CurrentVersion = current.Version
		biz.MinAcceptableVersion = current.MinAcceptableVersion
		biz.Reason = "version_content_conflict"
		biz.Details = map[string]any{"incoming_hash": incoming.ContentHash, "current_hash": current.ContentHash}
		return Policy{}, false, biz
	}
	if incoming.Kind != "full" && incoming.Kind != "incremental" {
		biz := NewBiz(CodePolicyInvalid, "policy_accept", "", "policy kind must be full or incremental")
		biz.CurrentVersion = current.Version
		biz.MinAcceptableVersion = current.MinAcceptableVersion
		return Policy{}, false, biz
	}
	if incoming.Kind == "full" && (incoming.MinAcceptableVersion == 0 || incoming.MinAcceptableVersion < incoming.Version) {
		incoming.MinAcceptableVersion = incoming.Version
	}
	incoming.Refresh()
	return incoming, true, nil
}

func ParsePolicyJSON(b []byte) (Policy, error) {
	var p Policy
	if err := json.Unmarshal(b, &p); err != nil {
		return Policy{}, NewBiz(CodePolicyInvalid, "policy", "", "invalid policy JSON: %v", err)
	}
	provided := p.ContentHash
	p.Refresh()
	if provided != "" && provided != p.ContentHash {
		return Policy{}, NewBiz(CodePolicyInvalid, "policy", "", "content_hash mismatch: provided %s computed %s", provided, p.ContentHash)
	}
	return p, nil
}

func ParsePatchJSON(b []byte) (Patch, error) {
	var patch Patch
	if err := json.Unmarshal(b, &patch); err != nil {
		return Patch{}, NewBiz(CodePolicyInvalid, "policy_patch", "", "invalid patch JSON: %v", err)
	}
	return patch, nil
}

func FloatDigest(prefix string, v any) string {
	h, _ := CanonicalHash(prefix, v)
	return h
}

var _ = fmt.Sprintf
