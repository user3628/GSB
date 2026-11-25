package policy

import (
	"fmt"
	"sync"

	"voiceline/internal/canonical"
	"voiceline/internal/model"
)

const (
	DecisionNormalAdvance   = "normal_advance"
	DecisionFullReplacement = "full_replacement"
	DecisionRollback        = "rollback"
	DecisionIdempotent      = "idempotent"
	StageNone               = "none"
	StageInput              = "input"
	StageAnalysis           = "analysis"
	StageMapping            = "mapping"
	StageDispatch           = "dispatch"
	StageReconstruction     = "reconstruction"
	StageValidation         = "validation"
)

// Strategy contains optional overrides. A nil pointer means "use manifest".
type Strategy struct {
	LineageID        string   `json:"lineage_id"`
	Version          int64    `json:"version"`
	FrameShiftMS     *float64 `json:"frame_shift_ms,omitempty"`
	BlockLength      *int     `json:"block_length,omitempty"`
	F0Shift          *float64 `json:"f0_shift,omitempty"`
	EnvelopeWarp     *float64 `json:"envelope_warp,omitempty"`
	MaxGroups        *int     `json:"max_groups,omitempty"`
	FramesPerGroup   *int     `json:"frames_per_group,omitempty"`
	RoundBudgetNanos *int64   `json:"round_budget_nanos,omitempty"`
	RecycleQuota     *bool    `json:"recycle_quota,omitempty"`
	OutputGain       *float64 `json:"output_gain,omitempty"`
}

type Patch struct {
	BaseVersion      int64    `json:"base_version"`
	Version          int64    `json:"version,omitempty"`
	FrameShiftMS     *float64 `json:"frame_shift_ms,omitempty"`
	BlockLength      *int     `json:"block_length,omitempty"`
	F0Shift          *float64 `json:"f0_shift,omitempty"`
	EnvelopeWarp     *float64 `json:"envelope_warp,omitempty"`
	MaxGroups        *int     `json:"max_groups,omitempty"`
	FramesPerGroup   *int     `json:"frames_per_group,omitempty"`
	RoundBudgetNanos *int64   `json:"round_budget_nanos,omitempty"`
	RecycleQuota     *bool    `json:"recycle_quota,omitempty"`
	OutputGain       *float64 `json:"output_gain,omitempty"`
}

type Invalidation struct {
	Decision           string   `json:"decision"`
	ChangedFields      []string `json:"changed_fields"`
	EarliestStage      string   `json:"earliest_stage"`
	EarliestBlock      int      `json:"earliest_block"`
	RebuildFromBlock   int      `json:"rebuild_from_block"`
	RebuildAllBlocks   bool     `json:"rebuild_all_blocks"`
	ReusableStages     []string `json:"reusable_stages"`
	InvalidStages      []string `json:"invalid_stages"`
	PolicyFingerprint  string   `json:"policy_fingerprint"`
	VersionFingerprint string   `json:"version_fingerprint"`
	Reason             string   `json:"reason"`
}

type Store struct {
	mu      sync.RWMutex
	current Strategy
}

func NewStore(initial Strategy) *Store {
	if initial.LineageID == "" {
		initial.LineageID = "default"
	}
	if initial.Version == 0 {
		initial.Version = 1
	}
	return &Store{current: initial}
}

func (s *Store) Current() Strategy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneStrategy(s.current)
}

func (s *Store) ApplyPatch(patch Patch) (Strategy, Invalidation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if patch.BaseVersion != s.current.Version {
		return Strategy{}, Invalidation{}, &model.BusinessError{
			Code:           model.CodeStaleBase,
			Message:        "patch base_version does not match current policy version",
			CurrentVersion: s.current.Version,
			Details: map[string]any{
				"base_version": patch.BaseVersion,
			},
		}
	}
	if patch.Version != 0 && patch.Version != s.current.Version+1 {
		return Strategy{}, Invalidation{}, model.NewError(model.CodeConflict, "",
			"monotonic patch version must be %d, got %d", s.current.Version+1, patch.Version)
	}

	next := cloneStrategy(s.current)
	next.Version = s.current.Version + 1
	if patch.FrameShiftMS != nil {
		next.FrameShiftMS = cloneFloat(patch.FrameShiftMS)
	}
	if patch.BlockLength != nil {
		next.BlockLength = cloneInt(patch.BlockLength)
	}
	if patch.F0Shift != nil {
		next.F0Shift = cloneFloat(patch.F0Shift)
	}
	if patch.EnvelopeWarp != nil {
		next.EnvelopeWarp = cloneFloat(patch.EnvelopeWarp)
	}
	if patch.MaxGroups != nil {
		next.MaxGroups = cloneInt(patch.MaxGroups)
	}
	if patch.FramesPerGroup != nil {
		next.FramesPerGroup = cloneInt(patch.FramesPerGroup)
	}
	if patch.RoundBudgetNanos != nil {
		v := *patch.RoundBudgetNanos
		next.RoundBudgetNanos = &v
	}
	if patch.RecycleQuota != nil {
		v := *patch.RecycleQuota
		next.RecycleQuota = &v
	}
	if patch.OutputGain != nil {
		next.OutputGain = cloneFloat(patch.OutputGain)
	}

	inv := PlanInvalidation(s.current, next)
	inv.Decision = DecisionNormalAdvance
	s.current = next
	return cloneStrategy(next), inv, nil
}

type SnapshotResult struct {
	Strategy     Strategy     `json:"strategy"`
	Invalidation Invalidation `json:"invalidation"`
}

func (s *Store) ApplySnapshot(next Strategy, allowRollback bool) (SnapshotResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if next.LineageID == "" {
		next.LineageID = "default"
	}

	decision := ClassifySnapshot(s.current, next)
	if decision == DecisionRollback && !allowRollback {
		return SnapshotResult{}, &model.BusinessError{
			Code:           model.CodeConflict,
			Message:        "snapshot is a policy version rollback; set allow_rollback to apply",
			CurrentVersion: s.current.Version,
			Details:        map[string]any{"requested_version": next.Version, "decision": decision},
		}
	}

	inv := PlanInvalidation(s.current, next)
	inv.Decision = decision
	switch decision {
	case DecisionFullReplacement, DecisionRollback:
		inv.EarliestStage = StageAnalysis
		inv.EarliestBlock = 0
		inv.RebuildFromBlock = 0
		inv.RebuildAllBlocks = true
		inv.InvalidStages = []string{StageAnalysis, StageMapping, StageDispatch, StageReconstruction, StageValidation}
		inv.ReusableStages = []string{StageInput}
		if decision == DecisionFullReplacement {
			inv.Reason = "snapshot lineage changed; all frame blocks and waveform artifacts are invalid"
		} else {
			inv.Reason = "rollback applied; all derived artifacts are rebuilt for deterministic downgrade"
		}
	case DecisionIdempotent:
		inv.Reason = "snapshot version and fingerprint are unchanged"
	default:
		if inv.Reason == "" {
			inv.Reason = "monotonic snapshot advance applied"
		}
	}

	if next.Version <= s.current.Version && decision == DecisionNormalAdvance {
		next.Version = s.current.Version + 1
	}
	s.current = next
	return SnapshotResult{Strategy: cloneStrategy(next), Invalidation: inv}, nil
}

func ClassifySnapshot(current, next Strategy) string {
	if next.LineageID != current.LineageID {
		return DecisionFullReplacement
	}
	if next.Version < current.Version {
		return DecisionRollback
	}
	if next.Version == current.Version && Fingerprint(current) == Fingerprint(next) {
		return DecisionIdempotent
	}
	return DecisionNormalAdvance
}

func PlanInvalidation(old, next Strategy) Invalidation {
	changed := make([]string, 0)
	earliestStage := StageNone
	earliestBlock := -1
	allBlocks := false
	invalid := map[string]bool{}
	reusable := []string{StageInput}

	mark := func(field, stage string, block int, all bool) {
		changed = append(changed, field)
		invalid[stage] = true
		if block >= 0 && (earliestBlock < 0 || block < earliestBlock) {
			earliestBlock = block
		}
		if stageRank(stage) < stageRank(earliestStage) || earliestStage == StageNone {
			earliestStage = stage
		}
		if all {
			allBlocks = true
		}
	}

	if !floatPtrEqual(old.FrameShiftMS, next.FrameShiftMS) {
		mark("frame_shift_ms", StageAnalysis, 0, true)
		invalid[StageMapping] = true
		invalid[StageReconstruction] = true
		invalid[StageValidation] = true
	}
	if !intPtrEqual(old.BlockLength, next.BlockLength) {
		mark("block_length", StageDispatch, 0, true)
		invalid[StageReconstruction] = true
	}
	if !floatPtrEqual(old.F0Shift, next.F0Shift) {
		mark("f0_shift", StageMapping, 0, true)
		invalid[StageReconstruction] = true
		invalid[StageValidation] = true
	}
	if !floatPtrEqual(old.EnvelopeWarp, next.EnvelopeWarp) {
		mark("envelope_warp", StageMapping, 0, true)
		invalid[StageReconstruction] = true
	}
	if !intPtrEqual(old.MaxGroups, next.MaxGroups) ||
		!intPtrEqual(old.FramesPerGroup, next.FramesPerGroup) ||
		!int64PtrEqual(old.RoundBudgetNanos, next.RoundBudgetNanos) ||
		!boolPtrEqual(old.RecycleQuota, next.RecycleQuota) {
		mark("concurrency_budget", StageDispatch, 0, false)
	}
	if !floatPtrEqual(old.OutputGain, next.OutputGain) {
		mark("output_gain", StageReconstruction, 0, true)
	}

	for _, stage := range []string{StageAnalysis, StageMapping, StageDispatch, StageReconstruction, StageValidation} {
		if !invalid[stage] {
			reusable = append(reusable, stage)
		}
	}
	invalidList := make([]string, 0, len(invalid))
	for stage := range invalid {
		invalidList = append(invalidList, stage)
	}

	policyFingerprint := Fingerprint(next)
	inv := Invalidation{
		ChangedFields:      changed,
		EarliestStage:      earliestStage,
		EarliestBlock:      earliestBlock,
		RebuildFromBlock:   earliestBlock,
		RebuildAllBlocks:   allBlocks,
		ReusableStages:     reusable,
		InvalidStages:      invalidList,
		PolicyFingerprint:  policyFingerprint,
		VersionFingerprint: policyFingerprint,
	}
	if len(changed) == 0 {
		inv.Reason = "no effective strategy field changed"
		inv.EarliestBlock = -1
		inv.RebuildFromBlock = -1
	} else if earliestStage == StageDispatch && !allBlocks {
		inv.Reason = "dispatch budget changed; block PCM and waveform digests remain reusable"
	}
	return inv
}

func Fingerprint(strategy Strategy) string {
	b, digest, err := canonical.JSON(strategy)
	if err != nil {
		panic(err)
	}
	_ = b
	return digest
}

type StageFingerprint struct {
	Stage         string `json:"stage"`
	Digest        string `json:"digest"`
	EarliestBlock int    `json:"earliest_block,omitempty"`
}

type VersionFingerprint struct {
	PolicyVersion int64              `json:"policy_version"`
	PolicyDigest  string             `json:"policy_digest"`
	InputDigest   string             `json:"input_digest"`
	Stages        []StageFingerprint `json:"stages"`
	Combined      string             `json:"combined"`
}

func BuildVersionFingerprint(strategy Strategy, inputDigest string, stages ...StageFingerprint) VersionFingerprint {
	vf := VersionFingerprint{
		PolicyVersion: strategy.Version,
		PolicyDigest:  Fingerprint(strategy),
		InputDigest:   inputDigest,
		Stages:        stages,
	}
	_, digest, _ := canonical.JSON(vf)
	vf.Combined = digest
	return vf
}

type BlockFingerprint struct {
	BlockIndex     int    `json:"block_index"`
	AnalysisDigest string `json:"analysis_digest"`
	MappingDigest  string `json:"mapping_digest"`
	DispatchDigest string `json:"dispatch_digest"`
	RenderDigest   string `json:"render_digest"`
	ChainDigest    string `json:"chain_digest"`
}

func ChainBlockFingerprints(blocks []BlockFingerprint) []BlockFingerprint {
	previous := "genesis"
	out := make([]BlockFingerprint, len(blocks))
	for i, block := range blocks {
		block.BlockIndex = i
		payload := fmt.Sprintf("%s|%s|%s|%s|%s", previous, block.AnalysisDigest, block.MappingDigest, block.DispatchDigest, block.RenderDigest)
		block.ChainDigest = canonical.String(payload)
		previous = block.ChainDigest
		out[i] = block
	}
	return out
}

func EarliestMismatchedBlock(oldBlocks, newBlocks []BlockFingerprint) (string, int) {
	n := len(oldBlocks)
	if len(newBlocks) < n {
		n = len(newBlocks)
	}
	for i := 0; i < n; i++ {
		if oldBlocks[i].AnalysisDigest != newBlocks[i].AnalysisDigest {
			return StageAnalysis, i
		}
		if oldBlocks[i].MappingDigest != newBlocks[i].MappingDigest {
			return StageMapping, i
		}
		if oldBlocks[i].DispatchDigest != newBlocks[i].DispatchDigest {
			return StageDispatch, i
		}
		if oldBlocks[i].RenderDigest != newBlocks[i].RenderDigest ||
			oldBlocks[i].ChainDigest != newBlocks[i].ChainDigest {
			return StageReconstruction, i
		}
	}
	if len(oldBlocks) != len(newBlocks) {
		return StageDispatch, n
	}
	return StageNone, -1
}

func stageRank(stage string) int {
	switch stage {
	case StageInput:
		return 0
	case StageAnalysis:
		return 1
	case StageMapping:
		return 2
	case StageDispatch:
		return 3
	case StageReconstruction:
		return 4
	case StageValidation:
		return 5
	default:
		return 100
	}
}

func cloneStrategy(in Strategy) Strategy {
	out := in
	out.FrameShiftMS = cloneFloat(in.FrameShiftMS)
	out.BlockLength = cloneInt(in.BlockLength)
	out.F0Shift = cloneFloat(in.F0Shift)
	out.EnvelopeWarp = cloneFloat(in.EnvelopeWarp)
	out.MaxGroups = cloneInt(in.MaxGroups)
	out.FramesPerGroup = cloneInt(in.FramesPerGroup)
	if in.RoundBudgetNanos != nil {
		v := *in.RoundBudgetNanos
		out.RoundBudgetNanos = &v
	}
	if in.RecycleQuota != nil {
		v := *in.RecycleQuota
		out.RecycleQuota = &v
	}
	out.OutputGain = cloneFloat(in.OutputGain)
	return out
}

func cloneFloat(v *float64) *float64 {
	if v == nil {
		return nil
	}
	x := *v
	return &x
}

func cloneInt(v *int) *int {
	if v == nil {
		return nil
	}
	x := *v
	return &x
}

func floatPtrEqual(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func intPtrEqual(a, b *int) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func int64PtrEqual(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func boolPtrEqual(a, b *bool) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
