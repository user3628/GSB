package policy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"

	"queryrewrite/internal/spec"
)

// LoadYAML reads and validates a policy snapshot YAML file.
func LoadYAML(path string) (spec.Policy, error) {
	var p spec.Policy
	b, err := os.ReadFile(path)
	if err != nil {
		return p, err
	}
	if err := yaml.Unmarshal(b, &p); err != nil {
		return p, err
	}
	p.ContentDigest = Digest(p)
	if err := Validate(p); err != nil {
		return p, err
	}
	return p, nil
}

// WriteYAML serializes a snapshot deterministically (sorted YAML via JSON tag
// struct order) and writes it.
func WriteYAML(path string, p spec.Policy) error {
	p.ContentDigest = Digest(p)
	b, err := yaml.Marshal(p)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// Validate enforces legal snapshot fields.
func Validate(p spec.Policy) error {
	if p.Version <= 0 {
		return spec.NewBizError(spec.CodeInvalidPolicy, "version must be positive", "", 0, "")
	}
	if p.CandidateDepth <= 0 {
		return spec.NewBizError(spec.CodeInvalidPolicy, "candidate_depth must be positive", "", 0, "")
	}
	if p.CacheCapacity <= 0 {
		return spec.NewBizError(spec.CodeInvalidPolicy, "cache_capacity must be positive", "", 0, "")
	}
	if p.MinPrefixRunes <= 0 {
		return spec.NewBizError(spec.CodeInvalidPolicy, "min_prefix_runes must be positive", "", 0, "")
	}
	if p.PrefixBlockRunes <= 0 {
		return spec.NewBizError(spec.CodeInvalidPolicy, "prefix_block_runes must be positive", "", 0, "")
	}
	if p.EvalBudgetPerTurn <= 0 {
		return spec.NewBizError(spec.CodeInvalidPolicy, "eval_budget_per_turn must be positive", "", 0, "")
	}
	if len(p.StrategyPerMille) == 0 {
		return spec.NewBizError(spec.CodeInvalidPolicy, "strategy_per_mille must not be empty", "", 0, "")
	}
	if w := p.Weights; w.Overlap <= 0 || w.Hit <= 0 || w.Age <= 0 || w.FailPenalty <= 0 {
		return spec.NewBizError(spec.CodeInvalidPolicy, "all weights must be positive", "", 0, "")
	}
	if p.HitGainThreshold <= 0 || p.HitGainThreshold > 1 {
		return spec.NewBizError(spec.CodeInvalidPolicy, "hit_gain_threshold must be within (0,1]", "", 0, "")
	}
	if p.FailureUpperLimit <= 0 || p.FailureUpperLimit > 1 {
		return spec.NewBizError(spec.CodeInvalidPolicy, "failure_upper_limit must be within (0,1]", "", 0, "")
	}
	return nil
}

// Digest returns the deterministic content digest of a snapshot, excluding the
// digest/kind bookkeeping fields themselves.
func Digest(p spec.Policy) string {
	core := struct {
		Version           int                  `json:"version"`
		ParentVersion     int                  `json:"parent_version"`
		CandidateDepth    int                  `json:"candidate_depth"`
		StrategyPerMille  map[string]int       `json:"strategy_per_mille"`
		CacheCapacity     int                  `json:"cache_capacity"`
		MinPrefixRunes    int                  `json:"min_prefix_runes"`
		PrefixBlockRunes  int                  `json:"prefix_block_runes"`
		HitGainThreshold  float64              `json:"hit_gain_threshold"`
		FailureUpperLimit float64              `json:"failure_upper_limit"`
		EvalBudgetPerTurn int                  `json:"eval_budget_per_turn"`
		Weights           spec.DecisionWeights `json:"weights"`
	}{
		Version: p.Version, ParentVersion: p.ParentVersion,
		CandidateDepth: p.CandidateDepth, StrategyPerMille: p.StrategyPerMille,
		CacheCapacity: p.CacheCapacity, MinPrefixRunes: p.MinPrefixRunes,
		PrefixBlockRunes: p.PrefixBlockRunes, HitGainThreshold: p.HitGainThreshold,
		EvalBudgetPerTurn: p.EvalBudgetPerTurn, Weights: p.Weights, FailureUpperLimit: p.FailureUpperLimit,
	}
	b, _ := canonicalJSON(core)
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func canonicalJSON(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var tmp any
	if err := json.Unmarshal(b, &tmp); err != nil {
		return nil, err
	}
	return json.Marshal(canonicalize(tmp))
}

// canonicalize sorts map keys recursively so JSON is byte-stable.
func canonicalize(v any) any {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := map[string]any{}
		for _, k := range keys {
			out[k] = canonicalize(t[k])
		}
		return out
	case []any:
		for i := range t {
			t[i] = canonicalize(t[i])
		}
		return t
	default:
		return v
	}
}

// ApplyPatch applies a single-field incremental fragment. It returns the next
// snapshot, the earliest affected stage and the change kind ("incremental").
func ApplyPatch(cur spec.Policy, patch spec.PolicyPatch) (spec.Policy, string, error) {
	if patch.Version != cur.Version+1 {
		return cur, "", spec.NewBizError(spec.CodeStaleVersion,
			"incremental patch version must be current+1", "", 0, "").
			WithDetail("current_version", fmt.Sprintf("%d", cur.Version)).
			WithDetail("observed_version", fmt.Sprintf("%d", patch.Version))
	}
	changed := 0
	next := cur
	next.ParentVersion = cur.Version
	next.Version = patch.Version
	earliest := spec.StageSchedule
	bump := func(stage string) {
		if stageOrder(stage) < stageOrder(earliest) {
			earliest = stage
		}
	}
	if patch.CandidateDepth != nil {
		next.CandidateDepth = *patch.CandidateDepth
		changed++
		bump(spec.StageGenerate)
	}
	if len(patch.StrategyPerMille) > 0 {
		next.StrategyPerMille = cloneInt(patch.StrategyPerMille)
		changed++
		bump(spec.StageSchedule)
	}
	if patch.CacheCapacity != nil {
		next.CacheCapacity = *patch.CacheCapacity
		changed++
		bump(spec.StageSchedule)
	}
	if patch.MinPrefixRunes != nil {
		next.MinPrefixRunes = *patch.MinPrefixRunes
		changed++
		bump(spec.StagePrefix)
	}
	if patch.HitGainThreshold != nil {
		next.HitGainThreshold = *patch.HitGainThreshold
		changed++
		bump(spec.StageAdjudge)
	}
	if changed == 0 {
		return cur, "", spec.NewBizError(spec.CodeInvalidPolicy, "empty patch: no field changed", "", 0, "")
	}
	next.ContentDigest = Digest(next)
	next.Kind = "incremental"
	if err := Validate(next); err != nil {
		return cur, "", err
	}
	return next, earliest, nil
}

// Classify distinguishes incremental append, full replacement and rollback by
// content digest and version sequence.
func Classify(cur spec.Policy, incoming spec.Policy) (string, *spec.BizError) {
	if incoming.Version < cur.Version {
		return "rollback", spec.NewBizError(spec.CodeStaleVersion,
			"policy version rollback rejected", "", 0, "").
			WithDetail("current_version", fmt.Sprintf("%d", cur.Version)).
			WithDetail("incoming_version", fmt.Sprintf("%d", incoming.Version)).
			WithDetail("current_digest", cur.ContentDigest)
	}
	if incoming.Version == cur.Version && incoming.ContentDigest != cur.ContentDigest {
		return "rollback", spec.NewBizError(spec.CodeStaleVersion,
			"same version with different content digest", "", 0, "").
			WithDetail("current_version", fmt.Sprintf("%d", cur.Version))
	}
	if incoming.ParentVersion == cur.Version && incoming.ContentDigest != cur.ContentDigest {
		// Single-field lineage => incremental; broad lineage break => replacement.
		if incoming.Kind == "replacement" {
			return "replacement", nil
		}
		return "incremental", nil
	}
	if incoming.ParentVersion != cur.Version || incoming.Version > cur.Version+1 {
		return "replacement", nil
	}
	return "incremental", nil
}

func stageOrder(stage string) int {
	for i, s := range spec.AllStages() {
		if s == stage {
			return i
		}
	}
	return len(spec.AllStages())
}

func cloneInt(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
