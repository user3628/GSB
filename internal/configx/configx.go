package configx

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"featureplatform/internal/model"
	"featureplatform/internal/textx"
)

type VersionPolicy struct {
	MismatchAction   string         `json:"mismatch_action"`
	FallbackVersions map[string]int `json:"fallback_versions,omitempty"`
	MaxSkew          int            `json:"max_skew"`
}

type MappingRule struct {
	MappingVersion string            `json:"mapping_version"`
	Terms          map[string]string `json:"terms,omitempty"`
}

type TaskMapping struct {
	Rules map[string]MappingRule `json:"rules"`
}

type Config struct {
	ConfigVersion          int64                        `json:"config_version"`
	HashDim                int                          `json:"hash_dim"`
	LabelTableVersion      string                       `json:"label_table_version"`
	CoverageThreshold      float64                      `json:"coverage_threshold"`
	TaskCoverageThresholds map[string]float64           `json:"task_coverage_thresholds,omitempty"`
	BatchBudget            int                          `json:"batch_budget"`
	TaskVersions           map[string]int               `json:"task_versions"`
	VersionPolicy          VersionPolicy                `json:"version_policy"`
	TaskMappings           map[string]TaskMapping       `json:"task_mappings"`
	LabelMappings          map[string]map[string]string `json:"label_mappings"`
}

func (c Config) Validate(forSamples []model.Sample) *model.CodedError {
	if c.HashDim == 0 {
		for _, s := range forSamples {
			return model.HashDimZero(s.SampleID, s.Task)
		}
		return &model.CodedError{Code: model.CodeHashDimZero, Message: "hash_dim must be greater than zero"}
	}
	if c.HashDim < 0 {
		return &model.CodedError{Code: model.CodeHashDimZero, Message: "hash_dim must be non-negative"}
	}
	if c.BatchBudget <= 0 {
		return &model.CodedError{Code: "E_INVALID_CONFIG", Message: "batch_budget must be greater than zero"}
	}
	if c.CoverageThreshold < 0 || c.CoverageThreshold > 1 {
		return &model.CodedError{Code: "E_INVALID_CONFIG", Message: "coverage_threshold must be in [0,1]"}
	}
	for _, s := range forSamples {
		if _, ok := c.TaskMappings[s.Task]; !ok {
			return model.TaskMappingMissing(s.SampleID, s.Task)
		}
	}
	return nil
}

func (c Config) ThresholdFor(task string) float64 {
	if v, ok := c.TaskCoverageThresholds[task]; ok {
		return v
	}
	return c.CoverageThreshold
}

func (c Config) RuleFor(task string, sampleVersion int) (MappingRule, int, string, []string, *model.CodedError) {
	tm, ok := c.TaskMappings[task]
	if !ok {
		return MappingRule{}, 0, "missing", nil, model.TaskMappingMissing("", task)
	}
	currentVersion := c.TaskVersions[task]
	key := strconv.Itoa(sampleVersion)
	if rule, ok := tm.Rules[key]; ok {
		if sampleVersion < currentVersion {
			return rule, sampleVersion, "downgraded_historical", []string{
				fmt.Sprintf("W_TASK_VERSION_DOWNGRADE: sample=v%d current=v%d applied=%s", sampleVersion, currentVersion, rule.MappingVersion),
			}, nil
		}
		if sampleVersion > currentVersion {
			return rule, sampleVersion, "newer_known", []string{
				fmt.Sprintf("W_TASK_VERSION_NEWER: sample=v%d current=v%d applied=%s", sampleVersion, currentVersion, rule.MappingVersion),
			}, nil
		}
		return rule, sampleVersion, "current", nil, nil
	}

	policy := c.VersionPolicy
	if policy.MismatchAction == "reject" {
		return MappingRule{}, 0, "rejected", nil, &model.CodedError{
			Code:    model.CodeTaskVersionUnsupported,
			Message: fmt.Sprintf("no mapping rule for task version %s v%d and policy rejects mismatch", task, sampleVersion),
			Task:    task,
		}
	}
	fallback := policy.FallbackVersions[task]
	if fallback == 0 {
		fallback = 1
	}
	if policy.MaxSkew > 0 {
		delta := sampleVersion - currentVersion
		if delta < 0 {
			delta = -delta
		}
		if delta > policy.MaxSkew {
			return MappingRule{}, 0, "rejected", nil, &model.CodedError{
				Code:    model.CodeTaskVersionUnsupported,
				Message: fmt.Sprintf("version skew %d exceeds max_skew %d", delta, policy.MaxSkew),
				Task:    task,
			}
		}
	}
	rule, ok := tm.Rules[strconv.Itoa(fallback)]
	if !ok {
		return MappingRule{}, 0, "rejected", nil, &model.CodedError{
			Code:    model.CodeTaskVersionUnsupported,
			Message: fmt.Sprintf("fallback mapping v%d is absent", fallback),
			Task:    task,
		}
	}
	return rule, fallback, "downgraded_fallback", []string{
		fmt.Sprintf("W_TASK_VERSION_FALLBACK: sample=v%d current=v%d fallback=v%d applied=%s", sampleVersion, currentVersion, fallback, rule.MappingVersion),
	}, nil
}

type FingerprintSet struct {
	ConfigFingerprint    string `json:"config_fingerprint"`
	NormalizeFingerprint string `json:"normalize_fingerprint"`
	LabelFingerprint     string `json:"label_fingerprint"`
	FeatureFingerprint   string `json:"feature_fingerprint"`
	CoverageFingerprint  string `json:"coverage_fingerprint"`
}

func fingerprint(prefix string, v any) string {
	d, _, err := model.DigestValue(prefix, v)
	if err != nil {
		panic(err)
	}
	return d
}

func (c Config) Fingerprints() FingerprintSet {
	configFP := fingerprint("config/v1:", c)
	normalizeFP := fingerprint("normalize-stage/v1:", struct {
		NormalizerVersion string `json:"normalizer_version"`
	}{textx.NormalizerVersion})
	labelFP := fingerprint("label-stage/v1:", struct {
		LabelTableVersion string                       `json:"label_table_version"`
		LabelMappings     map[string]map[string]string `json:"label_mappings"`
	}{c.LabelTableVersion, c.LabelMappings})
	featureFP := fingerprint("feature-stage/v1:", struct {
		SchemaVersion string                 `json:"schema_version"`
		HashDim       int                    `json:"hash_dim"`
		TaskVersions  map[string]int         `json:"task_versions"`
		VersionPolicy VersionPolicy          `json:"version_policy"`
		TaskMappings  map[string]TaskMapping `json:"task_mappings"`
	}{"unified-hash-v1", c.HashDim, c.TaskVersions, c.VersionPolicy, c.TaskMappings})
	coverageFP := fingerprint("coverage-stage/v1:", struct {
		CoverageThreshold      float64            `json:"coverage_threshold"`
		TaskCoverageThresholds map[string]float64 `json:"task_coverage_thresholds"`
	}{c.CoverageThreshold, c.TaskCoverageThresholds})
	return FingerprintSet{
		ConfigFingerprint:    configFP,
		NormalizeFingerprint: normalizeFP,
		LabelFingerprint:     labelFP,
		FeatureFingerprint:   featureFP,
		CoverageFingerprint:  coverageFP,
	}
}

type Change struct {
	Op    string          `json:"op"`
	Path  string          `json:"path"`
	Value json.RawMessage `json:"value,omitempty"`
}

type Fragment struct {
	FragmentID       string   `json:"fragment_id"`
	BaseVersion      int64    `json:"base_version"`
	NewVersion       int64    `json:"new_version"`
	Reason           string   `json:"reason,omitempty"`
	Changes          []Change `json:"changes"`
	ForceFullRebuild bool     `json:"force_full_rebuild,omitempty"`
}

func ApplyFragment(current Config, fragment Fragment) (Config, error) {
	if fragment.BaseVersion != current.ConfigVersion {
		return Config{}, &model.CodedError{Code: model.CodeStaleVersion, Message: "fragment base_version does not match current config version", CurrentVersion: current.ConfigVersion}
	}
	if fragment.NewVersion != current.ConfigVersion+1 {
		return Config{}, &model.CodedError{Code: model.CodeStaleVersion, Message: "fragment new_version must be current config_version + 1", CurrentVersion: current.ConfigVersion}
	}
	b, err := json.Marshal(current)
	if err != nil {
		return Config{}, err
	}
	var root map[string]any
	if err := json.Unmarshal(b, &root); err != nil {
		return Config{}, err
	}
	for _, ch := range fragment.Changes {
		if ch.Op == "" {
			ch.Op = "replace"
		}
		parts, err := parsePath(ch.Path)
		if err != nil {
			return Config{}, err
		}
		switch ch.Op {
		case "replace", "add":
			var value any
			if len(ch.Value) > 0 {
				if err := json.Unmarshal(ch.Value, &value); err != nil {
					return Config{}, fmt.Errorf("decode value for %s: %w", ch.Path, err)
				}
			}
			if err := setPath(root, parts, value); err != nil {
				return Config{}, err
			}
		case "remove":
			if err := removePath(root, parts); err != nil {
				return Config{}, err
			}
		default:
			return Config{}, fmt.Errorf("unsupported fragment op %q", ch.Op)
		}
	}
	root["config_version"] = fragment.NewVersion
	nextBytes, err := json.Marshal(root)
	if err != nil {
		return Config{}, err
	}
	var next Config
	if err := json.Unmarshal(nextBytes, &next); err != nil {
		return Config{}, err
	}
	next.ConfigVersion = fragment.NewVersion
	if next.BatchBudget <= 0 {
		next.BatchBudget = current.BatchBudget
	}
	return next, nil
}

func parsePath(path string) ([]string, error) {
	if path == "" || path[0] != '/' {
		return nil, fmt.Errorf("path must start with /: %q", path)
	}
	raw := strings.Split(path[1:], "/")
	parts := make([]string, 0, len(raw))
	for _, p := range raw {
		p = strings.ReplaceAll(p, "~1", "/")
		p = strings.ReplaceAll(p, "~0", "~")
		if p == "" {
			return nil, fmt.Errorf("empty path segment in %q", path)
		}
		parts = append(parts, p)
	}
	return parts, nil
}

func setPath(m map[string]any, parts []string, value any) error {
	cur := m
	for i, key := range parts {
		if i == len(parts)-1 {
			cur[key] = value
			return nil
		}
		next, ok := cur[key].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[key] = next
		}
		cur = next
	}
	return nil
}

func removePath(m map[string]any, parts []string) error {
	cur := m
	for i, key := range parts {
		if i == len(parts)-1 {
			delete(cur, key)
			return nil
		}
		next, ok := cur[key].(map[string]any)
		if !ok {
			return nil
		}
		cur = next
	}
	return nil
}

type InvalidationPlan struct {
	EarliestStage string         `json:"earliest_stage"`
	FullRebuild   bool           `json:"full_rebuild"`
	AffectedTasks []string       `json:"affected_tasks"`
	Reasons       []string       `json:"reasons"`
	ChangedPaths  []string       `json:"changed_paths"`
	Before        FingerprintSet `json:"before"`
	After         FingerprintSet `json:"after"`
	FragmentID    string         `json:"fragment_id"`
	ConfigVersion int64          `json:"config_version"`
}

func (p InvalidationPlan) AffectsTask(task string) bool {
	if p.FullRebuild {
		return true
	}
	for _, t := range p.AffectedTasks {
		if t == task {
			return true
		}
	}
	return false
}

func PlanFragment(old, next Config, fragment Fragment) InvalidationPlan {
	plan := InvalidationPlan{
		Before:        old.Fingerprints(),
		After:         next.Fingerprints(),
		FragmentID:    fragment.FragmentID,
		ConfigVersion: next.ConfigVersion,
		ChangedPaths:  make([]string, 0, len(fragment.Changes)),
	}
	taskSet := map[string]struct{}{}
	order := map[string]int{"none": 0, "normalize": 1, "label": 2, "feature_map": 3, "coverage": 4, "record": 5}
	earliest := "none"
	setEarliest := func(stage string) {
		if earliest == "none" || order[stage] < order[earliest] {
			earliest = stage
		}
	}
	addTask := func(task string) {
		if task != "" {
			taskSet[task] = struct{}{}
		}
	}

	if fragment.ForceFullRebuild {
		plan.FullRebuild = true
		plan.Reasons = append(plan.Reasons, "explicit_force_full_rebuild")
		earliest = "normalize"
	}

	for _, ch := range fragment.Changes {
		plan.ChangedPaths = append(plan.ChangedPaths, ch.Path)
		parts, _ := parsePath(ch.Path)
		op := ch.Op
		if op == "" {
			op = "replace"
		}
		switch {
		case ch.Path == "/task_mappings" && op == "replace":
			plan.FullRebuild = true
			plan.Reasons = append(plan.Reasons, "mapping_table_replaced")
			setEarliest("feature_map")
		case ch.Path == "/label_mappings" && op == "replace":
			plan.FullRebuild = true
			plan.Reasons = append(plan.Reasons, "label_table_replaced")
			setEarliest("label")
		case ch.Path == "/hash_dim":
			plan.FullRebuild = true
			plan.Reasons = append(plan.Reasons, "hash_dim_changed")
			setEarliest("feature_map")
		case len(parts) >= 2 && parts[0] == "task_versions":
			task := parts[1]
			addTask(task)
			oldV := old.TaskVersions[task]
			newV := next.TaskVersions[task]
			if newV < oldV {
				plan.FullRebuild = true
				plan.Reasons = append(plan.Reasons, fmt.Sprintf("task_version_rollback:%s:v%d->v%d", task, oldV, newV))
				setEarliest("feature_map")
			} else {
				plan.Reasons = append(plan.Reasons, fmt.Sprintf("task_version_changed:%s:v%d->v%d", task, oldV, newV))
				setEarliest("feature_map")
			}
		case len(parts) >= 2 && parts[0] == "task_mappings":
			task := parts[1]
			addTask(task)
			setEarliest("feature_map")
			plan.Reasons = append(plan.Reasons, "task_mapping_changed:"+task)
		case len(parts) >= 2 && parts[0] == "label_mappings":
			task := parts[1]
			addTask(task)
			setEarliest("label")
			plan.Reasons = append(plan.Reasons, "label_mapping_changed:"+task)
		case ch.Path == "/label_table_version":
			plan.FullRebuild = true
			plan.Reasons = append(plan.Reasons, "label_table_version_changed")
			setEarliest("label")
		case ch.Path == "/version_policy":
			plan.FullRebuild = true
			plan.Reasons = append(plan.Reasons, "version_policy_changed")
			setEarliest("feature_map")
		case ch.Path == "/coverage_threshold":
			plan.Reasons = append(plan.Reasons, "global_coverage_threshold_changed")
			setEarliest("coverage")
		case len(parts) >= 2 && parts[0] == "task_coverage_thresholds":
			addTask(parts[1])
			plan.Reasons = append(plan.Reasons, "task_coverage_threshold_changed:"+parts[1])
			setEarliest("coverage")
		case ch.Path == "/batch_budget":
			plan.Reasons = append(plan.Reasons, "batch_budget_changed_no_artifact_invalidation")
		default:
			plan.FullRebuild = true
			plan.Reasons = append(plan.Reasons, "unknown_config_path_conservative_full_rebuild:"+ch.Path)
			setEarliest("normalize")
		}
	}

	if plan.Before.LabelFingerprint != plan.After.LabelFingerprint {
		setEarliest("label")
	}
	if plan.Before.FeatureFingerprint != plan.After.FeatureFingerprint {
		setEarliest("feature_map")
	}
	if plan.Before.CoverageFingerprint != plan.After.CoverageFingerprint {
		setEarliest("coverage")
	}
	if plan.Before.NormalizeFingerprint != plan.After.NormalizeFingerprint {
		earliest = "normalize"
	}

	if plan.FullRebuild {
		taskSet = map[string]struct{}{}
	}
	tasks := make([]string, 0, len(taskSet))
	for t := range taskSet {
		tasks = append(tasks, t)
	}
	sort.Strings(tasks)
	plan.AffectedTasks = tasks
	plan.EarliestStage = earliest
	return plan
}
