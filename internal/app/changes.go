package app

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"csrouter/internal/domain"
)

type ChangeResult struct {
	Code                    string            `json:"code"`
	Tier                    string            `json:"tier"`
	Scope                   string            `json:"scope"`
	Key                     string            `json:"key,omitempty"`
	FromVersion             int64             `json:"from_version"`
	ToVersion               int64             `json:"to_version"`
	EarliestStage           string            `json:"earliest_stage"`
	AffectedTicketIDs       []string          `json:"affected_ticket_ids"`
	ModelAffectedTicketIDs  []string          `json:"model_affected_ticket_ids,omitempty"`
	InvalidatedArtifactRows int64             `json:"invalidated_artifact_rows"`
	ReusedNormalizeTickets  int               `json:"reused_normalize_tickets,omitempty"`
	ReusedIntentTickets     int               `json:"reused_intent_tickets,omitempty"`
	BeforeFingerprints      map[string]string `json:"before_fingerprints,omitempty"`
	AfterFingerprints       map[string]string `json:"after_fingerprints,omitempty"`
	Message                 string            `json:"message"`
	Policy                  domain.Policy     `json:"policy"`
}

type ConfigChangeRequest struct {
	Tier  string `json:"tier"`
	Key   string `json:"key"`
	Value any    `json:"value"`
	Note  string `json:"note"`
}

func (a *App) ChangeConfig(req ConfigChangeRequest) (*ChangeResult, *domain.BizError) {
	a.runMu.Lock()
	defer a.runMu.Unlock()

	old, err := a.loadPolicy(req.Tier)
	if err != nil {
		return nil, domain.NewBizError(domain.ErrNotFound, "", "", 0, "%v", err)
	}
	before, _ := a.DB.FingerprintSnapshot(req.Tier)
	updated, bizErr := applyConfigKey(old, req.Key, req.Value)
	if bizErr != nil {
		return nil, bizErr
	}
	newVersion := old.EffectiveVersion() + 1
	updated.Version = newVersion
	updated.MonotonicVersion = newVersion
	updated.Tier = req.Tier
	updated.BasedOnVersion = old.EffectiveVersion()
	updated.RollbackOfVersion = 0
	updated.ChangeCode = domain.CodeConfigUpdated
	updated.ChangeNote = req.Note
	if updated.ChangeNote == "" {
		updated.ChangeNote = "single configuration change: " + req.Key
	}
	if err := a.DB.SavePolicy(updated); err != nil {
		return nil, domain.NewBizError("E_STORE", "", "", newVersion, "%v", err)
	}

	stage := configStage(req.Key)
	allIDs := a.ticketIDs(req.Tier)
	invalidated, _, err := a.DB.InvalidateFromStage(req.Tier, nil, stage)
	if err != nil {
		return nil, domain.NewBizError("E_STORE", "", "", newVersion, "%v", err)
	}
	after, _ := a.DB.FingerprintSnapshot(req.Tier)
	result := &ChangeResult{
		Code: domain.CodeConfigUpdated, Tier: req.Tier, Scope: "single_config", Key: req.Key,
		FromVersion: old.EffectiveVersion(), ToVersion: newVersion, EarliestStage: stage,
		AffectedTicketIDs: allIDs, InvalidatedArtifactRows: invalidated,
		BeforeFingerprints: before, AfterFingerprints: after, Policy: updated,
		Message: fmt.Sprintf("config %s changed; all %d tickets invalidated from %s downstream", req.Key, len(allIDs), stage),
	}
	a.fillReusedCounts(result)
	return result, nil
}

func (a *App) RollbackConfig(tier string, targetVersion int64) (*ChangeResult, *domain.BizError) {
	a.runMu.Lock()
	defer a.runMu.Unlock()

	current, err := a.loadPolicy(tier)
	if err != nil {
		return nil, domain.NewBizError(domain.ErrNotFound, "", "", 0, "%v", err)
	}
	target, err := a.DB.GetPolicy(tier, targetVersion)
	if err != nil {
		return nil, domain.NewBizError(domain.ErrNotFound, "", "", current.EffectiveVersion(), "policy version %d not found", targetVersion)
	}
	before, _ := a.DB.FingerprintSnapshot(tier)
	newVersion := current.EffectiveVersion() + 1
	rolledBack := *target
	rolledBack.Version = newVersion
	rolledBack.MonotonicVersion = newVersion
	rolledBack.Tier = tier
	rolledBack.BasedOnVersion = current.EffectiveVersion()
	rolledBack.RollbackOfVersion = targetVersion
	rolledBack.ChangeCode = domain.CodeConfigRolledBack
	rolledBack.ChangeNote = fmt.Sprintf("rollback content to version %d, published as monotonic version %d", targetVersion, newVersion)
	if err := a.DB.SavePolicy(rolledBack); err != nil {
		return nil, domain.NewBizError("E_STORE", "", "", newVersion, "%v", err)
	}

	stage := earliestPolicyDifference(current, rolledBack)
	var invalidated int64
	allIDs := a.ticketIDs(tier)
	if stage != "" {
		invalidated, _, err = a.DB.InvalidateFromStage(tier, nil, stage)
		if err != nil {
			return nil, domain.NewBizError("E_STORE", "", "", newVersion, "%v", err)
		}
	}
	after, _ := a.DB.FingerprintSnapshot(tier)
	return &ChangeResult{
		Code: domain.CodeConfigRolledBack, Tier: tier, Scope: "config_rollback",
		FromVersion: current.EffectiveVersion(), ToVersion: newVersion, EarliestStage: stage,
		AffectedTicketIDs: allIDs, InvalidatedArtifactRows: invalidated,
		BeforeFingerprints: before, AfterFingerprints: after, Policy: rolledBack,
		Message: fmt.Sprintf("rolled back to v%d content; new monotonic v%d; earliest affected stage=%s", targetVersion, newVersion, stageOrNone(stage)),
	}, nil
}

func (a *App) ReplacePolicy(policy domain.Policy) (*ChangeResult, *domain.BizError) {
	a.runMu.Lock()
	defer a.runMu.Unlock()

	current, err := a.loadPolicy(policy.Tier)
	if err != nil {
		return nil, domain.NewBizError(domain.ErrNotFound, "", "", 0, "%v", err)
	}
	before, _ := a.DB.FingerprintSnapshot(policy.Tier)
	newVersion := current.EffectiveVersion() + 1
	policy.Version = newVersion
	policy.MonotonicVersion = newVersion
	policy.BasedOnVersion = current.EffectiveVersion()
	policy.ChangeCode = domain.CodeSnapshotReplaced
	if policy.ChangeNote == "" {
		policy.ChangeNote = "whole policy snapshot replaced"
	}
	if err := a.DB.SavePolicy(policy); err != nil {
		return nil, domain.NewBizError("E_STORE", "", "", newVersion, "%v", err)
	}
	stage := earliestPolicyDifference(current, policy)
	if stage == "" {
		stage = domain.StageIntent
	}
	allIDs := a.ticketIDs(policy.Tier)
	invalidated, _, _ := a.DB.InvalidateFromStage(policy.Tier, nil, stage)
	after, _ := a.DB.FingerprintSnapshot(policy.Tier)
	return &ChangeResult{
		Code: domain.CodeSnapshotReplaced, Tier: policy.Tier, Scope: "whole_policy",
		FromVersion: current.EffectiveVersion(), ToVersion: newVersion, EarliestStage: stage,
		AffectedTicketIDs: allIDs, InvalidatedArtifactRows: invalidated,
		BeforeFingerprints: before, AfterFingerprints: after, Policy: policy,
		Message: "whole policy replacement uses SNAPSHOT_REPLACED, distinct from incremental CONFIG_UPDATED",
	}, nil
}

func (a *App) ReplaceShardFile(tier, shardRelPath, sourceAbsPath string) (*ChangeResult, *domain.BizError) {
	a.runMu.Lock()
	defer a.runMu.Unlock()

	policy, err := a.loadPolicy(tier)
	if err != nil {
		return nil, domain.NewBizError(domain.ErrNotFound, "", "", 0, "%v", err)
	}
	before, _ := a.DB.FingerprintSnapshot(tier)
	oldIDs, err := a.DB.ListTicketIDsByShard(shardRelPath)
	if err != nil {
		return nil, domain.NewBizError("E_STORE", "", "", policy.EffectiveVersion(), "%v", err)
	}
	oldFingerprints := map[string]string{}
	for _, id := range oldIDs {
		row, err := a.DB.GetTicket(id)
		if err == nil && row != nil {
			oldFingerprints[id] = row.InputFingerprint
		}
	}

	tickets, columns, sha, err := ReadTicketFile(sourceAbsPath)
	if err != nil {
		return nil, domain.NewBizError(domain.ErrShardMismatch, "", "", policy.EffectiveVersion(), "%v", err)
	}
	if len(tickets) == 0 || !allColumnsMatch(columns, 7) {
		return nil, domain.NewBizError(domain.ErrShardMismatch, "", "", policy.EffectiveVersion(),
			"replacement shard must be non-empty JSONL with 7 columns")
	}

	manifest := domain.ShardManifest{
		Path: shardRelPath, Tier: tier, Rows: len(tickets), Columns: 7,
		DeclaredRows: len(tickets), DeclaredCols: 7, SHA256: sha,
	}
	if err := a.DB.UpsertShard(manifest, policy.EffectiveVersion()); err != nil {
		return nil, domain.NewBizError("E_STORE", "", "", policy.EffectiveVersion(), "%v", err)
	}
	if err := a.DB.UpsertTickets(tier, shardRelPath, tickets); err != nil {
		return nil, domain.NewBizError("E_STORE", "", "", policy.EffectiveVersion(), "%v", err)
	}
	newIDs := make([]string, 0, len(tickets))
	changed := []string{}
	for _, ticket := range tickets {
		newIDs = append(newIDs, ticket.TicketID)
		if oldFP, ok := oldFingerprints[ticket.TicketID]; !ok || oldFP != ticket.ContentFingerprint() {
			changed = append(changed, ticket.TicketID)
		}
	}
	for _, oldID := range oldIDs {
		if !containsString(newIDs, oldID) {
			changed = append(changed, oldID)
		}
	}
	if _, err := a.DB.DeleteTicketsNotIn(shardRelPath, newIDs); err != nil {
		return nil, domain.NewBizError("E_STORE", "", "", policy.EffectiveVersion(), "%v", err)
	}

	var invalidated int64
	if len(changed) > 0 {
		invalidated, _, err = a.DB.InvalidateFromStage(tier, changed, domain.StageNormalize)
		if err != nil {
			return nil, domain.NewBizError("E_STORE", "", "", policy.EffectiveVersion(), "%v", err)
		}
	}
	allIDs := a.ticketIDs(tier)
	modelRows, _, err := a.DB.InvalidateFromStage(tier, allIDs, domain.StageIntent)
	if err != nil {
		return nil, domain.NewBizError("E_STORE", "", "", policy.EffectiveVersion(), "%v", err)
	}

	after, _ := a.DB.FingerprintSnapshot(tier)
	result := &ChangeResult{
		Code: domain.CodeShardReplaced, Tier: tier, Scope: "single_shard", Key: shardRelPath,
		FromVersion: policy.EffectiveVersion(), ToVersion: policy.EffectiveVersion(),
		EarliestStage: domain.StageNormalize, AffectedTicketIDs: changed,
		ModelAffectedTicketIDs: allIDs, InvalidatedArtifactRows: invalidated + modelRows,
		BeforeFingerprints: before, AfterFingerprints: after, Policy: policy,
		Message: fmt.Sprintf("shard replaced; %d directly changed tickets, model statistics require intent rebuild for %d tickets", len(changed), len(allIDs)),
	}
	a.fillReusedCounts(result)
	return result, nil
}

func (a *App) DemoReplaceShard(tier string) (*ChangeResult, *domain.BizError) {
	manifest, err := a.LoadManifest()
	if err != nil {
		return nil, domain.NewBizError(domain.ErrNotFound, "", "", 0, "%v", err)
	}
	tm, ok := manifest.Tiers[tier]
	if !ok || len(tm.Shards) == 0 {
		return nil, domain.NewBizError(domain.ErrNotFound, "", "", 0, "tier/shard not found")
	}
	relPath := tm.Shards[0].Path
	tickets, _, _, err := ReadTicketFile(a.SamplePath(relPath))
	if err != nil {
		return nil, domain.NewBizError(domain.ErrNotFound, "", "", 0, "%v", err)
	}
	target := 20
	if target >= len(tickets) {
		target = 1
	}
	tickets[target].Text = tickets[target].Text + " 补充凭证：refund callback webhook 已收到"

	mutationPath := filepath.Join(a.MutationDir, tier, "shard-000.replaced.jsonl")
	if err := writeTicketsJSONL(mutationPath, tickets); err != nil {
		return nil, domain.NewBizError("E_STORE", "", "", 0, "%v", err)
	}
	return a.ReplaceShardFile(tier, relPath, mutationPath)
}

func writeTicketsJSONL(path string, tickets []domain.Ticket) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	for _, ticket := range tickets {
		if err := enc.Encode(ticket); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) ticketIDs(tier string) []string {
	rows, err := a.DB.ListTickets(tier)
	if err != nil {
		return []string{}
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.Ticket.TicketID)
	}
	return ids
}

func (a *App) fillReusedCounts(result *ChangeResult) {
	statuses, err := a.DB.StageStatuses(result.Tier)
	if err != nil {
		return
	}
	for _, status := range statuses {
		switch status.Stage {
		case domain.StageNormalize:
			result.ReusedNormalizeTickets = status.Completed
		case domain.StageIntent:
			result.ReusedIntentTickets = status.Completed
		}
	}
}

func configStage(key string) string {
	switch key {
	case "priority_thresholds", "priority_thresholds.p1", "priority_thresholds.p2", "priority_thresholds.p3",
		"missing_confidence_fallback_pct", "intent_severity":
		return domain.StagePriority
	default:
		return domain.StageIntent
	}
}

func earliestPolicyDifference(old, next domain.Policy) string {
	if old.SectionFingerprint("vocab_weights") != next.SectionFingerprint("vocab_weights") ||
		old.SectionFingerprint("smoothing") != next.SectionFingerprint("smoothing") ||
		old.SectionFingerprint("hash") != next.SectionFingerprint("hash") ||
		old.SectionFingerprint("budget") != next.SectionFingerprint("budget") {
		return domain.StageIntent
	}
	if old.SectionFingerprint("priority") != next.SectionFingerprint("priority") {
		return domain.StagePriority
	}
	return ""
}

func stageOrNone(stage string) string {
	if stage == "" {
		return "none"
	}
	return stage
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func applyConfigKey(policy domain.Policy, key string, value any) (domain.Policy, *domain.BizError) {
	raw, _ := json.Marshal(policy)
	var copy domain.Policy
	_ = json.Unmarshal(raw, &copy)

	switch key {
	case "smoothing_alpha":
		v, ok := toFloat(value)
		if !ok || v < 0 {
			return copy, domain.NewBizError(domain.ErrInvalidConfig, "", "", policy.EffectiveVersion(), "smoothing_alpha must be a non-negative number")
		}
		copy.SmoothingAlpha = v
	case "hash_dimensions":
		v, ok := toInt(value)
		if !ok || v <= 0 {
			return copy, domain.NewBizError(domain.ErrInvalidConfig, "", "", policy.EffectiveVersion(), "hash_dimensions must be positive integer")
		}
		copy.HashDimensions = v
		copy.Scale.HashDimensions = v
	case "round_budget":
		v, ok := toInt(value)
		if !ok || v <= 0 {
			return copy, domain.NewBizError(domain.ErrInvalidConfig, "", "", policy.EffectiveVersion(), "round_budget must be positive integer")
		}
		copy.RoundBudget = v
		copy.Scale.RoundBudget = v
	case "vocab_top_k":
		v, ok := toInt(value)
		if !ok || v < 0 {
			return copy, domain.NewBizError(domain.ErrInvalidConfig, "", "", policy.EffectiveVersion(), "vocab_top_k must be non-negative integer")
		}
		copy.VocabTopK = v
	case "missing_confidence_fallback_pct":
		v, ok := toFloat(value)
		if !ok || v < 0 || v > 100 {
			return copy, domain.NewBizError(domain.ErrInvalidConfig, "", "", policy.EffectiveVersion(), "fallback confidence must be in [0,100]")
		}
		copy.MissingConfidenceFallback = v
	case "priority_thresholds.p1", "p1":
		v, ok := toFloat(value)
		if !ok {
			return invalidConfig(policy, key)
		}
		copy.PriorityThresholds.P1 = v
	case "priority_thresholds.p2", "p2":
		v, ok := toFloat(value)
		if !ok {
			return invalidConfig(policy, key)
		}
		copy.PriorityThresholds.P2 = v
	case "priority_thresholds.p3", "p3":
		v, ok := toFloat(value)
		if !ok {
			return invalidConfig(policy, key)
		}
		copy.PriorityThresholds.P3 = v
	case "intent_vocab_weights":
		rawValue, err := json.Marshal(value)
		if err != nil {
			return invalidConfig(policy, key)
		}
		var weights map[string]map[string]float64
		if err := json.Unmarshal(rawValue, &weights); err != nil {
			return invalidConfig(policy, key)
		}
		copy.IntentVocabWeights = weights
	case "intent_severity":
		rawValue, _ := json.Marshal(value)
		var severity map[string]float64
		if err := json.Unmarshal(rawValue, &severity); err != nil {
			return invalidConfig(policy, key)
		}
		copy.IntentSeverity = severity
	default:
		if len(key) > len("intent_severity.") && key[:len("intent_severity.")] == "intent_severity." {
			v, ok := toFloat(value)
			if !ok {
				return invalidConfig(policy, key)
			}
			if copy.IntentSeverity == nil {
				copy.IntentSeverity = map[string]float64{}
			}
			copy.IntentSeverity[key[len("intent_severity."):]] = v
			break
		}
		if len(key) > len("intent_vocab_weights.") && key[:len("intent_vocab_weights.")] == "intent_vocab_weights." {
			rest := key[len("intent_vocab_weights."):]
			parts := splitDot(rest)
			if len(parts) != 2 {
				return invalidConfig(policy, key)
			}
			v, ok := toFloat(value)
			if !ok || v <= 0 {
				return invalidConfig(policy, key)
			}
			if copy.IntentVocabWeights == nil {
				copy.IntentVocabWeights = map[string]map[string]float64{}
			}
			if copy.IntentVocabWeights[parts[0]] == nil {
				copy.IntentVocabWeights[parts[0]] = map[string]float64{}
			}
			copy.IntentVocabWeights[parts[0]][parts[1]] = v
			break
		}
		return copy, domain.NewBizError(domain.ErrInvalidConfig, "", "", policy.EffectiveVersion(), "unsupported config key %q", key)
	}
	return copy, nil
}

func invalidConfig(p domain.Policy, key string) (domain.Policy, *domain.BizError) {
	return p, domain.NewBizError(domain.ErrInvalidConfig, "", "", p.EffectiveVersion(), "invalid value for config key %q", key)
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func toInt(v any) (int, bool) {
	f, ok := toFloat(v)
	if !ok {
		return 0, false
	}
	return int(f), float64(int(f)) == f
}

func splitDot(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}
