package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	"netpolcompiler/internal/compiler"
	"netpolcompiler/internal/model"
	"netpolcompiler/internal/store"
)

type CommitData struct {
	RequestID          string                 `json:"requestId,omitempty"`
	Kind               string                 `json:"kind"`
	Version            int64                  `json:"version"`
	BaseVersion        int64                  `json:"baseVersion,omitempty"`
	CurrentVersion     int64                  `json:"currentVersion"`
	AffectedNamespaces []string               `json:"affectedNamespaces"`
	CompiledDigests    map[string]string      `json:"compiledDigests"`
	GlobalReport       *compiler.GlobalReport `json:"globalReport"`
}

type referenceRow struct {
	Namespace     string
	Component     string
	RuleID        string
	Field         string
	SourceVersion int64
}

func (s *Service) SubmitSnapshot(ctx context.Context, snapshot model.Snapshot) RawResult {
	return s.Execute(snapshot.RequestID, "snapshot", snapshot, func(requestID string) RawResult {
		if requestID != "" {
			snapshot.RequestID = requestID
		}
		return s.commitSnapshot(ctx, snapshot, requestID)
	})
}

func (s *Service) SubmitIncrement(ctx context.Context, increment model.Increment) RawResult {
	return s.Execute(increment.RequestID, "increment", increment, func(requestID string) RawResult {
		if requestID != "" {
			increment.RequestID = requestID
		}
		return s.commitIncrement(ctx, increment, requestID)
	})
}

func (s *Service) commitSnapshot(ctx context.Context, snapshot model.Snapshot, requestID string) RawResult {
	if err := model.ValidateSnapshot(&snapshot); err != nil {
		return RuleInvalidError(err)
	}
	if !model.VerifyDigest(snapshot) {
		biz := NewBizError("E_DIGEST_MISMATCH", 400, "snapshot digest does not match canonical rule content")
		biz.Field = "digest"
		biz.Context["providedDigest"] = snapshot.Digest
		biz.Context["expectedDigest"] = model.DigestSnapshot(snapshot)
		return FailureResult(biz)
	}
	snapshot.Rules = model.CanonicalRules(snapshot.Rules)

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	current, err := s.store.CurrentVersion(ctx)
	if err != nil {
		return InternalError("failed to read current version", err)
	}
	if snapshot.Version <= current {
		biz := NewBizError("E_STALE_SNAPSHOT", 409, "snapshot version is not newer than committed version")
		biz.Context["submittedVersion"] = snapshot.Version
		biz.Context["currentVersion"] = current
		return FailureResult(biz)
	}

	namespaces, grouped := groupRules(snapshot.Rules)
	intent := store.Intent{
		IntentID: newIntentID("snapshot", snapshot.Version), RequestID: requestID, Kind: "snapshot_commit_and_compile",
		TargetVersion: snapshot.Version, Namespaces: jsonString(namespaces), PayloadJSON: jsonString(map[string]any{"digest": snapshot.Digest, "ruleCount": len(snapshot.Rules)}),
		Status: "prepared", CreatedAt: store.NowRFC3339(), UpdatedAt: store.NowRFC3339(),
	}
	if err := s.store.CreateIntent(ctx, intent); err != nil {
		return InternalError("failed to write snapshot intent", err)
	}
	s.appendIntentLog(intent)
	s.setPending(snapshot.Version, namespaces, "snapshot_cascade_compile")
	defer s.clearPending()
	if s.cfg.Hooks.AfterIntent != nil {
		s.cfg.Hooks.AfterIntent(snapshot.Version, namespaces)
	}
	s.crashIfConfigured("after_intent")
	s.waitRebuildDelay()

	views := make(map[string]*compiler.CompiledNamespace, len(namespaces))
	digests := make(map[string]string, len(namespaces))
	for namespaceIndex, namespace := range namespaces {
		view := compiler.CompileNamespace(snapshot.Version, namespace, grouped[namespace])
		views[namespace] = view
		digests[namespace] = view.CompiledDigest
		if namespaceIndex == 0 {
			s.crashIfConfigured("mid_compile")
		}
	}
	// 50,000 ppm = 5%.
	globalReport := compiler.BuildGlobalReport(snapshot.Version, views, 50_000)

	data := CommitData{
		RequestID: requestID, Kind: "snapshot", Version: snapshot.Version, CurrentVersion: snapshot.Version,
		AffectedNamespaces: namespaces, CompiledDigests: digests, GlobalReport: globalReport,
	}
	success := SuccessResult(200, data)
	if s.cfg.Hooks.BeforeCommit != nil {
		s.cfg.Hooks.BeforeCommit(snapshot.Version, namespaces)
	}

	tx, err := s.store.DB.BeginTx(ctx, nil)
	if err != nil {
		_ = s.store.MarkIntent(ctx, intent.IntentID, "failed", err.Error())
		return InternalError("failed to begin snapshot transaction", err)
	}
	defer func() { _ = tx.Rollback() }()

	canonicalSnapshot, _ := json.Marshal(snapshot)
	if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(version,kind,base_version,request_id,generated_at,digest,canonical_json,created_at) VALUES(?,?,?,?,?,?,?,?)`,
		snapshot.Version, "full", 0, requestID, snapshot.GeneratedAt, snapshot.Digest, string(canonicalSnapshot), store.NowRFC3339()); err != nil {
		return failSnapshotTx(ctx, s, intent, "failed to insert snapshot", err)
	}
	for _, rule := range snapshot.Rules {
		ruleJSON, _ := json.Marshal(rule)
		if err := insertRuleTx(tx, snapshot.Version, rule, ruleJSON); err != nil {
			return failSnapshotTx(ctx, s, intent, "failed to insert rule", err)
		}
	}
	for _, namespace := range namespaces {
		if err := persistCompiledView(ctx, tx, snapshot.Version, views[namespace]); err != nil {
			return failSnapshotTx(ctx, s, intent, "failed to persist compiled view", err)
		}
	}
	if err := persistGlobalReportTx(ctx, tx, snapshot.Version, globalReport); err != nil {
		return failSnapshotTx(ctx, s, intent, "failed to persist consistency report", err)
	}
	if err := upsertMetaTx(tx, snapshot.Version); err != nil {
		return failSnapshotTx(ctx, s, intent, "failed to update version", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed', reason='', updated_at=? WHERE intent_id=?`, store.NowRFC3339(), intent.IntentID); err != nil {
		return failSnapshotTx(ctx, s, intent, "failed to commit intent", err)
	}
	if requestID != "" {
		if err := completeRequestTx(ctx, tx, requestID, success.Status, success.Body); err != nil {
			return failSnapshotTx(ctx, s, intent, "failed to persist idempotent response", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return InternalError("failed to commit snapshot transaction", err)
	}

	s.cache.Store(&cacheSnapshot{version: snapshot.Version, views: views})
	s.clearPending()
	success.RequestPersisted = requestID != ""
	return success
}

func (s *Service) commitIncrement(ctx context.Context, increment model.Increment, requestID string) RawResult {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	current, err := s.store.CurrentVersion(ctx)
	if err != nil {
		return InternalError("failed to read current version", err)
	}
	if increment.BaseVersion != current {
		biz := NewBizError("E_STALE_BASE", 409, "increment base version is not the current committed version")
		biz.Context["baseVersion"] = increment.BaseVersion
		biz.Context["currentVersion"] = current
		return FailureResult(biz)
	}
	if err := model.ValidateIncrement(&increment, current); err != nil {
		return RuleInvalidError(err)
	}

	allRules, err := s.loadRules(ctx, current)
	if err != nil {
		return InternalError("failed to load current rules", err)
	}
	affectedSet := map[string]struct{}{}
	for _, change := range increment.Changes {
		switch change.Op {
		case "upsert", "":
			rule := model.NormalizeRule(*change.Rule)
			allRules = upsertRule(allRules, rule)
			affectedSet[rule.Namespace] = struct{}{}
		case "delete":
			allRules = deleteRule(allRules, change.Namespace, change.RuleID)
			affectedSet[change.Namespace] = struct{}{}
		case "replace_namespace":
			allRules = deleteNamespaceRules(allRules, change.Namespace)
			for _, rule := range change.Rules {
				rule.Namespace = change.Namespace
				allRules = append(allRules, model.NormalizeRule(rule))
			}
			affectedSet[change.Namespace] = struct{}{}
		}
	}
	namespaces := sortedSet(affectedSet)
	newVersion := current + 1

	intent := store.Intent{
		IntentID: newIntentID("increment", newVersion), RequestID: requestID, Kind: "increment_cascade_rebuild",
		TargetVersion: newVersion, Namespaces: jsonString(namespaces),
		PayloadJSON: jsonString(map[string]any{"baseVersion": current, "changes": len(increment.Changes)}),
		Status:      "prepared", CreatedAt: store.NowRFC3339(), UpdatedAt: store.NowRFC3339(),
	}
	if err := s.store.CreateIntent(ctx, intent); err != nil {
		return InternalError("failed to write increment intent", err)
	}
	s.appendIntentLog(intent)
	s.setPending(newVersion, namespaces, "increment_cascade_rebuild")
	defer s.clearPending()
	if s.cfg.Hooks.AfterIntent != nil {
		s.cfg.Hooks.AfterIntent(newVersion, namespaces)
	}
	s.crashIfConfigured("after_intent")
	s.waitRebuildDelay()

	grouped := groupRulesNoSort(allRules)
	oldCache := s.cache.Load()
	allViews := map[string]*compiler.CompiledNamespace{}
	if oldCache != nil {
		for namespace, view := range oldCache.views {
			allViews[namespace] = view
		}
	}
	changedViews := map[string]*compiler.CompiledNamespace{}
	digests := map[string]string{}
	for _, namespace := range namespaces {
		view := compiler.CompileNamespace(newVersion, namespace, grouped[namespace])
		changedViews[namespace] = view
		allViews[namespace] = view
		digests[namespace] = view.CompiledDigest
	}
	s.crashIfConfigured("mid_compile")
	globalReport := compiler.BuildGlobalReport(newVersion, allViews, 50_000)
	data := CommitData{
		RequestID: requestID, Kind: "increment", Version: newVersion, BaseVersion: current,
		CurrentVersion: newVersion, AffectedNamespaces: namespaces, CompiledDigests: digests, GlobalReport: globalReport,
	}
	success := SuccessResult(200, data)
	if s.cfg.Hooks.BeforeCommit != nil {
		s.cfg.Hooks.BeforeCommit(newVersion, namespaces)
	}

	tx, err := s.store.DB.BeginTx(ctx, nil)
	if err != nil {
		_ = s.store.MarkIntent(ctx, intent.IntentID, "failed", err.Error())
		return InternalError("failed to begin increment transaction", err)
	}
	defer func() { _ = tx.Rollback() }()

	derived := model.Snapshot{Version: newVersion, GeneratedAt: increment.GeneratedAt, Rules: allRules}
	if derived.GeneratedAt == "" {
		derived.GeneratedAt = store.NowRFC3339()
	}
	derived.Digest = model.DigestSnapshot(derived)
	canonical, _ := json.Marshal(increment)
	if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(version,kind,base_version,request_id,generated_at,digest,canonical_json,created_at) VALUES(?,?,?,?,?,?,?,?)`,
		newVersion, "increment", current, requestID, derived.GeneratedAt, derived.Digest, string(canonical), store.NowRFC3339()); err != nil {
		return failSnapshotTx(ctx, s, intent, "failed to insert increment version", err)
	}
	for _, rule := range allRules {
		ruleJSON, _ := json.Marshal(rule)
		if err := insertRuleTx(tx, newVersion, rule, ruleJSON); err != nil {
			return failSnapshotTx(ctx, s, intent, "failed to copy rule into new version", err)
		}
	}
	for _, namespace := range namespaces {
		if err := persistCompiledView(ctx, tx, newVersion, changedViews[namespace]); err != nil {
			return failSnapshotTx(ctx, s, intent, "failed to atomically rebuild namespace view", err)
		}
	}
	if err := persistGlobalReportTx(ctx, tx, newVersion, globalReport); err != nil {
		return failSnapshotTx(ctx, s, intent, "failed to persist cross-namespace consistency", err)
	}
	if err := upsertMetaTx(tx, newVersion); err != nil {
		return failSnapshotTx(ctx, s, intent, "failed to update version", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed', reason='', updated_at=? WHERE intent_id=?`, store.NowRFC3339(), intent.IntentID); err != nil {
		return failSnapshotTx(ctx, s, intent, "failed to commit intent", err)
	}
	if requestID != "" {
		if err := completeRequestTx(ctx, tx, requestID, success.Status, success.Body); err != nil {
			return failSnapshotTx(ctx, s, intent, "failed to persist idempotent response", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return InternalError("failed to commit increment transaction", err)
	}

	s.cache.Store(&cacheSnapshot{version: newVersion, views: allViews})
	s.clearPending()
	success.RequestPersisted = requestID != ""
	return success
}

func failSnapshotTx(ctx context.Context, s *Service, intent store.Intent, message string, cause error) RawResult {
	_ = s.store.MarkIntent(ctx, intent.IntentID, "failed", cause.Error())
	return InternalError(message, cause)
}

func completeRequestTx(ctx context.Context, tx *sql.Tx, requestID string, status int, body []byte) error {
	_, err := tx.ExecContext(ctx, `UPDATE requests SET status='succeeded',http_status=?,response_json=?,error_code='',completed_at=? WHERE request_id=?`,
		status, string(body), store.NowRFC3339(), requestID)
	return err
}

func persistGlobalReportTx(ctx context.Context, tx *sql.Tx, version int64, report *compiler.GlobalReport) error {
	data, err := json.Marshal(report)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO consistency_reports(snapshot_version,report_json,created_at) VALUES(?,?,?)`, version, string(data), store.NowRFC3339()); err != nil {
		return err
	}
	rows := []referenceRow{{Namespace: "*", Component: "cross_namespace_consistency", Field: "global_score_and_ranking", SourceVersion: version}}
	if report != nil {
		for _, item := range report.Namespaces {
			rows = append(rows, referenceRow{Namespace: item.Namespace, Component: "cross_namespace_consistency", Field: "namespace_consistency_score", SourceVersion: version})
		}
	}
	return insertReferencesTx(ctx, tx, version, rows)
}

func persistCompiledView(ctx context.Context, tx *sql.Tx, version int64, view *compiler.CompiledNamespace) error {
	selectorJSON, _ := json.Marshal(view.SelectorIndex)
	portJSON, _ := json.Marshal(view.PortView)
	decisionJSON, _ := json.Marshal(view.Adjudication)
	consistencyJSON, _ := json.Marshal(view.Consistency)
	compiledJSON, _ := json.Marshal(view)
	_, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO compile_views(snapshot_version,namespace,status,selector_index_json,port_view_json,decision_json,consistency_json,compiled_json,compiled_digest,created_at) VALUES(?,?, 'ready',?,?,?,?,?,?,?)`,
		version, view.Namespace, string(selectorJSON), string(portJSON), string(decisionJSON), string(consistencyJSON), string(compiledJSON), view.CompiledDigest, store.NowRFC3339())
	if err != nil {
		return err
	}
	return insertReferencesTx(ctx, tx, version, buildReferences(version, view))
}

func insertReferencesTx(ctx context.Context, tx *sql.Tx, version int64, rows []referenceRow) error {
	for _, row := range rows {
		if _, err := tx.ExecContext(ctx, `INSERT INTO view_references(snapshot_version,namespace,component,rule_id,field,source_version) VALUES(?,?,?,?,?,?)`,
			version, row.Namespace, row.Component, row.RuleID, row.Field, row.SourceVersion); err != nil {
			return err
		}
	}
	return nil
}

func buildReferences(version int64, view *compiler.CompiledNamespace) []referenceRow {
	rows := []referenceRow{}
	for _, rule := range view.Rules {
		rows = append(rows,
			referenceRow{Namespace: view.Namespace, Component: "selector_index", RuleID: rule.ID, Field: "source_selector", SourceVersion: version},
			referenceRow{Namespace: view.Namespace, Component: "selector_index", RuleID: rule.ID, Field: "destination_selector", SourceVersion: version},
			referenceRow{Namespace: view.Namespace, Component: "port_view", RuleID: rule.ID, Field: "port_ranges", SourceVersion: version},
			referenceRow{Namespace: view.Namespace, Component: "adjudication", RuleID: rule.ID, Field: "priority", SourceVersion: version},
			referenceRow{Namespace: view.Namespace, Component: "adjudication", RuleID: rule.ID, Field: "action", SourceVersion: version},
			referenceRow{Namespace: view.Namespace, Component: "consistency", RuleID: rule.ID, Field: "compiled_rule_count", SourceVersion: version},
			referenceRow{Namespace: view.Namespace, Component: "consistency", RuleID: rule.ID, Field: "port_interval_merge_rate", SourceVersion: version},
		)
	}
	for _, pair := range view.Adjudication.ConflictPairs {
		for index, ruleID := range pair.RuleIDs {
			rows = append(rows,
				referenceRow{Namespace: view.Namespace, Component: "conflict_pair", RuleID: ruleID, Field: fmt.Sprintf("conflict_pair_action_%d", index), SourceVersion: version},
				referenceRow{Namespace: view.Namespace, Component: "consistency", RuleID: ruleID, Field: "conflict_pair_ratio", SourceVersion: version},
			)
		}
	}
	return rows
}

func (s *Service) loadRules(ctx context.Context, version int64) ([]model.Rule, error) {
	if version == 0 {
		return nil, nil
	}
	rows, err := s.store.DB.QueryContext(ctx, `SELECT rule_json FROM rules WHERE snapshot_version=? ORDER BY namespace, rule_id`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rules []model.Rule
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var rule model.Rule
		if err := json.Unmarshal([]byte(raw), &rule); err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	return rules, rows.Err()
}

func groupRules(rules []model.Rule) ([]string, map[string][]model.Rule) {
	grouped := map[string][]model.Rule{}
	for _, rule := range rules {
		grouped[rule.Namespace] = append(grouped[rule.Namespace], rule)
	}
	return sortedSetFromMap(grouped), grouped
}

func groupRulesNoSort(rules []model.Rule) map[string][]model.Rule {
	_, grouped := groupRules(rules)
	return grouped
}

func sortedSetFromMap(groups map[string][]model.Rule) []string {
	namespaces := make([]string, 0, len(groups))
	for namespace := range groups {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)
	return namespaces
}

func sortedSet(set map[string]struct{}) []string {
	namespaces := make([]string, 0, len(set))
	for namespace := range set {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)
	return namespaces
}

func upsertRule(rules []model.Rule, replacement model.Rule) []model.Rule {
	for i := range rules {
		if rules[i].Namespace == replacement.Namespace && rules[i].ID == replacement.ID {
			rules[i] = replacement
			return rules
		}
	}
	return append(rules, replacement)
}

func deleteRule(rules []model.Rule, namespace, ruleID string) []model.Rule {
	result := rules[:0]
	for _, rule := range rules {
		if rule.Namespace == namespace && rule.ID == ruleID {
			continue
		}
		result = append(result, rule)
	}
	return result
}

func deleteNamespaceRules(rules []model.Rule, namespace string) []model.Rule {
	result := rules[:0]
	for _, rule := range rules {
		if rule.Namespace == namespace {
			continue
		}
		result = append(result, rule)
	}
	return result
}

func jsonString(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}
