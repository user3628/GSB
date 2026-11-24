package truststore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"
)

// ChainDiff records the verdict before and after one incremental apply.
type ChainDiff struct {
	ChainID string      `json:"chain_id"`
	Before  ChainResult `json:"before"`
	After   ChainResult `json:"after"`
	Changed bool        `json:"changed"`
}

// ApplyAudit is persisted with the resulting snapshot and reused by reports.
type ApplyAudit struct {
	Namespace      string               `json:"namespace"`
	RequestID      string               `json:"request_id"`
	Code           Code                 `json:"code"`
	SnapshotBefore SnapshotRef          `json:"snapshot_before"`
	SnapshotAfter  SnapshotRef          `json:"snapshot_after"`
	Replacement    bool                 `json:"replacement"`
	Changeset      Changeset            `json:"changeset"`
	Chains         []ChainDiff          `json:"chains"`
	Closure        *InvalidationClosure `json:"closure,omitempty"`
	Codes          []string             `json:"codes"`
}

// ApplyResult describes one committed snapshot transition.
type ApplyResult struct {
	Namespace      string               `json:"namespace"`
	RequestID      string               `json:"request_id"`
	Code           Code                 `json:"code"`
	SnapshotBefore SnapshotRef          `json:"snapshot_before"`
	SnapshotAfter  SnapshotRef          `json:"snapshot_after"`
	Replacement    bool                 `json:"replacement"`
	Chains         []ChainDiff          `json:"chains"`
	Closure        *InvalidationClosure `json:"closure,omitempty"`
	Codes          []string             `json:"codes"`
}

func (a ApplyAudit) Result() ApplyResult {
	return ApplyResult{
		Namespace:      a.Namespace,
		RequestID:      a.RequestID,
		Code:           a.Code,
		SnapshotBefore: a.SnapshotBefore,
		SnapshotAfter:  a.SnapshotAfter,
		Replacement:    a.Replacement,
		Chains:         a.Chains,
		Closure:        a.Closure,
		Codes:          a.Codes,
	}
}

// Apply atomically applies a change set. Per-namespace serialization, the
// in-flight semaphore and SQLite BEGIN IMMEDIATE jointly ensure a monotonic
// version chain. A failure at any point rolls back the entire transaction.
func (s *Store) Apply(ctx context.Context, namespace string, changeset Changeset, options ApplyOptions) (*ApplyResult, error) {
	opStart := time.Now()
	namespace = strings.TrimSpace(namespace)
	requestID := strings.TrimSpace(options.RequestID)
	if requestID == "" {
		requestID = "request-anonymous"
	}
	wait, err := s.acquire(ctx, namespace, requestID, options.WaitTimeout)
	if err != nil {
		return nil, err
	}
	defer s.release(namespace)

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		mapped := mapSQLiteError("apply changeset", "begin transaction", err)
		s.recordApplyContention(namespace, requestID, wait, opStart, mapped)
		return nil, mapped
	}
	defer tx.Rollback()
	loaded, err := s.loadNamespace(ctx, tx, namespace)
	if err != nil {
		return nil, err
	}
	if loaded.Health != nil {
		return nil, loaded.Health
	}
	before := loaded.Latest
	if options.ExpectedVersion != nil && *options.ExpectedVersion != before.Version {
		s.recordContention(ContentionRecord{Namespace: namespace, RequestID: requestID, WaitMillis: wait.Milliseconds(), Outcome: "rejected-version", Code: CodeTrustContention})
		return nil, NewError(CodeTrustContention, "apply changeset", fmt.Sprintf("expected version %d, found %d", *options.ExpectedVersion, before.Version), nil)
	}

	mutation, err := newMutationState(before.Content, changeset)
	if err != nil {
		return nil, err
	}
	if err := mutation.apply(changeset); err != nil {
		return nil, err
	}
	afterContent, err := mutation.finalContent()
	if err != nil {
		return nil, err
	}
	contentHash, err := afterContent.ContentHash()
	if err != nil {
		return nil, err
	}
	if contentHash == before.ContentHash {
		return nil, invalid("apply changeset", "changeset produced no state change")
	}
	trustHash, err := afterContent.TrustHash()
	if err != nil {
		return nil, err
	}
	replacement := detectWholeReplacement(before.Content, afterContent)
	after := Snapshot{
		Namespace:     namespace,
		Version:       before.Version + 1,
		ParentVersion: before.Version,
		ContentHash:   contentHash,
		TrustHash:     trustHash,
		Replacement:   replacement,
		Content:       afterContent,
	}

	beforeChains := VerifyAll(before.Content, VerificationOptions{})
	afterChains := VerifyAll(afterContent, VerificationOptions{})
	closure, err := buildInvalidationClosure(afterContent, mutation.originIDs(), VerificationOptions{})
	if err != nil {
		return nil, NewError(CodeDependencyInvalidated, "compute invalidation closure", "", err)
	}
	codes := collectCodes(beforeChains, afterChains, closure, CodeOK)
	audit := ApplyAudit{
		Namespace:      namespace,
		RequestID:      requestID,
		Code:           CodeOK,
		SnapshotBefore: before.Ref(),
		SnapshotAfter:  after.Ref(),
		Replacement:    replacement,
		Changeset:      changeset,
		Chains:         chainDiffs(beforeChains, afterChains),
		Closure:        closure,
		Codes:          codes,
	}
	contentJSON, err := json.Marshal(afterContent)
	if err != nil {
		return nil, NewError(CodeStoreFailure, "apply changeset", "encode snapshot", err)
	}
	auditJSON, err := json.Marshal(audit)
	if err != nil {
		return nil, NewError(CodeStoreFailure, "apply changeset", "encode audit", err)
	}
	if err := insertSnapshot(ctx, tx, after, contentJSON, auditJSON); err != nil {
		return nil, err
	}
	update, err := tx.ExecContext(ctx, `UPDATE namespaces SET current_version = ? WHERE namespace = ? AND current_version = ?`, after.Version, namespace, before.Version)
	if err != nil {
		mapped := mapSQLiteError("apply changeset", "advance namespace", err)
		s.recordApplyContention(namespace, requestID, wait, opStart, mapped)
		return nil, mapped
	}
	affected, _ := update.RowsAffected()
	if affected != 1 {
		return nil, NewError(CodeTrustContention, "apply changeset", "namespace version changed concurrently", nil)
	}
	if err := runDiagnosticPoint(options.Diagnostics, "before-commit", after.Version); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		mapped := mapSQLiteError("apply changeset", "commit", err)
		s.recordApplyContention(namespace, requestID, wait, opStart, mapped)
		return nil, mapped
	}
	if err := runDiagnosticAfterCommit(options.Diagnostics, after.Version); err != nil {
		result := audit.Result()
		return &result, err
	}
	result := audit.Result()
	return &result, nil
}

func (s *Store) recordApplyContention(namespace, requestID string, gateWait time.Duration, opStart time.Time, err error) {
	if err == nil || CodeOf(err) != CodeTrustContention {
		return
	}
	s.recordContention(ContentionRecord{
		Namespace:  namespace,
		RequestID:  requestID,
		WaitMillis: (gateWait + time.Since(opStart)).Milliseconds(),
		Outcome:    "rejected-store",
		Code:       CodeTrustContention,
	})
}

func runDiagnosticPoint(diagnostics *ApplyDiagnostics, stage string, version int64) error {
	if diagnostics == nil {
		return nil
	}
	if diagnostics.MarkerBeforeCommit != "" {
		if err := writeDiagnosticMarker(diagnostics.MarkerBeforeCommit, stage, version); err != nil {
			return NewError(CodeStoreFailure, "apply diagnostics", "write before-commit marker", err)
		}
	}
	if diagnostics.HoldBeforeCommit > 0 {
		time.Sleep(diagnostics.HoldBeforeCommit)
	}
	return nil
}

func runDiagnosticAfterCommit(diagnostics *ApplyDiagnostics, version int64) error {
	if diagnostics == nil {
		return nil
	}
	if diagnostics.MarkerAfterCommit != "" {
		if err := writeDiagnosticMarker(diagnostics.MarkerAfterCommit, "after-commit", version); err != nil {
			return NewError(CodeStoreFailure, "apply diagnostics", "write after-commit marker", err)
		}
	}
	if diagnostics.HoldAfterCommit > 0 {
		time.Sleep(diagnostics.HoldAfterCommit)
	}
	return nil
}

func writeDiagnosticMarker(path, stage string, version int64) error {
	return os.WriteFile(path, []byte(fmt.Sprintf("%s version=%d\n", stage, version)), 0o600)
}

func mapSQLiteError(op, message string, err error) error {
	if err == nil {
		return nil
	}
	text := strings.ToLower(err.Error())
	if strings.Contains(text, "database is locked") || strings.Contains(text, "database table is locked") || strings.Contains(text, "busy") {
		return NewError(CodeTrustContention, op, message, err)
	}
	return NewError(CodeStoreFailure, op, message, err)
}

func chainDiffs(before, after []ChainResult) []ChainDiff {
	beforeByID := make(map[string]ChainResult, len(before))
	afterByID := make(map[string]ChainResult, len(after))
	ids := make(map[string]struct{}, len(before)+len(after))
	for _, result := range before {
		beforeByID[result.ChainID] = result
		ids[result.ChainID] = struct{}{}
	}
	for _, result := range after {
		afterByID[result.ChainID] = result
		ids[result.ChainID] = struct{}{}
	}
	out := make([]ChainDiff, 0, len(ids))
	for id := range ids {
		beforeResult := beforeByID[id]
		afterResult := afterByID[id]
		out = append(out, ChainDiff{
			ChainID: id,
			Before:  beforeResult,
			After:   afterResult,
			Changed: !chainResultsEqual(beforeResult, afterResult),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ChainID != out[j].ChainID {
			return out[i].ChainID < out[j].ChainID
		}
		if out[i].After.Code != out[j].After.Code {
			return out[i].After.Code < out[j].After.Code
		}
		return out[i].Before.Code < out[j].Before.Code
	})
	return out
}

func chainResultsEqual(a, b ChainResult) bool {
	return a.ChainID == b.ChainID && a.Code == b.Code && a.Detail == b.Detail && a.ScopeID == b.ScopeID && a.AnchorID == b.AnchorID && reflect.DeepEqual(a.PathIDs, b.PathIDs) && reflect.DeepEqual(a.PathNames, b.PathNames)
}

func collectCodes(before, after []ChainResult, closure *InvalidationClosure, applyCode Code) []string {
	set := make(map[string]struct{})
	if applyCode != "" {
		set[string(applyCode)] = struct{}{}
	}
	for _, result := range before {
		set[string(result.Code)] = struct{}{}
	}
	for _, result := range after {
		set[string(result.Code)] = struct{}{}
	}
	if closure != nil && closure.Code != "" {
		set[string(closure.Code)] = struct{}{}
	}
	if len(set) == 0 {
		set[string(CodeOK)] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for code := range set {
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}

// Verify validates one named host chain against the current snapshot.
func (s *Store) Verify(ctx context.Context, namespace, chainID string) (ChainResult, error) {
	return s.VerifyWithOptions(ctx, namespace, chainID, VerificationOptions{})
}

// VerifyWithOptions is Verify with an injectable clock for deterministic tests.
func (s *Store) VerifyWithOptions(ctx context.Context, namespace, chainID string, options VerificationOptions) (ChainResult, error) {
	snapshot, err := s.Load(ctx, namespace)
	if err != nil {
		code := CodeOf(err)
		return ChainResult{ChainID: chainID, Code: code, Detail: err.Error(), PathIDs: []string{}, PathNames: []string{}}, err
	}
	for _, chain := range snapshot.Content.HostChains {
		if chain.ID == chainID {
			return ValidateChain(snapshot.Content, chain, options), nil
		}
	}
	return ChainResult{ChainID: chainID, Code: CodeNotFound, PathIDs: []string{}, PathNames: []string{}}, NewError(CodeNotFound, "verify chain", fmt.Sprintf("chain %q does not exist", chainID), nil)
}

// VerifyAll validates all registered host chains in the current snapshot.
func (s *Store) VerifyAll(ctx context.Context, namespace string) ([]ChainResult, error) {
	snapshot, err := s.Load(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return VerifyAll(snapshot.Content, VerificationOptions{}), nil
}
