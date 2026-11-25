package store

import (
	"context"
	"sort"
	"strings"
	"time"

	"example.com/gsb-router/internal/errs"
	"example.com/gsb-router/internal/model"
)

func errsConflict(commitID, storedHash, incomingHash string) *errs.Error {
	return errs.New(errs.CodeConflictingRequest, "commit id was already used with different content", map[string]any{
		"commit_id":     commitID,
		"stored_hash":   storedHash,
		"incoming_hash": incomingHash,
	})
}

func (s *Store) CommitSnapshot(ctx context.Context, req SnapshotCommitRequest) (CommitResult, error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	contentHash, err := fingerprint(struct {
		Snapshot     model.Snapshot `json:"snapshot"`
		SealChecksum bool           `json:"seal_checksum"`
		Inject       string         `json:"inject"`
	}{req.Snapshot, req.SealChecksum, req.Inject})
	if err != nil {
		return CommitResult{}, internalError("fingerprint snapshot request", err)
	}
	if strings.TrimSpace(req.CommitID) == "" {
		req.CommitID = "auto-snapshot-" + contentHash[:16]
	}
	if existing, err := s.existingCommit(ctx, req.CommitID, contentHash); err != nil {
		return CommitResult{}, err
	} else if existing != nil {
		return *existing, nil
	}

	current := s.currentState()
	snapshot := req.Snapshot.Clone()
	if applySnapshotFault(&snapshot, req.Inject) {
		req.SealChecksum = true
	}
	if req.Inject != "" && req.Inject != InjectMappingMissing && req.Inject != InjectRangeOverlap {
		return CommitResult{}, errs.Snapshot(errs.CodeSnapshotInvalid, "inject mode is not valid for snapshot commit", map[string]any{"inject": req.Inject})
	}
	validated, err := model.ValidateSnapshot(snapshot, model.ValidationOptions{ExpectedVersion: current.version, SealChecksum: req.SealChecksum})
	if err != nil {
		return CommitResult{}, err
	}
	builtAt := nowString(s.now())
	caches, aggregates, err := buildNextCaches(validated, current.caches, current.aggregates, allTableIDs(validated), allViewIDs(validated), true, builtAt)
	if err != nil {
		return CommitResult{}, internalError("build caches", err)
	}
	affectedTables := allTableIDs(validated)
	affectedViews := allViewIDs(validated)
	intentID, err := s.persistIntent(ctx, req.CommitID, "snapshot", current.version, validated.Version, req)
	if err != nil {
		return CommitResult{}, internalError("persist snapshot intent", err)
	}
	if req.Inject == InjectRebuildInterrupt {
		return CommitResult{}, interrupted(intentID, req.CommitID, "snapshot", current.version, validated.Version)
	}

	result := CommitResult{
		CommitID:       req.CommitID,
		Kind:           "snapshot",
		Version:        validated.Version,
		Checksum:       validated.Checksum,
		AffectedTables: sortedSet(affectedTables),
		AffectedViews:  sortedSet(affectedViews),
		CommittedAt:    builtAt,
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.markIntent(ctx, intentID, "aborted", "transaction_begin_failed")
		return CommitResult{}, internalError("begin snapshot transaction", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := persistSnapshotTx(ctx, tx, validated, builtAt); err != nil {
		s.markIntent(ctx, intentID, "aborted", "snapshot_write_failed")
		return CommitResult{}, internalError("persist snapshot", err)
	}
	if err := replaceAllCachesTx(ctx, tx, caches, aggregates); err != nil {
		s.markIntent(ctx, intentID, "aborted", "cache_write_failed")
		return CommitResult{}, internalError("persist snapshot caches", err)
	}
	if err := insertCommitTx(ctx, tx, result, contentHash); err != nil {
		s.markIntent(ctx, intentID, "aborted", "receipt_write_failed")
		return CommitResult{}, internalError("persist snapshot receipt", err)
	}
	if err := markIntentCommittedTx(ctx, tx, intentID, builtAt); err != nil {
		s.markIntent(ctx, intentID, "aborted", "intent_finalize_failed")
		return CommitResult{}, internalError("finalize snapshot intent", err)
	}
	if err := tx.Commit(); err != nil {
		s.markIntent(ctx, intentID, "aborted", "transaction_commit_failed")
		return CommitResult{}, internalError("commit snapshot transaction", err)
	}

	s.mu.Lock()
	s.state = buildRuntimeState(validated, caches, aggregates)
	s.mu.Unlock()
	return result, nil
}

func (s *Store) CommitIncrement(ctx context.Context, inc model.Increment) (CommitResult, error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	contentHash, err := fingerprint(inc)
	if err != nil {
		return CommitResult{}, internalError("fingerprint increment", err)
	}
	if strings.TrimSpace(inc.CommitID) == "" {
		inc.CommitID = "auto-increment-" + contentHash[:16]
	}
	if existing, err := s.existingCommit(ctx, inc.CommitID, contentHash); err != nil {
		return CommitResult{}, err
	} else if existing != nil {
		return *existing, nil
	}
	current := s.currentState()
	if inc.Inject == InjectBaseRegression {
		inc.BaseVersion = current.version - 1
	}
	if inc.BaseVersion != current.version {
		return CommitResult{}, staleBase(inc.BaseVersion, current.version, inc.CommitID)
	}
	if strings.TrimSpace(inc.CommitID) == "" {
		return CommitResult{}, errs.New(errs.CodeInvalidJSON, "commit_id is required", nil)
	}
	// Fault-injection modes deliberately alter request semantics before the
	// merged snapshot is judged. A checksum attached to the original, valid
	// increment must not mask the requested fault code.
	if inc.Checksum != "" && inc.Inject == "" {
		sum, err := model.IncrementChecksum(inc)
		if err != nil || !strings.EqualFold(sum, inc.Checksum) {
			return CommitResult{}, errs.Snapshot(errs.CodeSnapshotInvalid, "increment checksum mismatch", map[string]any{"expected": sum, "actual": inc.Checksum})
		}
	}
	switch inc.Inject {
	case "", InjectMappingMissing, InjectRangeOverlap, InjectBaseRegression, InjectRebuildInterrupt:
	default:
		return CommitResult{}, errs.Snapshot(errs.CodeSnapshotInvalid, "unknown inject mode", map[string]any{"inject": inc.Inject})
	}

	candidate, affectedTables, affectedViews, err := applyIncrement(current.snapshot, inc)
	if err != nil {
		return CommitResult{}, err
	}
	if applySnapshotFault(&candidate, inc.Inject) {
		// Faults are intentionally inserted into the merged result so the same
		// snapshot validator judges both normal and injected commits.
	}
	validated, err := model.ValidateSnapshot(candidate, model.ValidationOptions{ExpectedVersion: current.version, SealChecksum: true})
	if err != nil {
		return CommitResult{}, err
	}
	builtAt := nowString(s.now())
	caches, aggregates, err := buildNextCaches(validated, current.caches, current.aggregates, affectedTables, affectedViews, false, builtAt)
	if err != nil {
		return CommitResult{}, internalError("build incremental caches", err)
	}
	intentID, err := s.persistIntent(ctx, inc.CommitID, "increment", current.version, validated.Version, inc)
	if err != nil {
		return CommitResult{}, internalError("persist increment intent", err)
	}
	if inc.Inject == InjectRebuildInterrupt {
		return CommitResult{}, interrupted(intentID, inc.CommitID, "increment", current.version, validated.Version)
	}

	tableList := sortedSet(affectedTables)
	viewList := sortedSet(affectedViews)
	result := CommitResult{
		CommitID:       inc.CommitID,
		Kind:           "increment",
		Version:        validated.Version,
		Checksum:       validated.Checksum,
		AffectedTables: tableList,
		AffectedViews:  viewList,
		CommittedAt:    builtAt,
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.markIntent(ctx, intentID, "aborted", "transaction_begin_failed")
		return CommitResult{}, internalError("begin increment transaction", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := persistSnapshotTx(ctx, tx, validated, builtAt); err != nil {
		s.markIntent(ctx, intentID, "aborted", "snapshot_write_failed")
		return CommitResult{}, internalError("persist increment snapshot", err)
	}
	if err := replaceAllCachesTx(ctx, tx, caches, aggregates); err != nil {
		s.markIntent(ctx, intentID, "aborted", "cache_write_failed")
		return CommitResult{}, internalError("persist increment caches", err)
	}
	if err := insertCommitTx(ctx, tx, result, contentHash); err != nil {
		s.markIntent(ctx, intentID, "aborted", "receipt_write_failed")
		return CommitResult{}, internalError("persist increment receipt", err)
	}
	if err := markIntentCommittedTx(ctx, tx, intentID, builtAt); err != nil {
		s.markIntent(ctx, intentID, "aborted", "intent_finalize_failed")
		return CommitResult{}, internalError("finalize increment intent", err)
	}
	if err := tx.Commit(); err != nil {
		s.markIntent(ctx, intentID, "aborted", "transaction_commit_failed")
		return CommitResult{}, internalError("commit increment transaction", err)
	}

	s.mu.Lock()
	s.state = buildRuntimeState(validated, caches, aggregates)
	s.mu.Unlock()
	return result, nil
}

func applyIncrement(base model.Snapshot, inc model.Increment) (model.Snapshot, map[string]struct{}, map[string]struct{}, error) {
	candidate := base.Clone()
	candidate.Version = base.Version + 1
	if inc.GeneratedAt == "" {
		candidate.GeneratedAt = nowString(timeNowUTC())
	} else {
		candidate.GeneratedAt = inc.GeneratedAt
	}
	candidate.Checksum = ""
	affectedTables := map[string]struct{}{}
	affectedViews := map[string]struct{}{}

	physical := make(map[string]model.PhysicalTable, len(candidate.PhysicalTables))
	for _, pt := range candidate.PhysicalTables {
		physical[pt.ID] = pt
	}
	for _, pt := range inc.AddPhysicalTables {
		if existing, ok := physical[pt.ID]; ok {
			if existing.LogicalDB != pt.LogicalDB || existing.Name != pt.Name {
				return model.Snapshot{}, nil, nil, errs.Snapshot(errs.CodeSnapshotInvalid, "physical table id already exists with different mapping", map[string]any{"physical_table": pt.ID})
			}
			continue
		}
		physical[pt.ID] = pt
		candidate.PhysicalTables = append(candidate.PhysicalTables, pt)
	}
	if len(inc.Changes) == 0 {
		return model.Snapshot{}, nil, nil, errs.Snapshot(errs.CodeSnapshotInvalid, "increment must contain at least one change", map[string]any{"commit_id": inc.CommitID})
	}

	tableIndex := make(map[string]int, len(candidate.LogicalTables))
	for i, table := range candidate.LogicalTables {
		tableIndex[table.ID] = i
	}
	for _, change := range inc.Changes {
		tableID := strings.TrimSpace(change.TableID)
		if tableID == "" && change.Table != nil {
			tableID = change.Table.ID
		}
		if tableID == "" {
			return model.Snapshot{}, nil, nil, errs.Snapshot(errs.CodeSnapshotInvalid, "increment change requires table_id", nil)
		}
		switch change.Op {
		case model.ChangeUpsertTable:
			if change.Table == nil {
				return model.Snapshot{}, nil, nil, errs.Snapshot(errs.CodeSnapshotInvalid, "upsert_table requires table", map[string]any{"table_id": tableID})
			}
			table := *change.Table
			if table.ID == "" {
				table.ID = tableID
			}
			if table.ID != tableID {
				return model.Snapshot{}, nil, nil, errs.Snapshot(errs.CodeSnapshotInvalid, "table id does not match change table_id", map[string]any{"table_id": tableID, "table_payload_id": table.ID})
			}
			if idx, ok := tableIndex[tableID]; ok {
				candidate.LogicalTables[idx] = table
			} else {
				tableIndex[tableID] = len(candidate.LogicalTables)
				candidate.LogicalTables = append(candidate.LogicalTables, table)
			}
		case model.ChangeReplaceRanges:
			idx, ok := tableIndex[tableID]
			if !ok {
				return model.Snapshot{}, nil, nil, errs.Snapshot(errs.CodeSnapshotInvalid, "replace_ranges target table does not exist", map[string]any{"table_id": tableID})
			}
			candidate.LogicalTables[idx].Ranges = append([]model.ShardRange(nil), change.Ranges...)
		case model.ChangeDeleteTable:
			idx, ok := tableIndex[tableID]
			if !ok {
				return model.Snapshot{}, nil, nil, errs.Snapshot(errs.CodeSnapshotInvalid, "delete_table target table does not exist", map[string]any{"table_id": tableID})
			}
			candidate.LogicalTables = append(candidate.LogicalTables[:idx], candidate.LogicalTables[idx+1:]...)
			tableIndex = make(map[string]int, len(candidate.LogicalTables))
			for i, table := range candidate.LogicalTables {
				tableIndex[table.ID] = i
			}
		default:
			return model.Snapshot{}, nil, nil, errs.Snapshot(errs.CodeSnapshotInvalid, "unsupported increment change op", map[string]any{"table_id": tableID, "op": change.Op})
		}
		affectedTables[tableID] = struct{}{}
	}

	if len(inc.Aggregates) > 0 {
		viewIndex := make(map[string]int, len(candidate.Aggregates))
		for i, view := range candidate.Aggregates {
			viewIndex[view.ID] = i
		}
		for _, view := range inc.Aggregates {
			if idx, ok := viewIndex[view.ID]; ok {
				candidate.Aggregates[idx] = view
			} else {
				viewIndex[view.ID] = len(candidate.Aggregates)
				candidate.Aggregates = append(candidate.Aggregates, view)
			}
			affectedViews[view.ID] = struct{}{}
		}
	}
	// Any aggregate that consumes a changed table is a downstream cache and
	// must be rebuilt in the same transaction.
	for _, view := range candidate.Aggregates {
		for _, dep := range view.DependsOn {
			if _, changed := affectedTables[dep]; changed {
				affectedViews[view.ID] = struct{}{}
			}
		}
	}
	return candidate, affectedTables, affectedViews, nil
}

func buildNextCaches(snapshot model.Snapshot, previous map[string]RouteCache, previousAggregates map[string]AggregateCache, affectedTables, affectedViews map[string]struct{}, full bool, builtAt string) (map[string]RouteCache, map[string]AggregateCache, error) {
	next := make(map[string]RouteCache, len(snapshot.LogicalTables))
	for _, table := range snapshot.LogicalTables {
		if !full {
			if _, affected := affectedTables[table.ID]; !affected {
				if existing, ok := previous[table.ID]; ok {
					next[table.ID] = existing
					continue
				}
			}
		}
		cache, err := buildRouteCache(table, snapshot.Version, snapshot.Version, builtAt)
		if err != nil {
			return nil, nil, err
		}
		next[table.ID] = cache
	}

	nextAggregates := make(map[string]AggregateCache, len(snapshot.Aggregates))
	for _, view := range snapshot.Aggregates {
		if !full {
			if _, affected := affectedViews[view.ID]; !affected {
				if existing, ok := previousAggregates[view.ID]; ok {
					nextAggregates[view.ID] = existing
					continue
				}
			}
		}
		cache, err := buildAggregateCache(view, snapshot, next, snapshot.Version, snapshot.Version, builtAt)
		if err != nil {
			return nil, nil, err
		}
		nextAggregates[view.ID] = cache
	}
	return next, nextAggregates, nil
}

func (s *Store) currentState() *runtimeState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func allTableIDs(snapshot model.Snapshot) map[string]struct{} {
	ids := make(map[string]struct{}, len(snapshot.LogicalTables))
	for _, table := range snapshot.LogicalTables {
		ids[table.ID] = struct{}{}
	}
	return ids
}

func allViewIDs(snapshot model.Snapshot) map[string]struct{} {
	ids := make(map[string]struct{}, len(snapshot.Aggregates))
	for _, view := range snapshot.Aggregates {
		ids[view.ID] = struct{}{}
	}
	return ids
}

func sortedSet(set map[string]struct{}) []string {
	items := make([]string, 0, len(set))
	for item := range set {
		items = append(items, item)
	}
	sort.Strings(items)
	return items
}

func staleBase(base, current int64, commitID string) *errs.Error {
	return errs.New(errs.CodeStaleBase, "increment base version is not the current committed version", map[string]any{
		"commit_id":       commitID,
		"base_version":    base,
		"current_version": current,
	})
}

func interrupted(intentID, commitID, kind string, base, target int64) *errs.Error {
	return errs.New(errs.CodeRebuildInterrupted, "rebuild interruption marker persisted; process termination is scheduled", map[string]any{
		"intent_id":      intentID,
		"commit_id":      commitID,
		"kind":           kind,
		"base_version":   base,
		"target_version": target,
		"persisted":      true,
	})
}

func internalError(operation string, err error) *errs.Error {
	return errs.New(errs.CodeInternal, operation, map[string]any{"cause": err.Error()})
}

func timeNowUTC() time.Time { return time.Now().UTC() }
