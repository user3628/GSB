package edgeconfig

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"sort"
)

func (s *Service) SubmitSnapshot(ctx context.Context, snapshot Snapshot) (*CommitResult, error) {
	if snapshot.CommitID == "" {
		return nil, NewAPIError(http.StatusBadRequest, CodeValidation, "commit_id is required", nil)
	}
	expectedDigest, err := SnapshotDigest(snapshot)
	if err != nil {
		return nil, NewAPIError(http.StatusBadRequest, CodeInvalidJSON, "cannot compute snapshot digest", err.Error())
	}
	if snapshot.Digest != expectedDigest {
		return nil, NewAPIError(http.StatusUnprocessableEntity, CodeChecksumMismatch, "snapshot digest does not match canonical payload", map[string]string{
			"provided": snapshot.Digest,
			"expected": expectedDigest,
		})
	}

	return runWrite(s, ctx, "snapshot", snapshot.CommitID, snapshot, func(job *writeJob[CommitResult]) (*CommitResult, *APIError) {
		current, apiErr := currentVersionAPI(job.ctx, s.store)
		if apiErr != nil {
			return nil, apiErr
		}
		if snapshot.Version <= current {
			return nil, NewAPIError(http.StatusConflict, CodeStaleSnapshot, "snapshot version is not newer than committed version", map[string]any{
				"submitted_version": snapshot.Version,
				"current_version":   current,
			})
		}
		topology, err := s.LoadTopology(job.ctx)
		if err != nil {
			return nil, internalError(err)
		}
		candidate := Topology{Keys: snapshot.Keys, Nodes: topology.Nodes, Policy: topology.Policy}
		if err := ValidateTopology(candidate); err != nil {
			return nil, asAPIErrorOrInternal(err)
		}
		policy := withDefaultPolicy(topology.Policy)
		graph := NewGraph(snapshot.Keys, topology.Nodes, policy)
		views, err := BuildExpectedViews(snapshot.Version, graph, nil)
		if err != nil {
			return nil, internalError(err)
		}
		changedKeys := snapshotChangedKeys(topology.Keys, snapshot.Keys)
		affected := graph.AnalyzeImpact(ImpactInput{
			BaseVersion:   current,
			TargetVersion: snapshot.Version,
			ChangedKeys:   changedKeys,
		})
		result := &CommitResult{
			Kind:     "snapshot",
			CommitID: snapshot.CommitID,
			Version:  snapshot.Version,
			Digest:   snapshot.Digest,
			Affected: affected,
			Views:    views,
		}

		apiErr = job.Complete(func(tx *sql.Tx) error {
			now := s.NowMS()
			if _, err := tx.ExecContext(job.ctx, `DELETE FROM config_keys`); err != nil {
				return err
			}
			for _, key := range snapshot.Keys {
				valueJSON, err := json.Marshal(key.Value)
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(job.ctx, `
					INSERT INTO config_keys(path, type, value_json, order_no, updated_version)
					VALUES(?, ?, ?, ?, ?)`, key.Path, key.Type, string(valueJSON), key.OrderNo, snapshot.Version); err != nil {
					return err
				}
			}
			canonical, err := canonicalJSON(struct {
				Version       int64       `json:"version"`
				GeneratedAtMS int64       `json:"generated_at_ms"`
				Digest        string      `json:"digest"`
				Keys          []ConfigKey `json:"keys"`
			}{snapshot.Version, snapshot.GeneratedAtMS, snapshot.Digest, snapshot.Keys})
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(job.ctx, `
				INSERT INTO snapshots(version, commit_id, generated_at_ms, digest, canonical_json, complete, created_at_ms)
				VALUES(?, ?, ?, ?, ?, 1, ?)`,
				snapshot.Version, snapshot.CommitID, snapshot.GeneratedAtMS, snapshot.Digest, string(canonical), now); err != nil {
				return err
			}
			if err := replaceExpectedViewsTx(job.ctx, tx, views); err != nil {
				return err
			}
			if err := upsertExpectedConvergenceTx(job.ctx, tx, views, snapshot.Version, now); err != nil {
				return err
			}
			if err := saveAffectedReportTx(job.ctx, tx, snapshot.Version, affected, now); err != nil {
				return err
			}
			consistency, err := refreshAndRecomputeTx(job.ctx, tx, snapshot.Version, "", topology.Nodes, policy)
			if err != nil {
				return err
			}
			result.Consistency = consistency
			return s.store.setMetaTx(job.ctx, tx, "current_version", snapshot.Version)
		}, result, snapshot.Version)
		if apiErr != nil {
			return nil, apiErr
		}
		return result, nil
	})
}

func (s *Service) SubmitIncrement(ctx context.Context, increment Increment) (*CommitResult, error) {
	if increment.CommitID == "" {
		return nil, NewAPIError(http.StatusBadRequest, CodeValidation, "commit_id is required", nil)
	}
	return runWrite(s, ctx, "increment", increment.CommitID, increment, func(job *writeJob[CommitResult]) (*CommitResult, *APIError) {
		current, apiErr := currentVersionAPI(job.ctx, s.store)
		if apiErr != nil {
			return nil, apiErr
		}
		if increment.BaseVersion != current {
			return nil, NewAPIError(http.StatusConflict, CodeStaleBase, "increment base_version is not the current committed version", map[string]any{
				"base_version":          increment.BaseVersion,
				"current_version":       current,
				"expected_base_version": current,
			})
		}
		topology, err := s.LoadTopology(job.ctx)
		if err != nil {
			return nil, internalError(err)
		}
		policy := withDefaultPolicy(topology.Policy)
		keyMap := make(map[string]ConfigKey, len(topology.Keys))
		for _, key := range topology.Keys {
			keyMap[key.Path] = key
		}
		changedKeys := make([]string, 0, len(increment.Changes)+len(increment.RemoveKeys))
		for _, key := range increment.Changes {
			if !ValidKeyPath(key.Path) || !allowedKeyTypes[key.Type] {
				return nil, NewAPIError(http.StatusBadRequest, CodeValidation, "invalid changed config key", key)
			}
			keyMap[key.Path] = key
			changedKeys = append(changedKeys, key.Path)
		}
		for _, removed := range increment.RemoveKeys {
			delete(keyMap, removed)
			changedKeys = append(changedKeys, removed)
		}

		nodeMap := make(map[string]Node, len(topology.Nodes))
		for _, node := range topology.Nodes {
			nodeMap[node.ID] = node
		}
		seedTriggers := make(map[string][]string)
		for _, change := range increment.SubscriptionChanges {
			node, ok := nodeMap[change.NodeID]
			if !ok {
				return nil, NewAPIError(http.StatusNotFound, CodeNotFound, "subscription change references unknown node", change)
			}
			subSet := make(map[string]bool, len(node.SubscribedKeys))
			for _, keyPath := range node.SubscribedKeys {
				subSet[keyPath] = true
			}
			for _, keyPath := range change.Remove {
				delete(subSet, keyPath)
				seedTriggers[node.ID] = append(seedTriggers[node.ID], keyPath)
			}
			for _, keyPath := range change.Add {
				if _, exists := keyMap[keyPath]; !exists {
					return nil, NewAPIError(http.StatusBadRequest, CodeUndeclaredKey, "subscription add references undeclared key: "+keyPath, change)
				}
				subSet[keyPath] = true
				seedTriggers[node.ID] = append(seedTriggers[node.ID], keyPath)
			}
			subs := make([]string, 0, len(subSet))
			for keyPath := range subSet {
				subs = append(subs, keyPath)
			}
			sort.Strings(subs)
			node.SubscribedKeys = subs
			nodeMap[node.ID] = node
		}

		nodes := make([]Node, 0, len(nodeMap))
		for _, node := range nodeMap {
			for _, keyPath := range node.SubscribedKeys {
				if _, exists := keyMap[keyPath]; !exists {
					return nil, NewAPIError(http.StatusBadRequest, CodeUndeclaredKey, "subscription references removed or undeclared key: "+keyPath, map[string]string{
						"node_id":  node.ID,
						"key_path": keyPath,
					})
				}
			}
			nodes = append(nodes, node)
		}
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
		keys := make([]ConfigKey, 0, len(keyMap))
		for _, key := range keyMap {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].Path < keys[j].Path })

		newVersion := current + 1
		graph := NewGraph(keys, nodes, policy)
		previous, err := loadExpectedViews(job.ctx, s.db, current)
		if err != nil {
			return nil, internalError(err)
		}
		views, err := BuildExpectedViews(newVersion, graph, previous)
		if err != nil {
			return nil, internalError(err)
		}
		affected := graph.AnalyzeImpact(ImpactInput{
			BaseVersion:   current,
			TargetVersion: newVersion,
			ChangedKeys:   changedKeys,
			SeedTriggers:  seedTriggers,
		})
		result := &CommitResult{
			Kind:        "increment",
			CommitID:    increment.CommitID,
			BaseVersion: increment.BaseVersion,
			Version:     newVersion,
			Affected:    affected,
			Views:       views,
		}

		apiErr = job.Complete(func(tx *sql.Tx) error {
			now := s.NowMS()
			for _, key := range increment.Changes {
				valueJSON, err := json.Marshal(key.Value)
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(job.ctx, `
					INSERT INTO config_keys(path, type, value_json, order_no, updated_version)
					VALUES(?, ?, ?, ?, ?)
					ON CONFLICT(path) DO UPDATE SET type=excluded.type, value_json=excluded.value_json,
						order_no=excluded.order_no, updated_version=excluded.updated_version`,
					key.Path, key.Type, string(valueJSON), key.OrderNo, newVersion); err != nil {
					return err
				}
			}
			for _, keyPath := range increment.RemoveKeys {
				if _, err := tx.ExecContext(job.ctx, `DELETE FROM config_keys WHERE path=?`, keyPath); err != nil {
					return err
				}
			}
			for _, change := range increment.SubscriptionChanges {
				if _, err := tx.ExecContext(job.ctx, `DELETE FROM subscriptions WHERE node_id=?`, change.NodeID); err != nil {
					return err
				}
				node := nodeMap[change.NodeID]
				for position, keyPath := range node.SubscribedKeys {
					if _, err := tx.ExecContext(job.ctx, `
						INSERT INTO subscriptions(node_id, key_path, position) VALUES(?, ?, ?)`,
						node.ID, keyPath, position); err != nil {
						return err
					}
				}
			}
			payloadJSON, err := json.Marshal(increment)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(job.ctx, `
				INSERT INTO increments(version, base_version, commit_id, payload_json, status, created_at_ms)
				VALUES(?, ?, ?, ?, 'committed', ?)`,
				newVersion, increment.BaseVersion, increment.CommitID, string(payloadJSON), now); err != nil {
				return err
			}
			if err := replaceExpectedViewsTx(job.ctx, tx, views); err != nil {
				return err
			}
			if err := upsertExpectedConvergenceTx(job.ctx, tx, views, newVersion, now); err != nil {
				return err
			}
			if err := saveAffectedReportTx(job.ctx, tx, newVersion, affected, now); err != nil {
				return err
			}
			consistency, err := refreshAndRecomputeTx(job.ctx, tx, newVersion, "", nodes, policy)
			if err != nil {
				return err
			}
			result.Consistency = consistency
			return s.store.setMetaTx(job.ctx, tx, "current_version", newVersion)
		}, result, newVersion)
		if apiErr != nil {
			return nil, apiErr
		}
		return result, nil
	})
}

func currentVersionAPI(ctx context.Context, store *Store) (int64, *APIError) {
	current, err := store.metaInt(ctx, "current_version")
	if err != nil {
		return 0, internalError(err)
	}
	return current, nil
}

func snapshotChangedKeys(oldKeys, newKeys []ConfigKey) []string {
	oldMap := make(map[string]ConfigKey, len(oldKeys))
	for _, key := range oldKeys {
		oldMap[key.Path] = key
	}
	newMap := make(map[string]ConfigKey, len(newKeys))
	for _, key := range newKeys {
		newMap[key.Path] = key
	}
	changed := make([]string, 0)
	for path, oldKey := range oldMap {
		newKey, ok := newMap[path]
		if !ok {
			changed = append(changed, path)
			continue
		}
		oldCanonical, _ := canonicalJSON(oldKey)
		newCanonical, _ := canonicalJSON(newKey)
		if string(oldCanonical) != string(newCanonical) {
			changed = append(changed, path)
		}
	}
	for path := range newMap {
		if _, ok := oldMap[path]; !ok {
			changed = append(changed, path)
		}
	}
	sort.Strings(changed)
	return changed
}

func replaceExpectedViewsTx(ctx context.Context, tx *sql.Tx, views []ExpectedView) error {
	for _, view := range views {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO expected_views(commit_version, node_id, region, expected_version, expected_digest, key_count, digest_json)
			VALUES(?, ?, ?, ?, ?, ?, ?)`,
			view.CommitVersion, view.NodeID, view.Region, view.ExpectedVersion, view.ExpectedDigest, view.KeyCount, view.DigestJSON); err != nil {
			return err
		}
	}
	return nil
}

func upsertExpectedConvergenceTx(ctx context.Context, tx *sql.Tx, views []ExpectedView, version int64, now int64) error {
	for _, view := range views {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO node_convergence(node_id, region, expected_version, expected_digest, updated_version, updated_at_ms)
			VALUES(?, ?, ?, ?, ?, ?)
			ON CONFLICT(node_id) DO UPDATE SET
				region=excluded.region,
				expected_version=excluded.expected_version,
				expected_digest=excluded.expected_digest,
				updated_version=excluded.updated_version,
				updated_at_ms=excluded.updated_at_ms`,
			view.NodeID, view.Region, view.ExpectedVersion, view.ExpectedDigest, version, now); err != nil {
			return err
		}
	}
	return nil
}

func saveAffectedReportTx(ctx context.Context, tx *sql.Tx, version int64, report *AffectedReport, now int64) error {
	if report == nil {
		return nil
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO affected_reports(version, report_json, created_at_ms)
		VALUES(?, ?, ?)
		ON CONFLICT(version) DO UPDATE SET report_json=excluded.report_json, created_at_ms=excluded.created_at_ms`,
		version, string(raw), now)
	return err
}
