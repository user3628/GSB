package mfa

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

func writeSnapshot(ctx context.Context, tx *sql.Tx, reason, attemptID string) (int64, error) {
	var headVersion int64
	var headHash string
	if err := tx.QueryRowContext(ctx, `SELECT head_version, head_hash FROM snapshot_meta WHERE singleton = 1`).Scan(&headVersion, &headHash); err != nil {
		return 0, err
	}
	tenants, users, policies, factors, err := readState(ctx, tx)
	if err != nil {
		return 0, err
	}
	version := headVersion + 1
	payload := SnapshotPayload{
		Version:       version,
		ParentVersion: headVersion,
		Reason:        reason,
		AttemptID:     attemptID,
		Tenants:       tenants,
		Users:         users,
		Policies:      policies,
		Factors:       factors,
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	contentHash := sha256Hex(payloadJSON)
	chainHash := snapshotChainHash(headHash, version, contentHash)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO snapshots(version, parent_version, parent_hash, payload, content_hash, chain_hash, reason, attempt_id)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)
`, version, headVersion, headHash, string(payloadJSON), contentHash, chainHash, reason, attemptID); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO snapshot_seals(version, content_hash, chain_hash, reason, attempt_id)
VALUES(?, ?, ?, ?, ?)
`, version, contentHash, chainHash, reason, attemptID); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE snapshot_meta SET head_version = ?, head_hash = ? WHERE singleton = 1
`, version, chainHash); err != nil {
		return 0, err
	}
	return version, nil
}

func (e *Engine) SnapshotStatus(ctx context.Context) SnapshotStatus {
	status, _ := e.verifySnapshots(ctx)
	return status
}

func (e *Engine) VerifySnapshots(ctx context.Context) (SnapshotStatus, error) {
	status, err := e.verifySnapshots(ctx)
	if err != nil {
		return status, err
	}
	if status.Code != CodeOK {
		return status, bizError(status.Code, status.Reason)
	}
	return status, nil
}

func (e *Engine) verifySnapshots(ctx context.Context) (SnapshotStatus, error) {
	rows, err := e.db.QueryContext(ctx, `
SELECT s.version, s.parent_version, s.parent_hash, s.payload, s.content_hash, s.chain_hash,
       s.reason, s.attempt_id, seal.content_hash, seal.chain_hash
FROM snapshots s
LEFT JOIN snapshot_seals seal ON seal.version = s.version
ORDER BY s.version
`)
	if err != nil {
		return SnapshotStatus{}, fmt.Errorf("query snapshots: %w", err)
	}
	defer rows.Close()

	status := SnapshotStatus{Code: CodeOK}
	expectedVersion := int64(1)
	previousChain := ""
	invalidSeen := false
	for rows.Next() {
		var info SnapshotInfo
		var payload, parentHash, contentHash, chainHash, attemptID string
		var sealContent, sealChain sql.NullString
		if err := rows.Scan(
			&info.Version, &info.ParentVersion, &parentHash, &payload, &contentHash, &chainHash,
			&info.Reason, &attemptID, &sealContent, &sealChain,
		); err != nil {
			return SnapshotStatus{}, err
		}
		info.AttemptID = attemptID
		info.ContentHash = contentHash
		info.ChainHash = chainHash
		info.Valid = true
		if status.HeadVersion < info.Version {
			status.HeadVersion = info.Version
		}
		if invalidSeen {
			info.Valid = false
			info.Issue = "after_invalid_chain"
			status.Entries = append(status.Entries, info)
			continue
		}
		issue := ""
		switch {
		case info.Version != expectedVersion:
			issue = fmt.Sprintf("version_gap: expected %d, observed %d", expectedVersion, info.Version)
		case info.ParentVersion != info.Version-1:
			issue = "parent_version_mismatch"
		case parentHash != previousChain:
			issue = "parent_hash_mismatch"
		case !sealContent.Valid || !sealChain.Valid:
			issue = "snapshot_seal_missing"
		case sealContent.String != contentHash:
			issue = "content_seal_mismatch"
		case sealChain.String != chainHash:
			issue = "chain_seal_mismatch"
		case sha256Hex([]byte(payload)) != contentHash:
			issue = "content_hash_mismatch"
		case snapshotChainHash(parentHash, info.Version, contentHash) != chainHash:
			issue = "chain_hash_mismatch"
		}
		if issue != "" {
			info.Valid = false
			info.Issue = issue
			invalidSeen = true
			if status.Reason == "" {
				status.Reason = issue
			}
			status.Entries = append(status.Entries, info)
			continue
		}
		status.LatestValid = info.Version
		status.Entries = append(status.Entries, info)
		previousChain = chainHash
		expectedVersion++
	}
	if err := rows.Err(); err != nil {
		return SnapshotStatus{}, err
	}

	var metaHead int64
	var metaHash string
	if err := e.db.QueryRowContext(ctx, `SELECT head_version, head_hash FROM snapshot_meta WHERE singleton = 1`).Scan(&metaHead, &metaHash); err != nil {
		return SnapshotStatus{}, fmt.Errorf("read snapshot head: %w", err)
	}
	if metaHead > status.HeadVersion {
		if status.Reason == "" {
			status.Reason = fmt.Sprintf("snapshot_head_missing: committed head %d not present", metaHead)
		}
		invalidSeen = true
	} else if !invalidSeen && metaHead != status.LatestValid {
		if status.Reason == "" {
			status.Reason = "snapshot_head_mismatch"
		}
		invalidSeen = true
	} else if !invalidSeen && status.LatestValid > 0 {
		var latestChain string
		if err := e.db.QueryRowContext(ctx, `SELECT chain_hash FROM snapshots WHERE version = ?`, status.LatestValid).Scan(&latestChain); err != nil {
			return SnapshotStatus{}, err
		}
		if latestChain != metaHash {
			if status.Reason == "" {
				status.Reason = "snapshot_head_hash_mismatch"
			}
			invalidSeen = true
		}
	}
	if invalidSeen || status.LatestValid != status.HeadVersion {
		status.Code = CodeSnapshotVersion
		status.FallbackUsed = true
		status.FallbackVersion = status.LatestValid
		if status.Reason == "" {
			status.Reason = "snapshot_chain_invalid"
		}
	}
	status.HeadVersion = metaHead
	return status, nil
}

func (e *Engine) ReadSnapshot(ctx context.Context, version int64) (*SnapshotPayload, error) {
	if version < 1 {
		return nil, bizError(CodeSnapshotVersion, "snapshot version must be positive")
	}
	var payloadJSON, contentHash, chainHash, parentHash string
	var parentVersion int64
	var sealContent, sealChain string
	err := e.db.QueryRowContext(ctx, `
SELECT s.payload, s.content_hash, s.chain_hash, s.parent_version, s.parent_hash, seal.content_hash, seal.chain_hash
FROM snapshots s JOIN snapshot_seals seal ON seal.version = s.version
WHERE s.version = ?
`, version).Scan(&payloadJSON, &contentHash, &chainHash, &parentVersion, &parentHash, &sealContent, &sealChain)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, bizError(CodeSnapshotVersion, "snapshot version not found")
		}
		return nil, err
	}
	if sealContent != contentHash || sealChain != chainHash || sha256Hex([]byte(payloadJSON)) != contentHash || snapshotChainHash(parentHash, version, contentHash) != chainHash {
		return nil, bizError(CodeSnapshotVersion, "snapshot content or chain seal mismatch")
	}
	if parentVersion != version-1 {
		return nil, bizError(CodeSnapshotVersion, "snapshot parent version mismatch")
	}
	expectedParentHash := ""
	if version > 1 {
		if err := e.db.QueryRowContext(ctx, `SELECT chain_hash FROM snapshots WHERE version = ?`, version-1).Scan(&expectedParentHash); err != nil {
			if err == sql.ErrNoRows {
				return nil, bizError(CodeSnapshotVersion, "snapshot parent is missing")
			}
			return nil, err
		}
	}
	if parentHash != expectedParentHash {
		return nil, bizError(CodeSnapshotVersion, "snapshot parent chain mismatch")
	}
	var payload SnapshotPayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return nil, bizError(CodeSnapshotVersion, "snapshot payload is not decodable")
	}
	if payload.Version != version {
		return nil, bizError(CodeSnapshotVersion, "snapshot payload version mismatch")
	}
	return &payload, nil
}

func (e *Engine) Recover(ctx context.Context) (RecoveryResult, error) {
	status, err := e.verifySnapshots(ctx)
	if err != nil {
		return RecoveryResult{}, err
	}
	result := RecoveryResult{
		Code:            status.Code,
		SnapshotStatus:  status,
		CommittedHead:   status.LatestValid,
		RestoredVersion: status.LatestValid,
	}
	if status.Code != CodeOK {
		result.Message = status.Reason
		return result, bizError(CodeSnapshotVersion, status.Reason)
	}
	return result, nil
}

func (e *Engine) ApplyFault(ctx context.Context, injection FaultInjection) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	switch injection.Kind {
	case FaultSnapshotVersionGap:
		tx, err := e.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var version int64
		var payload, contentHash, chainHash, reason, attemptID string
		if err := tx.QueryRowContext(ctx, `
SELECT version, payload, content_hash, chain_hash, reason, attempt_id
FROM snapshots ORDER BY version DESC LIMIT 1
`).Scan(&version, &payload, &contentHash, &chainHash, &reason, &attemptID); err != nil {
			return err
		}
		gapVersion := version + 2
		if _, err := tx.ExecContext(ctx, `
INSERT INTO snapshots(version, parent_version, parent_hash, payload, content_hash, chain_hash, reason, attempt_id)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)
`, gapVersion, version, chainHash, payload, contentHash, chainHash, reason, attemptID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO snapshot_seals(version, content_hash, chain_hash, reason, attempt_id)
VALUES(?, ?, ?, ?, ?)
`, gapVersion, contentHash, chainHash, reason, attemptID); err != nil {
			return err
		}
		return tx.Commit()
	case FaultSnapshotWholesaleReplacement:
		tx, err := e.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var version int64
		if err := tx.QueryRowContext(ctx, `SELECT version FROM snapshots ORDER BY version DESC LIMIT 1`).Scan(&version); err != nil {
			return err
		}
		replacement := []byte(fmt.Sprintf(`{"version":%d,"replacement":true}`, version))
		if _, err := tx.ExecContext(ctx, `UPDATE snapshots SET payload = ? WHERE version = ?`, string(replacement), version); err != nil {
			return err
		}
		return tx.Commit()
	default:
		return policyError("fault.kind", "unsupported fault injection kind")
	}
}
