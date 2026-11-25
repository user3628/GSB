package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"csrouter/internal/domain"
)

func stageIndex(stage string) int {
	for i, candidate := range domain.Stages {
		if candidate == stage {
			return i
		}
	}
	return -1
}

func (s *Store) UpsertShard(manifest domain.ShardManifest, policyVersion int64) error {
	_, err := s.db.Exec(`INSERT INTO shards(path,tier,sha256,row_count,column_count,declared_rows,declared_columns,config_version)
		VALUES(?,?,?,?,?,?,?,?)
		ON CONFLICT(path) DO UPDATE SET sha256=excluded.sha256,row_count=excluded.row_count,
			column_count=excluded.column_count,declared_rows=excluded.declared_rows,
			declared_columns=excluded.declared_columns,config_version=excluded.config_version`,
		manifest.Path, manifest.Tier, manifest.SHA256, manifest.Rows, manifest.Columns,
		manifest.DeclaredRows, manifest.DeclaredCols, policyVersion)
	return err
}

func (s *Store) UpsertTickets(tier, shardPath string, tickets []domain.Ticket) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	for ordinal, ticket := range tickets {
		raw, err := json.Marshal(ticket)
		if err != nil {
			return err
		}
		var priority any
		if ticket.Priority != nil {
			priority = *ticket.Priority
		}
		var confidence any
		if ticket.ConfidencePct != nil {
			confidence = *ticket.ConfidencePct
		}
		_, err = tx.Exec(`INSERT INTO tickets(
			ticket_id,tier,shard_path,ordinal,raw_json,input_fingerprint,intent_label,
			priority_label,confidence)
			VALUES(?,?,?,?,?,?,?,?,?)
			ON CONFLICT(ticket_id) DO UPDATE SET
				tier=excluded.tier,shard_path=excluded.shard_path,ordinal=excluded.ordinal,
				raw_json=excluded.raw_json,input_fingerprint=excluded.input_fingerprint,
				intent_label=excluded.intent_label,priority_label=excluded.priority_label,
				confidence=excluded.confidence`,
			ticket.TicketID, tier, shardPath, ordinal, string(raw), ticket.ContentFingerprint(),
			ticket.Intent, priority, confidence)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) DeleteTicketsNotIn(shardPath string, keepIDs []string) (int64, error) {
	if len(keepIDs) == 0 {
		res, err := s.db.Exec(`DELETE FROM tickets WHERE shard_path=?`, shardPath)
		if err != nil {
			return 0, err
		}
		n, _ := res.RowsAffected()
		return n, nil
	}
	query := `DELETE FROM tickets WHERE shard_path=? AND ticket_id NOT IN (`
	args := []any{shardPath}
	for i, id := range keepIDs {
		if i > 0 {
			query += ","
		}
		query += "?"
		args = append(args, id)
	}
	query += ")"
	res, err := s.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func scanTicket(scanner interface{ Scan(...any) error }) (TicketRow, error) {
	var row TicketRow
	var raw string
	var priority sql.NullInt64
	var confidence sql.NullFloat64
	if err := scanner.Scan(&row.Ticket.TicketID, &row.Tier, &row.ShardPath, &row.Ordinal,
		&raw, &row.InputFingerprint, &row.Ticket.Intent, &priority, &confidence); err != nil {
		return row, err
	}
	if err := json.Unmarshal([]byte(raw), &row.Ticket); err != nil {
		return row, err
	}
	if priority.Valid {
		v := int(priority.Int64)
		row.Ticket.Priority = &v
	}
	if confidence.Valid {
		v := confidence.Float64
		row.Ticket.ConfidencePct = &v
	}
	return row, nil
}

const ticketSelect = `SELECT ticket_id,tier,shard_path,ordinal,raw_json,input_fingerprint,intent_label,priority_label,confidence FROM tickets`

func (s *Store) ListTickets(tier string) ([]TicketRow, error) {
	rows, err := s.db.Query(ticketSelect+` WHERE tier=? ORDER BY ticket_id ASC`, tier)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TicketRow
	for rows.Next() {
		row, err := scanTicket(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) ListTicketIDsByShard(shardPath string) ([]string, error) {
	rows, err := s.db.Query(`SELECT ticket_id FROM tickets WHERE shard_path=? ORDER BY ticket_id`, shardPath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) GetTicket(ticketID string) (*TicketRow, error) {
	row, err := scanTicket(s.db.QueryRow(ticketSelect+` WHERE ticket_id=?`, ticketID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (s *Store) PublishArtifact(in PublishInput) (*PublishResult, *domain.BizError) {
	if stageIndex(in.Stage) < 0 {
		return nil, domain.NewBizError(domain.ErrIllegalStage, in.TicketID, "", in.ConfigVersion, "unknown stage %q", in.Stage)
	}

	var result *PublishResult
	var bizErr *domain.BizError
	err := s.WithFileLock(func() error {
		tx, err := s.db.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		if idx := stageIndex(in.Stage); idx > 0 {
			prev := domain.Stages[idx-1]
			var count int
			if err := tx.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE ticket_id=? AND stage=?`, in.TicketID, prev).Scan(&count); err != nil {
				return err
			}
			if count == 0 {
				bizErr = domain.NewBizError(domain.ErrIllegalStage, in.TicketID, "", in.ConfigVersion,
					"missing prerequisite stage %q for %q", prev, in.Stage)
				return nil
			}
		}

		if _, err := tx.Exec(`INSERT INTO stage_cursors(stage,last_seq) VALUES(?,0) ON CONFLICT(stage) DO NOTHING`, in.Stage); err != nil {
			return err
		}
		var lastSeq int64
		if err := tx.QueryRow(`SELECT last_seq FROM stage_cursors WHERE stage=?`, in.Stage).Scan(&lastSeq); err != nil {
			return err
		}
		if in.ExpectedHead != nil && *in.ExpectedHead != lastSeq {
			bizErr = domain.NewBizError(domain.ErrStaleCheckpoint, in.TicketID, "", in.ConfigVersion,
				"expected stage head %d but actual %d", *in.ExpectedHead, lastSeq)
			return nil
		}
		conditionalSeq := lastSeq
		if in.ClientStageSeq != nil {
			if *in.ClientStageSeq <= lastSeq {
				bizErr = domain.NewBizError(domain.ErrStaleCheckpoint, in.TicketID, "", in.ConfigVersion,
					"client stage_seq=%d is behind or duplicate; head=%d", *in.ClientStageSeq, lastSeq)
				return nil
			}
			if *in.ClientStageSeq != lastSeq+1 {
				bizErr = domain.NewBizError(domain.ErrStaleCheckpoint, in.TicketID, "", in.ConfigVersion,
					"non-contiguous stage_seq=%d; head=%d", *in.ClientStageSeq, lastSeq)
				return nil
			}
			conditionalSeq = *in.ClientStageSeq - 1
		}

		res, err := tx.Exec(`UPDATE stage_cursors SET last_seq=last_seq+1 WHERE stage=? AND last_seq=?`, in.Stage, conditionalSeq)
		if err != nil {
			return err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			bizErr = domain.NewBizError(domain.ErrStaleCheckpoint, in.TicketID, "", in.ConfigVersion,
				"conditional stage cursor update failed")
			return nil
		}
		stageSeq := conditionalSeq + 1

		var existingDigest string
		var revision int64
		err = tx.QueryRow(`SELECT digest,revision FROM artifacts WHERE ticket_id=? AND stage=?`, in.TicketID, in.Stage).Scan(&existingDigest, &revision)
		if err == nil {
			bizErr = domain.NewBizError(domain.ErrStaleCheckpoint, in.TicketID, "", in.ConfigVersion,
				"duplicate publish for existing current artifact revision=%d", revision)
			return nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		revision = 1

		payload, err := json.Marshal(in.Artifact)
		if err != nil {
			return err
		}
		digest := artifactDigest(in.Artifact)
		if digest == "" {
			digest = domain.FingerprintHex("publish", string(payload))
		}
		status := in.Status
		if status == "" {
			status = "completed"
		}
		_, err = tx.Exec(`INSERT INTO artifacts(
			ticket_id,stage,stage_seq,revision,input_fingerprint,config_version,config_fingerprint,
			digest,payload_json,status,error_code)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
			in.TicketID, in.Stage, stageSeq, revision, in.InputFingerprint, in.ConfigVersion,
			in.ConfigFingerprint, digest, string(payload), status, in.ErrorCode)
		if err != nil {
			return err
		}
		ledgerRes, err := tx.Exec(`INSERT INTO artifact_ledger(
			stage,stage_seq,ticket_id,revision,digest,config_version,status,error_code,operation)
			VALUES(?,?,?,?,?,?,?,?,?)`,
			in.Stage, stageSeq, in.TicketID, revision, digest, in.ConfigVersion, status, in.ErrorCode, "publish")
		if err != nil {
			return err
		}
		globalSeq, _ := ledgerRes.LastInsertId()
		if err := tx.Commit(); err != nil {
			return err
		}
		result = &PublishResult{StageSeq: stageSeq, Revision: revision, GlobalSeq: globalSeq}
		return nil
	})
	if err != nil {
		return nil, domain.NewBizError("E_STORE", in.TicketID, "", in.ConfigVersion, "%v", err)
	}
	if bizErr != nil {
		return nil, bizErr
	}
	return result, nil
}

func artifactDigest(v any) string {
	switch x := v.(type) {
	case domain.NormalizeArtifact:
		return x.Digest
	case *domain.NormalizeArtifact:
		return x.Digest
	case domain.IntentArtifact:
		return x.Digest
	case *domain.IntentArtifact:
		return x.Digest
	case domain.PriorityArtifact:
		return x.Digest
	case *domain.PriorityArtifact:
		return x.Digest
	case domain.VerificationDetail:
		return domain.DigestArtifact(domain.StageVerify, x)
	case *domain.VerificationDetail:
		return domain.DigestArtifact(domain.StageVerify, x)
	default:
		return ""
	}
}

func (s *Store) GetArtifact(ticketID, stage string) (*ArtifactRecord, error) {
	var rec ArtifactRecord
	err := s.db.QueryRow(`SELECT ticket_id,stage,stage_seq,revision,input_fingerprint,config_version,
		config_fingerprint,digest,payload_json,status,error_code
		FROM artifacts WHERE ticket_id=? AND stage=?`, ticketID, stage).Scan(
		&rec.TicketID, &rec.Stage, &rec.StageSeq, &rec.Revision, &rec.InputFingerprint,
		&rec.ConfigVersion, &rec.ConfigFingerprint, &rec.Digest, &rec.Payload, &rec.Status, &rec.ErrorCode)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *Store) StageHead(stage string) (int64, error) {
	var seq int64
	err := s.db.QueryRow(`SELECT COALESCE(MAX(last_seq),0) FROM stage_cursors WHERE stage=?`, stage).Scan(&seq)
	return seq, err
}

func (s *Store) InvalidateFromStage(tier string, ticketIDs []string, fromStage string) (int64, []string, error) {
	from := stageIndex(fromStage)
	if from < 0 {
		return 0, nil, domain.NewBizError(domain.ErrIllegalStage, "", "", 0, "unknown stage %q", fromStage)
	}
	stages := domain.Stages[from:]
	var total int64
	affected := []string{}

	err := s.WithFileLock(func() error {
		tx, err := s.db.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		query := `SELECT ticket_id, stage FROM artifacts WHERE stage IN (` + placeholders(len(stages))
		args := make([]any, 0, len(stages)+1)
		for _, stage := range stages {
			args = append(args, stage)
		}
		query += ")"
		if tier != "" {
			query += ` AND ticket_id IN (SELECT ticket_id FROM tickets WHERE tier=?)`
			args = append(args, tier)
		}
		if len(ticketIDs) > 0 {
			query += ` AND ticket_id IN (` + placeholders(len(ticketIDs)) + `)`
			for _, id := range ticketIDs {
				args = append(args, id)
			}
		}
		rows, err := tx.Query(query, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var ticketID, stage string
			if err := rows.Scan(&ticketID, &stage); err != nil {
				rows.Close()
				return err
			}
			affected = append(affected, ticketID+":"+stage)
		}
		if err := rows.Close(); err != nil {
			return err
		}

		deleteQuery := `DELETE FROM artifacts WHERE stage IN (` + placeholders(len(stages)) + `)`
		deleteArgs := make([]any, 0, len(stages)+1)
		for _, stage := range stages {
			deleteArgs = append(deleteArgs, stage)
		}
		if tier != "" {
			deleteQuery += ` AND ticket_id IN (SELECT ticket_id FROM tickets WHERE tier=?)`
			deleteArgs = append(deleteArgs, tier)
		}
		if len(ticketIDs) > 0 {
			deleteQuery += ` AND ticket_id IN (` + placeholders(len(ticketIDs)) + `)`
			for _, id := range ticketIDs {
				deleteArgs = append(deleteArgs, id)
			}
		}
		res, err := tx.Exec(deleteQuery, deleteArgs...)
		if err != nil {
			return err
		}
		total, _ = res.RowsAffected()
		return tx.Commit()
	})
	return total, affected, err
}

func placeholders(n int) string {
	out := ""
	for i := 0; i < n; i++ {
		if i > 0 {
			out += ","
		}
		out += "?"
	}
	return out
}

type StageStatus struct {
	Stage     string `json:"stage"`
	Completed int    `json:"completed"`
	HeadSeq   int64  `json:"head_seq"`
}

func (s *Store) StageStatuses(tier string) ([]StageStatus, error) {
	out := make([]StageStatus, 0, len(domain.Stages))
	for _, stage := range domain.Stages {
		var count int
		err := s.db.QueryRow(`SELECT COUNT(1) FROM artifacts a JOIN tickets t ON t.ticket_id=a.ticket_id
			WHERE a.stage=? AND t.tier=?`, stage, tier).Scan(&count)
		if err != nil {
			return nil, err
		}
		head, err := s.StageHead(stage)
		if err != nil {
			return nil, err
		}
		out = append(out, StageStatus{Stage: stage, Completed: count, HeadSeq: head})
	}
	return out, nil
}

func (s *Store) SaveReport(report domain.Report) error {
	payload, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO reports(tier,policy_version,fingerprint,payload_json)
		VALUES(?,?,?,?) ON CONFLICT(tier) DO UPDATE SET policy_version=excluded.policy_version,
		fingerprint=excluded.fingerprint,payload_json=excluded.payload_json`,
		report.Tier, report.PolicyVersion, report.ReportFingerprint, string(payload))
	return err
}

func (s *Store) GetReport(tier string) (*domain.Report, error) {
	var payload string
	err := s.db.QueryRow(`SELECT payload_json FROM reports WHERE tier=?`, tier).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var report domain.Report
	if err := json.Unmarshal([]byte(payload), &report); err != nil {
		return nil, err
	}
	return &report, nil
}

func (s *Store) FingerprintSnapshot(tier string) (map[string]string, error) {
	result := map[string]string{}
	for _, stage := range domain.Stages {
		rows, err := s.db.Query(`SELECT a.ticket_id, a.digest FROM artifacts a
			JOIN tickets t ON t.ticket_id=a.ticket_id
			WHERE t.tier=? AND a.stage=? ORDER BY a.ticket_id ASC`, tier, stage)
		if err != nil {
			return nil, err
		}
		h := domain.FingerprintHex("stage-fingerprint/v1", tier, stage)
		count := 0
		for rows.Next() {
			var id, digest string
			if err := rows.Scan(&id, &digest); err != nil {
				rows.Close()
				return nil, err
			}
			h = domain.FingerprintHex(h, id, digest)
			count++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		_ = rows.Close()
		result[stage] = h
		result[stage+":count"] = fmt.Sprintf("%d", count)
	}
	policy, err := s.LatestPolicy(tier)
	if err != nil {
		return nil, err
	}
	if policy != nil {
		result["policy"] = policy.Fingerprint()
		result["policy_version"] = fmt.Sprintf("%d", policy.EffectiveVersion())
	}
	return result, nil
}
