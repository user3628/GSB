package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"mitmpipeline/internal/domain"
)

type SaveBatchInput struct {
	BatchID     string
	Format      string
	Negotiation *domain.Negotiation
	Records     []domain.Record
	Conclusions []*domain.Conclusion
}

func (s *Store) SaveBatch(ctx context.Context, input SaveBatchInput) (int64, string, error) {
	payload, _ := json.Marshal(map[string]any{
		"batch_id": input.BatchID, "format": input.Format,
		"connection_count": len(input.Conclusions),
	})
	return s.Submit(ctx, "import_replay_conclusions", input.BatchID, string(payload), func(tx *sql.Tx, version int64) error {
		negotiationJSON := marshalOrEmpty(input.Negotiation)
		reportJSON, _ := json.Marshal(summary(input.Conclusions))
		if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO batches(batch_id,commit_version,format,negotiation_json,report_json,created_at_ms) VALUES(?,?,?,?,?,?)`,
			input.BatchID, version, input.Format, negotiationJSON, string(reportJSON), domain.NowMS()); err != nil {
			return domain.WrapError(domain.CodeInternal, err, "insert batch")
		}
		for recordIndex := range input.Records {
			record := input.Records[recordIndex]
			recordJSON, err := json.Marshal(record)
			if err != nil {
				return domain.WrapError(domain.CodeInternal, err, "marshal record")
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO records(canonical_id,format,format_version,record_json,latest_commit,updated_at_ms)
				VALUES(?,?,?,?,?,?)
				ON CONFLICT(canonical_id) DO UPDATE SET format=excluded.format,format_version=excluded.format_version,
				record_json=excluded.record_json,latest_commit=excluded.latest_commit,updated_at_ms=excluded.updated_at_ms`,
				record.ConnectionID, record.Format, record.FormatVersion, string(recordJSON), version, domain.NowMS()); err != nil {
				return domain.WrapError(domain.CodeInternal, err, "upsert record")
			}
		}
		for _, conclusion := range input.Conclusions {
			conclusion.CommitVersion = version
			if err := saveConclusionTx(ctx, tx, conclusion, version); err != nil {
				return err
			}
		}
		return nil
	})
}

func SavePolicySwitch(ctx context.Context, s *Store, policy domain.Policy, connectionID string, effectiveAtMS int64, retireVersion string) (domain.StoredPolicyVersion, int64, error) {
	payload, _ := json.Marshal(map[string]any{"policy": policy, "connection_id": connectionID, "effective_at_ms": effectiveAtMS})
	version, _, err := s.Submit(ctx, "policy_switch", policy.Version, string(payload), func(tx *sql.Tx, commit int64) error {
		now := domain.NowMS()
		if retireVersion != "" {
			if _, err := tx.ExecContext(ctx, `UPDATE policy_versions SET status='retired', retired_at_ms=?, replaced_by=? WHERE version=? AND status='active'`,
				now, policy.Version, retireVersion); err != nil {
				return domain.WrapError(domain.CodeInternal, err, "retire policy version")
			}
		}
		if err := upsertPolicyTx(ctx, tx, domain.StoredPolicyVersion{
			Policy: policy, Status: "active", AvailableFromMS: effectiveAtMS, CreatedCommit: commit,
		}, commit); err != nil {
			return err
		}
		if connectionID != "" {
			documentJSON, _ := json.Marshal(policy)
			if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO connection_policy_switches(connection_id,version,mode,document_json,effective_at_ms,commit_version,created_at_ms) VALUES(?,?,?,?,?,?,?)`,
				connectionID, policy.Version, policy.Mode, string(documentJSON), effectiveAtMS, commit, now); err != nil {
				return domain.WrapError(domain.CodeInternal, err, "insert connection policy switch")
			}
		}
		return nil
	})
	stored := domain.StoredPolicyVersion{Policy: policy, Status: "active", AvailableFromMS: effectiveAtMS, CreatedCommit: version}
	return stored, version, err
}

func saveConclusionTx(ctx context.Context, tx *sql.Tx, conclusion *domain.Conclusion, version int64) error {
	now := domain.NowMS()
	reportJSON, err := json.Marshal(conclusion)
	if err != nil {
		return domain.WrapError(domain.CodeInternal, err, "marshal conclusion")
	}
	policySequenceJSON, _ := json.Marshal(conclusion.PolicyVersionSequence)
	policyVersionsJSON, _ := json.Marshal(conclusion.PolicyVersions)

	if _, err := tx.ExecContext(ctx, `INSERT INTO connections(canonical_id,raw_id,sni,protocol_version,leaf_serial,state,conclusion,reason_code,latest_commit,updated_at_ms,report_json)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(canonical_id) DO UPDATE SET raw_id=excluded.raw_id,sni=excluded.sni,protocol_version=excluded.protocol_version,
		leaf_serial=COALESCE(NULLIF(excluded.leaf_serial,''),connections.leaf_serial),state=excluded.state,conclusion=excluded.conclusion,
		reason_code=excluded.reason_code,latest_commit=excluded.latest_commit,updated_at_ms=excluded.updated_at_ms,report_json=excluded.report_json`,
		conclusion.CanonicalConnectionID, conclusion.ConnectionID, conclusion.SNI, conclusion.ProtocolVersion,
		firstSerial(conclusion), conclusion.State, conclusion.Conclusion, conclusion.ReasonCode, version, now, string(reportJSON)); err != nil {
		return domain.WrapError(domain.CodeInternal, err, "upsert connection")
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO conclusion_versions(connection_id,commit_version,conclusion,state,reason_code,policy_sequence_json,policy_versions_json,report_json,decided_at_ms)
		VALUES(?,?,?,?,?,?,?,?,?)`,
		conclusion.CanonicalConnectionID, version, conclusion.Conclusion, conclusion.State, conclusion.ReasonCode,
		string(policySequenceJSON), string(policyVersionsJSON), string(reportJSON), now); err != nil {
		return domain.WrapError(domain.CodeInternal, err, "insert conclusion version")
	}

	judgmentBySeq := map[int]domain.EventJudgment{}
	for _, judgment := range conclusion.EventJudgments {
		judgmentBySeq[judgment.EventSeq] = judgment
	}
	for _, rounding := range conclusion.Rounding {
		roundingJSON, _ := json.Marshal(rounding)
		judgment := judgmentBySeq[rounding.EventSeq]
		judgmentJSON, _ := json.Marshal(judgment)
		_, err := tx.ExecContext(ctx, `INSERT INTO events(connection_id,commit_version,seq,type,raw_unix_ns,timestamp_ms,policy_version,rounding_json,judgment_json)
			VALUES(?,?,?,?,?,?,?,?,?)`, conclusion.CanonicalConnectionID, version, rounding.EventSeq, rounding.EventType,
			rounding.RawUnixNS, rounding.TimestampMS, judgment.PolicyVersion, string(roundingJSON), string(judgmentJSON))
		if err != nil {
			return domain.WrapError(domain.CodeInternal, err, "insert event")
		}
	}

	for _, evidence := range conclusion.EvidenceChain {
		detailsJSON := ""
		if evidence.Details != nil {
			encoded, _ := json.Marshal(evidence.Details)
			detailsJSON = string(encoded)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO evidence(connection_id,commit_version,seq,event_seq,policy_version,kind,severity,code,message,details_json,observed_at_ms)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`, conclusion.CanonicalConnectionID, version, evidence.Seq, evidence.EventSeq,
			evidence.PolicyVersion, evidence.Kind, evidence.Severity, evidence.Code, evidence.Message, detailsJSON, evidence.ObservedAtMS); err != nil {
			return domain.WrapError(domain.CodeInternal, err, "insert evidence")
		}
	}

	for index, policyVersion := range conclusion.PolicyVersions {
		policy := conclusion.PolicyDocuments[policyVersion]
		status := "active"
		var retiredAt any
		var replacedBy any
		replacedByVersion := ""
		if index < len(conclusion.PolicyVersions)-1 {
			status = "retired"
			replacedByVersion = conclusion.PolicyVersions[index+1]
			replacedBy = replacedByVersion
			retiredAt = domain.NowMS()
		}
		stored := domain.StoredPolicyVersion{
			Policy: policy, Status: status, AvailableFromMS: firstEventMS(conclusion),
			RetiredAtMS: 0, ReplacedBy: replacedByVersion, CreatedCommit: version,
		}
		if status == "retired" {
			stored.RetiredAtMS = domain.NowMS()
		}
		if err := upsertPolicyTx(ctx, tx, stored, version); err != nil {
			return err
		}
		if status == "retired" {
			if _, err := tx.ExecContext(ctx, `UPDATE policy_versions SET status='retired',retired_at_ms=?,replaced_by=? WHERE version=?`,
				retiredAt, replacedBy, policyVersion); err != nil {
				return domain.WrapError(domain.CodeInternal, err, "mark policy retired")
			}
		}
	}
	return nil
}

func upsertPolicyTx(ctx context.Context, tx *sql.Tx, stored domain.StoredPolicyVersion, commit int64) error {
	var maxRevision int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(revision),0) FROM policy_versions`).Scan(&maxRevision); err != nil {
		return domain.WrapError(domain.CodeInternal, err, "read policy revision")
	}
	documentJSON, _ := json.Marshal(stored.Policy)
	_, err := tx.ExecContext(ctx, `INSERT INTO policy_versions(version,revision,mode,document_json,status,available_from_ms,retired_at_ms,replaced_by,created_commit)
		VALUES(?,?,?,?,?,?,?,?,?)
		ON CONFLICT(version) DO UPDATE SET mode=excluded.mode,document_json=excluded.document_json`,
		stored.Version, maxRevision+1, stored.Mode, string(documentJSON), stored.Status,
		stored.AvailableFromMS, nullableInt64(stored.RetiredAtMS), nullableString(stored.ReplacedBy), commit)
	if err != nil {
		return domain.WrapError(domain.CodeInternal, err, "upsert policy version")
	}
	return nil
}

func marshalOrEmpty(value any) string {
	if value == nil {
		return ""
	}
	data, _ := json.Marshal(value)
	return string(data)
}

func summary(conclusions []*domain.Conclusion) []map[string]any {
	out := make([]map[string]any, 0, len(conclusions))
	for _, c := range conclusions {
		out = append(out, map[string]any{
			"connection_id": c.CanonicalConnectionID, "conclusion": c.Conclusion,
			"state": c.State, "reason_code": c.ReasonCode,
			"policy_versions": c.PolicyVersions,
		})
	}
	return out
}

func firstSerial(c *domain.Conclusion) string {
	for _, judgment := range c.EventJudgments {
		if judgment.Chain != nil && len(judgment.Chain.Certificates) > 0 {
			return judgment.Chain.Certificates[0].Serial
		}
	}
	return ""
}

func firstEventMS(c *domain.Conclusion) int64 {
	if len(c.Rounding) > 0 {
		return c.Rounding[0].TimestampMS
	}
	return domain.NowMS()
}

func nullableInt64(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
