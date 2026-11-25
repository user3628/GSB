package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"profilehotspot/internal/engine"
	"profilehotspot/internal/model"
)

func ReadStateTx(ctx context.Context, tx *sql.Tx) (StateMeta, error) {
	return currentStateTx(ctx, tx)
}

func SetStateTx(ctx context.Context, tx *sql.Tx, state StateMeta) error {
	return updateStateTx(ctx, tx, state.CurrentPolicyVersion, state.CommitSeq, state.NextSampleOrder)
}

func ReadPolicyTx(ctx context.Context, tx *sql.Tx, version uint64) (model.Policy, error) {
	var raw string
	if err := tx.QueryRowContext(ctx, `SELECT policy_json FROM policy_snapshots WHERE version=?`, version).Scan(&raw); err != nil {
		return model.Policy{}, err
	}
	var policy model.Policy
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return model.Policy{}, err
	}
	return policy, nil
}

func PolicyExistsTx(ctx context.Context, tx *sql.Tx, version uint64) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM policy_snapshots WHERE version=?)`, version).Scan(&exists)
	return exists == 1, err
}

func InsertPolicyTx(ctx context.Context, tx *sql.Tx, policy model.Policy, commitSeq int64, rebuildReport any) error {
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	rebuildJSON, err := json.Marshal(rebuildReport)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO policy_snapshots(version, commit_seq, generated_at, policy_json, rebuild_report_json, committed_at)
		VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)`, policy.Version, commitSeq, policy.GeneratedAt.Format(timeLayout), string(policyJSON), string(rebuildJSON))
	return err
}

const timeLayout = "2006-01-02T15:04:05.999999999Z07:00"

type PolicySnapshot struct {
	Version     uint64          `json:"version"`
	CommitSeq   int64           `json:"commit_seq"`
	GeneratedAt string          `json:"generated_at"`
	Policy      model.Policy    `json:"policy"`
	Rebuild     json.RawMessage `json:"rebuild_report"`
}

func (s *Store) PolicySnapshot(ctx context.Context, version uint64) (PolicySnapshot, error) {
	var snap PolicySnapshot
	var policyJSON, rebuildJSON string
	err := s.db.QueryRowContext(ctx, `SELECT version, commit_seq, generated_at, policy_json, rebuild_report_json FROM policy_snapshots WHERE version=?`, version).
		Scan(&snap.Version, &snap.CommitSeq, &snap.GeneratedAt, &policyJSON, &rebuildJSON)
	if err != nil {
		return PolicySnapshot{}, err
	}
	if err := json.Unmarshal([]byte(policyJSON), &snap.Policy); err != nil {
		return PolicySnapshot{}, err
	}
	snap.Rebuild = json.RawMessage(rebuildJSON)
	return snap, nil
}

type BatchCommit struct {
	BatchID       string          `json:"batch_id"`
	PayloadHash   string          `json:"payload_hash"`
	PolicyVersion uint64          `json:"policy_version"`
	CommitSeq     int64           `json:"commit_seq"`
	Response      json.RawMessage `json:"response"`
}

func FindBatchTx(ctx context.Context, tx *sql.Tx, batchID string) (BatchCommit, bool, error) {
	var batch BatchCommit
	var response string
	err := tx.QueryRowContext(ctx, `SELECT batch_id, payload_hash, policy_version, commit_seq, response_json FROM sample_batches WHERE batch_id=?`, batchID).
		Scan(&batch.BatchID, &batch.PayloadHash, &batch.PolicyVersion, &batch.CommitSeq, &response)
	if err == sql.ErrNoRows {
		return BatchCommit{}, false, nil
	}
	if err != nil {
		return BatchCommit{}, false, err
	}
	batch.Response = json.RawMessage(response)
	return batch, true, nil
}

func InsertBatchTx(ctx context.Context, tx *sql.Tx, batch BatchCommit) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO sample_batches(batch_id, payload_hash, policy_version, commit_seq, response_json, committed_at)
		VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)`, batch.BatchID, batch.PayloadHash, batch.PolicyVersion, batch.CommitSeq, string(batch.Response))
	return err
}

func InsertBatchItemTx(ctx context.Context, tx *sql.Tx, batchID, sampleID, outcome string, occurrences int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO sample_batch_items(batch_id, sample_id, outcome, occurrences) VALUES(?,?,?,?)
		ON CONFLICT(batch_id, sample_id) DO UPDATE SET outcome=excluded.outcome, occurrences=excluded.occurrences`,
		batchID, sampleID, outcome, occurrences)
	return err
}

func (s *Store) BatchByID(ctx context.Context, batchID string) (BatchCommit, bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return BatchCommit{}, false, err
	}
	defer tx.Rollback()
	batch, found, err := FindBatchTx(ctx, tx, batchID)
	if err != nil {
		return BatchCommit{}, false, err
	}
	return batch, found, nil
}

func LoadSamplesByIDsTx(ctx context.Context, tx *sql.Tx, ids []string) (map[string]engine.SampleRecord, error) {
	result := make(map[string]engine.SampleRecord, len(ids))
	for start := 0; start < len(ids); start += 500 {
		end := start + 500
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[start:end]
		placeholders := strings.TrimRight(strings.Repeat("?,", len(chunk)), ",")
		args := make([]any, 0, len(chunk))
		for _, id := range chunk {
			args = append(args, id)
		}
		rows, err := tx.QueryContext(ctx, `SELECT sample_id, content_hash, timestamp_ms, service, first_channel, frames_json, weight, seen_order, first_batch_id, first_commit_seq, duplicate_count, received_count FROM samples WHERE sample_id IN (`+placeholders+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			record, err := scanSample(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			result[record.ID] = record
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return result, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanSample(row rowScanner) (engine.SampleRecord, error) {
	var record engine.SampleRecord
	var framesJSON string
	if err := row.Scan(&record.ID, &record.ContentHash, &record.TimestampMS, &record.Service, &record.FirstChannel, &framesJSON,
		&record.Weight, &record.SeenOrder, &record.FirstBatchID, &record.FirstCommitSeq, &record.DuplicateCount, &record.ReceivedCount); err != nil {
		return engine.SampleRecord{}, err
	}
	if err := json.Unmarshal([]byte(framesJSON), &record.Frames); err != nil {
		return engine.SampleRecord{}, err
	}
	return record, nil
}

func LoadAllSamplesTx(ctx context.Context, tx *sql.Tx) ([]engine.SampleRecord, error) {
	rows, err := tx.QueryContext(ctx, `SELECT sample_id, content_hash, timestamp_ms, service, first_channel, frames_json, weight, seen_order, first_batch_id, first_commit_seq, duplicate_count, received_count FROM samples ORDER BY seen_order`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var samples []engine.SampleRecord
	for rows.Next() {
		record, err := scanSample(rows)
		if err != nil {
			return nil, err
		}
		samples = append(samples, record)
	}
	return samples, rows.Err()
}

func InsertSampleTx(ctx context.Context, tx *sql.Tx, record engine.SampleRecord, channelHits map[string]int64) error {
	framesJSON, err := json.Marshal(record.Frames)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO samples(sample_id, content_hash, timestamp_ms, service, first_channel, frames_json, weight, seen_order, first_batch_id, first_commit_seq, duplicate_count, received_count)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, record.ID, record.ContentHash, record.TimestampMS, record.Service, record.FirstChannel, string(framesJSON),
		record.Weight, record.SeenOrder, record.FirstBatchID, record.FirstCommitSeq, record.DuplicateCount, record.ReceivedCount)
	if err != nil {
		return err
	}
	return addChannelHitsTx(ctx, tx, record.ID, channelHits)
}

func AddSampleObservationsTx(ctx context.Context, tx *sql.Tx, sampleID string, additional int64, channelHits map[string]int64) error {
	if additional < 0 {
		return fmt.Errorf("additional observations must not be negative")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE samples SET duplicate_count=duplicate_count+?, received_count=received_count+? WHERE sample_id=?`, additional, additional, sampleID); err != nil {
		return err
	}
	return addChannelHitsTx(ctx, tx, sampleID, channelHits)
}

func addChannelHitsTx(ctx context.Context, tx *sql.Tx, sampleID string, channelHits map[string]int64) error {
	channels := make([]string, 0, len(channelHits))
	for channel := range channelHits {
		channels = append(channels, channel)
	}
	sort.Strings(channels)
	for _, channel := range channels {
		count := channelHits[channel]
		if count <= 0 {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO sample_channel_hits(sample_id, channel, observations) VALUES(?,?,?)
			ON CONFLICT(sample_id, channel) DO UPDATE SET observations=observations+excluded.observations`, sampleID, channel, count); err != nil {
			return err
		}
	}
	return nil
}

func ReplaceReservoirsTx(ctx context.Context, tx *sql.Tx, version uint64, commitSeq int64, reservoirs []engine.Reservoir) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM reservoirs WHERE policy_version=?`, version); err != nil {
		return err
	}
	statement, err := tx.PrepareContext(ctx, `INSERT INTO reservoirs(policy_version, reservoir_id, commit_seq, service, channel, layer_key, depth_prefixes_json, capacity, seen_count, duplicate_observation_count, selected_count, selected_json, replaced_ids_json, replaced_total, rejected_total, references_json)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer statement.Close()
	for index, reservoir := range reservoirs {
		depthJSON, _ := json.Marshal(reservoir.DepthPrefixes)
		selectedJSON, _ := json.Marshal(reservoir.Members)
		replacedJSON, _ := json.Marshal(reservoir.ReplacedSampleIDs)
		referencesJSON, _ := json.Marshal(reservoir.References)
		if _, err := statement.ExecContext(ctx, version, reservoir.ID, commitSeq, reservoir.Service, reservoir.Channel, reservoir.LayerKey,
			string(depthJSON), reservoir.Capacity, reservoir.SeenCount, reservoir.DuplicateObservationCount, reservoir.SelectedCount,
			string(selectedJSON), string(replacedJSON), reservoir.ReplacedTotal, reservoir.RejectedTotal, string(referencesJSON)); err != nil {
			return err
		}
		if len(reservoirs) > 1 && index == len(reservoirs)/2 {
			MaybeCrash("during_reservoir_replace")
		}
	}
	return nil
}

func loadReservoirsTx(ctx context.Context, tx *sql.Tx, version uint64, service, channel string) ([]engine.Reservoir, error) {
	query := `SELECT reservoir_id, policy_version, service, channel, layer_key, depth_prefixes_json, capacity, seen_count, duplicate_observation_count, selected_count, selected_json, replaced_ids_json, replaced_total, rejected_total, references_json FROM reservoirs WHERE policy_version=?`
	args := []any{version}
	if service != "" {
		query += ` AND service=?`
		args = append(args, service)
	}
	if channel != "" {
		query += ` AND channel=?`
		args = append(args, channel)
	}
	query += ` ORDER BY service, channel, layer_key`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var reservoirs []engine.Reservoir
	for rows.Next() {
		var reservoir engine.Reservoir
		var depthJSON, selectedJSON, replacedJSON, referencesJSON string
		if err := rows.Scan(&reservoir.ID, &reservoir.PolicyVersion, &reservoir.Service, &reservoir.Channel, &reservoir.LayerKey,
			&depthJSON, &reservoir.Capacity, &reservoir.SeenCount, &reservoir.DuplicateObservationCount, &reservoir.SelectedCount,
			&selectedJSON, &replacedJSON, &reservoir.ReplacedTotal, &reservoir.RejectedTotal, &referencesJSON); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(depthJSON), &reservoir.DepthPrefixes); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(selectedJSON), &reservoir.Members); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(replacedJSON), &reservoir.ReplacedSampleIDs); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(referencesJSON), &reservoir.References); err != nil {
			return nil, err
		}
		reservoirs = append(reservoirs, reservoir)
	}
	return reservoirs, rows.Err()
}

func InsertHotspotTx(ctx context.Context, tx *sql.Tx, report engine.HotspotReport) error {
	resultJSON, err := json.Marshal(report)
	if err != nil {
		return err
	}
	modeJSON, _ := json.Marshal(report.Modes)
	_, err = tx.ExecContext(ctx, `INSERT INTO hotspot_runs(policy_version, commit_seq, built_at, elapsed_ms, result_json, mode_summary_json)
		VALUES(?,?,?,?,?,?) ON CONFLICT(policy_version) DO UPDATE SET commit_seq=excluded.commit_seq, built_at=excluded.built_at, elapsed_ms=excluded.elapsed_ms, result_json=excluded.result_json, mode_summary_json=excluded.mode_summary_json`,
		report.PolicyVersion, report.CommitSeq, report.BuiltAt.Format(timeLayout), report.ElapsedMS, string(resultJSON), string(modeJSON))
	return err
}

func loadHotspotTx(ctx context.Context, tx *sql.Tx, version uint64) (engine.HotspotReport, error) {
	var raw string
	if err := tx.QueryRowContext(ctx, `SELECT result_json FROM hotspot_runs WHERE policy_version=?`, version).Scan(&raw); err != nil {
		return engine.HotspotReport{}, err
	}
	var report engine.HotspotReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return engine.HotspotReport{}, err
	}
	return report, nil
}

func InsertRebuildReportTx(ctx context.Context, tx *sql.Tx, report any) error {
	type versioned struct {
		PolicyVersion uint64 `json:"policy_version"`
		OldVersion    uint64 `json:"old_version"`
		CommitSeq     int64  `json:"commit_seq"`
	}
	raw, _ := json.Marshal(report)
	var v versioned
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO rebuild_reports(policy_version, old_version, commit_seq, report_json, created_at) VALUES(?,?,?,?,CURRENT_TIMESTAMP)
		ON CONFLICT(policy_version) DO UPDATE SET old_version=excluded.old_version, commit_seq=excluded.commit_seq, report_json=excluded.report_json, created_at=excluded.created_at`,
		v.PolicyVersion, v.OldVersion, v.CommitSeq, string(raw))
	return err
}

func (s *Store) RebuildReport(ctx context.Context, version uint64) (json.RawMessage, error) {
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT report_json FROM rebuild_reports WHERE policy_version=?`, version).Scan(&raw); err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}
