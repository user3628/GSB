package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

func (s *Store) CreateIntent(ctx context.Context, rec IntentRecord) error {
	facts := marshalAny(rec.Facts)
	_, err := s.db.ExecContext(ctx, `INSERT INTO intents(intent_id, request_id, kind, snapshot_version, status, created_at_ms, completed_at_ms, facts_json)
		VALUES(?, ?, ?, ?, ?, ?, NULL, ?)`, rec.IntentID, rec.RequestID, rec.Kind, int64(rec.SnapshotVersion), rec.Status, rec.CreatedAtMs, facts)
	return err
}

func (s *Store) CreateRequestPending(ctx context.Context, rec RequestRecord) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO request_registry(request_id, request_hash, status, http_status, response_json, report_id, created_at_ms, updated_at_ms)
		VALUES(?, ?, 'in_progress', 0, '', '', ?, ?)`, rec.RequestID, rec.RequestHash, rec.CreatedAtMs, rec.CreatedAtMs)
	return err
}

func (s *Store) UpdateRequestPendingResponse(ctx context.Context, requestID, requestHash string, nowMs int64) error {
	res, err := s.db.ExecContext(ctx, `UPDATE request_registry SET request_hash = ?, updated_at_ms = ? WHERE request_id = ? AND status = 'in_progress'`, requestHash, nowMs, requestID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("request %s is not pending", requestID)
	}
	return nil
}

type TerminalOutcome struct {
	ReportID    string
	Report      *ReportRecord
	RequestID   string
	RequestHash string
	HTTPStatus  int
	Response    []byte
	IntentID    string
	IntentState string
	Facts       map[string]any
	NowMs       int64
}

// PersistTerminalOutcome records failed/rejected attempts without touching the
// active snapshot or any derived structure. The intent row already exists.
func (s *Store) PersistTerminalOutcome(ctx context.Context, out TerminalOutcome) error {
	tx, err := s.BeginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if out.Report != nil {
		if err := insertReportTx(ctx, tx, *out.Report); err != nil {
			return err
		}
	}
	if err := completeRequestTx(ctx, tx, out.RequestID, out.HTTPStatus, out.Response, out.ReportID, out.NowMs); err != nil {
		return err
	}
	facts := marshalAny(out.Facts)
	res, err := tx.ExecContext(ctx, `UPDATE intents SET status = ?, completed_at_ms = ?, facts_json = ? WHERE intent_id = ? AND status = 'pending'`, out.IntentState, out.NowMs, facts, out.IntentID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("intent %s is not pending", out.IntentID)
	}
	return tx.Commit()
}

func insertReportTx(ctx context.Context, tx *sql.Tx, r ReportRecord) error {
	reportJSON := r.ReportJSON
	if len(reportJSON) == 0 {
		reportJSON = []byte("{}")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO import_reports(report_id, request_id, snapshot_version, status, decision, source_format, format_version,
		source_digest, content_digest, report_json, created_at_ms) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ReportID, r.RequestID, int64(r.SnapshotVersion), r.Status, r.Decision, r.SourceFormat, r.FormatVersion,
		r.SourceDigest, r.ContentDigest, string(reportJSON), r.CreatedAtMs)
	if err != nil {
		return err
	}
	for i, item := range r.Items {
		_, err := tx.ExecContext(ctx, `INSERT INTO import_report_items(report_id, seq, policy_id, outcome, basis_json, detail_json)
			VALUES(?, ?, ?, ?, ?, ?)`, r.ReportID, i+1, item.PolicyID, item.Outcome, marshalStrings(item.Basis), marshalAny(item.Detail))
		if err != nil {
			return err
		}
	}
	return nil
}

func completeRequestTx(ctx context.Context, tx *sql.Tx, requestID string, status int, response []byte, reportID string, nowMs int64) error {
	res, err := tx.ExecContext(ctx, `UPDATE request_registry SET status = 'completed', http_status = ?, response_json = ?, report_id = ?, updated_at_ms = ?
		WHERE request_id = ? AND status = 'in_progress'`, status, string(response), reportID, nowMs, requestID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("request %s is not pending", requestID)
	}
	return nil
}

func (s *Store) MarkIntentAborted(ctx context.Context, intentID string, facts map[string]any, nowMs int64) error {
	factsJSON := marshalAny(facts)
	res, err := s.db.ExecContext(ctx, `UPDATE intents SET status = 'aborted', completed_at_ms = ?, facts_json = ? WHERE intent_id = ? AND status = 'pending'`, nowMs, factsJSON, intentID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return errors.New("intent is not pending")
	}
	return nil
}

func (s *Store) FindIntent(ctx context.Context, intentID string) (*IntentRecord, error) {
	var r IntentRecord
	var version int64
	var completed sql.NullInt64
	var facts string
	err := s.db.QueryRowContext(ctx, `SELECT intent_id, request_id, kind, snapshot_version, status, created_at_ms, completed_at_ms, facts_json FROM intents WHERE intent_id = ?`, intentID).
		Scan(&r.IntentID, &r.RequestID, &r.Kind, &version, &r.Status, &r.CreatedAtMs, &completed, &facts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	r.SnapshotVersion = uint64(version)
	_ = json.Unmarshal([]byte(facts), &r.Facts)
	return &r, nil
}
