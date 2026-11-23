package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"gatewayfreshness.local/workbench/internal/freshness"
)

type scanner interface {
	Scan(dest ...any) error
}

type storedBatch struct {
	BatchID         string
	ContentHash     string
	Mode            string
	ReferenceTimeMS int64
	ReferenceSource string
	WindowVersion   int64
	ConfigVersion   string
	Outcome         string
	Code            string
	Message         string
	RequestCount    int
	PrecheckedCount int
	PrecheckLimit   int
	TotalWeight     int64
	SampleRate      float64
	PrecheckMethod  string
	Confidence      string
	ConfidenceNote  string
	BoundaryRule    string
	CommittedAtMS   int64
}

const batchSelect = `SELECT batch_id,content_hash,mode,reference_time_ms,reference_source,window_version,config_version,outcome,code,message,
	request_count,prechecked_count,precheck_limit,total_weight,sample_rate,precheck_method,confidence,confidence_note,boundary_rule,committed_at_ms
	FROM batches`

func scanStoredBatch(row scanner) (*storedBatch, error) {
	var b storedBatch
	err := row.Scan(&b.BatchID, &b.ContentHash, &b.Mode, &b.ReferenceTimeMS, &b.ReferenceSource, &b.WindowVersion,
		&b.ConfigVersion, &b.Outcome, &b.Code, &b.Message, &b.RequestCount, &b.PrecheckedCount, &b.PrecheckLimit,
		&b.TotalWeight, &b.SampleRate, &b.PrecheckMethod, &b.Confidence, &b.ConfidenceNote, &b.BoundaryRule, &b.CommittedAtMS)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, sql.ErrNoRows
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (b *storedBatch) decision(items []freshness.ItemDecision) *freshness.BatchDecision {
	return &freshness.BatchDecision{
		BatchID: b.BatchID, Mode: b.Mode, ContentHash: b.ContentHash,
		ReferenceTimeMS: b.ReferenceTimeMS, ReferenceSource: b.ReferenceSource,
		WindowVersion: b.WindowVersion, ToleranceConfigVersion: b.ConfigVersion,
		Outcome: b.Outcome, Code: b.Code, Message: b.Message,
		RequestCount: b.RequestCount, PrecheckedCount: b.PrecheckedCount,
		TotalWeight: b.TotalWeight, SampleRate: b.SampleRate, PrecheckLimit: b.PrecheckLimit,
		PrecheckMethod: b.PrecheckMethod, Confidence: b.Confidence, ConfidenceNote: b.ConfidenceNote,
		BoundaryRule: b.BoundaryRule, Items: items, CommittedAtMS: b.CommittedAtMS,
	}
}

func (s *Store) GetDecision(ctx context.Context, batchID string) (*freshness.BatchDecision, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin decision read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	decision, err := s.getDecisionTx(ctx, tx, batchID)
	if err != nil {
		return nil, err
	}
	if decision == nil {
		return nil, ErrBatchNotFound
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return decision, nil
}

func (s *Store) getDecisionTx(ctx context.Context, tx *sql.Tx, batchID string) (*freshness.BatchDecision, error) {
	b, err := scanStoredBatch(tx.QueryRowContext(ctx, batchSelect+` WHERE batch_id=?`, batchID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read batch decision: %w", err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT ordinal,window_version,config_version,item_json FROM batch_items WHERE batch_id=? ORDER BY ordinal`, batchID)
	if err != nil {
		return nil, fmt.Errorf("read batch items: %w", err)
	}
	defer rows.Close()
	items := make([]freshness.ItemDecision, 0, b.RequestCount)
	for rows.Next() {
		var ordinal int
		var windowVersion int64
		var configVersion, itemJSON string
		if err := rows.Scan(&ordinal, &windowVersion, &configVersion, &itemJSON); err != nil {
			return nil, err
		}
		if windowVersion != b.WindowVersion || configVersion != b.ConfigVersion {
			return nil, fmt.Errorf("cross-version batch item detected for %s", batchID)
		}
		var item freshness.ItemDecision
		if err := json.Unmarshal([]byte(itemJSON), &item); err != nil {
			return nil, fmt.Errorf("decode item %d: %w", ordinal, err)
		}
		if item.Ordinal != ordinal || item.WindowVersion != b.WindowVersion || item.ToleranceConfigVersion != b.ConfigVersion {
			return nil, fmt.Errorf("item version mismatch for batch %s ordinal %d", batchID, ordinal)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(items) != b.RequestCount {
		return nil, fmt.Errorf("batch %s items=%d expected=%d", batchID, len(items), b.RequestCount)
	}
	return b.decision(items), nil
}
