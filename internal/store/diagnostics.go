package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gatewayfreshness.local/workbench/internal/freshness"
)

type SnapshotOptions struct {
	Version    int64
	Budget     time.Duration
	Include    []string
	ClientID   string
	BatchID    string
	TrailLimit int
}

func (s *Store) Snapshot(ctx context.Context, opt SnapshotOptions) (*freshness.DiagnosticSnapshot, error) {
	start := time.Now()
	latest, latestRef, latestConfig, err := s.latestState(ctx)
	if err != nil {
		return nil, err
	}
	version := opt.Version
	if version == 0 {
		version = latest
	}
	if version < 0 || version > latest {
		return nil, freshness.NewAppError(freshness.CodeVersionInvalid, "requested window version is not committed", map[string]any{
			"requested": version, "latest_committed": latest,
		})
	}
	if version > 0 && version != latest {
		committed, err := s.committedVersionExists(ctx, version)
		if err != nil {
			return nil, err
		}
		if !committed {
			return nil, freshness.NewAppError(freshness.CodeVersionInvalid, "requested window version is an aborted/version gap, not a committed version", map[string]any{
				"requested": version, "latest_committed": latest,
			})
		}
	}
	cfgVersion, asOfMS, err := s.versionMetadata(ctx, version, latest, latestRef, latestConfig)
	if err != nil {
		return nil, err
	}
	include := normalizeInclude(opt.Include)
	snapshot := &freshness.DiagnosticSnapshot{
		SnapshotWindowVersion: version, ToleranceConfigVersion: cfgVersion, AsOfMS: asOfMS,
		BudgetMS: opt.Budget.Milliseconds(), CompletedSections: []string{}, IncompleteSections: []string{},
	}
	if version == 0 {
		snapshot.Complete = true
		snapshot.ElapsedMS = time.Since(start).Milliseconds()
		snapshot.Occupancy = &freshness.OccupancySection{Records: []freshness.OccupancyRecord{}, AsOfMS: asOfMS}
		snapshot.Nonces = &freshness.NonceTableSection{Records: []freshness.NonceRecord{}}
		snapshot.Trails = &freshness.TrailSection{Records: []freshness.TrailRecord{}}
		return snapshot, nil
	}
	if opt.Budget <= 0 {
		snapshot.Partial = true
		snapshot.IncompleteSections = include
		snapshot.Reason = "budget_exhausted_before_read_transaction"
		snapshot.ElapsedMS = time.Since(start).Milliseconds()
		return snapshot, nil
	}

	budgetCtx, cancel := context.WithDeadline(ctx, start.Add(opt.Budget))
	defer cancel()
	tx, err := s.db.BeginTx(budgetCtx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || budgetCtx.Err() != nil {
			snapshot.Partial = true
			snapshot.IncompleteSections = include
			snapshot.Reason = "budget_exhausted_before_read_transaction"
			snapshot.ElapsedMS = time.Since(start).Milliseconds()
			return snapshot, nil
		}
		return nil, fmt.Errorf("begin diagnostic read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for i, section := range include {
		if budgetCtx.Err() != nil || !time.Now().Before(start.Add(opt.Budget)) {
			snapshot.IncompleteSections = append(snapshot.IncompleteSections, include[i:]...)
			snapshot.Reason = "budget_exhausted_between_consistent_sections"
			break
		}
		var secErr error
		switch section {
		case "occupancy":
			snapshot.Occupancy, secErr = s.queryOccupancy(budgetCtx, tx, version, asOfMS, opt.ClientID)
		case "nonces":
			snapshot.Nonces, secErr = s.queryNonceTable(budgetCtx, tx, version, opt.ClientID)
		case "trails":
			snapshot.Trails, secErr = s.queryTrails(budgetCtx, tx, version, opt.BatchID, opt.TrailLimit)
		}
		if secErr != nil {
			if budgetCtx.Err() != nil || errors.Is(secErr, context.DeadlineExceeded) {
				snapshot.IncompleteSections = append(snapshot.IncompleteSections, include[i:]...)
				snapshot.Reason = "budget_exhausted_during_section"
				break
			}
			return nil, fmt.Errorf("diagnostic section %s: %w", section, secErr)
		}
		snapshot.CompletedSections = append(snapshot.CompletedSections, section)
	}
	_ = tx.Commit()
	snapshot.Partial = len(snapshot.IncompleteSections) > 0
	snapshot.Complete = !snapshot.Partial
	snapshot.ElapsedMS = time.Since(start).Milliseconds()
	return snapshot, nil
}

func (s *Store) latestState(ctx context.Context) (version, referenceMS int64, configVersion string, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT last_committed_version,last_reference_time_ms,last_config_version FROM server_state WHERE id=1`).
		Scan(&version, &referenceMS, &configVersion)
	return
}

func (s *Store) committedVersionExists(ctx context.Context, version int64) (bool, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM batches WHERE window_version=?`, version).Scan(&count); err != nil {
		return false, err
	}
	return count == 1, nil
}

func (s *Store) versionMetadata(ctx context.Context, version, latest, latestRef int64, latestConfig string) (string, int64, error) {
	if version == 0 {
		return latestConfig, latestRef, nil
	}
	if version == latest {
		return latestConfig, latestRef, nil
	}
	var cfg string
	var ref int64
	err := s.db.QueryRowContext(ctx, `SELECT config_version,reference_time_ms FROM batches WHERE window_version<=? ORDER BY window_version DESC LIMIT 1`, version).Scan(&cfg, &ref)
	if errors.Is(err, sql.ErrNoRows) {
		return latestConfig, 0, nil
	}
	return cfg, ref, err
}

func normalizeInclude(in []string) []string {
	if len(in) == 0 {
		return []string{"occupancy", "nonces", "trails"}
	}
	seen := map[string]bool{}
	out := make([]string, 0, 3)
	for _, name := range []string{"occupancy", "nonces", "trails"} {
		for _, requested := range in {
			if strings.EqualFold(strings.TrimSpace(requested), name) {
				seen[name] = true
			}
		}
		if seen[name] {
			out = append(out, name)
		}
	}
	if len(out) == 0 {
		return []string{"occupancy", "nonces", "trails"}
	}
	return out
}

type nonceState struct {
	freshness.NonceRecord
}

func (s *Store) loadNonceStates(ctx context.Context, tx *sql.Tx, version int64, excludeBatch string) (map[string]*nonceState, error) {
	query := `SELECT client_id,nonce,event_type,at_ms,first_occupied_at_ms,expires_at_ms,window_ms,weight,batch_id,request_id,window_version,config_version
		FROM nonce_events WHERE window_version<=?`
	args := []any{version}
	if excludeBatch != "" {
		query += ` AND batch_id<>?`
		args = append(args, excludeBatch)
	}
	query += ` ORDER BY id`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	states := map[string]*nonceState{}
	for rows.Next() {
		var rec freshness.NonceRecord
		var eventType, batchID, requestID string
		if err := rows.Scan(&rec.ClientID, &rec.Nonce, &eventType, &rec.LastEventAtMS, &rec.FirstOccupiedAtMS, &rec.ExpiresAtMS,
			&rec.WindowMS, &rec.Weight, &batchID, &requestID, &rec.LastEventVersion, &rec.ToleranceConfigVersion); err != nil {
			return nil, err
		}
		key := rec.ClientID + "\x00" + rec.Nonce
		if eventType == "occupy" {
			rec.State = "active"
			rec.FirstBatchID = batchID
			rec.FirstRequestID = requestID
			rec.WindowVersion = rec.LastEventVersion
			states[key] = &nonceState{NonceRecord: rec}
		} else {
			if st, ok := states[key]; ok {
				st.State = "reclaimed"
				st.LastEventVersion = rec.LastEventVersion
				st.LastEventAtMS = rec.LastEventAtMS
			}
		}
	}
	return states, rows.Err()
}

func (s *Store) queryOccupancy(ctx context.Context, tx *sql.Tx, version, asOfMS int64, clientID string) (*freshness.OccupancySection, error) {
	states, err := s.loadNonceStates(ctx, tx, version, "")
	if err != nil {
		return nil, err
	}
	section := &freshness.OccupancySection{Records: []freshness.OccupancyRecord{}, AsOfMS: asOfMS}
	for _, st := range states {
		if st.State != "active" || (clientID != "" && st.ClientID != clientID) {
			continue
		}
		remaining := st.ExpiresAtMS - asOfMS
		if remaining <= 0 {
			section.ExpiredUnreclaimed++
			continue
		}
		section.Records = append(section.Records, freshness.OccupancyRecord{
			ClientID: st.ClientID, Nonce: st.Nonce, FirstOccupiedAtMS: st.FirstOccupiedAtMS,
			WindowMS: st.WindowMS, ExpiresAtMS: st.ExpiresAtMS, RemainingWindowMS: remaining,
			Weight: st.Weight, FirstBatchID: st.FirstBatchID, FirstRequestID: st.FirstRequestID,
			WindowVersion: st.WindowVersion, ToleranceConfigVersion: st.ToleranceConfigVersion,
		})
		section.ActiveCount++
		section.TotalOutstandingWeight += st.Weight
	}
	sort.Slice(section.Records, func(i, j int) bool {
		if section.Records[i].ClientID == section.Records[j].ClientID {
			return section.Records[i].Nonce < section.Records[j].Nonce
		}
		return section.Records[i].ClientID < section.Records[j].ClientID
	})
	return section, nil
}

func (s *Store) queryNonceTable(ctx context.Context, tx *sql.Tx, version int64, clientID string) (*freshness.NonceTableSection, error) {
	states, err := s.loadNonceStates(ctx, tx, version, "")
	if err != nil {
		return nil, err
	}
	section := &freshness.NonceTableSection{Records: []freshness.NonceRecord{}}
	for _, st := range states {
		if clientID != "" && st.ClientID != clientID {
			continue
		}
		section.Records = append(section.Records, st.NonceRecord)
		if st.State == "active" {
			section.ActiveCount++
		} else {
			section.ReclaimedCount++
		}
	}
	sort.Slice(section.Records, func(i, j int) bool {
		if section.Records[i].ClientID == section.Records[j].ClientID {
			return section.Records[i].Nonce < section.Records[j].Nonce
		}
		return section.Records[i].ClientID < section.Records[j].ClientID
	})
	return section, nil
}

func (s *Store) queryTrails(ctx context.Context, tx *sql.Tx, version int64, batchID string, limit int) (*freshness.TrailSection, error) {
	if limit <= 0 || limit > 2000 {
		limit = 500
	}
	query := `SELECT id,batch_id,window_version,config_version,ordinal,request_id,stage,outcome,code,at_ms,details_json
		FROM audit_trail WHERE window_version<=?`
	args := []any{version}
	if batchID != "" {
		query += ` AND batch_id=?`
		args = append(args, batchID)
	}
	query += ` ORDER BY id LIMIT ?`
	args = append(args, limit)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	section := &freshness.TrailSection{Records: []freshness.TrailRecord{}}
	for rows.Next() {
		var r freshness.TrailRecord
		if err := rows.Scan(&r.ID, &r.BatchID, &r.WindowVersion, &r.ToleranceConfigVersion, &r.Ordinal, &r.RequestID,
			&r.Stage, &r.Outcome, &r.Code, &r.AtMS, &r.DetailsJSON); err != nil {
			return nil, err
		}
		section.Records = append(section.Records, r)
	}
	section.Count = len(section.Records)
	return section, rows.Err()
}

func (s *Store) ConfigAtVersion(ctx context.Context, version int64) (string, error) {
	if version <= 0 {
		latest, _, cfg, err := s.latestState(ctx)
		_ = latest
		return cfg, err
	}
	var cfg string
	err := s.db.QueryRowContext(ctx, `SELECT config_version FROM batches WHERE window_version<=? ORDER BY window_version DESC LIMIT 1`, version).Scan(&cfg)
	if errors.Is(err, sql.ErrNoRows) {
		latest, _, latestCfg, lerr := s.latestState(ctx)
		_ = latest
		return latestCfg, lerr
	}
	return cfg, err
}
