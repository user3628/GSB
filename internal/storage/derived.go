package storage

import (
	"database/sql"

	"alarmengine/internal/model"
)

// InsertSuppressionHit writes one rule-hit row inside tx.
func InsertSuppressionHit(tx *sql.Tx, version int64, h model.SuppressionHit) error {
	_, err := tx.Exec(
		`INSERT INTO suppression_hits
		 (alert_id,version,rule_id,rule_service,rule_zone,hop,order_index,matched_at_ms,release_at_ms)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		h.AlertID, version, h.RuleID, h.Service, h.Zone, h.Hop, h.OrderIndex, h.MatchedAtMS, h.ReleaseAtMS)
	return err
}

// LoadSuppressionHits reads all hits of a version ordered for audit.
func LoadSuppressionHits(q Querier, version int64) ([]model.SuppressionHit, error) {
	rows, err := q.Query(
		`SELECT alert_id,rule_id,rule_service,rule_zone,hop,order_index,matched_at_ms,release_at_ms
		 FROM suppression_hits WHERE version=?
		 ORDER BY alert_id, order_index`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.SuppressionHit
	for rows.Next() {
		var h model.SuppressionHit
		if err := rows.Scan(&h.AlertID, &h.RuleID, &h.Service, &h.Zone, &h.Hop,
			&h.OrderIndex, &h.MatchedAtMS, &h.ReleaseAtMS); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// InsertRootCauseScore writes one score row inside tx.
func InsertRootCauseScore(tx *sql.Tx, r model.RootCauseScore) error {
	_, err := tx.Exec(
		`INSERT INTO root_cause_scores
		 (version,trigger_alert_id,candidate_service,hops,time_delta_ms,score_raw_milli,
		  score_final_milli,rounding_error_nano,rank)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		r.Version, r.TriggerAlertID, r.CandidateService, r.Hops, r.TimeDeltaMS,
		r.ScoreRawMilli, r.ScoreFinalMilli, r.RoundingErrorNano, r.Rank)
	return err
}

// LoadRootCauseScores reads scores of a version in rank order.
func LoadRootCauseScores(q Querier, version int64) ([]model.RootCauseScore, error) {
	rows, err := q.Query(
		`SELECT trigger_alert_id,candidate_service,hops,time_delta_ms,score_raw_milli,
		        score_final_milli,rounding_error_nano,rank,version
		 FROM root_cause_scores WHERE version=?
		 ORDER BY trigger_alert_id, rank`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.RootCauseScore
	for rows.Next() {
		var r model.RootCauseScore
		if err := rows.Scan(&r.TriggerAlertID, &r.CandidateService, &r.Hops, &r.TimeDeltaMS,
			&r.ScoreRawMilli, &r.ScoreFinalMilli, &r.RoundingErrorNano, &r.Rank, &r.Version); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// InsertDerivedRef records a derived-structure reference inside tx.
func InsertDerivedRef(tx *sql.Tx, version int64, serviceID, fingerprint, kind, refKey string) error {
	_, err := tx.Exec(
		`INSERT INTO derived_refs(version,service_id,fingerprint,ref_kind,ref_key)
		 VALUES(?,?,?,?,?)`, version, serviceID, fingerprint, kind, refKey)
	return err
}

// LoadDerivedRefs reads all references of a version.
func LoadDerivedRefs(q Querier, version int64) ([]DerivedRef, error) {
	rows, err := q.Query(
		`SELECT service_id,fingerprint,ref_kind,ref_key FROM derived_refs WHERE version=?
		 ORDER BY service_id, fingerprint, ref_kind`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DerivedRef
	for rows.Next() {
		var r DerivedRef
		if err := rows.Scan(&r.ServiceID, &r.Fingerprint, &r.Kind, &r.RefKey); err != nil {
			return nil, err
		}
		r.Version = version
		out = append(out, r)
	}
	return out, rows.Err()
}

// DerivedRef is one derived-structure reference.
type DerivedRef struct {
	Version     int64
	ServiceID   string
	Fingerprint string
	Kind        string
	RefKey      string
}

// DeleteDerivedForVersion removes alerts, hits, scores and refs for a version
// inside one transaction (atomic rebuild).
func DeleteDerivedForVersion(tx *sql.Tx, version int64) error {
	stmts := []string{
		`DELETE FROM suppression_hits WHERE version=?`,
		`DELETE FROM root_cause_scores WHERE version=?`,
		`DELETE FROM derived_refs WHERE version=?`,
		`DELETE FROM alerts WHERE version=?`,
	}
	for _, st := range stmts {
		if _, err := tx.Exec(st, version); err != nil {
			return err
		}
	}
	return nil
}
