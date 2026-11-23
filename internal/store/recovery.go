package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"strings"

	"cdn-edge-consistency/internal/model"
)

type IntentRecovery struct {
	ID           int64  `json:"id"`
	Kind         string `json:"kind"`
	RequestID    string `json:"request_id"`
	Action       string `json:"action"`
	BusinessCode string `json:"business_code"`
}

type PurgeRecovery struct {
	PurgeID         string   `json:"purge_id"`
	PreviousStatus  string   `json:"previous_status"`
	BusinessCode    string   `json:"business_code"`
	IncompleteNodes []string `json:"incomplete_nodes"`
}

type BusinessCodeFact struct {
	Code   string `json:"code"`
	Count  int64  `json:"count"`
	Source string `json:"source"`
}

type RecoveryReport struct {
	GeneratedAtMs    int64              `json:"generated_at_ms"`
	FactsOnly        bool               `json:"facts_only"`
	ScannedIntents   int                `json:"scanned_intents"`
	RecoveredIntents []IntentRecovery   `json:"recovered_intents,omitempty"`
	AbortedPurges    []PurgeRecovery    `json:"aborted_purges,omitempty"`
	AbortedRequests  []string           `json:"aborted_requests,omitempty"`
	RolledBackRows   int                `json:"rolled_back_rows"`
	BusinessCodes    []BusinessCodeFact `json:"business_codes,omitempty"`
}

func (s *Store) Recover(ctx context.Context, nowMs int64) (*RecoveryReport, error) {
	report := &RecoveryReport{GeneratedAtMs: nowMs, FactsOnly: true}
	err := s.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		intents, err := ListPendingIntentsTx(ctx, tx)
		if err != nil {
			return err
		}
		report.ScannedIntents = len(intents)
		for _, in := range intents {
			code := in.BusinessCode
			if code == "" {
				if in.Kind == "purge" || strings.HasPrefix(in.Kind, "purge_") {
					code = model.CodePurgeIncomplete
				} else {
					code = model.CodeRequestAborted
				}
			}
			n, err := ApplyUndoTx(ctx, tx, in.Payload.Undo, nowMs)
			if err != nil {
				return err
			}
			report.RolledBackRows += n
			if err := AbortIntentTx(ctx, tx, in.ID, code, nowMs); err != nil {
				return err
			}
			report.RecoveredIntents = append(report.RecoveredIntents, IntentRecovery{
				ID: in.ID, Kind: in.Kind, RequestID: in.RequestID, Action: "undone_and_aborted", BusinessCode: code,
			})
		}

		rows, err := tx.QueryContext(ctx, `SELECT purge_id,status FROM purge_records WHERE status IN ('preparing','pending') ORDER BY purge_id`)
		if err != nil {
			return err
		}
		type pendingPurge struct{ id, status string }
		var purges []pendingPurge
		for rows.Next() {
			var p pendingPurge
			if err := rows.Scan(&p.id, &p.status); err != nil {
				rows.Close()
				return err
			}
			purges = append(purges, p)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, p := range purges {
			incomplete, err := incompleteNodesTx(ctx, tx, p.id)
			if err != nil {
				return err
			}
			raw, _ := json.Marshal(incomplete)
			if _, err := tx.ExecContext(ctx, `UPDATE purge_records SET status='aborted',error_code=?,incomplete_nodes_json=?,completed_at_ms=? WHERE purge_id=?`,
				model.CodePurgeIncomplete, string(raw), nowMs, p.id); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE purge_deliveries SET status='aborted',error_code=?,completed_at_ms=? WHERE purge_id=? AND status<>'complete'`,
				model.CodePurgeIncomplete, nowMs, p.id); err != nil {
				return err
			}
			report.AbortedPurges = append(report.AbortedPurges, PurgeRecovery{
				PurgeID: p.id, PreviousStatus: p.status, BusinessCode: model.CodePurgeIncomplete, IncompleteNodes: incomplete,
			})
		}

		reqRows, err := tx.QueryContext(ctx, `SELECT request_id FROM request_records WHERE status='inflight' ORDER BY created_at_ms,request_id`)
		if err != nil {
			return err
		}
		var inflight []string
		for reqRows.Next() {
			var id string
			if err := reqRows.Scan(&id); err != nil {
				reqRows.Close()
				return err
			}
			inflight = append(inflight, id)
		}
		if err := reqRows.Close(); err != nil {
			return err
		}
		body, _ := json.Marshal(map[string]any{
			"code":    model.CodeRequestAborted,
			"message": "request was in flight when the process stopped; recovered as aborted",
		})
		for _, id := range inflight {
			if _, err := tx.ExecContext(ctx, `UPDATE request_records SET status='aborted',response_status=409,response_body=?,business_code=?,completed_at_ms=? WHERE request_id=?`,
				body, model.CodeRequestAborted, nowMs, id); err != nil {
				return err
			}
			report.AbortedRequests = append(report.AbortedRequests, id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	codes, err := s.businessCodeFacts(ctx)
	if err != nil {
		return nil, err
	}
	report.BusinessCodes = codes
	raw, err := json.Marshal(report)
	if err != nil {
		return nil, err
	}
	if err := s.Do(ctx, func(_ context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO recovery_reports(generated_at_ms,report_json) VALUES(?,?)`, nowMs, string(raw))
		return err
	}); err != nil {
		return nil, err
	}
	return report, nil
}

func incompleteNodesTx(ctx context.Context, tx *sql.Tx, purgeID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT node_id,status FROM purge_deliveries WHERE purge_id=? ORDER BY node_id`, purgeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var incomplete []string
	var all []string
	for rows.Next() {
		var node, status string
		if err := rows.Scan(&node, &status); err != nil {
			return nil, err
		}
		all = append(all, node)
		if status != "complete" {
			incomplete = append(incomplete, node)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// If every simulated delivery completed but the transaction never carried
	// a final commit marker, the purge as a whole is still incomplete and every
	// applicable node must appear in the recovery report.
	if len(incomplete) == 0 {
		return all, nil
	}
	return incomplete, nil
}

func (s *Store) businessCodeFacts(ctx context.Context) ([]BusinessCodeFact, error) {
	facts := map[string]*BusinessCodeFact{}
	add := func(code, source string, count int64) {
		if code == "" {
			return
		}
		k := source + "\x00" + code
		if _, ok := facts[k]; !ok {
			facts[k] = &BusinessCodeFact{Code: code, Source: source}
		}
		facts[k].Count += count
	}
	queries := []struct {
		source string
		query  string
	}{
		{"request_records", `SELECT business_code,COUNT(*) FROM request_records WHERE business_code<>'' GROUP BY business_code`},
		{"purge_records", `SELECT error_code,COUNT(*) FROM purge_records WHERE error_code<>'' GROUP BY error_code`},
		{"intents", `SELECT business_code,COUNT(*) FROM intents WHERE business_code<>'' GROUP BY business_code`},
		{"audit_events", `SELECT code,COUNT(*) FROM audit_events GROUP BY code`},
		{"origin_attempts", `SELECT code,COUNT(*) FROM origin_attempts WHERE code<>'' GROUP BY code`},
	}
	for _, q := range queries {
		rows, err := s.db.QueryContext(ctx, q.query)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var code string
			var n int64
			if err := rows.Scan(&code, &n); err != nil {
				rows.Close()
				return nil, err
			}
			add(code, q.source, n)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	out := make([]BusinessCodeFact, 0, len(facts))
	for _, f := range facts {
		out = append(out, *f)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Code != out[j].Code {
			return out[i].Code < out[j].Code
		}
		return out[i].Source < out[j].Source
	})
	return out, nil
}

func (s *Store) LatestRecoveryReport(ctx context.Context) (*RecoveryReport, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM recovery_reports ORDER BY id DESC LIMIT 1`).Scan(&raw)
	if err != nil {
		return nil, err
	}
	var r RecoveryReport
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return nil, err
	}
	return &r, nil
}
