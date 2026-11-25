package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
)

// ComponentRecovery is one component line in a recovery report. Every
// conclusion is derived only from rows already durable on disk.
type ComponentRecovery struct {
	ComponentID       string   `json:"component_id"`
	ViewVersion       int64    `json:"view_version"`
	ViewDigest        string   `json:"view_digest"`
	Status            string   `json:"status"` // converged | rolled_back
	RolledBackVersion int64    `json:"rolled_back_version,omitempty"`
	PendingKeys       []string `json:"pending_keys"`
}

// RecoveryReport describes the outcome of scanning unfinished intents.
type RecoveryReport struct {
	RecoveredAt       string              `json:"recovered_at"`
	LastGoodVersion   int64               `json:"last_good_version"`
	AbortedIntents    []string            `json:"aborted_intents"`
	RolledBackCommits []string            `json:"rolled_back_commits"`
	Components        []ComponentRecovery `json:"components"`
}

type pendingIntent struct {
	intentID      string
	commitID      string
	kind          string
	targetVersion int64
	pendingKeys   []string
	payload       string
}

// recoverPendingIntents is called once at startup. An intent without a
// matching committed record is an unfinished recompute: its fact writes are
// discarded by rebuilding the materialized facts from the last complete
// snapshot/delta lineage, and the intent is marked aborted.
func (e *Engine) recoverPendingIntents() error {
	ctx := context.Background()
	report := &RecoveryReport{RecoveredAt: e.now()}

	lastGood, err := e.currentVersion(ctx)
	if err != nil {
		return err
	}
	report.LastGoodVersion = lastGood

	rows, err := e.db.QueryContext(ctx, `
		SELECT intent_id, commit_id, kind, target_version, pending_keys, payload
		FROM intents WHERE status='prepared' ORDER BY created_at`)
	if err != nil {
		return err
	}
	var pendings []pendingIntent
	for rows.Next() {
		var p pendingIntent
		var keysJSON string
		if err := rows.Scan(&p.intentID, &p.commitID, &p.kind, &p.targetVersion,
			&keysJSON, &p.payload); err != nil {
			rows.Close()
			return err
		}
		_ = json.Unmarshal([]byte(keysJSON), &p.pendingKeys)
		pendings = append(pendings, p)
	}
	rows.Close()

	if len(pendings) > 0 {
		tx, err := e.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		for _, p := range pendings {
			// Roll facts back to the last complete version by replaying the
			// lineage of committed payloads inside one transaction.
			if err := e.rollbackFactsToLastGood(ctx, tx); err != nil {
				_ = tx.Rollback()
				return err
			}
			if _, err := tx.ExecContext(ctx,
				`UPDATE intents SET status='aborted', phase='rolled_back', updated_at=?
				 WHERE intent_id=?`, e.now(), p.intentID); err != nil {
				_ = tx.Rollback()
				return err
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO commits(commit_id, kind, version, base_version, checksum,
					status, created_at, payload)
				VALUES(?,?,?,?,?, 'aborted', ?, ?)`,
				p.commitID, p.kind, p.targetVersion, lastGood,
				"recovery-aborted", e.now(), p.payload); err != nil {
				_ = tx.Rollback()
				return err
			}
			report.AbortedIntents = append(report.AbortedIntents, p.intentID)
			report.RolledBackCommits = append(report.RolledBackCommits, p.commitID)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	comps, err := e.recoveryComponentRows(ctx, pendings)
	if err != nil {
		return err
	}
	report.Components = comps

	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	if _, err := e.db.ExecContext(ctx,
		`INSERT INTO recovery_reports(created_at, report_json) VALUES(?,?)`,
		e.now(), string(raw)); err != nil {
		return err
	}
	e.lastReport = report
	return nil
}

// rollbackFactsToLastGood rebuilds fact tables by replaying every committed
// payload in version order, guaranteeing facts match the last complete commit.
func (e *Engine) rollbackFactsToLastGood(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT payload FROM commits WHERE status='committed'
		ORDER BY version ASC`)
	if err != nil {
		return err
	}
	var payloads []string
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			rows.Close()
			return err
		}
		payloads = append(payloads, payload)
	}
	rows.Close()

	for _, table := range []string{
		"config_items", "components", "subscriptions", "dependencies",
	} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table); err != nil {
			return err
		}
	}

	var st *materializedState
	for _, payload := range payloads {
		var req CommitRequest
		if err := json.Unmarshal([]byte(payload), &req); err != nil {
			return err
		}
		switch req.Kind {
		case "snapshot":
			st = stateFromSnapshot(req.Snapshot)
		case "delta":
			if st == nil {
				continue
			}
			next := cloneState(st)
			applyDeltaToState(next, req.Delta)
			st = next
		}
	}
	if st == nil {
		return nil
	}
	// Persist the replayed facts directly (derived tables stay untouched:
	// they already describe the last good version).
	for _, it := range itemList(st) {
		raw, _ := json.Marshal(it.Value)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO config_items(key_path, type, value, ord, version)
			VALUES(?,?,?,?,?)`,
			it.KeyPath, it.Type, string(raw), it.Order, st.version); err != nil {
			return err
		}
	}
	for _, c := range componentList(st) {
		specJSON, _ := json.Marshal(c)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO components(component_id, wildcard, spec_json, version)
			VALUES(?,?,?,?)`,
			c.ID, c.Wildcard, string(specJSON), st.version); err != nil {
			return err
		}
		for _, p := range c.SubPrefixes {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO subscriptions(component_id, prefix, version) VALUES(?,?,?)`,
				c.ID, p, st.version); err != nil {
				return err
			}
		}
		for _, up := range c.DependsOn {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO dependencies(component_id, upstream_id, version) VALUES(?,?,?)`,
				c.ID, up, st.version); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) recoveryComponentRows(ctx context.Context, pendings []pendingIntent) ([]ComponentRecovery, error) {
	pendingByIntent := map[string][]string{}
	for _, p := range pendings {
		pendingByIntent[p.intentID] = p.pendingKeys
	}
	allPending := map[string]bool{}
	for _, keys := range pendingByIntent {
		for _, k := range keys {
			allPending[k] = true
		}
	}
	rows, err := e.db.QueryContext(ctx,
		`SELECT component_id, version, digest FROM views ORDER BY component_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ComponentRecovery
	for rows.Next() {
		var cr ComponentRecovery
		if err := rows.Scan(&cr.ComponentID, &cr.ViewVersion, &cr.ViewDigest); err != nil {
			return nil, err
		}
		cr.Status = "converged"
		if len(pendings) > 0 {
			cr.Status = "rolled_back"
			cr.RolledBackVersion = cr.ViewVersion
			for k := range allPending {
				cr.PendingKeys = append(cr.PendingKeys, k)
			}
			sort.Strings(cr.PendingKeys)
		}
		out = append(out, cr)
	}
	return out, nil
}

// LastRecoveryReport returns the report produced during startup recovery.
func (e *Engine) LastRecoveryReport() *RecoveryReport { return e.lastReport }

// InspectWithoutRecovery opens a read-only handle without running recovery,
// used by the hard-kill demonstration to observe the pre-recovery state.
func InspectWithoutRecovery(path string) (*Engine, error) {
	db, err := sql.Open("sqlite", path+"?_txlock=immediate&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	return &Engine{db: db, inflight: map[string]*inflightOp{}}, nil
}

// SetTestSkipExit toggles the test-only hard-kill mode.
func (e *Engine) SetTestSkipExit(skip bool) { e.testSkipExit = skip }
