package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"auditdrift/internal/drift"
	"auditdrift/internal/model"
)

type InitializeInput struct {
	SessionID          string
	Hosts              []string
	Baselines          []model.Baseline
	Observations       []model.Observation
	PropagationMaxHops int
	RepairQuota        int
}

type InitializeResult struct {
	IntentID  string              `json:"intent_id"`
	Session   *Session            `json:"session"`
	Summaries []model.HostSummary `json:"summaries"`
}

func (s *Store) InitializeSession(in InitializeInput) (*InitializeResult, error) {
	if in.SessionID == "" {
		in.SessionID = "default"
	}
	if in.PropagationMaxHops <= 0 {
		in.PropagationMaxHops = 3
	}
	if err := model.ValidateBaselines(in.Baselines); err != nil {
		return nil, err
	}
	if len(in.Hosts) == 0 {
		return nil, model.NewCodedError(model.ErrBaselineInvalid, "at least one host is required")
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	existing, err := s.GetSession(in.SessionID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, fmt.Errorf("session %s already exists at version %d", in.SessionID, existing.CurrentVersion)
	}

	intentID, err := s.CreateIntent(in.SessionID, "session_initialize", 1, map[string]any{
		"baseline_count": len(in.Baselines), "observation_count": len(in.Observations), "hosts": in.Hosts,
	})
	if err != nil {
		return nil, err
	}

	tx, err := s.writer.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UnixMilli()
	_, err = tx.Exec(`INSERT INTO sessions(session_id,status,current_version,last_complete_version,view_version,propagation_max_hops,repair_quota_total,repair_quota_remaining,created_at_ms,updated_at_ms)
		VALUES(?, 'ACTIVE', 1, 1, 1, ?, ?, ?, ?, ?)`,
		in.SessionID, in.PropagationMaxHops, in.RepairQuota, in.RepairQuota, now, now)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(`INSERT INTO snapshots(session_id,version,base_version,reason,intent_id,created_at_ms) VALUES(?,1,0,'initialize',?,?)`, in.SessionID, intentID, now)
	if err != nil {
		return nil, err
	}
	for _, baseline := range in.Baselines {
		if err := insertBaselineTx(tx, in.SessionID, 1, baseline); err != nil {
			return nil, err
		}
	}
	if err := insertObservationsTx(tx, in.SessionID, 1, in.Observations); err != nil {
		return nil, err
	}
	for _, host := range in.Hosts {
		for _, baseline := range in.Baselines {
			key := taskKey(in.SessionID, host, baseline.ID)
			_, err := tx.Exec(`INSERT INTO audit_tasks(session_id,host,baseline_id,task_key,severity_weight,repair_cost,status,priority_order,updated_at_ms)
				VALUES(?,?,?,?,?,?,'PENDING',0,?)`,
				in.SessionID, host, baseline.ID, key, baseline.SeverityWeight, baseline.SeverityWeight, now)
			if err != nil {
				return nil, err
			}
		}
	}
	summaries, err := rebuildConclusionsTx(tx, in.SessionID, 1, in.PropagationMaxHops, in.RepairQuota)
	if err != nil {
		return nil, err
	}
	if err := markIntentCommitted(tx, intentID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	session, err := s.GetSession(in.SessionID)
	if err != nil {
		return nil, err
	}
	return &InitializeResult{IntentID: intentID, Session: session, Summaries: summaries}, nil
}

func insertBaselineTx(tx *sql.Tx, sessionID string, version int64, b model.Baseline) error {
	expectedJSON, err := json.Marshal(b.Expected)
	if err != nil {
		return err
	}
	depsJSON, err := json.Marshal(b.DependsOn)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO baselines(session_id,snapshot_version,id,category,expected_json,severity_weight,depends_on_json)
		VALUES(?,?,?,?,?,?,?)`, sessionID, version, b.ID, b.Category, string(expectedJSON), b.SeverityWeight, string(depsJSON))
	return err
}

func insertObservationsTx(tx *sql.Tx, sessionID string, version int64, observations []model.Observation) error {
	for _, obs := range observations {
		valueJSON, err := json.Marshal(obs.Actual)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT INTO observations(session_id,host,baseline_id,value_json,sampled_at_ms,source,committed_version)
			VALUES(?,?,?,?,?,?,?)`, sessionID, obs.Host, obs.BaselineID, string(valueJSON), obs.SampledAtMS, obs.Source, version)
		if err != nil {
			return err
		}
	}
	return nil
}

func loadBaselinesTx(tx *sql.Tx, sessionID string, version int64) ([]model.Baseline, map[string]model.Baseline, error) {
	rows, err := tx.Query(`SELECT id,category,expected_json,severity_weight,depends_on_json FROM baselines WHERE session_id=? AND snapshot_version=? ORDER BY id`, sessionID, version)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	items := make([]model.Baseline, 0)
	byID := make(map[string]model.Baseline)
	for rows.Next() {
		var b model.Baseline
		var category string
		var expectedJSON, depsJSON string
		if err := rows.Scan(&b.ID, &category, &expectedJSON, &b.SeverityWeight, &depsJSON); err != nil {
			return nil, nil, err
		}
		b.Category = model.Category(category)
		if err := json.Unmarshal([]byte(expectedJSON), &b.Expected); err != nil {
			return nil, nil, err
		}
		if err := json.Unmarshal([]byte(depsJSON), &b.DependsOn); err != nil {
			return nil, nil, err
		}
		items = append(items, b)
		byID[b.ID] = b
	}
	return items, byID, rows.Err()
}

func loadObservationsTx(tx *sql.Tx, sessionID string) (map[drift.ConclusionKey][]model.Observation, error) {
	rows, err := tx.Query(`SELECT host,baseline_id,value_json,sampled_at_ms,source FROM observations WHERE session_id=? ORDER BY id`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[drift.ConclusionKey][]model.Observation)
	for rows.Next() {
		var obs model.Observation
		var valueJSON string
		if err := rows.Scan(&obs.Host, &obs.BaselineID, &valueJSON, &obs.SampledAtMS, &obs.Source); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(valueJSON), &obs.Actual); err != nil {
			return nil, err
		}
		obs.Actual = model.CanonicalValue(obs.Actual)
		key := drift.ConclusionKey{Host: obs.Host, BaselineID: obs.BaselineID}
		out[key] = append(out[key], obs)
	}
	return out, rows.Err()
}

type taskState struct {
	Status model.TaskStatus
	Order  int
}

func loadTaskStatesTx(tx *sql.Tx, sessionID string) (map[drift.ConclusionKey]taskState, []string, error) {
	rows, err := tx.Query(`SELECT host,baseline_id,status,priority_order FROM audit_tasks WHERE session_id=? ORDER BY priority_order,host,baseline_id`, sessionID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	out := make(map[drift.ConclusionKey]taskState)
	hostSet := make(map[string]struct{})
	hosts := make([]string, 0)
	for rows.Next() {
		var host, baselineID, status string
		var order int
		if err := rows.Scan(&host, &baselineID, &status, &order); err != nil {
			return nil, nil, err
		}
		key := drift.ConclusionKey{Host: host, BaselineID: baselineID}
		out[key] = taskState{Status: model.TaskStatus(status), Order: order}
		if _, ok := hostSet[host]; !ok {
			hostSet[host] = struct{}{}
			hosts = append(hosts, host)
		}
	}
	sort.Strings(hosts)
	return out, hosts, rows.Err()
}

func rebuildConclusionsTx(tx *sql.Tx, sessionID string, version int64, maxHops int, remainingQuota int) ([]model.HostSummary, error) {
	baselines, _, err := loadBaselinesTx(tx, sessionID, version)
	if err != nil {
		return nil, err
	}
	observations, err := loadObservationsTx(tx, sessionID)
	if err != nil {
		return nil, err
	}
	taskStates, hosts, err := loadTaskStatesTx(tx, sessionID)
	if err != nil {
		return nil, err
	}

	conclusionMap := make(map[drift.ConclusionKey]*model.Conclusion)
	conclusions := make([]model.Conclusion, 0, len(taskStates))
	keys := make([]drift.ConclusionKey, 0, len(taskStates))
	for key := range taskStates {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Host != keys[j].Host {
			return keys[i].Host < keys[j].Host
		}
		return keys[i].BaselineID < keys[j].BaselineID
	})

	baselineByID := make(map[string]model.Baseline, len(baselines))
	for _, b := range baselines {
		baselineByID[b.ID] = b
	}
	for _, key := range keys {
		b, ok := baselineByID[key.BaselineID]
		if !ok {
			continue
		}
		conclusion, err := drift.EvaluateTarget(sessionID, key.Host, b, observations[key], nil, version)
		if err != nil {
			return nil, err
		}
		if taskStates[key].Status == model.TaskDeferred {
			conclusion.Status = model.TaskDeferred
			conclusion.Reason = "repair_quota_or_budget_deferred"
		}
		conclusionMap[key] = &conclusion
		conclusions = append(conclusions, conclusion)

		if taskStates[key].Status != model.TaskDeferred {
			_, err = tx.Exec(`UPDATE audit_tasks SET status='DONE', error_code='', reason='', updated_at_ms=? WHERE session_id=? AND host=? AND baseline_id=?`,
				time.Now().UnixMilli(), sessionID, key.Host, key.BaselineID)
			if err != nil {
				return nil, err
			}
		}
	}

	drift.ApplyPropagation(baselines, conclusionMap, maxHops)

	if _, err := tx.Exec(`DELETE FROM conclusions WHERE session_id=?`, sessionID); err != nil {
		return nil, err
	}
	for _, key := range keys {
		conclusion := conclusionMap[key]
		if conclusion == nil {
			continue
		}
		if err := insertConclusionTx(tx, conclusion); err != nil {
			return nil, err
		}
	}

	summaries := make([]model.HostSummary, 0, len(hosts))
	for _, host := range hosts {
		summary := model.HostSummary{SessionID: sessionID, Host: host, ViewVersion: version, RemainingQuota: remainingQuota}
		for _, key := range keys {
			if key.Host != host {
				continue
			}
			c := conclusionMap[key]
			if c == nil {
				continue
			}
			summary.Total++
			switch c.Status {
			case model.TaskDeferred:
				summary.Deferred++
			case model.TaskError:
				summary.Errors++
			default:
				summary.Done++
				if c.DriftLevel == 0 {
					summary.Conformant++
				} else {
					summary.Drift++
				}
			}
			if c.DriftLevel > summary.MaxDriftLevel {
				summary.MaxDriftLevel = c.DriftLevel
			}
			summary.WeightedScore += c.ScoreScaled
		}
		if err := upsertSummaryTx(tx, &summary); err != nil {
			return nil, err
		}
		summaries = append(summaries, summary)
	}
	return summaries, nil
}

func insertConclusionTx(tx *sql.Tx, c *model.Conclusion) error {
	roundingJSON, _ := json.Marshal(c.LevelRounding)
	missingJSON, _ := json.Marshal(c.MissingElements)
	extraJSON, _ := json.Marshal(c.ExtraElements)
	propagationJSON, _ := json.Marshal(c.Propagation)
	detailsJSON, _ := json.Marshal(c.Details)
	now := time.Now().UnixMilli()
	_, err := tx.Exec(`INSERT INTO conclusions(session_id,host,baseline_id,category,status,drift_category,own_level,drift_level,score_scaled,offset_permille,rounding_json,missing_json,extra_json,recommendation,propagation_json,repair_cost,reason,error_code,details_json,view_version,updated_at_ms)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		c.SessionID, c.Host, c.BaselineID, c.Category, c.Status, c.DriftCategory, c.OwnLevel, c.DriftLevel, c.ScoreScaled, c.OffsetPermille,
		string(roundingJSON), string(missingJSON), string(extraJSON), c.Recommendation, string(propagationJSON), c.RepairCost, c.Reason, c.ErrorCode, string(detailsJSON), c.ViewVersion, now)
	return err
}

func upsertSummaryTx(tx *sql.Tx, summary *model.HostSummary) error {
	summaryJSON, _ := json.Marshal(summary)
	now := time.Now().UnixMilli()
	_, err := tx.Exec(`INSERT INTO host_summaries(session_id,host,view_version,total,done,conformant,drift,deferred,errors,max_drift_level,weighted_score_scaled,remaining_quota,summary_json,updated_at_ms)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(session_id,host) DO UPDATE SET
			view_version=excluded.view_version,total=excluded.total,done=excluded.done,conformant=excluded.conformant,
			drift=excluded.drift,deferred=excluded.deferred,errors=excluded.errors,max_drift_level=excluded.max_drift_level,
			weighted_score_scaled=excluded.weighted_score_scaled,remaining_quota=excluded.remaining_quota,
			summary_json=excluded.summary_json,updated_at_ms=excluded.updated_at_ms`,
		summary.SessionID, summary.Host, summary.ViewVersion, summary.Total, summary.Done, summary.Conformant, summary.Drift,
		summary.Deferred, summary.Errors, summary.MaxDriftLevel, summary.WeightedScore, summary.RemainingQuota, string(summaryJSON), now)
	return err
}

func taskKey(sessionID, host, baselineID string) string {
	return sessionID + "\x00" + host + "\x00" + baselineID
}
