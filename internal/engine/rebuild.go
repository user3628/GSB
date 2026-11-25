package engine

import (
	"database/sql"
	"fmt"

	"alarmengine/internal/model"
	"alarmengine/internal/storage"
)

// rebuildDerivedInTx performs the cascade invalidation + atomic rebuild inside
// one transaction. It recomputes every derived fact from durable observations
// and the target policy version, so readers either see the previous complete
// version or the new complete version, never an interleaved one.
func (e *Engine) rebuildDerivedInTx(tx *sql.Tx, version int64) error {
	e.mu.RLock()
	policy := clonePolicy(e.policy)
	e.mu.RUnlock()
	return e.rebuildWithPolicy(tx, version, policy)
}

// rebuildWithPolicy recomputes all derived facts for version using an explicit
// policy, so observation commits reuse the active snapshot and snapshot
// replacements use the incoming snapshot inside the same transaction.
func (e *Engine) rebuildWithPolicy(tx *sql.Tx, version int64, policy model.PolicySnapshot) error {
	svcRows, err := storage.LoadServices(tx)
	if err != nil {
		return err
	}
	services := make([]model.Service, 0, len(svcRows))
	svcMap := map[string]model.Service{}
	for _, r := range svcRows {
		services = append(services, r.Service)
		svcMap[r.Service.ID] = r.Service
	}
	topo := model.NewTopology(services)

	obsRows, err := storage.LoadObservations(tx)
	if err != nil {
		return err
	}

	// Cascade: drop every derived row for this version first. Alerts,
	// suppression hits, root-cause scores and refs share the transaction, so
	// the invalidation and replacement are atomic.
	if err := storage.DeleteDerivedForVersion(tx, version); err != nil {
		return err
	}

	candidates := make([]rawCandidate, 0, len(obsRows))
	for _, row := range obsRows {
		c, ok, err := candidateFromObservation(row.Observation, row.Seq, policy)
		if err != nil {
			return err
		}
		if ok {
			candidates = append(candidates, c)
		}
	}

	// Use a scoped engine view so buildAlerts/scoreRootCauses see exactly the
	// policy/topology being rebuilt, independent of in-memory cache state.
	scoped := &scopedView{policy: policy, topology: topo, services: svcMap}
	alerts, hits := scopedBuildAlerts(scoped, candidates)
	scores := scopedScoreRootCauses(scoped, alerts)

	batchID := fmt.Sprintf("rebuild-%d", version)
	for i := range alerts {
		alerts[i].Version = version
		if err := storage.InsertAlert(tx, alerts[i], batchID); err != nil {
			return err
		}
		// Derived reference registry: service+fingerprint -> alert/suppression/rcs.
		if err := storage.InsertDerivedRef(tx, version, alerts[i].ServiceID,
			alerts[i].Fingerprint, "candidate_alert", alerts[i].ID); err != nil {
			return err
		}
	}
	for _, h := range hits {
		if err := storage.InsertSuppressionHit(tx, version, h); err != nil {
			return err
		}
		if err := storage.InsertDerivedRef(tx, version, alertsByID(alerts, h.AlertID).ServiceID,
			alertsByID(alerts, h.AlertID).Fingerprint, "suppression_index", h.RuleID); err != nil {
			return err
		}
	}
	for i := range scores {
		scores[i].Version = version
	}
	for _, s := range scores {
		if err := storage.InsertRootCauseScore(tx, s); err != nil {
			return err
		}
		if err := storage.InsertDerivedRef(tx, version, s.CandidateService,
			"", "root_cause_score", s.TriggerAlertID+":"+s.CandidateService); err != nil {
			return err
		}
	}
	e.maybeCrash(CrashMidRebuild, fmt.Sprintf("rebuild-%d", version))
	return nil
}

func alertsByID(alerts []model.Alert, id string) model.Alert {
	for _, a := range alerts {
		if a.ID == id {
			return a
		}
	}
	return model.Alert{}
}
