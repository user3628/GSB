package engine

import (
	"encoding/json"

	"alarmengine/internal/model"
)

// scopedView is an immutable engine-like view used by atomic rebuilds so the
// rebuild is computed from the exact snapshot/topology passed in rather than
// the live in-memory cache.
type scopedView struct {
	policy   model.PolicySnapshot
	topology *model.Topology
	services map[string]model.Service
}

func scopedBuildAlerts(v *scopedView, candidates []rawCandidate) ([]model.Alert, []model.SuppressionHit) {
	type fpGroups struct {
		open   *model.Alert
		window int64
		all    []*model.Alert
	}
	groups := map[string]*fpGroups{}
	var fpOrder []string
	policy := v.policy
	for _, c := range candidates {
		svc := v.services[c.obs.ServiceID]
		window := dedupWindow(policy, svc)
		g := groups[c.fingerprint]
		if g != nil {
			end, _ := safeAdd(g.open.WindowStartMS, g.window)
			if c.obs.TimestampMS <= end {
				g.open.LastSeenMS = c.obs.TimestampMS
				g.open.WindowEndMS = end
				g.open.MergedCount++
				g.open.SourceObservations = append(g.open.SourceObservations, c.seq)
				g.open.LastObservationSeq = c.seq
				continue
			}
		}
		end, _ := safeAdd(c.obs.TimestampMS, window)
		a := &model.Alert{
			ID: versionedAlertID(alertID(c.fingerprint, c.obs.TimestampMS, c.seq), policy.Version), Version: policy.Version,
			Fingerprint: c.fingerprint, ServiceID: c.obs.ServiceID, Zone: svc.Zone,
			Metric: c.obs.Metric, Labels: cloneLabels(c.obs.Labels),
			FirstSeenMS: c.obs.TimestampMS, LastSeenMS: c.obs.TimestampMS,
			WindowStartMS: c.obs.TimestampMS, WindowEndMS: end, State: model.StateFiring,
			MergedCount: 1, SuppressedBy: []string{}, SourceObservations: []int64{c.seq},
			LastObservationSeq: c.seq,
		}
		if g == nil {
			g = &fpGroups{}
			groups[c.fingerprint] = g
			fpOrder = append(fpOrder, c.fingerprint)
		}
		g.open = a
		g.window = window
		g.all = append(g.all, a)
	}
	var alerts []model.Alert
	for _, fp := range fpOrder {
		for _, a := range groups[fp].all {
			alerts = append(alerts, *a)
		}
	}
	var hits []model.SuppressionHit
	for i := range alerts {
		a := &alerts[i]
		matched := matchSuppression(a, policy, v.topology)
		hits = append(hits, matched...)
		if len(matched) > 0 {
			a.State = model.StateSuppressed
			a.PrimaryReason = matched[0].RuleID
			for _, h := range matched {
				a.SuppressedBy = append(a.SuppressedBy, h.RuleID)
			}
			a.ReleaseAtMS = matched[0].ReleaseAtMS
		}
	}
	return alerts, hits
}

func scopedScoreRootCauses(v *scopedView, alerts []model.Alert) []model.RootCauseScore {
	return scoreRootCausesView(v.policy, v.topology, alerts)
}

func jsonUnmarshalPolicy(raw string, p *model.PolicySnapshot) error {
	return json.Unmarshal([]byte(raw), p)
}
