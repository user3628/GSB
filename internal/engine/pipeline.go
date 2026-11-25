package engine

import (
	"fmt"
	"sort"
	"strings"

	"alarmengine/internal/model"
)

// Window semantics (explicit, tested contract):
//
//   - All timestamps are integer milliseconds. The truncation rule is: inputs
//     are already whole-millisecond integers and no sub-ms truncation is
//     applied, so comparisons use raw integer timestamps.
//   - A dedup group opened at W with window D covers the INCLUSIVE interval
//     [W, W+D]. A candidate at t <= W+D merges into the open group. The first
//     candidate with t > W+D opens a NEW group; window-out candidates are
//     never merged into an older entry.
//   - Suppression release time is W+D+1: the first millisecond no longer
//     covered by the inclusive window.

type rawCandidate struct {
	obs         model.Observation
	seq         int64
	fingerprint string
	fpParts     []string
}

func candidateFromObservation(o model.Observation, seq int64, policy model.PolicySnapshot) (rawCandidate, bool, error) {
	threshold, ok := policy.Thresholds[o.Metric]
	if !ok || o.Value < threshold {
		return rawCandidate{}, false, nil
	}
	fp, parts, err := model.Fingerprint(o.ServiceID, o.Metric, o.Labels, policy.FingerprintFields)
	if err != nil {
		return rawCandidate{}, false, err
	}
	return rawCandidate{obs: o, seq: seq, fingerprint: fp, fpParts: parts}, true, nil
}

// dedupWindow returns the effective inclusive dedup window for a service:
// min(policy suppression window, service silence window). Both are
// milliseconds; taking the smaller guarantees neither declared boundary is
// crossed silently.
func dedupWindow(policy model.PolicySnapshot, svc model.Service) int64 {
	w := policy.SuppressionWindowMS
	if svc.SilenceWindow > 0 && svc.SilenceWindow < w {
		w = svc.SilenceWindow
	}
	return w
}

type fpGroups struct {
	open   *model.Alert
	window int64
	all    []*model.Alert
}

func (e *Engine) buildAlerts(candidates []rawCandidate) ([]model.Alert, []model.SuppressionHit) {
	e.mu.RLock()
	policy := clonePolicy(e.policy)
	topo := e.topology
	services := cloneServices(e.services)
	e.mu.RUnlock()

	groups := map[string]*fpGroups{}
	var fpOrder []string

	for _, c := range candidates {
		svc := services[c.obs.ServiceID]
		window := dedupWindow(policy, svc)
		g := groups[c.fingerprint]
		if g != nil {
			end, _ := safeAdd(g.open.WindowStartMS, g.window)
			if c.obs.TimestampMS <= end {
				// Inside the inclusive window of the open group: merge.
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
			ID:                 alertID(c.fingerprint, c.obs.TimestampMS, c.seq),
			Version:            policy.Version,
			Fingerprint:        c.fingerprint,
			ServiceID:          c.obs.ServiceID,
			Zone:               svc.Zone,
			Metric:             c.obs.Metric,
			Labels:             cloneLabels(c.obs.Labels),
			FirstSeenMS:        c.obs.TimestampMS,
			LastSeenMS:         c.obs.TimestampMS,
			WindowStartMS:      c.obs.TimestampMS,
			WindowEndMS:        end,
			State:              model.StateFiring,
			MergedCount:        1,
			SuppressedBy:       []string{},
			SourceObservations: []int64{c.seq},
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
		matched := matchSuppression(a, policy, topo)
		hits = append(hits, matched...)
		if len(matched) > 0 {
			sort.SliceStable(matched, func(x, y int) bool {
				return matched[x].OrderIndex < matched[y].OrderIndex
			})
			primary := matched[0]
			a.State = model.StateSuppressed
			a.PrimaryReason = primary.RuleID
			for _, h := range matched {
				a.SuppressedBy = append(a.SuppressedBy, h.RuleID)
			}
			a.ReleaseAtMS = primary.ReleaseAtMS
		}
	}
	sort.Slice(alerts, func(i, j int) bool {
		if alerts[i].FirstSeenMS != alerts[j].FirstSeenMS {
			return alerts[i].FirstSeenMS < alerts[j].FirstSeenMS
		}
		return alerts[i].ID < alerts[j].ID
	})
	sort.SliceStable(hits, func(i, j int) bool {
		if hits[i].AlertID != hits[j].AlertID {
			return hits[i].AlertID < hits[j].AlertID
		}
		return hits[i].OrderIndex < hits[j].OrderIndex
	})
	return alerts, hits
}

// matchSuppression evaluates every declared rule in declaration order,
// retaining ALL hits; the primary reason is the lowest OrderIndex hit.
func matchSuppression(a *model.Alert, policy model.PolicySnapshot, topo *model.Topology) []model.SuppressionHit {
	dist := topo.HopsFrom(a.ServiceID)
	var out []model.SuppressionHit
	for idx, rule := range policy.SuppressionRules {
		if !ruleMatches(a, rule, topo, dist) {
			continue
		}
		out = append(out, model.SuppressionHit{
			AlertID:     a.ID,
			RuleID:      rule.ID,
			Service:     rule.Service,
			Zone:        rule.Zone,
			Hop:         rule.Hop,
			OrderIndex:  idx,
			MatchedAtMS: a.LastSeenMS,
			ReleaseAtMS: a.WindowEndMS + 1,
		})
	}
	return out
}

// ruleMatches applies the (service, zone, hop) conjunction. Empty service or
// zone is a wildcard. Hop is the minimum dependency distance from the alert's
// service to a service satisfying the service/zone constraint.
func ruleMatches(a *model.Alert, rule model.SuppressionRule, topo *model.Topology, dist map[string]int) bool {
	if rule.Service == "" && rule.Zone == "" {
		return false
	}
	if rule.Service != "" {
		h, ok := dist[rule.Service]
		if !ok || h != rule.Hop {
			return false
		}
	}
	if rule.Zone != "" {
		if !zoneReachableAtHop(rule, topo, dist) {
			return false
		}
	}
	return true
}

func zoneReachableAtHop(rule model.SuppressionRule, topo *model.Topology, dist map[string]int) bool {
	// Deterministic scan; only existence matters.
	ids := make([]string, 0, len(dist))
	for svc := range dist {
		ids = append(ids, svc)
	}
	sort.Strings(ids)
	for _, svc := range ids {
		if dist[svc] == rule.Hop && topo.Zone(svc) == rule.Zone {
			return true
		}
	}
	return false
}

func alertID(fingerprint string, ts, seq int64) string {
	return fmt.Sprintf("alert-%d-%d-%s", ts, seq, fingerprint[:12])
}

// versionedAlertID qualifies an alert id with its policy/data version so the
// same rebuilt alert can coexist across snapshot versions.
func versionedAlertID(base string, version int64) string {
	return fmt.Sprintf("v%d-%s", version, base)
}

func safeAdd(a, b int64) (int64, error) {
	return model.CheckOverflowAdd(a, b)
}

func cloneLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func clonePolicy(p model.PolicySnapshot) model.PolicySnapshot {
	cp := p
	cp.Thresholds = make(map[string]int64, len(p.Thresholds))
	for k, v := range p.Thresholds {
		cp.Thresholds[k] = v
	}
	cp.FingerprintFields = append([]string(nil), p.FingerprintFields...)
	cp.SuppressionRules = append([]model.SuppressionRule(nil), p.SuppressionRules...)
	return cp
}

func cloneServices(in map[string]model.Service) map[string]model.Service {
	out := make(map[string]model.Service, len(in))
	for k, v := range in {
		v.Upstreams = append([]string(nil), v.Upstreams...)
		out[k] = v
	}
	return out
}

func summarizeSuppressed(alerts []model.Alert) []string {
	var out []string
	for _, a := range alerts {
		if a.State == model.StateSuppressed {
			out = append(out, strings.Join(a.SuppressedBy, "+")+"->"+a.ID)
		}
	}
	sort.Strings(out)
	return out
}
