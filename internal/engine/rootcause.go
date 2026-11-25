package engine

import (
	"sort"

	"alarmengine/internal/model"
)

// scoreRootCauses produces, for every FIRING trigger alert, the ranked
// root-cause candidate list. Candidates are services reachable through the
// upstream dependency topology. All arithmetic is fixed-point and every score
// carries its rounding audit trail.
func (e *Engine) scoreRootCauses(alerts []model.Alert) []model.RootCauseScore {
	e.mu.RLock()
	policy := clonePolicy(e.policy)
	topo := e.topology
	e.mu.RUnlock()
	return scoreRootCausesView(policy, topo, alerts)
}

func scoreRootCausesView(policy model.PolicySnapshot, topo *model.Topology, alerts []model.Alert) []model.RootCauseScore {
	var out []model.RootCauseScore
	for _, trigger := range alerts {
		if trigger.State != model.StateFiring {
			continue
		}
		dist := topo.HopsFrom(trigger.ServiceID)
		type scored struct {
			rc model.RootCauseScore
		}
		var ranked []scored
		for cand, hops := range dist {
			if hops == 0 {
				continue // the trigger service itself is not a root candidate
			}
			// Time delta: difference between the trigger's first-seen time and
			// the most recent alert of the candidate service before it. When no
			// candidate alert exists, fall back to topology-only proximity using
			// the trigger timestamp and a documented delta of the hop window.
			delta := candidateTimeDelta(trigger, cand, alerts)
			res, err := model.ComputeScore(hops, delta)
			if err != nil {
				continue
			}
			ranked = append(ranked, scored{rc: model.RootCauseScore{
				TriggerAlertID:   trigger.ID,
				CandidateService: cand,
				Hops:             hops,
				TimeDeltaMS:      delta,
				ScoreRawMilli: res.HopDecayMilli*model.WeightHopMilli/1000 +
					res.TimeProximityMilli*model.WeightTimeMilli/1000,
				ScoreFinalMilli:   res.FinalMilli,
				RoundingErrorNano: res.TotalErrorNano,
				Version:           policy.Version,
			}})
		}
		// Deterministic ordering: score desc with relative-tolerance tie
		// equivalence; ties inside tolerance break by candidate service id
		// lexicographically, then by hops.
		sort.SliceStable(ranked, func(i, j int) bool {
			a, b := ranked[i].rc, ranked[j].rc
			if !model.EquivalentWithTolerance(a.ScoreFinalMilli, b.ScoreFinalMilli, policy.RootCauseTolerance) {
				return a.ScoreFinalMilli > b.ScoreFinalMilli
			}
			if a.CandidateService != b.CandidateService {
				return a.CandidateService < b.CandidateService
			}
			return a.Hops < b.Hops
		})
		for i := range ranked {
			ranked[i].rc.Rank = i + 1
			out = append(out, ranked[i].rc)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].TriggerAlertID != out[j].TriggerAlertID {
			return out[i].TriggerAlertID < out[j].TriggerAlertID
		}
		return out[i].Rank < out[j].Rank
	})
	return out
}

// candidateTimeDelta returns the absolute millisecond delta between the
// trigger alert and the latest alert of the candidate service. It is always a
// non-negative integer; no floating point is used.
func candidateTimeDelta(trigger model.Alert, candidate string, alerts []model.Alert) int64 {
	var best int64 = -1
	for _, a := range alerts {
		if a.ServiceID != candidate {
			continue
		}
		if a.FirstSeenMS <= trigger.FirstSeenMS && a.FirstSeenMS > best {
			best = a.FirstSeenMS
		}
	}
	if best < 0 {
		// No earlier candidate alert: use hop window as documented fallback so
		// the delta remains an explicit integer fact.
		return model.TimeWindowMS
	}
	d := trigger.FirstSeenMS - best
	if d < 0 {
		return -d
	}
	return d
}
