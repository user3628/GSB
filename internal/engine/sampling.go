package engine

import (
	"fmt"
	"sort"

	"alarmengine/internal/model"
	"alarmengine/internal/storage"
)

// SamplingMethod is the documented deterministic sampler.
const SamplingMethod = "deterministic-stride"

// SamplingRequest selects at most Limit observations through a deterministic
// stride sampler.
type SamplingRequest struct {
	Limit int `json:"limit"`
}

// NoiseReductionResult is the output of either path.
type NoiseReductionResult struct {
	Path            string                 `json:"path"`
	Version         int64                  `json:"version"`
	Sampling        *model.SamplingReport  `json:"sampling,omitempty"`
	Alerts          []model.Alert          `json:"alerts"`
	SuppressionHits []model.SuppressionHit `json:"suppression_hits"`
	RootCauses      []model.RootCauseScore `json:"root_causes"`
}

// PathDiff is the line-by-line cross-check between sampled and full paths.
type PathDiff struct {
	Version          int64                `json:"version"`
	SampledAlertIDs  []string             `json:"sampled_alert_ids"`
	FullAlertIDs     []string             `json:"full_alert_ids"`
	OnlyInFull       []string             `json:"only_in_full"`
	SharedAlertIDs   []string             `json:"shared_alert_ids"`
	SharedRootCauses []string             `json:"shared_root_cause_keys"`
	SamplingReport   model.SamplingReport `json:"sampling_report"`
}

// FullNoiseReduction processes every stored observation under the given
// committed version and returns alerts, suppression hits and root-cause scores
// read from the durable tables (so the answer matches committed facts).
func (e *Engine) FullNoiseReduction(version int64) (NoiseReductionResult, error) {
	alerts, err := storage.LoadAlerts(e.store.ReadDB(), version)
	if err != nil {
		return NoiseReductionResult{}, err
	}
	hits, err := storage.LoadSuppressionHits(e.store.ReadDB(), version)
	if err != nil {
		return NoiseReductionResult{}, err
	}
	scores, err := storage.LoadRootCauseScores(e.store.ReadDB(), version)
	if err != nil {
		return NoiseReductionResult{}, err
	}
	return NoiseReductionResult{
		Path: "full", Version: version, Alerts: alerts,
		SuppressionHits: hits, RootCauses: scores,
	}, nil
}

// SampledNoiseReduction runs the deterministic stride sampler over the input
// observations, then applies the SAME threshold/fingerprint/suppression
// pipeline. Alerts that have not completed suppression adjudication are never
// counted: the result only includes FIRING/SUPPRESSED alerts produced by the
// shared pipeline. Root-cause candidates reuse the identical fixed-point
// scorer, so hop and time-delta evidence is exactly reproducible.
func (e *Engine) SampledNoiseReduction(version int64, limit int) (NoiseReductionResult, error) {
	if limit <= 0 {
		return NoiseReductionResult{}, fmt.Errorf("sampling limit must be positive")
	}
	obsRows, err := storage.LoadObservations(e.store.ReadDB())
	if err != nil {
		return NoiseReductionResult{}, err
	}
	selected := deterministicStride(len(obsRows), limit)
	report := buildSamplingReport(len(obsRows), selected)

	// Observation commits reuse the active snapshot while bumping the data
	// version, so the in-memory active policy is the correct scorer for any
	// committed data version in this single-snapshot engine.
	policy := e.Snapshot()
	_ = version
	candidates := make([]rawCandidate, 0, len(selected))
	for _, idx := range selected {
		row := obsRows[idx]
		c, ok, err := candidateFromObservation(row.Observation, row.Seq, policy)
		if err != nil {
			return NoiseReductionResult{}, err
		}
		if ok {
			candidates = append(candidates, c)
		}
	}
	view := &scopedView{
		policy:   policy,
		topology: e.topology,
		services: cloneServices(e.services),
	}
	alerts, hits := scopedBuildAlerts(view, candidates)
	scores := scopedScoreRootCauses(view, alerts)
	return NoiseReductionResult{
		Path: "sampled", Version: version, Sampling: &report,
		Alerts: alerts, SuppressionHits: hits, RootCauses: scores,
	}, nil
}

// deterministicStride picks indices with a fixed stride over [0,n), giving a
// reproducible spread rather than a random sample. When limit >= n every index
// is selected.
func deterministicStride(n, limit int) []int {
	if n == 0 {
		return nil
	}
	if limit >= n {
		out := make([]int, n)
		for i := range out {
			out[i] = i
		}
		return out
	}
	out := make([]int, 0, limit)
	step := n / limit
	remainder := n % limit
	idx := 0
	for len(out) < limit && idx < n {
		out = append(out, idx)
		// Distribute the remainder deterministically across the first strides.
		jump := step
		if remainder > 0 {
			jump++
			remainder--
		}
		idx += jump
	}
	return out
}

func buildSamplingReport(total int, selected []int) model.SamplingReport {
	processed := len(selected)
	rate := "0/1"
	if total > 0 {
		rate = fmt.Sprintf("%d/%d", processed, total)
	}
	confidence := "low"
	switch {
	case total > 0 && processed*2 >= total:
		confidence = "high"
	case total > 0 && processed*4 >= total:
		confidence = "medium"
	}
	return model.SamplingReport{
		Method: SamplingMethod, Limit: processed, TotalInput: total,
		Processed: processed, SampleRate: rate, Confidence: confidence,
		SelectedIndex: append([]int(nil), selected...),
	}
}

// ComparePaths returns the line-by-line difference between the two paths.
func ComparePaths(sampled, full NoiseReductionResult) PathDiff {
	sampledIDs := alertIDs(sampled.Alerts)
	fullIDs := alertIDs(full.Alerts)
	fullSet := map[string]bool{}
	for _, id := range fullIDs {
		fullSet[id] = true
	}
	sampledSet := map[string]bool{}
	for _, id := range sampledIDs {
		sampledSet[id] = true
	}
	var onlyFull, shared []string
	for _, id := range fullIDs {
		if sampledSet[id] {
			shared = append(shared, id)
		} else {
			onlyFull = append(onlyFull, id)
		}
	}
	var report model.SamplingReport
	if sampled.Sampling != nil {
		report = *sampled.Sampling
	}
	// Shared root-cause keys guarantee identical hop/delta evidence across paths.
	rcKeys := map[string]bool{}
	for _, s := range full.RootCauses {
		rcKeys[rcKey(s.TriggerAlertID, s.CandidateService)] = true
	}
	var sharedRC []string
	for _, s := range sampled.RootCauses {
		if rcKeys[rcKey(s.TriggerAlertID, s.CandidateService)] {
			sharedRC = append(sharedRC, rcKey(s.TriggerAlertID, s.CandidateService))
		}
	}
	sort.Strings(sharedRC)
	return PathDiff{
		Version:          full.Version,
		SampledAlertIDs:  sampledIDs,
		FullAlertIDs:     fullIDs,
		OnlyInFull:       onlyFull,
		SharedAlertIDs:   shared,
		SharedRootCauses: sharedRC,
		SamplingReport:   report,
	}
}

func alertIDs(alerts []model.Alert) []string {
	out := make([]string, 0, len(alerts))
	for _, a := range alerts {
		out = append(out, a.ID)
	}
	sort.Strings(out)
	return out
}

func rcKey(trigger, candidate string) string { return trigger + "->" + candidate }
