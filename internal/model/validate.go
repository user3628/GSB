package model

import (
	"fmt"
	"math"

	"alarmengine/internal/errcode"
)

// ValidateServices enforces: unique non-empty IDs, weight in [1,1000],
// upstream references exist, valid sensitivity and non-negative silence
// windows with overflow checks.
func ValidateServices(services []Service) error {
	seen := make(map[string]bool, len(services))
	for _, s := range services {
		if s.ID == "" {
			return errcode.New(errcode.ESampleInvalid, "empty service id")
		}
		if seen[s.ID] {
			return errcode.New(errcode.ESampleInvalid, "duplicate service id: %s", s.ID)
		}
		seen[s.ID] = true
		if s.WeightPerMille < 1 || s.WeightPerMille > 1000 {
			return errcode.New(errcode.ESampleInvalid, "service %s weight %d out of [1,1000]", s.ID, s.WeightPerMille)
		}
		if !s.Sensitivity.Valid() {
			return errcode.New(errcode.ESampleInvalid, "service %s invalid sensitivity %d", s.ID, s.Sensitivity)
		}
		if s.SilenceWindow < 0 {
			return errcode.New(errcode.ESampleInvalid, "service %s negative silence window", s.ID)
		}
		if s.SilenceWindow > math.MaxInt64/2 {
			return errcode.New(errcode.ESampleInvalid, "service %s silence window overflow", s.ID)
		}
	}
	for _, s := range services {
		for _, up := range s.Upstreams {
			if !seen[up] {
				return errcode.New(errcode.ESampleInvalid, "service %s references unknown upstream %s", s.ID, up)
			}
			if up == s.ID {
				return errcode.New(errcode.ESampleInvalid, "service %s depends on itself", s.ID)
			}
		}
	}
	if err := detectCycle(services); err != nil {
		return err
	}
	return nil
}

func detectCycle(services []Service) error {
	adj := make(map[string][]string, len(services))
	for _, s := range services {
		adj[s.ID] = s.Upstreams
	}
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(services))
	var visit func(id string) error
	visit = func(id string) error {
		color[id] = gray
		for _, up := range adj[id] {
			switch color[up] {
			case gray:
				return errcode.New(errcode.ESampleInvalid, "dependency cycle detected at %s", up)
			case white:
				if err := visit(up); err != nil {
					return err
				}
			}
		}
		color[id] = black
		return nil
	}
	for _, s := range services {
		if color[s.ID] == white {
			if err := visit(s.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateObservations enforces non-negative timestamps (overflow checked),
// known services, non-empty metric and finite int64 values.
func ValidateObservations(obs []Observation, known map[string]bool) error {
	for i, o := range obs {
		if o.TimestampMS < 0 {
			return errcode.New(errcode.ESampleInvalid, "observation %d negative timestamp", i)
		}
		if o.TimestampMS > math.MaxInt64/2 {
			return errcode.New(errcode.ESampleInvalid, "observation %d timestamp overflow", i)
		}
		if o.ServiceID == "" || !known[o.ServiceID] {
			return errcode.New(errcode.ESampleInvalid, "observation %d unknown service %q", i, o.ServiceID)
		}
		if o.Metric == "" {
			return errcode.New(errcode.ESampleInvalid, "observation %d empty metric", i)
		}
		if o.Labels == nil {
			return errcode.New(errcode.ESampleInvalid, "observation %d labels must be non-nil", i)
		}
	}
	return nil
}

// ValidatePolicySnapshot enforces non-negative integer fields with overflow
// checks, valid fingerprint fields, rules and tolerance range. Checksum
// verification is performed separately by the storage layer.
func ValidatePolicySnapshot(p PolicySnapshot) error {
	if p.Version < 0 {
		return errcode.New(errcode.ESnapshotInvalid, "negative snapshot version")
	}
	if p.SuppressionWindowMS < 0 {
		return errcode.New(errcode.ESnapshotInvalid, "negative suppression window")
	}
	if p.SuppressionWindowMS > math.MaxInt64/2 {
		return errcode.New(errcode.ESnapshotInvalid, "suppression window overflow")
	}
	if p.RoundBudgetMS < 0 {
		return errcode.New(errcode.ESnapshotInvalid, "negative round budget")
	}
	if p.ConcurrencyLimit <= 0 {
		return errcode.New(errcode.ESnapshotInvalid, "concurrency limit must be positive")
	}
	if p.RootCauseTolerance < 0 || p.RootCauseTolerance > 1000 {
		return errcode.New(errcode.ESnapshotInvalid, "root cause tolerance %d out of [0,1000]", p.RootCauseTolerance)
	}
	if len(p.FingerprintFields) == 0 {
		return errcode.New(errcode.ESnapshotInvalid, "empty fingerprint fields")
	}
	for _, f := range p.FingerprintFields {
		if !ValidFingerprintField(f) {
			return errcode.New(errcode.ESnapshotInvalid, "unknown fingerprint field %q", f)
		}
	}
	ruleIDs := make(map[string]bool, len(p.SuppressionRules))
	for i, r := range p.SuppressionRules {
		if r.ID == "" {
			return errcode.New(errcode.ESnapshotInvalid, "rule %d empty id", i)
		}
		if ruleIDs[r.ID] {
			return errcode.New(errcode.ESnapshotInvalid, "duplicate rule id %s", r.ID)
		}
		ruleIDs[r.ID] = true
		if r.Hop < 0 {
			return errcode.New(errcode.ESnapshotInvalid, "rule %s negative hop", r.ID)
		}
	}
	for metric, threshold := range p.Thresholds {
		if metric == "" {
			return errcode.New(errcode.ESnapshotInvalid, "empty metric name in thresholds")
		}
		if threshold < 0 {
			return errcode.New(errcode.ESnapshotInvalid, "negative threshold for %s", metric)
		}
		if threshold > math.MaxInt64/2 {
			return errcode.New(errcode.ESnapshotInvalid, "threshold overflow for %s", metric)
		}
	}
	return nil
}

// CheckOverflowAdd returns an error if a+b overflows int64.
func CheckOverflowAdd(a, b int64) (int64, error) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, fmt.Errorf("int64 overflow: %d+%d", a, b)
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, fmt.Errorf("int64 underflow: %d+%d", a, b)
	}
	return a + b, nil
}
