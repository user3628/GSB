package report

import (
	"bytes"
	"encoding/json"
	"sort"

	"secverify/internal/engine"
	"secverify/internal/model"
)

const Schema = "secverify.verification-report/v1"

type VerificationReport struct {
	Schema      string               `json:"schema"`
	Marker      string               `json:"marker"`
	Scenario    string               `json:"scenario"`
	Result      string               `json:"result"`
	Checks      []CheckAssertion     `json:"checks"`
	Namespaces  []NamespaceReport    `json:"namespaces"`
	Injections  []InjectionAssertion `json:"injections"`
	CrashChecks []CrashAssertion     `json:"crash_checks"`
}

type CheckAssertion struct {
	Name     string `json:"name"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	Passed   bool   `json:"passed"`
}

type NamespaceReport struct {
	Namespace      string               `json:"namespace"`
	Rounds         []engine.RoundResult `json:"rounds"`
	VersionChain   []engine.SnapshotRef `json:"version_chain"`
	FinalVersion   int64                `json:"final_version"`
	FinalStateHash string               `json:"final_state_hash"`
}

type InjectionAssertion struct {
	Scenario            string `json:"scenario"`
	InjectedBy          string `json:"injected_by"`
	ExpectedCode        string `json:"expected_code"`
	ObservedCode        string `json:"observed_code"`
	VersionBefore       int64  `json:"version_before"`
	VersionAfter        int64  `json:"version_after"`
	FallbackVersion     int64  `json:"fallback_version"`
	StateUnpolluted     bool   `json:"state_unpolluted"`
	SnapshotNotAdvanced bool   `json:"snapshot_not_advanced"`
	Passed              bool   `json:"passed"`
}

type CrashAssertion struct {
	CrashPoint           string `json:"crash_point"`
	ExpectedVersion      int64  `json:"expected_version"`
	RecoveredVersion     int64  `json:"recovered_version"`
	CommittedPreserved   bool   `json:"committed_preserved"`
	UncommittedDiscarded bool   `json:"uncommitted_discarded"`
	RestartedVersion     int64  `json:"restarted_version"`
	RestartSucceeded     bool   `json:"restart_succeeded"`
	Passed               bool   `json:"passed"`
}

func New(scenario string, checks []CheckAssertion, namespaces []NamespaceReport, injections []InjectionAssertion, crashChecks []CrashAssertion) VerificationReport {
	r := VerificationReport{
		Schema:      Schema,
		Marker:      model.SampleMarker,
		Scenario:    scenario,
		Checks:      checks,
		Namespaces:  namespaces,
		Injections:  injections,
		CrashChecks: crashChecks,
	}
	r.Normalize()
	return r
}

func (r *VerificationReport) Normalize() {
	if r.Marker == "" {
		r.Marker = model.SampleMarker
	}
	if r.Namespaces == nil {
		r.Namespaces = []NamespaceReport{}
	}
	if r.Checks == nil {
		r.Checks = []CheckAssertion{}
	}
	if r.Injections == nil {
		r.Injections = []InjectionAssertion{}
	}
	if r.CrashChecks == nil {
		r.CrashChecks = []CrashAssertion{}
	}
	sort.Slice(r.Namespaces, func(i, j int) bool { return r.Namespaces[i].Namespace < r.Namespaces[j].Namespace })
	for i := range r.Namespaces {
		ns := &r.Namespaces[i]
		if ns.Rounds == nil {
			ns.Rounds = []engine.RoundResult{}
		}
		if ns.VersionChain == nil {
			ns.VersionChain = []engine.SnapshotRef{}
		}
		for j := range ns.Rounds {
			normalizeRound(&ns.Rounds[j])
		}
		sort.Slice(ns.VersionChain, func(a, b int) bool { return ns.VersionChain[a].Version < ns.VersionChain[b].Version })
	}
	sort.Slice(r.Injections, func(i, j int) bool {
		if r.Injections[i].Scenario != r.Injections[j].Scenario {
			return r.Injections[i].Scenario < r.Injections[j].Scenario
		}
		return r.Injections[i].ExpectedCode < r.Injections[j].ExpectedCode
	})
	sort.Slice(r.Checks, func(i, j int) bool { return r.Checks[i].Name < r.Checks[j].Name })
	sort.Slice(r.CrashChecks, func(i, j int) bool { return r.CrashChecks[i].CrashPoint < r.CrashChecks[j].CrashPoint })
	r.computeResult()
}

func (r *VerificationReport) computeResult() {
	r.Result = "PASS"
	for _, injection := range r.Injections {
		if !injection.Passed {
			r.Result = "FAIL"
			return
		}
	}
	for _, check := range r.Checks {
		if !check.Passed {
			r.Result = "FAIL"
			return
		}
	}
	for _, crash := range r.CrashChecks {
		if !crash.Passed {
			r.Result = "FAIL"
			return
		}
	}
}

func normalizeRound(round *engine.RoundResult) {
	if round.Diffs == nil {
		round.Diffs = []model.Diff{}
	}
	if round.Probes == nil {
		round.Probes = []engine.ProbeResult{}
	}
	if round.Invalidation.Triggers == nil {
		round.Invalidation.Triggers = []string{}
	}
	if round.Invalidation.Items == nil {
		round.Invalidation.Items = []string{}
	}
	if round.Invalidation.Edges == nil {
		round.Invalidation.Edges = []model.Edge{}
	}
	if round.Invalidation.Traces == nil {
		round.Invalidation.Traces = []model.TraceEntry{}
	}
	for i := range round.Invalidation.Traces {
		if round.Invalidation.Traces[i].Chain == nil {
			round.Invalidation.Traces[i].Chain = []model.Edge{}
		}
	}
	for i := range round.Diffs {
		if round.Diffs[i].ChangedFields == nil {
			round.Diffs[i].ChangedFields = []string{}
		}
	}
	if round.Issue != nil {
		if round.Issue.Items == nil {
			round.Issue.Items = []string{}
		}
		if round.Issue.Edges == nil {
			round.Issue.Edges = []model.Edge{}
		}
	}
}

func Marshal(r VerificationReport) ([]byte, error) {
	r.Normalize()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
