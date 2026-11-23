package report

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"protocolfuzzlab/internal/codes"
	"protocolfuzzlab/internal/model"
	"protocolfuzzlab/internal/protocol"
	"protocolfuzzlab/internal/replay"
	"protocolfuzzlab/internal/store"
)

const Format = "protocol-fuzzlab.diagnostic-report/v1"

type Report struct {
	Format             string                `json:"format"`
	SampledAt          string                `json:"sampled_at"`
	CaseSet            CaseSetView           `json:"case_set"`
	Target             string                `json:"target"`
	SnapshotChain      []SnapshotView        `json:"snapshot_chain"`
	SnapshotChainState string                `json:"snapshot_chain_state"`
	ChainError         *codes.Error          `json:"chain_error,omitempty"`
	Replay             ReplayView            `json:"replay"`
	AffectedCases      []AffectedCase        `json:"affected_cases"`
	Invalidations      []InvalidationView    `json:"invalidations"`
	CoverageLost       []model.TransitionKey `json:"coverage_lost"`
	CoverageAdded      []model.TransitionKey `json:"coverage_added"`
}

type CaseSetView struct {
	Version          int    `json:"version"`
	Digest           string `json:"digest"`
	Description      string `json:"description,omitempty"`
	SeedCount        int    `json:"seed_count"`
	OperatorCount    int    `json:"operator_count"`
	AssertionCount   int    `json:"assertion_count"`
	CaseCount        int    `json:"case_count"`
	InvalidCaseCount int    `json:"invalid_case_count"`
}

type SnapshotView struct {
	Version          int    `json:"version"`
	ParentVersion    int    `json:"parent_version"`
	CaseSetVersion   int    `json:"caseset_version"`
	BatchID          string `json:"batch_id"`
	Digest           string `json:"digest"`
	SampledAt        string `json:"sampled_at"`
	WholeReplacement bool   `json:"whole_replacement"`
	ContentChanged   bool   `json:"content_changed"`
	PreviousVersion  int    `json:"previous_version"`
}

type ReplayView struct {
	ContentDigest  string           `json:"content_digest"`
	Scheduler      SchedulerView    `json:"scheduler"`
	CodeCounts     []CodeCount      `json:"code_counts"`
	Cases          []CaseView       `json:"cases"`
	Coverage       []TransitionView `json:"coverage"`
	CoverageMatrix []MatrixRow      `json:"coverage_matrix"`
}

type SchedulerView struct {
	Limit      int `json:"limit"`
	BatchCount int `json:"batch_count"`
	InFlight   int `json:"in_flight"`
	Queued     int `json:"queued"`
}

type CodeCount struct {
	Code  string `json:"code"`
	Count int    `json:"count"`
}

type CaseView struct {
	CaseID     string                   `json:"case_id"`
	Code       string                   `json:"code"`
	FinalState protocol.State           `json:"final_state"`
	FramesSent int                      `json:"frames_sent"`
	Trace      []replay.TraceStep       `json:"trace"`
	Coverage   []model.TransitionKey    `json:"coverage"`
	Assertions []replay.AssertionResult `json:"assertions"`
	RootChain  []string                 `json:"root_chain,omitempty"`
}

type TransitionView struct {
	From  protocol.State `json:"from"`
	To    protocol.State `json:"to"`
	Cases []string       `json:"cases"`
}

type MatrixRow struct {
	From    protocol.State `json:"from"`
	Message string         `json:"message_type"`
	To      protocol.State `json:"to"`
	Covered bool           `json:"covered"`
	Lost    bool           `json:"lost"`
	Cases   []string       `json:"cases"`
}

type AffectedCase struct {
	CaseID    string   `json:"case_id"`
	Code      string   `json:"code"`
	Present   bool     `json:"present"`
	RootChain []string `json:"root_chain"`
}

type InvalidationView struct {
	Entity    string   `json:"entity_type"`
	EntityID  string   `json:"entity_id"`
	Code      string   `json:"code"`
	RootChain []string `json:"root_chain"`
	Version   int      `json:"caseset_version"`
}

func Build(st *store.Store, result replay.RunResult) (Report, error) {
	version, err := st.LoadCaseSet(result.CaseSetVersion)
	if err != nil {
		return Report{}, err
	}
	currentCoverage := map[string]model.TransitionKey{}
	for _, key := range result.Coverage {
		currentCoverage[key.String()] = key
	}
	previousCoverage := map[string]model.TransitionKey{}
	previousVersion := previousCaseSetVersion(st, result.CaseSetVersion)
	if previousVersion > 0 {
		for _, snap := range result.Snapshots {
			if snap.CaseSetVersion != previousVersion {
				continue
			}
			var exec replay.ExecutionBatch
			if err := json.Unmarshal(snap.Content, &exec); err != nil {
				continue
			}
			for _, key := range exec.Coverage {
				previousCoverage[key.String()] = key
			}
		}
	}

	cases := make([]replay.CaseResult, 0)
	for _, batch := range result.Batches {
		cases = append(cases, batch.Execution.Cases...)
	}
	sort.Slice(cases, func(i, j int) bool {
		if cases[i].CaseID != cases[j].CaseID {
			return cases[i].CaseID < cases[j].CaseID
		}
		return cases[i].Code < cases[j].Code
	})
	caseViews := make([]CaseView, 0, len(cases))
	codeCounts := map[string]int{}
	for _, c := range cases {
		root := []string(nil)
		if c.Diagnostic != nil {
			root = append(root, c.Diagnostic.RootChain...)
		}
		caseViews = append(caseViews, CaseView{
			CaseID: c.CaseID, Code: c.Code, FinalState: c.FinalState, FramesSent: c.FramesSent,
			Trace: append([]replay.TraceStep(nil), c.Trace...), Coverage: append([]model.TransitionKey(nil), c.Coverage...),
			Assertions: append([]replay.AssertionResult(nil), c.Assertions...), RootChain: root,
		})
		codeCounts[c.Code]++
	}

	coverageCases := map[string][]string{}
	for _, c := range cases {
		for _, key := range c.Coverage {
			coverageCases[key.String()] = append(coverageCases[key.String()], c.CaseID)
		}
	}
	for k := range coverageCases {
		sort.Strings(coverageCases[k])
	}
	transitionViews := make([]TransitionView, 0, len(currentCoverage))
	for _, key := range sortedTransitions(currentCoverage) {
		transitionViews = append(transitionViews, TransitionView{From: key.From, To: key.To, Cases: coverageCases[key.String()]})
	}

	lost := difference(previousCoverage, currentCoverage)
	added := difference(currentCoverage, previousCoverage)
	matrix := buildMatrix(currentCoverage, lost, coverageCases)

	invalidations := mergeCurrentInvalidations(version.CaseSet)
	affected := buildAffectedCases(version, cases, invalidations)

	chainState := "CONTIGUOUS"
	if result.ChainError != nil {
		chainState = fmt.Sprintf("FALLBACK_TO_%d", result.ChainError.FallbackVersion)
	}
	snapshots := make([]SnapshotView, 0, len(result.Snapshots))
	for _, snap := range result.Snapshots {
		snapshots = append(snapshots, snapshotView(snap))
	}

	report := Report{
		Format: Format, SampledAt: store.SampleMarker,
		CaseSet: CaseSetView{
			Version: version.Version, Digest: version.Digest, Description: version.Summary.Description,
			SeedCount: len(version.CaseSet.Seeds), OperatorCount: len(version.CaseSet.Operators),
			AssertionCount: len(version.CaseSet.Assertions), CaseCount: len(version.CaseSet.Cases),
			InvalidCaseCount: len(result.InvalidCaseIDs),
		},
		Target: result.Target, SnapshotChain: snapshots, SnapshotChainState: chainState, ChainError: result.ChainError,
		Replay: ReplayView{
			ContentDigest: result.ContentDigest,
			Scheduler:     SchedulerView{Limit: result.Stats.Limit, BatchCount: len(result.Batches), InFlight: 0, Queued: 0},
			CodeCounts:    sortedCodeCounts(codeCounts),
			Cases:         caseViews, Coverage: transitionViews, CoverageMatrix: matrix,
		},
		AffectedCases: affected, Invalidations: invalidations,
		CoverageLost: sortedTransitionValues(lost), CoverageAdded: sortedTransitionValues(added),
	}
	return report, nil
}

func Marshal(r Report) ([]byte, error) {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

func previousCaseSetVersion(st *store.Store, current int) int {
	versions, err := st.CaseSetVersions()
	if err != nil {
		return 0
	}
	best := 0
	for _, v := range versions {
		if v.Version < current && v.Version > best {
			best = v.Version
		}
	}
	return best
}

func snapshotView(s store.SnapshotRecord) SnapshotView {
	return SnapshotView{
		Version: s.Version, ParentVersion: s.ParentVersion, CaseSetVersion: s.CaseSetVersion,
		BatchID: s.BatchID, Digest: s.Digest, SampledAt: s.CreatedMarker,
		WholeReplacement: s.WholeReplacement, ContentChanged: s.ContentChanged, PreviousVersion: s.PreviousVersion,
	}
}

func mergeCurrentInvalidations(cs model.CaseSet) []InvalidationView {
	seen := map[string]bool{}
	var out []InvalidationView
	for _, d := range cs.Invalidations {
		key := d.Entity + "\x00" + d.EntityID + "\x00" + d.Code + "\x00" + strings.Join(d.RootChain, ">")
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, InvalidationView{
			Entity: d.Entity, EntityID: d.EntityID, Code: d.Code,
			RootChain: append([]string(nil), d.RootChain...), Version: d.Version,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Entity != out[j].Entity {
			return out[i].Entity < out[j].Entity
		}
		if out[i].EntityID != out[j].EntityID {
			return out[i].EntityID < out[j].EntityID
		}
		if out[i].Code != out[j].Code {
			return out[i].Code < out[j].Code
		}
		return strings.Join(out[i].RootChain, ">") < strings.Join(out[j].RootChain, ">")
	})
	return out
}

func buildAffectedCases(version store.CaseSetVersion, cases []replay.CaseResult, invalidations []InvalidationView) []AffectedCase {
	caseCode := map[string]string{}
	caseRoot := map[string][]string{}
	present := map[string]bool{}
	for _, c := range cases {
		present[c.CaseID] = true
		caseCode[c.CaseID] = c.Code
		if c.Diagnostic != nil {
			caseRoot[c.CaseID] = append([]string(nil), c.Diagnostic.RootChain...)
		}
	}
	affectedSet := map[string]bool{}
	for _, id := range version.Summary.AffectedCases {
		affectedSet[id] = true
	}
	for _, inv := range invalidations {
		if inv.Entity == model.EntityCase {
			affectedSet[inv.EntityID] = true
			if _, ok := caseRoot[inv.EntityID]; !ok {
				caseRoot[inv.EntityID] = append([]string(nil), inv.RootChain...)
			}
		}
	}
	for id := range present {
		if caseCode[id] != codes.OK {
			affectedSet[id] = true
		}
	}
	var out []AffectedCase
	for id := range affectedSet {
		code := caseCode[id]
		if code == "" {
			code = codes.DepInvalidated
		}
		root := caseRoot[id]
		if len(root) == 0 {
			root = []string{"case:" + id}
		}
		out = append(out, AffectedCase{CaseID: id, Code: code, Present: present[id], RootChain: root})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CaseID != out[j].CaseID {
			return out[i].CaseID < out[j].CaseID
		}
		return out[i].Code < out[j].Code
	})
	return out
}

func buildMatrix(current map[string]model.TransitionKey, lost map[string]model.TransitionKey, cases map[string][]string) []MatrixRow {
	rows := map[string]MatrixRow{}
	messages := map[string][]string{}
	for _, rule := range protocol.Rules() {
		key := model.TransitionKey{From: rule.From, To: rule.To}.String()
		if _, exists := rows[key]; !exists {
			rows[key] = MatrixRow{From: rule.From, To: rule.To}
		}
		messages[key] = append(messages[key], rule.Type.String())
	}
	for key, tr := range current {
		row := rows[key]
		if row.From == "" {
			row.From, row.To = tr.From, tr.To
		}
		row.Covered = true
		row.Cases = append([]string(nil), cases[key]...)
		rows[key] = row
	}
	for key, tr := range lost {
		row := rows[key]
		if row.From == "" {
			row.From, row.To = tr.From, tr.To
		}
		row.Lost = true
		rows[key] = row
	}
	out := make([]MatrixRow, 0, len(rows))
	for key, row := range rows {
		if types := messages[key]; len(types) > 0 {
			row.Message = strings.Join(types, ",")
		} else {
			row.Message = "ANY"
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if stateRank(out[i].From) != stateRank(out[j].From) {
			return stateRank(out[i].From) < stateRank(out[j].From)
		}
		if stateRank(out[i].To) != stateRank(out[j].To) {
			return stateRank(out[i].To) < stateRank(out[j].To)
		}
		return out[i].Message < out[j].Message
	})
	return out
}

func sortedCodeCounts(counts map[string]int) []CodeCount {
	out := make([]CodeCount, 0, len(counts))
	for code, count := range counts {
		out = append(out, CodeCount{Code: code, Count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out
}

func sortedTransitions(m map[string]model.TransitionKey) []model.TransitionKey {
	out := make([]model.TransitionKey, 0, len(m))
	for _, tr := range m {
		out = append(out, tr)
	}
	sortTransitions(out)
	return out
}

func sortedTransitionValues(m map[string]model.TransitionKey) []model.TransitionKey {
	out := sortedTransitions(m)
	if out == nil {
		return []model.TransitionKey{}
	}
	return out
}

func sortTransitions(out []model.TransitionKey) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return stateRank(out[i].From) < stateRank(out[j].From)
		}
		if out[i].To != out[j].To {
			return stateRank(out[i].To) < stateRank(out[j].To)
		}
		return out[i].From < out[j].From
	})
}

func difference(a, b map[string]model.TransitionKey) map[string]model.TransitionKey {
	out := map[string]model.TransitionKey{}
	for key, value := range a {
		if _, ok := b[key]; !ok {
			out[key] = value
		}
	}
	return out
}

func stateRank(s protocol.State) int {
	for i, candidate := range protocol.States() {
		if s == candidate {
			return i
		}
	}
	return len(protocol.States())
}
