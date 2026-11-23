package workbench

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"protocolfuzzlab/internal/codes"
	"protocolfuzzlab/internal/graph"
	"protocolfuzzlab/internal/model"
	"protocolfuzzlab/internal/protocol"
	"protocolfuzzlab/internal/replay"
	"protocolfuzzlab/internal/store"
)

func StatusText(st *store.Store) (string, error) {
	head, err := st.Head()
	if err != nil {
		return "", err
	}
	chain, chainErr := st.SnapshotChain()
	var b strings.Builder
	fmt.Fprintf(&b, "caseset_version=%d\n", head.Version)
	fmt.Fprintf(&b, "caseset_digest=%s\n", head.Digest)
	fmt.Fprintf(&b, "seeds=%d operators=%d assertions=%d cases=%d invalidations=%d\n",
		len(head.CaseSet.Seeds), len(head.CaseSet.Operators), len(head.CaseSet.Assertions), len(head.CaseSet.Cases), len(head.CaseSet.Invalidations))
	fmt.Fprintf(&b, "in_flight_sessions=0\n")
	fmt.Fprintf(&b, "snapshot_count=%d\n", len(chain))
	if chainErr != nil {
		coded := codes.From(chainErr)
		fmt.Fprintf(&b, "snapshot_chain_state=FALLBACK_TO_%d\n", coded.FallbackVersion)
		fmt.Fprintf(&b, "snapshot_chain_error=%s %s\n", coded.Code, coded.Message)
	} else {
		fmt.Fprintf(&b, "snapshot_chain_state=CONTIGUOUS\n")
	}
	for _, snap := range chain {
		fmt.Fprintf(&b, "snapshot=%d parent=%d caseset=%d batch=%s code=%s whole_replacement=%t\n",
			snap.Version, snap.ParentVersion, snap.CaseSetVersion, snap.BatchID, executionCode(snap.Content), snap.WholeReplacement)
	}
	return b.String(), chainErr
}

func ClosureText(st *store.Store, version int) (string, error) {
	result, err := diagnosticsForVersion(st, version)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "caseset_version=%d\n", version)
	fmt.Fprintf(&b, "closure_state=%s\n", map[bool]string{true: "EMPTY", false: "INVALIDATED"}[len(result) == 0])
	for _, d := range result {
		fmt.Fprintf(&b, "code=%s entity=%s:%s caseset_version=%d root=%s\n",
			d.Code, d.Entity, d.EntityID, d.Version, strings.Join(d.RootChain, " -> "))
	}
	return b.String(), nil
}

func MatrixText(st *store.Store, version int) (string, error) {
	if _, err := st.LoadCaseSet(version); err != nil {
		return "", err
	}
	chain, _ := st.SnapshotChain()
	coverageCases := map[string][]string{}
	for _, snap := range chain {
		if snap.CaseSetVersion != version {
			continue
		}
		var exec replay.ExecutionBatch
		if err := json.Unmarshal(snap.Content, &exec); err != nil {
			continue
		}
		for _, c := range exec.Cases {
			for _, key := range c.Coverage {
				coverageCases[key.String()] = append(coverageCases[key.String()], c.CaseID)
			}
		}
	}
	for key := range coverageCases {
		sort.Strings(coverageCases[key])
	}
	rows := make([]model.TransitionKey, 0)
	messages := map[string][]string{}
	seen := map[string]bool{}
	for _, rule := range protocol.Rules() {
		key := model.TransitionKey{From: rule.From, To: rule.To}
		if !seen[key.String()] {
			seen[key.String()] = true
			rows = append(rows, key)
		}
		messages[key.String()] = append(messages[key.String()], rule.Type.String())
	}
	sort.Slice(rows, func(i, j int) bool {
		if stateRank(rows[i].From) != stateRank(rows[j].From) {
			return stateRank(rows[i].From) < stateRank(rows[j].From)
		}
		return stateRank(rows[i].To) < stateRank(rows[j].To)
	})
	var b strings.Builder
	fmt.Fprintf(&b, "caseset_version=%d\n", version)
	fmt.Fprintf(&b, "%-10s %-16s %-10s %-7s %s\n", "FROM", "MESSAGE", "TO", "COVERED", "CASES")
	for _, key := range rows {
		message := strings.Join(messages[key.String()], ",")
		if message == "" {
			message = "ANY"
		}
		cases := coverageCases[key.String()]
		fmt.Fprintf(&b, "%-10s %-16s %-10s %-7t %s\n", key.From, message, key.To, len(cases) > 0, listOrDash(cases))
	}
	return b.String(), nil
}

func TraceText(st *store.Store, version int, caseID string) (string, error) {
	cs, err := st.LoadCaseSet(version)
	if err != nil {
		return "", err
	}
	if _, ok := cs.CaseSet.Case(caseID); !ok {
		return "", codes.New(codes.BadRequest, "case %q does not exist in version %d", caseID, version)
	}
	links := graph.TraceCase(cs.CaseSet, caseID)
	var b strings.Builder
	fmt.Fprintf(&b, "caseset_version=%d case=%s\n", version, caseID)
	for _, d := range diagnosticList(cs.CaseSet, version) {
		if d.EntityID == caseID {
			fmt.Fprintf(&b, "invalid code=%s root=%s\n", d.Code, strings.Join(d.RootChain, " -> "))
		}
	}
	fmt.Fprintf(&b, "dependencies=%d\n", len(links))
	for _, link := range links {
		fmt.Fprintf(&b, "%s:%s -[%s]-> %s:%s\n", link.FromType, link.FromID, link.Relation, link.ToType, link.ToID)
	}
	return b.String(), nil
}

func diagnosticsForVersion(st *store.Store, version int) ([]model.Diagnostic, error) {
	cs, err := st.LoadCaseSet(version)
	if err != nil {
		return nil, err
	}
	return diagnosticList(cs.CaseSet, version), nil
}

func diagnosticList(cs model.CaseSet, version int) []model.Diagnostic {
	seen := map[string]bool{}
	var out []model.Diagnostic
	add := func(d model.Diagnostic) {
		key := d.Entity + "\x00" + d.EntityID + "\x00" + d.Code + "\x00" + strings.Join(d.RootChain, ">")
		if seen[key] {
			return
		}
		seen[key] = true
		if d.Version == 0 {
			d.Version = version
		}
		out = append(out, d)
	}
	for _, d := range cs.Invalidations {
		add(d)
	}
	for _, d := range graph.Analyze(cs, version, nil).Diagnostics {
		add(d)
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

func executionCode(content json.RawMessage) string {
	var exec replay.ExecutionBatch
	if err := json.Unmarshal(content, &exec); err != nil {
		return "UNREADABLE"
	}
	return exec.Code
}

func listOrDash(list []string) string {
	if len(list) == 0 {
		return "-"
	}
	return strings.Join(list, ",")
}

func stateRank(s protocol.State) int {
	for i, candidate := range protocol.States() {
		if s == candidate {
			return i
		}
	}
	return len(protocol.States())
}
