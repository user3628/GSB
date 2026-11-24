// Package app orchestrates the full adjudication pipeline: parse,
// commit, compare, classify, persist and report. Fault injection is
// driven exclusively by external parameters (scenario selection and
// limits), never by source modification.
package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"fwadjudge/internal/codes"
	"fwadjudge/internal/compare"
	"fwadjudge/internal/engine"
	"fwadjudge/internal/fixed"
	"fwadjudge/internal/ledger"
	"fwadjudge/internal/model"
	"fwadjudge/internal/parse"
	"fwadjudge/internal/report"
	"fwadjudge/internal/store"
)

// Options configures one engine run.
type Options struct {
	DBPath        string
	SamplesDir    string
	ReportPath    string // empty: no report file is written
	Scenario      string // "all" or a single scenario name
	MaxConcurrent int
	RuleBudget    int
	WaitThreshold time.Duration
}

// Outcome is the result of a run.
type Outcome struct {
	Report   *report.Report
	Codes    []string
	Scenario map[string][]string // scenario -> sorted codes
}

type probeFile struct {
	Probes []struct {
		ID        string `json:"id"`
		Direction string `json:"direction"`
		Protocol  string `json:"protocol"`
		Src       string `json:"src"`
		Dst       string `json:"dst"`
		Port      int    `json:"port"`
		Weight    string `json:"weight"`
		Count     string `json:"count"`
	} `json:"probes"`
}

func loadProbes(path string) ([]model.Probe, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pf probeFile
	if err := json.Unmarshal(data, &pf); err != nil {
		return nil, err
	}
	out := make([]model.Probe, 0, len(pf.Probes))
	for _, p := range pf.Probes {
		out = append(out, model.Probe{
			ID:        p.ID,
			Direction: model.Direction(p.Direction),
			Protocol:  model.Protocol(p.Protocol),
			SrcIP:     p.Src,
			DstIP:     p.Dst,
			DstPort:   p.Port,
			Weight:    p.Weight,
			Count:     p.Count,
		})
	}
	return out, nil
}

func loadJSONSnapshot(path string) (*parse.Result, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parse.ParseJSON(data)
}

// scenarioNames lists every scenario in deterministic order.
var scenarioNames = []string{
	"normal", "diverge", "conflict",
	"bad-cidr", "port-invert", "overflow",
	"version-gap", "replace",
}

// Run executes the selected scenarios and, on the normal exit path
// only, writes the deterministic report.
func Run(opts Options) (*Outcome, error) {
	st, err := store.Open(opts.DBPath)
	if err != nil {
		return nil, err
	}
	defer st.Close()

	mgr := compare.NewManager(compare.Config{
		MaxConcurrent: opts.MaxConcurrent,
		RuleBudget:    opts.RuleBudget,
		WaitThreshold: opts.WaitThreshold,
	})

	rep := report.New()
	out := &Outcome{Report: rep, Scenario: map[string][]string{}}
	snapCodes := map[string]map[string]bool{}

	names := scenarioNames
	if opts.Scenario != "" && opts.Scenario != "all" {
		names = []string{opts.Scenario}
	}
	for _, name := range names {
		sc := &scenarioCtx{st: st, mgr: mgr, dir: opts.SamplesDir, rep: rep, codes: map[string]bool{}, snapCodes: snapCodes}
		var err error
		switch name {
		case "normal":
			err = sc.runNormal()
		case "diverge":
			err = sc.runCompare([]string{"normal/hq.json", "normal/branch-b.json", "normal/branch-c.json"}, "normal/probes.json", nil)
		case "conflict":
			err = sc.runCompare([]string{"conflict/conflict.json", "normal/hq.json"}, "normal/probes.json", nil)
		case "bad-cidr":
			err = sc.runCompare([]string{"bad-cidr/bad.json"}, "normal/probes.json", nil)
		case "port-invert":
			err = sc.runCompare([]string{"port-invert/inv.json"}, "normal/probes.json", nil)
		case "overflow":
			err = sc.runCompare([]string{"overflow/ovf.json"}, "overflow/probes.json", nil)
		case "version-gap":
			err = sc.runVersionGap()
		case "replace":
			err = sc.runReplace()
		default:
			err = fmt.Errorf("unknown scenario %q", name)
		}
		if err != nil {
			return nil, fmt.Errorf("scenario %s: %w", name, err)
		}
		out.Scenario[name] = sc.sortedCodes()
		for _, c := range out.Scenario[name] {
			rep.AddCode(c)
		}
	}

	if err := out.fillFromStore(st); err != nil {
		return nil, err
	}
	for i := range rep.Snapshots {
		set := snapCodes[rep.Snapshots[i].SnapshotID]
		for c := range set {
			rep.Snapshots[i].Codes = append(rep.Snapshots[i].Codes, c)
		}
		if len(rep.Snapshots[i].Codes) == 0 {
			rep.Snapshots[i].Codes = append(rep.Snapshots[i].Codes, codes.OK)
		}
	}
	rep.Sort()
	out.Codes = append(out.Codes, rep.Codes...)

	// The report is written only on the normal exit path; a killed
	// process leaves no report behind and recovery relies solely on
	// committed SQLite state.
	if opts.ReportPath != "" {
		data, err := rep.Bytes()
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(opts.ReportPath, data, 0o644); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// runNormal compares the consistent snapshots, including one loaded
// from the length-prefixed text-block source.
func (sc *scenarioCtx) runNormal() error {
	probes, err := loadProbes(filepath.Join(sc.dir, "normal/probes.json"))
	if err != nil {
		return err
	}
	var tasks []compare.Task
	for _, f := range []string{"normal/hq.json", "normal/branch-a.json"} {
		res, err := loadJSONSnapshot(filepath.Join(sc.dir, f))
		if err != nil {
			return err
		}
		t, err := sc.commitParsed(res, 0)
		if err != nil {
			return err
		}
		t.Probes = probes
		tasks = append(tasks, *t)
	}
	blkData, err := os.ReadFile(filepath.Join(sc.dir, "textblk/branch-d.blk"))
	if err != nil {
		return err
	}
	res := parse.ParseBlocks(blkData, "branch-d")
	t, err := sc.commitParsed(res, 0)
	if err != nil {
		return err
	}
	t.Probes = probes
	tasks = append(tasks, *t)
	return sc.compareAndCommit(tasks, probes)
}

type scenarioCtx struct {
	st    *store.Store
	mgr   *compare.Manager
	dir   string
	rep   *report.Report
	codes map[string]bool
	// per-snapshot business codes collected during the scenario
	snapCodes map[string]map[string]bool
}

func (sc *scenarioCtx) addCode(snapshotID, code string) {
	sc.codes[code] = true
	if snapshotID == "" {
		return
	}
	if sc.snapCodes == nil {
		sc.snapCodes = map[string]map[string]bool{}
	}
	if sc.snapCodes[snapshotID] == nil {
		sc.snapCodes[snapshotID] = map[string]bool{}
	}
	sc.snapCodes[snapshotID][code] = true
}

func (sc *scenarioCtx) sortedCodes() []string {
	out := make([]string, 0, len(sc.codes))
	for c := range sc.codes {
		out = append(out, c)
	}
	sort.Strings(out)
	if len(out) == 0 {
		out = append(out, codes.OK)
	}
	return out
}

// commitParsed commits one parsed snapshot and records parse issues.
func (sc *scenarioCtx) commitParsed(res *parse.Result, forcedVersion int) (*compare.Task, error) {
	for _, iss := range res.Issues {
		sc.addCode(iss.SnapshotID, iss.Code)
	}
	if _, cerr := sc.st.CommitSnapshot(res.SnapshotID, res.Rules, nil, forcedVersion); cerr != nil {
		return nil, fmt.Errorf("commit snapshot %s: %w", res.SnapshotID, cerr)
	}
	return &compare.Task{SnapshotID: res.SnapshotID, Rules: res.Rules}, nil
}

// runCompare parses, commits and compares the given snapshots over the
// probe list, then commits verdicts and divergences in one transaction.
// extraTasks may inject externally-built tasks (used by version-gap).
func (sc *scenarioCtx) runCompare(files []string, probesFile string, extraTasks []compare.Task) error {
	probes, err := loadProbes(filepath.Join(sc.dir, probesFile))
	if err != nil {
		return err
	}
	var tasks []compare.Task
	for _, f := range files {
		res, err := loadJSONSnapshot(filepath.Join(sc.dir, f))
		if err != nil {
			return err
		}
		t, err := sc.commitParsed(res, 0)
		if err != nil {
			return err
		}
		t.Probes = probes
		tasks = append(tasks, *t)
	}
	tasks = append(tasks, extraTasks...)
	return sc.compareAndCommit(tasks, probes)
}

// compareAndCommit runs the compare manager and commits one verdict
// round in a single transaction.
func (sc *scenarioCtx) compareAndCommit(tasks []compare.Task, probes []model.Probe) error {
	results := sc.mgr.Run(tasks)
	byID := map[string]*compare.Result{}
	for i := range results {
		r := &results[i]
		byID[r.SnapshotID] = r
		for _, e := range r.Errors {
			sc.addCode(e.SnapshotID, e.Code)
		}
	}
	// Pairwise divergence localization, pairs ordered by snapshot id.
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var divs []engine.Divergence
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			a, b := byID[ids[i]], byID[ids[j]]
			if a.Session == nil || b.Session == nil {
				continue
			}
			vA := verdictsByProbe(a.Verdicts)
			vB := verdictsByProbe(b.Verdicts)
			for _, p := range probes {
				va, okA := vA[p.ID]
				vb, okB := vB[p.ID]
				if !okA || !okB {
					continue
				}
				if d := engine.ComparePair(a.Session, b.Session, va, vb); d != nil {
					divs = append(divs, *d)
					sc.addCode(d.SnapA, codes.ERuleDivergence)
					sc.addCode(d.SnapB, codes.ERuleDivergence)
				}
			}
		}
	}
	// Commit the whole verdict round in one transaction.
	versions := map[string]int{}
	var verdicts []engine.Verdict
	var entries []ledger.Entry
	for _, id := range ids {
		r := byID[id]
		latest, cerr := sc.st.LoadChain(id)
		if cerr != nil {
			sc.addCode(id, cerr.Code)
		}
		versions[id] = latest
		if r.Session == nil {
			continue
		}
		verdicts = append(verdicts, r.Verdicts...)
		entries = append(entries, r.Session.Ledger.Entries()...)
	}
	if cerr := sc.st.CommitVerdicts(versions, verdicts, divs, entries); cerr != nil {
		return fmt.Errorf("commit verdicts: %w", cerr)
	}
	return nil
}

func verdictsByProbe(vs []engine.Verdict) map[string]engine.Verdict {
	out := map[string]engine.Verdict{}
	for _, v := range vs {
		out[v.ProbeID] = v
	}
	return out
}

// runVersionGap injects a version-chain gap via a forced version
// number (external parameter), then verifies the rollback to the
// latest continuous version.
func (sc *scenarioCtx) runVersionGap() error {
	res, err := loadJSONSnapshot(filepath.Join(sc.dir, "normal/hq.json"))
	if err != nil {
		return err
	}
	res.SnapshotID = "gap"
	for i := range res.Rules {
		res.Rules[i].SnapshotID = "gap"
	}
	if _, err := sc.commitParsed(res, 0); err != nil {
		return err
	}
	// Fault injection: skip version 2, commit version 3 directly.
	if _, err := sc.commitParsed(res, 3); err != nil {
		return err
	}
	latest, cerr := sc.st.LoadChain("gap")
	if cerr != nil {
		sc.addCode("gap", cerr.Code)
	}
	rules, err := sc.st.LoadRules("gap", latest)
	if err != nil {
		return err
	}
	probes, err := loadProbes(filepath.Join(sc.dir, "normal/probes.json"))
	if err != nil {
		return err
	}
	return sc.compareAndCommit([]compare.Task{{SnapshotID: "gap", Rules: rules, Probes: probes}}, probes)
}

// runReplace commits two wholesale-different versions of one snapshot;
// the second commit must be flagged as a whole replacement.
func (sc *scenarioCtx) runReplace() error {
	for _, f := range []string{"replace/v1.json", "replace/v2.json"} {
		res, err := loadJSONSnapshot(filepath.Join(sc.dir, f))
		if err != nil {
			return err
		}
		if _, err := sc.commitParsed(res, 0); err != nil {
			return err
		}
	}
	return nil
}

// fillFromStore fills the report snapshots, divergences and version
// chains from committed state.
func (o *Outcome) fillFromStore(st *store.Store) error {
	metas, err := st.ListSnapshots()
	if err != nil {
		return err
	}
	rep := o.Report
	chainSeen := map[string]bool{}
	for _, m := range metas {
		sr := report.SnapshotReport{
			SnapshotID:  m.SnapshotID,
			Version:     m.Version,
			ContentHash: m.ContentHash,
			Replaced:    m.Replaced,
			RuleCount:   m.RuleCount,
			Rules:       []report.RuleSummary{},
			Codes:       []string{},
		}
		rules, err := st.LoadRules(m.SnapshotID, m.Version)
		if err != nil {
			return err
		}
		for i := range rules {
			sr.Rules = append(sr.Rules, report.RuleSummary{
				Index:    rules[i].Index,
				Action:   string(rules[i].Action),
				Priority: rules[i].Priority,
				Hash:     hashOf(&rules[i]),
			})
		}
		bound, err := st.LedgerBound(m.SnapshotID)
		if err != nil {
			return err
		}
		sr.LedgerBound = boundString(bound)
		rep.Snapshots = append(rep.Snapshots, sr)

		if !chainSeen[m.SnapshotID] {
			chainSeen[m.SnapshotID] = true
			latest, cerr := st.LoadChain(m.SnapshotID)
			vr := report.VersionChainReport{
				SnapshotID:       m.SnapshotID,
				LatestContinuous: latest,
				Code:             codes.OK,
			}
			if cerr != nil {
				vr.Code = cerr.Code
			}
			rep.Chains = append(rep.Chains, vr)
		}
	}
	// Chain versions.
	for i := range rep.Chains {
		for _, m := range metas {
			if m.SnapshotID == rep.Chains[i].SnapshotID {
				rep.Chains[i].Versions = append(rep.Chains[i].Versions, m.Version)
			}
		}
	}
	divs, err := st.Divergences()
	if err != nil {
		return err
	}
	for _, d := range divs {
		rep.Divergences = append(rep.Divergences, report.DivergenceReport{
			SnapA:     d.SnapA,
			SnapB:     d.SnapB,
			ProbeID:   d.ProbeID,
			RuleA:     d.RuleA,
			RuleB:     d.RuleB,
			Field:     d.Field,
			Class:     d.Class,
			IntervalA: [2]string{d.LoA.String(), d.HiA.String()},
			IntervalB: [2]string{d.LoB.String(), d.HiB.String()},
			Code:      codes.ERuleDivergence,
		})
	}
	return nil
}

func boundString(units int64) string {
	return fixed.Fixed(ledger.BoundFixed(units)).String()
}

func hashOf(r *model.Rule) string {
	sum := sha256.Sum256([]byte(r.Canonical()))
	return hex.EncodeToString(sum[:])
}
