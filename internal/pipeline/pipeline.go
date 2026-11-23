// Package pipeline orchestrates the full tracing run: sample loading,
// anomaly injection, concurrent window scheduling, snapshot commits,
// crash/recovery and deterministic report building.
package pipeline

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"flowtrace/internal/adjudicate"
	"flowtrace/internal/codes"
	"flowtrace/internal/latency"
	"flowtrace/internal/model"
	"flowtrace/internal/parse"
	"flowtrace/internal/policy"
	"flowtrace/internal/report"
	"flowtrace/internal/samplegen"
	"flowtrace/internal/scheduler"
	"flowtrace/internal/snapshot"
	"flowtrace/internal/trace"
)

// InjectFlags selects anomaly injection scenarios (input/parameter only).
type InjectFlags struct {
	Reorder     bool // out-of-order arrival
	Cycle       bool // cyclic propagation events
	Escape      bool // extra out-of-boundary export
	SnapshotGap bool // break the snapshot version chain
	Contention  bool // more windows than the concurrency limit
}

// Options configures one pipeline run.
type Options struct {
	Dir                 string
	DBPath              string
	Inject              InjectFlags
	MaxConcurrent       int
	MaxEventsPerWindow  int
	SessionMemoryBudget int64
	CrashAfter          int  // commit only N windows, then abort (simulated kill)
	Resume              bool // recover committed windows from the DB
}

// Outcome is the result of a run.
type Outcome struct {
	Report  *report.Report
	Crashed bool
}

// Run executes the pipeline.
func Run(opts Options) (*Outcome, error) {
	manifest, err := samplegen.LoadManifest(opts.Dir)
	if err != nil {
		return nil, err
	}
	polRaw, err := os.ReadFile(filepath.Join(opts.Dir, manifest.PolicyFile))
	if err != nil {
		return nil, err
	}
	pol, err := policy.ParseSnapshot(polRaw)
	if err != nil {
		return nil, err
	}

	sessions, parseFailures, err := loadSessions(opts.Dir, manifest, pol, opts.Inject)
	if err != nil {
		return nil, err
	}

	maxConcurrent := opts.MaxConcurrent
	if opts.Inject.Contention {
		maxConcurrent = 2
		sessions = addContentionWindows(sessions)
	}
	cfg := scheduler.Config{
		MaxConcurrent:       maxConcurrent,
		MaxEventsPerWindow:  opts.MaxEventsPerWindow,
		SessionMemoryBudget: opts.SessionMemoryBudget,
	}

	store, err := snapshot.Open(opts.DBPath)
	if err != nil {
		return nil, err
	}
	defer store.Close()

	committed := map[string]bool{}
	if opts.Resume {
		rows, err := store.CommittedWindows()
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			committed[r.WindowID] = true
		}
		// Uncommitted windows are discarded wholesale and re-traced below.
	}

	sampler := latency.NewSampler()
	results := scheduler.Run(cfg, sessions, sampler)

	// Commit snapshots in stable window order; CrashAfter simulates a kill.
	sort.Slice(results, func(i, j int) bool { return results[i].WindowID < results[j].WindowID })
	commits := 0
	for _, res := range results {
		if committed[res.WindowID] || res.Incomplete {
			continue
		}
		if opts.CrashAfter > 0 && commits >= opts.CrashAfter {
			return &Outcome{Crashed: true}, nil
		}
		w := res.Window
		if _, err := store.Commit(res.WindowID, pol.Version, w.InteractiveMarks(), w.SettlementJudgments(), codes.OK, res.EventCount, w.Version()); err != nil {
			return nil, err
		}
		commits++
	}

	if opts.Inject.SnapshotGap {
		if err := injectSnapshotGap(store, results); err != nil {
			return nil, err
		}
	}

	rep, err := buildReport(results, committed, store, sampler, parseFailures)
	if err != nil {
		return nil, err
	}
	return &Outcome{Report: rep}, nil
}

func loadSessions(dir string, manifest *samplegen.Manifest, pol *policy.Snapshot, inject InjectFlags) ([]scheduler.Session, []report.ParseFailure, error) {
	var sessions []scheduler.Session
	var failures []report.ParseFailure
	for _, ms := range manifest.Sessions {
		sess := scheduler.Session{ID: ms.ID, Policy: pol}
		for _, mw := range ms.Windows {
			raw, err := os.ReadFile(filepath.Join(dir, mw.File))
			if err != nil {
				return nil, nil, err
			}
			var res parse.Result
			switch mw.Format {
			case "bin":
				res = parse.ParseBinary(raw)
			case "jsonl":
				res = parse.ParseJSONL(raw)
			default:
				return nil, nil, fmt.Errorf("unknown format %q", mw.Format)
			}
			for _, e := range res.Errors {
				failures = append(failures, report.ParseFailure{WindowID: mw.ID, Code: e.Code, Detail: e.Detail})
			}
			events := res.Events
			if inject.Reorder {
				events = reorder(events)
			}
			sess.Windows = append(sess.Windows, scheduler.WindowInput{ID: mw.ID, Events: events})
		}
		sessions = append(sessions, sess)
	}
	if inject.Cycle && len(sessions) > 0 && len(sessions[0].Windows) > 0 {
		sessions[0].Windows[0].Events = append(sessions[0].Windows[0].Events, samplegen.CycleEvents()...)
	}
	if inject.Escape && len(sessions) > 0 && len(sessions[0].Windows) > 1 {
		sessions[0].Windows[1].Events = append(sessions[0].Windows[1].Events, samplegen.EscapeEvents()...)
	}
	return sessions, failures, nil
}

// reorder deterministically reverses arrival order to simulate out-of-order
// and late events.
func reorder(events []model.Event) []model.Event {
	out := append([]model.Event{}, events...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// addContentionWindows clones existing windows under new IDs so the number
// of windows exceeds the concurrency limit and the wait queue engages.
func addContentionWindows(sessions []scheduler.Session) []scheduler.Session {
	if len(sessions) == 0 || len(sessions[0].Windows) == 0 {
		return sessions
	}
	base := sessions[0].Windows[0]
	for i := 0; i < 6; i++ {
		w := scheduler.WindowInput{
			ID:     fmt.Sprintf("WC%02d", i+1),
			Events: append([]model.Event{}, base.Events...),
		}
		sessions[0].Windows = append(sessions[0].Windows, w)
	}
	return sessions
}

// injectSnapshotGap deletes a middle snapshot version of the first window
// with at least two versions, simulating a broken chain.
func injectSnapshotGap(store *snapshot.Store, results []scheduler.WindowResult) error {
	// Commit a second snapshot for the first window so a middle version
	// exists, then remove v1 to create a gap.
	for _, res := range results {
		if res.Incomplete {
			continue
		}
		w := res.Window
		if _, err := store.Commit(res.WindowID, wSettlementPolicyVersion(store, res.WindowID), w.InteractiveMarks(), w.SettlementJudgments(), codes.OK, res.EventCount, w.Version()); err != nil {
			return err
		}
		return store.DeleteVersion(res.WindowID, 1)
	}
	return nil
}

func wSettlementPolicyVersion(store *snapshot.Store, windowID string) int {
	chain, err := store.LoadChain(windowID)
	if err != nil || len(chain) == 0 {
		return 0
	}
	return chain[len(chain)-1].PolicyVersion
}

func buildReport(results []scheduler.WindowResult, previouslyCommitted map[string]bool, store *snapshot.Store, sampler *latency.Sampler, failures []report.ParseFailure) (*report.Report, error) {
	rep := &report.Report{OverallConsistent: true}
	seen := map[string]bool{}

	for _, res := range results {
		seen[res.WindowID] = true
		wr := report.WindowReport{
			WindowID: res.WindowID, SessionID: res.SessionID,
			Code: res.Code, Incomplete: res.Incomplete, Consistent: true,
		}
		if !res.Incomplete {
			w := res.Window
			wr.Events = trace.CanonicalOrder(arrivalEvents(w))
			wr.Judgments = sortedJudgments(w.SettlementJudgments())
			wr.Corrections = w.Corrections()
			cons := w.Consistency()
			wr.Consistent = cons.Consistent
			wr.Mismatches = cons.Mismatches
			if !cons.Consistent {
				rep.OverallConsistent = false
			}
		}
		rep.Windows = append(rep.Windows, wr)
	}

	// Snapshot chains (all committed windows, including recovered ones).
	rows, err := store.CommittedWindows()
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		chain, chainErr := store.LoadChain(row.WindowID)
		cr := report.ChainReport{WindowID: row.WindowID}
		for _, snap := range chain {
			cr.Versions = append(cr.Versions, snap.Version)
			cr.PolicyVersion = snap.PolicyVersion
		}
		if chainErr != nil {
			if ce, ok := chainErr.(*snapshot.ChainError); ok {
				cr.Error = ce.Code + ": " + ce.Detail
				cr.ValidPrefix = ce.ValidPrefix
			}
		}
		rep.Chains = append(rep.Chains, cr)

		// Recovered windows (committed before a crash) also appear in the
		// window list, sourced from their latest snapshot.
		if !seen[row.WindowID] && len(chain) > 0 {
			last := chain[len(chain)-1]
			rep.Windows = append(rep.Windows, report.WindowReport{
				WindowID:         row.WindowID,
				SessionID:        "recovered",
				Code:             codes.OK,
				Consistent:       true,
				Judgments:        sortedJudgments(last.Judgments),
				SnapshotVersions: cr.Versions,
			})
		}
	}
	for i := range rep.Windows {
		if chain, err := store.LoadChain(rep.Windows[i].WindowID); err == nil {
			for _, snap := range chain {
				rep.Windows[i].SnapshotVersions = append(rep.Windows[i].SnapshotVersions, snap.Version)
			}
		}
	}

	sort.Slice(rep.Windows, func(i, j int) bool { return rep.Windows[i].WindowID < rep.Windows[j].WindowID })
	sort.Slice(rep.Chains, func(i, j int) bool { return rep.Chains[i].WindowID < rep.Chains[j].WindowID })
	sort.Slice(failures, func(i, j int) bool {
		if failures[i].WindowID != failures[j].WindowID {
			return failures[i].WindowID < failures[j].WindowID
		}
		return failures[i].Detail < failures[j].Detail
	})
	rep.ParseFailures = failures
	rep.Latency = sampler.Stats()
	return rep, nil
}

func arrivalEvents(w *adjudicate.Window) []model.Event { return w.Arrival() }

func sortedJudgments(js []model.Judgment) []model.Judgment {
	out := append([]model.Judgment{}, js...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].EventSeq != out[j].EventSeq {
			return out[i].EventSeq < out[j].EventSeq
		}
		return out[i].DstField < out[j].DstField
	})
	return out
}
