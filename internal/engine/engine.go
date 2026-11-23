package engine

import (
	"encoding/json"
	"errors"
	"sort"
)

// ErrSimulatedCrash is returned when the CrashAfter option aborts the run
// before the snapshot commit, simulating a hard process kill.
var ErrSimulatedCrash = errors.New("simulated crash before snapshot commit")

type Options struct {
	Seed              int64
	NumWindows        int
	Concurrency       int
	Budget            BudgetParams
	Inject            []string
	DBPath            string
	CrashAfter        int
	InjectSnapshotGap bool
}

type Outcome struct {
	Report     Report
	ReportJSON []byte
	Snapshot   Snapshot
}

// Run executes one full deterministic round: parse, recover, schedule,
// correlate, adjudicate, commit verdicts, merge conflicts, commit snapshot
// and build the report.
func Run(opts Options) (*Outcome, error) {
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}

	sample := GenerateSample(opts.Seed, opts.NumWindows, opts.Inject)
	valid, malformed := ParseWindows(sample)

	store, err := OpenStore(opts.DBPath)
	if err != nil {
		return nil, err
	}
	defer store.Close()

	_, broken, err := store.LoadSnapshots()
	if err != nil {
		return nil, err
	}
	snapCodes := []string{}
	if broken {
		snapCodes = append(snapCodes, CodeSnapshotVersion)
	}

	committed, err := store.LoadVerdicts()
	if err != nil {
		return nil, err
	}

	pending := make([]Window, 0, len(valid))
	for _, w := range valid {
		if _, ok := committed[w.ID]; !ok {
			pending = append(pending, w)
		}
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].ID < pending[j].ID })

	corr, queue := CorrelateAll(pending, opts.Concurrency)

	// Budgets already consumed by committed verdicts are deducted so the
	// recovery run resumes the exact deterministic consumption sequence.
	var usedCPU, usedMem int64
	for _, c := range committed {
		usedCPU += c.CpuCost
		usedMem += c.MemCost
	}
	remCPU := opts.Budget.GlobalCPU - usedCPU
	remMem := opts.Budget.GlobalMemory - usedMem

	fresh := AssignVerdicts(pending, corr, remCPU, remMem, opts.Budget.WindowCPUMax)

	committedThisRun := 0
	for _, c := range fresh {
		if err := store.SaveVerdict(c); err != nil {
			return nil, err
		}
		committedThisRun++
		if opts.CrashAfter >= 0 && committedThisRun >= opts.CrashAfter {
			return nil, ErrSimulatedCrash
		}
	}

	all := make(map[string]Conclusion, len(committed)+len(fresh))
	for id, c := range committed {
		all[id] = c
	}
	for _, c := range fresh {
		all[c.WindowID] = c
	}

	verdictOf := make(map[string]Verdict, len(all))
	for id, c := range all {
		verdictOf[id] = c.Verdict
	}
	conflicts := DetectConflicts(valid, verdictOf)

	conclusions := make([]Conclusion, 0, len(all)+len(malformed))
	for _, c := range all {
		conclusions = append(conclusions, c)
	}
	conclusions = append(conclusions, malformed...)
	sort.Slice(conclusions, func(i, j int) bool { return conclusions[i].WindowID < conclusions[j].WindowID })

	fp := opts.Budget.Fingerprint()
	content, err := json.Marshal(struct {
		Conclusions []Conclusion   `json:"conclusions"`
		Conflicts   []ConflictItem `json:"conflicts"`
		ParamsFP    string         `json:"params_fingerprint"`
	}{conclusions, conflicts, fp})
	if err != nil {
		return nil, err
	}

	snap, err := store.CommitSnapshot(fp, string(content))
	if err != nil {
		return nil, err
	}

	if opts.InjectSnapshotGap {
		if err := store.InsertSnapshotRow(snap.Version+2, "00", "00", fp, `{"injected":true}`); err != nil {
			return nil, err
		}
		chain, brokenNow, err := store.LoadSnapshots()
		if err != nil {
			return nil, err
		}
		if brokenNow {
			snapCodes = append(snapCodes, CodeSnapshotVersion)
			snap = chain[len(chain)-1]
		}
	}

	report := buildReport(opts, snap, snapCodes, queue, conclusions, conflicts)
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	return &Outcome{Report: report, ReportJSON: data, Snapshot: snap}, nil
}
