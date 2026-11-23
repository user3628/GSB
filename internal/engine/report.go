package engine

// Report is the deterministic JSON export. It contains no wall-clock time;
// the only time field is the fixed sampling marker sample_tick.
type Report struct {
	Meta         Meta           `json:"meta"`
	Snapshot     SnapshotInfo   `json:"snapshot"`
	Queue        QueueStats     `json:"queue"`
	Windows      []WindowReport `json:"windows"`
	Conflicts    []ConflictItem `json:"conflicts"`
	CodeSequence []string       `json:"code_sequence"`
}

type Meta struct {
	Engine            string `json:"engine"`
	SampleTick        string `json:"sample_tick"`
	Seed              int64  `json:"seed"`
	ParamsFingerprint string `json:"params_fingerprint"`
}

type SnapshotInfo struct {
	Version           int      `json:"version"`
	Hash              string   `json:"hash"`
	ParamsFingerprint string   `json:"params_fingerprint"`
	Codes             []string `json:"codes"`
}

type WindowReport struct {
	ID      string   `json:"id"`
	Verdict Verdict  `json:"verdict"`
	Codes   []string `json:"codes"`
	Reason  string   `json:"reason,omitempty"`
	Chains  int      `json:"chains"`
	Depth   int      `json:"depth,omitempty"`
	Orphans []string `json:"orphans,omitempty"`
	Digest  string   `json:"digest,omitempty"`
}

func buildReport(opts Options, snap Snapshot, snapCodes []string, queue QueueStats, conclusions []Conclusion, conflicts []ConflictItem) Report {
	windows := make([]WindowReport, 0, len(conclusions))
	sequence := make([]string, 0, len(conclusions))
	for _, c := range conclusions {
		codes := c.Codes
		if codes == nil {
			codes = []string{}
		}
		windows = append(windows, WindowReport{
			ID:      c.WindowID,
			Verdict: c.Verdict,
			Codes:   codes,
			Reason:  c.Reason,
			Chains:  len(c.Chains),
			Depth:   c.Depth,
			Orphans: c.Orphans,
			Digest:  c.Digest,
		})
		sequence = append(sequence, codes...)
	}
	sequence = append(sequence, snapCodes...)
	if snapCodes == nil {
		snapCodes = []string{}
	}
	if conflicts == nil {
		conflicts = []ConflictItem{}
	}
	return Report{
		Meta: Meta{
			Engine:            "freplay",
			SampleTick:        "T0",
			Seed:              opts.Seed,
			ParamsFingerprint: opts.Budget.Fingerprint(),
		},
		Snapshot: SnapshotInfo{
			Version:           snap.Version,
			Hash:              snap.Hash,
			ParamsFingerprint: snap.ParamsFP,
			Codes:             snapCodes,
		},
		Queue:        queue,
		Windows:      windows,
		Conflicts:    conflicts,
		CodeSequence: sequence,
	}
}
