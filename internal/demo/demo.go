package demo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"

	"maskrevision/internal/generator"
	"maskrevision/internal/model"
	"maskrevision/internal/replay"
	"maskrevision/internal/revlog"
	"maskrevision/internal/store"
	"maskrevision/internal/workspace"
)

type Options struct {
	Work    string
	Seed    int64
	Exe     string
	Workers int
}

type Report struct {
	SchemaVersion   string                   `json:"schema_version"`
	Seed            int64                    `json:"seed"`
	GeneratedAt     string                   `json:"generated_at_utc"`
	WorkspaceDigest string                   `json:"workspace_digest"`
	ReopenedDigest  string                   `json:"reopened_digest"`
	Shards          []workspace.ShardSummary `json:"shards"`
	SnapshotProbe   SnapshotProbe            `json:"snapshot_probe"`
	DiffProbe       DiffProbe                `json:"diff_probe"`
	V1Upgrade       UpgradeProbe             `json:"v1_upgrade"`
	Unrecoverable   revlog.UpgradeReport     `json:"unrecoverable_v1_fixture"`
	Fixtures        map[string]FixtureProbe  `json:"reconcile_fixtures"`
	Recovery        RecoveryProbe            `json:"crash_recovery"`
	PublishRace     PublishRaceProbe         `json:"publish_race"`
	SummaryDigest   string                   `json:"summary_digest"`
}

type SnapshotProbe struct {
	ShardID            string `json:"shard_id"`
	Revision           int    `json:"revision"`
	SnapshotDigest     string `json:"snapshot_digest"`
	PixelDigest        string `json:"pixel_digest"`
	SegmentTableDigest string `json:"segment_table_digest"`
	SegmentCount       int    `json:"segment_count"`
}

type DiffProbe struct {
	ShardID              string             `json:"shard_id"`
	From                 int                `json:"from_revision"`
	To                   int                `json:"to_revision"`
	DifferencePixelCount int                `json:"difference_pixel_count"`
	AffectedSegments     int                `json:"affected_segment_count"`
	FirstCauseRevision   int                `json:"first_cause_revision"`
	FirstCauseAction     string             `json:"first_cause_action"`
	SamplePixels         []replay.DiffPixel `json:"sample_pixels"`
}

type UpgradeProbe struct {
	ShardID            string                    `json:"shard_id"`
	RevisionCount      int                       `json:"revision_count"`
	TraceEqual         bool                      `json:"trace_equal"`
	V1TraceDigest      string                    `json:"v1_trace_digest"`
	V2TraceDigest      string                    `json:"v2_trace_digest"`
	UnrecoverableCount int                       `json:"unrecoverable_count"`
	Inferences         []revlog.UpgradeInference `json:"inferences"`
}

type FixtureProbe struct {
	Kind                string               `json:"kind"`
	EarliestAffected    int                  `json:"earliest_affected_revision"`
	ReusableRevision    int                  `json:"reusable_revision"`
	ReplayErrorCode     string               `json:"replay_error_code,omitempty"`
	ForkDecisions       []model.ForkDecision `json:"fork_decisions,omitempty"`
	RevisionsReplayed   int                  `json:"revisions_replayed"`
	FinalSnapshotDigest string               `json:"final_snapshot_digest,omitempty"`
}

type RecoveryProbe struct {
	CrashedAfter      int    `json:"crashed_after_committed_checkpoints"`
	Killed            bool   `json:"process_killed"`
	RecoveredDigest   string `json:"recovered_digest"`
	EqualToContinuous bool   `json:"equal_to_continuous"`
}

type WorkerResult struct {
	Worker         string `json:"worker"`
	Accepted       bool   `json:"accepted"`
	Code           string `json:"code"`
	Revision       int    `json:"revision"`
	SnapshotDigest string `json:"snapshot_digest,omitempty"`
}

type PublishRaceProbe struct {
	Workers             int      `json:"workers"`
	SuccessCount        int      `json:"success_count"`
	StaleCount          int      `json:"stale_count"`
	FinalRevision       int      `json:"final_revision"`
	FinalSnapshotDigest string   `json:"final_snapshot_digest"`
	Codes               []string `json:"codes_sorted"`
	StaleProbeCodes     []string `json:"stale_probe_codes"`
}

type RaceOutcome struct {
	Probe  PublishRaceProbe
	Status []WorkerResult
}

func Run(opts Options) (*Report, string, error) {
	if opts.Seed == 0 {
		opts.Seed = generator.DefaultSeed
	}
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.Exe == "" {
		exe, err := os.Executable()
		if err != nil {
			return nil, "", err
		}
		opts.Exe = exe
	}
	layout, err := workspace.Prepare(opts.Work, true)
	if err != nil {
		return nil, "", err
	}
	if _, err := generator.Generate(layout.Data, opts.Seed); err != nil {
		return nil, "", err
	}
	ws, err := workspace.Open(layout.Root, workspace.Options{})
	if err != nil {
		return nil, "", err
	}
	mainDigest := ws.Digest()
	summaries, err := ws.ShardsSummary()
	if err != nil {
		_ = ws.Close()
		return nil, "", err
	}
	snap, be := ws.Snapshot("shard-00", 4)
	if be != nil {
		_ = ws.Close()
		return nil, "", be
	}
	diff, be := ws.Diff("shard-00", 1, 4)
	if be != nil {
		_ = ws.Close()
		return nil, "", be
	}
	upgrade, be := ws.ExportAndVerifyV1("shard-01")
	if be != nil {
		_ = ws.Close()
		return nil, "", be
	}
	unrecoverable, be := ws.UnrecoverableFixture()
	if be != nil {
		_ = ws.Close()
		return nil, "", be
	}
	fixtures := map[string]FixtureProbe{}
	for name, path := range map[string]string{
		"fork": "fixtures/shard-00.fork.json", "edited": "fixtures/shard-00.edited.json",
		"rollback": "fixtures/shard-00.rollback.json",
	} {
		f, be := ws.ClassifyFixture("shard-00", path)
		if be != nil {
			_ = ws.Close()
			return nil, "", be
		}
		fixtures[name] = FixtureProbe{
			Kind: f.Result.Kind, EarliestAffected: f.Result.EarliestAffectedRevision,
			ReusableRevision: f.Result.ReusableRevision, ReplayErrorCode: f.ReplayErrorCode,
			ForkDecisions: f.ForkDecisions, RevisionsReplayed: f.RevisionsReplayed,
			FinalSnapshotDigest: f.FinalSnapshotDigest,
		}
	}
	race, err := runPublishRace(ws, layout.Root, opts.Exe, opts.Workers)
	if err != nil {
		_ = ws.Close()
		return nil, "", err
	}
	recovery, err := runRecoveryProbe(layout.Root, opts.Exe, opts.Seed, mainDigest)
	if err != nil {
		_ = ws.Close()
		return nil, "", err
	}
	if err := ws.Close(); err != nil {
		return nil, "", err
	}
	reopened, err := workspace.Open(layout.Root, workspace.Options{})
	if err != nil {
		return nil, "", err
	}
	reopenedDigest := reopened.Digest()
	if err := reopened.Close(); err != nil {
		return nil, "", err
	}
	if reopenedDigest != mainDigest {
		return nil, "", fmt.Errorf("reopen changed workspace digest: %s != %s", mainDigest, reopenedDigest)
	}
	report := &Report{
		SchemaVersion: "maskrevision-demo-v1", Seed: opts.Seed, GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		WorkspaceDigest: mainDigest, ReopenedDigest: reopenedDigest, Shards: summaries,
		SnapshotProbe: SnapshotProbe{
			ShardID: snap.ShardID, Revision: snap.Revision, SnapshotDigest: snap.SnapshotDigest,
			PixelDigest: snap.PixelDigest, SegmentTableDigest: snap.SegmentDigest, SegmentCount: len(snap.Segments),
		},
		DiffProbe: DiffProbe{
			ShardID: diff.ShardID, From: diff.FromRevision, To: diff.ToRevision,
			DifferencePixelCount: diff.DifferencePixelCount, AffectedSegments: len(diff.AffectedSegments),
			FirstCauseRevision: causeRevision(diff.FirstCause), FirstCauseAction: causeAction(diff.FirstCause),
			SamplePixels: firstPixels(diff.Pixels, 8),
		},
		V1Upgrade: UpgradeProbe{
			ShardID: upgrade.ShardID, RevisionCount: upgrade.RevisionCount, TraceEqual: upgrade.TraceEqual,
			V1TraceDigest: upgrade.V1TraceDigest, V2TraceDigest: upgrade.V2TraceDigest,
			UnrecoverableCount: upgrade.UnrecoverableCount, Inferences: upgrade.Inferences,
		},
		Unrecoverable: unrecoverable, Fixtures: fixtures, Recovery: recovery, PublishRace: race.Probe,
	}
	report.SummaryDigest = stableDigest(report)
	reportPath := filepath.Join(layout.Root, "report.json")
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, "", err
	}
	data = append(data, '\n')
	if err := os.WriteFile(reportPath, data, 0o644); err != nil {
		return nil, "", err
	}
	return report, reportPath, nil
}

func runPublishRace(ws *workspace.Workspace, root, exe string, workers int) (RaceOutcome, error) {
	if err := ws.Store.SeedPublication("shard-00", 1); err != nil {
		return RaceOutcome{}, err
	}
	type item struct {
		index  int
		cmd    *exec.Cmd
		stdout bytes.Buffer
		stderr bytes.Buffer
	}
	items := make([]*item, workers)
	for i := 0; i < workers; i++ {
		it := &item{index: i}
		it.cmd = exec.Command(exe, "publish-worker",
			"--work", root, "--shard", "shard-00", "--expected", "1", "--revision", "2",
			"--publisher", fmt.Sprintf("worker-%02d", i))
		it.cmd.Stdout = &it.stdout
		it.cmd.Stderr = &it.stderr
		items[i] = it
	}
	for _, it := range items {
		if err := it.cmd.Start(); err != nil {
			return RaceOutcome{}, err
		}
	}
	statuses := make([]WorkerResult, 0, workers)
	for _, it := range items {
		waitErr := it.cmd.Wait()
		var res WorkerResult
		if err := json.Unmarshal(it.stdout.Bytes(), &res); err != nil {
			return RaceOutcome{}, fmt.Errorf("worker %d returned non-JSON output %q (stderr=%q, wait=%v)", it.index, it.stdout.String(), it.stderr.String(), waitErr)
		}
		statuses = append(statuses, res)
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].Worker < statuses[j].Worker })
	success, stale := 0, 0
	codes := make([]string, 0, workers)
	for _, st := range statuses {
		codes = append(codes, st.Code)
		if st.Accepted {
			success++
		} else if st.Code == "E_STALE_CHECKPOINT" {
			stale++
		}
	}
	sort.Strings(codes)
	state, ok, err := ws.Store.PublicationState("shard-00")
	if err != nil || !ok {
		return RaceOutcome{}, fmt.Errorf("read publication state: %w", err)
	}
	if success != 1 || stale != workers-1 || state.Revision != 2 {
		return RaceOutcome{}, fmt.Errorf("unexpected publish race outcome: success=%d stale=%d state=%+v statuses=%+v", success, stale, state, statuses)
	}
	probe := PublishRaceProbe{
		Workers: workers, SuccessCount: success, StaleCount: stale, FinalRevision: state.Revision,
		FinalSnapshotDigest: state.SnapshotDigest, Codes: codes,
	}
	lockPath := store.PublishLockPath(root, "shard-00")
	// Duplicate, non-contiguous/stale expectation, and a snapshot borrowed from
	// another shard must all fail through the same business code.
	staleCodes := []string{}
	other, ok, err := ws.Store.At("shard-01", 2)
	if err != nil || !ok {
		return RaceOutcome{}, fmt.Errorf("read cross-shard checkpoint: %w", err)
	}
	probes := []store.PublishRequest{
		{ShardID: "shard-00", Revision: 2, ExpectedRevision: 1, Publisher: "probe-stale"},
		{ShardID: "shard-00", Revision: 2, ExpectedRevision: 2, Publisher: "probe-duplicate"},
		{ShardID: "shard-00", Revision: 3, ExpectedRevision: 2, Publisher: "probe-cross-shard", SnapshotDigest: other.SnapshotDigest},
	}
	for _, req := range probes {
		_, be := ws.Store.Publish(lockPath, req)
		if be == nil {
			return RaceOutcome{}, fmt.Errorf("probe %s unexpectedly succeeded", req.Publisher)
		}
		staleCodes = append(staleCodes, be.Code)
	}
	probe.StaleProbeCodes = staleCodes
	return RaceOutcome{Probe: probe, Status: statuses}, nil
}

func runRecoveryProbe(mainRoot, exe string, seed int64, continuousDigest string) (RecoveryProbe, error) {
	recoveryRoot := filepath.Join(mainRoot, "recovery")
	layout, err := workspace.Prepare(recoveryRoot, true)
	if err != nil {
		return RecoveryProbe{}, err
	}
	if _, err := generator.Generate(layout.Data, seed); err != nil {
		return RecoveryProbe{}, err
	}
	cmd := exec.Command(exe, "replay", "--work", recoveryRoot, "--crash-after", "3", "--hard-kill")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	runErr := cmd.Run()
	if runErr == nil {
		return RecoveryProbe{}, fmt.Errorf("crash child exited successfully; expected forced termination")
	}
	ws, err := workspace.Open(recoveryRoot, workspace.Options{})
	if err != nil {
		return RecoveryProbe{}, fmt.Errorf("recover workspace after kill: %w (child output=%q)", err, out.String())
	}
	defer ws.Close()
	digest := ws.Digest()
	return RecoveryProbe{
		CrashedAfter: 3, Killed: true, RecoveredDigest: digest, EqualToContinuous: digest == continuousDigest,
	}, nil
}

func stableDigest(report *Report) string {
	cp := *report
	cp.GeneratedAt = ""
	cp.SummaryDigest = ""
	data, _ := json.Marshal(cp)
	return model.SHA256Hex(data)
}

func firstPixels(in []replay.DiffPixel, n int) []replay.DiffPixel {
	if len(in) > n {
		in = in[:n]
	}
	out := make([]replay.DiffPixel, len(in))
	copy(out, in)
	return out
}

func causeRevision(c *replay.Cause) int {
	if c == nil {
		return 0
	}
	return c.Revision
}

func causeAction(c *replay.Cause) string {
	if c == nil {
		return ""
	}
	return c.Action
}
