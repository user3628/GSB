package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"local/quantops/internal/force"
	"local/quantops/pkg/canon"
	"local/quantops/pkg/dataset"
	"local/quantops/pkg/qerr"
	"local/quantops/pkg/scheduler"
	"local/quantops/pkg/store"
)

func runOnce(t *testing.T, ds *dataset.Loaded, db string, limits scheduler.Limits, shards ...string) *Result {
	t.Helper()
	st, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	r, err := New(ds, st)
	if err != nil {
		t.Fatal(err)
	}
	res, err := r.Run(context.Background(), RunOptions{Limits: limits, ShardIDs: shards})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return res
}

func TestRunDeterministicCoverage(t *testing.T) {
	dir := t.TempDir()
	ds, err := dataset.Generate(filepath.Join(dir, "data"), dataset.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	limits := scheduler.DefaultLimits()
	res1 := runOnce(t, ds, filepath.Join(dir, "a.db"), limits)
	if res1.Summary.Status != "ok" {
		t.Fatalf("status = %s, errors=%v", res1.Summary.Status, res1.NumericErrors)
	}
	if len(res1.Summary.Shards) != len(ds.Manifest.Shards) {
		t.Fatalf("shards = %d", len(res1.Summary.Shards))
	}
	if res1.Concurrency != 2 {
		t.Fatalf("concurrency = %d", res1.Concurrency)
	}
	nanSeen := false
	for _, sh := range res1.Summary.Shards {
		if sh.Diagnostics.SanitizedInputCount > 0 {
			nanSeen = true
		}
	}
	if !nanSeen {
		t.Fatal("NaN anomaly was not reported in diagnostics")
	}
	// Re-running an already-complete database must produce byte-identical output.
	res2 := runOnce(t, ds, filepath.Join(dir, "a.db"), limits)
	if !bytes.Equal(res1.SummaryBytes, res2.SummaryBytes) {
		t.Fatal("rerun summary differs")
	}
	// A fresh database with the same configuration must also be byte-identical.
	res3 := runOnce(t, ds, filepath.Join(dir, "b.db"), limits)
	if !bytes.Equal(res1.SummaryBytes, res3.SummaryBytes) {
		t.Fatal("fresh-run summary differs")
	}
}

func TestHardKillRecoveryByteIdentical(t *testing.T) {
	dir := t.TempDir()
	ds, err := dataset.Generate(filepath.Join(dir, "data"), dataset.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	limits := scheduler.Limits{TotalElements: 2_000_000, PerWorkerElements: 1_000_000, PeakMemoryBytes: 128 << 20, MaxWorkers: 1}
	baseline := runOnce(t, ds, filepath.Join(dir, "continuous.db"), limits)
	crashDB := filepath.Join(dir, "crash.db")
	crashSummary := filepath.Join(dir, "crash-summary.json")

	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
	cmd.Env = append(os.Environ(),
		"QUANTOPS_TEST_HELPER=crash",
		"QUANTOPS_HELPER_DATA="+ds.ManifestPath,
		"QUANTOPS_HELPER_DB="+crashDB,
		"QUANTOPS_HELPER_CRASH_AFTER=9",
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("crash helper exited normally: %s", out)
	}
	if _, statErr := os.Stat(crashSummary); !os.IsNotExist(statErr) {
		t.Fatalf("crashed process unexpectedly left a summary: %v", statErr)
	}
	st, err := store.Open(crashDB)
	if err != nil {
		t.Fatal(err)
	}
	progress, err := st.ListProgress(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if len(progress) == 0 {
		t.Fatal("no committed checkpoints survived forced termination")
	}

	resumed := runOnce(t, ds, crashDB, limits)
	if !bytes.Equal(baseline.SummaryBytes, resumed.SummaryBytes) {
		t.Fatalf("recovered summary differs from continuous run\ncontinuous: %s\nrecovered:  %s",
			canon.HashBytes(baseline.SummaryBytes), canon.HashBytes(resumed.SummaryBytes))
	}
	if err := canon.WriteFileAtomic(crashSummary, resumed.SummaryBytes, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCrashHelper(t *testing.T) {
	if os.Getenv("QUANTOPS_TEST_HELPER") != "crash" {
		t.Skip("helper process only")
	}
	ds, err := dataset.Load(os.Getenv("QUANTOPS_HELPER_DATA"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(os.Getenv("QUANTOPS_HELPER_DB"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	r, err := New(ds, st)
	if err != nil {
		t.Fatal(err)
	}
	crashAfter := 9
	fmt.Sscanf(os.Getenv("QUANTOPS_HELPER_CRASH_AFTER"), "%d", &crashAfter)
	_, err = r.Run(context.Background(), RunOptions{
		Limits: scheduler.Limits{TotalElements: 2_000_000, PerWorkerElements: 1_000_000, PeakMemoryBytes: 128 << 20, MaxWorkers: 1},
		CommitHook: func(n int) {
			if n >= crashAfter {
				force.Terminate()
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Fatal("expected forced termination")
}

func TestConcurrentProcessesDisjointShards(t *testing.T) {
	dir := t.TempDir()
	ds, err := dataset.Generate(filepath.Join(dir, "data"), dataset.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	limits := scheduler.Limits{TotalElements: 2_000_000, PerWorkerElements: 1_000_000, PeakMemoryBytes: 128 << 20, MaxWorkers: 1}
	baseline := runOnce(t, ds, filepath.Join(dir, "baseline.db"), limits)
	sharedDB := filepath.Join(dir, "shared.db")
	var left, right []string
	for i, sh := range ds.Manifest.Shards {
		if i%2 == 0 {
			left = append(left, sh.ID)
		} else {
			right = append(right, sh.ID)
		}
	}
	start := func(ids []string) (*exec.Cmd, *bytes.Buffer) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestConcurrentHelper$")
		cmd.Env = append(os.Environ(),
			"QUANTOPS_TEST_HELPER=concurrent",
			"QUANTOPS_HELPER_DATA="+ds.ManifestPath,
			"QUANTOPS_HELPER_DB="+sharedDB,
			"QUANTOPS_HELPER_SHARDS="+strings.Join(ids, ","),
		)
		var buf bytes.Buffer
		cmd.Stdout, cmd.Stderr = &buf, &buf
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd, &buf
	}
	c1, b1 := start(left)
	c2, b2 := start(right)
	if err := c1.Wait(); err != nil {
		t.Fatalf("worker 1: %v\n%s", err, b1.String())
	}
	if err := c2.Wait(); err != nil {
		t.Fatalf("worker 2: %v\n%s", err, b2.String())
	}
	final := runOnce(t, ds, sharedDB, limits)
	if !bytes.Equal(baseline.SummaryBytes, final.SummaryBytes) {
		t.Fatalf("concurrent-process result differs from continuous run: %s vs %s",
			canon.HashBytes(baseline.SummaryBytes), canon.HashBytes(final.SummaryBytes))
	}
}

func TestConcurrentHelper(t *testing.T) {
	if os.Getenv("QUANTOPS_TEST_HELPER") != "concurrent" {
		t.Skip("helper process only")
	}
	ds, err := dataset.Load(os.Getenv("QUANTOPS_HELPER_DATA"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(os.Getenv("QUANTOPS_HELPER_DB"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	r, err := New(ds, st)
	if err != nil {
		t.Fatal(err)
	}
	ids := strings.Split(os.Getenv("QUANTOPS_HELPER_SHARDS"), ",")
	res, err := r.Run(context.Background(), RunOptions{
		Limits:   scheduler.Limits{TotalElements: 2_000_000, PerWorkerElements: 1_000_000, PeakMemoryBytes: 128 << 20, MaxWorkers: 1},
		ShardIDs: ids,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.NumericErrors) > 0 {
		t.Fatalf("numeric errors: %v", res.NumericErrors)
	}
}

func TestToleranceChangeInvalidatesOnlyVerification(t *testing.T) {
	dir := t.TempDir()
	ds, err := dataset.Generate(filepath.Join(dir, "data"), dataset.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	db := filepath.Join(dir, "checkpoint.db")
	limits := scheduler.DefaultLimits()
	runOnce(t, ds, db, limits)
	shardID := ds.Manifest.Shards[0].ID
	before := progressAndStates(t, db, shardID)
	beforeVerify := before.global[store.Verification]

	manifest := readManifest(t, ds.ManifestPath)
	manifest.Shards[0].Verification.AbsoluteTolerance += 0.25
	writeJSON(t, ds.ManifestPath, manifest)
	ds2, err := dataset.Load(ds.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	res := runOnce(t, ds2, db, limits)
	if res.Summary.Status != "ok" {
		t.Fatalf("status %s", res.Summary.Status)
	}
	after := progressAndStates(t, db, shardID)
	for _, stage := range []store.Stage{store.Calibration, store.Forward, store.Dequantization} {
		if after.global[stage] != before.global[stage] {
			t.Errorf("%s should not be republished (%d -> %d)", stage, before.global[stage], after.global[stage])
		}
	}
	if after.global[store.Verification] == beforeVerify {
		t.Error("verification was not republished after tolerance change")
	}
	if after.next != 5 {
		t.Errorf("next stage = %d", after.next)
	}
}

func TestOperatorChainChangeInvalidatesForwardOnly(t *testing.T) {
	dir := t.TempDir()
	ds, err := dataset.Generate(filepath.Join(dir, "data"), dataset.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	db := filepath.Join(dir, "checkpoint.db")
	limits := scheduler.DefaultLimits()
	runOnce(t, ds, db, limits)
	shardID := ds.Manifest.Shards[0].ID
	before := progressAndStates(t, db, shardID)
	manifest := readManifest(t, ds.ManifestPath)
	manifest.Shards[0].Chain[0].Name += "_renamed"
	writeJSON(t, ds.ManifestPath, manifest)
	ds2, err := dataset.Load(ds.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	runOnce(t, ds2, db, limits)
	after := progressAndStates(t, db, shardID)
	if after.global[store.Calibration] != before.global[store.Calibration] {
		t.Error("calibration should be retained for a chain-only change")
	}
	for _, stage := range []store.Stage{store.Forward, store.Dequantization, store.Verification} {
		if after.global[stage] == before.global[stage] {
			t.Errorf("%s should be rebuilt after operator chain change", stage)
		}
	}
}

func TestTensorChangeInvalidatesCalibrationAndDownstream(t *testing.T) {
	dir := t.TempDir()
	ds, err := dataset.Generate(filepath.Join(dir, "data"), dataset.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	db := filepath.Join(dir, "checkpoint.db")
	limits := scheduler.DefaultLimits()
	runOnce(t, ds, db, limits)
	shardID := ds.Manifest.Shards[0].ID
	before := progressAndStates(t, db, shardID)
	tfPath := filepath.Join(filepath.Dir(ds.ManifestPath), filepath.FromSlash(ds.Manifest.Shards[0].Activation.Path))
	tf, _, err := dataset.LoadTensorFile(tfPath)
	if err != nil {
		t.Fatal(err)
	}
	tf.Values[0] += 0.125
	writeJSON(t, tfPath, tf)
	ds2, err := dataset.Load(ds.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	runOnce(t, ds2, db, limits)
	after := progressAndStates(t, db, shardID)
	for _, stage := range store.Stages() {
		if after.global[stage] == before.global[stage] {
			t.Errorf("%s was not rebuilt after tensor content change", stage)
		}
	}
}

func TestNumericMismatchRetainsDiagnostics(t *testing.T) {
	dir := t.TempDir()
	ds, err := dataset.Generate(filepath.Join(dir, "data"), dataset.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	manifest := readManifest(t, ds.ManifestPath)
	manifest.Shards[0].Verification.AbsoluteTolerance = 0
	manifest.Shards[0].Verification.RelativeTolerance = 0
	writeJSON(t, ds.ManifestPath, manifest)
	ds, err = dataset.Load(ds.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	db := filepath.Join(dir, "checkpoint.db")
	st, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	r, err := New(ds, st)
	if err != nil {
		t.Fatal(err)
	}
	res, err := r.Run(context.Background(), RunOptions{Limits: scheduler.DefaultLimits()})
	if !qerr.IsCode(err, qerr.CodeNumericMismatch) {
		t.Fatalf("numeric mismatch business error = %v", err)
	}
	if res.Summary.Status != "numeric_mismatch" || len(res.NumericErrors) != 1 {
		t.Fatalf("status=%s numeric=%d", res.Summary.Status, len(res.NumericErrors))
	}
	be := res.NumericErrors[0]
	if be.Code != qerr.CodeNumericMismatch || be.ShardID == "" || be.Layer == "" || len(be.Details) == 0 {
		t.Fatalf("incomplete numeric error: %+v", be)
	}
	if len(res.Summary.Shards[0].Diagnostics.Layers) == 0 {
		t.Fatal("summary lacks layer diagnostics")
	}
	verify, err := st.State(context.Background(), ds.Manifest.Shards[0].ID, store.Verification)
	if err != nil || verify == nil {
		t.Fatalf("verification checkpoint missing: %v", err)
	}
}

func TestBudgetLimitedRunRecordsUntreated(t *testing.T) {
	dir := t.TempDir()
	ds, err := dataset.Generate(filepath.Join(dir, "data"), dataset.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	limits := scheduler.Limits{TotalElements: 100, PerWorkerElements: 100, PeakMemoryBytes: 1 << 20, MaxWorkers: 2}
	st, err := store.Open(filepath.Join(dir, "checkpoint.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	r, err := New(ds, st)
	if err != nil {
		t.Fatal(err)
	}
	res, err := r.Run(context.Background(), RunOptions{Limits: limits})
	if !qerr.IsCode(err, qerr.CodeBudgetExceeded) {
		t.Fatalf("budget business error = %v", err)
	}
	if res.Summary.Status != "budget_limited" {
		t.Fatalf("status = %s", res.Summary.Status)
	}
	if len(res.Summary.Shards) != 0 || len(res.Summary.Untreated) != len(ds.Manifest.Shards) {
		t.Fatalf("shards=%d untreated=%d", len(res.Summary.Shards), len(res.Summary.Untreated))
	}
	if len(res.BudgetErrors) != len(ds.Manifest.Shards) {
		t.Fatalf("budget errors = %d", len(res.BudgetErrors))
	}
	for _, e := range res.BudgetErrors {
		if e.Code != qerr.CodeBudgetExceeded || len(e.Details) == 0 {
			t.Fatalf("bad budget error %+v", e)
		}
	}
}

type progressView struct {
	next   int
	global map[store.Stage]int64
}

func progressAndStates(t *testing.T, db, shardID string) progressView {
	t.Helper()
	st, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	p, err := st.Progress(context.Background(), shardID)
	if err != nil || p == nil {
		t.Fatalf("progress: %v %v", p, err)
	}
	states, err := st.States(context.Background(), shardID)
	if err != nil {
		t.Fatal(err)
	}
	v := progressView{next: p.NextStageSeq, global: map[store.Stage]int64{}}
	for _, s := range states {
		v.global[s.Stage] = s.GlobalSequence
	}
	return v
}

func readManifest(t *testing.T, path string) dataset.Manifest {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var m dataset.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, '\n')
	if err := canon.WriteFileAtomic(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}
