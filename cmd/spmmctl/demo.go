package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"spmmopt/engine"
	"spmmopt/samples"
	"spmmopt/spmm"
	"spmmopt/store"
)

type childResult struct {
	ExitCode int
	Output   string
}

type publishOutcome struct {
	Worker   int         `json:"worker"`
	ExitCode int         `json:"exit_code"`
	OK       bool        `json:"ok"`
	PlanKey  string      `json:"plan_key"`
	Version  int64       `json:"version"`
	Digest   string      `json:"digest"`
	Error    *spmm.Error `json:"error,omitempty"`
}

type demoRecoveryReport struct {
	Sample                 string `json:"sample"`
	PlanKey                string `json:"plan_key"`
	PlanVersion            int64  `json:"plan_version"`
	TotalUnits             int    `json:"total_units"`
	CrashExitCode          int    `json:"crash_exit_code"`
	CommittedBeforeCrash   int    `json:"committed_units_before_crash"`
	BlockIndexesAfterCrash []int  `json:"block_indexes_after_crash"`
	BaselineDigest         string `json:"baseline_canonical_digest"`
	ResumedDigest          string `json:"resumed_canonical_digest"`
	ByteIdentical          bool   `json:"byte_identical"`
	QuerySettlementMatch   bool   `json:"query_settlement_match"`
	FinalBlockIndexes      []int  `json:"final_block_indexes"`
}

type demoGracefulReport struct {
	StoppedAfterUnits    int    `json:"stopped_after_units"`
	PartialStopped       bool   `json:"partial_stopped"`
	PartialCompleted     int    `json:"partial_completed_units"`
	PersistedState       string `json:"persisted_state"`
	ResumedDigest        string `json:"resumed_canonical_digest"`
	MatchesContinuousRun bool   `json:"matches_continuous_run"`
}

type demoConcurrencyReport struct {
	Outcomes      []publishOutcome `json:"outcomes"`
	RejectedStale bool             `json:"rejected_stale"`
	HeadVersion   int64            `json:"head_version"`
	HistoryLength int              `json:"history_length"`
}

type demoReport struct {
	Version             int                   `json:"version"`
	SampleDir           string                `json:"sample_dir"`
	DemoDir             string                `json:"demo_dir"`
	Verification        *verifyReport         `json:"verification"`
	Recovery            demoRecoveryReport    `json:"forced_termination_recovery"`
	Graceful            demoGracefulReport    `json:"graceful_stop_resume"`
	Concurrency         demoConcurrencyReport `json:"concurrent_publication"`
	AllAcceptancePassed bool                  `json:"all_acceptance_passed"`
}

func cmdDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	o := addPlanFlags(fs, false)
	sampleName := fs.String("sample", "square_p10", "recovery demonstration sample")
	outDir := fs.String("out", "artifacts/demo", "demo output directory")
	reportPath := fs.String("report", "", "demo report path (default <out>/report.json)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	o.sample = *sampleName
	ctx := context.Background()
	manifest, err := samples.GenerateAll(o.samplesDir, o.genSeed)
	if err != nil {
		return fmt.Errorf("generate samples: %w", err)
	}
	s, err := manifest.Load(o.samplesDir, o.sample)
	if err != nil {
		return err
	}
	p, err := o.selectPlan(s)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(*outDir); err != nil {
		return err
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return err
	}
	if *reportPath == "" {
		*reportPath = filepath.Join(*outDir, "report.json")
	}
	report := demoReport{Version: 1, SampleDir: o.samplesDir, DemoDir: *outDir, AllAcceptancePassed: true}

	// 1. Full local corpus dual-path verification.
	verifyOpts := *o
	verifyOpts.db = filepath.Join(*outDir, "verify.db")
	if err := resetDB(verifyOpts.db); err != nil {
		return err
	}
	verification, err := verifyCorpus(ctx, &verifyOpts, manifest.Names())
	if err != nil {
		return err
	}
	report.Verification = verification
	if !verification.AllMatch {
		report.AllAcceptancePassed = false
	}

	// 2. Continuous baseline, forced termination, restart from durable facts.
	baselineDB := filepath.Join(*outDir, "baseline.db")
	resumeDB := filepath.Join(*outDir, "crash-resume.db")
	baselineFile := filepath.Join(*outDir, "baseline-result.json")
	resumedFile := filepath.Join(*outDir, "resumed-result.json")
	for _, db := range []string{baselineDB, resumeDB} {
		if err := resetDB(db); err != nil {
			return err
		}
		rec, err := publishDemoPlan(ctx, db, p)
		if err != nil {
			return fmt.Errorf("publish recovery plan: %w", err)
		}
		if rec.Version != 1 {
			return fmt.Errorf("fresh demo database published version %d", rec.Version)
		}
	}
	if err := runDemoChild(o, baselineDB, p, "--result-file", baselineFile, "--quiet"); err != nil {
		return fmt.Errorf("continuous baseline run: %w", err)
	}
	crash := runDemoChildRaw(o, resumeDB, p, "--hard-exit-before-commit", "1", "--quiet")
	if crash.ExitCode != 137 {
		return fmt.Errorf("forced termination exit=%d output=%s", crash.ExitCode, strings.TrimSpace(crash.Output))
	}
	crashStore, err := store.Open(ctx, resumeDB)
	if err != nil {
		return err
	}
	crashDigest, err := planDigestFromDB(crashStore, spmm.PlanKey(p))
	if err != nil {
		crashStore.Close()
		return err
	}
	ref := store.PlanRef{PlanKey: spmm.PlanKey(p), Version: 1, Digest: crashDigest}
	afterCrash, err := crashStore.LoadBlocks(ctx, ref)
	if err != nil {
		crashStore.Close()
		return err
	}
	crashStore.Close()
	if len(afterCrash) == 0 || len(afterCrash) >= len(p.Memory.Rows) {
		return fmt.Errorf("forced termination left %d/%d committed units; choose a plan with more units", len(afterCrash), len(p.Memory.Rows))
	}
	if err := runDemoChild(o, resumeDB, p, "--result-file", resumedFile, "--quiet"); err != nil {
		return fmt.Errorf("resume run: %w", err)
	}
	baselineBytes, err := os.ReadFile(baselineFile)
	if err != nil {
		return err
	}
	resumedBytes, err := os.ReadFile(resumedFile)
	if err != nil {
		return err
	}
	baselineResult, err := readDemoResult(baselineFile)
	if err != nil {
		return err
	}
	resumedResult, err := readDemoResult(resumedFile)
	if err != nil {
		return err
	}
	queryMatch, err := verifyResumedPaths(ctx, resumeDB, s, ref)
	if err != nil {
		return err
	}
	finalStore, err := store.Open(ctx, resumeDB)
	if err != nil {
		return err
	}
	finalBlocks, err := finalStore.LoadBlocks(ctx, ref)
	finalStore.Close()
	if err != nil {
		return err
	}
	finalIndexes := make([]int, len(finalBlocks))
	for i := range finalBlocks {
		finalIndexes[i] = finalBlocks[i].ExecutionRowIndex
	}
	crashIndexes := make([]int, len(afterCrash))
	for i := range afterCrash {
		crashIndexes[i] = afterCrash[i].ExecutionRowIndex
	}
	byteIdentical := bytes.Equal(baselineBytes, resumedBytes)
	report.Recovery = demoRecoveryReport{
		Sample: s.Name, PlanKey: ref.PlanKey, PlanVersion: ref.Version, TotalUnits: len(p.Memory.Rows),
		CrashExitCode: crash.ExitCode, CommittedBeforeCrash: len(afterCrash),
		BlockIndexesAfterCrash: crashIndexes, BaselineDigest: baselineResult.CanonicalDigest,
		ResumedDigest: resumedResult.CanonicalDigest, ByteIdentical: byteIdentical,
		QuerySettlementMatch: queryMatch, FinalBlockIndexes: finalIndexes,
	}
	if !byteIdentical || !queryMatch {
		report.AllAcceptancePassed = false
	}

	// 3. Graceful stop after the current unit, then restart.
	gracefulDB := filepath.Join(*outDir, "graceful-resume.db")
	partialFile := filepath.Join(*outDir, "graceful-partial.json")
	gracefulFinalFile := filepath.Join(*outDir, "graceful-resumed-result.json")
	if err := resetDB(gracefulDB); err != nil {
		return err
	}
	if _, err := publishDemoPlan(ctx, gracefulDB, p); err != nil {
		return err
	}
	if err := runDemoChild(o, gracefulDB, p, "--graceful-after", "1", "--result-file", partialFile, "--quiet"); err != nil {
		return fmt.Errorf("graceful stop run: %w", err)
	}
	partial, err := readDemoResult(partialFile)
	if err != nil {
		return err
	}
	gracefulStore, err := store.Open(ctx, gracefulDB)
	if err != nil {
		return err
	}
	state, stateErr := gracefulStore.State(ctx, ref.PlanKey)
	gracefulStore.Close()
	if stateErr != nil {
		return stateErr
	}
	if err := runDemoChild(o, gracefulDB, p, "--result-file", gracefulFinalFile, "--quiet"); err != nil {
		return fmt.Errorf("graceful resume run: %w", err)
	}
	gracefulBytes, err := os.ReadFile(gracefulFinalFile)
	if err != nil {
		return err
	}
	gracefulResult, err := readDemoResult(gracefulFinalFile)
	if err != nil {
		return err
	}
	report.Graceful = demoGracefulReport{
		StoppedAfterUnits: 1, PartialStopped: partial.Stopped,
		PartialCompleted: partial.CompletedUnits, PersistedState: state.State,
		ResumedDigest:        gracefulResult.CanonicalDigest,
		MatchesContinuousRun: bytes.Equal(baselineBytes, gracefulBytes),
	}
	if !partial.Stopped || partial.CompletedUnits != 1 || state.State != "stopped" || !report.Graceful.MatchesContinuousRun {
		report.AllAcceptancePassed = false
	}

	// 4. Two real worker processes publish against the same head.
	concurrencyDB := filepath.Join(*outDir, "concurrency.db")
	if err := resetDB(concurrencyDB); err != nil {
		return err
	}
	initStore, err := store.Open(ctx, concurrencyDB)
	if err != nil {
		return err
	}
	initStore.Close()
	concReport, err := runConcurrentPublishDemo(o, p, concurrencyDB, *outDir)
	if err != nil {
		return err
	}
	report.Concurrency = *concReport
	if !concReport.RejectedStale {
		report.AllAcceptancePassed = false
	}

	if err := writeJSON(*reportPath, report); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
	return nil
}

func publishDemoPlan(ctx context.Context, db string, p *spmm.Plan) (*store.PlanRecord, error) {
	st, err := store.Open(ctx, db)
	if err != nil {
		return nil, err
	}
	defer st.Close()
	return st.PublishPlan(ctx, p, store.PlanExpectation{})
}

func planDigestFromDB(st *store.Store, key string) (string, error) {
	head, err := st.Head(context.Background(), key)
	if err != nil {
		return "", err
	}
	return head.Digest, nil
}

func runDemoChild(o *planOptions, db string, p *spmm.Plan, extra ...string) error {
	res := runDemoChildRaw(o, db, p, extra...)
	if res.ExitCode != 0 {
		return fmt.Errorf("exit=%d output=%s", res.ExitCode, strings.TrimSpace(res.Output))
	}
	return nil
}

func runDemoChildRaw(o *planOptions, db string, p *spmm.Plan, extra ...string) childResult {
	exe, err := os.Executable()
	if err != nil {
		return childResult{ExitCode: -1, Output: err.Error()}
	}
	args := []string{
		"run", "--samples", o.samplesDir, "--sample", o.sample, "--db", db,
		"--plan-key", spmm.PlanKey(p),
	}
	args = append(args, extra...)
	cmd := exec.Command(exe, args...)
	b, err := cmd.CombinedOutput()
	res := childResult{Output: string(b)}
	if err == nil {
		return res
	}
	if ee, ok := err.(*exec.ExitError); ok {
		res.ExitCode = ee.ExitCode()
		return res
	}
	res.ExitCode = -1
	res.Output += " " + err.Error()
	return res
}

func readDemoResult(path string) (*engine.Result, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r engine.Result
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func verifyResumedPaths(ctx context.Context, db string, s *samples.Sample, ref store.PlanRef) (bool, error) {
	st, err := store.Open(ctx, db)
	if err != nil {
		return false, err
	}
	defer st.Close()
	en, err := engine.Open(ctx, st, s.CSR, s.Dense, ref.PlanKey)
	if err != nil {
		return false, err
	}
	query, err := en.Query(ctx, engine.QueryOptions{RowStart: 0, RowEnd: s.CSR.Rows, LatencyBudget: 30_000_000_000})
	if err != nil {
		return false, err
	}
	settlement, err := en.Settle(ctx, engine.RunOptions{Persist: true})
	if err != nil {
		return false, err
	}
	if err := engine.ComparePaths(query, settlement); err != nil {
		return false, err
	}
	return query.CanonicalDigest == settlement.CanonicalDigest, nil
}

func runConcurrentPublishDemo(o *planOptions, p *spmm.Plan, db, outDir string) (*demoConcurrencyReport, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	type running struct {
		cmd    *exec.Cmd
		path   string
		buffer bytes.Buffer
		err    error
	}
	workers := make([]*running, 2)
	for i := range workers {
		path := filepath.Join(outDir, fmt.Sprintf("publish-worker-%d.json", i+1))
		args := []string{
			"publish-once", "--samples", o.samplesDir, "--sample", o.sample, "--db", db,
			"--expect-version", "0", "--expect-digest", "", "--delay-ms", "20",
			"--result-file", path, "--row-block", fmt.Sprint(o.rowBlock), "--col-block", fmt.Sprint(o.colBlock),
			"--sum", o.sum, "--order", o.order, "--peak-bytes", fmt.Sprint(o.peakBytes),
			"--cache-bytes", fmt.Sprint(o.cacheBytes), "--skip-zeros=" + fmt.Sprint(o.skipZeros),
			"--error-seed", fmt.Sprint(o.errorSeed), "--error-samples", fmt.Sprint(o.errorSamples),
			"--abs-tol", fmt.Sprint(o.absTol), "--rel-tol", fmt.Sprint(o.relTol),
			"--interactive-budget-ms", fmt.Sprint(o.interactiveMS),
		}
		cmd := exec.Command(exe, args...)
		w := &running{cmd: cmd, path: path}
		cmd.Stdout = &w.buffer
		cmd.Stderr = &w.buffer
		workers[i] = w
		if err := cmd.Start(); err != nil {
			return nil, err
		}
	}
	for _, w := range workers {
		w.err = w.cmd.Wait()
	}
	report := &demoConcurrencyReport{}
	for i, w := range workers {
		var payload struct {
			OK      bool        `json:"ok"`
			PlanKey string      `json:"plan_key"`
			Version int64       `json:"version"`
			Digest  string      `json:"digest"`
			Error   *spmm.Error `json:"error"`
		}
		b, readErr := os.ReadFile(w.path)
		if readErr != nil {
			return nil, readErr
		}
		if err := json.Unmarshal(b, &payload); err != nil {
			return nil, err
		}
		code := 0
		if w.err != nil {
			if ee, ok := w.err.(*exec.ExitError); ok {
				code = ee.ExitCode()
			} else {
				code = -1
			}
		}
		report.Outcomes = append(report.Outcomes, publishOutcome{
			Worker: i + 1, ExitCode: code, OK: payload.OK, PlanKey: payload.PlanKey,
			Version: payload.Version, Digest: payload.Digest, Error: payload.Error,
		})
		if payload.Error != nil && payload.Error.Code == spmm.CodeStalePublish {
			report.RejectedStale = true
		}
	}
	sort.Slice(report.Outcomes, func(i, j int) bool { return report.Outcomes[i].Worker < report.Outcomes[j].Worker })
	ctx := context.Background()
	st, err := store.Open(ctx, db)
	if err != nil {
		return nil, err
	}
	defer st.Close()
	history, err := st.History(ctx, spmm.PlanKey(p))
	if err != nil {
		return nil, err
	}
	report.HistoryLength = len(history)
	if len(history) > 0 {
		report.HeadVersion = history[len(history)-1].Version
	}
	if !report.RejectedStale || report.HeadVersion != 1 || report.HistoryLength != 1 {
		return report, fmt.Errorf("concurrency protection did not reject exactly one stale publisher")
	}
	return report, nil
}
