package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"abeval"
)

type demoScenarioResult struct {
	Scenario     string `json:"scenario"`
	BatchID      string `json:"batch_id"`
	Database     string `json:"database"`
	Report       string `json:"report"`
	Decision     string `json:"decision"`
	DecisionCode string `json:"decision_code"`
	SHA256       string `json:"sha256"`
}

type demoRecoveryResult struct {
	ContinuousReport string `json:"continuous_report"`
	RestartedReport  string `json:"restarted_report"`
	ForceReport      string `json:"force_restart_report"`
	ByteIdentical    bool   `json:"byte_identical"`
	SHA256           string `json:"sha256"`
}

type demoRaceResult struct {
	Writers   int   `json:"writers"`
	Committed int64 `json:"committed"`
	Stale     int64 `json:"stale"`
}

type demoSummary struct {
	OK                 bool                 `json:"ok"`
	Scenarios          []demoScenarioResult `json:"scenarios"`
	Recovery           demoRecoveryResult   `json:"recovery"`
	VersionReplacement map[string]any       `json:"version_replacement"`
	VersionRollback    *abeval.Error        `json:"version_rollback_rejection"`
	Race               demoRaceResult       `json:"race"`
}

func runDemo(ctx context.Context, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("demo", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	outDir := flags.String("out-dir", filepath.Join("artifacts", "demo"), "demo output directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return err
	}

	summary := demoSummary{OK: true, Scenarios: make([]demoScenarioResult, 0, 4)}
	for _, scenario := range []string{abeval.ScenarioRollout, abeval.ScenarioInsufficient, abeval.ScenarioRollback} {
		result, err := runDemoScenario(ctx, *outDir, scenario)
		if err != nil {
			return err
		}
		summary.Scenarios = append(summary.Scenarios, result)
	}

	recovery, err := runDemoRecovery(ctx, *outDir)
	if err != nil {
		return err
	}
	summary.Recovery = recovery
	if !recovery.ByteIdentical {
		summary.OK = false
	}

	replacement, rollbackError, err := runDemoVersioning(ctx, *outDir)
	if err != nil {
		return err
	}
	summary.VersionReplacement = replacement
	summary.VersionRollback = rollbackError

	race, err := runDemoRace(ctx, *outDir)
	if err != nil {
		return err
	}
	summary.Race = race
	if race.Committed != 1 || race.Stale != int64(race.Writers-1) {
		summary.OK = false
	}

	summaryJSON, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	summaryJSON = append(summaryJSON, '\n')
	if err := os.WriteFile(filepath.Join(*outDir, "summary.json"), summaryJSON, 0o644); err != nil {
		return err
	}
	_, err = stdout.Write(summaryJSON)
	return err
}

func runDemoScenario(ctx context.Context, outDir, scenario string) (demoScenarioResult, error) {
	batchID := "batch-" + scenario
	databasePath := filepath.Join(outDir, scenario+".db")
	reportPath := filepath.Join(outDir, scenario+".report.json")
	removeSQLiteFiles(databasePath)

	dataset := abeval.DefaultDataset(scenario)
	dataset.BatchID = batchID
	store, err := abeval.OpenStore(databasePath)
	if err != nil {
		return demoScenarioResult{}, err
	}
	if _, err := store.ImportDataset(ctx, batchID, dataset); err != nil {
		_ = store.Close()
		return demoScenarioResult{}, err
	}
	report, err := abeval.RunBatch(ctx, store, batchID, abeval.RunOptions{WriterID: "demo-" + scenario})
	if err != nil {
		_ = store.Close()
		return demoScenarioResult{}, err
	}
	reportBytes, err := store.ExportReport(ctx, batchID)
	if err != nil {
		_ = store.Close()
		return demoScenarioResult{}, err
	}
	if err := os.WriteFile(reportPath, reportBytes, 0o644); err != nil {
		_ = store.Close()
		return demoScenarioResult{}, err
	}
	if err := store.Close(); err != nil {
		return demoScenarioResult{}, err
	}
	return demoScenarioResult{
		Scenario: scenario, BatchID: batchID, Database: filepath.Base(databasePath),
		Report: filepath.Base(reportPath), Decision: report.Decision,
		DecisionCode: report.DecisionCode, SHA256: abeval.SHA256Hex(reportBytes),
	}, nil
}

func runDemoRecovery(ctx context.Context, outDir string) (demoRecoveryResult, error) {
	dataset := abeval.DefaultDataset(abeval.ScenarioRecovery)
	batchID := "batch-recovery-demo"
	dataset.BatchID = batchID

	continuousPath := filepath.Join(outDir, "recovery-continuous.db")
	continuousReportPath := filepath.Join(outDir, "recovery-continuous.report.json")
	removeSQLiteFiles(continuousPath)
	continuousStore, err := abeval.OpenStore(continuousPath)
	if err != nil {
		return demoRecoveryResult{}, err
	}
	if _, err := continuousStore.ImportDataset(ctx, batchID, dataset); err != nil {
		return demoRecoveryResult{}, err
	}
	if _, err := abeval.RunBatch(ctx, continuousStore, batchID, abeval.RunOptions{WriterID: "continuous"}); err != nil {
		return demoRecoveryResult{}, err
	}
	continuousBytes, err := continuousStore.ExportReport(ctx, batchID)
	if err != nil {
		return demoRecoveryResult{}, err
	}
	if err := os.WriteFile(continuousReportPath, continuousBytes, 0o644); err != nil {
		return demoRecoveryResult{}, err
	}
	if err := continuousStore.Close(); err != nil {
		return demoRecoveryResult{}, err
	}

	interruptedPath := filepath.Join(outDir, "recovery-interrupted.db")
	restartedReportPath := filepath.Join(outDir, "recovery-restarted.report.json")
	removeSQLiteFiles(interruptedPath)
	interruptedStore, err := abeval.OpenStore(interruptedPath)
	if err != nil {
		return demoRecoveryResult{}, err
	}
	if _, err := interruptedStore.ImportDataset(ctx, batchID, dataset); err != nil {
		return demoRecoveryResult{}, err
	}
	_, err = abeval.RunBatch(ctx, interruptedStore, batchID, abeval.RunOptions{
		WriterID: "graceful", StopAfterStage: dataset.Config.StopAfterStage,
	})
	if !errors.Is(err, abeval.ErrGracefulStop) {
		return demoRecoveryResult{}, fmt.Errorf("expected graceful stop, got %w", err)
	}
	statusBytes, _ := json.MarshalIndent(map[string]any{"graceful_stopped": true, "stop_after_stage": dataset.Config.StopAfterStage}, "", "  ")
	if err := os.WriteFile(filepath.Join(outDir, "recovery-graceful-stop.json"), append(statusBytes, '\n'), 0o644); err != nil {
		return demoRecoveryResult{}, err
	}
	if err := interruptedStore.Close(); err != nil {
		return demoRecoveryResult{}, err
	}

	restartedStore, err := abeval.OpenStore(interruptedPath)
	if err != nil {
		return demoRecoveryResult{}, err
	}
	if _, err := abeval.RunBatch(ctx, restartedStore, batchID, abeval.RunOptions{WriterID: "restarted"}); err != nil {
		return demoRecoveryResult{}, err
	}
	restartedBytes, err := restartedStore.ExportReport(ctx, batchID)
	if err != nil {
		return demoRecoveryResult{}, err
	}
	if err := os.WriteFile(restartedReportPath, restartedBytes, 0o644); err != nil {
		return demoRecoveryResult{}, err
	}
	if err := restartedStore.Close(); err != nil {
		return demoRecoveryResult{}, err
	}

	forcePath := filepath.Join(outDir, "recovery-force.db")
	forceReportPath := filepath.Join(outDir, "recovery-force-restart.report.json")
	removeSQLiteFiles(forcePath)
	forceStore, err := abeval.OpenStore(forcePath)
	if err != nil {
		return demoRecoveryResult{}, err
	}
	if _, err := forceStore.ImportDataset(ctx, batchID, dataset); err != nil {
		return demoRecoveryResult{}, err
	}
	if _, err := forceStore.AdvanceStage(ctx, batchID, "committed-before-sigkill"); err != nil {
		return demoRecoveryResult{}, err
	}
	if err := forceStore.Close(); err != nil {
		return demoRecoveryResult{}, err
	}
	forceReopened, err := abeval.OpenStore(forcePath)
	if err != nil {
		return demoRecoveryResult{}, err
	}
	if _, err := abeval.RunBatch(ctx, forceReopened, batchID, abeval.RunOptions{WriterID: "force-restart"}); err != nil {
		return demoRecoveryResult{}, err
	}
	forceBytes, err := forceReopened.ExportReport(ctx, batchID)
	if err != nil {
		return demoRecoveryResult{}, err
	}
	if err := os.WriteFile(forceReportPath, forceBytes, 0o644); err != nil {
		return demoRecoveryResult{}, err
	}
	if err := forceReopened.Close(); err != nil {
		return demoRecoveryResult{}, err
	}

	identical := string(continuousBytes) == string(restartedBytes) && string(continuousBytes) == string(forceBytes)
	return demoRecoveryResult{
		ContinuousReport: filepath.Base(continuousReportPath),
		RestartedReport:  filepath.Base(restartedReportPath),
		ForceReport:      filepath.Base(forceReportPath),
		ByteIdentical:    identical,
		SHA256:           abeval.SHA256Hex(continuousBytes),
	}, nil
}

func runDemoVersioning(ctx context.Context, outDir string) (map[string]any, *abeval.Error, error) {
	batchID := "batch-version-demo"
	databasePath := filepath.Join(outDir, "versions.db")
	removeSQLiteFiles(databasePath)
	store, err := abeval.OpenStore(databasePath)
	if err != nil {
		return nil, nil, err
	}
	defer store.Close()

	v1 := abeval.DefaultDataset(abeval.ScenarioRollout)
	v1.BatchID = batchID
	if _, err := store.ImportDataset(ctx, batchID, v1); err != nil {
		return nil, nil, err
	}
	if _, err := store.AdvanceStage(ctx, batchID, "v1"); err != nil {
		return nil, nil, err
	}

	v2 := abeval.DefaultDataset(abeval.ScenarioRollback)
	v2.BatchID = batchID
	v2.Version = 2
	replacement, err := store.ImportDataset(ctx, batchID, v2)
	if err != nil {
		return nil, nil, err
	}
	replacementJSON, _ := json.MarshalIndent(replacement, "", "  ")
	if err := os.WriteFile(filepath.Join(outDir, "version-replace.json"), append(replacementJSON, '\n'), 0o644); err != nil {
		return nil, nil, err
	}

	rollbackCandidate := abeval.DefaultDataset(abeval.ScenarioRollout)
	rollbackCandidate.BatchID = batchID
	rollbackCandidate.Version = 3
	_, rollbackErr := store.ImportDataset(ctx, batchID, rollbackCandidate)
	businessError, ok := abeval.AsError(rollbackErr)
	if !ok || abeval.ErrorCode(rollbackErr) != abeval.CodeVersionRollback {
		return nil, nil, fmt.Errorf("expected version rollback error, got %w", rollbackErr)
	}
	errorJSON, _ := json.MarshalIndent(map[string]any{"ok": false, "error": businessError}, "", "  ")
	if err := os.WriteFile(filepath.Join(outDir, "version-rollback-error.json"), append(errorJSON, '\n'), 0o644); err != nil {
		return nil, nil, err
	}
	return map[string]any{
		"database": filepath.Base(databasePath), "replacement": replacement,
	}, businessError, nil
}

func runDemoRace(ctx context.Context, outDir string) (demoRaceResult, error) {
	batchID := "batch-race-demo"
	databasePath := filepath.Join(outDir, "race.db")
	removeSQLiteFiles(databasePath)
	store, err := abeval.OpenStore(databasePath)
	if err != nil {
		return demoRaceResult{}, err
	}
	defer store.Close()

	dataset := abeval.DefaultDataset(abeval.ScenarioRollout)
	dataset.BatchID = batchID
	if _, err := store.ImportDataset(ctx, batchID, dataset); err != nil {
		return demoRaceResult{}, err
	}
	metrics, err := abeval.BuildStageMetrics(dataset.Config, dataset.Events, 1)
	if err != nil {
		return demoRaceResult{}, err
	}
	verdict := abeval.EvaluateStage(metrics, dataset.Config)

	const writers = 8
	var wg sync.WaitGroup
	var committed, stale int64
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, commitErr := store.CommitStage(ctx, abeval.StageCheckpoint{
				BatchID: batchID, StageIndex: 1, Stats: metrics, Verdict: verdict,
				WriterID: fmt.Sprintf("demo-writer-%d", i),
			})
			if commitErr == nil {
				atomic.AddInt64(&committed, 1)
				return
			}
			if abeval.ErrorCode(commitErr) == abeval.CodeStaleCheckpoint {
				atomic.AddInt64(&stale, 1)
				return
			}
			_, _ = fmt.Fprintf(os.Stderr, "demo race writer %d failed: %v\n", i, commitErr)
		}(i)
	}
	wg.Wait()
	result := demoRaceResult{Writers: writers, Committed: committed, Stale: stale}
	resultJSON, _ := json.MarshalIndent(result, "", "  ")
	if err := os.WriteFile(filepath.Join(outDir, "race.json"), append(resultJSON, '\n'), 0o644); err != nil {
		return demoRaceResult{}, err
	}
	return result, nil
}

func removeSQLiteFiles(path string) {
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(path + suffix)
	}
}
