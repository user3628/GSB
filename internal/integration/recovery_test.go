package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"replicationlag/internal/apperr"
	"replicationlag/internal/compat"
	"replicationlag/internal/domain"
	"replicationlag/internal/engine"
	"replicationlag/internal/samplegen"
	"replicationlag/internal/store"
)

// TestForcedTerminationAndRecovery runs a real child test process that exits
// with code 137 immediately after its durable intent is written. The parent
// reopens the same SQLite file and verifies fact-based recovery.
func TestForcedTerminationAndRecovery(t *testing.T) {
	if os.Getenv("LAGENGINE_RECOVERY_CHILD") == "1" {
		runCrashChild()
		return
	}
	ctx := context.Background()
	dir := t.TempDir()
	if err := samplegen.Generate(dir); err != nil {
		t.Fatal(err)
	}
	matrix, err := compat.Load(filepath.Join(dir, "compat.json"))
	if err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(dir, "recovery.db")
	database, err := store.Open(ctx, dbPath, store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	engineInstance, err := engine.New(database, matrix, domain.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	seed, err := os.ReadFile(filepath.Join(dir, "observations.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	seedResult, err := engineInstance.Import(ctx, engine.ImportRequest{BatchID: "seed", Data: seed})
	if err != nil || !seedResult.Success {
		t.Fatalf("seed import: %v body=%s", err, seedResult.Body)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestForcedTerminationAndRecovery$")
	command.Env = append(os.Environ(),
		"LAGENGINE_RECOVERY_CHILD=1",
		"LAGENGINE_RECOVERY_DB="+dbPath,
		"LAGENGINE_RECOVERY_MATRIX="+filepath.Join(dir, "compat.json"),
		"LAGENGINE_RECOVERY_LEGACY="+filepath.Join(dir, "legacy.jsonl"),
	)
	started := time.Now()
	output, childErr := command.CombinedOutput()
	if childErr == nil {
		t.Fatalf("child process unexpectedly exited successfully; output=%s", output)
	}
	var exitErr *exec.ExitError
	if !errors.As(childErr, &exitErr) || exitErr.ExitCode() != 137 {
		t.Fatalf("child exit = %v output=%s", childErr, output)
	}
	t.Logf("child forced termination observed after %s: exit=137", time.Since(started).Round(time.Millisecond))

	reopened, err := store.Open(ctx, dbPath, store.OpenOptions{})
	if err != nil {
		t.Fatalf("reopen after forced termination: %v", err)
	}
	defer reopened.Close()
	report, version, err := reopened.RecoveryReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 || report.LastCompleteVersion != 1 || report.PendingIntentsFound != 1 || len(report.Events) != 1 {
		t.Fatalf("unexpected recovery report: version=%d report=%+v", version, report)
	}
	event := report.Events[0]
	if event.Action != "ABORT_PENDING_INTENT" || event.BatchID != "crashed-batch" || event.BaseVersion != 1 {
		t.Fatalf("unexpected recovery event: %+v", event)
	}
	observations, observedVersion, err := reopened.ObservationSet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if observedVersion != 1 || len(observations) != 13 {
		t.Fatalf("observations after recovery: version=%d count=%d", observedVersion, len(observations))
	}
	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var batchStatus, responseJSON string
	if err := raw.QueryRowContext(ctx, `SELECT status, response_json FROM batches WHERE batch_id='crashed-batch'`).Scan(&batchStatus, &responseJSON); err != nil {
		t.Fatal(err)
	}
	if batchStatus != "ABORTED" {
		t.Fatalf("crashed batch status = %s", batchStatus)
	}
	var recoveredError struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal([]byte(responseJSON), &recoveredError); err != nil {
		t.Fatal(err)
	}
	if recoveredError.Code != apperr.CodeBatchAborted {
		t.Fatalf("recovered batch code = %s, want %s", recoveredError.Code, apperr.CodeBatchAborted)
	}
	var pending int
	if err := raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM intents WHERE status='PENDING'`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("pending intents after recovery = %d", pending)
	}
}

func runCrashChild() {
	ctx := context.Background()
	matrix, err := compat.Load(os.Getenv("LAGENGINE_RECOVERY_MATRIX"))
	if err != nil {
		os.Exit(90)
	}
	data, err := os.ReadFile(os.Getenv("LAGENGINE_RECOVERY_LEGACY"))
	if err != nil {
		os.Exit(91)
	}
	database, err := store.Open(ctx, os.Getenv("LAGENGINE_RECOVERY_DB"), store.OpenOptions{
		AfterIntent: func() { os.Exit(137) },
	})
	if err != nil {
		os.Exit(92)
	}
	defer database.Close()
	engineInstance, err := engine.New(database, matrix, domain.DefaultPolicy())
	if err != nil {
		os.Exit(93)
	}
	result, err := engineInstance.Import(ctx, engine.ImportRequest{BatchID: "crashed-batch", Data: data})
	if err != nil || result.Success {
		os.Exit(94)
	}
	// The callback normally exits above. If it did not, fail distinctly.
	os.Exit(95)
}
