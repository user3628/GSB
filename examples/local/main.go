package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"abeval"
)

func main() {
	ctx := context.Background()
	outputDir := filepath.Join("artifacts", "example-local")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		log.Fatal(err)
	}

	for _, scenario := range []string{
		abeval.ScenarioRollout,
		abeval.ScenarioInsufficient,
		abeval.ScenarioRollback,
	} {
		dataset := abeval.DefaultDataset(scenario)
		report, err := abeval.EvaluateDataset(dataset)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-12s decision=%-8s code=%s sha256=%s\n",
			scenario, report.Decision, report.DecisionCode, hashReport(report))
	}

	dataset := abeval.DefaultDataset(abeval.ScenarioRecovery)
	batchID := "example-recovery"
	dataset.BatchID = batchID
	continuousDB := filepath.Join(outputDir, "continuous.db")
	recoveryDB := filepath.Join(outputDir, "recovery.db")
	removeSQLiteFiles(continuousDB)
	removeSQLiteFiles(recoveryDB)

	continuousBytes := runPersistedBatch(ctx, continuousDB, batchID, dataset)

	gracefulStore, err := abeval.OpenStore(recoveryDB)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := gracefulStore.ImportDataset(ctx, batchID, dataset); err != nil {
		log.Fatal(err)
	}
	_, err = abeval.RunBatch(ctx, gracefulStore, batchID, abeval.RunOptions{
		WriterID:       "example-graceful",
		StopAfterStage: dataset.Config.StopAfterStage,
	})
	if err != abeval.ErrGracefulStop {
		log.Fatalf("expected graceful stop, got %v", err)
	}
	if err := gracefulStore.Close(); err != nil {
		log.Fatal(err)
	}

	recoveredBytes := runPersistedBatch(ctx, recoveryDB, batchID, dataset)
	if string(continuousBytes) != string(recoveredBytes) {
		log.Fatal("recovery report is not byte-identical")
	}
	reportPath := filepath.Join(outputDir, "recovery-report.json")
	if err := os.WriteFile(reportPath, recoveredBytes, 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%-12s decision=%-8s report=%s sha256=%s byte_identical=true\n",
		"recovery", abeval.DecisionRollout, reportPath, abeval.SHA256Hex(recoveredBytes))
}

func runPersistedBatch(ctx context.Context, databasePath, batchID string, dataset abeval.Dataset) []byte {
	store, err := abeval.OpenStore(databasePath)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := store.ImportDataset(ctx, batchID, dataset); err != nil {
		log.Fatal(err)
	}
	if _, err := abeval.RunBatch(ctx, store, batchID, abeval.RunOptions{WriterID: "example"}); err != nil {
		log.Fatal(err)
	}
	report, err := store.ExportReport(ctx, batchID)
	if err != nil {
		log.Fatal(err)
	}
	if err := store.Close(); err != nil {
		log.Fatal(err)
	}
	return report
}

func hashReport(report *abeval.Report) string {
	data, err := abeval.CanonicalJSON(report)
	if err != nil {
		log.Fatal(err)
	}
	return abeval.SHA256Hex(data)
}

func removeSQLiteFiles(path string) {
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(path + suffix)
	}
}
