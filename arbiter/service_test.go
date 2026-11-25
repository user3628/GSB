package arbiter_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"leasearbiter/arbiter"
	"leasearbiter/internal/sample"
)

func TestPublicFacadeRunsSnapshotIngestAndRecovery(t *testing.T) {
	service, err := arbiter.Open(arbiter.Config{
		DBPath: filepath.Join(t.TempDir(), "public.db"),
		Coordinator: arbiter.CoordinatorConfig{
			Concurrency:   2,
			QueueCapacity: 2,
			Budget:        2 * time.Second,
		},
		HealTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report := service.StartupRecovery(); report.LastCommittedVersion != 0 || len(report.PreparedIntents) != 0 {
		t.Fatalf("unexpected startup recovery on new database: %+v", report)
	}
	defer service.Close()
	ctx := context.Background()
	dataset := sample.Generate(sample.DefaultSeed)
	empty := sample.Snapshot(1, sample.DefaultSeed)
	empty.Dataset.Observations = nil
	digest, err := arbiter.ComputeSnapshotDigest(empty)
	if err != nil {
		t.Fatal(err)
	}
	empty.Digest = digest
	if _, bizErr := service.SubmitSnapshot(ctx, empty); bizErr != nil {
		t.Fatal(bizErr)
	}
	report, bizErr := service.Ingest(ctx, dataset, arbiter.ProcessOptions{RunHeal: true})
	if bizErr != nil || !report.Committed {
		t.Fatalf("ingest: %+v %v", report, bizErr)
	}
	read, bizErr := service.Read(ctx, report.StateVersion)
	if bizErr != nil || len(read.Results) != 4 {
		t.Fatalf("read: %+v %v", read, bizErr)
	}
	recovery, bizErr := service.Recover(ctx)
	if bizErr != nil || recovery.LastCommittedVersion != report.StateVersion {
		t.Fatalf("recover: %+v %v", recovery, bizErr)
	}
}
