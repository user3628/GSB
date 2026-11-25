package service_test

import (
	"context"
	"testing"

	"flashattention/internal/dataset"
	"flashattention/internal/errs"
	"flashattention/internal/service"
)

func TestStrictNumericDiagnosticSurvivesRerun(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	spec := dataset.Spec{
		ID: "numeric-persist", B: 1, H: 1, S: 20, D: 4,
		RowBlock: 4, ColBlock: 4, Group: 1, Bits: 16, ExpSumBits: 4,
		Strategy: "logsum", Extreme: true, Seed: 909,
	}
	if _, err := svc.Generate(spec, true); err != nil {
		t.Fatal(err)
	}
	first, err := svc.Run(ctx, spec.ID, service.RunOptions{})
	if err != nil {
		t.Fatalf("non-strict run: %v", err)
	}
	if first.CheckpointsWritten == 0 {
		t.Fatal("expected initial run to publish checkpoints")
	}

	strict, err := svc.Run(ctx, spec.ID, service.RunOptions{StrictNumeric: true, Resume: true})
	if errs.Code(err) != errs.ENumeric {
		t.Fatalf("resumed strict run code=%s err=%v", errs.Code(err), err)
	}
	if strict == nil {
		t.Fatal("strict run returned nil report")
	}
	if strict.CheckpointsWritten != 0 {
		t.Fatalf("strict rerun wrote %d checkpoints; want reuse", strict.CheckpointsWritten)
	}
	if strict.NumericEventCount == 0 || len(strict.PersistedEvents) == 0 {
		t.Fatalf("numeric events not recovered: count=%d persisted=%d", strict.NumericEventCount, len(strict.PersistedEvents))
	}
	for _, event := range strict.PersistedEvents {
		if event.Code != errs.ENumeric || event.Row < 0 || event.RowBlock < 0 {
			t.Fatalf("persisted event missing diagnostic position: %+v", event)
		}
	}
}
