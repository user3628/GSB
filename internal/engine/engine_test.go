package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"ticketpipeline/internal/gen"
	"ticketpipeline/internal/model"
)

func seedWorkspace(t *testing.T, dataDir string) {
	t.Helper()
	for _, sh := range gen.GenerateShards() {
		if err := gen.WriteShard(dataDir, sh); err != nil {
			t.Fatal(err)
		}
	}
	if err := gen.WritePolicy(dataDir, gen.GeneratePolicy()); err != nil {
		t.Fatal(err)
	}
}

func runOnce(t *testing.T, dataDir, outDir string) *Snapshot {
	t.Helper()
	eng, err := New(dataDir, outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	snap, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := VerifyCheckpoints(eng.Store()); err != nil {
		t.Fatalf("verify: %v", err)
	}
	return snap
}

func TestEndToEndDeterminism(t *testing.T) {
	base := t.TempDir()
	d1, o1 := filepath.Join(base, "data1"), filepath.Join(base, "out1")
	d2, o2 := filepath.Join(base, "data2"), filepath.Join(base, "out2")
	seedWorkspace(t, d1)
	seedWorkspace(t, d2)
	a := runOnce(t, d1, o1)
	b := runOnce(t, d2, o2)
	if a.Report == nil || b.Report == nil {
		t.Fatal("missing report")
	}
	if a.Report.Digest != b.Report.Digest {
		t.Fatalf("fresh runs diverged: %s != %s", a.Report.Digest, b.Report.Digest)
	}
	if len(a.NormErr) != 2 {
		t.Fatalf("want 2 E_EMPTY_TEXT, got %d", len(a.NormErr))
	}
	for _, be := range a.NormErr {
		if be.Code != model.CodeEmptyText {
			t.Fatalf("unexpected normalize code %s", be.Code)
		}
	}
	if len(a.PrioErr) != 1 {
		t.Fatalf("want 1 E_LABEL_RANGE, got %d", len(a.PrioErr))
	}
	for _, be := range a.PrioErr {
		if be.Code != model.CodeLabelRange {
			t.Fatalf("unexpected priority code %s", be.Code)
		}
	}
}

func TestIncrementalMatchesFresh(t *testing.T) {
	base := t.TempDir()
	dA, oA := filepath.Join(base, "da"), filepath.Join(base, "oa")
	dB, oB := filepath.Join(base, "db"), filepath.Join(base, "ob")
	seedWorkspace(t, dA)
	seedWorkspace(t, dB)

	runOnce(t, dA, oA)
	runOnce(t, dB, oB)

	// Same threshold bump applied to both histories.
	if _, err := BumpPolicy(dA, PolicyPatch{PriorityThresh: []float64{-25, -15, -5}}); err != nil {
		t.Fatal(err)
	}
	if _, err := BumpPolicy(dB, PolicyPatch{PriorityThresh: []float64{-25, -15, -5}}); err != nil {
		t.Fatal(err)
	}
	inc := runOnce(t, dA, oA)
	fresh := runOnce(t, dB, oB)
	if inc.Report.Digest != fresh.Report.Digest {
		t.Fatalf("incremental %s != replayed %s", inc.Report.Digest, fresh.Report.Digest)
	}

	// Upstream stages must keep generation 1; downstream advanced to 2.
	eng, err := New(dA, oA)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	st, err := Status(eng.Store())
	if err != nil {
		t.Fatal(err)
	}
	genByStage := map[string]int{}
	for _, s := range st {
		genByStage[s.Stage] = s.LatestGen
		if !s.Contiguous {
			t.Fatalf("stage %s lost contiguity", s.Stage)
		}
	}
	if genByStage[model.StageNormalize] != 1 || genByStage[model.StageIntent] != 1 {
		t.Fatalf("upstream stages should be reused: %+v", genByStage)
	}
	if genByStage[model.StagePriority] != 2 || genByStage[model.StageValidate] != 2 {
		t.Fatalf("downstream stages should rebuild to gen2: %+v", genByStage)
	}
}

func TestZeroConfigBusinessCodes(t *testing.T) {
	base := t.TempDir()
	dataDir, outDir := filepath.Join(base, "data"), filepath.Join(base, "out")
	seedWorkspace(t, dataDir)
	zero := 0.0
	if _, err := BumpPolicy(dataDir, PolicyPatch{Smoothing: &zero}); err != nil {
		t.Fatal(err)
	}
	eng, err := New(dataDir, outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_, err = eng.Run(context.Background())
	be, ok := model.AsBizError(err)
	if !ok || be.Code != model.CodeConfig || be.Stage != model.StageIntent {
		t.Fatalf("want E_CONFIG @ intent, got %v", err)
	}
}

func TestReplayAfterReopenIsIdempotent(t *testing.T) {
	base := t.TempDir()
	dataDir, outDir := filepath.Join(base, "data"), filepath.Join(base, "out")
	seedWorkspace(t, dataDir)
	first := runOnce(t, dataDir, outDir)
	// Reopen and re-run without any input/config change: full reuse, same digest.
	second := runOnce(t, dataDir, outDir)
	if first.Report.Digest != second.Report.Digest {
		t.Fatal("reopen replay diverged")
	}
}

// TestCrashRecoverySemantics simulates a forced kill after only part of stage
// 1 has committed, then reopens the engine and verifies replay completes with
// contiguous sequences and the same report as an uninterrupted run.
func TestCrashRecoverySemantics(t *testing.T) {
	base := t.TempDir()
	dataDir, outCrash, outFull :=
		filepath.Join(base, "data"),
		filepath.Join(base, "out-crash"),
		filepath.Join(base, "out-full")
	seedWorkspace(t, dataDir)

	// Reference uninterrupted result.
	full := runOnce(t, dataDir, outFull)

	// Emulate a forced kill halfway through stage 1: run to completion once,
	// then remove the downstream stages plus the trailing normalize rows so
	// only an early committed prefix remains.
	_ = runOnce(t, dataDir, outCrash)
	eng, err := New(dataDir, outCrash)
	if err != nil {
		t.Fatal(err)
	}
	cps, err := eng.Store().ListCheckpoints(model.StageNormalize)
	if err != nil {
		eng.Close()
		t.Fatal(err)
	}
	half := len(cps) / 2
	// Reproduce a kill halfway through stage 1: no downstream stage ever ran.
	if err := eng.Store().ResetFromStage(model.StageIntent); err != nil {
		eng.Close()
		t.Fatal(err)
	}
	if err := truncateNormalize(eng, half); err != nil {
		eng.Close()
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	// Remove orphaned downstream artifact directories left by the first run.
	for _, stage := range model.Stages[1:] {
		_ = os.RemoveAll(filepath.Join(outCrash, stage))
	}

	// Reopen: recovery must rebuild from normalize using only committed facts.
	recovered := runOnce(t, dataDir, outCrash)
	if recovered.Report.Digest != full.Report.Digest {
		t.Fatalf("recovered %s != full %s", recovered.Report.Digest, full.Report.Digest)
	}
	eng2, err := New(dataDir, outCrash)
	if err != nil {
		t.Fatal(err)
	}
	defer eng2.Close()
	for _, stage := range model.Stages {
		if err := eng2.Store().AssertContiguous(stage); err != nil {
			t.Fatalf("post-recovery contiguity broken for %s: %v", stage, err)
		}
	}
}
