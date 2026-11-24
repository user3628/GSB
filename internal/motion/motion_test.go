package motion_test

import (
	"bytes"
	"testing"

	"motionlab/internal/model"
	"motionlab/internal/motion"
)

func TestGeneratedCorpusIsByteDeterministic(t *testing.T) {
	dirA, dirB := t.TempDir(), t.TempDir()
	if err := motion.Initialize(dirA, true); err != nil {
		t.Fatal(err)
	}
	if err := motion.Initialize(dirB, true); err != nil {
		t.Fatal(err)
	}
	a, err := motion.LoadCatalog(dirA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := motion.LoadCatalog(dirB)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Order) < 3 {
		t.Fatalf("expected at least 3 generated sequences, got %d", len(a.Order))
	}
	for _, id := range a.Order {
		sa, sb := a.Sequences[id], b.Sequences[id]
		if sa.Shared.Initial.SHA256 != sb.Shared.Initial.SHA256 {
			t.Fatalf("%s initial frame differs", id)
		}
		if len(sa.Truth) != len(sb.Truth) {
			t.Fatalf("%s truth length differs", id)
		}
		for i := range sa.Truth {
			if sa.Truth[i].SHA256 != sb.Truth[i].SHA256 {
				t.Fatalf("%s truth frame %d differs", id, i)
			}
		}
		states := motion.TruthTrajectory(sa.Spec)
		if !states[sa.Spec.DirectionChangeStep].EventApplied {
			t.Fatalf("%s direction change was not applied", id)
		}
	}
}

func TestRendererBranchZeroMatchesTruth(t *testing.T) {
	dir := t.TempDir()
	if err := motion.Initialize(dir, true); err != nil {
		t.Fatal(err)
	}
	cat, err := motion.LoadCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	seq := cat.Sequences["harbor_slide"]
	state := seq.Spec.InitialState
	profile := motion.ProfileFor(seq.Spec, 0)
	renderer := motion.Renderer{}
	for step := 1; step <= seq.Spec.PredictSteps; step++ {
		state = motion.AdvanceBranch(state, seq.Spec, profile, step)
		frame, err := renderer.Render(seq.Shared, seq.Spec, state)
		if err != nil {
			t.Fatal(err)
		}
		if frame.SHA256 != seq.Truth[step].SHA256 {
			t.Fatalf("step %d frame digest differs from truth", step)
		}
		mae, err := motion.MAE(frame, seq.Truth[step])
		if err != nil || mae != 0 {
			t.Fatalf("step %d MAE=%v err=%v", step, mae, err)
		}
	}
}

func TestSharedFrameWriteGuardAndQuantization(t *testing.T) {
	dir := t.TempDir()
	if err := motion.Initialize(dir, true); err != nil {
		t.Fatal(err)
	}
	cat, err := motion.LoadCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	shared := cat.Sequences["harbor_slide"].Shared
	if appErr := shared.Write("harbor_slide", 2, 4, []byte{1, 2}); appErr == nil || appErr.Code != model.CodeSharedFrameWrite {
		t.Fatalf("expected structured shared-frame write error, got %v", appErr)
	}
	if model.Quantize8(0.5) != 128 || model.Quantize8(-1) != 0 || model.Quantize8(2) != 255 {
		t.Fatalf("unexpected 8-bit quantization")
	}
}

func TestImportRoundTrip(t *testing.T) {
	sourceDir, targetDir := t.TempDir(), t.TempDir()
	if err := motion.Initialize(sourceDir, true); err != nil {
		t.Fatal(err)
	}
	if err := motion.Initialize(targetDir, true); err != nil {
		t.Fatal(err)
	}
	source, err := motion.LoadCatalog(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	base := source.Sequences["orchid_drift"]
	spec := base.Spec
	spec.SequenceID = "imported_orchid"
	spec.Name = "Imported Orchid"
	cat, err := motion.ImportSequence(targetDir, motion.ImportBundle{Spec: spec, Initial: base.Shared.Initial, Background: base.Shared.Background, Truth: base.Truth})
	if err != nil {
		t.Fatal(err)
	}
	imported := cat.Sequences[spec.SequenceID]
	if imported == nil || len(cat.Order) != 4 {
		t.Fatalf("import did not appear in catalog")
	}
	for i := range base.Truth {
		if imported.Truth[i].SHA256 != base.Truth[i].SHA256 {
			t.Fatalf("imported truth frame %d differs", i)
		}
	}
	manifestA, err := motion.LoadCatalog(targetDir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal([]byte(manifestA.Sequences[spec.SequenceID].Shared.Initial.SHA256), []byte(base.Shared.Initial.SHA256)) {
		t.Fatal("imported initial frame digest mismatch after reload")
	}
}
