package pipeline

import (
	"testing"

	"matbench/internal/demo"
	"matbench/internal/errorsx"
	"matbench/internal/recipe"
	"matbench/internal/store"
)

func newTestPipeline(t *testing.T) (*Pipeline, *recipe.Book) {
	t.Helper()
	dir := t.TempDir()
	book, err := demo.Setup(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return &Pipeline{Dir: dir, Store: st}, book
}

type manualStop struct{ stop bool }

func (m *manualStop) StopRequested() bool { return m.stop }

func TestFullRunDeterministicDigest(t *testing.T) {
	p, book := newTestPipeline(t)
	s1, err := p.RunFull(book, RunOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if s1.MaterialCount != 4 || s1.ReportDigest == "" {
		t.Fatalf("bad summary: %+v", s1)
	}
	p2, book2 := newTestPipeline(t)
	s2, err := p2.RunFull(book2, RunOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if s1.ReportDigest != s2.ReportDigest {
		t.Fatalf("digest differs across workspaces: %s vs %s", s1.ReportDigest, s2.ReportDigest)
	}
}

func TestIncrementalByteIdenticalAndInvalidation(t *testing.T) {
	p, book := newTestPipeline(t)
	if _, err := p.RunFull(book, RunOptions{Workers: 2}); err != nil {
		t.Fatal(err)
	}
	desc := "Brushed Ａｌｕｍｉｎｕｍ 拉丝铝 metal, NEW 新条纹 finish"
	patch := recipe.Patch{MaterialID: "MAT-0001", Description: &desc}
	sum, err := p.RunIncremental(book, []recipe.Patch{patch}, RunOptions{Workers: 2})
	if err != nil {
		t.Fatalf("incremental mismatch: %v", err)
	}
	if sum.MaterialCount != 1 {
		t.Fatalf("incremental should rebuild one material, got %d", sum.MaterialCount)
	}
	rows, err := p.Store.Invalidations("MAT-0001")
	if err != nil {
		t.Fatal(err)
	}
	stages := map[string]bool{}
	for _, r := range rows {
		stages[r["stage"].(string)] = true
	}
	for _, want := range Stages {
		if !stages[want] {
			t.Fatalf("missing invalidation trail for stage %s: %v", want, stages)
		}
	}
	// Description change rebuilds control and cascades through every downstream
	// stage even though their own declared inputs are unchanged.
	cp, _ := p.Store.Active("MAT-0001", StageNoise)
	if cp == nil || cp.Seq != 2 {
		t.Fatalf("noise stage should cascade to seq 2, got %+v", cp)
	}
	cp, _ = p.Store.Active("MAT-0001", StageControl)
	if cp == nil || cp.Seq != 2 {
		t.Fatalf("control stage should advance to seq 2, got %+v", cp)
	}
	cp, _ = p.Store.Active("MAT-0001", StageReport)
	if cp == nil || cp.Seq != 2 {
		t.Fatalf("report stage should cascade to seq 2, got %+v", cp)
	}
}

func TestIncrementalNoiseOnlyKeepsControl(t *testing.T) {
	p, book := newTestPipeline(t)
	if _, err := p.RunFull(book, RunOptions{}); err != nil {
		t.Fatal(err)
	}
	freq := 3.5
	patch := recipe.Patch{MaterialID: "MAT-0003", NoiseFrequency: &freq}
	if _, err := p.RunIncremental(book, []recipe.Patch{patch}, RunOptions{}); err != nil {
		t.Fatal(err)
	}
	cp, _ := p.Store.Active("MAT-0003", StageControl)
	if cp == nil || cp.Seq != 1 {
		t.Fatal("control must be reused when only noise changes")
	}
	cp, _ = p.Store.Active("MAT-0003", StageNoise)
	if cp == nil || cp.Seq != 2 {
		t.Fatal("noise must advance to seq 2")
	}
}

func TestGracefulStopAndResumeIdentical(t *testing.T) {
	p, book := newTestPipeline(t)
	stop := &manualStop{}
	// Stop immediately: the first material unit completes (indivisible unit),
	// then the run exits.
	stop.stop = true
	partial, err := p.RunFull(book, RunOptions{Stop: stop})
	if err != nil {
		t.Fatal(err)
	}
	if !partial.Stopped {
		t.Fatal("expected stopped summary")
	}
	// Resume on the same workspace and compare with a fresh continuous run.
	resumed, err := p.RunFull(book, RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p2, book2 := newTestPipeline(t)
	continuous, err := p2.RunFull(book2, RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.ReportDigest != continuous.ReportDigest {
		t.Fatalf("resumed digest %s != continuous %s", resumed.ReportDigest, continuous.ReportDigest)
	}
	for id, dg := range continuous.TextureDigest {
		if resumed.TextureDigest[id] != dg {
			t.Fatalf("texture digest mismatch for %s", id)
		}
	}
}

func TestIncrementalMismatchCode(t *testing.T) {
	p, book := newTestPipeline(t)
	if _, err := p.RunFull(book, RunOptions{}); err != nil {
		t.Fatal(err)
	}
	// Tamper with the control payload, then change only noise so control is
	// reused during the incremental run and the divergence surfaces at verify.
	cp, err := p.Store.Active("MAT-0002", StageControl)
	if err != nil || cp == nil {
		t.Fatal(err)
	}
	tampered := []byte(`{"material_id":"MAT-0002","normalized_prompt":"tampered",
"tokens":[],"vector":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
"hash_seed":1,"fingerprint":"tampered"}`)
	if err := p.tamperPayload(cp, tampered); err != nil {
		t.Fatal(err)
	}
	freq := 4.25
	patch := recipe.Patch{MaterialID: "MAT-0002", NoiseFrequency: &freq}
	_, err = p.RunIncremental(book, []recipe.Patch{patch}, RunOptions{})
	if err == nil || errorsx.CodeOf(err) != errorsx.EIncrementalMismatch {
		t.Fatalf("want E_INCREMENTAL_MISMATCH, got %v", err)
	}
}
