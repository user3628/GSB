package engine

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "_crash_worker" {
		os.Exit(RunCrashWorker(os.Args[2:]))
	}
	os.Exit(m.Run())
}

func tempDB(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(t.TempDir(), name)
}

func TestFeatureDeterminismNFKCAndMissingBucket(t *testing.T) {
	cfg := DefaultConfig()
	in := SampleInput{ID: "feat-1", Text: "ＶＩＰ 退款\nＡＢ 退款", Fields: map[string]string{"source": ""}}
	normalized := NormalizeText(in.Text)
	if normalized != "vip 退款 ab 退款" {
		t.Fatalf("unexpected normalization: %q", normalized)
	}
	tokens := Tokenize(normalized)
	foundVIP := false
	for _, token := range tokens {
		if token == "l:vip" {
			foundVIP = true
		}
	}
	if !foundVIP {
		t.Fatalf("Latin run token missing: %v", tokens)
	}

	fv1, err := Featurize(in, cfg)
	if err != nil {
		t.Fatal(err)
	}
	fv2, err := Featurize(in, cfg)
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := CanonicalJSON(fv1)
	b2, _ := CanonicalJSON(fv2)
	if string(b1) != string(b2) || fv1.Digest != fv2.Digest {
		t.Fatal("feature vector is not byte deterministic")
	}
	if fv1.MissingBucket != MissingBucket || fv1.MissingCount == 0 {
		t.Fatalf("missing bucket not recorded: %+v", fv1)
	}

	small := Config{Dim: 2, Units: 1, Epochs: 1, LRNum: 1, LRDen: 1, BaseScale: 16, ScoreScale: 16, MissingFields: []string{"source"}}
	colliding, err := Featurize(SampleInput{ID: "collide", Text: "甲乙丙 alpha beta gamma", Label: 1}, small)
	if err != nil {
		t.Fatal(err)
	}
	if colliding.CollisionCount == 0 {
		t.Fatalf("expected explicit hash collisions in dim=2: %+v", colliding)
	}
}

func TestInjectionStableCodes(t *testing.T) {
	cases := []struct {
		kind string
		code string
	}{
		{"hash_dim_zero", CodeHashDimZero},
		{"illegal_feature_dim", CodeFeatureDimInvalid},
		{"label_non_binary", CodeLabelInvalid},
		{"learning_rate_non_positive", CodeLearningRateInvalid},
		{"numeric_nan", CodeNumeric},
		{"numeric_inf", CodeNumeric},
	}
	for _, tc := range cases {
		err := InjectParam(ParamInjection{Kind: tc.kind, RequestID: "req-" + tc.kind, Dim: 7})
		if got := ErrorCode(err); got != tc.code {
			t.Fatalf("%s: want %s, got %s (%v)", tc.kind, tc.code, got, err)
		}
		var ee *Error
		if errors.As(err, &ee) && tc.code == CodeNumeric {
			if ee.RequestID == "" || ee.Dim == nil {
				t.Fatalf("E_NUMERIC missing request/dimension: %+v", ee)
			}
		}
	}
}

func importDemo(t *testing.T, path string) *Engine {
	t.Helper()
	e, err := OpenDB(path, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	data := GenerateDemoData(DefaultConfig().Seed)
	for _, batch := range data.Batches {
		if _, err := e.ImportBatch(batch); err != nil {
			t.Fatalf("import %s: %v", batch.ID, err)
		}
	}
	return e
}

func TestResumeMatchesContinuousTrainingBytes(t *testing.T) {
	resumePath := tempDB(t, "resume.db")
	e1, err := OpenDB(resumePath, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	data := GenerateDemoData(DefaultConfig().Seed)
	if _, err := e1.ImportBatch(data.Batches[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := e1.ImportBatch(data.Batches[1]); err != nil {
		t.Fatal(err)
	}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := OpenDB(resumePath, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, batch := range data.Batches[2:] {
		if _, err := e2.ImportBatch(batch); err != nil {
			t.Fatal(err)
		}
	}
	resumeCP, err := e2.LoadCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	resumeBytes, err := resumeCP.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}

	fullPath := tempDB(t, "full.db")
	e3 := importDemo(t, fullPath)
	fullCP, err := e3.LoadCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	fullBytes, err := fullCP.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if err := e3.Close(); err != nil {
		t.Fatal(err)
	}
	if string(resumeBytes) != string(fullBytes) {
		diff := FirstByteDifference(resumeBytes, fullBytes)
		t.Fatalf("resume checkpoint differs at byte %d", diff)
	}
}

func TestBackfillByteEquivalenceAndInferenceBounds(t *testing.T) {
	path := tempDB(t, "backfill.db")
	e := importDemo(t, path)
	defer e.Close()
	data := GenerateDemoData(DefaultConfig().Seed)

	_, err := e.Score(ScoreRequest{RequestID: "before", Text: "退款 invoice"})
	if err != nil {
		t.Fatal(err)
	}
	bf, err := e.Backfill(BackfillRequest{
		SampleID: data.Correction.SampleID,
		Label:    data.Correction.Label,
		Fields:   data.Correction.Fields,
	})
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if !bf.ByteEqual || bf.FirstDiffByte != -1 {
		t.Fatalf("backfill mismatch: %+v", bf)
	}
	if len(bf.AffectedUnits) == 0 || len(bf.ReusedUnits) == 0 {
		t.Fatalf("expected both affected and reused units in default demo: %+v", bf)
	}

	_, err = e.Backfill(BackfillRequest{SampleID: data.Correction.SampleID, Label: intPtr(9)})
	if ErrorCode(err) != CodeLabelInvalid {
		t.Fatalf("expected label code, got %v", err)
	}

	score, err := e.Score(ScoreRequest{RequestID: "bounds", Text: data.QueryText, Fields: data.QueryFields})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range score.DimBounds {
		if d.ObservedError > d.Bound+1e-10 {
			t.Fatalf("dim %d observed error %g exceeds bound %g", d.Dim, d.ObservedError, d.Bound)
		}
	}
	if score.ObservedError > score.TotalBound+1e-10 {
		t.Fatalf("total error %g exceeds bound %g", score.ObservedError, score.TotalBound)
	}

	_, err = e.Score(ScoreRequest{RequestID: "numeric-bad", Text: "退款", Fault: &NumericFault{Dim: 3, Kind: "nan"}})
	if ErrorCode(err) != CodeNumeric {
		t.Fatalf("expected E_NUMERIC, got %v", err)
	}
	var ne *Error
	if errors.As(err, &ne) && (ne.RequestID != "numeric-bad" || ne.Dim == nil || *ne.Dim != 3) {
		t.Fatalf("numeric error missing request/dim: %+v", ne)
	}
}

func TestConcurrentUnitPublicationAndStaleCodes(t *testing.T) {
	path := tempDB(t, "publish.db")
	e, err := OpenDB(path, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	const workers = 8
	const iterations = 5
	results, errs := ConcurrentPublish(path, 3, workers, iterations)
	if len(errs) != 0 {
		t.Fatalf("concurrent publish errors: %v", errs)
	}
	if len(results) != workers*iterations {
		t.Fatalf("got %d results", len(results))
	}
	st, err := e.Status()
	if err != nil {
		t.Fatal(err)
	}
	var current int64
	for _, u := range st.Units {
		if u.UnitID == 3 {
			current = u.CurrentVersion
		}
	}
	if current != workers*iterations {
		t.Fatalf("current version = %d, want %d", current, workers*iterations)
	}
	var currentRows int
	if err := e.db.QueryRow("SELECT COUNT(*) FROM unit_versions WHERE unit_id=3 AND is_current=1").Scan(&currentRows); err != nil {
		t.Fatal(err)
	}
	if currentRows != 1 {
		t.Fatalf("expected one current parameter row, got %d", currentRows)
	}

	if _, err := e.PublishUnit(PublishUnitRequest{UnitID: 3, ExpectedVersion: 1}); ErrorCode(err) != CodeStaleCheckpoint {
		t.Fatalf("expected stale expected-version, got %v", err)
	}
	if _, err := e.PublishUnit(PublishUnitRequest{UnitID: 3, NewVersion: current}); ErrorCode(err) != CodeStaleCheckpoint {
		t.Fatalf("expected duplicate stale, got %v", err)
	}
	if _, err := e.PublishUnit(PublishUnitRequest{UnitID: 3, NewVersion: current + 2}); ErrorCode(err) != CodeStaleCheckpoint {
		t.Fatalf("expected gap stale, got %v", err)
	}
	cross := &UnitCheckpoint{UnitID: 4}
	if _, err := e.PublishUnit(PublishUnitRequest{UnitID: 3, State: cross}); ErrorCode(err) != CodeStaleCheckpoint {
		t.Fatalf("expected cross-unit stale, got %v", err)
	}
}

func TestVersionClassification(t *testing.T) {
	cur := VersionInfo{Sequence: 24, Generation: 1, DatasetDigest: "d1", ConfigDigest: "c1"}
	if d, err := ClassifyVersion(cur, VersionRequest{Sequence: 25, DatasetDigest: "d2", ConfigDigest: "c1"}); err != nil || d.Action != ActionNormalAdvance {
		t.Fatalf("normal: %+v %v", d, err)
	}
	if d, err := ClassifyVersion(cur, VersionRequest{Sequence: 25, DatasetDigest: "d3", ConfigDigest: "c2", Replacement: true}); err != nil || d.Action != ActionFullReplacement || d.Generation != 2 {
		t.Fatalf("replacement: %+v %v", d, err)
	}
	if _, err := ClassifyVersion(cur, VersionRequest{Sequence: 23, DatasetDigest: "old"}); ErrorCode(err) != CodeStaleCheckpoint {
		t.Fatalf("rollback should be stale: %v", err)
	}
	if _, err := ClassifyVersion(cur, VersionRequest{Sequence: 24, DatasetDigest: "d1", ConfigDigest: "c1"}); ErrorCode(err) != CodeStaleCheckpoint {
		t.Fatalf("duplicate should be stale: %v", err)
	}
}

func TestIncrementalMismatchReportsFirstByte(t *testing.T) {
	cfg := DefaultConfig()
	a, err := NewCheckpoint(cfg, "d", 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := CloneCheckpoint(a)
	if err != nil {
		t.Fatal(err)
	}
	b.Units[0].Dims[0].Weight++
	err = EnsureCheckpointsEqual(b, a)
	if ErrorCode(err) != CodeIncrementalMismatch {
		t.Fatalf("expected mismatch code, got %v", err)
	}
	var me *Error
	if errors.As(err, &me) && me.FirstDiffByte < 0 {
		t.Fatalf("expected non-negative first diff: %+v", me)
	}
}

func TestWholeReplacementInvalidatesAndRebuilds(t *testing.T) {
	path := tempDB(t, "replacement.db")
	e := importDemo(t, path)
	defer e.Close()
	if _, err := e.Score(ScoreRequest{RequestID: "cache", Text: "退款 invoice"}); err != nil {
		t.Fatal(err)
	}
	cfgDigest, err := e.cfg.Digest()
	if err != nil {
		t.Fatal(err)
	}
	dec, err := e.RegisterVersion(VersionRequest{Sequence: 25, Replacement: true, DatasetDigest: "replacement-dataset", ConfigDigest: cfgDigest})
	if err != nil {
		t.Fatal(err)
	}
	if dec.Action != ActionFullReplacement || dec.Generation != 2 {
		t.Fatalf("unexpected decision: %+v", dec)
	}
	var validCaches, currentUnits int
	if err := e.db.QueryRow("SELECT COUNT(*) FROM inference_cache WHERE valid=1").Scan(&validCaches); err != nil {
		t.Fatal(err)
	}
	if validCaches != 0 {
		t.Fatalf("replacement left %d valid caches", validCaches)
	}
	if err := e.db.QueryRow("SELECT COUNT(*) FROM units WHERE current_version<>0").Scan(&currentUnits); err != nil {
		t.Fatal(err)
	}
	if currentUnits != 0 {
		t.Fatalf("replacement left %d current units", currentUnits)
	}

	cp, err := e.FullRebuildAndPublish("whole_replacement")
	if err != nil {
		t.Fatal(err)
	}
	if cp.Generation != 2 || cp.ModelVersion != 25 {
		t.Fatalf("unexpected rebuilt version gen=%d model=%d", cp.Generation, cp.ModelVersion)
	}
	st, err := e.Status()
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range st.Units {
		if u.CurrentVersion != 25 {
			t.Fatalf("unit %d version %d", u.UnitID, u.CurrentVersion)
		}
	}
	score, err := e.Score(ScoreRequest{RequestID: "after-replacement", Text: "退款 invoice"})
	if err != nil {
		t.Fatal(err)
	}
	if score.ObservedError > score.TotalBound+1e-10 {
		t.Fatal("replacement rebuild score exceeds bound")
	}
}

func TestDemoHardKillAndFixedDigest(t *testing.T) {
	s1, err := RunDemo(DemoOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := RunDemo(DemoOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !s1.OK || !s2.OK {
		t.Fatalf("demo not OK: %+v %+v", s1, s2)
	}
	if s1.SummaryDigest == "" || s1.SummaryDigest != s2.SummaryDigest {
		t.Fatalf("summary digests differ:\n%s\n%s", s1.SummaryDigest, s2.SummaryDigest)
	}
	if s1.SummaryDigest != "f1580c2da67d48acaf675c6b444c315d394f80b1579a35bc7e19ee05c69ce9d7" {
		t.Fatalf("unexpected fixed digest: %s", s1.SummaryDigest)
	}

	continuousPath := tempDB(t, "continuous-after-kill.db")
	continuous := importDemo(t, continuousPath)
	data := GenerateDemoData(DefaultConfig().Seed)
	bf, err := continuous.Backfill(BackfillRequest{
		SampleID: data.Correction.SampleID,
		Label:    data.Correction.Label,
		Fields:   data.Correction.Fields,
	})
	if err != nil {
		t.Fatal(err)
	}
	cp, err := continuous.LoadCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if cp.Fingerprint != s1.Fingerprint || bf.IncrementalFingerprint != s1.Fingerprint {
		t.Fatalf("killed/recovered fingerprint %s != continuous %s", s1.Fingerprint, cp.Fingerprint)
	}
	if err := continuous.Close(); err != nil {
		t.Fatal(err)
	}
}
