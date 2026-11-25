package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"ocrlab/internal/apperr"
	"ocrlab/internal/canonical"
	"ocrlab/internal/config"
	"ocrlab/internal/model"
	"ocrlab/internal/store"
)

func TestSmallPipelineAndDeterminism(t *testing.T) {
	ctx := context.Background()
	p, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	summary, err := p.Run(ctx, RunOptions{Groups: []string{"small"}})
	if err != nil {
		t.Fatal(err)
	}
	if summary.ImagesComplete != 4 || summary.UnitsExecuted != 16 || summary.Stopped {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	report, reportBytes, err := p.ReportForGroups(ctx, []string{"small"})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Groups) != 1 || report.Groups[0].DetectionRecall.Value == nil || *report.Groups[0].DetectionRecall.Value != 1 {
		t.Fatalf("expected perfect detection recall, got %+v", report.Groups)
	}
	if report.Groups[0].RecognitionAccuracy.Value == nil || *report.Groups[0].RecognitionAccuracy.Value != 1 {
		t.Fatalf("expected perfect recognition accuracy, got %+v", report.Groups[0])
	}
	firstDigest := report.Groups[0].Group
	_ = firstDigest
	firstReport := string(reportBytes)
	var firstDetection model.DetectionProduct
	_, firstDetectionDigest, err := p.LoadProduct(ctx, "small-001", model.StageDetect, &firstDetection)
	if err != nil {
		t.Fatal(err)
	}
	summary, err = p.Run(ctx, RunOptions{Groups: []string{"small"}})
	if err != nil {
		t.Fatal(err)
	}
	if summary.UnitsExecuted != 0 || summary.UnitsReused != 16 {
		t.Fatalf("expected all units reused, got %+v", summary)
	}
	var secondDetection model.DetectionProduct
	_, secondDetectionDigest, err := p.LoadProduct(ctx, "small-001", model.StageDetect, &secondDetection)
	if err != nil {
		t.Fatal(err)
	}
	if firstDetectionDigest != secondDetectionDigest {
		t.Fatalf("detection digest changed across runs: %s vs %s", firstDetectionDigest, secondDetectionDigest)
	}
	_, secondReportBytes, err := p.ReportForGroups(ctx, []string{"small"})
	if err != nil {
		t.Fatal(err)
	}
	if firstReport != string(secondReportBytes) {
		t.Fatal("report bytes changed across identical runs")
	}
}

func TestAllGroupsReportAndFullBuildEquivalence(t *testing.T) {
	ctx := context.Background()
	p, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	summary, err := p.Run(ctx, RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if summary.ImagesComplete != 60 || summary.UnitsExecuted != 240 || len(summary.Issues) != 0 {
		t.Fatalf("unexpected full-run summary: %+v", summary)
	}
	report, _, err := p.Report(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Groups) != 3 {
		t.Fatalf("expected three groups, got %d", len(report.Groups))
	}
	expected := map[string][3]int{
		"small":  {4, 43, 12},
		"medium": {16, 257, 64},
		"large":  {40, 806, 200},
	}
	for _, group := range report.Groups {
		want := expected[group.Group]
		if group.ShardCount != want[0] || group.TotalCharacters != want[1] || group.TruthBoxes != want[2] {
			t.Fatalf("unexpected observed totals for %s: %+v", group.Group, group)
		}
		if group.DetectionRecall.Value == nil || *group.DetectionRecall.Value != 1 {
			t.Fatalf("expected perfect detection recall in %s", group.Group)
		}
		if group.RecognitionAccuracy.Value == nil || *group.RecognitionAccuracy.Value != 1 {
			t.Fatalf("expected perfect recognition accuracy in %s", group.Group)
		}
		if group.AverageMatchDistance.Value == nil || *group.AverageMatchDistance.Value != 0 {
			t.Fatalf("expected zero average distance in %s", group.Group)
		}
		if group.NotApplicableCount != 0 {
			t.Fatalf("unexpected N/A count in %s: %d", group.Group, group.NotApplicableCount)
		}
	}
	if report.Totals.ShardCount != 60 || report.Totals.TotalCharacters != 1106 || report.Totals.TruthBoxes != 276 {
		t.Fatalf("unexpected report totals: %+v", report.Totals)
	}
	var strategy model.StrategyProduct
	_, _, err = p.LoadProduct(ctx, "small-001", model.StageStrategy, &strategy)
	if err != nil {
		t.Fatal(err)
	}
	if strategy.Detection.Chosen == "d-heavy" || strategy.Recognition.Chosen == "r-hash-dense" {
		t.Fatalf("small tier selected a heavy strategy: %+v", strategy)
	}
	if len(strategy.Detection.Evaluations) != 3 || len(strategy.Recognition.Evaluations) != 3 {
		t.Fatalf("strategy decisions must retain all three candidates: %+v", strategy)
	}
	heavyRejected := false
	for _, evaluation := range strategy.Detection.Evaluations {
		if evaluation.Code == "d-heavy" && !evaluation.Admissible && evaluation.EliminatedReason != "" {
			heavyRejected = true
		}
	}
	if !heavyRejected {
		t.Fatal("small tier did not record why the heavy strategy was rejected")
	}
	verification, err := p.VerifyIncremental(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !verification.Equal || verification.ReportDigest != verification.FullReportDigest || verification.BundleDigest != verification.FullBundleDigest {
		t.Fatalf("incremental and full builds differ: %+v", verification)
	}
}

func TestIncrementalTruthChangeInvalidatesOnlyRecognitionAndDownstream(t *testing.T) {
	ctx := context.Background()
	p, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Run(ctx, RunOptions{}); err != nil {
		t.Fatal(err)
	}
	var beforeDetection, beforeRecognition, beforeStrategy, beforeDiagnostic json.RawMessage
	beforeDetectionDigest := loadRawDigest(t, p, "small-001", model.StageDetect, &beforeDetection)
	beforeRecognitionDigest := loadRawDigest(t, p, "small-001", model.StageRecognize, &beforeRecognition)
	beforeStrategyDigest := loadRawDigest(t, p, "small-001", model.StageStrategy, &beforeStrategy)
	beforeDiagnosticDigest := loadRawDigest(t, p, "small-001", model.StageDiagnostic, &beforeDiagnostic)

	manifestBytes, err := os.ReadFile(p.Paths().ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest model.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	original := manifest.Images[0].Truth[0].Text
	replacement := []rune(original)
	if replacement[0] == 'A' {
		replacement[0] = 'B'
	} else {
		replacement[0] = 'A'
	}
	manifest.Images[0].Truth[0].Text = string(replacement)
	modified, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.Paths().ManifestPath, append(modified, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Reimport(ctx); err != nil {
		t.Fatal(err)
	}
	summary, err := p.Run(ctx, RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Stages[0].Reused != 60 || summary.Stages[0].Executed != 0 {
		t.Fatalf("truth change should retain all detection checkpoints: %+v", summary.Stages[0])
	}
	if summary.Stages[1].Executed != 1 || summary.Stages[2].Executed != 1 || summary.Stages[3].Executed != 1 {
		t.Fatalf("truth change should rebuild only the affected image downstream: %+v", summary.Stages)
	}
	var afterDetection, afterRecognition, afterStrategy, afterDiagnostic json.RawMessage
	afterDetectionDigest := loadRawDigest(t, p, "small-001", model.StageDetect, &afterDetection)
	afterRecognitionDigest := loadRawDigest(t, p, "small-001", model.StageRecognize, &afterRecognition)
	afterStrategyDigest := loadRawDigest(t, p, "small-001", model.StageStrategy, &afterStrategy)
	afterDiagnosticDigest := loadRawDigest(t, p, "small-001", model.StageDiagnostic, &afterDiagnostic)
	if beforeDetectionDigest != afterDetectionDigest || string(beforeDetection) != string(afterDetection) {
		t.Fatal("detection unexpectedly changed after a truth-only update")
	}
	if beforeRecognitionDigest == afterRecognitionDigest || string(beforeRecognition) == string(afterRecognition) {
		t.Fatal("recognition did not rebuild after its template inputs changed")
	}
	if beforeStrategyDigest == afterStrategyDigest || beforeDiagnosticDigest == afterDiagnosticDigest {
		t.Fatal("downstream strategy/diagnostic stages were not invalidated")
	}
	verification, err := p.VerifyIncremental(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !verification.Equal {
		t.Fatalf("incremental result differs from full build: %+v", verification)
	}
}

func TestSignalStopResumeMatchesCleanBuild(t *testing.T) {
	ctx := context.Background()
	resumed, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	stopCtx, cancel := context.WithCancel(ctx)
	summary, err := resumed.Run(stopCtx, RunOptions{Groups: []string{"small"}, Progress: func(progress Progress) {
		if progress.Stage == model.StageDetect {
			cancel()
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !summary.Stopped || summary.StopReason != "signal" || summary.UnitsExecuted != 1 {
		t.Fatalf("expected a graceful signal stop after one completed unit, got %+v", summary)
	}
	summary, err = resumed.Run(ctx, RunOptions{Groups: []string{"small"}})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Stopped || summary.ImagesComplete != 4 {
		t.Fatalf("resume did not complete the scope: %+v", summary)
	}
	resumedReport, resumedBytes, err := resumed.ReportForGroups(ctx, []string{"small"})
	if err != nil {
		t.Fatal(err)
	}
	clean, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer clean.Close()
	if _, err := clean.Run(ctx, RunOptions{Groups: []string{"small"}}); err != nil {
		t.Fatal(err)
	}
	cleanReport, cleanBytes, err := clean.ReportForGroups(ctx, []string{"small"})
	if err != nil {
		t.Fatal(err)
	}
	if string(resumedBytes) != string(cleanBytes) {
		t.Fatal("resumed diagnostic report differs from a clean run")
	}
	if !reflect.DeepEqual(resumedReport.Totals, cleanReport.Totals) {
		t.Fatal("resumed totals differ from a clean run")
	}
	for _, imageSpec := range resumed.Manifest().Images {
		if imageSpec.Group != "small" {
			continue
		}
		for _, stage := range []string{model.StageDetect, model.StageRecognize, model.StageStrategy, model.StageDiagnostic} {
			var left, right json.RawMessage
			loadRawDigest(t, resumed, imageSpec.ImageID, stage, &left)
			loadRawDigest(t, clean, imageSpec.ImageID, stage, &right)
			if string(left) != string(right) {
				t.Fatalf("stage product differs after resume: %s/%s", imageSpec.ImageID, stage)
			}
		}
	}
}

func TestStableErrorCodesAndCheckpointTamper(t *testing.T) {
	ctx := context.Background()
	p, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	product := []byte(`{"probe":"detect"}`)
	productDigest := canonical.DigestBytes(product)
	next, previous, token, err := p.Store().NextSequence(ctx, "small-001")
	if err != nil {
		t.Fatal(err)
	}
	if next != 1 || previous != 0 {
		t.Fatalf("unexpected initial sequence: %d/%d", next, previous)
	}
	publishProbe(t, p, "small-001", model.StageDetect, 1, 0, token, "input-1", productDigest, product)

	setNextSequence(t, p, "small-001", 2)
	if reason := publishProbeError(t, p, "small-001", model.StageDetect, 1, 0, store.SequenceToken("small-001", 1), "input-1", productDigest, product); reason != "sequence_rollback" {
		t.Fatalf("expected sequence_rollback, got %s", reason)
	}
	if reason := publishProbeError(t, p, "small-001", model.StageDetect, 3, 2, store.SequenceToken("small-001", 3), "input-1", productDigest, product); reason != "sequence_gap" {
		t.Fatalf("expected sequence_gap, got %s", reason)
	}
	if reason := publishProbeError(t, p, "small-001", model.StageRecognize, 2, 0, store.SequenceToken("small-001", 2), "input-1", productDigest, product); reason != "sequence_misaligned" {
		t.Fatalf("expected sequence_misaligned, got %s", reason)
	}
	if reason := publishProbeError(t, p, "small-001", model.StageRecognize, 2, 1, store.SequenceToken("small-002", 2), "input-1", productDigest, product); reason != "cross_shard_sequence" {
		t.Fatalf("expected cross_shard_sequence, got %s", reason)
	}
	setNextSequence(t, p, "small-001", 1)
	if reason := publishProbeError(t, p, "small-001", model.StageDetect, 1, 0, store.SequenceToken("small-001", 1), "input-1", productDigest, product); reason != "duplicate_checkpoint" {
		t.Fatalf("expected duplicate_checkpoint, got %s", reason)
	}
	if reason := publishProbeError(t, p, "small-001", model.StageDetect, 1, 0, store.SequenceToken("small-001", 1), "input-2", productDigest, product); reason != "checkpoint_fingerprint_changed" {
		t.Fatalf("expected checkpoint_fingerprint_changed, got %s", reason)
	}
	other := []byte(`{"probe":"replacement"}`)
	if reason := publishProbeError(t, p, "small-001", model.StageDetect, 1, 0, store.SequenceToken("small-001", 1), "input-1", canonical.DigestBytes(other), other); reason != "checkpoint_digest_mismatch" {
		t.Fatalf("expected checkpoint_digest_mismatch, got %s", reason)
	}
	setNextSequence(t, p, "small-001", 2)
	publishProbe(t, p, "small-001", model.StageRecognize, 2, 1, store.SequenceToken("small-001", 2), "recognition-input", productDigest, product)
	artifact := filepath.Join(p.Paths().Root, "state", "artifacts", "small", "small-001", "detect.json")
	if err := os.WriteFile(artifact, []byte(`{"tampered":true}`), 0o644); err != nil {
		t.Fatal(err)
	}
	var target model.DetectionProduct
	_, _, err = p.LoadProduct(ctx, "small-001", model.StageDetect, &target)
	appErr := apperr.From(err)
	if appErr.Code != apperr.CodeStaleCheckpoint || appErr.Reason != "digest_mismatch" {
		t.Fatalf("expected stale digest mismatch, got %+v", appErr)
	}
}

func TestDiagnosticProbesRecordBoxAndTemplateErrors(t *testing.T) {
	ctx := context.Background()
	p, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(p.Paths().ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Diagnostics.ProbeInvalidBox = true
	cfg.Diagnostics.ProbeEmptyTemplates = true
	if err := config.Save(p.Paths().ConfigPath, cfg); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = Open(ctx, filepath.Dir(p.Paths().ConfigPath))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	summary, err := p.Run(ctx, RunOptions{Groups: []string{"small"}})
	if err != nil {
		t.Fatal(err)
	}
	boxErrors := 0
	templateErrors := 0
	for _, issue := range summary.Issues {
		switch issue.Code {
		case apperr.CodeBoxInvalid:
			boxErrors++
		case apperr.CodeTemplateMiss:
			templateErrors++
		}
	}
	if boxErrors != 4 || templateErrors != 12 {
		t.Fatalf("unexpected probe issues: box=%d template=%d %+v", boxErrors, templateErrors, summary.Issues)
	}
	var diagnostic model.ImageDiagnostic
	_, _, err = p.LoadProduct(ctx, "small-001", model.StageDiagnostic, &diagnostic)
	if err != nil {
		t.Fatal(err)
	}
	foundBox, foundTemplate := false, false
	for _, issue := range diagnostic.Issues {
		if issue.Code == apperr.CodeBoxInvalid && issue.ImageID == "small-001" && issue.BoxIndex > 0 {
			foundBox = true
		}
		if issue.Code == apperr.CodeTemplateMiss && issue.CandidateIndex > 0 {
			foundTemplate = true
		}
	}
	if !foundBox || !foundTemplate {
		t.Fatalf("diagnostic product did not retain stable error codes: %+v", diagnostic.Issues)
	}
	var recognition model.RecognitionProduct
	_, _, err = p.LoadProduct(ctx, "small-001", model.StageDiagnostic, &diagnostic)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = p.LoadProduct(ctx, "small-001", model.StageRecognize, &recognition)
	if err != nil {
		t.Fatal(err)
	}
	if len(recognition.Results) != recognition.CandidateCount || len(recognition.Errors) != recognition.CandidateCount {
		t.Fatalf("template misses were silently skipped: %+v", recognition)
	}
}

func TestConfigChangesInvalidateExpectedEarliestStage(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name              string
		mutate            func(*config.Config)
		wantDetect        int
		wantRecognition   int
		wantDetectionCode string
	}{
		{
			name: "binarization",
			mutate: func(cfg *config.Config) {
				cfg.Detection.Threshold = 120
			},
			wantDetect:      4,
			wantRecognition: 4,
		},
		{
			name: "merge_rule",
			mutate: func(cfg *config.Config) {
				cfg.Detection.MergeGapRatio = 3.25
			},
			wantDetect:      4,
			wantRecognition: 4,
		},
		{
			name: "template_normalization",
			mutate: func(cfg *config.Config) {
				cfg.Recognition.NormWidth = 14
			},
			wantDetect:      0,
			wantRecognition: 4,
		},
		{
			name: "compute_budget",
			mutate: func(cfg *config.Config) {
				cfg.ComputeBudgetUnits = 100
			},
			wantDetect:        4,
			wantRecognition:   4,
			wantDetectionCode: "d-light",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			root := t.TempDir()
			p, err := Open(ctx, root)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.Run(ctx, RunOptions{Groups: []string{"small"}}); err != nil {
				p.Close()
				t.Fatal(err)
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(filepath.Join(root, "config.json"))
			if err != nil {
				t.Fatal(err)
			}
			testCase.mutate(&cfg)
			if err := config.Save(filepath.Join(root, "config.json"), cfg); err != nil {
				t.Fatal(err)
			}
			p, err = Open(ctx, root)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			summary, err := p.Run(ctx, RunOptions{Groups: []string{"small"}})
			if err != nil {
				t.Fatal(err)
			}
			if summary.Stages[0].Executed != testCase.wantDetect || summary.Stages[1].Executed != testCase.wantRecognition {
				t.Fatalf("unexpected invalidation frontier: %+v", summary.Stages)
			}
			if testCase.wantDetectionCode != "" {
				var strategy model.StrategyProduct
				_, _, err := p.LoadProduct(ctx, "small-001", model.StageStrategy, &strategy)
				if err != nil {
					t.Fatal(err)
				}
				if strategy.Detection.Chosen != testCase.wantDetectionCode {
					t.Fatalf("expected budget-constrained strategy %s, got %s", testCase.wantDetectionCode, strategy.Detection.Chosen)
				}
			}
		})
	}
}

func TestPixelChangeInvalidatesOnlyAffectedShard(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	p, err := Open(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Run(ctx, RunOptions{Groups: []string{"small"}}); err != nil {
		p.Close()
		t.Fatal(err)
	}
	imagePath := filepath.Join(root, "data", "images", "small", "small-001.png")
	imageBytes, err := os.ReadFile(imagePath)
	if err != nil {
		p.Close()
		t.Fatal(err)
	}
	// Append a byte after a valid PNG IEND chunk; the PNG decoder still accepts
	// the image and the content fingerprint must change.
	if err := os.WriteFile(imagePath, append(imageBytes, 0x00), 0o644); err != nil {
		p.Close()
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = Open(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	summary, err := p.Run(ctx, RunOptions{Groups: []string{"small"}})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Stages[0].Executed != 1 || summary.Stages[0].Reused != 3 {
		t.Fatalf("pixel change should invalidate exactly one shard's detection: %+v", summary.Stages[0])
	}
}

func TestBundleDifferenceReportsFirstItem(t *testing.T) {
	left := Bundle{SchemaVersion: "v", ManifestDigest: "m", ConfigVersion: "c", Products: []BundleProduct{
		{ImageID: "a", Stage: model.StageDetect, Digest: "1", Product: json.RawMessage(`{"x":1}`)},
		{ImageID: "a", Stage: model.StageRecognize, Digest: "2", Product: json.RawMessage(`{"x":2}`)},
	}}
	right := Bundle{SchemaVersion: "v", ManifestDigest: "m", ConfigVersion: "c", Products: []BundleProduct{
		{ImageID: "a", Stage: model.StageDetect, Digest: "1", Product: json.RawMessage(`{"x":1}`)},
		{ImageID: "a", Stage: model.StageRecognize, Digest: "3", Product: json.RawMessage(`{"x":3}`)},
	}}
	leftBytes, _ := canonical.Marshal(left)
	rightBytes, _ := canonical.Marshal(right)
	if first := firstBundleDifference(leftBytes, rightBytes); first != "a/recognize" {
		t.Fatalf("expected first differing stage item, got %s", first)
	}
}

func loadRawDigest(t *testing.T, p *Pipeline, imageID, stage string, target *json.RawMessage) string {
	t.Helper()
	_, digest, err := p.LoadProduct(context.Background(), imageID, stage, target)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func setNextSequence(t *testing.T, p *Pipeline, imageID string, next int64) {
	t.Helper()
	if _, err := p.Store().DB().ExecContext(context.Background(), `UPDATE shard_state SET next_seq=? WHERE image_id=?`, next, imageID); err != nil {
		t.Fatal(err)
	}
}

func publishProbe(t *testing.T, p *Pipeline, imageID, stage string, seq, previous int64, token, inputVersion, digest string, product []byte) {
	t.Helper()
	err := p.Store().Publish(context.Background(), store.PublishRequest{
		ImageID: imageID, Stage: stage, Seq: seq, ExpectedPrevSeq: previous, SequenceToken: token,
		InputVersion: inputVersion, ConfigVersion: p.Config().Version(), ProductDigest: digest,
		ArtifactRel: filepath.ToSlash(filepath.Join("state", "artifacts", "small", imageID, stage+".json")), Product: product,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func publishProbeError(t *testing.T, p *Pipeline, imageID, stage string, seq, previous int64, token, inputVersion, digest string, product []byte) string {
	t.Helper()
	err := p.Store().Publish(context.Background(), store.PublishRequest{
		ImageID: imageID, Stage: stage, Seq: seq, ExpectedPrevSeq: previous, SequenceToken: token,
		InputVersion: inputVersion, ConfigVersion: p.Config().Version(), ProductDigest: digest,
		ArtifactRel: filepath.ToSlash(filepath.Join("state", "artifacts", "small", imageID, stage+".json")), Product: product,
	})
	appErr := apperr.From(err)
	if appErr.Code != apperr.CodeStaleCheckpoint {
		t.Fatalf("expected E_STALE_CHECKPOINT, got %+v", appErr)
	}
	return appErr.Reason
}
