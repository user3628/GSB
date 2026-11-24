package rewardpack

import (
	"context"
	"encoding/json"
	"math"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
)

func testConfig(rows, dimension int) Config {
	cfg := DefaultConfig()
	cfg.Version = 1
	cfg.Scoring.TermWeights = map[string]float64{"good": 2, "好": 2, "bad": 0.5}
	cfg.Scoring.DefaultTermWeight = 1
	cfg.Scoring.LengthPenalty = 0.2
	cfg.Scoring.FactorFloors = FactorFloors{Length: 1e-4, Overlap: 1e-4, Consistency: 1e-4}
	cfg.Features.Dimension = dimension
	cfg.Features.QuantizationBits = 8
	cfg.Features.Projection = make([][]float64, rows)
	for row := range cfg.Features.Projection {
		cfg.Features.Projection[row] = make([]float64, dimension)
		for column := range cfg.Features.Projection[row] {
			cfg.Features.Projection[row][column] = float64((row+1)*(column+2)%7-3) / 5
			if row == column {
				cfg.Features.Projection[row][column] += 0.4
			}
		}
	}
	cfg.Budget.GlobalTokens = 100
	cfg.Budget.MinRetentionUnit = 1
	cfg.Budget.DefaultSourceQuota = 100
	return cfg
}

func TestNormalizePairDeterministicOffsetsAndEmpty(t *testing.T) {
	cfg := testConfig(4, 3)
	annotation := Annotation{
		PairID: "p-norm", Prompt: "ＧＯＯＤ　prompt", Chosen: "ＡＢＣ　好\t世界",
		Rejected: "bad response", Source: "s", Lang: "mixed", Consistency: 0.7,
	}
	first, err := NormalizePair(annotation, cfg)
	if err != nil {
		t.Fatalf("NormalizePair: %v", err)
	}
	second, err := NormalizePair(annotation, cfg)
	if err != nil {
		t.Fatalf("NormalizePair second: %v", err)
	}
	if !strings.Contains(first.Chosen.Normalized, "abc") || !strings.Contains(first.Chosen.Normalized, "好 世界") {
		t.Fatalf("unexpected normalization: %q", first.Chosen.Normalized)
	}
	if got, want := CanonicalString(first), CanonicalString(second); got != want {
		t.Fatalf("normalization is not deterministic\nfirst=%s\nsecond=%s", got, want)
	}
	runes := []rune(first.Chosen.Normalized)
	for _, token := range first.Chosen.Tokens {
		if string(runes[token.Start:token.End]) != token.Text {
			t.Errorf("rune span mismatch for %q: %d:%d", token.Text, token.Start, token.End)
		}
		if first.Chosen.Normalized[token.ByteStart:token.ByteEnd] != token.Text {
			t.Errorf("byte span mismatch for %q: %d:%d", token.Text, token.ByteStart, token.ByteEnd)
		}
	}
	if first.Chosen.Language != "mixed" || first.Rejected.Language != "en" {
		t.Fatalf("unexpected language markers: chosen=%s rejected=%s", first.Chosen.Language, first.Rejected.Language)
	}

	empty := annotation
	empty.PairID = "p-empty"
	empty.Chosen = " \t "
	if _, err := NormalizePair(empty, cfg); !HasCode(err, CodeEmptyText) {
		t.Fatalf("expected E_EMPTY_TEXT, got %v", err)
	} else {
		var be *BusinessError
		_ = json.Unmarshal([]byte(CanonicalString(err)), &be)
		if be.PairID != "p-empty" || be.Field != "chosen" {
			t.Fatalf("empty context missing: %#v", be)
		}
	}
	empty.Chosen = "!!!"
	empty.Rejected = "？？？"
	if _, err := NormalizePair(empty, cfg); !HasCode(err, CodeEmptyText) {
		t.Fatalf("expected rejected E_EMPTY_TEXT, got %v", err)
	}
}

func TestScoreFactorsFallbackAndNumericError(t *testing.T) {
	cfg := testConfig(4, 3)
	annotation := Annotation{
		PairID: "p-score", Prompt: "good 好 clear", Chosen: "准确 good answer 好",
		Rejected: "bad reply", Source: "s", Lang: "mixed", Consistency: 0.91,
	}
	normalized, err := NormalizePair(annotation, cfg)
	if err != nil {
		t.Fatal(err)
	}
	score, err := ScorePair(normalized, annotation, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(score.Factors) != 3 {
		t.Fatalf("expected three factors, got %d", len(score.Factors))
	}
	logSum := 0.0
	for _, factor := range score.Factors {
		if !finite(factor.Value) || factor.Value <= 0 || !finite(factor.LogContribution) {
			t.Fatalf("invalid factor %#v", factor)
		}
		logSum += factor.LogContribution
	}
	if math.Abs(logSum-score.LogStrength) > 1e-12 {
		t.Fatalf("log contribution mismatch: sum=%v log=%v", logSum, score.LogStrength)
	}
	strength, text := roundSignificant(math.Exp(score.LogStrength), cfg.Scoring.StrengthSignificantDigits)
	if score.Strength != strength || score.StrengthText != text {
		t.Fatalf("significant digit rounding mismatch: %#v", score)
	}

	outOfRange := annotation
	outOfRange.Consistency = 1.8
	normalizedOut, _ := NormalizePair(outOfRange, cfg)
	scoreOut, err := ScorePair(normalizedOut, outOfRange, cfg)
	if err != nil {
		t.Fatal(err)
	}
	foundFallback := false
	for _, factor := range scoreOut.Factors {
		if factor.Name == factorConsistency {
			foundFallback = factor.Fallback && factor.FallbackReason == "annotation_consistency_out_of_range" && factor.Value == cfg.Scoring.FactorFloors.Consistency
		}
	}
	if !foundFallback {
		t.Fatalf("out-of-range consistency did not fall back: %#v", scoreOut.Factors)
	}

	nanAnnotation := annotation
	nanAnnotation.PairID = "p-nan"
	nanAnnotation.Consistency = math.NaN()
	nanNormalized, err := NormalizePair(nanAnnotation, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ScorePair(nanNormalized, nanAnnotation, cfg); !HasCode(err, CodeNumeric) {
		t.Fatalf("expected E_NUMERIC for NaN consistency, got %v", err)
	}

	noPrompt := annotation
	noPrompt.Prompt = ""
	noPromptNormalized, _ := NormalizePair(noPrompt, cfg)
	noPromptScore, err := ScorePair(noPromptNormalized, noPrompt, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, factor := range noPromptScore.Factors {
		if factor.Name == factorOverlap && (!factor.Fallback || factor.FallbackReason != "prompt_has_no_weighted_terms") {
			t.Fatalf("empty prompt overlap did not document fallback: %#v", factor)
		}
	}
}

func TestFeatureQuantizationBoundsAndDimensions(t *testing.T) {
	cfg := testConfig(4, 3)
	annotation := Annotation{PairID: "p-feature", Prompt: "good 好", Chosen: "good 好 answer", Rejected: "bad reply", Source: "s", Lang: "mixed", Consistency: 0.8}
	normalized, err := NormalizePair(annotation, cfg)
	if err != nil {
		t.Fatal(err)
	}
	score, err := ScorePair(normalized, annotation, cfg)
	if err != nil {
		t.Fatal(err)
	}
	feature, err := BuildFeatures(normalized, score, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(feature.Dimensions) != 4 {
		t.Fatalf("expected 4 dimensions, got %d", len(feature.Dimensions))
	}
	for _, dimension := range feature.Dimensions {
		if dimension.Scale <= 0 || dimension.ErrorBound <= 0 {
			t.Fatalf("invalid scale/error bound: %#v", dimension)
		}
		if math.Abs(dimension.ErrorBound-0.5/dimension.Scale) > 1e-18 {
			t.Fatalf("error bound not derived from scale: %#v", dimension)
		}
		if dimension.ObservedError > dimension.ErrorBound+1e-15 {
			t.Fatalf("observed error exceeds bound: %#v", dimension)
		}
		if _, _, err := big.ParseFloat(dimension.Reference, 10, 256, big.ToNearestEven); err != nil {
			t.Fatalf("reference is not high-precision text: %q: %v", dimension.Reference, err)
		}
	}
	again, err := BuildFeatures(normalized, score, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if CanonicalString(feature) != CanonicalString(again) {
		t.Fatal("feature output is not deterministic")
	}

	badWidth := cfg
	badWidth.Features.Projection = [][]float64{{1, 2}}
	if _, err := BuildFeatures(normalized, score, badWidth); !HasCode(err, CodeDimMismatch) {
		t.Fatalf("expected E_DIM_MISMATCH, got %v", err)
	} else {
		be := err.(*BusinessError)
		if be.Row != 1 || be.Column != 3 {
			t.Fatalf("wrong dimension context: %#v", be)
		}
	}

	nanMatrix := cfg
	nanMatrix.Features.Projection = cloneMatrix(cfg.Features.Projection)
	nanMatrix.Features.Projection[0][1] = math.NaN()
	if _, err := BuildFeatures(normalized, score, nanMatrix); !HasCode(err, CodeDimMismatch) {
		t.Fatalf("expected E_DIM_MISMATCH for NaN matrix, got %v", err)
	} else {
		be := err.(*BusinessError)
		if be.Row != 1 || be.Column != 2 {
			t.Fatalf("wrong NaN context: %#v", be)
		}
	}

	zeroMatrix := cfg
	zeroMatrix.Features.Projection = [][]float64{{0, 0, 0}, {0, 0, 0}}
	if _, err := BuildFeatures(normalized, score, zeroMatrix); !HasCode(err, CodeDimMismatch) {
		t.Fatalf("expected E_DIM_MISMATCH for zero quantization scale, got %v", err)
	}
}

func cloneMatrix(input [][]float64) [][]float64 {
	out := make([][]float64, len(input))
	for i := range input {
		out[i] = append([]float64(nil), input[i]...)
	}
	return out
}

func TestBudgetDeterministicCoverageAndEmptyLayers(t *testing.T) {
	cfg := testConfig(4, 3)
	cfg.Budget.GlobalTokens = 10
	cfg.Budget.MinRetentionUnit = 2
	cfg.Budget.DefaultSourceQuota = 10
	cfg.Budget.SourceQuotas = map[string]int{"a": 6, "b": 4, "c": 1, "empty": 5}
	cfg.Budget.RequiredLanguages = []string{"zh", "en"}
	cfg.Budget.LanguageMinimums = map[string]int{"zh": 1, "en": 1}
	scores := []ScoreResult{
		{PairID: "a-1", Source: "a", Lang: "zh", Strength: 0.5, TokenCost: 3},
		{PairID: "a-2", Source: "a", Lang: "en", Strength: 0.9, TokenCost: 3},
		{PairID: "a-3", Source: "a", Lang: "en", Strength: 0.9, TokenCost: 3},
		{PairID: "b-1", Source: "b", Lang: "en", Strength: 0.95, TokenCost: 4},
		{PairID: "c-1", Source: "c", Lang: "zh", Strength: 0.99, TokenCost: 5},
	}
	budget, err := DecideBudget(scores, cfg, []string{"a", "b", "c", "empty"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := DecideBudget(scores, cfg, []string{"a", "b", "c", "empty"})
	if err != nil {
		t.Fatal(err)
	}
	if CanonicalString(budget) != CanonicalString(again) {
		t.Fatal("budget decision is not deterministic")
	}
	if budget.UsedTokens > cfg.Budget.GlobalTokens {
		t.Fatalf("global budget exceeded: %#v", budget)
	}
	if len(budget.Retained) == 0 {
		t.Fatal("expected retained pairs")
	}
	var sawEmpty, sawSmall bool
	for _, layer := range budget.SourceLayers {
		switch layer.Source {
		case "empty":
			sawEmpty = layer.Status == "EMPTY" && layer.AvailablePairs == 0
		case "c":
			sawSmall = layer.Status == "QUOTA_BELOW_MIN_UNIT" && layer.KeptPairs == 0
		}
	}
	if !sawEmpty || !sawSmall {
		t.Fatalf("missing empty/small source layer: %#v", budget.SourceLayers)
	}
	if len(budget.Eliminated) == 0 {
		t.Fatal("expected elimination decisions")
	}
	for _, eliminated := range budget.Eliminated {
		if eliminated.Reason == "" || eliminated.Detail == "" {
			t.Fatalf("elimination lacks evidence: %#v", eliminated)
		}
	}
	retained := map[string]bool{}
	for _, pair := range budget.Retained {
		retained[pair.PairID] = true
	}
	if !retained["a-1"] {
		t.Fatalf("language coverage did not retain zh pair: %#v", budget.Retained)
	}
	if !retained["b-1"] {
		t.Fatalf("higher-strength en pair was not retained: %#v", budget.Retained)
	}
}

func TestSampleBuildAndIncrementalEditAndReplacement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := OpenStoreContext(ctx, filepath.Join(dir, "reward.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cfg := SampleConfig()
	if err := store.ApplyConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	annotations := sampleAnnotations(42)
	bySource := map[string][]Annotation{}
	for _, annotation := range annotations {
		bySource[annotation.Source] = append(bySource[annotation.Source], annotation)
	}
	for _, source := range sortedStringKeys(bySource) {
		if err := store.ReplaceSource(ctx, source, bySource[source]); err != nil {
			t.Fatal(err)
		}
	}
	engine := NewEngine(store)
	initial, err := engine.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if initial.ChangeKind != ChangeInitialBuild {
		t.Fatalf("unexpected initial change kind: %s", initial.ChangeKind)
	}
	if len(initial.Bundle.Budget.SourceLayers) < 4 {
		t.Fatalf("expected empty and active source layers: %#v", initial.Bundle.Budget.SourceLayers)
	}
	beforeScores, err := store.LoadPairArtifacts(ctx, StageScore)
	if err != nil {
		t.Fatal(err)
	}

	edited := annotations[0]
	edited.Consistency = 0.42
	if err := store.UpsertAnnotation(ctx, edited); err != nil {
		t.Fatal(err)
	}
	afterEdit, err := engine.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if afterEdit.ChangeKind != ChangeAnnotationEdit {
		t.Fatalf("expected annotation_edit, got %s", afterEdit.ChangeKind)
	}
	if got := afterEdit.RebuiltPairs[StageScore]; len(got) != 1 || got[0] != edited.PairID {
		t.Fatalf("edit rebuilt too much: %#v", afterEdit.RebuiltPairs)
	}
	afterScores, err := store.LoadPairArtifacts(ctx, StageScore)
	if err != nil {
		t.Fatal(err)
	}
	for pairID, before := range beforeScores {
		if pairID == edited.PairID {
			continue
		}
		if afterScores[pairID].Digest != before.Digest {
			t.Fatalf("unaffected pair artifact changed: %s", pairID)
		}
	}
	if _, err := engine.VerifyIncremental(ctx); err != nil {
		t.Fatalf("incremental edit verification failed: %v", err)
	}

	// Replace one complete source and prove other source artifacts are untouched.
	shardA, _ := store.LoadPairArtifacts(ctx, StageScore)
	shardBPairs := append([]Annotation(nil), bySource["shard-b"]...)
	shardBPairs[0].Chosen += " with a clear cap"
	if err := store.ReplaceSource(ctx, "shard-b", shardBPairs); err != nil {
		t.Fatal(err)
	}
	afterReplace, err := engine.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if afterReplace.ChangeKind != ChangeSourceReplace {
		t.Fatalf("expected source_replace, got %s", afterReplace.ChangeKind)
	}
	if afterReplace.Invalidation == nil || len(afterReplace.Invalidation.AffectedSources) != 1 || afterReplace.Invalidation.AffectedSources[0] != "shard-b" {
		t.Fatalf("replacement escaped source scope: %#v", afterReplace.Invalidation)
	}
	if afterReplace.Invalidation.FirstDiffPairID != shardBPairs[0].PairID {
		t.Fatalf("unexpected first diff: %#v", afterReplace.Invalidation)
	}
	shardAAfter, _ := store.LoadPairArtifacts(ctx, StageScore)
	for pairID, artifact := range shardA {
		if strings.HasPrefix(pairID, "a-") && shardAAfter[pairID].Digest != artifact.Digest {
			t.Fatalf("unrelated source artifact changed after replacement: %s", pairID)
		}
	}
	if _, err := engine.VerifyIncremental(ctx); err != nil {
		t.Fatalf("incremental replacement verification failed: %v", err)
	}

	if err := store.ReplaceSource(ctx, "shard-b", shardBPairs); err != nil {
		t.Fatal(err)
	}
	identicalReplace, err := engine.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if identicalReplace.ChangeKind != ChangeNoop {
		t.Fatalf("identical source replacement should be a no-op, got %s", identicalReplace.ChangeKind)
	}
}

func TestGracefulStopResumeMatchesFull(t *testing.T) {
	ctx := context.Background()
	stoppedStore := newSampleStore(t, ctx)
	normalStore := newSampleStore(t, ctx)
	path := stoppedStore.Path()
	_, err := NewEngine(stoppedStore).Build(ctx, BuildOptions{StopAfterUnits: len(sampleAnnotations(42)) + 1})
	if !HasCode(err, CodeStopped) {
		t.Fatalf("expected E_STOPPED, got %v", err)
	}
	normalizeState, exists, stateErr := stoppedStore.StageState(ctx, StageNormalize)
	if stateErr != nil || !exists || normalizeState.Sequence != 1 {
		t.Fatalf("expected committed normalize checkpoint before forced resume: state=%#v exists=%v err=%v", normalizeState, exists, stateErr)
	}
	if _, exists, _ := stoppedStore.StageState(ctx, StageScore); exists {
		t.Fatal("partial score stage must not be published")
	}
	if err := stoppedStore.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStoreContext(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	resumed, err := NewEngine(reopened).Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	full, err := NewEngine(normalStore).Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if CanonicalString(resumed.Bundle.Scores) != CanonicalString(full.Bundle.Scores) {
		t.Fatal("resumed scores differ from full build")
	}
	if CanonicalString(resumed.Bundle.Features) != CanonicalString(full.Bundle.Features) {
		t.Fatal("resumed features differ from full build")
	}
	if CanonicalString(resumed.Bundle.Budget) != CanonicalString(full.Bundle.Budget) {
		t.Fatal("resumed budget differs from full build")
	}
	if CanonicalString(resumed.Bundle.Report) != CanonicalString(full.Bundle.Report) {
		t.Fatal("resumed report differs from full build")
	}
	if _, err := NewEngine(reopened).VerifyIncremental(ctx); err != nil {
		t.Fatalf("resumed store failed full consistency check: %v", err)
	}
}

func TestEmptySourceLayerAndPipelineEmptyTextError(t *testing.T) {
	ctx := context.Background()
	store, err := OpenStoreContext(ctx, filepath.Join(t.TempDir(), "reward.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cfg := testConfig(4, 3)
	cfg.Version = 1
	cfg.Budget.GlobalTokens = 100
	cfg.Budget.MinRetentionUnit = 1
	cfg.Budget.DefaultSourceQuota = 100
	if err := store.ApplyConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceSource(ctx, "empty-src", nil); err != nil {
		t.Fatal(err)
	}
	valid := Annotation{PairID: "v1", Prompt: "good", Chosen: "good reply", Rejected: "bad reply", Source: "active", Lang: "en", Consistency: 0.8}
	if err := store.ReplaceSource(ctx, "active", []Annotation{valid}); err != nil {
		t.Fatal(err)
	}
	result, err := NewEngine(store).Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, layer := range result.Bundle.Budget.SourceLayers {
		if layer.Source == "empty-src" {
			found = layer.Status == "EMPTY" && layer.AvailablePairs == 0 && strings.Contains(layer.Note, "empty layer")
		}
	}
	if !found {
		t.Fatalf("empty source layer missing: %#v", result.Bundle.Budget.SourceLayers)
	}
	if _, err := NewEngine(store).VerifyIncremental(ctx); err != nil {
		t.Fatalf("full build with empty source must include the same empty layer: %v", err)
	}

	bad := valid
	bad.PairID = "bad-empty"
	bad.Source = "active"
	bad.Rejected = "！！！"
	if err := store.UpsertAnnotation(ctx, bad); err != nil {
		t.Fatal(err)
	}
	if _, err := NewEngine(store).Build(ctx, BuildOptions{}); !HasCode(err, CodeEmptyText) {
		t.Fatalf("pipeline should surface E_EMPTY_TEXT, got %v", err)
	}
	if err := store.ReplaceSource(ctx, "active", nil); err != nil {
		t.Fatal(err)
	}
	emptyBuild, err := NewEngine(store).Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatalf("build with only empty sources: %v", err)
	}
	if emptyBuild.Bundle.Budget.UsedTokens != 0 || len(emptyBuild.Bundle.Budget.Retained) != 0 {
		t.Fatalf("empty sources retained pairs: %#v", emptyBuild.Bundle.Budget)
	}
	if _, err := NewEngine(store).VerifyIncremental(ctx); err != nil {
		t.Fatalf("empty-source incremental/full comparison failed: %v", err)
	}
}

func TestNaNProjectionFailsAtFeatureStageWithContext(t *testing.T) {
	ctx := context.Background()
	store, err := OpenStoreContext(ctx, filepath.Join(t.TempDir(), "reward.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cfg := testConfig(4, 3)
	cfg.Version = 1
	cfg.Features.Projection[0][1] = math.NaN()
	if err := store.ApplyConfig(ctx, cfg); err != nil {
		t.Fatalf("config with NaN matrix should be accepted so feature stage can report it: %v", err)
	}
	annotation := Annotation{PairID: "nan-matrix", Prompt: "good", Chosen: "good reply", Rejected: "bad reply", Source: "s", Lang: "en", Consistency: 0.8}
	if err := store.ReplaceSource(ctx, "s", []Annotation{annotation}); err != nil {
		t.Fatal(err)
	}
	_, err = NewEngine(store).Build(ctx, BuildOptions{})
	if !HasCode(err, CodeDimMismatch) {
		t.Fatalf("expected E_DIM_MISMATCH, got %v", err)
	}
	be := err.(*BusinessError)
	if be.Stage != StageFeatures || be.Row != 1 || be.Column != 2 {
		t.Fatalf("wrong feature-stage NaN context: %#v", be)
	}
	if _, exists, err := store.StageState(ctx, StageNormalize); err != nil || !exists {
		t.Fatalf("normalize should be committed before feature failure: exists=%v err=%v", exists, err)
	}
	if _, exists, err := store.StageState(ctx, StageFeatures); err != nil || exists {
		t.Fatalf("feature stage must not be committed after NaN failure: exists=%v err=%v", exists, err)
	}
}

func newSampleStore(t *testing.T, ctx context.Context) *Store {
	t.Helper()
	store, err := OpenStoreContext(ctx, filepath.Join(t.TempDir(), "reward.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.ApplyConfig(ctx, SampleConfig()); err != nil {
		t.Fatal(err)
	}
	bySource := map[string][]Annotation{}
	for _, annotation := range sampleAnnotations(42) {
		bySource[annotation.Source] = append(bySource[annotation.Source], annotation)
	}
	for _, source := range sortedStringKeys(bySource) {
		if err := store.ReplaceSource(ctx, source, bySource[source]); err != nil {
			t.Fatal(err)
		}
	}
	return store
}
