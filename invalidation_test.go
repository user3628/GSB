package rewardpack

import (
	"context"
	"path/filepath"
	"testing"
)

func TestAppendEditRollbackAndConfigInvalidationScopes(t *testing.T) {
	ctx := context.Background()
	store, err := OpenStoreContext(ctx, filepath.Join(t.TempDir(), "reward.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cfg := testConfig(4, 3)
	cfg.Version = 1
	cfg.Budget.GlobalTokens = 500
	cfg.Budget.DefaultSourceQuota = 500
	cfg.Budget.MinRetentionUnit = 1
	if err := store.ApplyConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	original := []Annotation{
		{PairID: "p1", Prompt: "good 好", Chosen: "good 好 answer", Rejected: "bad reply", Source: "s1", Lang: "mixed", Consistency: 0.7},
		{PairID: "p2", Prompt: "clear 清楚", Chosen: "clear answer", Rejected: "模糊 reply", Source: "s1", Lang: "mixed", Consistency: 0.8},
		{PairID: "p3", Prompt: "英文", Chosen: "good english reply", Rejected: "bad english reply", Source: "s2", Lang: "en", Consistency: 0.9},
	}
	if err := store.ReplaceSource(ctx, "s1", []Annotation{original[0], original[1]}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceSource(ctx, "s2", []Annotation{original[2]}); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(store)
	if _, err := engine.Build(ctx, BuildOptions{}); err != nil {
		t.Fatal(err)
	}

	appended := Annotation{PairID: "p0", Prompt: "新问题", Chosen: "新的 good answer", Rejected: "bad answer", Source: "s1", Lang: "mixed", Consistency: 0.6}
	if err := store.UpsertAnnotation(ctx, appended); err != nil {
		t.Fatal(err)
	}
	appendResult, err := engine.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if appendResult.ChangeKind != ChangeAppend {
		t.Fatalf("expected append, got %s", appendResult.ChangeKind)
	}
	if got := appendResult.RebuiltPairs[StageNormalize]; len(got) != 1 || got[0] != "p0" {
		t.Fatalf("append rebuilt wrong pairs: %#v", got)
	}
	if appendResult.Invalidation == nil || appendResult.Invalidation.FirstDiffPairID != "p0" {
		t.Fatalf("append invalidation lacks first diff: %#v", appendResult.Invalidation)
	}

	edited := original[0]
	edited.Consistency = 0.1
	if err := store.UpsertAnnotation(ctx, edited); err != nil {
		t.Fatal(err)
	}
	editResult, err := engine.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if editResult.ChangeKind != ChangeAnnotationEdit {
		t.Fatalf("expected edit, got %s", editResult.ChangeKind)
	}
	if got := editResult.RebuiltPairs[StageNormalize]; len(got) != 1 || got[0] != "p1" {
		t.Fatalf("edit rebuilt wrong pairs: %#v", got)
	}

	if err := store.UpsertAnnotation(ctx, original[0]); err != nil {
		t.Fatal(err)
	}
	rollbackResult, err := engine.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if rollbackResult.ChangeKind != ChangeRollback {
		t.Fatalf("expected content rollback, got %s", rollbackResult.ChangeKind)
	}
	if rollbackResult.Invalidation == nil || rollbackResult.Invalidation.FirstDiffPairID != "p1" {
		t.Fatalf("rollback invalidation lacks first diff: %#v", rollbackResult.Invalidation)
	}
	if _, err := engine.VerifyIncremental(ctx); err != nil {
		t.Fatalf("rollback verification failed: %v", err)
	}

	scoreCfg := cfg
	scoreCfg.Version = 2
	scoreCfg.Scoring.LengthPenalty = 0.55
	if err := store.ApplyConfig(ctx, scoreCfg); err != nil {
		t.Fatal(err)
	}
	scoreResult, err := engine.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if scoreResult.Invalidation == nil || scoreResult.Invalidation.EarliestStage != StageScore || !scoreResult.Invalidation.AllPairs {
		t.Fatalf("weight change scope is wrong: %#v", scoreResult.Invalidation)
	}
	if len(scoreResult.RebuiltPairs[StageNormalize]) != 0 {
		t.Fatalf("score config change rebuilt normalization: %#v", scoreResult.RebuiltPairs)
	}
	if len(scoreResult.RebuiltPairs[StageScore]) != len(original)+1 {
		t.Fatalf("score config change did not rebuild all scores: %#v", scoreResult.RebuiltPairs)
	}

	featureCfg := scoreCfg
	featureCfg.Version = 3
	featureCfg.Features.Projection = cloneMatrix(scoreCfg.Features.Projection)
	featureCfg.Features.Projection[0][0] += 0.25
	if err := store.ApplyConfig(ctx, featureCfg); err != nil {
		t.Fatal(err)
	}
	featureResult, err := engine.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if featureResult.Invalidation == nil || featureResult.Invalidation.EarliestStage != StageFeatures {
		t.Fatalf("projection change scope is wrong: %#v", featureResult.Invalidation)
	}
	if len(featureResult.RebuiltPairs[StageScore]) != 0 || len(featureResult.RebuiltPairs[StageFeatures]) != len(original)+1 {
		t.Fatalf("projection change rebuilt wrong stages: %#v", featureResult.RebuiltPairs)
	}

	budgetCfg := featureCfg
	budgetCfg.Version = 4
	budgetCfg.Budget.GlobalTokens = 300
	if err := store.ApplyConfig(ctx, budgetCfg); err != nil {
		t.Fatal(err)
	}
	budgetResult, err := engine.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if budgetResult.Invalidation == nil || budgetResult.Invalidation.EarliestStage != StageBudget {
		t.Fatalf("budget change scope is wrong: %#v", budgetResult.Invalidation)
	}
	if len(budgetResult.RebuiltPairs[StageFeatures]) != 0 {
		t.Fatalf("budget change rebuilt features: %#v", budgetResult.RebuiltPairs)
	}
	if _, err := engine.VerifyIncremental(ctx); err != nil {
		t.Fatalf("config invalidation verification failed: %v", err)
	}

	if err := store.ApplyConfig(ctx, featureCfg); !HasCode(err, CodeVersionRollback) {
		t.Fatalf("expected config version rollback, got %v", err)
	}
}
