package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"

	"gopkg.in/yaml.v3"

	"searchlab/internal/apperr"
	"searchlab/internal/checkpoint"
	"searchlab/internal/config"
	"searchlab/internal/engine"
	"searchlab/internal/generator"
	"searchlab/internal/hp"
	"searchlab/internal/model"
)

type demoStageResult struct {
	Stage          string `json:"stage"`
	InputDigest    string `json:"input_digest"`
	ArtifactDigest string `json:"artifact_digest"`
	Reused         bool   `json:"reused"`
}

type demoBuildResult struct {
	PipelineID   string            `json:"pipeline_id"`
	ConfigDigest string            `json:"config_digest"`
	IndexVersion string            `json:"index_version"`
	Stages       []demoStageResult `json:"stages"`
}

type demoQueryResult struct {
	QueryID          string  `json:"query_id"`
	Status           string  `json:"status"`
	Code             string  `json:"code,omitempty"`
	Hits             int     `json:"hits"`
	RankOrder        []int64 `json:"rank_order"`
	ReferenceOK      bool    `json:"reference_ok"`
	CacheKey         string  `json:"cache_key"`
	CacheHit         bool    `json:"cache_hit"`
	CacheReplayEqual bool    `json:"cache_replay_byte_equal"`
	DiagnosticDigest string  `json:"diagnostic_digest"`
}

type demoIncrementalResult struct {
	EarliestAffectedStage string          `json:"earliest_affected_stage"`
	Reused                []string        `json:"reused_stages"`
	Rebuilt               []string        `json:"rebuilt_stages"`
	Incremental           demoBuildResult `json:"incremental_build"`
	Fresh                 demoBuildResult `json:"fresh_build"`
	FreshArtifactsMatch   bool            `json:"fresh_artifacts_match"`
	FreshIndexMatch       bool            `json:"fresh_index_version_match"`
	OriginalCacheKey      string          `json:"original_cache_key"`
	ChangedCacheKey       string          `json:"changed_cache_key"`
	CacheKeyChanged       bool            `json:"cache_key_changed"`
}

type demoConcurrencyResult struct {
	Processes         int      `json:"processes"`
	Succeeded         int      `json:"succeeded"`
	Stale             int      `json:"stale"`
	Codes             []string `json:"codes"`
	PrereqMissingCode string   `json:"prereq_missing_code"`
}

type demoVerificationSummary struct {
	FormatVersion           string                `json:"format_version"`
	GeneratorSeed           int64                 `json:"generator_seed"`
	GeneratorSummary        string                `json:"generator_summary_sha256"`
	ConfigDigest            string                `json:"config_digest"`
	ShardDigest             string                `json:"shard_digest"`
	ReferenceDigest         string                `json:"reference_sha256"`
	FreshBuild              demoBuildResult       `json:"fresh_build"`
	ReferenceArtifactsMatch bool                  `json:"reference_artifacts_match"`
	IncrementalNoChange     demoBuildResult       `json:"no_change_rebuild"`
	Queries                 []demoQueryResult     `json:"queries"`
	Incremental             demoIncrementalResult `json:"config_change"`
	Concurrency             demoConcurrencyResult `json:"concurrency"`
	ErrorCodes              map[string]string     `json:"error_codes"`
}

func runDemoWorkflow(outDir string, seed int64) error {
	summary, err := generator.Generate(outDir, seed)
	if err != nil {
		return err
	}
	stateDir := filepath.Join(outDir, "state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return err
	}
	for _, name := range []string{"searchlab.db", "searchlab.db-wal", "searchlab.db-shm", "searchlab.db.lock", "incremental.db", "incremental.db-wal", "incremental.db-shm", "incremental.db.lock", "fresh-changed.db", "fresh-changed.db-wal", "fresh-changed.db-shm", "fresh-changed.db.lock", "concurrency.db", "concurrency.db-wal", "concurrency.db-shm", "concurrency.db.lock"} {
		_ = os.Remove(filepath.Join(stateDir, name))
	}
	cfg, _, err := config.Load(filepath.Join(outDir, "config.yaml"))
	if err != nil {
		return err
	}
	corpus, err := model.LoadShards(filepath.Join(outDir, "shards"))
	if err != nil {
		return err
	}
	dbPath := filepath.Join(stateDir, "searchlab.db")
	lockPath := dbPath + ".lock"
	store, err := checkpoint.Open(dbPath, lockPath)
	if err != nil {
		return err
	}
	freshReport, err := engine.Build(context.Background(), cfg, corpus, store, true)
	if err != nil {
		store.Close()
		return err
	}
	freshBuild := toDemoBuild(freshReport)
	var reference generator.Reference
	referencePayload, err := os.ReadFile(filepath.Join(outDir, "reference.json"))
	if err != nil {
		store.Close()
		return err
	}
	if err := json.Unmarshal(referencePayload, &reference); err != nil {
		store.Close()
		return err
	}
	referenceArtifactsMatch := reportArtifactsMatch(freshReport, reference.Artifacts)
	runtime, err := engine.LoadRuntime(context.Background(), store)
	if err != nil {
		store.Close()
		return err
	}
	queryEngine := engine.NewQueryEngine(runtime)
	queryResults := make([]demoQueryResult, 0, len(reference.Queries))
	for _, fixture := range reference.Queries {
		req := engine.QueryRequest{QueryID: fixture.QueryID, Terms: fixture.RawTerms, Mode: fixture.Mode}
		first, firstPayload, firstHit, err := queryEngine.Execute(context.Background(), req)
		if err != nil {
			store.Close()
			return err
		}
		_, replayPayload, replayHit, err := queryEngine.Execute(context.Background(), req)
		if err != nil {
			store.Close()
			return err
		}
		referenceOK, err := verifyReferenceResult(first, fixture)
		if err != nil {
			store.Close()
			return err
		}
		order := make([]int64, len(first.Hits))
		for i := range first.Hits {
			order[i] = first.Hits[i].DocID
		}
		queryResults = append(queryResults, demoQueryResult{
			QueryID: fixture.QueryID, Status: first.Status, Code: first.Code, Hits: len(first.Hits),
			RankOrder: order, ReferenceOK: referenceOK, CacheKey: first.Diagnostics.CacheKey,
			CacheHit: replayHit, CacheReplayEqual: bytesEqual(firstPayload, replayPayload),
			DiagnosticDigest: first.Diagnostics.DiagnosticDigest,
		})
		_ = firstHit
	}
	noChangeReport, err := engine.Build(context.Background(), cfg, corpus, store, false)
	if err != nil {
		store.Close()
		return err
	}
	noChange := toDemoBuild(noChangeReport)
	if err := store.Close(); err != nil {
		return err
	}

	changedCfg := cfg
	changedCfg.Index.SkipStep = 5
	changedPath := filepath.Join(outDir, "config-skip5.yaml")
	changedYAML, _ := yaml.Marshal(changedCfg)
	if err := os.WriteFile(changedPath, changedYAML, 0o644); err != nil {
		return err
	}
	changedCfg, _, err = config.Load(changedPath)
	if err != nil {
		return err
	}
	incStore, err := checkpoint.Open(filepath.Join(stateDir, "incremental.db"), filepath.Join(stateDir, "incremental.db.lock"))
	if err != nil {
		return err
	}
	initialReport, err := engine.Build(context.Background(), cfg, corpus, incStore, true)
	if err != nil {
		incStore.Close()
		return err
	}
	incRuntime, err := engine.LoadRuntime(context.Background(), incStore)
	if err != nil {
		incStore.Close()
		return err
	}
	incQuery := engine.NewQueryEngine(incRuntime)
	originalQuery, _, _, err := incQuery.Execute(context.Background(), engine.QueryRequest{QueryID: "cache-original", Terms: []string{"Go", "语言"}, Mode: "AND"})
	if err != nil {
		incStore.Close()
		return err
	}
	incrementalReport, err := engine.Build(context.Background(), changedCfg, corpus, incStore, false)
	if err != nil {
		incStore.Close()
		return err
	}
	incrementalRuntime, err := engine.LoadRuntime(context.Background(), incStore)
	if err != nil {
		incStore.Close()
		return err
	}
	changedQueryEngine := engine.NewQueryEngine(incrementalRuntime)
	changedQuery, _, _, err := changedQueryEngine.Execute(context.Background(), engine.QueryRequest{QueryID: "cache-original", Terms: []string{"Go", "语言"}, Mode: "AND"})
	if err != nil {
		incStore.Close()
		return err
	}
	incStore.Close()
	freshChangedStore, err := checkpoint.Open(filepath.Join(stateDir, "fresh-changed.db"), filepath.Join(stateDir, "fresh-changed.db.lock"))
	if err != nil {
		return err
	}
	freshChangedReport, err := engine.Build(context.Background(), changedCfg, corpus, freshChangedStore, true)
	if err != nil {
		freshChangedStore.Close()
		return err
	}
	freshChangedStore.Close()
	initialBuild := toDemoBuild(initialReport)
	incrementalBuild := toDemoBuild(incrementalReport)
	freshChangedBuild := toDemoBuild(freshChangedReport)
	earliest := "none"
	reusedStages := make([]string, 0)
	rebuiltStages := make([]string, 0)
	for _, stage := range incrementalReport.Stages {
		if stage.Reused {
			reusedStages = append(reusedStages, stage.Stage)
		} else {
			rebuiltStages = append(rebuiltStages, stage.Stage)
			if earliest == "none" {
				earliest = stage.Stage
			}
		}
	}
	incremental := demoIncrementalResult{
		EarliestAffectedStage: earliest,
		Reused:                reusedStages, Rebuilt: rebuiltStages,
		Incremental: incrementalBuild, Fresh: freshChangedBuild,
		FreshArtifactsMatch: reportArtifactsEqual(incrementalReport, freshChangedReport),
		FreshIndexMatch:     incrementalBuild.IndexVersion == freshChangedBuild.IndexVersion,
		OriginalCacheKey:    originalQuery.Diagnostics.CacheKey,
		ChangedCacheKey:     changedQuery.Diagnostics.CacheKey,
		CacheKeyChanged:     originalQuery.Diagnostics.CacheKey != changedQuery.Diagnostics.CacheKey,
	}
	_ = initialBuild

	concurrency, err := runConcurrencyDemo(stateDir)
	if err != nil {
		return err
	}
	referenceDigest := digestBytes(referencePayload)
	summaryPayload, _ := os.ReadFile(filepath.Join(outDir, "run-summary.json"))
	verification := demoVerificationSummary{
		FormatVersion:           "searchlab-verification/v1",
		GeneratorSeed:           seed,
		GeneratorSummary:        digestBytes(summaryPayload),
		ConfigDigest:            cfg.Digest(),
		ShardDigest:             corpus.ShardDigest,
		ReferenceDigest:         referenceDigest,
		FreshBuild:              freshBuild,
		ReferenceArtifactsMatch: referenceArtifactsMatch,
		IncrementalNoChange:     noChange,
		Queries:                 queryResults,
		Incremental:             incremental,
		Concurrency:             concurrency,
		ErrorCodes: map[string]string{
			"numeric":          apperr.CodeNumeric,
			"decode_mismatch":  apperr.CodeDecodeMismatch,
			"stale_checkpoint": apperr.CodeStaleCheckpoint,
			"prereq_missing":   apperr.CodePrereqMissing,
			"latency_budget":   apperr.CodeLatencyBudget,
		},
	}
	payload, _ := json.MarshalIndent(verification, "", "  ")
	path := filepath.Join(outDir, "verification-summary.json")
	if err := os.WriteFile(path, append(payload, '\n'), 0o644); err != nil {
		return err
	}
	_ = summary
	if !referenceArtifactsMatch {
		return apperr.E(apperr.CodeInput, "built artifacts differ from generator reference", nil)
	}
	for _, result := range queryResults {
		if !result.ReferenceOK || !result.CacheReplayEqual {
			return apperr.E(apperr.CodeInput, "query reference or cache replay mismatch", nil)
		}
	}
	if !incremental.FreshArtifactsMatch || !incremental.FreshIndexMatch || !incremental.CacheKeyChanged {
		return apperr.E(apperr.CodeInput, "incremental rebuild does not match fresh build", nil)
	}
	fmt.Println(string(payload))
	return nil
}

func runConcurrencyDemo(stateDir string) (demoConcurrencyResult, error) {
	result := demoConcurrencyResult{Processes: 5}
	payload := apperr.E(apperr.CodeNumeric, "numeric demo", nil).JSONBytes()
	prereqPath := filepath.Join(stateDir, "prereq.db")
	prereqLock := prereqPath + ".lock"
	prereqStore, err := checkpoint.Open(prereqPath, prereqLock)
	if err != nil {
		return result, err
	}
	_, prereqErr := prereqStore.Publish(context.Background(), checkpoint.PublishRequest{Seq: 1, Stage: engine.StageStats, ShardDigest: "prereq", InputDigest: "stats", ConfigVersion: "v", PipelineID: "p", Payload: payload})
	prereqStore.Close()
	if prereqErr == nil || !apperr.IsCode(prereqErr, apperr.CodePrereqMissing) {
		return result, apperr.E(apperr.CodeInput, "missing-prerequisite checkpoint was not rejected", nil)
	}
	result.PrereqMissingCode = apperr.CodePrereqMissing
	_ = resetFile(prereqPath)
	_ = os.Remove(prereqLock)

	dbPath := filepath.Join(stateDir, "concurrency.db")
	lockPath := dbPath + ".lock"
	_ = resetFile(dbPath)
	_ = os.Remove(lockPath)
	artifactPath := filepath.Join(stateDir, "concurrency-artifact.json")
	if err := os.WriteFile(artifactPath, []byte(`{"stage":"tokenize","concurrent":true}`), 0o644); err != nil {
		return result, err
	}
	executable, err := os.Executable()
	if err != nil {
		return result, err
	}
	type processResult struct {
		index  int
		stdout string
		err    error
	}
	var wg sync.WaitGroup
	results := make(chan processResult, result.Processes)
	for i := 0; i < result.Processes; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			cmd := exec.Command(executable, "checkpoint-publish",
				"--db", dbPath, "--lock", lockPath, "--stage", "tokenize",
				"--seq", "1", "--pipeline", "concurrent-pipeline",
				"--input-digest", "concurrency", "--config-version", "v",
				"--payload", artifactPath)
			out, err := cmd.CombinedOutput()
			results <- processResult{index: index, stdout: string(out), err: err}
		}(i)
	}
	go func() { wg.Wait(); close(results) }()
	for proc := range results {
		if proc.err == nil {
			result.Succeeded++
			result.Codes = append(result.Codes, "OK")
		} else {
			result.Stale++
			code := apperr.CodeStaleCheckpoint
			var parsed apperr.Error
			if json.Unmarshal([]byte(proc.stdout), &parsed) == nil && parsed.Code != "" {
				code = parsed.Code
			}
			result.Codes = append(result.Codes, code)
		}
	}
	sort.Strings(result.Codes)
	if result.Succeeded != 1 || result.Stale != result.Processes-1 {
		return result, apperr.E(apperr.CodeInput, "concurrent checkpoint publication invariant failed", nil)
	}
	return result, nil
}

func resetFile(path string) error {
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(path + suffix)
	}
	return nil
}

func verifyReferenceResult(actual *engine.QueryResult, expected generator.ReferenceQuery) (bool, error) {
	if len(actual.Hits) != len(expected.Hits) {
		return false, nil
	}
	for i := range expected.Hits {
		if actual.Hits[i].DocID != expected.Hits[i].DocID {
			return false, nil
		}
		if math.Abs(actual.Hits[i].Score-expected.Hits[i].DecodedScore) > 1e-12*math.Max(1, math.Abs(expected.Hits[i].DecodedScore)) {
			return false, nil
		}
		ref, ok := new(big.Float).SetPrec(hp.Precision).SetString(expected.Hits[i].ReferenceScore)
		if !ok {
			return false, fmt.Errorf("invalid reference score")
		}
		refFloat, _ := ref.Float64()
		if math.Abs(actual.Hits[i].Score-refFloat) > actual.Hits[i].ErrorBound {
			return false, nil
		}
		if len(actual.Hits[i].Components) != len(expected.Hits[i].Components) {
			return false, nil
		}
		for j := range expected.Hits[i].Components {
			if actual.Hits[i].Components[j].Term != expected.Hits[i].Components[j].Term || actual.Hits[i].Components[j].Quantized != expected.Hits[i].Components[j].Quantized {
				return false, nil
			}
		}
	}
	return true, nil
}

func toDemoBuild(report *engine.BuildReport) demoBuildResult {
	out := demoBuildResult{PipelineID: report.PipelineID, ConfigDigest: report.ConfigDigest, IndexVersion: report.IndexVersion}
	for _, stage := range report.Stages {
		out.Stages = append(out.Stages, demoStageResult{Stage: stage.Stage, InputDigest: stage.InputDigest, ArtifactDigest: stage.ArtifactDigest, Reused: stage.Reused})
	}
	return out
}

func reportArtifactsMatch(report *engine.BuildReport, expected generator.ArtifactDigests) bool {
	for _, stage := range report.Stages {
		switch stage.Stage {
		case engine.StageTokenize:
			if stage.ArtifactDigest != expected.Tokenize {
				return false
			}
		case engine.StageStats:
			if stage.ArtifactDigest != expected.Stats {
				return false
			}
		case engine.StagePostings:
			if stage.ArtifactDigest != expected.Postings {
				return false
			}
		case engine.StageDiagnostics:
			if stage.ArtifactDigest != expected.Diagnostics {
				return false
			}
		}
	}
	return report.IndexVersion == expected.Index
}

func reportArtifactsEqual(a, b *engine.BuildReport) bool {
	if len(a.Stages) != len(b.Stages) || a.IndexVersion != b.IndexVersion {
		return false
	}
	for i := range a.Stages {
		if a.Stages[i].Stage != b.Stages[i].Stage || a.Stages[i].ArtifactDigest != b.Stages[i].ArtifactDigest || a.Stages[i].InputDigest != b.Stages[i].InputDigest {
			return false
		}
	}
	return true
}

func bytesEqual(a, b []byte) bool { return string(a) == string(b) }

func digestBytes(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
