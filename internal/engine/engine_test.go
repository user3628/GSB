package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"searchlab/internal/apperr"
	"searchlab/internal/checkpoint"
	"searchlab/internal/compress"
	"searchlab/internal/config"
	"searchlab/internal/model"
	"searchlab/internal/stage"
	"searchlab/internal/tokenize"
)

func testConfig() config.Config {
	cfg := config.MustDefault()
	cfg.Index.BlockSize = 3
	cfg.Index.SkipStep = 2
	cfg.Query.CacheCapacity = 8
	cfg.Query.LatencyBudgetMS = 500
	return cfg
}

func testCorpus() model.Corpus {
	docs := make([]model.Document, 0, 12)
	for i := 0; i < 12; i++ {
		docID := int64(10 + i*10)
		title := fmt.Sprintf("go search 文档 %d", i)
		body := "go search sqlite 本地索引 中文 语言 查询。"
		switch {
		case i == 1:
			title = "go search 文档 0"
			body = "go search sqlite 本地索引 中文 语言 查询。"
		case i == 2:
			body = ""
		case i == 11:
			body = strings.Repeat("go 语言 search 索引 compression。", 24) + " lasttoken"
		}
		docs = append(docs, model.Document{DocID: docID, Title: title, Body: body, Lang: "zh-en", Source: "test"})
	}
	payload, _ := json.Marshal(docs)
	return model.Corpus{Docs: docs, ShardDigest: model.DigestBytes(payload)}
}

func buildAndLoad(t *testing.T, cfg config.Config, corpus model.Corpus) (*checkpoint.Store, *Runtime, *BuildReport) {
	t.Helper()
	dir := t.TempDir()
	store, err := checkpoint.Open(filepath.Join(dir, "state.db"), filepath.Join(dir, "state.lock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	report, err := Build(context.Background(), cfg, corpus, store, true)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	runtime, err := LoadRuntime(context.Background(), store)
	if err != nil {
		t.Fatalf("load runtime: %v", err)
	}
	return store, runtime, report
}

func TestQueryAndOrOrderingDiagnosticsAndCache(t *testing.T) {
	cfg := testConfig()
	corpus := testCorpus()
	_, runtime, _ := buildAndLoad(t, cfg, corpus)
	engine := NewQueryEngine(runtime)

	result, payload, cacheHit, err := engine.Execute(context.Background(), QueryRequest{QueryID: "and-go-语言", Terms: []string{"Go", "语言"}, Mode: "AND"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "ok" || len(result.Hits) == 0 {
		t.Fatalf("unexpected result %+v", result)
	}
	if cacheHit {
		t.Fatal("first query unexpectedly hit cache")
	}
	for i := 1; i < len(result.Hits); i++ {
		previous, current := result.Hits[i-1], result.Hits[i]
		if previous.Score < current.Score || (previous.Score == current.Score && previous.DocID > current.DocID) {
			t.Fatalf("hits are not ordered: %+v then %+v", previous, current)
		}
	}
	for _, hit := range result.Hits {
		if hit.ErrorBound <= 0 || hit.EffectiveDigits < 0 {
			t.Fatalf("missing error diagnostics: %+v", hit)
		}
		for _, component := range hit.Components {
			if component.CompressionErrorBound != 0 {
				t.Fatalf("lossless compression reported non-zero error: %+v", component)
			}
			if component.QuantizationError > component.QuantizationErrorBound {
				t.Fatalf("quantization error exceeds bound: %+v", component)
			}
			if !component.LogContributionDefined {
				t.Fatalf("log contribution is undefined: %+v", component)
			}
		}
	}
	cachedResult, cachedPayload, hit, err := engine.Execute(context.Background(), QueryRequest{QueryID: "and-go-语言", Terms: []string{"Go", "语言"}, Mode: "AND"})
	if err != nil {
		t.Fatal(err)
	}
	if !hit {
		t.Fatal("second query did not hit cache")
	}
	if !bytes.Equal(payload, cachedPayload) {
		t.Fatal("cache hit payload differs byte-for-byte")
	}
	if cachedResult.Diagnostics.DiagnosticDigest != result.Diagnostics.DiagnosticDigest {
		t.Fatal("diagnostic digest changed on cache replay")
	}
	if result.Diagnostics.BlocksRead == 0 || result.Diagnostics.PostingsDecoded == 0 {
		t.Fatalf("query counters are empty: %+v", result.Diagnostics)
	}

	orResult, _, _, err := engine.Execute(context.Background(), QueryRequest{QueryID: "or-search-sqlite", Terms: []string{"search", "sqlite"}, Mode: "OR"})
	if err != nil {
		t.Fatal(err)
	}
	if orResult.Status != "ok" || len(orResult.Hits) < len(result.Hits) {
		t.Fatalf("OR did not union results: %+v", orResult)
	}
}

func TestSkipPointerIsUsedForLateIntersection(t *testing.T) {
	cfg := testConfig()
	cfg.Index.BlockSize = 2
	cfg.Index.SkipStep = 2
	_, runtime, _ := buildAndLoad(t, cfg, testCorpus())
	engine := NewQueryEngine(runtime)
	result, _, _, err := engine.Execute(context.Background(), QueryRequest{QueryID: "skip", Terms: []string{"go", "lasttoken"}, Mode: "AND"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 {
		t.Fatalf("late-token query hits = %d, want 1", len(result.Hits))
	}
	if result.Diagnostics.SkipJumps == 0 {
		t.Fatalf("expected skip pointer jumps, diagnostics=%+v", result.Diagnostics)
	}
}

func TestEmptyAndMissingQueriesReturnDiagnostics(t *testing.T) {
	cfg := testConfig()
	_, runtime, _ := buildAndLoad(t, cfg, testCorpus())
	engine := NewQueryEngine(runtime)
	empty, _, _, err := engine.Execute(context.Background(), QueryRequest{QueryID: "empty", Terms: []string{"the"}, Mode: "OR"})
	if err != nil {
		t.Fatal(err)
	}
	if empty.Status != "diagnostic" || empty.Code != apperr.CodeEmptyQuery || empty.Message == "" {
		t.Fatalf("empty query result = %+v", empty)
	}
	missing, _, _, err := engine.Execute(context.Background(), QueryRequest{QueryID: "missing", Terms: []string{"zzzz"}, Mode: "AND"})
	if err != nil {
		t.Fatal(err)
	}
	if missing.Status != "diagnostic" || missing.Code != apperr.CodeNoMatch || len(missing.Hits) != 0 {
		t.Fatalf("missing query result = %+v", missing)
	}
}

func TestLatencyBudgetBusinessError(t *testing.T) {
	cfg := testConfig()
	cfg.Query.LatencyBudgetMS = 1
	_, runtime, _ := buildAndLoad(t, cfg, testCorpus())
	engine := NewQueryEngine(runtime)
	req := QueryRequest{QueryID: "latency", Terms: []string{"go"}, Mode: "OR"}
	_, _, _, err := engine.Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(100, 0)
	calls := 0
	engine.now = func() time.Time {
		calls++
		return base.Add(time.Duration(calls) * 2 * time.Millisecond)
	}
	_, _, _, err = engine.Execute(context.Background(), req)
	if !apperr.IsCode(err, apperr.CodeLatencyBudget) {
		t.Fatalf("latency error = %v, want E_LATENCY_BUDGET", err)
	}
	business := apperr.As(err)
	if business.QueryID != "latency" || business.ElapsedMS <= 0 {
		t.Fatalf("latency error lacks query id/elapsed_ms: %+v", business)
	}
}

func TestAllEmptyCorpusReturnsNumericError(t *testing.T) {
	cfg := testConfig()
	corpus := model.Corpus{ShardDigest: "empty", Docs: []model.Document{{DocID: 1, Title: "", Body: "", Lang: "zh", Source: "test"}}}
	tokensPayload, tokens, err := tokenize.New(cfg).Build(corpus)
	if err != nil {
		t.Fatal(err)
	}
	_ = tokensPayload
	if len(tokens.Docs[0].BodyTokens) != 0 || tokens.Docs[0].BodyTokens == nil {
		t.Fatal("empty body document was not retained")
	}
	_, _, err = BuildStats(cfg, *tokens)
	if !apperr.IsCode(err, apperr.CodeNumeric) {
		t.Fatalf("empty corpus stats error = %v, want E_NUMERIC", err)
	}
}

func TestNonFiniteBM25ReturnsNumericError(t *testing.T) {
	cfg := testConfig()
	cfg.Index.BM25.K1 = math.NaN()
	corpus := testCorpus()
	tokensPayload, tokens, err := tokenize.New(cfg).Build(corpus)
	if err != nil {
		t.Fatal(err)
	}
	_ = tokensPayload
	_, _, err = BuildStats(cfg, *tokens)
	if !apperr.IsCode(err, apperr.CodeNumeric) {
		t.Fatalf("non-finite BM25 error = %v, want E_NUMERIC", err)
	}
	business := apperr.As(err)
	if business.Term == "" || business.DocumentID == 0 {
		t.Fatalf("numeric error lacks stable term/document identity: %+v", business)
	}
}

func TestRuntimeDetectsDecodedWeightMismatchAtFirstBlock(t *testing.T) {
	cfg := testConfig()
	_, runtime, _ := buildAndLoad(t, cfg, testCorpus())
	// Copy the artifact deeply through JSON so the runtime's active artifact is not mutated by the test.
	payload, _ := json.Marshal(runtime.Postings)
	var copyArtifact = runtime.Postings
	if err := json.Unmarshal(payload, &copyArtifact); err != nil {
		t.Fatal(err)
	}
	copyArtifact.Terms[0].Blocks = append([]compress.Block(nil), copyArtifact.Terms[0].Blocks...)
	copyArtifact.Terms[0].Blocks[0].Checksum = "corrupt"
	err := verifyRuntimeArtifacts(runtime.Stats, copyArtifact)
	if !apperr.IsCode(err, apperr.CodeDecodeMismatch) {
		t.Fatalf("decode mismatch error = %v, want E_DECODE_MISMATCH", err)
	}
	if apperr.As(err).FirstDiffBlock != 1 {
		t.Fatalf("first diff block = %d, want 1", apperr.As(err).FirstDiffBlock)
	}
}

func TestCacheKeyIncludesConfigurationAndIndexVersions(t *testing.T) {
	terms := []string{"go", "search"}
	mode := "AND"
	if queryCacheKey("q", terms, mode, "config-a", "index-a") == queryCacheKey("q", terms, mode, "config-b", "index-a") {
		t.Fatal("cache key does not change with configuration version")
	}
	if queryCacheKey("q", terms, mode, "config-a", "index-a") == queryCacheKey("q", terms, mode, "config-a", "index-b") {
		t.Fatal("cache key does not change with index fingerprint")
	}
	if queryCacheKey("q", []string{"go"}, mode, "config-a", "index-a") == queryCacheKey("q", []string{"go", "search"}, mode, "config-a", "index-a") {
		t.Fatal("cache key does not change with term set")
	}
}

func TestIncrementalInvalidationByConfigurationDimension(t *testing.T) {
	cases := []struct {
		name     string
		earliest string
		change   func(*config.Config, *model.Corpus)
	}{
		{name: "document", earliest: stage.Tokenize, change: func(c *config.Config, corpus *model.Corpus) {
			corpus.Docs = append([]model.Document(nil), corpus.Docs...)
			corpus.Docs[0].Body += " changed"
			payload, _ := json.Marshal(corpus.Docs)
			corpus.ShardDigest = model.DigestBytes(payload)
		}},
		{name: "stopwords", earliest: stage.Tokenize, change: func(c *config.Config, _ *model.Corpus) {
			c.Tokenizer.Stopwords = append(c.Tokenizer.Stopwords, "go")
		}},
		{name: "bm25", earliest: stage.Stats, change: func(c *config.Config, _ *model.Corpus) { c.Index.BM25.K1 += 0.1 }},
		{name: "quant_bits", earliest: stage.Stats, change: func(c *config.Config, _ *model.Corpus) { c.Index.QuantBits-- }},
		{name: "skip_step", earliest: stage.Postings, change: func(c *config.Config, _ *model.Corpus) { c.Index.SkipStep++ }},
		{name: "block_size", earliest: stage.Postings, change: func(c *config.Config, _ *model.Corpus) { c.Index.BlockSize++ }},
		{name: "varint_encoding", earliest: stage.Postings, change: func(c *config.Config, _ *model.Corpus) { c.Index.VarintEncoding = "base128" }},
		{name: "delta_encoding", earliest: stage.Postings, change: func(c *config.Config, _ *model.Corpus) { c.Index.DeltaEncoding = "docid_delta_first" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			corpus := testCorpus()
			dir := t.TempDir()
			store, err := checkpoint.Open(filepath.Join(dir, "state.db"), filepath.Join(dir, "state.lock"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if _, err := Build(context.Background(), cfg, corpus, store, true); err != nil {
				t.Fatal(err)
			}
			changedCfg := cfg
			changedCorpus := corpus
			tc.change(&changedCfg, &changedCorpus)
			if err := changedCfg.Validate(); err != nil {
				t.Fatalf("changed config: %v", err)
			}
			report, err := Build(context.Background(), changedCfg, changedCorpus, store, false)
			if err != nil {
				t.Fatal(err)
			}
			earliestSeen := ""
			for _, item := range report.Stages {
				if !item.Reused {
					if earliestSeen == "" {
						earliestSeen = item.Stage
					}
				} else if earliestSeen != "" {
					t.Fatalf("stage %s was reused after affected stage %s", item.Stage, earliestSeen)
				}
			}
			if earliestSeen != tc.earliest {
				t.Fatalf("earliest affected stage = %q, want %q (report=%+v)", earliestSeen, tc.earliest, report.Stages)
			}
		})
	}
}

func TestIncrementalRebuildEqualsFreshBuild(t *testing.T) {
	cfg := testConfig()
	corpus := testCorpus()
	dir := t.TempDir()
	incremental, err := checkpoint.Open(filepath.Join(dir, "incremental.db"), filepath.Join(dir, "incremental.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer incremental.Close()
	if _, err := Build(context.Background(), cfg, corpus, incremental, true); err != nil {
		t.Fatal(err)
	}
	changed := cfg
	changed.Index.BlockSize = 4
	changed.Index.SkipStep = 3
	incrementalReport, err := Build(context.Background(), changed, corpus, incremental, false)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := checkpoint.Open(filepath.Join(dir, "fresh.db"), filepath.Join(dir, "fresh.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	freshReport, err := Build(context.Background(), changed, corpus, fresh, true)
	if err != nil {
		t.Fatal(err)
	}
	if incrementalReport.IndexVersion != freshReport.IndexVersion {
		t.Fatalf("index versions differ: incremental=%s fresh=%s", incrementalReport.IndexVersion, freshReport.IndexVersion)
	}
	for i := range incrementalReport.Stages {
		if incrementalReport.Stages[i].ArtifactDigest != freshReport.Stages[i].ArtifactDigest {
			t.Fatalf("stage %s artifact digest differs", incrementalReport.Stages[i].Stage)
		}
	}
}

func TestQueryConfigChangeInvalidatesCacheKey(t *testing.T) {
	cfg := testConfig()
	corpus := testCorpus()
	_, runtime, _ := buildAndLoad(t, cfg, corpus)
	before := NewQueryEngine(runtime)
	resultA, _, _, err := before.Execute(context.Background(), QueryRequest{QueryID: "cache", Terms: []string{"go"}, Mode: "OR"})
	if err != nil {
		t.Fatal(err)
	}
	changed := cfg
	changed.Query.CacheCapacity = 1
	changed.Query.LatencyBudgetMS = 499
	dir := t.TempDir()
	store, err := checkpoint.Open(filepath.Join(dir, "changed.db"), filepath.Join(dir, "changed.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := Build(context.Background(), changed, corpus, store, true); err != nil {
		t.Fatal(err)
	}
	changedRuntime, err := LoadRuntime(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	after := NewQueryEngine(changedRuntime)
	resultB, _, cacheHit, err := after.Execute(context.Background(), QueryRequest{QueryID: "cache", Terms: []string{"go"}, Mode: "OR"})
	if err != nil {
		t.Fatal(err)
	}
	if cacheHit {
		t.Fatal("query cache was reused after configuration change")
	}
	if resultA.Diagnostics.CacheKey == resultB.Diagnostics.CacheKey {
		t.Fatal("cache key did not change after configuration change")
	}
	if math.IsNaN(resultB.Diagnostics.ErrorBound) {
		t.Fatal("query produced NaN error bound")
	}
}
