package generator

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"searchlab/internal/apperr"
	"searchlab/internal/compress"
	"searchlab/internal/config"
	"searchlab/internal/engine"
	"searchlab/internal/hp"
	"searchlab/internal/model"
	"searchlab/internal/stage"
	"searchlab/internal/tokenize"
)

type ArtifactDigests struct {
	Tokenize    string `json:"tokenize"`
	Stats       string `json:"stats"`
	Postings    string `json:"postings"`
	Diagnostics string `json:"diagnostics"`
	Index       string `json:"index_version"`
}

type ReferenceTerm struct {
	Term            string  `json:"term"`
	TermID          int32   `json:"term_id"`
	DF              int     `json:"df"`
	IDF             string  `json:"idf"`
	MaxWeight       string  `json:"max_weight"`
	Scale           float64 `json:"scale"`
	QuantStep       float64 `json:"quant_step"`
	QuantErrorBound float64 `json:"quant_error_bound"`
}

type ReferenceComponent struct {
	Term        string  `json:"term"`
	DocID       int64   `json:"doc_id"`
	Weight      string  `json:"weight"`
	Scale       float64 `json:"scale"`
	Quantized   int64   `json:"quantized"`
	Dequantized float64 `json:"dequantized"`
	ErrorBound  float64 `json:"error_bound"`
}

type ReferenceHit struct {
	DocID          int64                `json:"doc_id"`
	Rank           int                  `json:"rank"`
	ReferenceScore string               `json:"reference_score"`
	DecodedScore   float64              `json:"decoded_score"`
	ErrorBound     float64              `json:"error_bound"`
	Components     []ReferenceComponent `json:"components"`
}

type ReferenceQuery struct {
	QueryID  string         `json:"query_id"`
	RawTerms []string       `json:"raw_terms"`
	Terms    []string       `json:"terms"`
	Mode     string         `json:"mode"`
	Hits     []ReferenceHit `json:"hits"`
}

type Reference struct {
	GeneratorSeed int64            `json:"generator_seed"`
	ConfigVersion string           `json:"config_version"`
	ConfigDigest  string           `json:"config_digest"`
	ShardDigest   string           `json:"shard_digest"`
	DocCount      int              `json:"doc_count"`
	TermCount     int              `json:"term_count"`
	TotalLength   int              `json:"total_length"`
	AverageLength string           `json:"average_length"`
	ReferenceImpl string           `json:"reference_implementation"`
	Artifacts     ArtifactDigests  `json:"artifacts"`
	Terms         []ReferenceTerm  `json:"terms"`
	Queries       []ReferenceQuery `json:"queries"`
}

type Summary struct {
	FormatVersion string          `json:"format_version"`
	GeneratorSeed int64           `json:"generator_seed"`
	DocCount      int             `json:"doc_count"`
	ShardFiles    []string        `json:"shard_files"`
	ShardDigest   string          `json:"shard_digest"`
	ConfigVersion string          `json:"config_version"`
	ConfigDigest  string          `json:"config_digest"`
	TermCount     int             `json:"term_count"`
	Artifacts     ArtifactDigests `json:"artifacts"`
	Queries       []QuerySummary  `json:"queries"`
	ReferenceSHA  string          `json:"reference_sha256"`
	Files         []FileDigest    `json:"files"`
}

type QuerySummary struct {
	QueryID string `json:"query_id"`
	Status  string `json:"status"`
	Hits    int    `json:"hits"`
}

type FileDigest struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

func Generate(outDir string, seed int64) (*Summary, error) {
	if seed < 0 {
		return nil, apperr.E(apperr.CodeInput, "seed must be non-negative", nil)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}
	shardDir := filepath.Join(outDir, "shards")
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		return nil, fmt.Errorf("create shard directory: %w", err)
	}
	entries, _ := os.ReadDir(shardDir)
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".jsonl") {
			if err := os.Remove(filepath.Join(shardDir, entry.Name())); err != nil {
				return nil, fmt.Errorf("remove old shard: %w", err)
			}
		}
	}
	docs := generateDocuments(seed)
	half := (len(docs) + 1) / 2
	if err := writeShard(filepath.Join(shardDir, "part-000.jsonl"), docs[:half]); err != nil {
		return nil, err
	}
	if err := writeShard(filepath.Join(shardDir, "part-001.jsonl"), docs[half:]); err != nil {
		return nil, err
	}
	cfg := config.MustDefault()
	cfg.Seed = seed
	cfgRaw, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	configPath := filepath.Join(outDir, "config.yaml")
	if err := os.WriteFile(configPath, cfgRaw, 0o644); err != nil {
		return nil, fmt.Errorf("write config: %w", err)
	}
	corpus, err := model.LoadShards(shardDir)
	if err != nil {
		return nil, err
	}
	reference, referencePayload, err := buildReference(seed, cfg, corpus)
	if err != nil {
		return nil, err
	}
	referencePath := filepath.Join(outDir, "reference.json")
	if err := os.WriteFile(referencePath, referencePayload, 0o644); err != nil {
		return nil, fmt.Errorf("write reference: %w", err)
	}
	summary := &Summary{
		FormatVersion: "searchlab-demo/v1",
		GeneratorSeed: seed,
		DocCount:      len(corpus.Docs),
		ShardFiles:    corpus.ShardFiles,
		ShardDigest:   corpus.ShardDigest,
		ConfigVersion: cfg.Version,
		ConfigDigest:  cfg.Digest(),
		TermCount:     reference.TermCount,
		Artifacts:     reference.Artifacts,
		ReferenceSHA:  model.DigestBytes(referencePayload),
	}
	for _, query := range reference.Queries {
		status := "ok"
		if len(query.Hits) == 0 {
			status = "diagnostic"
		}
		summary.Queries = append(summary.Queries, QuerySummary{QueryID: query.QueryID, Status: status, Hits: len(query.Hits)})
	}
	for _, path := range []string{"config.yaml", "reference.json"} {
		raw, err := os.ReadFile(filepath.Join(outDir, path))
		if err != nil {
			return nil, err
		}
		summary.Files = append(summary.Files, FileDigest{Path: path, SHA256: model.DigestBytes(raw)})
	}
	for _, shard := range corpus.ShardFiles {
		raw, err := os.ReadFile(filepath.Join(shardDir, shard))
		if err != nil {
			return nil, err
		}
		summary.Files = append(summary.Files, FileDigest{Path: "shards/" + shard, SHA256: model.DigestBytes(raw)})
	}
	summaryPayload, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(outDir, "run-summary.json"), append(summaryPayload, '\n'), 0o644); err != nil {
		return nil, fmt.Errorf("write run summary: %w", err)
	}
	return summary, nil
}

func writeShard(path string, docs []model.Document) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create shard: %w", err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	for _, doc := range docs {
		if err := encoder.Encode(doc); err != nil {
			return fmt.Errorf("encode shard: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return file.Sync()
}

func generateDocuments(seed int64) []model.Document {
	random := rand.New(rand.NewSource(seed))
	topics := []string{"本地搜索", "倒排索引", "压缩查询", "误差传播", "BM25", "SQLite", "Go 并发", "缓存一致性"}
	pick := func() string { return topics[random.Intn(len(topics))] }
	docs := []model.Document{
		{DocID: 1001, Title: "Go 语言与本地搜索", Body: "Ｇｏ 语言实现本地搜索 inverted index，支持中文和 English 混排查询。误差沿倒排链路传播。", Lang: "zh-en", Source: "local://demo"},
		{DocID: 1002, Title: "SQLite 事务索引实验", Body: "SQLite WAL 事务保存 checkpoint。The search index stores compressed postings and verifies every decoded block.", Lang: "zh-en", Source: "local://demo"},
		{DocID: 1003, Title: "稀疏倒排与跳表", Body: "倒排列表按 doc_id 升序，差值编码后使用 base128 变长字节写入分块，跳表按固定步长跳跃求交。", Lang: "zh", Source: "local://demo"},
		{DocID: 1004, Title: "Go 语言与本地搜索", Body: "Ｇｏ 语言实现本地搜索 inverted index，支持中文和 English 混排查询。误差沿倒排链路传播。", Lang: "zh-en", Source: "local://demo/duplicate"},
		{DocID: 1005, Title: "空正文占位文档", Body: "", Lang: "zh", Source: "local://demo"},
		{DocID: 1006, Title: "Quantization error bounds", Body: "BM25 weights live in the natural-log domain. Fixed-point storage records a scale and half-step error bound for every term.", Lang: "en", Source: "local://demo"},
		{DocID: 1007, Title: "查询延迟与缓存指纹", Body: "Cache keys contain the query term set, semantics, config version and index fingerprint. Stale cache entries are never reused.", Lang: "zh", Source: "local://demo"},
		{DocID: 1008, Title: "并发发布保护", Body: "SQLite conditional updates plus an operating-system file lock reject stale checkpoints and require consecutive sequence numbers.", Lang: "zh", Source: "local://demo"},
		{DocID: 1009, Title: "增量失效传播", Body: "Stage content digests identify the earliest affected stage. Upstream token and statistics products remain reusable.", Lang: "zh", Source: "local://demo"},
		{DocID: 1010, Title: "搜索 search 混排样例", Body: "搜索 search 混排 go Go 语言 language 查询 query。", Lang: "zh-en", Source: "local://demo"},
	}
	longParagraph := "Go 本地搜索 search compression 误差传播 quantization SQLite 中文 索引 查询。"
	docs = append(docs, model.Document{DocID: 1011, Title: "超长文档：" + pick(), Body: strings.Repeat(longParagraph, 96) + " tail-token", Lang: "zh-en", Source: "local://demo"})
	for i := 0; i < 4; i++ {
		topicA, topicB := pick(), pick()
		docs = append(docs, model.Document{
			DocID:  int64(1012 + i),
			Title:  fmt.Sprintf("%s 与 %s", topicA, topicB),
			Body:   fmt.Sprintf("seed=%d topic=%s local search 文档。BM25 参数与 quant_bits 决定误差上界；分词包含相邻二字组合。", seed, topicB),
			Lang:   "zh-en",
			Source: "local://demo",
		})
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].DocID < docs[j].DocID })
	return docs
}

func buildReference(seed int64, cfg config.Config, corpus model.Corpus) (*Reference, []byte, error) {
	tokenBuilder := tokenize.New(cfg)
	tokensPayload, tokensArtifact, err := tokenBuilder.Build(corpus)
	if err != nil {
		return nil, nil, err
	}
	statsPayload, statsArtifact, err := engine.BuildStats(cfg, *tokensArtifact)
	if err != nil {
		return nil, nil, err
	}
	postingsPayload, postingsArtifact, err := compress.Build(cfg, *statsArtifact)
	if err != nil {
		return nil, nil, err
	}
	diagnosticsPayload, _, err := engine.BuildDiagnostics(cfg, *statsArtifact, *postingsArtifact)
	if err != nil {
		return nil, nil, err
	}
	parts := []string{
		stage.Tokenize + ":" + model.DigestBytes(tokensPayload),
		stage.Stats + ":" + model.DigestBytes(statsPayload),
		stage.Postings + ":" + model.DigestBytes(postingsPayload),
		stage.Diagnostics + ":" + model.DigestBytes(diagnosticsPayload),
	}
	sort.Strings(parts)
	indexPayload, _ := json.Marshal(parts)
	reference := &Reference{
		GeneratorSeed: seed,
		ConfigVersion: cfg.Version,
		ConfigDigest:  cfg.Digest(),
		ShardDigest:   corpus.ShardDigest,
		DocCount:      len(corpus.Docs),
		TermCount:     len(statsArtifact.Terms),
		TotalLength:   statsArtifact.TotalLength,
		AverageLength: statsArtifact.AverageLength,
		ReferenceImpl: fmt.Sprintf("math/big.Float precision=%d; natural-log atanh series; BM25 natural-log domain", hp.Precision),
		Artifacts: ArtifactDigests{
			Tokenize:    model.DigestBytes(tokensPayload),
			Stats:       model.DigestBytes(statsPayload),
			Postings:    model.DigestBytes(postingsPayload),
			Diagnostics: model.DigestBytes(diagnosticsPayload),
			Index:       model.DigestBytes(indexPayload),
		},
	}
	for _, term := range statsArtifact.Terms {
		reference.Terms = append(reference.Terms, ReferenceTerm{
			Term:            term.Term,
			TermID:          term.TermID,
			DF:              term.DF,
			IDF:             term.IDF,
			MaxWeight:       term.MaxWeight,
			Scale:           term.Scale,
			QuantStep:       term.QuantStep,
			QuantErrorBound: term.QuantErrorBound,
		})
	}
	queries := []struct {
		id    string
		raw   []string
		mode  string
		empty bool
	}{
		{id: "demo-and", raw: []string{"Go", "语言"}, mode: "AND"},
		{id: "demo-or", raw: []string{"search", "SQLite", "空正文"}, mode: "OR"},
		{id: "demo-single", raw: []string{"压缩查询"}, mode: "OR"},
		{id: "demo-empty", raw: []string{}, mode: "OR", empty: true},
		{id: "demo-missing", raw: []string{"zzzz-not-present"}, mode: "AND"},
	}
	for _, fixture := range queries {
		query := ReferenceQuery{QueryID: fixture.id, RawTerms: append([]string(nil), fixture.raw...), Mode: fixture.mode}
		if !fixture.empty {
			query.Terms = tokenBuilder.QueryTerms(fixture.raw)
			hits, err := referenceQuery(cfg, *statsArtifact, query.Terms, query.Mode)
			if err != nil {
				return nil, nil, err
			}
			query.Hits = hits
		}
		reference.Queries = append(reference.Queries, query)
	}
	payload, err := json.MarshalIndent(reference, "", "  ")
	if err != nil {
		return nil, nil, fmt.Errorf("marshal reference: %w", err)
	}
	return reference, append(payload, '\n'), nil
}

func referenceQuery(cfg config.Config, stats engine.StatsArtifact, terms []string, mode string) ([]ReferenceHit, error) {
	termByString := make(map[string]*engine.TermStat, len(stats.Terms))
	for i := range stats.Terms {
		termByString[stats.Terms[i].Term] = &stats.Terms[i]
	}
	docs := make(map[int64][]ReferenceComponent)
	if mode == "AND" {
		for _, term := range terms {
			stat, ok := termByString[term]
			if !ok {
				return nil, nil
			}
			if len(docs) == 0 {
				for _, posting := range stat.Postings {
					docs[posting.DocID] = append(docs[posting.DocID], componentFor(stat, posting))
				}
			} else {
				for docID, components := range docs {
					found := false
					for _, posting := range stat.Postings {
						if posting.DocID == docID {
							docs[docID] = append(components, componentFor(stat, posting))
							found = true
							break
						}
					}
					if !found {
						delete(docs, docID)
					}
				}
			}
		}
	} else {
		for _, term := range terms {
			stat, ok := termByString[term]
			if !ok {
				continue
			}
			for _, posting := range stat.Postings {
				docs[posting.DocID] = append(docs[posting.DocID], componentFor(stat, posting))
			}
		}
	}
	hits := make([]ReferenceHit, 0, len(docs))
	for docID, components := range docs {
		sort.Slice(components, func(i, j int) bool { return components[i].Term < components[j].Term })
		referenceScore := new(big.Float).SetPrec(hp.Precision)
		decoded := 0.0
		bound := 0.0
		for _, component := range components {
			parsed, ok := new(big.Float).SetPrec(hp.Precision).SetString(component.Weight)
			if !ok {
				return nil, fmt.Errorf("invalid reference component")
			}
			referenceScore.Add(referenceScore, parsed)
			decoded += component.Dequantized
			bound += component.ErrorBound
		}
		hits = append(hits, ReferenceHit{
			DocID:          docID,
			ReferenceScore: hp.Decimal(referenceScore, 20),
			DecodedScore:   decoded,
			ErrorBound:     math.Nextafter(bound, math.Inf(1)),
			Components:     components,
		})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].DecodedScore != hits[j].DecodedScore {
			return hits[i].DecodedScore > hits[j].DecodedScore
		}
		return hits[i].DocID < hits[j].DocID
	})
	for i := range hits {
		hits[i].Rank = i + 1
	}
	return hits, nil
}

func componentFor(term *engine.TermStat, posting engine.QuantPosting) ReferenceComponent {
	return ReferenceComponent{
		Term:        term.Term,
		DocID:       posting.DocID,
		Weight:      posting.Reference,
		Scale:       term.Scale,
		Quantized:   posting.Quantized,
		Dequantized: posting.Dequantized,
		ErrorBound:  posting.ErrorBound,
	}
}
