package sample

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"localsearch/multirecall/internal/model"
)

const (
	ItemsFile     = "items.jsonl"
	QueriesFile   = "queries.jsonl"
	JudgmentsFile = "qrels.jsonl"
	ConfigFile    = "config.json"
	ManifestFile  = "manifest.json"
)

var categories = []string{
	"coffee", "bakery", "hotpot", "sushi", "noodle", "dessert",
	"barbecue", "tea", "brunch", "seafood", "vegetarian", "streetfood",
}

var vocabulary = []string{
	"咖啡", "手冲", "拿铁", "烘焙", "面包", "火锅", "麻辣", "寿司", "面馆",
	"甜品", "烧烤", "茶饮", "早午餐", "海鲜", "素食", "小吃", "夜宵", "套餐",
	"coffee", "latte", "bakery", "bread", "hotpot", "spicy", "sushi", "noodle",
	"dessert", "barbecue", "tea", "brunch", "seafood", "vegetarian", "snack",
	"family", "cheap", "quiet", "fast", "local", "fresh", "nearby", "popular",
}

type GenerateOptions struct {
	OutDir     string
	Seed       int64
	ItemCount  int
	QueryCount int
	VectorDim  int
	Strategy   string
}

type Manifest struct {
	Seed              int64  `json:"seed"`
	ItemCount         int    `json:"item_count"`
	QueryCount        int    `json:"query_count"`
	JudgmentCount     int    `json:"judgment_count"`
	VectorDim         int    `json:"vector_dim"`
	ItemsDigest       string `json:"items_digest"`
	QueriesDigest     string `json:"queries_digest"`
	JudgmentsDigest   string `json:"judgments_digest"`
	ConfigFingerprint string `json:"config_fingerprint"`
}

type Dataset struct {
	Items           []model.Item
	Queries         []model.Query
	Judgments       []model.Judgment
	Config          model.Config
	ItemsDigest     string
	QueriesDigest   string
	JudgmentsDigest string
	DataFingerprint string
}

func DefaultConfig(seed int64, vectorDim int) model.Config {
	return model.Config{
		Version:         "fusion-v1",
		Seed:            seed,
		VectorDimension: vectorDim,
		Routes: model.RoutesConfig{
			Lexical:  model.RouteConfig{CandidateDepth: 24, EvaluationQuota: 24000},
			Category: model.RouteConfig{CandidateDepth: 24, EvaluationQuota: 24000},
			Vector:   model.RouteConfig{CandidateDepth: 24, EvaluationQuota: 24000},
		},
		Fusion: model.FusionConfig{
			Strategy:       "weighted",
			Weights:        model.RouteWeights{Lexical: 0.45, Category: 0.25, Vector: 0.30},
			RRFK:           60,
			TopK:           10,
			DedupeByItemID: true,
		},
		Budget: model.BudgetConfig{
			GlobalCandidateEvaluations: 60000,
			PerRoundTimeBudgetMS:       500,
			EstimatedCandidateMicros:   25,
		},
		Evaluation: model.EvaluationConfig{FusedTopK: 10},
		Execution: model.ExecutionConfig{
			MaxConcurrentQueries: 4,
			WorkerProcesses:      4,
		},
	}
}

func Generate(opts GenerateOptions) (Manifest, error) {
	if opts.OutDir == "" {
		return Manifest{}, model.NewError(model.CodeInvalidConfig, "output directory is required", "")
	}
	if opts.ItemCount < 1 || opts.QueryCount < 1 || opts.VectorDim < 1 {
		return Manifest{}, model.NewError(model.CodeInvalidConfig, "item count, query count and vector dimension must be positive", "")
	}
	if opts.QueryCount > 1000000 {
		return Manifest{}, model.NewError(model.CodeInvalidConfig, "query count exceeds sample generator limit", "")
	}
	rng := rand.New(rand.NewSource(opts.Seed))
	items := make([]model.Item, 0, opts.ItemCount)
	for i := 0; i < opts.ItemCount; i++ {
		category := categories[rng.Intn(len(categories))]
		titleTerms := make([]string, 0, 5)
		titleTerms = append(titleTerms, category)
		for j := 0; j < 4; j++ {
			titleTerms = append(titleTerms, vocabulary[rng.Intn(len(vocabulary))])
		}
		vector := make([]float64, opts.VectorDim)
		for j := range vector {
			vector[j] = float64(rng.Intn(2001)-1000) / 1000.0
		}
		items = append(items, model.Item{
			ItemID:   fmt.Sprintf("item-%05d", i+1),
			Category: category,
			Vector:   vector,
			Title:    strings.Join(titleTerms, " "),
		})
	}

	queries := make([]model.Query, 0, opts.QueryCount)
	judgments := make([]model.Judgment, 0, opts.QueryCount*4)
	for i := 0; i < opts.QueryCount; i++ {
		termCount := 2 + rng.Intn(3)
		queryTerms := make([]string, 0, termCount)
		for j := 0; j < termCount; j++ {
			queryTerms = append(queryTerms, vocabulary[rng.Intn(len(vocabulary))])
		}
		historyCount := 3 + rng.Intn(3)
		history := make([]string, 0, historyCount)
		for j := 0; j < historyCount; j++ {
			history = append(history, categories[rng.Intn(len(categories))])
		}
		locale := "zh-CN"
		if i%4 == 0 {
			locale = "en-US"
		}
		query := model.Query{
			QueryID:           fmt.Sprintf("q-%06d", i+1),
			Text:              strings.Join(queryTerms, " "),
			Locale:            locale,
			ClickedCategories: history,
		}
		queries = append(queries, query)

		type scoredItem struct {
			item  model.Item
			score int
		}
		labels := make([]scoredItem, 0, 8)
		historySet := make(map[string]struct{}, len(history))
		for _, h := range history {
			historySet[h] = struct{}{}
		}
		lowerTitle := strings.ToLower
		for _, item := range items {
			title := lowerTitle(item.Title)
			score := 0
			for _, term := range queryTerms {
				if strings.Contains(title, strings.ToLower(term)) {
					score += 2
				}
			}
			if _, ok := historySet[item.Category]; ok {
				score++
			}
			if score > 0 {
				labels = append(labels, scoredItem{item: item, score: score})
			}
		}
		sort.Slice(labels, func(a, b int) bool {
			if labels[a].score != labels[b].score {
				return labels[a].score > labels[b].score
			}
			return labels[a].item.ItemID < labels[b].item.ItemID
		})
		limit := 3 + rng.Intn(4)
		if limit > len(labels) {
			limit = len(labels)
		}
		for rank := 0; rank < limit; rank++ {
			judgments = append(judgments, model.Judgment{
				QueryID:       query.QueryID,
				ItemID:        labels[rank].item.ItemID,
				Relevance:     labels[rank].score,
				ReferenceRank: rank + 1,
			})
		}
	}

	items = model.NormalizeItems(items)
	queries = model.NormalizeQueries(queries)
	judgments = model.NormalizeJudgments(judgments)
	config := DefaultConfig(opts.Seed, opts.VectorDim)
	if opts.Strategy != "" {
		config.Fusion.Strategy = strings.ToLower(strings.TrimSpace(opts.Strategy))
	}
	if config.Fusion.Strategy == "reciprocal_rank" {
		config.Fusion.Strategy = "rrf"
	}
	config.Budget.GlobalCandidateEvaluations = max(config.Budget.GlobalCandidateEvaluations, opts.QueryCount*config.Routes.Lexical.CandidateDepth*3)
	for _, route := range model.RouteOrder {
		rc := config.Routes.Get(route)
		rc.EvaluationQuota = max(rc.EvaluationQuota, opts.QueryCount*rc.CandidateDepth)
		config.Routes.Set(route, rc)
	}
	if err := config.Validate(); err != nil {
		return Manifest{}, err
	}

	itemsDigest, err := model.ItemsDigest(items)
	if err != nil {
		return Manifest{}, err
	}
	queriesDigest, err := model.QueriesDigest(queries)
	if err != nil {
		return Manifest{}, err
	}
	judgmentsDigest, err := model.JudgmentsDigest(judgments)
	if err != nil {
		return Manifest{}, err
	}
	configFingerprint, err := model.ConfigContentFingerprint(config)
	if err != nil {
		return Manifest{}, err
	}
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return Manifest{}, err
	}
	if err := writeJSONLines(filepath.Join(opts.OutDir, ItemsFile), items); err != nil {
		return Manifest{}, err
	}
	if err := writeJSONLines(filepath.Join(opts.OutDir, QueriesFile), queries); err != nil {
		return Manifest{}, err
	}
	if err := writeJSONLines(filepath.Join(opts.OutDir, JudgmentsFile), judgments); err != nil {
		return Manifest{}, err
	}
	if err := writeJSONFile(filepath.Join(opts.OutDir, ConfigFile), config); err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{
		Seed:              opts.Seed,
		ItemCount:         len(items),
		QueryCount:        len(queries),
		JudgmentCount:     len(judgments),
		VectorDim:         opts.VectorDim,
		ItemsDigest:       itemsDigest,
		QueriesDigest:     queriesDigest,
		JudgmentsDigest:   judgmentsDigest,
		ConfigFingerprint: configFingerprint,
	}
	if err := writeJSONFile(filepath.Join(opts.OutDir, ManifestFile), manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func Load(dir string) (Dataset, error) {
	if dir == "" {
		return Dataset{}, model.NewError(model.CodeDataLoad, "data directory is required", "")
	}
	var items []model.Item
	if err := readJSONLines(filepath.Join(dir, ItemsFile), &items); err != nil {
		return Dataset{}, err
	}
	var queries []model.Query
	if err := readJSONLines(filepath.Join(dir, QueriesFile), &queries); err != nil {
		return Dataset{}, err
	}
	var judgments []model.Judgment
	if err := readJSONLines(filepath.Join(dir, JudgmentsFile), &judgments); err != nil {
		return Dataset{}, err
	}
	configData, err := os.ReadFile(filepath.Join(dir, ConfigFile))
	if err != nil {
		return Dataset{}, model.NewError(model.CodeDataLoad, "read config: "+err.Error(), "")
	}
	var config model.Config
	if err := json.Unmarshal(configData, &config); err != nil {
		return Dataset{}, model.NewError(model.CodeDataLoad, "parse config: "+err.Error(), "")
	}
	config = config.Normalize()
	if err := config.Validate(); err != nil {
		return Dataset{}, err
	}
	items = model.NormalizeItems(items)
	queries = model.NormalizeQueries(queries)
	judgments = model.NormalizeJudgments(judgments)
	if err := validateDataset(items, queries, judgments, config); err != nil {
		return Dataset{}, err
	}
	itemsDigest, err := model.ItemsDigest(items)
	if err != nil {
		return Dataset{}, err
	}
	queriesDigest, err := model.QueriesDigest(queries)
	if err != nil {
		return Dataset{}, err
	}
	judgmentsDigest, err := model.JudgmentsDigest(judgments)
	if err != nil {
		return Dataset{}, err
	}
	return Dataset{
		Items:           items,
		Queries:         queries,
		Judgments:       judgments,
		Config:          config,
		ItemsDigest:     itemsDigest,
		QueriesDigest:   queriesDigest,
		JudgmentsDigest: judgmentsDigest,
		DataFingerprint: model.DataFingerprint(itemsDigest, queriesDigest, judgmentsDigest),
	}, nil
}

func validateDataset(items []model.Item, queries []model.Query, judgments []model.Judgment, config model.Config) error {
	itemIDs := make(map[string]struct{}, len(items))
	for _, item := range items {
		if strings.TrimSpace(item.ItemID) == "" {
			return model.NewError(model.CodeDataLoad, "item_id must not be empty", "")
		}
		if _, exists := itemIDs[item.ItemID]; exists {
			return model.NewError(model.CodeDataLoad, "duplicate item_id: "+item.ItemID, "")
		}
		itemIDs[item.ItemID] = struct{}{}
		if len(item.Vector) != config.VectorDimension {
			return model.NewError(model.CodeDataLoad, "item vector dimension mismatch: "+item.ItemID, "")
		}
	}
	queryIDs := make(map[string]struct{}, len(queries))
	for _, query := range queries {
		if strings.TrimSpace(query.QueryID) == "" {
			return model.NewError(model.CodeDataLoad, "query_id must not be empty", "")
		}
		if _, exists := queryIDs[query.QueryID]; exists {
			return model.NewError(model.CodeDataLoad, "duplicate query_id: "+query.QueryID, "")
		}
		queryIDs[query.QueryID] = struct{}{}
	}
	for _, judgment := range judgments {
		if _, exists := queryIDs[judgment.QueryID]; !exists {
			return model.NewError(model.CodeDataLoad, "qrel references unknown query: "+judgment.QueryID, judgment.QueryID)
		}
		if _, exists := itemIDs[judgment.ItemID]; !exists {
			return model.NewError(model.CodeDataLoad, "qrel references unknown item: "+judgment.ItemID, judgment.QueryID)
		}
	}
	return nil
}

func writeJSONLines[T any](path string, values []T) error {
	return writeAtomic(path, func(f *os.File) error {
		w := bufio.NewWriterSize(f, 64*1024)
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		for _, value := range values {
			if err := enc.Encode(value); err != nil {
				return err
			}
		}
		return w.Flush()
	})
}

func writeJSONFile(path string, value any) error {
	return writeAtomic(path, func(f *os.File) error {
		w := bufio.NewWriterSize(f, 64*1024)
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		if err := enc.Encode(value); err != nil {
			return err
		}
		return w.Flush()
	})
}

func writeAtomic(path string, write func(*os.File) error) error {
	tmp := path + ".tmp-" + strconv.Itoa(os.Getpid())
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	writeErr := write(f)
	closeErr := f.Close()
	if writeErr != nil {
		_ = os.Remove(tmp)
		return writeErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(path)
		if retryErr := os.Rename(tmp, path); retryErr != nil {
			_ = os.Remove(tmp)
			return retryErr
		}
	}
	return nil
}

func readJSONLines[T any](path string, out *[]T) error {
	f, err := os.Open(path)
	if err != nil {
		return model.NewError(model.CodeDataLoad, "open "+filepath.Base(path)+": "+err.Error(), "")
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 32*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		data := scanner.Bytes()
		if len(strings.TrimSpace(string(data))) == 0 {
			continue
		}
		var value T
		if err := json.Unmarshal(data, &value); err != nil {
			return model.NewError(model.CodeDataLoad, fmt.Sprintf("parse %s line %d: %v", filepath.Base(path), line, err), "")
		}
		*out = append(*out, value)
	}
	if err := scanner.Err(); err != nil {
		return model.NewError(model.CodeDataLoad, "read "+filepath.Base(path)+": "+err.Error(), "")
	}
	return nil
}
