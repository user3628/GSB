package rewardpack

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// GenerateSample writes a deterministic local sample set. Invalid-text records
// live in stress.jsonl so a valid full build remains possible while the error
// contract is still exercised explicitly by tests and the `validate` command.
func GenerateSample(dir string, seed int64) error {
	if err := os.MkdirAll(filepath.Join(dir, "shards"), 0o755); err != nil {
		return errorWith(CodeInvalidInput, err)
	}
	cfg := SampleConfig()
	configBytes, err := yaml.Marshal(cfg)
	if err != nil {
		return errorWith(CodeInternal, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), configBytes, 0o644); err != nil {
		return &BusinessError{Code: CodeInvalidInput, Context: "config.yaml", Message: err.Error()}
	}

	valid := sampleAnnotations(seed)
	shards := map[string][]Annotation{}
	for _, annotation := range valid {
		shards[annotation.Source] = append(shards[annotation.Source], annotation)
	}
	for _, source := range sortedStringKeys(shards) {
		lines := append([]Annotation(nil), shards[source]...)
		sort.Slice(lines, func(i, j int) bool { return lines[i].PairID < lines[j].PairID })
		if err := writeJSONL(filepath.Join(dir, "shards", source+".jsonl"), lines); err != nil {
			return err
		}
	}
	all := append([]Annotation(nil), valid...)
	stress := stressAnnotations()
	all = append(all, stress...)
	sort.Slice(all, func(i, j int) bool { return all[i].PairID < all[j].PairID })
	if err := writeJSONL(filepath.Join(dir, "annotations.jsonl"), all); err != nil {
		return err
	}
	if err := writeJSONL(filepath.Join(dir, "valid.jsonl"), valid); err != nil {
		return err
	}
	if err := writeJSONL(filepath.Join(dir, "stress.jsonl"), stress); err != nil {
		return err
	}
	readme := "Deterministic rewardpack sample\n\n" +
		"config.yaml       feature configuration\n" +
		"shards/*.jsonl    valid source shards for a full build\n" +
		"valid.jsonl       concatenation of all valid annotations\n" +
		"stress.jsonl      invalid text and out-of-range consistency cases\n" +
		"annotations.jsonl all cases, useful for error-contract tests\n"
	if err := os.WriteFile(filepath.Join(dir, "README.txt"), []byte(readme), 0o644); err != nil {
		return errorWith(CodeInvalidInput, err)
	}
	return nil
}

func SampleConfig() Config {
	projection := make([][]float64, 6)
	for row := 0; row < len(projection); row++ {
		projection[row] = make([]float64, 8)
		for column := 0; column < 8; column++ {
			value := float64(((row+1)*(column+2))%11-5) / 7.0
			if row == column {
				value += 0.5
			}
			projection[row][column] = value
		}
	}
	return Config{
		Version:       1,
		Normalization: NormalizationConfig{NFKC: true, CollapseWhitespace: true, Lowercase: true},
		Scoring: ScoringConfig{
			TermWeights: map[string]float64{
				"好": 1.8, "差": 1.2, "准确": 1.6, "清楚": 1.4,
				"good": 1.5, "bad": 1.2, "clear": 1.4, "accurate": 1.6,
				"推荐": 1.3, "错误": 1.7,
			},
			DefaultTermWeight:         1,
			LengthPenalty:             0.18,
			OverlapScale:              1.1,
			FactorFloors:              FactorFloors{Length: 0.02, Overlap: 0.02, Consistency: 0.01},
			StrengthSignificantDigits: 8,
		},
		Features: FeaturesConfig{
			Dimension:        8,
			Projection:       projection,
			QuantizationBits: 8,
		},
		Budget: BudgetConfig{
			GlobalTokens:       72,
			MinRetentionUnit:   3,
			DefaultSourceQuota: 30,
			SourceQuotas: map[string]int{
				"shard-a":          44,
				"shard-b":          32,
				"shard-c":          24,
				"shard-d":          2,
				"configured-empty": 8,
			},
			RequiredLanguages: []string{"zh", "en", "mixed"},
			LanguageMinimums:  map[string]int{"zh": 1, "en": 1, "mixed": 1},
		},
	}
}

func sampleAnnotations(seed int64) []Annotation {
	values := []Annotation{
		{PairID: "a-001", Prompt: "解释一下缓存穿透", Chosen: "使用布隆过滤器并缓存空值。", Rejected: "直接忽略。", Source: "shard-a", Lang: "zh", Consistency: 0.91},
		{PairID: "a-002", Prompt: "Explain connection pooling", Chosen: "Use a bounded pool and recycle connections.", Rejected: "Open a new connection for every query.", Source: "shard-a", Lang: "en", Consistency: 0.88},
		{PairID: "a-003", Prompt: "same prompt 同一问题", Chosen: "先复现，再定位日志与指标。", Rejected: "重启服务即可。", Source: "shard-a", Lang: "mixed", Consistency: 0.73},
		{PairID: "a-004", Prompt: "same prompt 同一问题", Chosen: "先复现并检查 trace，保持证据。", Rejected: "先清空数据库再观察。", Source: "shard-a", Lang: "mixed", Consistency: 0.95},
		{PairID: "a-005", Prompt: "如何写 commit message", Chosen: "说明动机和影响，good and clear.", Rejected: "update stuff", Source: "shard-a", Lang: "mixed", Consistency: 0.82},
		{PairID: "b-001", Prompt: "Design a retry policy", Chosen: "Use exponential backoff with jitter and a clear cap.", Rejected: "Retry forever without delay.", Source: "shard-b", Lang: "en", Consistency: 0.94},
		{PairID: "b-002", Prompt: "解释 SQLite WAL", Chosen: "WAL 允许读者与写者并发，checkpoint 控制日志大小。", Rejected: "WAL 没有任何代价。", Source: "shard-b", Lang: "mixed", Consistency: 0.86},
		{PairID: "b-003", Prompt: "如何做幂等", Chosen: "使用幂等键并记录处理结果。", Rejected: "重复执行也没关系。", Source: "shard-b", Lang: "zh", Consistency: 0.9},
		{PairID: "c-001", Prompt: "测试失败怎么办", Chosen: "先阅读错误，再最小复现并修复根因。", Rejected: "删除失败测试。", Source: "shard-c", Lang: "zh", Consistency: 0.97},
		{PairID: "c-002", Prompt: "如何审查边界", Chosen: "检查空值、重复输入和越界。", Rejected: "只测正常路径。", Source: "shard-c", Lang: "zh", Consistency: 0.84},
		{PairID: "c-003", Prompt: "How to audit a feature pack", Chosen: "Compare digests and document every fallback.", Rejected: "Trust the cache blindly.", Source: "shard-c", Lang: "en", Consistency: 0.8},
		{PairID: "d-001", Prompt: "低配额来源", Chosen: "这个 source 只保留一条候选。", Rejected: "该回复不会被选中。", Source: "shard-d", Lang: "zh", Consistency: 0.77},
	}
	for i := range values {
		// A deterministic sub-unit perturbation proves output stability without
		// introducing nondeterminism or an unintended tie.
		delta := (seed + int64(i)*7919) % 5
		values[i].Consistency += float64(delta-2) / 1000.0
	}
	sort.Slice(values, func(i, j int) bool { return values[i].PairID < values[j].PairID })
	return values
}

func stressAnnotations() []Annotation {
	return []Annotation{
		{PairID: "z-001-empty-chosen", Prompt: "空回复测试", Chosen: "", Rejected: "这是一个有效回复。", Source: "stress", Lang: "zh", Consistency: 0.5},
		{PairID: "z-002-punctuation", Prompt: "标点测试", Chosen: "！！！", Rejected: "This is a valid reply.", Source: "stress", Lang: "mixed", Consistency: 0.5},
		{PairID: "z-003-out-of-range", Prompt: "越界一致性", Chosen: "正确的 good answer。", Rejected: "错误的 bad answer。", Source: "stress", Lang: "mixed", Consistency: 1.75},
		{PairID: "z-004-negative", Prompt: "负一致性", Chosen: "清晰 clear reply", Rejected: "模糊 reply", Source: "stress", Lang: "mixed", Consistency: -0.4},
	}
}

func writeJSONL(path string, values []Annotation) error {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	for _, value := range values {
		if err := encoder.Encode(value); err != nil {
			return errorWith(CodeInternal, err)
		}
	}
	if err := os.WriteFile(path, buffer.Bytes(), 0o644); err != nil {
		return errorWith(CodeInvalidInput, err)
	}
	return nil
}

// ReadJSONL loads annotations while preserving source order inside each file.
func ReadJSONL(path string) ([]Annotation, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errorWith(CodeInvalidInput, fmt.Errorf("open %s: %w", path, err))
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	var result []Annotation
	line := 0
	for scanner.Scan() {
		line++
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		var annotation Annotation
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&annotation); err != nil {
			return nil, &BusinessError{Code: CodeInvalidInput, Context: fmt.Sprintf("%s:%d", path, line), Message: err.Error()}
		}
		if err := validateAnnotation(annotation); err != nil {
			return nil, err
		}
		result = append(result, annotation)
	}
	if err := scanner.Err(); err != nil {
		return nil, errorWith(CodeInvalidInput, err)
	}
	return result, nil
}

// LoadShards reads every *.jsonl file under dir in lexical order and rejects
// duplicate pair IDs across shards.
func LoadShards(dir string) ([]Annotation, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		return nil, errorWith(CodeInvalidInput, err)
	}
	sort.Strings(matches)
	seen := map[string]string{}
	var result []Annotation
	for _, path := range matches {
		values, err := ReadJSONL(path)
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			if previous, ok := seen[value.PairID]; ok {
				return nil, &BusinessError{Code: CodeInvalidInput, PairID: value.PairID, Context: previous + " and " + path, Message: "duplicate pair_id across shards"}
			}
			seen[value.PairID] = path
			result = append(result, value)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].PairID < result[j].PairID })
	return result, nil
}

// SyncShardDirectory replaces every observed source with the annotations from
// its files. Files are grouped by their annotation source field.
func SyncShardDirectory(ctx context.Context, store *Store, dir string) error {
	annotations, err := LoadShards(dir)
	if err != nil {
		return err
	}
	bySource := map[string][]Annotation{}
	for _, annotation := range annotations {
		bySource[annotation.Source] = append(bySource[annotation.Source], annotation)
	}
	for _, source := range sortedStringKeys(bySource) {
		if err := store.ReplaceSource(ctx, source, bySource[source]); err != nil {
			return err
		}
	}
	return nil
}
