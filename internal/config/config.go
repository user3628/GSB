package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Version   string          `json:"version" yaml:"version"`
	Seed      int64           `json:"seed" yaml:"seed"`
	Tokenizer TokenizerConfig `json:"tokenizer" yaml:"tokenizer"`
	Index     IndexConfig     `json:"index" yaml:"index"`
	Query     QueryConfig     `json:"query" yaml:"query"`
}

type TokenizerConfig struct {
	Granularity   string   `json:"granularity" yaml:"granularity"`
	Normalization []string `json:"normalization" yaml:"normalization"`
	Stopwords     []string `json:"stopwords" yaml:"stopwords"`
}

type IndexConfig struct {
	SkipStep       int        `json:"skip_step" yaml:"skip_step"`
	BlockSize      int        `json:"block_size" yaml:"block_size"`
	DeltaEncoding  string     `json:"delta_encoding" yaml:"delta_encoding"`
	VarintEncoding string     `json:"varint_encoding" yaml:"varint_encoding"`
	BM25           BM25Config `json:"bm25" yaml:"bm25"`
	QuantBits      int        `json:"quant_bits" yaml:"quant_bits"`
}

type BM25Config struct {
	K1 float64 `json:"k1" yaml:"k1"`
	B  float64 `json:"b" yaml:"b"`
}

type QueryConfig struct {
	CacheCapacity   int `json:"cache_capacity" yaml:"cache_capacity"`
	LatencyBudgetMS int `json:"latency_budget_ms" yaml:"latency_budget_ms"`
}

func Load(path string) (Config, []byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, nil, err
	}
	return cfg.canonical(), raw, nil
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Version) == "" {
		return fmt.Errorf("config version is required")
	}
	if c.Tokenizer.Granularity != "cjk_unigram_bigram" {
		return fmt.Errorf("unsupported tokenizer granularity %q", c.Tokenizer.Granularity)
	}
	for _, n := range c.Tokenizer.Normalization {
		if n != "NFKC" && n != "lowercase" && n != "collapse_whitespace" {
			return fmt.Errorf("unsupported normalization %q", n)
		}
	}
	if !contains(c.Tokenizer.Normalization, "NFKC") || !contains(c.Tokenizer.Normalization, "lowercase") || !contains(c.Tokenizer.Normalization, "collapse_whitespace") {
		return fmt.Errorf("normalization must include NFKC, lowercase and collapse_whitespace")
	}
	if c.Index.SkipStep <= 0 || c.Index.BlockSize <= 0 {
		return fmt.Errorf("skip_step and block_size must be positive")
	}
	if c.Index.DeltaEncoding != "docid_delta" && c.Index.DeltaEncoding != "docid_delta_first" {
		return fmt.Errorf("unsupported delta_encoding %q", c.Index.DeltaEncoding)
	}
	if c.Index.VarintEncoding != "base128_zigzag" && c.Index.VarintEncoding != "base128" {
		return fmt.Errorf("unsupported varint_encoding %q", c.Index.VarintEncoding)
	}
	if c.Index.QuantBits < 2 || c.Index.QuantBits > 31 {
		return fmt.Errorf("quant_bits must be between 2 and 31")
	}
	if c.Index.BM25.K1 < 0 || c.Index.BM25.B < 0 || c.Index.BM25.B > 1 {
		return fmt.Errorf("invalid BM25 parameters: k1=%v b=%v", c.Index.BM25.K1, c.Index.BM25.B)
	}
	if c.Query.CacheCapacity < 0 {
		return fmt.Errorf("cache_capacity must be non-negative")
	}
	if c.Query.LatencyBudgetMS <= 0 {
		return fmt.Errorf("latency_budget_ms must be positive")
	}
	return nil
}

func (c Config) canonical() Config {
	c.Tokenizer.Stopwords = append([]string(nil), c.Tokenizer.Stopwords...)
	sort.Strings(c.Tokenizer.Stopwords)
	c.Tokenizer.Normalization = append([]string(nil), c.Tokenizer.Normalization...)
	sort.Strings(c.Tokenizer.Normalization)
	return c
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func (c Config) CanonicalJSON() []byte {
	b, _ := json.Marshal(c.canonical())
	return b
}

func (c Config) Digest() string {
	return DigestBytes(c.CanonicalJSON())
}

func (c Config) TokenizerDigest() string {
	payload := struct {
		Version   string          `json:"version"`
		Tokenizer TokenizerConfig `json:"tokenizer"`
	}{c.Version, c.canonical().Tokenizer}
	b, _ := json.Marshal(payload)
	return DigestBytes(b)
}

func (c Config) StatsDigest() string {
	payload := struct {
		Version   string     `json:"version"`
		BM25      BM25Config `json:"bm25"`
		QuantBits int        `json:"quant_bits"`
	}{c.Version, c.Index.BM25, c.Index.QuantBits}
	b, _ := json.Marshal(payload)
	return DigestBytes(b)
}

func (c Config) PostingsDigest() string {
	payload := struct {
		Version        string `json:"version"`
		SkipStep       int    `json:"skip_step"`
		BlockSize      int    `json:"block_size"`
		DeltaEncoding  string `json:"delta_encoding"`
		VarintEncoding string `json:"varint_encoding"`
	}{c.Version, c.Index.SkipStep, c.Index.BlockSize, c.Index.DeltaEncoding, c.Index.VarintEncoding}
	b, _ := json.Marshal(payload)
	return DigestBytes(b)
}

func (c Config) DiagnosticsDigest() string {
	payload := struct {
		Version string `json:"version"`
		Stats   string `json:"stats"`
	}{c.Version, c.StatsDigest()}
	b, _ := json.Marshal(payload)
	return DigestBytes(b)
}

func DigestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func MustDefault() Config {
	c := Config{
		Version: "demo-v1",
		Seed:    42,
		Tokenizer: TokenizerConfig{
			Granularity:   "cjk_unigram_bigram",
			Normalization: []string{"NFKC", "lowercase", "collapse_whitespace"},
			Stopwords:     []string{"a", "an", "and", "is", "of", "the", "to", "的", "了", "和", "是"},
		},
		Index: IndexConfig{
			SkipStep:       4,
			BlockSize:      6,
			DeltaEncoding:  "docid_delta",
			VarintEncoding: "base128_zigzag",
			BM25:           BM25Config{K1: 1.2, B: 0.75},
			QuantBits:      12,
		},
		Query: QueryConfig{CacheCapacity: 32, LatencyBudgetMS: 250},
	}
	return c.canonical()
}
