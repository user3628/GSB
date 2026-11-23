package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"gatewayfreshness.local/workbench/internal/freshness"
)

type Config struct {
	HTTPAddr               string `json:"http_addr"`
	DBPath                 string `json:"db_path"`
	SigningKey             string `json:"signing_key"`
	SigningKeyID           string `json:"signing_key_id"`
	MaxToleranceMS         int64  `json:"max_tolerance_ms"`
	MaxWindowMS            int64  `json:"max_window_ms"`
	MaxRequestsPerBatch    int    `json:"max_requests_per_batch"`
	FastPrecheckLimit      int    `json:"fast_precheck_limit"`
	DefaultQueryBudgetMS   int    `json:"default_query_budget_ms"`
	MaxDiagnosticBudgetMS  int    `json:"max_diagnostic_budget_ms"`
	AllowReferenceOverride bool   `json:"allow_reference_override"`
	MaxRequestBodyBytes    int64  `json:"max_request_body_bytes"`

	configVersion string
	baseDir       string
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	if err := c.normalize(); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(path)
	if err == nil {
		c.baseDir = filepath.Dir(abs)
	}
	return &c, nil
}

func (c *Config) normalize() error {
	if c.HTTPAddr == "" {
		c.HTTPAddr = "127.0.0.1:8787"
	}
	if c.DBPath == "" {
		c.DBPath = filepath.Join("data", "workbench.db")
	}
	if err := EnsureLoopback(c.HTTPAddr); err != nil {
		return err
	}
	if len(c.SigningKey) < 16 {
		return fmt.Errorf("signing_key must be at least 16 bytes")
	}
	if c.SigningKeyID == "" {
		c.SigningKeyID = "local-key"
	}
	if c.MaxToleranceMS <= 0 {
		return fmt.Errorf("max_tolerance_ms must be positive")
	}
	if c.MaxWindowMS <= 0 {
		return fmt.Errorf("max_window_ms must be positive")
	}
	if c.MaxRequestsPerBatch <= 0 {
		c.MaxRequestsPerBatch = 1000
	}
	if c.FastPrecheckLimit <= 0 {
		return fmt.Errorf("fast_precheck_limit must be positive")
	}
	if c.FastPrecheckLimit > c.MaxRequestsPerBatch {
		c.FastPrecheckLimit = c.MaxRequestsPerBatch
	}
	if c.DefaultQueryBudgetMS <= 0 {
		c.DefaultQueryBudgetMS = 50
	}
	if c.MaxDiagnosticBudgetMS <= 0 {
		c.MaxDiagnosticBudgetMS = 5000
	}
	if c.DefaultQueryBudgetMS > c.MaxDiagnosticBudgetMS {
		c.DefaultQueryBudgetMS = c.MaxDiagnosticBudgetMS
	}
	if c.MaxRequestBodyBytes <= 0 {
		c.MaxRequestBodyBytes = 4 << 20
	}
	c.configVersion = "cfg-" + shortHash(
		"gwfresh.config.v1",
		c.SigningKeyID,
		hex.EncodeToString(sha256Bytes([]byte(c.SigningKey))),
		fmt.Sprint(c.MaxToleranceMS),
		fmt.Sprint(c.MaxWindowMS),
		fmt.Sprint(c.MaxRequestsPerBatch),
		fmt.Sprint(c.FastPrecheckLimit),
		fmt.Sprint(c.DefaultQueryBudgetMS),
		fmt.Sprint(c.MaxDiagnosticBudgetMS),
		fmt.Sprint(c.AllowReferenceOverride),
	)[:20]
	return nil
}

func (c *Config) Version() string { return c.configVersion }

func (c *Config) SigningKeyBytes() []byte { return []byte(c.SigningKey) }

func (c *Config) ResolveDBPath(override string) string {
	p := override
	if p == "" {
		p = c.DBPath
	}
	if filepath.IsAbs(p) || c.baseDir == "" {
		return p
	}
	// Relative sample paths are rooted at the repository working directory,
	// not at the config directory, so the documented commands stay stable.
	return p
}

func (c *Config) ValidationPolicy() freshness.ValidationPolicy {
	return freshness.ValidationPolicy{
		MaxToleranceMS:         c.MaxToleranceMS,
		MaxWindowMS:            c.MaxWindowMS,
		MaxRequestsPerBatch:    c.MaxRequestsPerBatch,
		AllowReferenceOverride: c.AllowReferenceOverride,
	}
}

func (c *Config) View() freshness.ConfigView {
	return freshness.ConfigView{
		Version:             c.configVersion,
		SigningKeyID:        c.SigningKeyID,
		FastPrecheckLimit:   c.FastPrecheckLimit,
		MaxToleranceMS:      c.MaxToleranceMS,
		MaxWindowMS:         c.MaxWindowMS,
		MaxRequestsPerBatch: c.MaxRequestsPerBatch,
		QueryBudgetMS:       c.DefaultQueryBudgetMS,
	}
}

func (c *Config) SnapshotJSON() string {
	data, _ := json.Marshal(c.View())
	return string(data)
}

func EnsureLoopback(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("http_addr must be host:port: %w", err)
	}
	if port == "" {
		return fmt.Errorf("http_addr must include a port")
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("http_addr must bind to a loopback address, got %q", host)
	}
	return nil
}

func shortHash(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		_, _ = fmt.Fprintf(h, "%d:%s|", len(p), p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func sha256Bytes(b []byte) []byte {
	s := sha256.Sum256(b)
	return s[:]
}
