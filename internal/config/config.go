package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type SessionSpec struct {
	SessionID     string `json:"session_id"`
	RequestsFile  string `json:"requests_file"`
	RequestBudget int    `json:"request_budget"`
}

type RunConfig struct {
	Schema           string        `json:"schema"`
	PolicyFile       string        `json:"policy_file"`
	ConcurrencyLimit int           `json:"concurrency_limit"`
	LatencyClock     string        `json:"latency_clock"`
	RequestBudget    int           `json:"request_budget"`
	Sessions         []SessionSpec `json:"sessions"`
}

type AnomalyParams struct {
	Schema                 string        `json:"schema"`
	Case                   string        `json:"case"`
	PolicyFile             string        `json:"policy_file,omitempty"`
	ReplacementPolicyFile  string        `json:"replacement_policy_file,omitempty"`
	RequestsFile           string        `json:"requests_file,omitempty"`
	ConcurrencyLimit       int           `json:"concurrency_limit,omitempty"`
	RequestBudget          int           `json:"request_budget,omitempty"`
	Sessions               []SessionSpec `json:"sessions,omitempty"`
	ExpectedCodes          []string      `json:"expected_codes"`
	ExpectSnapshotClean    bool          `json:"expect_snapshot_clean"`
	ExpectOthersUnaffected bool          `json:"expect_other_sessions_unaffected"`
}

func ReadRun(path string) (RunConfig, error) {
	var cfg RunConfig
	if err := readJSON(path, &cfg); err != nil {
		return cfg, err
	}
	base := filepath.Dir(path)
	cfg.PolicyFile = Resolve(base, cfg.PolicyFile)
	for i := range cfg.Sessions {
		cfg.Sessions[i].RequestsFile = Resolve(base, cfg.Sessions[i].RequestsFile)
	}
	if cfg.Schema == "" {
		cfg.Schema = "vpnbench.run/v1"
	}
	if cfg.ConcurrencyLimit < 1 {
		cfg.ConcurrencyLimit = 1
	}
	if cfg.LatencyClock == "" {
		cfg.LatencyClock = "fixed"
	}
	if cfg.RequestBudget == 0 {
		cfg.RequestBudget = 10000
	}
	for i := range cfg.Sessions {
		if cfg.Sessions[i].RequestBudget == 0 {
			cfg.Sessions[i].RequestBudget = cfg.RequestBudget
		}
	}
	return cfg, nil
}

func ReadAnomaly(path string) (AnomalyParams, error) {
	var params AnomalyParams
	if err := readJSON(path, &params); err != nil {
		return params, err
	}
	base := filepath.Dir(path)
	params.PolicyFile = Resolve(base, params.PolicyFile)
	params.ReplacementPolicyFile = Resolve(base, params.ReplacementPolicyFile)
	params.RequestsFile = Resolve(base, params.RequestsFile)
	for i := range params.Sessions {
		params.Sessions[i].RequestsFile = Resolve(base, params.Sessions[i].RequestsFile)
	}
	if params.Schema == "" {
		params.Schema = "vpnbench.anomaly/v1"
	}
	if params.ConcurrencyLimit < 1 {
		params.ConcurrencyLimit = 1
	}
	if params.RequestBudget == 0 {
		params.RequestBudget = 1000
	}
	for i := range params.Sessions {
		if params.Sessions[i].RequestBudget == 0 {
			params.Sessions[i].RequestBudget = params.RequestBudget
		}
	}
	return params, nil
}

func Resolve(base, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Clean(filepath.Join(base, path))
}

func readJSON(path string, destination any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(data, destination); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}
