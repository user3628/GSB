package main

import (
	"encoding/json"
	"os"
)

// Config 是外部参数注入入口，可由 JSON 配置文件覆盖默认值。
type Config struct {
	DBPath        string   `json:"db_path"`
	OutDir        string   `json:"out_dir"`
	Threshold     int      `json:"threshold"`
	MaxConcurrent int      `json:"max_concurrent"`
	ItemBudget    int      `json:"item_budget"`
	Inject        []string `json:"inject"`
	Roots         []string `json:"roots"`
}

func defaultConfig() Config {
	return Config{
		DBPath:        "depverify.db",
		OutDir:        "samples",
		Threshold:     32,
		MaxConcurrent: 2,
		ItemBudget:    0,
	}
}

func loadConfig(path string) (Config, error) {
	cfg := defaultConfig()
	if path == "" {
		return cfg, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
