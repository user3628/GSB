package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	Listen   string `json:"listen"`
	Database string `json:"database"`
	Seed     uint64 `json:"seed"`
}

func Default() Config {
	return Config{
		Listen:   "127.0.0.1:18190",
		Database: "./var/router.db",
		Seed:     20261125,
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %s: %w", path, err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config %s: %w", path, err)
	}
	if strings.TrimSpace(cfg.Listen) == "" {
		return Config{}, errors.New("config listen must not be empty")
	}
	if strings.TrimSpace(cfg.Database) == "" {
		return Config{}, errors.New("config database must not be empty")
	}
	if !filepath.IsAbs(cfg.Database) {
		cfg.Database = filepath.Join(filepath.Dir(path), cfg.Database)
	}
	return cfg, nil
}
