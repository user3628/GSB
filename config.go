package mfa

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func LoadConfig(reader io.Reader) (Config, error) {
	var cfg Config
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, policyError("config", fmt.Sprintf("invalid JSON: %v", err))
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Config{}, policyError("config", "multiple JSON values are not allowed")
	}
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return Config{}, err
	}
	_ = normalized
	return cfg, nil
}

func LoadConfigFile(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer file.Close()
	return LoadConfig(file)
}

func ValidateConfig(cfg Config) (NormalizedConfig, error) {
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return NormalizedConfig{}, err
	}
	return *normalized, nil
}
