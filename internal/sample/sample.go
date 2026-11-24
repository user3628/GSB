package sample

import (
	"encoding/json"
	"fmt"
	"os"

	"local/sqlitepool/internal/domain"
)

type ConcurrentSample struct {
	Workers           int   `json:"workers"`
	RequestsPerWorker int   `json:"requests_per_worker"`
	AcquireTimeoutMS  int64 `json:"acquire_timeout_ms"`
}

type RecoverySample struct {
	AcquireTimeout int64 `json:"acquire_timeout_ms"`
}

type Sample struct {
	Listen       string                    `json:"listen"`
	DatabaseFile string                    `json:"database_file"`
	MetadataFile string                    `json:"metadata_file"`
	InitialPool  domain.Config             `json:"initial_pool"`
	Concurrent   ConcurrentSample          `json:"concurrent"`
	Recovery     RecoverySample            `json:"recovery"`
	Injections   map[string]map[string]any `json:"injections"`
}

func Load(path string) (Sample, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Sample{}, err
	}
	var s Sample
	if err := json.Unmarshal(data, &s); err != nil {
		return Sample{}, fmt.Errorf("decode sample: %w", err)
	}
	if err := s.InitialPool.Validate(); err != nil {
		return Sample{}, fmt.Errorf("sample pool config: %w", err)
	}
	if s.Listen == "" {
		return Sample{}, fmt.Errorf("sample listen address is required")
	}
	if s.DatabaseFile == "" || s.MetadataFile == "" {
		return Sample{}, fmt.Errorf("sample database_file and metadata_file are required")
	}
	return s, nil
}
