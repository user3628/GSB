package config

import (
	"encoding/json"
	"os"
)

type File struct {
	Mode                string   `json:"mode,omitempty"`
	MaxConcurrency      int      `json:"max_concurrency,omitempty"`
	MaxItems            int      `json:"max_items,omitempty"`
	MaxErrorBoundMicros int64    `json:"max_error_bound_micros,omitempty"`
	Inject              []string `json:"inject,omitempty"`
}

func Load(path string) (File, error) {
	if path == "" {
		return File{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return File{}, err
	}
	var value File
	if err := json.Unmarshal(data, &value); err != nil {
		return File{}, err
	}
	return value, nil
}
