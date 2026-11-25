package dataset

import (
	"encoding/json"
	"os"

	"regworkbench/internal/canonical"
)

const ManifestName = "manifest.json"

type FeatureSpec struct {
	Name            string   `json:"name"`
	Index           int      `json:"index"`
	Semantic        string   `json:"semantic"`
	DeclaredMissing bool     `json:"declared_missing"`
	DefaultEncoding string   `json:"default_encoding"`
	Categories      []string `json:"categories,omitempty"`
}

type DatasetManifest struct {
	Key            string        `json:"key"`
	File           string        `json:"file"`
	Rows           int           `json:"rows"`
	Columns        int           `json:"columns"`
	FeatureColumns int           `json:"feature_columns"`
	LabelColumn    string        `json:"label_column"`
	Seed           int64         `json:"seed"`
	Generator      string        `json:"generator"`
	Scale          string        `json:"scale"`
	Features       []FeatureSpec `json:"features"`
}

type Manifest struct {
	Version  string            `json:"version"`
	Seed     int64             `json:"seed"`
	Manifest string            `json:"manifest"`
	Datasets []DatasetManifest `json:"datasets"`
}

func LoadManifest(path string) (*Manifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (m *Manifest) Find(key string) *DatasetManifest {
	for i := range m.Datasets {
		if m.Datasets[i].Key == key {
			return &m.Datasets[i]
		}
	}
	return nil
}

func SaveManifest(path string, m *Manifest) error {
	b, err := canonical.JSON(m)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return canonical.AtomicWriteFile(path, b, 0o644)
}
