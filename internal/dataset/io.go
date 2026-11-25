package dataset

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"regworkbench/internal/canonical"
)

type Dataset struct {
	Spec    DatasetManifest `json:"spec"`
	Header  []string        `json:"header"`
	Records [][]string      `json:"records"`
	RawSHA  string          `json:"raw_sha256"`
}

func LoadDataset(manifestPath, key string) (*Dataset, error) {
	m, err := LoadManifest(manifestPath)
	if err != nil {
		return nil, err
	}
	spec := m.Find(key)
	if spec == nil {
		return nil, fmt.Errorf("dataset key %q not found in manifest", key)
	}
	csvPath := filepath.Join(filepath.Dir(manifestPath), spec.File)
	return LoadDatasetCSV(csvPath, *spec)
}

func LoadDatasetCSV(csvPath string, spec DatasetManifest) (*Dataset, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	records, err := r.ReadAll()
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(header) != spec.Columns {
		return nil, fmt.Errorf("csv %s has %d columns, manifest declares %d", csvPath, len(header), spec.Columns)
	}
	if len(records) != spec.Rows {
		return nil, fmt.Errorf("csv %s has %d rows, manifest declares %d", csvPath, len(records), spec.Rows)
	}
	rawSHA, err := canonical.SHA256File(csvPath)
	if err != nil {
		return nil, err
	}
	return &Dataset{Spec: spec, Header: header, Records: records, RawSHA: rawSHA}, nil
}
