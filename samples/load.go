package samples

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"spmmopt/spmm"
)

func LoadManifest(dir string) (*Manifest, error) {
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("read sample manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("decode sample manifest: %w", err)
	}
	if m.Version != ManifestVersion {
		return nil, fmt.Errorf("unsupported sample manifest version %d", m.Version)
	}
	return &m, nil
}

func (m *Manifest) Load(dir string, name string) (*Sample, error) {
	for i := range m.Samples {
		if m.Samples[i].Name != name {
			continue
		}
		s := m.Samples[i]
		csr, err := spmm.LoadCSR(filepath.Join(dir, s.MatrixFile))
		if err != nil {
			return nil, err
		}
		dense, err := spmm.LoadDense(filepath.Join(dir, s.DenseFile))
		if err != nil {
			return nil, err
		}
		if !matrixInputsMatch(&s, csr, dense) {
			return nil, fmt.Errorf("sample %s no longer matches its manifest fingerprint", name)
		}
		s.CSR, s.Dense = csr, dense
		return &s, nil
	}
	return nil, fmt.Errorf("sample %q not found", name)
}

func matrixInputsMatch(s *Sample, csr *spmm.CSRMatrix, dense *spmm.DenseMatrix) bool {
	stats := csr.Stats(dense)
	return stats.ShapeFingerprint == s.Stats.ShapeFingerprint &&
		stats.StructureFingerprint == s.Stats.StructureFingerprint &&
		stats.DenseFingerprint == s.Stats.DenseFingerprint
}

func (m *Manifest) Names() []string {
	out := make([]string, len(m.Samples))
	for i := range m.Samples {
		out[i] = m.Samples[i].Name
	}
	return out
}
