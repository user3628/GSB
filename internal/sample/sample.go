// Package sample generates fully deterministic local samples from a fixed
// seed: a block manifest, encoded page data and multi-replica digest
// reports. No cluster or external service is involved.
package sample

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"blockinspect/internal/manifest"
)

// Spec describes one deterministic sample.
type Spec struct {
	Name           string   `json:"name"`
	Seed           int64    `json:"seed"`
	BlockID        string   `json:"block_id"`
	Rows           int64    `json:"rows"`
	PageCount      int      `json:"page_count"`
	ColumnsPerPage int      `json:"columns_per_page"`
	Replicas       []string `json:"replicas"`
	// CorruptReplica, when non-empty, gets CorruptPages of its reported
	// page digests flipped so divergence location can be exercised.
	CorruptReplica string `json:"corrupt_replica,omitempty"`
	CorruptPages   []int  `json:"corrupt_pages,omitempty"`
}

// DefaultSpec returns the canonical fixed-seed sample used by the
// verification script.
func DefaultSpec() Spec {
	return Spec{
		Name:           "fixed-seed-demo",
		Seed:           20261125,
		BlockID:        "blk-demo-0001",
		Rows:           1_000_000,
		PageCount:      96,
		ColumnsPerPage: 8,
		Replicas:       []string{"replica-a", "replica-b", "replica-c"},
		CorruptReplica: "replica-c",
		CorruptPages:   []int{7, 8, 41},
	}
}

// ReplicaReport is what one replica reports during inspection.
type ReplicaReport struct {
	ReplicaID   string   `json:"replica_id"`
	BlockID     string   `json:"block_id"`
	PageDigests []string `json:"page_digests"`
	RootDigest  string   `json:"root_digest"`
}

// PageData returns the deterministic encoded bytes of one page.
func PageData(seed int64, pageNo int, length int64) []byte {
	r := rand.New(rand.NewSource(seed*1_000_003 + int64(pageNo)*9_176 + 17))
	b := make([]byte, length)
	r.Read(b)
	return b
}

// Generate writes the sample into dir and returns the built manifest.
// The same spec always produces byte-identical output.
func Generate(spec Spec, dir string) (*manifest.Manifest, error) {
	r := rand.New(rand.NewSource(spec.Seed))
	m := &manifest.Manifest{
		BlockID:        spec.BlockID,
		Rows:           spec.Rows,
		PageCount:      spec.PageCount,
		ColumnsPerPage: spec.ColumnsPerPage,
		Seed:           spec.Seed,
	}
	pagesDir := filepath.Join(dir, "pages")
	reportsDir := filepath.Join(dir, "reports")
	if err := os.MkdirAll(pagesDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(reportsDir, 0o755); err != nil {
		return nil, err
	}
	for p := 0; p < spec.PageCount; p++ {
		m.PageLengths = append(m.PageLengths, int64(256+r.Intn(768)))
	}
	for p := 0; p < spec.PageCount; p++ {
		data := PageData(spec.Seed, p, m.PageLengths[p])
		m.LeafDigests = append(m.LeafDigests, manifest.LeafDigest(data))
		if err := os.WriteFile(filepath.Join(pagesDir, pageFileName(p)), data, 0o644); err != nil {
			return nil, err
		}
	}
	m.RootDigest = manifest.BuildTree(m.LeafDigests).Root()

	mb, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), mb, 0o644); err != nil {
		return nil, err
	}

	corrupt := map[int]bool{}
	for _, p := range spec.CorruptPages {
		corrupt[p] = true
	}
	for _, rid := range spec.Replicas {
		rep := ReplicaReport{ReplicaID: rid, BlockID: spec.BlockID}
		digests := append([]manifest.Digest(nil), m.LeafDigests...)
		if rid == spec.CorruptReplica {
			for p := range corrupt {
				if p >= 0 && p < len(digests) {
					digests[p][0] ^= 0xFF // deterministic corruption
					digests[p][len(digests[p])-1] ^= 0x01
				}
			}
		}
		for _, d := range digests {
			rep.PageDigests = append(rep.PageDigests, d.Hex())
		}
		rep.RootDigest = manifest.BuildTree(digests).Root().Hex()
		rb, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(reportsDir, rid+".json"), rb, 0o644); err != nil {
			return nil, err
		}
	}

	sb, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "sample.json"), sb, 0o644); err != nil {
		return nil, err
	}
	return m, nil
}

// LoadReport reads one replica report from disk.
func LoadReport(path string) (*ReplicaReport, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rep ReplicaReport
	if err := json.Unmarshal(b, &rep); err != nil {
		return nil, fmt.Errorf("parse report %s: %w", path, err)
	}
	return &rep, nil
}

func pageFileName(p int) string { return fmt.Sprintf("page-%06d.bin", p) }

// PagePath returns the deterministic path of a page data file.
func PagePath(sampleDir string, p int) string {
	return filepath.Join(sampleDir, "pages", pageFileName(p))
}
