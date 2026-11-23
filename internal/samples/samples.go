package samples

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"baselineworkbench/internal/parse"
	"baselineworkbench/internal/policy"
)

const (
	Schema       = "baseline.workbench.samples/v1"
	SampleMarker = "SAMPLE-FIXED-2024-01-01T00:00:00Z"
)

type Package struct {
	Path     string `json:"path"`
	Class    string `json:"class"`
	Baseline string `json:"baseline"`
	Repairs  string `json:"repairs"`
}

type Injection struct {
	Class         string `json:"class"`
	PackagePath   string `json:"package_path"`
	ForcedVersion int64  `json:"forced_version"`
	ExpectedCode  string `json:"expected_code"`
}

type Manifest struct {
	Schema       string      `json:"schema"`
	SampleMarker string      `json:"sample_marker"`
	Reference    string      `json:"reference_package"`
	Packages     []Package   `json:"packages"`
	Injections   []Injection `json:"injections"`
}

type fileSpec struct {
	Path string
	Data []byte
}

type itemSpec struct {
	ID     string `json:"id"`
	Unit   string `json:"unit"`
	Value  string `json:"value"`
	Weight string `json:"weight"`
}

type baselineDoc struct {
	Schema       string     `json:"schema"`
	PackageID    string     `json:"package_id"`
	SampleMarker string     `json:"sample_marker"`
	Items        []itemSpec `json:"items"`
}

var referenceItems = []itemSpec{
	{ID: "account.lockout_threshold", Unit: "second", Value: "900", Weight: "1.0"},
	{ID: "audit.retention_days", Unit: "day", Value: "90", Weight: "0.75"},
	{ID: "host.snapshot.filetime", Unit: "filetime", Value: "133485408000000000", Weight: "1.0"},
	{ID: "log.level.score", Unit: "score", Value: "3.141592", Weight: "1.25"},
	{ID: "password.max_age", Unit: "day", Value: "60", Weight: "0.5"},
	{ID: "session.idle_timeout", Unit: "second", Value: "600", Weight: "1.0"},
}

var referenceByID = func() map[string]itemSpec {
	out := make(map[string]itemSpec, len(referenceItems))
	for _, item := range referenceItems {
		out[item.ID] = item
	}
	return out
}()

func Generate(root string) (Manifest, error) {
	if err := os.RemoveAll(root); err != nil {
		return Manifest{}, err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{
		Schema:       Schema,
		SampleMarker: SampleMarker,
		Reference:    "packages/reference",
		Packages: []Package{
			{Path: "packages/reference", Class: "reference", Baseline: "baseline.json", Repairs: "repairs.txt"},
			{Path: "packages/normal", Class: "normal", Baseline: "baseline.json", Repairs: "repairs.txt"},
			{Path: "packages/mixed-units", Class: "mixed_units", Baseline: "baseline.json", Repairs: "repairs.txt"},
			{Path: "packages/truncated-text", Class: "truncated_text", Baseline: "baseline.json", Repairs: "repairs.txt"},
			{Path: "packages/unknown-unit", Class: "unknown_unit", Baseline: "baseline.json", Repairs: "repairs.txt"},
			{Path: "packages/overflow-multiply", Class: "numeric_overflow", Baseline: "baseline.json", Repairs: "repairs.txt"},
			{Path: "packages/filetime-overflow", Class: "numeric_overflow", Baseline: "baseline.json", Repairs: "repairs.txt"},
			{Path: "packages/error-bound", Class: "error_bound", Baseline: "baseline.json", Repairs: "repairs.txt"},
		},
		Injections: []Injection{
			{
				Class:         "snapshot_version_gap",
				PackagePath:   "packages/normal",
				ForcedVersion: 3,
				ExpectedCode:  "E_SNAPSHOT_VERSION",
			},
		},
	}
	files, err := sampleFiles()
	if err != nil {
		return Manifest{}, err
	}
	for _, file := range files {
		target := filepath.Join(root, filepath.FromSlash(file.Path))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return Manifest{}, err
		}
		if err := os.WriteFile(target, file.Data, 0o644); err != nil {
			return Manifest{}, err
		}
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return Manifest{}, err
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), encoded, 0o644); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func sampleFiles() ([]fileSpec, error) {
	files := make([]fileSpec, 0, 24)
	addPackage := func(path, class string, baseline []itemSpec, repairs []itemSpec) error {
		doc := baselineDoc{
			Schema:       "baseline.workbench.snapshot/v1",
			PackageID:    class,
			SampleMarker: SampleMarker,
			Items:        baseline,
		}
		encoded, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			return err
		}
		encoded = append(encoded, '\n')
		files = append(files, fileSpec{Path: path + "/baseline.json", Data: encoded})
		files = append(files, fileSpec{Path: path + "/repairs.txt", Data: encodeRepairs(repairs)})
		return nil
	}

	if err := addPackage("packages/reference", "reference", copyItems(referenceItems), copyItems(referenceItems)); err != nil {
		return nil, err
	}
	normalBaseline := copyItems(referenceItems)
	setItem(normalBaseline, "account.lockout_threshold", itemSpec{ID: "account.lockout_threshold", Unit: "second", Value: "600", Weight: "1.0"})
	setItem(normalBaseline, "audit.retention_days", itemSpec{ID: "audit.retention_days", Unit: "day", Value: "30", Weight: "0.75"})
	normalRepairs := copyItems(referenceItems)
	setItem(normalRepairs, "audit.retention_days", itemSpec{ID: "audit.retention_days", Unit: "day", Value: "89", Weight: "0.75"})
	if err := addPackage("packages/normal", "normal", normalBaseline, normalRepairs); err != nil {
		return nil, err
	}

	mixedBaseline := []itemSpec{
		{ID: "account.lockout_threshold", Unit: "minute", Value: "15", Weight: "1.0"},
		{ID: "audit.retention_days", Unit: "hour", Value: "2160", Weight: "0.75"},
		{ID: "host.snapshot.filetime", Unit: "filetime", Value: "133485408000000000", Weight: "1.0"},
		{ID: "log.level.score", Unit: "score", Value: "3.141592", Weight: "1.25"},
		{ID: "password.max_age", Unit: "hour", Value: "1440", Weight: "0.5"},
		{ID: "session.idle_timeout", Unit: "minute", Value: "10", Weight: "1.0"},
	}
	mixedRepairs := []itemSpec{
		{ID: "account.lockout_threshold", Unit: "second", Value: "900", Weight: "1.0"},
		{ID: "audit.retention_days", Unit: "day", Value: "90", Weight: "0.75"},
		{ID: "host.snapshot.filetime", Unit: "filetime", Value: "133485408000000000", Weight: "1.0"},
		{ID: "log.level.score", Unit: "points", Value: "3.141592", Weight: "1.25"},
		{ID: "password.max_age", Unit: "day", Value: "60", Weight: "0.5"},
		{ID: "session.idle_timeout", Unit: "minute", Value: "10", Weight: "1.0"},
	}
	if err := addPackage("packages/mixed-units", "mixed_units", mixedBaseline, mixedRepairs); err != nil {
		return nil, err
	}

	truncated := copyItems(referenceItems)
	truncatedRepairs := encodeRepairs(truncated[:2])
	truncatedRepairs = append(truncatedRepairs, []byte(`999:{`+"\n")...)
	files = append(files,
		fileSpec{Path: "packages/truncated-text/baseline.json", Data: mustBaseline("truncated_text", truncated)},
		fileSpec{Path: "packages/truncated-text/repairs.txt", Data: truncatedRepairs},
	)

	unknownRepairs := copyItems(referenceItems)
	setItem(unknownRepairs, "session.idle_timeout", itemSpec{ID: "session.idle_timeout", Unit: "fortnight", Value: "1", Weight: "1.0"})
	if err := addPackage("packages/unknown-unit", "unknown_unit", copyItems(referenceItems), unknownRepairs); err != nil {
		return nil, err
	}

	overflowRepairs := copyItems(referenceItems)
	setItem(overflowRepairs, "log.level.score", itemSpec{ID: "log.level.score", Unit: "score", Value: "9000000000000.000000", Weight: "1.25"})
	if err := addPackage("packages/overflow-multiply", "numeric_overflow", copyItems(referenceItems), overflowRepairs); err != nil {
		return nil, err
	}

	filetimeOverflowRepairs := copyItems(referenceItems)
	setItem(filetimeOverflowRepairs, "host.snapshot.filetime", itemSpec{ID: "host.snapshot.filetime", Unit: "filetime", Value: "9223372036854775808", Weight: "1.0"})
	if err := addPackage("packages/filetime-overflow", "numeric_overflow", copyItems(referenceItems), filetimeOverflowRepairs); err != nil {
		return nil, err
	}

	errorBoundRepairs := copyItems(referenceItems)
	setItem(errorBoundRepairs, "host.snapshot.filetime", itemSpec{ID: "host.snapshot.filetime", Unit: "filetime", Value: "133485408009999999", Weight: "1.0"})
	setItem(errorBoundRepairs, "log.level.score", itemSpec{ID: "log.level.score", Unit: "score", Value: "3.1415927", Weight: "1.25"})
	if err := addPackage("packages/error-bound", "error_bound", copyItems(referenceItems), errorBoundRepairs); err != nil {
		return nil, err
	}

	injectionDoc := map[string]any{
		"schema":         "baseline.workbench.injection/v1",
		"class":          "snapshot_version_gap",
		"package_path":   "packages/normal",
		"forced_version": 3,
		"expected_code":  "E_SNAPSHOT_VERSION",
		"sample_marker":  SampleMarker,
	}
	encoded, err := json.MarshalIndent(injectionDoc, "", "  ")
	if err != nil {
		return nil, err
	}
	encoded = append(encoded, '\n')
	files = append(files, fileSpec{Path: "injections/version-gap.json", Data: encoded})
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func mustBaseline(class string, items []itemSpec) []byte {
	doc := baselineDoc{
		Schema:       "baseline.workbench.snapshot/v1",
		PackageID:    class,
		SampleMarker: SampleMarker,
		Items:        items,
	}
	encoded, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		panic(err)
	}
	return append(encoded, '\n')
}

func encodeRepairs(items []itemSpec) []byte {
	var builder strings.Builder
	for _, item := range items {
		builder.Write(parse.RepairBlock(policy.RawRecord{
			ID:     item.ID,
			Unit:   item.Unit,
			Value:  item.Value,
			Weight: item.Weight,
		}))
	}
	return []byte(builder.String())
}

func copyItems(items []itemSpec) []itemSpec {
	out := make([]itemSpec, len(items))
	copy(out, items)
	return out
}

func setItem(items []itemSpec, id string, replacement itemSpec) {
	for i := range items {
		if items[i].ID == id {
			items[i] = replacement
			return
		}
	}
	panic("sample item not found: " + id)
}

func Digest(root string) (string, error) {
	entries := make([]string, 0)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "manifest.sha256" {
			return nil
		}
		entries = append(entries, rel)
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(entries)
	hash := sha256.New()
	for _, rel := range entries {
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			return "", err
		}
		hash.Write([]byte(rel))
		hash.Write([]byte{0})
		hash.Write(data)
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func LoadManifest(root string) (Manifest, error) {
	data, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}
