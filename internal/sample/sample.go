package sample

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	BinaryV2File      = "binary-v2.bin"
	BinaryV1File      = "binary-v1.bin"
	JSONLFile         = "packages.jsonl"
	TextFile          = "legacy.txtblock"
	FaultManifestFile = "faults/manifest.json"
)

type Layout struct {
	WorkDir       string   `json:"workdir"`
	InputDir      string   `json:"input_dir"`
	FaultsDir     string   `json:"faults_dir"`
	Sources       []string `json:"sources"`
	FaultManifest string   `json:"fault_manifest"`
}

type BinaryRecord struct {
	Name      string
	Version   string
	Digest    string
	Origin    string
	Signature string
}

type JSONRecord struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	Digest    string `json:"digest"`
	Source    string `json:"source"`
	Signature string `json:"signature,omitempty"`
}

type TextRecord struct {
	Name    string
	Version string
	Digest  string
	Origin  string
}

type FaultScenario struct {
	ID               string `json:"id"`
	ExpectedCode     string `json:"expected_code"`
	Kind             string `json:"kind"`
	Input            string `json:"input,omitempty"`
	TargetComponent  string `json:"target_component,omitempty"`
	ControlComponent string `json:"control_component"`
}

type FaultManifest struct {
	SchemaVersion int             `json:"schema_version"`
	Scenarios     []FaultScenario `json:"scenarios"`
}

// Generate writes deterministic normal and malformed sample inputs. When clean
// is true it removes only the input/ and faults/ directories owned by this
// generator.
func Generate(workDir string, clean bool) (Layout, error) {
	if strings.TrimSpace(workDir) == "" {
		return Layout{}, fmt.Errorf("workdir is empty")
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return Layout{}, err
	}
	inputDir := filepath.Join(workDir, "input")
	faultsDir := filepath.Join(workDir, "faults")
	if clean {
		if err := os.RemoveAll(inputDir); err != nil {
			return Layout{}, err
		}
		if err := os.RemoveAll(faultsDir); err != nil {
			return Layout{}, err
		}
	}
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		return Layout{}, err
	}
	if err := os.MkdirAll(faultsDir, 0o755); err != nil {
		return Layout{}, err
	}

	sharedDigest := sha256Digest("shared@2.1.0")
	sharedJSONDigest := sharedDigest
	binaryV2 := []BinaryRecord{
		{Name: "@Demo/Shared-Lib", Version: "2.1.0+build.7", Digest: sharedDigest, Origin: "repo://binary/registry", Signature: "sig:" + sha256Hex("shared-signature")},
		{Name: "@Demo/Conflict-Lib", Version: "3.0.0", Digest: sha256Digest("conflict-binary@3.0.0"), Origin: "repo://binary/registry", Signature: "sig:" + sha256Hex("conflict-signature")},
		{Name: "@Demo/Build-Meta", Version: "4.2.0+build.7", Digest: sha256Digest("build-meta@4.2.0"), Origin: "repo://binary/registry"},
		{Name: "@Demo/Pre-Release", Version: "2.0.0", Digest: sha256Digest("pre-release@2.0.0"), Origin: "repo://binary/registry"},
		{Name: "@DEMO/Namespace/Deep", Version: "4.0.0", Digest: sha256Digest("namespace-deep@4.0.0"), Origin: "repo://binary/registry"},
	}
	binaryV1 := []BinaryRecord{
		{Name: "@DEMO/SHARED-LIB", Version: "2.1.0+build.6", Digest: sharedJSONDigest},
		{Name: "@Demo/V1-Only", Version: "1.5.0", Digest: sha1Digest("v1-only@1.5.0")},
	}
	jsonRecords := []JSONRecord{
		{Name: "@demo/shared-lib", Version: "2.1.0.0", Digest: sharedJSONDigest, Source: "json-export"},
		{Name: "@demo/conflict-lib", Version: "3.0.0", Digest: sha256Digest("conflict-json@3.0.0"), Source: "json-export"},
		{Name: "@demo/build-meta", Version: "4.2.0+build.9", Digest: sha256Digest("build-meta@4.2.0"), Source: "json-export"},
		{Name: "@demo/pre-release", Version: "2.0.0-rc.2", Digest: sha256Digest("pre-release@rc.2"), Source: "json-export"},
		{Name: "@demo/json-only", Version: "0.9.0", Digest: sha256Digest("json-only@0.9.0"), Source: "json-export"},
	}
	textRecords := []TextRecord{
		{Name: "@DEMO/SHARED-LIB", Version: "1.9.0", Digest: sha1Digest("shared@1.9.0"), Origin: "legacy-registry"},
		{Name: "@demo/conflict-lib", Version: "3.0.0", Digest: sha256Digest("conflict-text@3.0.0"), Origin: "legacy-registry"},
		{Name: "@demo/pre-release", Version: "1.0.0", Digest: sha1Digest("pre-release@1.0.0"), Origin: "legacy-registry"},
		{Name: "@demo/text-only", Version: "0.5.0", Digest: sha1Digest("text-only@0.5.0"), Origin: "legacy-registry"},
	}

	paths := []string{
		filepath.Join(inputDir, BinaryV2File),
		filepath.Join(inputDir, BinaryV1File),
		filepath.Join(inputDir, JSONLFile),
		filepath.Join(inputDir, TextFile),
	}
	files := []struct {
		path string
		data []byte
	}{
		{paths[0], encodeBinary(2, binaryV2, "")},
		{paths[1], encodeBinary(1, binaryV1, "collector-v1")},
		{paths[2], encodeJSONL(jsonRecords)},
		{paths[3], encodeText(textRecords)},
	}
	for _, file := range files {
		if err := writeDeterministic(file.path, file.data); err != nil {
			return Layout{}, err
		}
	}

	scenarios, err := generateFaults(faultsDir)
	if err != nil {
		return Layout{}, err
	}
	manifestBytes, err := json.MarshalIndent(FaultManifest{SchemaVersion: 1, Scenarios: scenarios}, "", "  ")
	if err != nil {
		return Layout{}, err
	}
	manifestBytes = append(manifestBytes, '\n')
	manifestPath := filepath.Join(workDir, filepath.FromSlash(FaultManifestFile))
	if err := writeDeterministic(manifestPath, manifestBytes); err != nil {
		return Layout{}, err
	}

	return Layout{
		WorkDir:       workDir,
		InputDir:      inputDir,
		FaultsDir:     faultsDir,
		Sources:       paths,
		FaultManifest: manifestPath,
	}, nil
}

func LoadFaultManifest(path string) (FaultManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return FaultManifest{}, err
	}
	var manifest FaultManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return FaultManifest{}, err
	}
	return manifest, nil
}

func generateFaults(faultsDir string) ([]FaultScenario, error) {
	scenarios := []FaultScenario{
		{ID: "digest-algorithm-mismatch", ExpectedCode: "E_DIGEST_INVALID", Kind: "sources", Input: "faults/digest-algorithm-mismatch/input", TargetComponent: "@fault/digest-algo", ControlComponent: "@fault/control"},
		{ID: "digest-conflict", ExpectedCode: "E_DIGEST_CONFLICT", Kind: "sources", Input: "faults/digest-conflict/input", TargetComponent: "@fault/conflict", ControlComponent: "@fault/control"},
		{ID: "snapshot-version-gap", ExpectedCode: "E_SNAPSHOT_VERSION", Kind: "snapshot_fixture", Input: "faults/snapshot-version-gap/fixture.json", TargetComponent: "snapshot:3", ControlComponent: "snapshot:1"},
		{ID: "text-length-overrun", ExpectedCode: "E_TRUNCATED", Kind: "sources", Input: "faults/text-length-overrun/input", TargetComponent: "@fault/truncated-tail", ControlComponent: "@fault/control"},
		{ID: "unknown-version-header", ExpectedCode: "E_FORMAT_VERSION", Kind: "sources", Input: "faults/unknown-version-header/input", TargetComponent: "@fault/unknown-version", ControlComponent: "@fault/control"},
		{ID: "version-downgrade", ExpectedCode: "E_VERSION_DOWNGRADE", Kind: "sources", Input: "faults/version-downgrade/input", TargetComponent: "@fault/downgrade", ControlComponent: "@fault/control"},
	}

	for _, scenario := range scenarios {
		base := filepath.Join(faultsDir, scenario.ID)
		if err := os.MkdirAll(filepath.Join(base, "input"), 0o755); err != nil {
			return nil, err
		}
		switch scenario.ID {
		case "unknown-version-header":
			if err := writeDeterministic(filepath.Join(base, "input", "manifest.bin"), encodeBinary(9, nil, "")); err != nil {
				return nil, err
			}
			if err := writeDeterministic(filepath.Join(base, "input", JSONLFile), encodeJSONL([]JSONRecord{{Name: scenario.ControlComponent, Version: "1.0.0", Digest: sha256Digest("fault-control"), Source: "json-export"}})); err != nil {
				return nil, err
			}
		case "text-length-overrun":
			valid := encodeText([]TextRecord{{Name: scenario.ControlComponent, Version: "1.0.0", Digest: sha256Digest("fault-control"), Origin: "legacy-registry"}})
			truncated := append(append([]byte(nil), valid...), 0, 0, 0x10, 0x00)
			if err := writeDeterministic(filepath.Join(base, "input", TextFile), truncated); err != nil {
				return nil, err
			}
		case "digest-algorithm-mismatch":
			badDigest := "sha1:" + base64.StdEncoding.EncodeToString([]byte("this-is-a-32-byte-digest-not-sha1!!"))
			records := []JSONRecord{
				{Name: scenario.TargetComponent, Version: "1.0.0", Digest: badDigest, Source: "json-export"},
				{Name: scenario.ControlComponent, Version: "1.0.0", Digest: sha256Digest("fault-control"), Source: "json-export"},
			}
			if err := writeDeterministic(filepath.Join(base, "input", JSONLFile), encodeJSONL(records)); err != nil {
				return nil, err
			}
		case "digest-conflict":
			binaryRecords := []BinaryRecord{
				{Name: scenario.TargetComponent, Version: "1.0.0", Digest: sha256Digest("conflict-a"), Origin: "repo://binary"},
				{Name: scenario.ControlComponent, Version: "1.0.0", Digest: sha256Digest("fault-control"), Origin: "repo://binary"},
			}
			jsonRecords := []JSONRecord{
				{Name: scenario.TargetComponent, Version: "1.0.0", Digest: sha256Digest("conflict-b"), Source: "json-export"},
				{Name: scenario.ControlComponent, Version: "1.0.0", Digest: sha256Digest("fault-control"), Source: "json-export"},
			}
			if err := writeDeterministic(filepath.Join(base, "input", BinaryV2File), encodeBinary(2, binaryRecords, "")); err != nil {
				return nil, err
			}
			if err := writeDeterministic(filepath.Join(base, "input", JSONLFile), encodeJSONL(jsonRecords)); err != nil {
				return nil, err
			}
		case "version-downgrade":
			binaryRecords := []BinaryRecord{
				{Name: scenario.TargetComponent, Version: "5.0.0", Digest: sha256Digest("downgrade-5"), Origin: "repo://binary"},
				{Name: scenario.ControlComponent, Version: "1.0.0", Digest: sha256Digest("fault-control"), Origin: "repo://binary"},
			}
			jsonRecords := []JSONRecord{
				{Name: scenario.TargetComponent, Version: "4.0.0", Digest: sha256Digest("downgrade-4"), Source: "json-export"},
				{Name: scenario.ControlComponent, Version: "1.0.0", Digest: sha256Digest("fault-control"), Source: "json-export"},
			}
			if err := writeDeterministic(filepath.Join(base, "input", BinaryV2File), encodeBinary(2, binaryRecords, "")); err != nil {
				return nil, err
			}
			if err := writeDeterministic(filepath.Join(base, "input", JSONLFile), encodeJSONL(jsonRecords)); err != nil {
				return nil, err
			}
		case "snapshot-version-gap":
			fixture := []byte("{\n  \"base_version\": 1,\n  \"missing_version\": 2,\n  \"target_version\": 3\n}\n")
			if err := writeDeterministic(filepath.Join(base, "fixture.json"), fixture); err != nil {
				return nil, err
			}
		}
	}
	sort.Slice(scenarios, func(i, j int) bool { return scenarios[i].ID < scenarios[j].ID })
	return scenarios, nil
}

func encodeBinary(version int, records []BinaryRecord, globalOrigin string) []byte {
	buffer := make([]byte, 0, 4096)
	buffer = append(buffer, []byte("SADBIN01")...)
	buffer = binary.LittleEndian.AppendUint16(buffer, uint16(version))
	buffer = binary.LittleEndian.AppendUint32(buffer, uint32(len(records)))
	if version == 1 {
		buffer = appendLength16(buffer, []byte(globalOrigin))
	}
	for _, record := range records {
		if version == 2 {
			buffer = binary.LittleEndian.AppendUint16(buffer, uint16(len(record.Name)))
			buffer = binary.LittleEndian.AppendUint16(buffer, uint16(len(record.Version)))
			buffer = binary.LittleEndian.AppendUint16(buffer, uint16(len(record.Digest)))
			buffer = binary.LittleEndian.AppendUint16(buffer, uint16(len(record.Origin)))
			buffer = binary.LittleEndian.AppendUint16(buffer, uint16(len(record.Signature)))
			buffer = append(buffer, []byte(record.Name)...)
			buffer = append(buffer, []byte(record.Version)...)
			buffer = append(buffer, []byte(record.Digest)...)
			buffer = append(buffer, []byte(record.Origin)...)
			buffer = append(buffer, []byte(record.Signature)...)
		} else {
			buffer = binary.LittleEndian.AppendUint16(buffer, uint16(len(record.Name)))
			buffer = binary.LittleEndian.AppendUint16(buffer, uint16(len(record.Version)))
			buffer = binary.LittleEndian.AppendUint16(buffer, uint16(len(record.Digest)))
			buffer = append(buffer, []byte(record.Name)...)
			buffer = append(buffer, []byte(record.Version)...)
			buffer = append(buffer, []byte(record.Digest)...)
		}
	}
	return buffer
}

func encodeJSONL(records []JSONRecord) []byte {
	buffer := make([]byte, 0, 1024)
	for _, record := range records {
		data, _ := json.Marshal(record)
		buffer = append(buffer, data...)
		buffer = append(buffer, '\n')
	}
	return buffer
}

func encodeText(records []TextRecord) []byte {
	buffer := make([]byte, 0, 1024)
	buffer = append(buffer, []byte("SADTXT01")...)
	for _, record := range records {
		payload := []byte(record.Name + "|" + record.Version + "|" + record.Digest + "|" + record.Origin)
		buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(payload)))
		buffer = append(buffer, payload...)
	}
	return buffer
}

func appendLength16(buffer []byte, value []byte) []byte {
	buffer = binary.LittleEndian.AppendUint16(buffer, uint16(len(value)))
	return append(buffer, value...)
}

func sha256Digest(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return "sha256:" + base64.StdEncoding.EncodeToString(sum[:])
}

func sha256Hex(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])
}

func sha1Digest(seed string) string {
	sum := sha1.Sum([]byte(seed))
	return "sha1:" + hex.EncodeToString(sum[:])
}

func writeDeterministic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temp := path + ".tmp"
	if err := os.WriteFile(temp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(temp, path)
}
