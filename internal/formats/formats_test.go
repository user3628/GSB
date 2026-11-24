package formats_test

import (
	"os"
	"path/filepath"
	"testing"

	"supplyaudit/internal/formats"
	"supplyaudit/internal/model"
	"supplyaudit/internal/sample"
)

func TestParseAllSampleFormats(t *testing.T) {
	workDir := t.TempDir()
	layout, err := sample.Generate(workDir, true)
	if err != nil {
		t.Fatal(err)
	}
	specs, err := formats.Discover(layout.InputDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 4 {
		t.Fatalf("expected 4 sources, got %d", len(specs))
	}
	counts := map[model.SourceFormat]int{}
	versions := map[model.SourceFormat]string{}
	for _, spec := range specs {
		result := formats.ParseSource(spec, formats.ParseOptions{BinaryFormatVersion: "v2"})
		if len(result.Issues) != 0 {
			t.Fatalf("%s unexpectedly had issues: %#v", spec.ReportPath, result.Issues)
		}
		counts[spec.Format] += len(result.Declarations)
		versions[spec.Format] = result.FormatVersion
		for _, declaration := range result.Declarations {
			if declaration.ComponentKey == "" || declaration.Version.Canonical == "" || declaration.Digest.Hex == "" {
				t.Fatalf("incomplete normalization in %s: %#v", spec.ReportPath, declaration)
			}
		}
	}
	if counts[model.FormatBinary] != 7 || counts[model.FormatJSONL] != 5 || counts[model.FormatTextBlock] != 4 {
		t.Fatalf("unexpected counts: %#v", counts)
	}
	if versions[model.FormatBinary] != "2" || versions[model.FormatJSONL] != "1" || versions[model.FormatTextBlock] != "1" {
		t.Fatalf("unexpected format versions: %#v", versions)
	}
}

func TestBinaryVersionAndTruncationCodes(t *testing.T) {
	workDir := t.TempDir()
	_, err := sample.Generate(workDir, true)
	if err != nil {
		t.Fatal(err)
	}
	unknownPath := filepath.Join(workDir, "faults", "unknown-version-header", "input", "manifest.bin")
	unknown := formats.ParseSource(model.SourceSpec{ID: "unknown", Format: model.FormatBinary, Path: unknownPath, ReportPath: "manifest.bin"}, formats.ParseOptions{BinaryFormatVersion: "v2"})
	if !unknown.HasCode(model.CodeFormatVersion) {
		t.Fatalf("expected E_FORMAT_VERSION, got %#v", unknown.Issues)
	}

	truncatedPath := filepath.Join(workDir, "faults", "text-length-overrun", "input", "legacy.txtblock")
	truncated := formats.ParseSource(model.SourceSpec{ID: "truncated", Format: model.FormatTextBlock, Path: truncatedPath, ReportPath: "legacy.txtblock"}, formats.ParseOptions{BinaryFormatVersion: "v2"})
	if !truncated.HasCode(model.CodeTruncated) {
		t.Fatalf("expected E_TRUNCATED, got %#v", truncated.Issues)
	}
	for _, issue := range truncated.Issues {
		if issue.Code == model.CodeTruncated && issue.BytesRead == 0 {
			t.Fatalf("truncation did not report bytes_read: %#v", issue)
		}
	}
	if len(truncated.Declarations) != 1 || truncated.Declarations[0].ComponentKey != "@fault/control" {
		t.Fatalf("valid record before truncation was not retained: %#v", truncated.Declarations)
	}
}

func TestBinaryV1RejectedByV1TargetOnlyWhenNewer(t *testing.T) {
	workDir := t.TempDir()
	layout, err := sample.Generate(workDir, true)
	if err != nil {
		t.Fatal(err)
	}
	v2Path := filepath.Join(layout.InputDir, sample.BinaryV2File)
	result := formats.ParseSource(model.SourceSpec{ID: "v2", Format: model.FormatBinary, Path: v2Path, ReportPath: sample.BinaryV2File}, formats.ParseOptions{BinaryFormatVersion: "v1"})
	if !result.HasCode(model.CodeFormatVersion) {
		t.Fatalf("v1 target should reject v2 input: %#v", result.Issues)
	}
	v1Path := filepath.Join(layout.InputDir, sample.BinaryV1File)
	result = formats.ParseSource(model.SourceSpec{ID: "v1", Format: model.FormatBinary, Path: v1Path, ReportPath: sample.BinaryV1File}, formats.ParseOptions{BinaryFormatVersion: "v1"})
	if len(result.Declarations) != 2 || result.FormatVersion != "1" {
		t.Fatalf("v1 target should load v1 input: %#v", result)
	}
}

func TestDigestErrorDoesNotSuppressFollowingJSONLine(t *testing.T) {
	workDir := t.TempDir()
	_, err := sample.Generate(workDir, true)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(workDir, "faults", "digest-algorithm-mismatch", "input", sample.JSONLFile)
	result := formats.ParseSource(model.SourceSpec{ID: "json", Format: model.FormatJSONL, Path: path, ReportPath: sample.JSONLFile}, formats.ParseOptions{BinaryFormatVersion: "v2"})
	if !result.HasCode(model.CodeDigestInvalid) {
		t.Fatalf("expected E_DIGEST_INVALID, got %#v", result.Issues)
	}
	if len(result.Declarations) != 1 || result.Declarations[0].ComponentKey != "@fault/control" {
		t.Fatalf("following valid record was not retained: %#v", result.Declarations)
	}
}

func TestUnknownMagic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.bin")
	if err := os.WriteFile(path, []byte("NOTMAGIC"), 0o644); err != nil {
		t.Fatal(err)
	}
	result := formats.ParseSource(model.SourceSpec{ID: "bad", Format: model.FormatBinary, Path: path, ReportPath: "bad.bin"}, formats.ParseOptions{BinaryFormatVersion: "v2"})
	if !result.HasCode(model.CodeFormatVersion) {
		t.Fatalf("expected E_FORMAT_VERSION, got %#v", result.Issues)
	}
}
