package parser

import (
	"os"
	"path/filepath"
	"testing"

	"configaudit/internal/fixedpoint"
	"configaudit/internal/sample"
)

func TestLoadGeneratedDataset(t *testing.T) {
	dir := t.TempDir()
	layout, err := sample.Generate(filepath.Join(dir, "samples"), sample.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := Load(layout.SpecFile, layout.NodeFile, layout.Reference, layout.Sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds.Specs) == 0 || len(ds.Nodes) == 0 || len(ds.References) == 0 {
		t.Fatalf("empty dataset: %+v", layout)
	}
}

func TestSpecInvalidReportsFieldAndLine(t *testing.T) {
	dir := t.TempDir()
	specPath := filepath.Join(dir, "specs.jsonl")
	// line 2 has a bad scale
	content := `{"key_path":"a.b","type":"decimal","scale":2,"chain":[],"priority":1,"declared_bound":"1"}` + "\n" +
		`{"key_path":"bad key","type":"integer","scale":0,"chain":[],"priority":1,"declared_bound":"1"}` + "\n"
	if err := os.WriteFile(specPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	nodePath := filepath.Join(dir, "nodes.jsonl")
	if err := os.WriteFile(nodePath, []byte(`{"id":"n1","channels":[]}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	refPath := filepath.Join(dir, "refs.jsonl")
	if err := os.WriteFile(refPath, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Load(specPath, nodePath, refPath, nil)
	ce, ok := err.(*fixedpoint.Error)
	if !ok || ce.Code != fixedpoint.ESpecInvalid {
		t.Fatalf("err = %v", err)
	}
	if ce.Line != 1 || ce.Field != "scale" {
		t.Fatalf("line=%d field=%s", ce.Line, ce.Field)
	}
}

func TestNonPositiveChainFactorRejected(t *testing.T) {
	dir := t.TempDir()
	specPath := filepath.Join(dir, "specs.jsonl")
	content := `{"key_path":"a.b","type":"integer","scale":0,"chain":[{"op":"mul","factor":0}],"priority":1,"declared_bound":"1"}` + "\n"
	_ = os.WriteFile(specPath, []byte(content), 0o644)
	nodePath := filepath.Join(dir, "nodes.jsonl")
	_ = os.WriteFile(nodePath, []byte(`{"id":"n1","channels":[]}`+"\n"), 0o644)
	refPath := filepath.Join(dir, "refs.jsonl")
	_ = os.WriteFile(refPath, []byte(""), 0o644)
	_, err := Load(specPath, nodePath, refPath, nil)
	if fixedpoint.CodeOf(err) != fixedpoint.ESpecInvalid {
		t.Fatalf("code = %v", fixedpoint.CodeOf(err))
	}
}
