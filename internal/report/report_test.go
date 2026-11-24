package report_test

import (
	"bytes"
	"testing"

	"supplyaudit/internal/formats"
	"supplyaudit/internal/pipeline"
	"supplyaudit/internal/report"
	"supplyaudit/internal/sample"
)

func TestAuditDataIsByteDeterministicAcrossWorkDirs(t *testing.T) {
	first := auditData(t.TempDir())
	second := auditData(t.TempDir())
	firstBytes, err := report.MarshalAuditData(first)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := report.MarshalAuditData(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatalf("audit data differs across clean work directories: %d vs %d bytes", len(firstBytes), len(secondBytes))
	}
	if !first.Counts.SelfConsistent || first.Counts.SourceDeclarations != first.Counts.MergedDeclarations {
		t.Fatalf("report counts are inconsistent: %#v", first.Counts)
	}
	if len(first.Sources) != 4 || len(first.Components) == 0 {
		t.Fatalf("unexpected report shape: %#v", first)
	}
}

func auditData(workDir string) report.AuditData {
	layout, err := sample.Generate(workDir, true)
	if err != nil {
		panic(err)
	}
	sources, err := formats.Discover(layout.InputDir)
	if err != nil {
		panic(err)
	}
	result, err := pipeline.Run(sources, pipeline.DefaultConfig())
	if err != nil {
		panic(err)
	}
	return report.BuildAuditData(result)
}
