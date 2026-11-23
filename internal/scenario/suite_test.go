package scenario

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	mfaengine "mfaengine"
)

func TestSuiteReportIsDeterministicAndPassing(t *testing.T) {
	ctx := context.Background()
	run := func(dir string) []byte {
		report, err := Run(ctx, Options{WorkDir: dir})
		if err != nil {
			t.Fatalf("run suite: %v", err)
		}
		if report.Summary.Failed != 0 || report.Summary.Passed != report.Summary.Total {
			t.Fatalf("scenario failures: %+v", report.Summary)
		}
		for _, fault := range report.Faults {
			if !fault.Passed {
				t.Fatalf("fault failed: %+v", fault)
			}
		}
		if report.Snapshot.Code != mfaengine.CodeOK {
			t.Fatalf("snapshot chain is not healthy: %+v", report.Snapshot)
		}
		file, err := os.CreateTemp(dir, "report-*.json")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if err := mfaengine.WriteReport(file, report); err != nil {
			t.Fatal(err)
		}
		content, err := os.ReadFile(file.Name())
		if err != nil {
			t.Fatal(err)
		}
		return content
	}

	firstDir := t.TempDir()
	secondDir := t.TempDir()
	first := run(firstDir)
	second := run(secondDir)
	if !bytes.Equal(first, second) {
		t.Fatal("diagnostic reports are not byte-identical")
	}
	var report mfaengine.DiagnosticReport
	if err := json.Unmarshal(first, &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Scenarios) < 13 || len(report.Faults) != 2 {
		t.Fatalf("unexpected report coverage: scenarios=%d faults=%d", len(report.Scenarios), len(report.Faults))
	}
	if _, err := os.Stat(filepath.Join(firstDir, "core.db")); err != nil {
		t.Fatalf("SQLite database was not persisted: %v", err)
	}
}
