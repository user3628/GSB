package workbench_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"dns-cache-workbench/internal/sample"
	"dns-cache-workbench/internal/workbench"
)

func generateClean(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	if err := sample.Generate(root); err != nil {
		t.Fatalf("generate samples: %v", err)
	}
	return root, filepath.Join(root, "clean")
}

func testScanOptions(cleanDir string) workbench.ScanOptions {
	return workbench.ScanOptions{
		InputDir:        cleanDir,
		TargetParser:    "v2",
		MaxConcurrency:  3,
		MaxEvents:       10000,
		WindowTicks:     10,
		FlipWindowTicks: 3,
		NoCommit:        true,
	}
}

func hasCode(report workbench.Report, code string) bool {
	for _, finding := range report.Findings {
		if finding.Code == code {
			return true
		}
	}
	for _, source := range report.Sources {
		for _, issue := range source.Issues {
			if issue.Code == code {
				return true
			}
		}
	}
	return false
}

func businessCode(err error) string {
	var businessErr *workbench.BusinessError
	if errors.As(err, &businessErr) {
		return businessErr.Code
	}
	return ""
}

func TestCleanThreeSourceParseAndCorrelation(t *testing.T) {
	_, cleanDir := generateClean(t)
	report, err := workbench.Scan(context.Background(), testScanOptions(cleanDir))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(report.Sources) != 4 {
		t.Fatalf("sources = %d, want 4", len(report.Sources))
	}
	wantEvents := map[string]int{
		"binary-v1.dnsbin":   2,
		"binary-v2.dnsbin":   3,
		"json-summary.jsonl": 2,
		"text-blocks.dnslog": 3,
	}
	for _, source := range report.Sources {
		if source.Events != wantEvents[source.Path] {
			t.Errorf("%s events = %d, want %d", source.Path, source.Events, wantEvents[source.Path])
		}
		if source.Skipped != 0 {
			t.Errorf("%s skipped = %d, want 0", source.Path, source.Skipped)
		}
	}
	if len(report.Transactions) != 4 {
		t.Fatalf("transactions = %d, want 4", len(report.Transactions))
	}
	for _, code := range []string{
		workbench.CodeUnsolicited,
		workbench.CodeAdditionalInject,
		workbench.CodeTTLMismatch,
		workbench.CodeFlipFlop,
	} {
		if !hasCode(report, code) {
			t.Errorf("missing expected finding %s", code)
		}
	}
	if hasCode(report, workbench.CodeNameConflict) {
		t.Errorf("clean sample unexpectedly produced E_NAME_CONFLICT")
	}
	formats := report.Compatibility.FormatVersions
	wantFormats := []string{"binary:v1", "binary:v2", "json:v2", "text:v1"}
	if !reflect.DeepEqual(formats, wantFormats) {
		t.Errorf("format versions = %#v, want %#v", formats, wantFormats)
	}
}

func TestTargetParserSwitchChangesOnlyCompatibility(t *testing.T) {
	_, cleanDir := generateClean(t)
	v2Options := testScanOptions(cleanDir)
	v2Report, err := workbench.Scan(context.Background(), v2Options)
	if err != nil {
		t.Fatalf("v2 scan: %v", err)
	}
	v1Options := testScanOptions(cleanDir)
	v1Options.TargetParser = "v1"
	v1Report, err := workbench.Scan(context.Background(), v1Options)
	if err != nil {
		t.Fatalf("v1 scan: %v", err)
	}
	if !reflect.DeepEqual(v2Report.Sources, v1Report.Sources) {
		t.Error("parser switch changed source parse summaries")
	}
	if !reflect.DeepEqual(v2Report.Transactions, v1Report.Transactions) {
		t.Error("parser switch changed transaction pairing or verdicts")
	}
	if !reflect.DeepEqual(v2Report.Findings, v1Report.Findings) {
		t.Error("parser switch changed findings")
	}
	if !v2Report.Compatibility.Compatible || v2Report.Compatibility.Code != "" {
		t.Errorf("v2 compatibility = %+v, want compatible with no code", v2Report.Compatibility)
	}
	if v1Report.Compatibility.Compatible || v1Report.Compatibility.Code != workbench.CodeSnapshotVersion {
		t.Errorf("v1 compatibility = %+v, want incompatible E_SNAPSHOT_VERSION", v1Report.Compatibility)
	}
}

func TestDeterministicReportBytes(t *testing.T) {
	_, cleanDir := generateClean(t)
	var encoded [2][]byte
	for i := 0; i < 2; i++ {
		options := testScanOptions(cleanDir)
		options.NoCommit = false
		options.DBPath = filepath.Join(t.TempDir(), "snapshot.db")
		report, err := workbench.Scan(context.Background(), options)
		if err != nil {
			t.Fatalf("scan %d: %v", i, err)
		}
		encoded[i], err = workbench.PrettyReportBytes(report)
		if err != nil {
			t.Fatalf("encode report %d: %v", i, err)
		}
	}
	if !reflect.DeepEqual(encoded[0], encoded[1]) {
		t.Fatal("repeated deterministic runs produced different report bytes")
	}
}

func TestSnapshotCompatibilityAndReplacementDetection(t *testing.T) {
	_, cleanDir := generateClean(t)
	dbPath := filepath.Join(t.TempDir(), "snapshot.db")
	options := testScanOptions(cleanDir)
	options.NoCommit = false
	options.DBPath = dbPath
	report, err := workbench.Scan(context.Background(), options)
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if report.Snapshot == nil || report.Snapshot.Version != 1 {
		t.Fatalf("first snapshot = %+v, want version 1", report.Snapshot)
	}
	secondReport, err := workbench.Scan(context.Background(), options)
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	if secondReport.Snapshot == nil || secondReport.Snapshot.Version != 2 {
		t.Fatalf("second snapshot = %+v, want version 2", secondReport.Snapshot)
	}
	if len(secondReport.Snapshot.Chain) != 2 {
		t.Fatalf("chain length = %d, want 2", len(secondReport.Snapshot.Chain))
	}
	store, err := workbench.OpenStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := store.LoadSnapshot(1, "v2"); err != nil {
		t.Errorf("v1 snapshot should load with v2 parser: %v", err)
	}
	if _, err := store.LoadSnapshot(1, "v1"); businessCode(err) != workbench.CodeSnapshotVersion {
		t.Errorf("v1 parser load error = %v, want E_SNAPSHOT_VERSION", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw sqlite: %v", err)
	}
	if _, err := db.Exec(`UPDATE snapshots SET report_json = ? WHERE version = 1`, []byte(`{"schema_version":1}`)); err != nil {
		t.Fatalf("replace snapshot content: %v", err)
	}
	db.Close()

	store, err = workbench.OpenStore(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store.Close()
	if _, err := store.LoadSnapshot(1, "v2"); businessCode(err) != workbench.CodeSnapshotVersion {
		t.Errorf("wholesale replacement error = %v, want E_SNAPSHOT_VERSION", err)
	}
}

func TestSnapshotVersionJumpRollsBack(t *testing.T) {
	_, cleanDir := generateClean(t)
	report, err := workbench.Scan(context.Background(), testScanOptions(cleanDir))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), "snapshot.db")
	store, err := workbench.OpenStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	committed, err := store.CommitReport(report, 0)
	if err != nil {
		t.Fatalf("commit baseline: %v", err)
	}
	if _, err := store.CommitReport(report, 1); businessCode(err) != workbench.CodeSnapshotVersion {
		t.Errorf("version jump error = %v, want E_SNAPSHOT_VERSION", err)
	}
	list, err := store.ListSnapshots("v2")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 || list[0].Version != committed.Snapshot.Version {
		t.Errorf("snapshot jump polluted chain: %+v", list)
	}
}

func TestEventBudgetRetainsSuccessfulEvents(t *testing.T) {
	_, cleanDir := generateClean(t)
	options := testScanOptions(cleanDir)
	options.MaxEvents = 1
	report, err := workbench.Scan(context.Background(), options)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	for _, source := range report.Sources {
		if source.Events > 1 {
			t.Errorf("%s retained %d events with budget 1", source.Path, source.Events)
		}
		foundBudget := false
		for _, issue := range source.Issues {
			if issue.Code == workbench.CodeEventBudget {
				foundBudget = true
				if issue.Source != source.Path || issue.Offset <= 0 {
					t.Errorf("budget issue lacks source/offset traceability: %+v", issue)
				}
			}
		}
		if !foundBudget {
			t.Errorf("%s did not report E_EVENT_BUDGET", source.Path)
		}
	}
}

func TestSingleConcurrencySlotProcessesAllSources(t *testing.T) {
	_, cleanDir := generateClean(t)
	options := testScanOptions(cleanDir)
	options.MaxConcurrency = 1
	report, err := workbench.Scan(context.Background(), options)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(report.Sources) != 4 {
		t.Fatalf("sources = %d, want all 4 queued sources processed", len(report.Sources))
	}
	if len(report.Transactions) != 4 {
		t.Fatalf("transactions = %d, want 4", len(report.Transactions))
	}
}

func TestNameAndTxIDNormalization(t *testing.T) {
	name, wire, err := workbench.NormalizeName("WWW.Example.Test.")
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if name != "www.example.test" {
		t.Errorf("name = %q", name)
	}
	if got := workbench.WireNameKey(wire); got != "3:www|7:example|4:test|" {
		t.Errorf("wire key = %q", got)
	}
	for _, input := range []string{"4097", "0x1001", "0X1001"} {
		txid, err := workbench.ParseTxID(input)
		if err != nil || txid != 0x1001 {
			t.Errorf("ParseTxID(%q) = %#x, %v", input, txid, err)
		}
	}
	if _, err := workbench.ParseTxID("4294967296"); err == nil {
		t.Error("expected overflow transaction id to fail")
	}
}

func TestParserCompatibilityMatrix(t *testing.T) {
	cases := []struct {
		current  string
		required string
		want     bool
	}{
		{current: "v1", required: "v1", want: true},
		{current: "v1", required: "v2", want: false},
		{current: "v2", required: "v1", want: true},
		{current: "v2", required: "v2", want: true},
	}
	for _, test := range cases {
		if got := workbench.ParserCompatible(test.current, test.required); got != test.want {
			t.Errorf("ParserCompatible(%s, %s) = %t, want %t", test.current, test.required, got, test.want)
		}
	}
}
