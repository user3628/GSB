package source_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"svcworkbench/internal/domain"
	"svcworkbench/internal/samplegen"
	"svcworkbench/internal/source"
)

func TestSchedulerCancellationReleasesAllSessions(t *testing.T) {
	dir := t.TempDir()
	inputs, err := samplegen.Generate(dir, samplegen.Options{TargetFormat: "v2", Fault: samplegen.FaultNone})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scheduler := source.NewScheduler(3)
	results, metrics := scheduler.ParseAll(ctx, inputs, source.Options{ServiceBudget: 128})
	if len(results) != 3 || metrics.CompletedSources != 3 || scheduler.InFlight() != 0 {
		t.Fatalf("cancelled scheduler did not release sessions: %+v %+v", metrics, scheduler.InFlight())
	}
	for _, result := range results {
		if len(result.Issues) != 1 || result.Issues[0].Code != domain.CodeSourceMalformed {
			t.Fatalf("cancelled result = %+v", result)
		}
	}
}

func TestParseAllSourcesV2(t *testing.T) {
	results, metrics := generateAndParse(t, "v2", samplegen.FaultNone, 128, 3)
	if metrics.CompletedSources != 3 || metrics.ObservedMaxInFlight < 1 || metrics.ObservedMaxInFlight > 3 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
	for _, result := range results {
		if len(result.Issues) != 0 {
			t.Fatalf("%s issues: %#v", result.SourceID, result.Issues)
		}
		if result.RecordsAccepted != 7 || result.UsableDeclarations != 7 || result.Partial {
			t.Fatalf("%s summary: %+v", result.SourceID, result)
		}
	}
	if results[0].FormatVersion != "policy-v2" {
		t.Fatalf("binary format = %q", results[0].FormatVersion)
	}
	writable := findDeclaration(t, results, "writable agent")
	if !writable.Path.WritableDirectory {
		t.Fatalf("writable agent was not marked writable: %+v", writable.Path)
	}
	updater := findDeclaration(t, results, "vendor updater")
	if len(updater.Path.ExecutableCandidates) == 0 {
		t.Fatalf("expected unquoted executable candidates: %+v", updater.Path)
	}
}

func TestParseAllSourcesV1DefaultsAccount(t *testing.T) {
	results, _ := generateAndParse(t, "v1", samplegen.FaultNone, 128, 3)
	binary := results[0]
	if binary.FormatVersion != "policy-v1" {
		t.Fatalf("binary format = %q", binary.FormatVersion)
	}
	for _, declaration := range binary.Declarations {
		if declaration.AccountPresent {
			t.Fatalf("v1 account should be implicit: %+v", declaration)
		}
		if declaration.AccountKind != domain.AccountLocalSystem {
			t.Fatalf("v1 account default = %s", declaration.AccountKind)
		}
	}
}

func TestUnknownVersionHasStableCodeAndRange(t *testing.T) {
	results, _ := generateAndParse(t, "v2", samplegen.FaultUnknownVersion, 128, 3)
	binary := results[0]
	if binary.FormatVersion != "policy-v99" {
		t.Fatalf("format version = %q", binary.FormatVersion)
	}
	if len(binary.Declarations) != 0 || len(binary.Issues) != 1 {
		t.Fatalf("unexpected binary result: %+v", binary)
	}
	if binary.Issues[0].Code != domain.CodeFormatVersion {
		t.Fatalf("code = %s", binary.Issues[0].Code)
	}
	if len(binary.SkippableRanges) != 1 || binary.SkippableRanges[0].End == 0 {
		t.Fatalf("missing skippable range: %+v", binary.SkippableRanges)
	}
	if results[1].UsableDeclarations != 7 || results[2].UsableDeclarations != 7 {
		t.Fatalf("unrelated sources were polluted: %+v %+v", results[1], results[2])
	}
}

func TestInvalidMagicHasStableCode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.bin")
	if err := os.WriteFile(path, []byte("NOTMAGIC-and-more"), 0o644); err != nil {
		t.Fatal(err)
	}
	result := source.ParseFile(context.Background(), source.Input{ID: "bad", Kind: domain.SourceBinaryPolicy, Path: path}, source.Options{ServiceBudget: 10})
	if len(result.Issues) == 0 || result.Issues[0].Code != domain.CodeFormatVersion {
		t.Fatalf("issues = %#v", result.Issues)
	}
	if len(result.SkippableRanges) == 0 {
		t.Fatalf("missing skippable range")
	}
}

func TestTextPrefixOverflowKeepsParsedRecords(t *testing.T) {
	results, _ := generateAndParse(t, "v2", samplegen.FaultTextPrefixOOB, 128, 3)
	text := results[2]
	if !text.Partial || text.RecordsAccepted != 6 || text.UsableDeclarations != 6 {
		t.Fatalf("unexpected text result: %+v", text)
	}
	if !hasCode(text.Issues, domain.CodeFormatVersion) {
		t.Fatalf("missing format code: %#v", text.Issues)
	}
	if len(text.SkippableRanges) == 0 {
		t.Fatalf("missing skippable range")
	}
}

func TestMalformedQuoteIsolatedToOneDeclaration(t *testing.T) {
	results, _ := generateAndParse(t, "v2", samplegen.FaultUnterminatedQuote, 128, 3)
	jsonResult := results[1]
	if !hasCode(jsonResult.Issues, domain.CodePathMalformed) {
		t.Fatalf("missing path code: %#v", jsonResult.Issues)
	}
	if len(jsonResult.Declarations) != 7 {
		t.Fatalf("all declarations should remain traceable: %d", len(jsonResult.Declarations))
	}
	for _, declaration := range jsonResult.Declarations {
		if declaration.NormalizedServiceName == "quoted tool service" {
			if declaration.Usable || !declaration.Path.Malformed {
				t.Fatalf("malformed declaration should be unusable: %+v", declaration)
			}
		} else if !declaration.Usable {
			t.Fatalf("unrelated declaration was polluted: %+v", declaration)
		}
	}
	if results[0].UsableDeclarations != 7 || results[2].UsableDeclarations != 7 {
		t.Fatalf("unrelated source was polluted")
	}
}

func TestUnknownAccountIsIsolated(t *testing.T) {
	results, _ := generateAndParse(t, "v2", samplegen.FaultUnknownAccount, 128, 3)
	for _, index := range []int{0, 1} {
		if !hasCode(results[index].Issues, domain.CodeAccountUnknown) {
			t.Fatalf("%s missing account code: %#v", results[index].SourceID, results[index].Issues)
		}
		for _, declaration := range results[index].Declarations {
			if declaration.NormalizedServiceName == "custom worker" {
				if declaration.Usable {
					t.Fatalf("unknown account declaration should be unusable")
				}
			} else if !declaration.Usable {
				t.Fatalf("unrelated declaration was polluted: %+v", declaration)
			}
		}
	}
	if results[2].UsableDeclarations != 7 {
		t.Fatalf("text source should remain usable")
	}
}

func TestServiceBudgetStopsEachSourceWithPartialData(t *testing.T) {
	results, _ := generateAndParse(t, "v2", samplegen.FaultNone, 2, 3)
	for _, result := range results {
		if !result.Partial || result.RecordsAccepted != 2 || result.RecordsSeen != 3 {
			t.Fatalf("%s summary: %+v", result.SourceID, result)
		}
		if !hasCode(result.Issues, domain.CodeServiceBudget) {
			t.Fatalf("%s missing budget code: %#v", result.SourceID, result.Issues)
		}
	}
}

func TestSchedulerQueuesAboveConcurrencyLimit(t *testing.T) {
	results, metrics := generateAndParse(t, "v2", samplegen.FaultNone, 128, 1)
	if len(results) != 3 || metrics.MaxConcurrency != 1 || metrics.QueuedSources != 2 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
	if metrics.ObservedMaxInFlight != 1 || metrics.CompletedSources != 3 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
}

func generateAndParse(t *testing.T, target, fault string, budget, concurrency int) ([]domain.SourceResult, source.BatchMetrics) {
	t.Helper()
	dir := t.TempDir()
	inputs, err := samplegen.Generate(dir, samplegen.Options{TargetFormat: target, Fault: fault})
	if err != nil {
		t.Fatal(err)
	}
	scheduler := source.NewScheduler(concurrency)
	return scheduler.ParseAll(context.Background(), inputs, source.Options{ServiceBudget: budget})
}

func findDeclaration(t *testing.T, results []domain.SourceResult, service string) domain.Declaration {
	t.Helper()
	for _, result := range results {
		for _, declaration := range result.Declarations {
			if declaration.NormalizedServiceName == service {
				return declaration
			}
		}
	}
	t.Fatalf("declaration %s not found", service)
	return domain.Declaration{}
}

func hasCode(issues []domain.Issue, code domain.Code) bool {
	for _, issue := range issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}
