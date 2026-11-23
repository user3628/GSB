package merge_test

import (
	"context"
	"reflect"
	"testing"

	"svcworkbench/internal/domain"
	"svcworkbench/internal/merge"
	"svcworkbench/internal/samplegen"
	"svcworkbench/internal/source"
)

func TestAdjudicateNormalSamples(t *testing.T) {
	results := parsed(t, "v2", samplegen.FaultNone)
	adjudication := merge.Adjudicate(results, merge.Options{Priorities: domain.DefaultSourcePriorities})
	if len(adjudication.Services) != 7 {
		t.Fatalf("services = %d", len(adjudication.Services))
	}
	for _, service := range adjudication.Services {
		if len(service.Merge.Conflicts) != 0 {
			t.Fatalf("%s unexpectedly conflicts: %+v", service.ServiceName, service.Merge.Conflicts)
		}
		if service.Merge.Authority.SourceID != "binary-policy" {
			t.Fatalf("%s authority = %s", service.ServiceName, service.Merge.Authority.SourceID)
		}
	}
	writable := findService(t, adjudication, "writable agent")
	if writable.Privilege.AccountKind != domain.AccountLocalSystem || !writable.Privilege.HighPrivilege {
		t.Fatalf("unexpected writable profile: %+v", writable.Privilege)
	}
	if !contains(writable.RiskFlags, "PRIVILEGE_EXPOSURE") || !hasCode(writable.Issues, domain.CodePrivilegeExposure) {
		t.Fatalf("missing privilege exposure: %+v", writable)
	}
	updater := findService(t, adjudication, "vendor updater")
	if !contains(updater.RiskFlags, "UNQUOTED_IMAGE_PATH") {
		t.Fatalf("missing unquoted risk: %+v", updater.RiskFlags)
	}
	if updater.ImagePath.Normalized != `c:\program files\vendor\updater.exe` {
		t.Fatalf("normalized image path = %q", updater.ImagePath.Normalized)
	}
}

func TestAdjudicateConfigurationChangesAuthority(t *testing.T) {
	results := parsed(t, "v2", samplegen.FaultNone)
	adjudication := merge.Adjudicate(results, merge.Options{Priorities: []domain.SourceKind{
		domain.SourceJSONManifest,
		domain.SourceLegacyText,
		domain.SourceBinaryPolicy,
	}})
	spooler := findService(t, adjudication, "spooler")
	if spooler.Merge.Authority.SourceID != "json-manifest" {
		t.Fatalf("authority = %s", spooler.Merge.Authority.SourceID)
	}
}

func TestDeclarationConflictCarriesBothOffsetsAndDoesNotPolluteOthers(t *testing.T) {
	results := parsed(t, "v2", samplegen.FaultDeclarationConflict)
	adjudication := merge.Adjudicate(results, merge.Options{Priorities: domain.DefaultSourcePriorities})
	updater := findService(t, adjudication, "vendor updater")
	if !hasCode(updater.Issues, domain.CodeDeclarationConflict) {
		t.Fatalf("missing conflict code: %+v", updater.Issues)
	}
	if len(updater.Merge.Conflicts) != 1 {
		t.Fatalf("conflicts = %+v", updater.Merge.Conflicts)
	}
	conflict := updater.Merge.Conflicts[0]
	if conflict.Field != "start_type" {
		t.Fatalf("conflict field = %s", conflict.Field)
	}
	if conflict.Authority.Source.SourceID != "binary-policy" || conflict.Challenger.Source.SourceID != "legacy-text" {
		t.Fatalf("conflict sides = %+v", conflict)
	}
	if conflict.Authority.Source.Offset < 0 || conflict.Challenger.Source.Offset <= 0 {
		t.Fatalf("missing offsets: %+v", conflict)
	}
	for _, service := range adjudication.Services {
		if service.ServiceName != "vendor updater" && hasCode(service.Issues, domain.CodeDeclarationConflict) {
			t.Fatalf("conflict leaked into %s", service.ServiceName)
		}
	}
}

func TestAdjudicationIsStable(t *testing.T) {
	results := parsed(t, "v2", samplegen.FaultDeclarationConflict)
	first := merge.Adjudicate(results, merge.Options{Priorities: domain.DefaultSourcePriorities})
	second := merge.Adjudicate(results, merge.Options{Priorities: domain.DefaultSourcePriorities})
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("adjudication is not deterministic")
	}
	for index := 1; index < len(first.Services); index++ {
		if first.Services[index-1].ServiceName > first.Services[index].ServiceName {
			t.Fatalf("services are not sorted")
		}
	}
}

func TestV1FieldFallbackDoesNotCreateAccountConflict(t *testing.T) {
	results := parsed(t, "v1", samplegen.FaultNone)
	adjudication := merge.Adjudicate(results, merge.Options{Priorities: domain.DefaultSourcePriorities})
	for _, service := range adjudication.Services {
		if len(service.Merge.Conflicts) != 0 {
			t.Fatalf("%s conflicts under v1: %+v", service.ServiceName, service.Merge.Conflicts)
		}
	}
	custom := findService(t, adjudication, "custom worker")
	if custom.Privilege.AccountKind != domain.AccountCustom {
		t.Fatalf("v1 account fallback failed: %+v", custom.Privilege)
	}
}

func parsed(t *testing.T, target, fault string) []domain.SourceResult {
	t.Helper()
	dir := t.TempDir()
	inputs, err := samplegen.Generate(dir, samplegen.Options{TargetFormat: target, Fault: fault})
	if err != nil {
		t.Fatal(err)
	}
	scheduler := source.NewScheduler(3)
	results, _ := scheduler.ParseAll(context.Background(), inputs, source.Options{ServiceBudget: 128})
	return results
}

func findService(t *testing.T, adjudication merge.Adjudication, name string) domain.ServiceVerdict {
	t.Helper()
	for _, service := range adjudication.Services {
		if service.ServiceName == name {
			return service
		}
	}
	t.Fatalf("service %s not found", name)
	return domain.ServiceVerdict{}
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func hasCode(issues []domain.Issue, target domain.Code) bool {
	for _, issue := range issues {
		if issue.Code == target {
			return true
		}
	}
	return false
}
