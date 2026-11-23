package workbench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"svcworkbench/internal/domain"
	"svcworkbench/internal/samplegen"
	"svcworkbench/internal/snapshot"
)

type FaultCheck struct {
	Fault                  string   `json:"fault"`
	ExpectedCode           string   `json:"expected_code"`
	ObservedCodes          []string `json:"observed_codes"`
	SnapshotVersionBefore  int      `json:"snapshot_version_before"`
	SnapshotVersionAfter   int      `json:"snapshot_version_after"`
	SnapshotCommitted      bool     `json:"snapshot_committed"`
	ServiceCount           int      `json:"service_count"`
	OtherServicesPreserved bool     `json:"other_services_preserved"`
	Passed                 bool     `json:"passed"`
	Detail                 string   `json:"detail,omitempty"`
}

type DemoOutput struct {
	*domain.Report
	DemoSchemaVersion string              `json:"demo_schema_version"`
	SnapshotVersions  []int               `json:"snapshot_versions"`
	Replay            *ReplayOutput       `json:"replay"`
	Trace             *domain.TraceOutput `json:"trace"`
	FaultChecks       []FaultCheck        `json:"fault_checks"`
}

type serviceConclusion struct {
	Path           string                   `json:"path"`
	Arguments      string                   `json:"arguments"`
	StartType      domain.StartType         `json:"start_type"`
	AccountKind    domain.AccountKind       `json:"account_kind"`
	Logons         []domain.LogonCapability `json:"logons"`
	EffectiveTier  string                   `json:"effective_tier"`
	EffectiveScore int                      `json:"effective_score"`
	HighPrivilege  bool                     `json:"high_privilege"`
	RiskFlags      []string                 `json:"risk_flags"`
}

func Demo(ctx context.Context, targetFormat string) (*DemoOutput, error) {
	if strings.TrimSpace(targetFormat) == "" {
		targetFormat = "v2"
	}
	stateDir, err := os.MkdirTemp("", "svcworkbench-demo-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(stateDir)
	options := Options{
		StateDir:       stateDir,
		TargetFormat:   targetFormat,
		Priorities:     append([]domain.SourceKind(nil), domain.DefaultSourcePriorities...),
		MaxConcurrency: 3,
		ServiceBudget:  128,
	}
	var reports []*domain.Report
	for round := 0; round < 3; round++ {
		outcome, err := Run(ctx, options)
		if err != nil {
			return nil, err
		}
		if outcome.Report.Snapshot == nil || !outcome.Report.Snapshot.Committed {
			return nil, fmt.Errorf("demo round %d was unexpectedly blocked: %v", round+1, outcome.Report.BusinessCodes)
		}
		reports = append(reports, outcome.Report)
	}
	finalReport := reports[len(reports)-1]
	expected := serviceConclusions(reports[0])
	replay, err := Replay(ctx, stateDir, 2, targetFormat, options.Priorities)
	if err != nil {
		return nil, err
	}
	trace, err := Trace(ctx, stateDir, 0, "Writable Agent", targetFormat, options.Priorities)
	if err != nil {
		return nil, err
	}
	checks, err := runFaultChecks(ctx, targetFormat, expected)
	if err != nil {
		return nil, err
	}
	return &DemoOutput{
		Report:            finalReport,
		DemoSchemaVersion: "service-adjudication-demo-v1",
		SnapshotVersions:  finalReport.Snapshot.ChainVersions,
		Replay:            replay,
		Trace:             trace,
		FaultChecks:       checks,
	}, nil
}

func runFaultChecks(ctx context.Context, targetFormat string, expected map[string]serviceConclusion) ([]FaultCheck, error) {
	checks := make([]FaultCheck, 0, 6)
	type checkCase struct {
		fault    string
		code     string
		affected map[string]bool
	}
	cases := []checkCase{
		{fault: samplegen.FaultUnknownVersion, code: string(domain.CodeFormatVersion), affected: map[string]bool{}},
		{fault: samplegen.FaultTextPrefixOOB, code: string(domain.CodeFormatVersion), affected: map[string]bool{"quoted tool service": true}},
		{fault: samplegen.FaultUnterminatedQuote, code: string(domain.CodePathMalformed), affected: map[string]bool{"quoted tool service": true}},
		{fault: samplegen.FaultUnknownAccount, code: string(domain.CodeAccountUnknown), affected: map[string]bool{"custom worker": true}},
		{fault: samplegen.FaultDeclarationConflict, code: string(domain.CodeDeclarationConflict), affected: map[string]bool{"vendor updater": true}},
	}
	for _, item := range cases {
		dir, err := os.MkdirTemp("", "svcworkbench-fault-*")
		if err != nil {
			return nil, err
		}
		outcome, runErr := Run(ctx, Options{
			StateDir:       dir,
			TargetFormat:   targetFormat,
			Priorities:     append([]domain.SourceKind(nil), domain.DefaultSourcePriorities...),
			MaxConcurrency: 3,
			ServiceBudget:  128,
			Fault:          item.fault,
		})
		os.RemoveAll(dir)
		if runErr != nil {
			return nil, runErr
		}
		check := buildFaultCheck(item.fault, item.code, outcome.Report, expected, item.affected, 0)
		checks = append(checks, check)
	}
	gapDir, err := os.MkdirTemp("", "svcworkbench-gap-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(gapDir)
	baseline, err := Run(ctx, Options{
		StateDir:       gapDir,
		TargetFormat:   targetFormat,
		Priorities:     append([]domain.SourceKind(nil), domain.DefaultSourcePriorities...),
		MaxConcurrency: 3,
		ServiceBudget:  128,
	})
	if err != nil {
		return nil, err
	}
	before := 0
	if baseline.Report.Snapshot != nil {
		before = baseline.Report.Snapshot.ActiveVersion
	}
	store, err := snapshot.Open(filepath.Join(gapDir, "workbench.db"))
	if err != nil {
		return nil, err
	}
	if err := store.InjectSnapshotGap(ctx); err != nil {
		store.Close()
		return nil, err
	}
	store.Close()
	gapOutcome, err := Run(ctx, Options{
		StateDir:       gapDir,
		TargetFormat:   targetFormat,
		Priorities:     append([]domain.SourceKind(nil), domain.DefaultSourcePriorities...),
		MaxConcurrency: 3,
		ServiceBudget:  128,
		Fault:          samplegen.FaultSnapshotGap,
	})
	if err != nil {
		return nil, err
	}
	checks = append(checks, buildFaultCheck(samplegen.FaultSnapshotGap, string(domain.CodeSnapshotVersion), gapOutcome.Report, expected, map[string]bool{}, before))
	return checks, nil
}

func buildFaultCheck(fault, expectedCode string, report *domain.Report, expected map[string]serviceConclusion, affected map[string]bool, before int) FaultCheck {
	after := 0
	committed := false
	if report.Snapshot != nil {
		after = report.Snapshot.ActiveVersion
		committed = report.Snapshot.Committed
	}
	actual := serviceConclusions(report)
	preserved := true
	detail := ""
	if len(actual) != len(expected) {
		preserved = false
		detail = fmt.Sprintf("service count changed from %d to %d", len(expected), len(actual))
	}
	for name, conclusion := range expected {
		if affected[name] {
			continue
		}
		actualConclusion, ok := actual[name]
		if !ok {
			preserved = false
			detail = fmt.Sprintf("service %s missing", name)
			break
		}
		if !reflect.DeepEqual(actualConclusion, conclusion) {
			preserved = false
			detail = fmt.Sprintf("service %s conclusion changed", name)
			break
		}
	}
	hasCode := containsString(report.BusinessCodes, expectedCode)
	passed := hasCode && !committed && before == after && preserved
	return FaultCheck{
		Fault:                  fault,
		ExpectedCode:           expectedCode,
		ObservedCodes:          append([]string(nil), report.BusinessCodes...),
		SnapshotVersionBefore:  before,
		SnapshotVersionAfter:   after,
		SnapshotCommitted:      committed,
		ServiceCount:           len(report.Services),
		OtherServicesPreserved: preserved,
		Passed:                 passed,
		Detail:                 detail,
	}
}

func serviceConclusions(report *domain.Report) map[string]serviceConclusion {
	out := make(map[string]serviceConclusion, len(report.Services))
	for _, service := range report.Services {
		logons := append([]domain.LogonCapability(nil), service.Privilege.LogonCapabilities...)
		out[service.ServiceName] = serviceConclusion{
			Path:           service.ImagePath.Normalized,
			Arguments:      service.ImagePath.Arguments,
			StartType:      service.Privilege.StartType,
			AccountKind:    service.Privilege.AccountKind,
			Logons:         logons,
			EffectiveTier:  service.Privilege.EffectiveTier,
			EffectiveScore: service.Privilege.EffectiveScore,
			HighPrivilege:  service.Privilege.HighPrivilege,
			RiskFlags:      append([]string(nil), service.RiskFlags...),
		}
	}
	return out
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
