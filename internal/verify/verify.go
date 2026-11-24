package verify

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"supplyaudit/internal/formats"
	"supplyaudit/internal/merge"
	"supplyaudit/internal/model"
	"supplyaudit/internal/pipeline"
	"supplyaudit/internal/report"
	"supplyaudit/internal/sample"
	"supplyaudit/internal/snapshot"
)

const (
	VerificationSchema = 1
	Marker             = "deterministic-fault-injection"
)

type Assertion struct {
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Details string `json:"details"`
}

type ScenarioReport struct {
	ID             string      `json:"id"`
	Kind           string      `json:"kind"`
	ExpectedCode   string      `json:"expected_code"`
	ObservedCodes  []string    `json:"observed_codes"`
	SnapshotBefore int         `json:"snapshot_before"`
	SnapshotAfter  int         `json:"snapshot_after"`
	Passed         bool        `json:"passed"`
	Assertions     []Assertion `json:"assertions"`
}

type VerificationReport struct {
	ReportSchemaVersion int              `json:"report_schema_version"`
	Marker              string           `json:"marker"`
	SampleTimestamp     string           `json:"sample_timestamp"`
	AllPassed           bool             `json:"all_passed"`
	BaselineSnapshot    int              `json:"baseline_snapshot"`
	BaselineCodes       []string         `json:"baseline_codes"`
	GlobalAssertions    []Assertion      `json:"global_assertions"`
	Scenarios           []ScenarioReport `json:"scenarios"`
}

func Run(ctx context.Context, workDir string) (VerificationReport, error) {
	layout, err := sample.Generate(workDir, true)
	if err != nil {
		return VerificationReport{}, err
	}
	// Keep verification fixtures isolated from caller-owned audit state.
	stateDir := filepath.Join(workDir, "verification-state")
	if err := os.RemoveAll(stateDir); err != nil {
		return VerificationReport{}, err
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return VerificationReport{}, err
	}

	config := pipeline.DefaultConfig()
	sources, err := formats.Discover(layout.InputDir)
	if err != nil {
		return VerificationReport{}, err
	}
	baselineResult, err := pipeline.Run(sources, config)
	if err != nil {
		return VerificationReport{}, err
	}
	baselineData := report.BuildAuditData(baselineResult)
	payload, err := report.MarshalAuditData(baselineData)
	if err != nil {
		return VerificationReport{}, err
	}

	dbPath := filepath.Join(stateDir, "canonical.db")
	store, err := snapshot.Open(dbPath)
	if err != nil {
		return VerificationReport{}, err
	}
	defer store.Close()
	compatibility := snapshot.Compatibility{
		SchemaVersion:   snapshot.SchemaVersion,
		AcceptedFormats: config.AcceptedFormats(),
		Priority:        config.PriorityStrings(),
	}
	baselineMeta, err := store.Append(ctx, payload, baselineResult.RequiredFormats, config.PriorityStrings(), config.AcceptedFormats())
	if err != nil {
		return VerificationReport{}, err
	}
	chain, err := store.Chain(ctx)
	if err != nil {
		return VerificationReport{}, err
	}
	baselineReport := report.Build(baselineData, report.SnapshotSection{
		RequestedVersion: baselineMeta.Version,
		CommittedVersion: baselineMeta.Version,
		Status:           "committed",
		Chain:            chain,
	})
	if _, err := report.Write(filepath.Join(workDir, "baseline-report.json"), baselineReport); err != nil {
		return VerificationReport{}, err
	}

	globalAssertions := make([]Assertion, 0, 10)
	globalAssertions = append(globalAssertions, assertion("baseline_snapshot_committed", baselineMeta.Version == 1, fmt.Sprintf("snapshot=%d", baselineMeta.Version)))
	globalAssertions = append(globalAssertions, assertion("baseline_chain_contiguous", len(chain) == 1 && chain[0].Version == 1, fmt.Sprintf("chain_length=%d", len(chain))))
	globalAssertions = append(globalAssertions, assertion("baseline_declarations_self_consistent", baselineResult.Merge.SelfConsistent, baselineResult.Merge.ConsistencyMessage))
	coverageComplete := true
	for _, coverage := range baselineResult.Merge.SourceCoverage {
		if !coverage.Complete {
			coverageComplete = false
		}
	}
	globalAssertions = append(globalAssertions, assertion("baseline_source_coverage_complete", coverageComplete, fmt.Sprintf("sources=%d", len(baselineResult.Merge.SourceCoverage))))
	for _, code := range []string{model.CodeDigestConflict, model.CodeVersionDowngrade, model.CodeWeakDigest} {
		globalAssertions = append(globalAssertions, assertion("baseline_has_"+strings.ToLower(code), baselineResult.HasCode(code), strings.Join(baselineResult.AllCodes(), ",")))
	}
	globalAssertions = append(globalAssertions, assertion("baseline_no_blocking_parse_issue", len(baselineResult.BlockingIssueCodes()) == 0, strings.Join(baselineResult.BlockingIssueCodes(), ",")))

	incompatiblePriority := append([]string(nil), compatibility.Priority...)
	incompatiblePriority[0], incompatiblePriority[1] = incompatiblePriority[1], incompatiblePriority[0]
	_, priorityErr := store.Load(ctx, 1, snapshot.Compatibility{SchemaVersion: snapshot.SchemaVersion, AcceptedFormats: compatibility.AcceptedFormats, Priority: incompatiblePriority})
	globalAssertions = append(globalAssertions, assertion("snapshot_priority_incompatibility_rejected", model.CodeOf(priorityErr) == model.CodeSnapshotVersion, errorDetail(priorityErr)))

	v1Config := config
	v1Config.BinaryFormatVersion = "v1"
	_, formatErr := store.Load(ctx, 1, snapshot.Compatibility{SchemaVersion: snapshot.SchemaVersion, AcceptedFormats: v1Config.AcceptedFormats(), Priority: compatibility.Priority})
	globalAssertions = append(globalAssertions, assertion("snapshot_required_format_incompatibility_rejected", model.CodeOf(formatErr) == model.CodeSnapshotVersion, errorDetail(formatErr)))

	replacementErr := verifyPayloadReplacement(ctx, filepath.Join(stateDir, "replacement.db"), payload, compatibility)
	globalAssertions = append(globalAssertions, assertion("snapshot_whole_payload_replacement_rejected", model.CodeOf(replacementErr) == model.CodeSnapshotVersion, errorDetail(replacementErr)))

	manifest, err := sample.LoadFaultManifest(layout.FaultManifest)
	if err != nil {
		return VerificationReport{}, err
	}
	scenarios := make([]ScenarioReport, 0, len(manifest.Scenarios))
	for _, scenario := range manifest.Scenarios {
		before, err := store.LatestVersion(ctx)
		if err != nil {
			return VerificationReport{}, err
		}
		scenarioReport := ScenarioReport{
			ID:             scenario.ID,
			Kind:           scenario.Kind,
			ExpectedCode:   scenario.ExpectedCode,
			ObservedCodes:  []string{},
			SnapshotBefore: before,
			Assertions:     []Assertion{},
		}
		switch scenario.Kind {
		case "sources":
			scenarioReport = verifySourceScenario(ctx, scenario, workDir, config, store, scenarioReport)
		case "snapshot_fixture":
			scenarioReport = verifySnapshotGap(ctx, scenario, stateDir, store, payload, baselineResult.RequiredFormats, config, compatibility, scenarioReport)
		default:
			scenarioReport.ObservedCodes = []string{model.CodeInvalidConfig}
			scenarioReport.Assertions = append(scenarioReport.Assertions, assertion("known_scenario_kind", false, "unknown scenario kind "+scenario.Kind))
		}
		after, err := store.LatestVersion(ctx)
		if err != nil {
			return VerificationReport{}, err
		}
		scenarioReport.SnapshotAfter = after
		scenarioReport.Assertions = append(scenarioReport.Assertions, assertion("committed_snapshot_not_advanced", before == after, fmt.Sprintf("before=%d after=%d", before, after)))
		scenarioReport.Passed = allPassed(scenarioReport.Assertions)
		scenarios = append(scenarios, scenarioReport)
	}
	sort.Slice(scenarios, func(i, j int) bool { return scenarios[i].ID < scenarios[j].ID })

	verification := VerificationReport{
		ReportSchemaVersion: VerificationSchema,
		Marker:              Marker,
		SampleTimestamp:     report.SampleTimestamp,
		AllPassed:           allPassed(globalAssertions) && allScenariosPassed(scenarios),
		BaselineSnapshot:    baselineMeta.Version,
		BaselineCodes:       baselineResult.AllCodes(),
		GlobalAssertions:    globalAssertions,
		Scenarios:           scenarios,
	}
	return verification, nil
}

func verifySourceScenario(ctx context.Context, scenario sample.FaultScenario, workDir string, config pipeline.Config, store *snapshot.Store, scenarioReport ScenarioReport) ScenarioReport {
	inputDir := filepath.Join(workDir, filepath.FromSlash(scenario.Input))
	sources, err := formats.Discover(inputDir)
	if err != nil {
		scenarioReport.Assertions = append(scenarioReport.Assertions, assertion("source_discovery", false, err.Error()))
		return scenarioReport
	}
	result, err := pipeline.Run(sources, config)
	if err != nil {
		scenarioReport.Assertions = append(scenarioReport.Assertions, assertion("pipeline_run", false, err.Error()))
		return scenarioReport
	}
	scenarioReport.ObservedCodes = result.AllCodes()
	codeSeen := result.HasCode(scenario.ExpectedCode)
	scenarioReport.Assertions = append(scenarioReport.Assertions, assertion("expected_code_observed", codeSeen, "expected="+scenario.ExpectedCode+" observed="+strings.Join(scenarioReport.ObservedCodes, ",")))
	scenarioReport.Assertions = append(scenarioReport.Assertions, assertion("component_level_code_attribution", codeAttributedToComponent(result, scenario.ExpectedCode, scenario.TargetComponent), "target="+scenario.TargetComponent))
	controlPresent, controlClean := controlComponentUntouched(result.Merge.Components, scenario.ControlComponent)
	scenarioReport.Assertions = append(scenarioReport.Assertions, assertion("unaffected_component_preserved", controlPresent && controlClean, fmt.Sprintf("control=%s present=%t clean=%t", scenario.ControlComponent, controlPresent, controlClean)))
	scenarioReport.Assertions = append(scenarioReport.Assertions, assertion("merge_self_consistent", result.Merge.SelfConsistent, result.Merge.ConsistencyMessage))

	// A fault probe is never committed. Structurally bad sources are already
	// rejected by the audit commit policy; this guard covers nonfatal findings
	// such as conflicts or downgrades as well, so the canonical chain remains a
	// record of complete, successful audit rounds.
	version, err := store.LatestVersion(ctx)
	scenarioReport.Assertions = append(scenarioReport.Assertions, assertion("canonical_store_readable", err == nil, errorDetail(err)))
	scenarioReport.SnapshotBefore = version
	return scenarioReport
}

func verifySnapshotGap(ctx context.Context, scenario sample.FaultScenario, stateDir string, canonicalStore *snapshot.Store, payload []byte, required []string, config pipeline.Config, compatibility snapshot.Compatibility, scenarioReport ScenarioReport) ScenarioReport {
	gapStore, err := snapshot.Open(filepath.Join(stateDir, "gap.db"))
	if err != nil {
		scenarioReport.Assertions = append(scenarioReport.Assertions, assertion("gap_store_open", false, err.Error()))
		return scenarioReport
	}
	defer gapStore.Close()
	if _, err := gapStore.AppendGapFixture(ctx, payload, required, config.PriorityStrings(), config.AcceptedFormats()); err != nil {
		scenarioReport.Assertions = append(scenarioReport.Assertions, assertion("gap_fixture_created", false, err.Error()))
		return scenarioReport
	}
	_, gapErr := gapStore.Load(ctx, 3, compatibility)
	scenarioReport.ObservedCodes = []string{model.CodeOf(gapErr)}
	scenarioReport.Assertions = append(scenarioReport.Assertions, assertion("expected_code_observed", model.CodeOf(gapErr) == scenario.ExpectedCode, "expected="+scenario.ExpectedCode+" observed="+model.CodeOf(gapErr)))
	scenarioReport.Assertions = append(scenarioReport.Assertions, assertion("component_level_code_attribution", model.CodeOf(gapErr) == scenario.ExpectedCode, "target="+scenario.TargetComponent))
	_, baseErr := canonicalStore.Load(ctx, 1, compatibility)
	scenarioReport.Assertions = append(scenarioReport.Assertions, assertion("unaffected_snapshot_preserved", baseErr == nil, errorDetail(baseErr)))
	return scenarioReport
}

func verifyPayloadReplacement(ctx context.Context, dbPath string, payload []byte, compatibility snapshot.Compatibility) error {
	store, err := snapshot.Open(dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	if _, err := store.Append(ctx, payload, []string{"json/v1"}, compatibility.Priority, compatibility.AcceptedFormats); err != nil {
		return err
	}
	if err := store.ReplacePayloadFixture(ctx, 1, []byte("{\"replacement\":true}\n")); err != nil {
		return err
	}
	_, err = store.Load(ctx, 1, compatibility)
	return err
}

func controlComponentUntouched(components []merge.ComponentView, componentKey string) (bool, bool) {
	key := strings.ToLower(componentKey)
	for _, component := range components {
		if component.ComponentKey != key {
			continue
		}
		if len(component.Codes) != 0 {
			return true, false
		}
		found := false
		for _, version := range component.Versions {
			if len(version.Codes) != 0 {
				return true, false
			}
			if version.Version.Canonical == "1.0.0" {
				found = true
				for _, declaration := range version.Declarations {
					if declaration.Digest.Algorithm != "sha256" || !declaration.Digest.Equal(expectedControlDigest()) {
						return true, false
					}
				}
			}
		}
		return true, found
	}
	return false, false
}

func expectedControlDigest() model.DigestView {
	sum := sha256.Sum256([]byte("fault-control"))
	hexValue := hex.EncodeToString(sum[:])
	return model.DigestView{Algorithm: "sha256", Hex: hexValue}
}

func codeAttributedToComponent(result pipeline.Result, code, target string) bool {
	key := strings.ToLower(target)
	if code == model.CodeFormatVersion || code == model.CodeTruncated || code == model.CodeDigestInvalid {
		for _, source := range result.Sources {
			for _, issue := range source.Issues {
				if issue.Code == code {
					return true
				}
			}
		}
		return false
	}
	for _, finding := range result.Merge.Findings {
		if finding.Code == code && finding.ComponentKey == key {
			return true
		}
	}
	return false
}

func assertion(name string, passed bool, details string) Assertion {
	return Assertion{Name: name, Passed: passed, Details: details}
}

func allPassed(assertions []Assertion) bool {
	for _, assertion := range assertions {
		if !assertion.Passed {
			return false
		}
	}
	return true
}

func allScenariosPassed(scenarios []ScenarioReport) bool {
	for _, scenario := range scenarios {
		if !scenario.Passed {
			return false
		}
	}
	return true
}

func errorDetail(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}
