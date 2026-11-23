package anomaly

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"vpnbench/internal/app"
	"vpnbench/internal/config"
	"vpnbench/internal/model"
	"vpnbench/internal/report"
	"vpnbench/internal/store"
)

const Schema = "vpnbench.anomaly-result/v1"

type Assertion struct {
	Name     string `json:"name"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	Passed   bool   `json:"passed"`
}

type SnapshotState struct {
	PolicyVersions        []int64  `json:"policy_versions"`
	SettlementVersions    []int64  `json:"settlement_versions"`
	PolicyGapDetected     bool     `json:"policy_gap_detected"`
	SettlementGapDetected bool     `json:"settlement_gap_detected"`
	FallbackVersion       int64    `json:"fallback_version,omitempty"`
	LatestPolicyHash      string   `json:"latest_policy_hash,omitempty"`
	FallbackPolicyHash    string   `json:"fallback_policy_hash,omitempty"`
	LatestChangeType      string   `json:"latest_change_type,omitempty"`
	WholeReplacement      bool     `json:"whole_replacement,omitempty"`
	BeforePolicyIDs       []string `json:"before_policy_ids,omitempty"`
	AfterPolicyIDs        []string `json:"after_policy_ids,omitempty"`
}

type Result struct {
	Schema                  string        `json:"schema"`
	Case                    string        `json:"case"`
	ExpectedCodes           []model.Code  `json:"expected_codes"`
	ObservedCodes           []model.Code  `json:"observed_codes"`
	Passed                  bool          `json:"passed"`
	SnapshotClean           bool          `json:"snapshot_clean"`
	OtherSessionsUnaffected bool          `json:"other_sessions_unaffected"`
	Snapshot                SnapshotState `json:"snapshot_state"`
	Assertions              []Assertion   `json:"assertions"`
	Workbench               report.Report `json:"workbench"`
}

func Run(ctx context.Context, paramsPath, dbPath string) (Result, error) {
	params, err := config.ReadAnomaly(paramsPath)
	if err != nil {
		return Result{}, model.NewError(model.CodeInternal, "read anomaly params: "+err.Error(), 0)
	}
	if err := resetDatabase(dbPath); err != nil {
		return Result{}, model.NewError(model.CodeInternal, "reset anomaly database: "+err.Error(), 0)
	}

	var workbench report.Report
	var snapshotState SnapshotState
	switch params.Case {
	case "no_match", "fixed_overflow":
		workbench, snapshotState, err = runSingle(ctx, params, dbPath)
	case "snapshot_gap":
		workbench, snapshotState, err = runSnapshotGap(ctx, params, dbPath)
	case "whole_replacement":
		workbench, snapshotState, err = runWholeReplacement(ctx, params, dbPath)
	case "session_race":
		workbench, snapshotState, err = runSessions(ctx, params, dbPath)
	default:
		return Result{}, model.NewError(model.CodeInternal, "unknown anomaly case "+params.Case, 0)
	}
	if err != nil {
		return Result{}, err
	}

	observed := collectCodes(workbench)
	expectedCodes := make([]model.Code, 0, len(params.ExpectedCodes))
	for _, code := range params.ExpectedCodes {
		expectedCodes = append(expectedCodes, model.Code(code))
	}
	sort.Slice(expectedCodes, func(i, j int) bool { return expectedCodes[i] < expectedCodes[j] })

	codesPassed := containsAll(observed, expectedCodes)
	snapshotClean, snapshotAssertion := snapshotGuard(params, snapshotState)
	othersUnaffected, othersAssertion := othersGuard(params, workbench)
	_, consistencyAssertion := consistencyGuard(workbench)

	result := Result{
		Schema:                  Schema,
		Case:                    params.Case,
		ExpectedCodes:           expectedCodes,
		ObservedCodes:           observed,
		SnapshotClean:           snapshotClean,
		OtherSessionsUnaffected: othersUnaffected,
		Snapshot:                snapshotState,
		Workbench:               workbench,
	}
	result.Assertions = []Assertion{
		{Name: "expected_business_codes", Expected: joinCodes(expectedCodes), Actual: joinCodes(observed), Passed: codesPassed},
		snapshotAssertion,
		othersAssertion,
		consistencyAssertion,
	}
	result.Passed = true
	for _, assertion := range result.Assertions {
		if !assertion.Passed {
			result.Passed = false
		}
	}
	return result, nil
}

func runSingle(ctx context.Context, params config.AnomalyParams, dbPath string) (report.Report, SnapshotState, error) {
	run, err := app.Run(ctx, app.RunSpec{
		PolicyPath:       params.PolicyFile,
		Sessions:         sessionsOrDefault(params, "case-"+params.Case),
		ConcurrencyLimit: params.ConcurrencyLimit,
		LatencyClock:     "fixed",
		DefaultBudget:    params.RequestBudget,
	}, dbPath)
	if err != nil {
		return report.Report{}, SnapshotState{}, err
	}
	state, err := loadSnapshotState(ctx, dbPath)
	return run.Report, state, err
}

func runSnapshotGap(ctx context.Context, params config.AnomalyParams, dbPath string) (report.Report, SnapshotState, error) {
	seedSpec := app.RunSpec{
		PolicyPath:       params.PolicyFile,
		Sessions:         []config.SessionSpec{{SessionID: "seed", RequestsFile: params.RequestsFile, RequestBudget: params.RequestBudget}},
		ConcurrencyLimit: 1,
		LatencyClock:     "fixed",
		DefaultBudget:    params.RequestBudget,
	}
	if _, err := app.Run(ctx, seedSpec, dbPath); err != nil {
		return report.Report{}, SnapshotState{}, err
	}
	database, err := store.Open(dbPath)
	if err != nil {
		return report.Report{}, SnapshotState{}, err
	}
	_, injectErr := database.InjectSnapshotGap(ctx, "policy", []byte(`{"schema":"vpnbench.injected-gap/v1"}`))
	closeErr := database.Close()
	if injectErr != nil {
		return report.Report{}, SnapshotState{}, injectErr
	}
	if closeErr != nil {
		return report.Report{}, SnapshotState{}, closeErr
	}
	run, err := app.Run(ctx, app.RunSpec{
		PolicyPath:       params.PolicyFile,
		Sessions:         sessionsOrDefault(params, "case-"+params.Case),
		ConcurrencyLimit: params.ConcurrencyLimit,
		LatencyClock:     "fixed",
		DefaultBudget:    params.RequestBudget,
	}, dbPath)
	if err != nil {
		return report.Report{}, SnapshotState{}, err
	}
	state, err := loadSnapshotState(ctx, dbPath)
	return run.Report, state, err
}

func runWholeReplacement(ctx context.Context, params config.AnomalyParams, dbPath string) (report.Report, SnapshotState, error) {
	if _, err := app.Run(ctx, app.RunSpec{
		PolicyPath:       params.PolicyFile,
		Sessions:         []config.SessionSpec{{SessionID: "replacement-seed", RequestsFile: params.RequestsFile, RequestBudget: params.RequestBudget}},
		ConcurrencyLimit: 1,
		LatencyClock:     "fixed",
		DefaultBudget:    params.RequestBudget,
	}, dbPath); err != nil {
		return report.Report{}, SnapshotState{}, err
	}
	run, err := app.Run(ctx, app.RunSpec{
		PolicyPath:       params.ReplacementPolicyFile,
		Sessions:         sessionsOrDefault(params, "case-"+params.Case),
		ConcurrencyLimit: params.ConcurrencyLimit,
		LatencyClock:     "fixed",
		DefaultBudget:    params.RequestBudget,
	}, dbPath)
	if err != nil {
		return report.Report{}, SnapshotState{}, err
	}
	state, err := loadSnapshotState(ctx, dbPath)
	return run.Report, state, err
}

func runSessions(ctx context.Context, params config.AnomalyParams, dbPath string) (report.Report, SnapshotState, error) {
	run, err := app.Run(ctx, app.RunSpec{
		PolicyPath:       params.PolicyFile,
		Sessions:         params.Sessions,
		ConcurrencyLimit: params.ConcurrencyLimit,
		LatencyClock:     "fixed",
		DefaultBudget:    params.RequestBudget,
	}, dbPath)
	if err != nil {
		return report.Report{}, SnapshotState{}, err
	}
	state, err := loadSnapshotState(ctx, dbPath)
	return run.Report, state, err
}

func sessionsOrDefault(params config.AnomalyParams, sessionID string) []config.SessionSpec {
	if len(params.Sessions) > 0 {
		return params.Sessions
	}
	return []config.SessionSpec{{SessionID: sessionID, RequestsFile: params.RequestsFile, RequestBudget: params.RequestBudget}}
}

func loadSnapshotState(ctx context.Context, dbPath string) (SnapshotState, error) {
	database, err := store.Open(dbPath)
	if err != nil {
		return SnapshotState{}, err
	}
	defer database.Close()
	policyChain, err := database.Chain(ctx, "policy")
	if err != nil {
		return SnapshotState{}, err
	}
	settlementChain, err := database.Chain(ctx, "settlement")
	if err != nil {
		return SnapshotState{}, err
	}
	state := SnapshotState{
		PolicyVersions:        versions(policyChain),
		SettlementVersions:    versions(settlementChain),
		PolicyGapDetected:     len(policyChain.MissingVersions) > 0,
		SettlementGapDetected: len(settlementChain.MissingVersions) > 0,
	}
	if policyChain.Fallback != nil {
		state.FallbackVersion = policyChain.Fallback.Version
		state.FallbackPolicyHash = policyChain.Fallback.ContentHash
	}
	if len(policyChain.Snapshots) > 0 {
		latest := policyChain.Snapshots[len(policyChain.Snapshots)-1]
		state.LatestPolicyHash = latest.ContentHash
		state.LatestChangeType = latest.ChangeType
		state.WholeReplacement = latest.WholeReplacement
		state.BeforePolicyIDs = append([]string(nil), latest.BeforeSummary.IdentityKeys...)
		state.AfterPolicyIDs = append([]string(nil), latest.AfterSummary.IdentityKeys...)
	}
	return state, nil
}

func snapshotGuard(params config.AnomalyParams, state SnapshotState) (bool, Assertion) {
	passed := !state.SettlementGapDetected && len(state.PolicyVersions) > 0 && len(state.SettlementVersions) > 0
	expected := "contiguous policy and settlement chains"
	actual := fmt.Sprintf("policy=%v settlement=%v", state.PolicyVersions, state.SettlementVersions)
	if params.Case == "snapshot_gap" {
		expected = "E_SNAPSHOT_VERSION with fallback to a committed contiguous version"
		passed = state.PolicyGapDetected && state.FallbackVersion > 0 && state.FallbackPolicyHash != "" && !state.SettlementGapDetected
		actual = fmt.Sprintf("gap=%t fallback=%d fallback_hash=%s", state.PolicyGapDetected, state.FallbackVersion, state.FallbackPolicyHash)
	}
	if params.Case == "whole_replacement" {
		passed = passed && state.WholeReplacement && state.LatestChangeType == "WHOLE_REPLACEMENT" &&
			len(state.BeforePolicyIDs) > 0 && len(state.AfterPolicyIDs) > 0
		actual = fmt.Sprintf("change=%s whole=%t before=%v after=%v", state.LatestChangeType, state.WholeReplacement, state.BeforePolicyIDs, state.AfterPolicyIDs)
	}
	return passed, Assertion{Name: "snapshot_guard", Expected: expected, Actual: actual, Passed: passed}
}

func othersGuard(params config.AnomalyParams, workbench report.Report) (bool, Assertion) {
	if !params.ExpectOthersUnaffected {
		return true, Assertion{Name: "other_sessions_unaffected", Expected: "not asserted", Actual: "not asserted", Passed: true}
	}
	healthy := 0
	okRequests := 0
	for _, session := range workbench.Sessions {
		allOK := len(session.Requests) > 0
		for _, request := range session.Requests {
			if request.Settlement.Code == model.CodeOK {
				okRequests++
			} else {
				allOK = false
			}
		}
		if allOK && session.Status == "FINALIZED" {
			healthy++
		}
	}
	passed := true
	expected := "at least one independent request remains OK"
	if params.Case == "session_race" {
		expected = "at least two sessions remain FINALIZED with all requests OK"
		passed = healthy >= 2
	} else {
		passed = okRequests > 0
	}
	actual := fmt.Sprintf("healthy_sessions=%d ok_requests=%d", healthy, okRequests)
	return passed, Assertion{Name: "other_sessions_unaffected", Expected: expected, Actual: actual, Passed: passed}
}

func consistencyGuard(workbench report.Report) (bool, Assertion) {
	total := 0
	consistent := 0
	for _, session := range workbench.Sessions {
		total += session.Consistency.Total
		consistent += session.Consistency.Consistent
		if len(session.Consistency.Mismatches) != 0 {
			return false, Assertion{Name: "interactive_settlement_consistency", Expected: "all rows consistent", Actual: string(session.Consistency.Mismatches[0].Code) + " at " + session.SessionID, Passed: false}
		}
	}
	passed := total == consistent
	return passed, Assertion{Name: "interactive_settlement_consistency", Expected: "all rows consistent", Actual: fmt.Sprintf("%d/%d", consistent, total), Passed: passed}
}

func collectCodes(workbench report.Report) []model.Code {
	set := make(map[model.Code]struct{})
	for _, occurrence := range workbench.BusinessCodes {
		set[occurrence.Code] = struct{}{}
	}
	codes := make([]model.Code, 0, len(set))
	for code := range set {
		codes = append(codes, code)
	}
	sort.Slice(codes, func(i, j int) bool { return codes[i] < codes[j] })
	return codes
}

func containsAll(observed, expected []model.Code) bool {
	set := make(map[model.Code]struct{}, len(observed))
	for _, code := range observed {
		set[code] = struct{}{}
	}
	for _, code := range expected {
		if _, ok := set[code]; !ok {
			return false
		}
	}
	return true
}

func joinCodes(codes []model.Code) string {
	if len(codes) == 0 {
		return ""
	}
	result := ""
	for i, code := range codes {
		if i > 0 {
			result += ","
		}
		result += string(code)
	}
	return result
}

func versions(chain store.Chain) []int64 {
	versions := make([]int64, 0, len(chain.Snapshots))
	for _, snapshot := range chain.Snapshots {
		versions = append(versions, snapshot.Version)
	}
	return versions
}

func resetDatabase(path string) error {
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Remove(candidate); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return os.MkdirAll(filepath.Dir(path), 0o755)
}

func (r Result) Bytes() ([]byte, error) {
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func (r Result) Write(path string) error {
	encoded, err := r.Bytes()
	if err != nil {
		return err
	}
	if directory := filepath.Dir(path); directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, encoded, 0o644)
}
