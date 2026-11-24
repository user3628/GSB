package verify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"mitmpipeline/internal/codec"
	"mitmpipeline/internal/domain"
	"mitmpipeline/internal/httpapi"
	"mitmpipeline/internal/sample"
	"mitmpipeline/internal/service"
	"mitmpipeline/internal/store"
)

type Config struct {
	Addr       string
	DBDir      string
	CLIPath    string
	ReportPath string
}

type Check struct {
	Name    string `json:"name"`
	OK      bool   `json:"ok"`
	Detail  string `json:"detail,omitempty"`
	Context any    `json:"context,omitempty"`
}

type Scenario struct {
	Name           string   `json:"name"`
	OK             bool     `json:"ok"`
	HTTPStatus     int      `json:"http_status"`
	Code           string   `json:"code,omitempty"`
	ConnectionID   string   `json:"connection_id,omitempty"`
	Conclusion     string   `json:"conclusion,omitempty"`
	State          string   `json:"state,omitempty"`
	CommitVersion  int64    `json:"commit_version,omitempty"`
	PolicyVersions []string `json:"policy_versions,omitempty"`
	Expected       string   `json:"expected"`
	Detail         any      `json:"detail,omitempty"`
}

type RecoveryDiff struct {
	Subprocess         string                 `json:"subprocess"`
	KillObserved       bool                   `json:"kill_observed"`
	ExitCode           int                    `json:"exit_code"`
	PendingBefore      []domain.IntentRecord  `json:"pending_before_restart"`
	VisibleBefore      []string               `json:"visible_before_restart"`
	HTTPStatus         int                    `json:"recovery_http_status"`
	Report             *domain.RecoveryReport `json:"report_after_scan"`
	PendingAfter       []domain.IntentRecord  `json:"pending_after_restart"`
	VisibleAfter       []string               `json:"visible_after_restart"`
	LastCompleteCommit int64                  `json:"last_complete_commit"`
	ReimportScenario   *Scenario              `json:"reimport_scenario,omitempty"`
}

type Report struct {
	OK                    bool                             `json:"ok"`
	StartedAt             string                           `json:"started_at"`
	WorkDir               string                           `json:"work_dir"`
	HTTPBaseURL           string                           `json:"http_base_url"`
	DatabasePath          string                           `json:"database_path"`
	Checks                []Check                          `json:"checks"`
	Scenarios             []Scenario                       `json:"scenarios"`
	Recovery              RecoveryDiff                     `json:"recovery"`
	DegradationDetails    []domain.Degradation             `json:"degradation_details"`
	RoundingDetails       map[string][]domain.RoundingInfo `json:"rounding_details"`
	ConnectionConclusions map[string]*domain.Conclusion    `json:"connection_conclusions"`
	GeneratedFiles        map[string]string                `json:"generated_files"`
}

func Run(cfg Config) (*Report, error) {
	ctx := context.Background()
	started := time.Now().UTC().Format(time.RFC3339Nano)
	workDir := cfg.DBDir
	if workDir == "" {
		var err error
		workDir, err = os.MkdirTemp("", "mtlp-verify-")
		if err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(workDir, "pipeline.db")
	report := &Report{
		StartedAt: started, WorkDir: workDir, DatabasePath: dbPath,
		RoundingDetails:       map[string][]domain.RoundingInfo{},
		ConnectionConclusions: map[string]*domain.Conclusion{},
		GeneratedFiles:        map[string]string{},
	}

	fx := sample.NewFixture()
	files, err := writeSampleFiles(workDir, fx)
	if err != nil {
		return nil, err
	}
	report.GeneratedFiles = files

	recovery, err := runKillSubprocess(cfg.CLIPath, dbPath)
	if err != nil {
		return report, err
	}
	report.Recovery = recovery

	st, err := store.Open(ctx, dbPath)
	if err != nil {
		return report, err
	}
	defer st.Close()

	recovery.PendingBefore, err = st.PendingIntents(ctx)
	if err != nil {
		return report, err
	}
	recovery.VisibleBefore, err = st.ListConnectionIDs(ctx)
	if err != nil {
		return report, err
	}

	svc := service.New(st)
	apiServer := httpapi.New(svc)
	addr := cfg.Addr
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return report, err
	}
	baseURL := "http://" + listener.Addr().String()
	report.HTTPBaseURL = baseURL
	httpServer := &http.Server{Handler: apiServer.Handler()}
	go func() { _ = httpServer.Serve(listener) }()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	if err := waitHealthy(baseURL); err != nil {
		return report, err
	}

	status, recoveryEnvelope, _, err := doJSON(http.MethodGet, baseURL+"/api/v1/recovery", nil)
	if err != nil {
		return report, err
	}
	recovery.HTTPStatus = status
	if raw, ok := recoveryEnvelope["recovery"]; ok {
		var parsed domain.RecoveryReport
		if err := json.Unmarshal(raw, &parsed); err == nil {
			recovery.Report = &parsed
		}
	}
	recovery.PendingAfter, _ = st.PendingIntents(ctx)
	recovery.VisibleAfter, _ = st.ListConnectionIDs(ctx)
	recovery.LastCompleteCommit, _ = st.LatestCompleteVersion(ctx)
	report.Checks = append(report.Checks, Check{Name: "force_kill_recovery", OK: recovery.KillObserved && status == 200 && recovery.Report != nil && len(recovery.Report.AbortedIntents) == 1 && len(recovery.PendingAfter) == 0,
		Detail: "SIGKILL subprocess left one durable started intent; HTTP recovery aborted it without exposing crashed connection"})

	if err := verifyImports(baseURL, fx, report); err != nil {
		return report, err
	}
	if err := verifyHotAndPolicy(baseURL, fx, report); err != nil {
		return report, err
	}
	if err := verifySwitchEndpoint(baseURL, fx, report); err != nil {
		return report, err
	}
	if err := verifyFailureAtomicity(baseURL, st, fx, report); err != nil {
		return report, err
	}

	crashBin, err := codec.EncodeBinary([]domain.Record{fx.CrashCandidate}, 2)
	if err != nil {
		return report, err
	}
	scenario := postImportScenario(baseURL, "recovery_batch_reimport_after_abort", crashBin, "binary", domain.ConclusionMITM)
	report.Scenarios = append(report.Scenarios, scenario)
	recovery.ReimportScenario = &scenario
	if scenario.OK {
		ids, _ := st.ListConnectionIDs(ctx)
		recovery.VisibleAfter = ids
	}

	report.OK = true
	for _, check := range report.Checks {
		if !check.OK {
			report.OK = false
		}
	}
	for _, scenario := range report.Scenarios {
		if !scenario.OK {
			report.OK = false
		}
	}
	report.Recovery = recovery
	if cfg.ReportPath != "" {
		if err := writeReport(cfg.ReportPath, report); err != nil {
			return report, err
		}
	}
	return report, nil
}

func verifyImports(baseURL string, fx *sample.Fixture, report *Report) error {
	acceptedBin, err := codec.EncodeBinary([]domain.Record{fx.Accepted}, 2)
	if err != nil {
		return err
	}
	accepted := postImportScenario(baseURL, "accept_current_binary", acceptedBin, domain.FormatBinary, domain.ConclusionTrusted)
	report.Scenarios = append(report.Scenarios, accepted)
	addConclusion(report, accepted)

	jsonAccepted, err := codec.EncodeJSONL([]domain.Record{fx.JSONAccepted})
	if err != nil {
		return err
	}
	jsonScenario := postImportScenario(baseURL, "accept_current_jsonl", jsonAccepted, domain.FormatJSONL, domain.ConclusionTrusted)
	report.Scenarios = append(report.Scenarios, jsonScenario)
	addConclusion(report, jsonScenario)

	aliasData, err := codec.EncodeJSONL([]domain.Record{fx.IdentityAlias})
	if err != nil {
		return err
	}
	alias := postImportScenario(baseURL, "identity_case_normalization_merge", aliasData, domain.FormatJSONL, domain.ConclusionTrusted)
	report.Scenarios = append(report.Scenarios, alias)
	addConclusion(report, alias)
	getStatus, _, _, getErr := doJSON(http.MethodGet, baseURL+"/api/v1/connections/accept-20251124-0001", nil)
	if getErr != nil {
		return getErr
	}
	report.Checks = append(report.Checks, Check{Name: "identity_normalization", OK: alias.OK, Detail: "lowercase connection id, uppercase SNI root dot, and hex serial normalize to same entity"})
	report.Checks = append(report.Checks, Check{Name: "normalized_http_identity_lookup", OK: getStatus == http.StatusOK, Detail: "lowercase path ID resolves to canonical connection entity"})

	degradedData, err := codec.EncodeJSONL([]domain.Record{fx.Degraded})
	if err != nil {
		return err
	}
	degraded := postImportScenario(baseURL, "degraded_accept_legacy_jsonl_v1", degradedData, domain.FormatJSONL, domain.ConclusionTrusted)
	if degraded.Detail != nil {
		if result, ok := degraded.Detail.(*service.ImportResult); ok {
			degraded.OK = degraded.HTTPStatus == 200 && result.Negotiation.Action == domain.ActionDegraded && len(result.Negotiation.Degradations) >= 2
			report.DegradationDetails = append(report.DegradationDetails, result.Negotiation.Degradations...)
		}
	}
	report.Scenarios = append(report.Scenarios, degraded)
	addConclusion(report, degraded)
	report.Checks = append(report.Checks, Check{Name: "degradation_matrix", OK: degraded.OK, Detail: "legacy v1 accepted with default protocol and low precision listed"})

	legacyBinary, err := codec.EncodeBinary([]domain.Record{fx.Accepted}, 1)
	if err != nil {
		return err
	}
	legacy := postImportScenario(baseURL, "degraded_accept_binary_v1", legacyBinary, domain.FormatBinary, domain.ConclusionTrusted)
	if result, ok := legacy.Detail.(*service.ImportResult); ok {
		legacy.OK = legacy.HTTPStatus == 200 && result.Negotiation.Action == domain.ActionDegraded && len(result.Negotiation.Degradations) >= 2
		report.DegradationDetails = append(report.DegradationDetails, result.Negotiation.Degradations...)
	}
	report.Scenarios = append(report.Scenarios, legacy)

	replayStatus, replayEnvelope, replayRaw, err := doJSON(http.MethodPost, baseURL+"/api/v1/connections/"+fx.Accepted.ConnectionID+"/replay?rounding=truncate", map[string]string{"rounding_rule": "truncate"})
	if err != nil {
		return err
	}
	replayScenario := Scenario{Name: "direct_connection_replay_endpoint", HTTPStatus: replayStatus, Expected: domain.ConclusionTrusted, OK: replayStatus == 200}
	if rawConclusion, ok := replayEnvelope["conclusion"]; ok {
		var conclusion domain.Conclusion
		if json.Unmarshal(rawConclusion, &conclusion) == nil {
			replayScenario.ConnectionID = conclusion.CanonicalConnectionID
			replayScenario.Conclusion = conclusion.Conclusion
			replayScenario.OK = replayScenario.OK && conclusion.Conclusion == domain.ConclusionTrusted
			report.RoundingDetails[conclusion.CanonicalConnectionID+"#replay"] = conclusion.Rounding
		}
	} else if apiErr := decodeAPIError(replayRaw); apiErr != nil {
		replayScenario.Code = apiErr.Code
		replayScenario.Detail = apiErr
		replayScenario.OK = false
	}
	report.Scenarios = append(report.Scenarios, replayScenario)
	report.Checks = append(report.Checks, Check{Name: "replay_api", OK: replayScenario.OK, Detail: "POST /connections/{id}/replay re-read durable record and wrote a complete conclusion version"})

	abortedBin, err := codec.EncodeBinary([]domain.Record{fx.Aborted}, 2)
	if err != nil {
		return err
	}
	aborted := postImportScenario(baseURL, "abnormal_termination_not_trusted", abortedBin, domain.FormatBinary, domain.ConclusionInsufficient)
	report.Scenarios = append(report.Scenarios, aborted)
	addConclusion(report, aborted)

	chainBin, err := codec.EncodeBinary([]domain.Record{fx.ChainInvalid}, 2)
	if err != nil {
		return err
	}
	chainInvalid := postImportScenario(baseURL, "missing_root_is_chain_invalid_not_mitm", chainBin, domain.FormatBinary, domain.ConclusionInsufficient)
	if result, ok := chainInvalid.Detail.(*service.ImportResult); ok && len(result.Conclusions) == 1 {
		chainInvalid.OK = chainInvalid.OK && result.Conclusions[0].ReasonCode == domain.CodeChainInvalid
	}
	report.Scenarios = append(report.Scenarios, chainInvalid)
	addConclusion(report, chainInvalid)

	chainReplayStatus, _, chainReplayRaw, err := doJSON(http.MethodPost, baseURL+"/api/v1/connections/CHAIN-MISSING-ROOT-0001/replay", map[string]string{"rounding_rule": "truncate"})
	if err != nil {
		return err
	}
	chainAPIErr := decodeAPIError(chainReplayRaw)
	report.Checks = append(report.Checks, Check{
		Name:   "chain_invalid_error_code",
		OK:     chainReplayStatus == http.StatusUnprocessableEntity && chainAPIErr != nil && chainAPIErr.Code == domain.CodeChainInvalid,
		Detail: "missing root replay returns E_CHAIN_INVALID with persisted insufficient_evidence conclusion",
	})

	mitmBin, err := codec.EncodeBinary([]domain.Record{fx.MITM}, 2)
	if err != nil {
		return err
	}
	mitm := postImportScenario(baseURL, "untrusted_root_pin_miss_is_mitm", mitmBin, domain.FormatBinary, domain.ConclusionMITM)
	report.Scenarios = append(report.Scenarios, mitm)
	addConclusion(report, mitm)
	return nil
}

func verifyHotAndPolicy(baseURL string, fx *sample.Fixture, report *Report) error {
	hotBin, err := codec.EncodeBinary([]domain.Record{fx.HotUpdate}, 2)
	if err != nil {
		return err
	}
	hot := postImportScenario(baseURL, "hot_update_event_level_binding_mixed", hotBin, domain.FormatBinary, domain.ConclusionMITM)
	if result, ok := hot.Detail.(*service.ImportResult); ok && len(result.Conclusions) == 1 {
		c := result.Conclusions[0]
		hot.OK = hot.HTTPStatus == 200 && c.Conclusion == domain.ConclusionMITM &&
			len(c.EventJudgments) >= 5 && judgmentResult(c, 3) == domain.ConclusionTrusted &&
			judgmentPolicy(c, 3) == "pol-hot-v1" && judgmentResult(c, 4) == domain.ConclusionMITM &&
			judgmentPolicy(c, 4) == "pol-hot-v2"
		report.RoundingDetails[c.CanonicalConnectionID] = c.Rounding
	}
	report.Scenarios = append(report.Scenarios, hot)
	addConclusion(report, hot)
	report.Checks = append(report.Checks, Check{Name: "event_level_policy_binding", OK: hot.OK, Detail: "seq3 trusted under v1; seq4 MITM under replacement v2; final result not backfilled"})

	status, envelope, _, err := doJSON(http.MethodGet, baseURL+"/api/v1/policy-versions/pol-hot-v1/conclusions", nil)
	if err != nil {
		return err
	}
	code, context := errorCodeContext(envelope)
	retiredOK := status == http.StatusConflict && code == domain.CodeVersionRetired
	report.Checks = append(report.Checks, Check{Name: "retired_version_query", OK: retiredOK, Detail: "old policy retrospective query returns E_VERSION_RETIRED with current range", Context: context})

	status2, envelope2, _, err := doJSON(http.MethodGet, baseURL+"/api/v1/policy-versions/pol-hot-v2/conclusions", nil)
	if err != nil {
		return err
	}
	activeOK := status2 == 200 && envelope2["conclusions"] != nil
	report.Checks = append(report.Checks, Check{Name: "active_version_query", OK: activeOK})
	_ = status
	return nil
}

func verifySwitchEndpoint(baseURL string, fx *sample.Fixture, report *Report) error {
	switchRecord := fx.Accepted
	switchRecord.ConnectionID = "switch-runtime-0001"
	switchBin, err := codec.EncodeBinary([]domain.Record{switchRecord}, 2)
	if err != nil {
		return err
	}
	initial := postImportScenario(baseURL, "switch_endpoint_initial_trusted", switchBin, domain.FormatBinary, domain.ConclusionTrusted)
	report.Scenarios = append(report.Scenarios, initial)

	zero := "sha256:" + base64Zero()
	body := map[string]any{
		"policy": map[string]any{
			"version": "pol-runtime-v2", "mode": domain.PolicyModeRoot,
			"trusted_roots": []string{zero},
		},
		"connection_id":   "SWITCH-RUNTIME-0001",
		"effective_at_ms": sample.FixedTime.UnixMilli() + 24,
		"retire_version":  "pol-accepted-v1",
		"replay":          true,
	}
	status, envelope, raw, err := doJSON(http.MethodPost, baseURL+"/api/v1/policy-versions/switch", body)
	if err != nil {
		return err
	}
	scenario := Scenario{Name: "http_policy_switch_and_replay", HTTPStatus: status, Expected: domain.ConclusionMITM, OK: status == 200}
	if resultRaw, ok := envelope["result"]; ok {
		var result service.SwitchPolicyResult
		if err := json.Unmarshal(resultRaw, &result); err == nil && result.Conclusion != nil {
			scenario.ConnectionID = result.Conclusion.CanonicalConnectionID
			scenario.Conclusion = result.Conclusion.Conclusion
			scenario.PolicyVersions = result.Conclusion.PolicyVersions
			scenario.OK = scenario.OK && result.Conclusion.Conclusion == domain.ConclusionMITM
			scenario.Detail = result
			report.ConnectionConclusions[result.Conclusion.CanonicalConnectionID] = result.Conclusion
			report.RoundingDetails[result.Conclusion.CanonicalConnectionID] = result.Conclusion.Rounding
		}
	} else if apiErr := decodeAPIError(raw); apiErr != nil {
		scenario.Code = apiErr.Code
		scenario.OK = false
	}
	report.Scenarios = append(report.Scenarios, scenario)
	report.Checks = append(report.Checks, Check{Name: "policy_switch_api", OK: scenario.OK, Detail: "runtime whole-policy replacement switches judgment on replay"})
	return nil
}

func verifyFailureAtomicity(baseURL string, st *store.Store, fx *sample.Fixture, report *Report) error {
	ctx := context.Background()
	beforeCommit, _ := st.LatestCompleteVersion(ctx)
	beforeIDs, _ := st.ListConnectionIDs(ctx)

	unsupported := postFailureScenario(baseURL, "reject_future_format_unknown_required", fx.UnsupportedJSON, domain.FormatJSONL, domain.CodeFormatUnsupported)
	report.Scenarios = append(report.Scenarios, unsupported)

	corrupt := postFailureScenario(baseURL, "reject_corrupt_crc_record_index", fx.CorruptBinary, domain.FormatBinary, domain.CodeRecordCorrupt)
	report.Scenarios = append(report.Scenarios, corrupt)

	conflictRecord := fx.IdentityAlias
	conflictRecord.SNI = "conflicting-identity.internal"
	conflictData, err := codec.EncodeJSONL([]domain.Record{conflictRecord})
	if err != nil {
		return err
	}
	identityConflict := postFailureScenario(baseURL, "reject_normalized_identity_conflict", conflictData, domain.FormatJSONL, domain.CodeIdentityConflict)
	report.Scenarios = append(report.Scenarios, identityConflict)

	afterCommit, _ := st.LatestCompleteVersion(ctx)
	afterIDs, _ := st.ListConnectionIDs(ctx)
	atomicOK := unsupported.OK && corrupt.OK && identityConflict.OK && beforeCommit == afterCommit && sameStrings(beforeIDs, afterIDs)
	report.Checks = append(report.Checks, Check{Name: "whole_batch_atomicity", OK: atomicOK,
		Detail:  "failed format/corrupt/identity imports did not advance complete commit or change visible connections",
		Context: map[string]any{"before_commit": beforeCommit, "after_commit": afterCommit, "before_ids": beforeIDs, "after_ids": afterIDs}})
	return nil
}

func runKillSubprocess(cliPath, dbPath string) (RecoveryDiff, error) {
	result := RecoveryDiff{Subprocess: cliPath}
	if cliPath == "" {
		return result, fmt.Errorf("CLI path is required for forced kill verification")
	}
	cmd := exec.Command(cliPath, "crash-import", "--db", dbPath)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return result, fmt.Errorf("crash subprocess unexpectedly completed: %s", output)
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		result.ExitCode = exitErr.ExitCode()
		result.KillObserved = killedBySigkill(exitErr)
	}
	if !result.KillObserved {
		return result, fmt.Errorf("crash subprocess was not SIGKILLed: %v: %s", err, output)
	}
	return result, nil
}

func writeSampleFiles(dir string, fx *sample.Fixture) (map[string]string, error) {
	acceptedBin, _ := codec.EncodeBinary([]domain.Record{fx.Accepted, fx.MITM, fx.HotUpdate, fx.Aborted}, 2)
	acceptedJSON, _ := codec.EncodeJSONL([]domain.Record{fx.JSONAccepted, fx.Degraded})
	legacyBin, _ := codec.EncodeBinary([]domain.Record{fx.Accepted}, 1)
	files := map[string][]byte{
		"current-batch.bin":        acceptedBin,
		"current-and-legacy.jsonl": acceptedJSON,
		"legacy-v1.bin":            legacyBin,
		"unsupported-v99.jsonl":    fx.UnsupportedJSON,
		"corrupt.bin":              fx.CorruptBinary,
		"good-chain.pem":           []byte(fx.Good.ChainPEM),
		"attacker-chain.pem":       []byte(fx.Attacker.ChainPEM),
	}
	paths := map[string]string{}
	for name, data := range files {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return nil, err
		}
		paths[name] = path
	}
	return paths, nil
}

func waitHealthy(baseURL string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	for i := 0; i < 50; i++ {
		resp, err := client.Get(baseURL + "/healthz")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	return fmt.Errorf("HTTP server did not become healthy at %s", baseURL)
}

func doJSON(method, url string, body any) (int, map[string]json.RawMessage, []byte, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, nil, nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return 0, nil, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, raw, err
	}
	envelope := map[string]json.RawMessage{}
	_ = json.Unmarshal(raw, &envelope)
	return resp.StatusCode, envelope, raw, nil
}

func postImportScenario(baseURL, name string, data []byte, format, expected string) Scenario {
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/api/v1/import?batch_id=verify-"+name+"&rounding=truncate", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Scenario{Name: name, Expected: expected, OK: false, Code: "E_HTTP", Detail: err.Error()}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	scenario := Scenario{Name: name, Expected: expected, HTTPStatus: resp.StatusCode}
	var envelope struct {
		Result *service.ImportResult `json:"result"`
		Error  *domain.Error         `json:"error"`
	}
	_ = json.Unmarshal(raw, &envelope)
	if envelope.Error != nil {
		scenario.Code = envelope.Error.Code
		scenario.Detail = envelope.Error
		return scenario
	}
	if envelope.Result == nil || len(envelope.Result.Conclusions) != 1 {
		scenario.Code = "E_NO_RESULT"
		scenario.Detail = string(raw)
		return scenario
	}
	result := envelope.Result
	c := result.Conclusions[0]
	scenario.Detail = result
	scenario.CommitVersion = result.CommitVersion
	scenario.ConnectionID = c.CanonicalConnectionID
	scenario.Conclusion = c.Conclusion
	scenario.State = c.State
	scenario.PolicyVersions = c.PolicyVersions
	scenario.OK = resp.StatusCode == 200 && c.Conclusion == expected
	if format == domain.FormatBinary && result.Negotiation.Format == "" {
		scenario.OK = false
	}
	return scenario
}

func postFailureScenario(baseURL, name string, data []byte, format, expectedCode string) Scenario {
	scenario := Scenario{Name: name, Expected: expectedCode}
	status, _, raw, err := doRawPost(baseURL+"/api/v1/import?batch_id=verify-fail-"+name, data)
	if err != nil {
		scenario.Code = "E_HTTP"
		scenario.Detail = err.Error()
		return scenario
	}
	scenario.HTTPStatus = status
	apiErr := decodeAPIError(raw)
	if apiErr != nil {
		scenario.Code = apiErr.Code
		scenario.Detail = apiErr
		scenario.OK = status >= 400 && apiErr.Code == expectedCode
	}
	return scenario
}

func doRawPost(url string, data []byte) (int, map[string]json.RawMessage, []byte, error) {
	resp, err := http.Post(url, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	envelope := map[string]json.RawMessage{}
	_ = json.Unmarshal(raw, &envelope)
	return resp.StatusCode, envelope, raw, nil
}

func decodeAPIError(raw []byte) *domain.Error {
	var envelope struct {
		Error *domain.Error `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil
	}
	return envelope.Error
}

func errorCodeContext(envelope map[string]json.RawMessage) (string, any) {
	rawError, ok := envelope["error"]
	if !ok {
		return "", nil
	}
	var apiErr domain.Error
	if err := json.Unmarshal(rawError, &apiErr); err != nil {
		return "", nil
	}
	return apiErr.Code, apiErr.Context
}

func addConclusion(report *Report, scenario Scenario) {
	if result, ok := scenario.Detail.(*service.ImportResult); ok {
		for _, c := range result.Conclusions {
			report.ConnectionConclusions[c.CanonicalConnectionID] = c
			if len(c.Rounding) > 0 {
				report.RoundingDetails[c.CanonicalConnectionID] = c.Rounding
			}
		}
	}
}

func judgmentResult(c *domain.Conclusion, seq int) string {
	for _, j := range c.EventJudgments {
		if j.EventSeq == seq {
			return j.Result
		}
	}
	return ""
}

func judgmentPolicy(c *domain.Conclusion, seq int) string {
	for _, j := range c.EventJudgments {
		if j.EventSeq == seq {
			return j.PolicyVersion
		}
	}
	return ""
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func writeReport(path string, report *Report) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func base64Zero() string {
	return "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
}
