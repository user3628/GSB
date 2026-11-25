package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"edgeconfig/pkg/edgeconfig"
)

type Options struct {
	SamplesDir    string
	DBPath        string
	OutputPath    string
	ResetDB       bool
	FreeEndpoints bool
	ForceKill     bool
	SelfExe       string
}

type Check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Code   string `json:"code,omitempty"`
	Detail any    `json:"detail,omitempty"`
	Error  string `json:"error,omitempty"`
}

type ForceKillResult struct {
	Scenario      string                     `json:"scenario"`
	DBPath        string                     `json:"db_path"`
	ReadyLine     string                     `json:"ready_line"`
	PendingBefore int                        `json:"pending_before_recovery"`
	Recovery      *edgeconfig.RecoveryReport `json:"recovery"`
	OK            bool                       `json:"ok"`
	Error         string                     `json:"error,omitempty"`
}

type Report struct {
	OK            bool              `json:"ok"`
	GeneratedAtMS int64             `json:"generated_at_ms"`
	SamplesDir    string            `json:"samples_dir"`
	DBPath        string            `json:"db_path"`
	ControlURL    string            `json:"control_url"`
	Checks        []Check           `json:"checks"`
	ForceKill     []ForceKillResult `json:"force_kill_recovery"`
}

func Run(ctx context.Context, options Options) (*Report, error) {
	if options.SamplesDir == "" {
		options.SamplesDir = "samples"
	}
	if options.DBPath == "" {
		options.DBPath = filepath.Join(".runtime", "edgeconfig-verify.db")
	}
	if options.ResetDB {
		resetSQLiteFiles(options.DBPath)
	} else if err := os.MkdirAll(filepath.Dir(options.DBPath), 0o755); err != nil {
		return nil, err
	}

	samples, err := LoadSampleSet(options.SamplesDir)
	if err != nil {
		return nil, fmt.Errorf("load samples: %w", err)
	}

	store, err := edgeconfig.OpenStore(options.DBPath)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	service := edgeconfig.NewService(store)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	controlServer := &http.Server{Handler: edgeconfig.NewHTTPHandler(service)}
	go func() { _ = controlServer.Serve(listener) }()
	controlURL := "http://" + listener.Addr().String()
	client := newAPIClient(controlURL)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = controlServer.Shutdown(shutdownCtx)
	}()

	report := &Report{
		OK:            true,
		GeneratedAtMS: time.Now().UnixMilli(),
		SamplesDir:    options.SamplesDir,
		DBPath:        options.DBPath,
		ControlURL:    controlURL,
		Checks:        make([]Check, 0),
	}

	mocks, mockServers, err := startMocks(samples, options.FreeEndpoints)
	if err != nil {
		return nil, err
	}
	defer closeMocks(mockServers)

	// 1. Invalid batch must return a business code before any state is written.
	status, apiErr, err := client.post(ctx, "/v1/topology", samples.InvalidRaw, nil)
	if err != nil {
		return nil, err
	}
	invalidOK := apiErr != nil && status == 400 && (apiErr.Code == edgeconfig.CodeInvalidKeyPath ||
		apiErr.Code == edgeconfig.CodeDuplicateNode || apiErr.Code == edgeconfig.CodeUndeclaredKey ||
		apiErr.Code == edgeconfig.CodeValidation)
	report.add("invalid_topology_business_error_and_no_write", invalidOK && currentVersionIs(ctx, service, report, 0), apiErr)

	var topologyResult edgeconfig.TopologyResult
	_, apiErr, err = client.post(ctx, "/v1/topology", samples.Topology, &topologyResult)
	if err != nil || apiErr != nil {
		return nil, failureOr(err, apiErr, "save topology")
	}
	report.add("valid_topology_persisted", topologyResult.NodeCount == len(samples.Topology.Nodes) && topologyResult.KeyCount == len(samples.Topology.Keys), topologyResult)

	var snapshotResult edgeconfig.CommitResult
	_, apiErr, err = client.post(ctx, "/v1/snapshots", samples.Snapshot, &snapshotResult)
	if err != nil || apiErr != nil {
		return nil, failureOr(err, apiErr, "submit snapshot")
	}
	report.add("snapshot_v1_commit", snapshotResult.Version == 1 && snapshotResult.Digest == samples.Snapshot.Digest, snapshotResult)
	configureMocksFromViews(mocks, snapshotResult.Views, 1)

	// Stale snapshot version with a new commit id must be rejected.
	staleSnapshot := samples.Snapshot
	staleSnapshot.CommitID = "snapshot-v1-stale-duplicate-version"
	_, staleSnapErr, postErr := client.post(ctx, "/v1/snapshots", staleSnapshot, nil)
	report.add("stale_snapshot_code", postErr == nil && staleSnapErr != nil && staleSnapErr.Code == edgeconfig.CodeStaleSnapshot, staleSnapErr)

	var incrementResult edgeconfig.CommitResult
	_, apiErr, err = client.post(ctx, "/v1/increments", samples.Increment, &incrementResult)
	if err != nil || apiErr != nil {
		return nil, failureOr(err, apiErr, "submit increment")
	}
	affectedOK := incrementResult.Version == 2 && incrementResult.Affected != nil &&
		incrementResult.Affected.SelectedPath == samples.Observations.ExpectedPath &&
		sameIDs(affectedIDs(incrementResult.Affected), samples.Observations.ExpectedAffectedIDs)
	report.add("increment_affected_set_region_bucket", affectedOK, incrementResult.Affected)
	configureMocksFromViews(mocks, incrementResult.Views, 2)

	// Idempotent increment replay returns the same result instead of creating v3.
	var replayedIncrement edgeconfig.CommitResult
	_, replayAPIErr, replayErr := client.post(ctx, "/v1/increments", samples.Increment, &replayedIncrement)
	report.add("idempotent_increment_replay", replayErr == nil && replayAPIErr == nil && replayedIncrement.Version == 2 && replayedIncrement.CommitID == incrementResult.CommitID, replayedIncrement)

	staleIncrement := samples.Increment
	staleIncrement.CommitID = "increment-stale-base-new-id"
	_, staleBaseErr, postErr := client.post(ctx, "/v1/increments", staleIncrement, nil)
	report.add("stale_base_code", postErr == nil && staleBaseErr != nil && staleBaseErr.Code == edgeconfig.CodeStaleBase, staleBaseErr)

	// Fresh reports for non-probed nodes establish weighted convergence.
	for _, fresh := range samples.Observations.FreshReports {
		digest := resolveDigest(fresh.Digest, snapshotResult.Views, incrementResult.Views, fresh.NodeID)
		body := edgeconfig.Report{
			ReportID:        "report-" + fresh.NodeID,
			NodeID:          fresh.NodeID,
			AppliedDigest:   digest,
			EffectiveAtMS:   1764028803000,
			ObservedVersion: fresh.ObservedVersion,
		}
		var decision edgeconfig.ReportDecision
		_, reportAPIErr, postErr := client.post(ctx, "/v1/reports", body, &decision)
		report.add("fresh_report_converged_"+fresh.NodeID, postErr == nil && reportAPIErr == nil && decision.Converged, decision)
	}

	staleCase := samples.Observations.StaleRead
	staleDigest := resolveDigest(staleCase.Digest, snapshotResult.Views, incrementResult.Views, staleCase.NodeID)
	staleBody := edgeconfig.Report{
		ReportID:        "report-stale-" + staleCase.NodeID,
		NodeID:          staleCase.NodeID,
		AppliedDigest:   staleDigest,
		EffectiveAtMS:   1764028803100,
		ObservedVersion: staleCase.ObservedVersion,
	}
	_, staleReadAPIErr, postErr := client.post(ctx, "/v1/reports", staleBody, nil)
	staleReadOK := postErr == nil && staleReadAPIErr != nil && staleReadAPIErr.Code == edgeconfig.CodeStaleRead
	if staleReadOK {
		details, _ := staleReadAPIErr.Details.(map[string]any)
		staleReadOK = details != nil && details["expected_version"] != nil && details["current_view_version"] != nil
	}
	report.add("stale_read_rejected_with_versions", staleReadOK, staleReadAPIErr)

	// Configure one real rejection and one real hanging loopback endpoint.
	probeSample := samples.Observations.Probe
	if mock, ok := mocks[probeSample.RejectedNode]; ok {
		mock.SetRejection(probeSample.RejectStatus, probeSample.RejectCode)
	}
	if mock, ok := mocks[probeSample.TimeoutNode]; ok {
		mock.SetHang(time.Duration(probeSample.HangMS) * time.Millisecond)
	}
	faultRequest := edgeconfig.ProbeRequest{
		RoundID:     probeSample.RoundID,
		RequestID:   probeSample.RequestID,
		NodeIDs:     probeSample.Targets,
		TimeoutMS:   probeSample.TimeoutMS,
		Concurrency: probeSample.Concurrency,
	}
	var faultRound edgeconfig.ProbeRoundResult
	_, probeAPIErr, postErr := client.post(ctx, "/v1/probe-rounds", faultRequest, &faultRound)
	if postErr != nil || probeAPIErr != nil {
		return nil, failureOr(postErr, probeAPIErr, "fault probe round")
	}
	outcomeByNode := make(map[string]edgeconfig.ProbeOutcome, len(faultRound.Outcomes))
	for _, outcome := range faultRound.Outcomes {
		outcomeByNode[outcome.NodeID] = outcome
	}
	timeoutOutcome := outcomeByNode[probeSample.TimeoutNode]
	rejectOutcome := outcomeByNode[probeSample.RejectedNode]
	successOutcome := outcomeByNode[probeSample.SuccessNode]
	report.add("real_probe_success", successOutcome.Status == edgeconfig.StatusConverged && successOutcome.Converged, successOutcome)
	report.add("real_probe_business_rejection", rejectOutcome.Status == edgeconfig.StatusRejected && rejectOutcome.ErrorCode == edgeconfig.CodeProbeRejected && rejectOutcome.HTTPStatus == probeSample.RejectStatus, rejectOutcome)
	report.add("real_probe_timeout_code", timeoutOutcome.Status == edgeconfig.StatusTimeout && timeoutOutcome.ErrorCode == edgeconfig.CodeProbeTimeout && timeoutOutcome.TimedOut, timeoutOutcome)

	timeoutMock := mocks[probeSample.TimeoutNode]
	serverClosed := waitForCondition(time.Second, 10*time.Millisecond, func() bool {
		_, active, _ := timeoutMock.Stats()
		return active == 0
	})
	report.add("timeout_closes_upstream_handler_and_connection", serverClosed, map[string]any{"node_id": probeSample.TimeoutNode})

	// Recover endpoints and run an all-node healthy round.
	for nodeID, mock := range mocks {
		mock.SetRejection(0, "")
		mock.SetHang(0)
		if view, ok := viewByNode(incrementResult.Views, nodeID); ok {
			mock.SetState(view.ExpectedDigest, view.ExpectedVersion, 1764028804000)
		}
	}
	healthyRequest := edgeconfig.ProbeRequest{RoundID: "probe-round-healthy", RequestID: "probe-request-healthy", TimeoutMS: 1000, Concurrency: 4}
	var healthyRound edgeconfig.ProbeRoundResult
	_, healthyAPIErr, postErr := client.post(ctx, "/v1/probe-rounds", healthyRequest, &healthyRound)
	if postErr != nil || healthyAPIErr != nil {
		return nil, failureOr(postErr, healthyAPIErr, "healthy probe round")
	}
	report.add("all_nodes_probe_converged", healthyRound.ConvergedCount == len(samples.Topology.Nodes), map[string]any{
		"converged": healthyRound.ConvergedCount,
		"total":     len(samples.Topology.Nodes),
	})
	report.add("fixed_point_consistency_pass", healthyRound.Consistency != nil &&
		healthyRound.Consistency.ScoreUnits == samples.Policy.Numeric.Scale &&
		healthyRound.Consistency.Decision == "PASS" &&
		len(healthyRound.Consistency.Steps) == len(samples.Topology.Nodes), healthyRound.Consistency)

	var replayedHealthy edgeconfig.ProbeRoundResult
	_, replayProbeErr, postErr := client.post(ctx, "/v1/probe-rounds", healthyRequest, &replayedHealthy)
	report.add("idempotent_probe_replay", postErr == nil && replayProbeErr == nil && replayedHealthy.RoundID == healthyRound.RoundID, replayedHealthy)

	conflictingProbe := healthyRequest
	conflictingProbe.TimeoutMS = 999
	_, conflictAPIErr, postErr := client.post(ctx, "/v1/probe-rounds", conflictingProbe, nil)
	report.add("conflicting_request_id_rejected", postErr == nil && conflictAPIErr != nil && conflictAPIErr.Code == edgeconfig.CodeConflictRequest, conflictAPIErr)

	var affectedGET edgeconfig.AffectedReport
	if err := client.get(ctx, "/v1/affected/2", &affectedGET); err != nil {
		report.add("affected_report_persisted_get", false, err.Error())
	} else {
		report.add("affected_report_persisted_get", affectedGET.SelectedPath == samples.Observations.ExpectedPath, affectedGET)
	}

	if options.ForceKill {
		exe := options.SelfExe
		if exe == "" {
			exe, _ = os.Executable()
		}
		for _, scenario := range samples.Observations.Recovery.Scenarios {
			result := runForceKillCheck(ctx, exe, samples.Dir, options.DBPath, scenario)
			report.ForceKill = append(report.ForceKill, result)
			report.add("force_kill_recovery_"+scenario, result.OK, result)
		}
	}

	report.OK = true
	for _, check := range report.Checks {
		if !check.OK {
			report.OK = false
			break
		}
	}
	if options.OutputPath != "" {
		if err := writeReport(options.OutputPath, report); err != nil {
			return report, err
		}
	}
	return report, nil
}

func (r *Report) add(name string, ok bool, detail any) bool {
	check := Check{Name: name, OK: ok, Detail: detail}
	if apiErr, isAPI := detail.(*edgeconfig.APIError); !ok && isAPI && apiErr != nil {
		check.Code = apiErr.Code
		check.Error = apiErr.Message
	} else if err, isErr := detail.(error); !ok && isErr {
		check.Error = err.Error()
	}
	r.Checks = append(r.Checks, check)
	return ok
}

func startMocks(samples *SampleSet, freeEndpoints bool) (map[string]*edgeconfig.MockNode, []*edgeconfig.MockServer, error) {
	mocks := make(map[string]*edgeconfig.MockNode)
	servers := make([]*edgeconfig.MockServer, 0, len(samples.Topology.Nodes))
	for i, node := range samples.Topology.Nodes {
		mock := edgeconfig.NewMockNode(node.ID, "", 0)
		var server *edgeconfig.MockServer
		var err error
		if freeEndpoints {
			server, err = edgeconfig.StartFreeMockServer(mock)
			if err == nil {
				samples.Topology.Nodes[i].Endpoint = server.URL
			}
		} else {
			server, err = edgeconfig.StartMockAtEndpoint(node.Endpoint, mock)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("start mock for %s: %w", node.ID, err)
		}
		mocks[node.ID] = mock
		servers = append(servers, server)
	}
	return mocks, servers, nil
}

func closeMocks(servers []*edgeconfig.MockServer) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, server := range servers {
		_ = server.Close(ctx)
	}
}

func configureMocksFromViews(mocks map[string]*edgeconfig.MockNode, views []edgeconfig.ExpectedView, version int64) {
	for _, view := range views {
		if mock, ok := mocks[view.NodeID]; ok {
			mock.SetState(view.ExpectedDigest, view.ExpectedVersion, 1764028801000+version)
		}
	}
}

func viewByNode(views []edgeconfig.ExpectedView, nodeID string) (edgeconfig.ExpectedView, bool) {
	for _, view := range views {
		if view.NodeID == nodeID {
			return view, true
		}
	}
	return edgeconfig.ExpectedView{}, false
}

func resolveDigest(kind string, v1, v2 []edgeconfig.ExpectedView, nodeID string) string {
	views := v2
	if kind == "v1_expected" {
		views = v1
	}
	if view, ok := viewByNode(views, nodeID); ok {
		return view.ExpectedDigest
	}
	return ""
}

func affectedIDs(report *edgeconfig.AffectedReport) []string {
	ids := make([]string, 0, len(report.Nodes))
	for _, node := range report.Nodes {
		ids = append(ids, node.NodeID)
	}
	return ids
}

func sameIDs(actual, expected []string) bool {
	a := append([]string(nil), actual...)
	e := append([]string(nil), expected...)
	sort.Strings(a)
	sort.Strings(e)
	if len(a) != len(e) {
		return false
	}
	for i := range a {
		if a[i] != e[i] {
			return false
		}
	}
	return true
}

func currentVersionIs(ctx context.Context, service *edgeconfig.Service, report *Report, expected int64) bool {
	version, err := service.CurrentVersion(ctx)
	if err != nil {
		return false
	}
	return version == expected
}

func waitForCondition(timeout, interval time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(interval)
	}
	return condition()
}

func resetSQLiteFiles(path string) {
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(path + suffix)
	}
}

func writeReport(path string, report *Report) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

func failureOr(err error, apiErr *edgeconfig.APIError, context string) error {
	if apiErr != nil {
		return fmt.Errorf("%s: %s: %s", context, apiErr.Code, apiErr.Message)
	}
	if err != nil {
		return fmt.Errorf("%s: %w", context, err)
	}
	return fmt.Errorf("%s: unknown failure", context)
}
