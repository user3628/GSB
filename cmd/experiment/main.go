package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"example.com/joinshuffle/internal/model"
	"example.com/joinshuffle/internal/sample"
	"example.com/joinshuffle/internal/snapshot"
)

type apiClient struct {
	base   string
	client *http.Client
}

type apiError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context"`
}

type jobEnvelope struct {
	OK    bool                     `json:"ok"`
	Job   *model.AggregateResponse `json:"job"`
	Error *apiError                `json:"error"`
}

type dataEnvelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data"`
	Error *apiError       `json:"error"`
}

type reportEnvelope struct {
	OK     bool                  `json:"ok"`
	Report *model.RecoveryReport `json:"report"`
	Error  *apiError             `json:"error"`
}

type snapshotEnvelope struct {
	OK        bool                   `json:"ok"`
	Snapshots model.SnapshotPairMeta `json:"snapshots"`
	Error     *apiError              `json:"error"`
}

type statusEnvelope struct {
	OK        bool `json:"ok"`
	Snapshots struct {
		Loaded    bool                   `json:"loaded"`
		Snapshots model.SnapshotPairMeta `json:"snapshots"`
	} `json:"snapshots"`
	Error *apiError `json:"error"`
}

type serverProcess struct {
	cmd     *exec.Cmd
	logFile *os.File
	logPath string
}

type algorithmSummary struct {
	Algorithm           string  `json:"algorithm"`
	Path                string  `json:"path"`
	P1CommitVersion     int64   `json:"p1_commit_version"`
	P4CommitVersion     int64   `json:"p4_commit_version"`
	MaxSingleVsMultiDev float64 `json:"max_single_vs_multi_relative_deviation"`
	MaxDifferenceVsDD   float64 `json:"max_relative_difference_vs_double_double"`
	MaxDeclaredRelBound float64 `json:"max_declared_relative_error_bound"`
}

func main() {
	workdirFlag := flag.String("workdir", "", "experiment directory (default: fresh temp directory)")
	clean := flag.Bool("clean", false, "remove the supplied workdir before running")
	leftRows := flag.Int("left-rows", 2048, "deterministic left sample rows")
	rightRows := flag.Int("right-rows", 1536, "deterministic right sample rows")
	flag.Parse()

	projectRoot, err := findProjectRoot()
	must(err)
	workdir := *workdirFlag
	if workdir == "" {
		dir, err := os.MkdirTemp("", "joinshuffle-experiment-*")
		must(err)
		workdir = dir
	} else {
		abs, err := filepath.Abs(workdir)
		must(err)
		workdir = abs
		if *clean {
			if workdir == projectRoot || strings.HasPrefix(projectRoot, workdir+string(filepath.Separator)) {
				fatalf("refusing to clean workdir %s because it contains the project root %s", workdir, projectRoot)
			}
			must(os.RemoveAll(workdir))
		}
	}
	must(os.MkdirAll(workdir, 0o755))

	logf := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "[experiment] "+format+"\n", args...)
	}
	logf("workdir: %s", workdir)

	left, right, err := sample.GeneratePair(20260101, *leftRows, *rightRows, 1, 1, sample.DefaultGeneratedAt)
	must(err)
	firstLeft, err := snapshot.Marshal(left)
	must(err)
	secondLeft, _, err := sample.GeneratePair(20260101, *leftRows, *rightRows, 1, 1, sample.DefaultGeneratedAt)
	must(err)
	repeatedLeft, err := snapshot.Marshal(secondLeft)
	must(err)
	if !bytes.Equal(firstLeft, repeatedLeft) {
		fatalf("sample generation is not bitwise deterministic")
	}
	dataDir := filepath.Join(workdir, "data")
	must(os.MkdirAll(dataDir, 0o755))
	leftPath := filepath.Join(dataDir, "left.snapshot")
	rightPath := filepath.Join(dataDir, "right.snapshot")
	leftMeta, err := snapshot.WriteFile(leftPath, left)
	must(err)
	rightMeta, err := snapshot.WriteFile(rightPath, right)
	must(err)
	logf("samples: left=%s rows=%d right=%s rows=%d", leftMeta.SHA256[:12], leftMeta.Rows, rightMeta.SHA256[:12], rightMeta.Rows)

	binName := "join-shuffle-server"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	binPath := filepath.Join(workdir, binName)
	logf("building server")
	build := exec.Command("go", "build", "-o", binPath, "./cmd/server")
	build.Dir = projectRoot
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	must(build.Run())

	addr, err := freeAddress()
	must(err)
	dbPath := filepath.Join(workdir, "engine.db")
	serverLog := filepath.Join(workdir, "server.log")
	server, err := startServer(binPath, addr, dbPath, serverLog, nil, logf)
	must(err)
	defer func() {
		if server != nil {
			stopServer(server)
		}
	}()
	api := &apiClient{base: "http://" + addr, client: &http.Client{Timeout: 30 * time.Second}}

	loaded, err := api.loadSnapshots(leftPath, rightPath)
	must(err)
	if loaded.Left.SHA256 != leftMeta.SHA256 || loaded.Right.SHA256 != rightMeta.SHA256 {
		fatalf("loaded snapshot checksums do not match generated files")
	}
	logf("snapshots loaded")

	baseRequest := model.AggregateRequest{
		JoinType:          "left",
		PartitionCount:    4,
		SkewThreshold:     0.35,
		FixedScaleBits:    80,
		RelativeTolerance: 1e-12,
	}
	jobs := make(map[string]*model.AggregateResponse)
	algorithms := []string{"double_double_fixed", "naive", "kahan"}
	for _, algorithm := range algorithms {
		for _, partitions := range []int{1, 4} {
			request := baseRequest
			request.RequestID = fmt.Sprintf("exp-%s-p%d", algorithm, partitions)
			request.Algorithm = algorithm
			request.PartitionCount = partitions
			if algorithm == "naive" {
				request.RelativeTolerance = 1e-3
			} else if algorithm == "kahan" {
				request.RelativeTolerance = 1e-5
			}
			response, err := api.submit(request)
			must(err)
			if response.Route.ResolvedAlgorithm != algorithm {
				fatalf("algorithm switched silently: requested=%s resolved=%s", algorithm, response.Route.ResolvedAlgorithm)
			}
			jobs[request.RequestID] = response
		}
	}
	ddReference := jobs["exp-double_double_fixed-p4"]
	summaries := make([]algorithmSummary, 0, len(algorithms))
	for _, algorithm := range algorithms {
		p1 := jobs[fmt.Sprintf("exp-%s-p1", algorithm)]
		p4 := jobs[fmt.Sprintf("exp-%s-p4", algorithm)]
		maxP1P4 := maxGroupRelativeDifference(p1, p4)
		maxVsDD := maxGroupRelativeDifference(p4, ddReference)
		summaries = append(summaries, algorithmSummary{
			Algorithm:           algorithm,
			Path:                p4.Route.Path,
			P1CommitVersion:     p1.CommitVersion,
			P4CommitVersion:     p4.CommitVersion,
			MaxSingleVsMultiDev: maxP1P4,
			MaxDifferenceVsDD:   maxVsDD,
			MaxDeclaredRelBound: p4.ErrorDeclaration.MaxRelativeBound,
		})
		if maxP1P4 > p4.PartitionInvariance.Tolerance {
			fatalf("%s exceeds its partition tolerance: %g > %g", algorithm, maxP1P4, p4.PartitionInvariance.Tolerance)
		}
		if !p4.PartitionInvariance.Passed {
			fatalf("%s failed internal partition invariance verification", algorithm)
		}
	}
	logf("three sum algorithms and partition comparisons completed")

	autoRequest := baseRequest
	autoRequest.RequestID = "exp-auto"
	autoRequest.Algorithm = "auto"
	autoResponse, err := api.submit(autoRequest)
	must(err)
	if autoResponse.Route.Path != string(model.PathHigh) {
		fatalf("auto route unexpectedly chose %s", autoResponse.Route.Path)
	}
	logf("auto route: %s -> %s", autoResponse.Route.ResolvedAlgorithm, autoResponse.Route.Path)

	ddRequest := baseRequest
	ddRequest.Algorithm = "double_double_fixed"
	ddRequest.RequestID = "exp-double_double_fixed-p4"
	duplicate, err := api.submit(ddRequest)
	must(err)
	if duplicate.CommitVersion != ddReference.CommitVersion || duplicate.ComputedAt != ddReference.ComputedAt {
		fatalf("duplicate request was re-executed instead of reusing its stored result")
	}
	conflicting := ddRequest
	conflicting.Algorithm = "naive"
	business, err := api.submitExpectError(conflicting)
	if err != nil {
		fatalf("conflict probe failed: %v", err)
	}
	if business.Code != model.CodeConflictingRequest {
		fatalf("expected E_CONFLICTING_REQUEST, got %s", business.Code)
	}
	logf("idempotency and E_CONFLICTING_REQUEST verified")

	corruptLeft := filepath.Join(dataDir, "left-corrupt.snapshot")
	corruptBytes, err := os.ReadFile(leftPath)
	must(err)
	corruptBytes[len(corruptBytes)/2] ^= 0x80
	must(os.WriteFile(corruptLeft, corruptBytes, 0o644))
	must(os.WriteFile(corruptLeft+".sha256", []byte(strings.Repeat("0", 64)+"  left-corrupt.snapshot\n"), 0o644))
	snapshotErr, err := api.loadSnapshotsExpectError(corruptLeft, rightPath)
	if err != nil {
		fatalf("snapshot failure probe failed: %v", err)
	}
	if snapshotErr.Code != model.CodeSnapshotInvalid {
		fatalf("expected E_SNAPSHOT_INVALID, got %s", snapshotErr.Code)
	}
	status, err := api.snapshotStatus()
	must(err)
	if !status.Snapshots.Loaded || status.Snapshots.Snapshots.Left.Version != 1 {
		fatalf("invalid snapshot load disturbed the active pair")
	}
	logf("E_SNAPSHOT_INVALID and old-snapshot preservation verified")

	partitions, err := api.getPartitions("exp-double_double_fixed-p4")
	must(err)
	if len(partitions) != 4 {
		fatalf("expected four persisted partition rows, got %d", len(partitions))
	}
	results, err := api.getResults("exp-double_double_fixed-p4")
	must(err)
	if asInt(results["partial_sums_persisted"]) != len(ddReference.Groups) {
		fatalf("persisted partial sums are inconsistent with committed groups")
	}
	persistedPartials, err := api.getPartialSums("exp-double_double_fixed-p4")
	must(err)
	if len(persistedPartials) != asInt(results["partial_sums_persisted"]) {
		fatalf("partial-sums endpoint disagrees with results query")
	}

	crashRequest := baseRequest
	crashRequest.RequestID = "exp-crash-recover"
	crashRequest.Algorithm = "double_double_fixed"
	marker := filepath.Join(workdir, "crash.marker")
	_ = os.Remove(marker)
	logf("stopping server to arm one-shot crash injection")
	stopServer(server)
	server = nil
	crashEnv := map[string]string{
		"SHUFFLE_ENABLE_CRASH_INJECTION": "1",
		"SHUFFLE_CRASH_REQUEST_ID":       crashRequest.RequestID,
		"SHUFFLE_CRASH_MARKER":           marker,
	}
	server, err = startServer(binPath, addr, dbPath, serverLog, crashEnv, logf)
	must(err)
	api = &apiClient{base: "http://" + addr, client: &http.Client{Timeout: 10 * time.Second}}
	_, crashErr := api.submit(crashRequest)
	if crashErr == nil {
		fatalf("crash request unexpectedly completed")
	}
	if err := server.cmd.Wait(); err == nil {
		fatalf("server was expected to exit via deterministic crash injection")
	}
	_ = server.logFile.Close()
	server = nil
	if _, err := os.Stat(marker); err != nil {
		fatalf("crash marker was not written: %v", err)
	}
	logf("server forcibly terminated after durable intent")

	server, err = startServer(binPath, addr, dbPath, serverLog, nil, logf)
	must(err)
	api = &apiClient{base: "http://" + addr, client: &http.Client{Timeout: 30 * time.Second}}
	report, err := api.recoveryReport()
	must(err)
	if report.PendingIntentsRemaining != 0 {
		fatalf("recovery left pending intents: %+v", report)
	}
	foundAbortedRequest := false
	for _, requestID := range report.AbortedRequests {
		if requestID == crashRequest.RequestID {
			foundAbortedRequest = true
		}
	}
	if !foundAbortedRequest {
		fatalf("recovery report did not reconstruct the persisted aborted request")
	}
	foundEvent := false
	for _, event := range report.Events {
		if event.RequestID == crashRequest.RequestID && event.Action == "aborted_uncommitted_attempt" {
			foundEvent = true
		}
	}
	if !foundEvent {
		fatalf("recovery report does not contain the persisted aborted intent")
	}
	abortedErr, err := api.getJobExpectError(crashRequest.RequestID)
	if err != nil {
		fatalf("aborted job query failed: %v", err)
	}
	if abortedErr.Code != model.CodeExecutionAborted {
		fatalf("expected E_EXECUTION_ABORTED, got %s", abortedErr.Code)
	}
	resumed, err := api.submit(crashRequest)
	must(err)
	if resumed.Attempt != 2 || resumed.Status != "completed" {
		fatalf("retry did not resume as attempt 2: attempt=%d status=%s", resumed.Attempt, resumed.Status)
	}
	if !groupSumsBitwiseEqual(resumed, ddReference) {
		fatalf("recovered result is not bitwise identical to the committed double-double reference")
	}
	logf("recovery scan, E_EXECUTION_ABORTED, and resumed result verified")

	reportPayload := map[string]any{
		"ok":                       true,
		"workdir":                  workdir,
		"samples":                  map[string]any{"left": leftMeta, "right": rightMeta, "bitwise_deterministic": true},
		"algorithms":               summaries,
		"auto_route":               autoResponse.Route,
		"skew":                     ddReference.Skew,
		"partition_invariance":     ddReference.PartitionInvariance,
		"partition_stats":          partitions,
		"commit_versions":          map[string]int64{"baseline": ddReference.CommitVersion, "auto": autoResponse.CommitVersion, "resumed": resumed.CommitVersion},
		"conflict_code":            business.Code,
		"snapshot_invalid_code":    snapshotErr.Code,
		"aborted_code":             abortedErr.Code,
		"recovery_report":          report,
		"recovered_result_matches": true,
		"stored_partial_count":     results["partial_sums_persisted"],
		"final_server_listening":   addr,
	}
	reportBytes, err := json.MarshalIndent(reportPayload, "", "  ")
	must(err)
	reportPath := filepath.Join(workdir, "report.json")
	must(os.WriteFile(reportPath, append(reportBytes, '\n'), 0o644))
	fmt.Println(string(reportBytes))
	logf("report written to %s", reportPath)
}

func findProjectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found from %s", dir)
		}
		dir = parent
	}
}

func freeAddress() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", err
	}
	return addr, nil
}

func startServer(binPath, addr, dbPath, logPath string, extraEnv map[string]string, logf func(string, ...any)) (*serverProcess, error) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(binPath, "-addr", addr, "-db", dbPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = append([]string(nil), os.Environ()...)
	for key, value := range extraEnv {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, err
	}
	proc := &serverProcess{cmd: cmd, logFile: logFile, logPath: logPath}
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get("http://" + addr + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return proc, nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	logFile.Close()
	logBytes, _ := os.ReadFile(logPath)
	return nil, fmt.Errorf("server did not become ready; log:\n%s", string(logBytes))
}

func stopServer(proc *serverProcess) {
	if proc == nil || proc.cmd == nil || proc.cmd.Process == nil {
		return
	}
	_ = proc.cmd.Process.Kill()
	_ = proc.cmd.Wait()
	if proc.logFile != nil {
		_ = proc.logFile.Close()
	}
}

func (c *apiClient) loadSnapshots(leftPath, rightPath string) (model.SnapshotPairMeta, error) {
	payload := model.LoadSnapshotsRequest{
		Left:  model.SnapshotLoadInput{Path: leftPath},
		Right: model.SnapshotLoadInput{Path: rightPath},
	}
	body, status, err := c.post("/v1/snapshots/load", payload)
	if err != nil {
		return model.SnapshotPairMeta{}, err
	}
	var envelope snapshotEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return model.SnapshotPairMeta{}, err
	}
	if status != http.StatusOK || !envelope.OK {
		return model.SnapshotPairMeta{}, fmt.Errorf("snapshot load failed: %s", apiErrorText(envelope.Error))
	}
	return envelope.Snapshots, nil
}

func (c *apiClient) loadSnapshotsExpectError(leftPath, rightPath string) (*apiError, error) {
	payload := model.LoadSnapshotsRequest{
		Left:  model.SnapshotLoadInput{Path: leftPath},
		Right: model.SnapshotLoadInput{Path: rightPath},
	}
	body, _, err := c.post("/v1/snapshots/load", payload)
	if err != nil {
		return nil, err
	}
	var envelope snapshotEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	if envelope.Error == nil {
		return nil, fmt.Errorf("expected an API error, got %s", string(body))
	}
	return envelope.Error, nil
}

func (c *apiClient) snapshotStatus() (statusEnvelope, error) {
	body, _, err := c.get("/v1/snapshots")
	if err != nil {
		return statusEnvelope{}, err
	}
	var envelope statusEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return statusEnvelope{}, err
	}
	if envelope.Error != nil {
		return statusEnvelope{}, fmt.Errorf("snapshot status failed: %s", apiErrorText(envelope.Error))
	}
	return envelope, nil
}

func (c *apiClient) submit(request model.AggregateRequest) (*model.AggregateResponse, error) {
	body, status, err := c.post("/v1/jobs", request)
	if err != nil {
		return nil, err
	}
	var envelope jobEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	if status != http.StatusOK || !envelope.OK || envelope.Job == nil {
		return nil, fmt.Errorf("job failed: %s", apiErrorText(envelope.Error))
	}
	return envelope.Job, nil
}

func (c *apiClient) submitExpectError(request model.AggregateRequest) (*apiError, error) {
	body, _, err := c.post("/v1/jobs", request)
	if err != nil {
		return nil, err
	}
	var envelope jobEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	if envelope.Error == nil {
		return nil, fmt.Errorf("expected API error, got %s", string(body))
	}
	return envelope.Error, nil
}

func (c *apiClient) getJobExpectError(requestID string) (*apiError, error) {
	body, _, err := c.get("/v1/jobs/" + requestID)
	if err != nil {
		return nil, err
	}
	var envelope dataEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	if envelope.Error == nil {
		return nil, fmt.Errorf("expected API error for %s, got %s", requestID, string(body))
	}
	return envelope.Error, nil
}

func (c *apiClient) getPartitions(requestID string) ([]model.PartitionStat, error) {
	body, _, err := c.get("/v1/jobs/" + requestID + "/partitions")
	if err != nil {
		return nil, err
	}
	var envelope dataEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	if envelope.Error != nil {
		return nil, fmt.Errorf("partition query failed: %s", apiErrorText(envelope.Error))
	}
	var partitions []model.PartitionStat
	if err := json.Unmarshal(envelope.Data, &partitions); err != nil {
		return nil, err
	}
	return partitions, nil
}

func (c *apiClient) getResults(requestID string) (map[string]any, error) {
	body, _, err := c.get("/v1/jobs/" + requestID + "/results")
	if err != nil {
		return nil, err
	}
	var envelope dataEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	if envelope.Error != nil {
		return nil, fmt.Errorf("result query failed: %s", apiErrorText(envelope.Error))
	}
	var results map[string]any
	if err := json.Unmarshal(envelope.Data, &results); err != nil {
		return nil, err
	}
	return results, nil
}

func (c *apiClient) getPartialSums(requestID string) ([]json.RawMessage, error) {
	body, _, err := c.get("/v1/jobs/" + requestID + "/partial-sums")
	if err != nil {
		return nil, err
	}
	var envelope dataEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	if envelope.Error != nil {
		return nil, fmt.Errorf("partial-sum query failed: %s", apiErrorText(envelope.Error))
	}
	var partials []json.RawMessage
	if err := json.Unmarshal(envelope.Data, &partials); err != nil {
		return nil, err
	}
	return partials, nil
}

func (c *apiClient) recoveryReport() (*model.RecoveryReport, error) {
	body, _, err := c.get("/v1/recovery/report")
	if err != nil {
		return nil, err
	}
	var envelope reportEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	if envelope.Error != nil || envelope.Report == nil {
		return nil, fmt.Errorf("recovery report failed: %s", apiErrorText(envelope.Error))
	}
	return envelope.Report, nil
}

func (c *apiClient) post(path string, payload any) ([]byte, int, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, 0, err
	}
	response, err := c.client.Post(c.base+path, "application/json", bytes.NewReader(encoded))
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, response.StatusCode, err
	}
	return body, response.StatusCode, nil
}

func (c *apiClient) get(path string) ([]byte, int, error) {
	response, err := c.client.Get(c.base + path)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, response.StatusCode, err
	}
	return body, response.StatusCode, nil
}

func maxGroupRelativeDifference(left, right *model.AggregateResponse) float64 {
	leftGroups := groupMap(left)
	rightGroups := groupMap(right)
	keys := make(map[int64]struct{}, len(leftGroups)+len(rightGroups))
	for key := range leftGroups {
		keys[key] = struct{}{}
	}
	for key := range rightGroups {
		keys[key] = struct{}{}
	}
	ordered := make([]int64, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	maxDeviation := 0.0
	for _, key := range ordered {
		a, aOK := leftGroups[key]
		b, bOK := rightGroups[key]
		if !aOK || !bOK {
			return math.Inf(1)
		}
		if a == nil || b == nil {
			if a != nil || b != nil {
				return math.Inf(1)
			}
			continue
		}
		denom := math.Abs(*b)
		deviation := 0.0
		if denom < 1e-300 {
			if *a != *b {
				deviation = math.Inf(1)
			}
		} else {
			deviation = math.Abs(*a-*b) / denom
		}
		if deviation > maxDeviation {
			maxDeviation = deviation
		}
	}
	return maxDeviation
}

func groupSumsBitwiseEqual(left, right *model.AggregateResponse) bool {
	leftGroups := groupMap(left)
	rightGroups := groupMap(right)
	if len(leftGroups) != len(rightGroups) {
		return false
	}
	for key, leftValue := range leftGroups {
		rightValue, ok := rightGroups[key]
		if !ok {
			return false
		}
		if (leftValue == nil) != (rightValue == nil) {
			return false
		}
		if leftValue != nil && math.Float64bits(*leftValue) != math.Float64bits(*rightValue) {
			return false
		}
	}
	return true
}

func groupMap(response *model.AggregateResponse) map[int64]*float64 {
	groups := make(map[int64]*float64, len(response.Groups))
	for i := range response.Groups {
		if response.Groups[i].Sum == nil {
			groups[response.Groups[i].Key] = nil
			continue
		}
		value := *response.Groups[i].Sum
		groups[response.Groups[i].Key] = &value
	}
	return groups
}

func asInt(value any) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	default:
		return -1
	}
}

func apiErrorText(err *apiError) string {
	if err == nil {
		return "unknown API error"
	}
	payload, _ := json.Marshal(err)
	return string(payload)
}

func must(err error) {
	if err != nil {
		fatalf("%v", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[experiment] FAIL: "+format+"\n", args...)
	os.Exit(1)
}
