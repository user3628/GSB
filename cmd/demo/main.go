package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"qosshaper/internal/qos"
)

type serviceProcess struct {
	cmd  *exec.Cmd
	base string
	done chan error
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "DEMO FAILED: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	root, err := findModuleRoot()
	if err != nil {
		return err
	}
	workdir := filepath.Join(root, "data", "demo-runtime")
	if err := os.RemoveAll(workdir); err != nil {
		return fmt.Errorf("reset demo runtime: %w", err)
	}
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		return err
	}
	binDir := filepath.Join(root, "data", "demo-bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return err
	}
	binName := "qosshaper"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	bin := filepath.Join(binDir, binName)
	fmt.Printf("== build ==\n%s\n", bin)
	build := exec.Command("go", "build", "-o", bin, "./cmd/qosshaper")
	build.Dir = root
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return fmt.Errorf("build service: %w", err)
	}

	svc, err := startService(bin, workdir, nil)
	if err != nil {
		return err
	}
	defer func() {
		if svc != nil {
			stopService(svc)
		}
	}()
	client := &demoClient{base: svc.base, http: &http.Client{Timeout: 15 * time.Second}}

	fmt.Printf("== service ==\n%s\n", svc.base)
	fmt.Printf("sample files: %s, %s\n", filepath.Join(workdir, "data", "policy_sample.json"), filepath.Join(workdir, "data", "traffic_sample.jsonl"))

	policyPath := filepath.Join(workdir, "data", "policy_sample.json")
	policyBytes, err := os.ReadFile(policyPath)
	if err != nil {
		return err
	}
	var snapshot qos.SnapshotRequest
	if err := json.Unmarshal(policyBytes, &snapshot); err != nil {
		return err
	}
	snapshot.RequestID = "demo-snapshot-v1"
	snapshot.Version = 1
	var mutation qos.MutationResponse
	if err := client.post("/v1/policy/snapshot", snapshot, &mutation, 200); err != nil {
		return err
	}
	fmt.Printf("\n== full snapshot ==\ncode=%s version=%d recomputed=%v digest=%s\n",
		mutation.Code, mutation.Version, mutation.RecomputedQueueIDs, short(mutation.PolicyDigest))

	var videoV1 qos.QueueViewResponse
	if err := client.get("/v1/policy/queues/video?observe_version=1", &videoV1, 200); err != nil {
		return err
	}
	fmt.Printf("video@v1 effective_cir=%d calculation_version=%d valid_through=%d\n",
		videoV1.EffectiveRates.EffectiveCIRBPS, videoV1.CalculationVersion, videoV1.ValidThroughVersion)

	sim1req := qos.SimulationRequest{RequestID: "demo-sim-v1", PolicyVersion: 1}
	var sim1 qos.SimulationSummaryResponse
	if err := client.post("/v1/simulations", sim1req, &sim1, 200); err != nil {
		return err
	}
	printSimulation("simulation v1", sim1, "video")

	cir := int64(1_234_567)
	inc := qos.IncrementRequest{
		RequestID: "demo-increment-video-cir", BaseVersion: 1,
		QueuePatches: []qos.QueuePatch{{QueueID: "video", CIRBPS: &cir}},
	}
	var mutation2 qos.MutationResponse
	if err := client.post("/v1/policy/increment", inc, &mutation2, 200); err != nil {
		return err
	}
	fmt.Printf("\n== incremental parent CIR change ==\ncode=%s version=%d recomputed=%v unchanged=%v\n",
		mutation2.Code, mutation2.Version, mutation2.RecomputedQueueIDs, mutation2.UnchangedQueueIDs)

	var videoV2, rtpV2, voiceV2 qos.QueueViewResponse
	if err := client.get("/v1/policy/queues/video?observe_version=2", &videoV2, 200); err != nil {
		return err
	}
	if err := client.get("/v1/policy/queues/video-rtp?observe_version=2", &rtpV2, 200); err != nil {
		return err
	}
	if err := client.get("/v1/policy/queues/voice?observe_version=2", &voiceV2, 200); err != nil {
		return err
	}
	fmt.Printf("video@v2 eff_cir=%d calc=%d valid=%d\n", videoV2.EffectiveRates.EffectiveCIRBPS, videoV2.CalculationVersion, videoV2.ValidThroughVersion)
	fmt.Printf("video-rtp@v2 eff_cir=%d source=%s score=%d calc=%d valid=%d\n",
		rtpV2.EffectiveRates.EffectiveCIRBPS, rtpV2.EffectiveRates.CIRSRC.Kind, rtpV2.Consistency.Score, rtpV2.CalculationVersion, rtpV2.ValidThroughVersion)
	fmt.Printf("voice@v2 eff_cir=%d calc=%d valid=%d (unaffected view was not recomputed)\n",
		voiceV2.EffectiveRates.EffectiveCIRBPS, voiceV2.CalculationVersion, voiceV2.ValidThroughVersion)

	sim2req := qos.SimulationRequest{RequestID: "demo-sim-v2", PolicyVersion: 2}
	var sim2 qos.SimulationSummaryResponse
	if err := client.post("/v1/simulations", sim2req, &sim2, 200); err != nil {
		return err
	}
	printSimulation("simulation v2", sim2, "video")

	var diag qos.QueueDiagnosticResponse
	if err := client.get("/v1/diagnostics/queues/video?observe_version=2", &diag, 200); err != nil {
		return err
	}
	fmt.Printf("video diagnostic: tokens=%d peak_tokens=%d sent=%d dropped=%d conclusion=%s carry=%d millibits\n",
		diag.TokenBalanceBits, diag.PeakTokenBalanceBits, diag.SentBytes, diag.DroppedBytes, diag.Conclusion,
		diag.CumulativeErrorMilliBits)
	for _, p := range diag.Packets {
		nonZero := false
		for _, refill := range p.Refill {
			if refill.CarryAfterMilliBits != 0 || refill.CapacityOverflowBits != 0 {
				nonZero = true
			}
		}
		if nonZero {
			fmt.Printf("first nontrivial packet rounding: seq=%d color=%s bytes=%d sent=%d dropped=%d\n",
				p.Sequence, p.Color, p.Bytes, p.SentBytes, p.DroppedBytes)
			for _, refill := range p.Refill {
				fmt.Printf("  %s: rate=%d delta_ms=%d raw=%d whole=%d carry=%d overflow=%d\n",
					refill.Bucket, refill.RateBPS, refill.DeltaMS, refill.RawMilliBits, refill.WholeBits,
					refill.CarryAfterMilliBits, refill.CapacityOverflowBits)
			}
			break
		}
	}

	status, body, err := client.raw(http.MethodGet, "/v1/simulations/demo-sim-v1/queues/video?observe_version=1", nil)
	if err != nil {
		return err
	}
	var stale qos.BusinessError
	if err := json.Unmarshal(body, &stale); err != nil {
		return fmt.Errorf("decode stale read: %w", err)
	}
	fmt.Printf("\n== stale read ==\nHTTP=%d code=%s observed=%v view=%v current=%v\n",
		status, stale.Code, stale.Context["observed_version"], stale.Context["view_version"], stale.Context["current_version"])
	if status != http.StatusConflict || stale.Code != qos.CodeStaleRead {
		return fmt.Errorf("expected E_STALE_READ, got HTTP %d %s", status, stale.Code)
	}

	// External termination recovery. The next service pauses inside a committed
	// boundary candidate's SQLite transaction, after derived rows are written.
	stopService(svc)
	svc = nil
	readyFile := filepath.Join(workdir, "data", "derived.ready")
	crashEnv := map[string]string{
		"QOS_FAILPOINT_STAGE":    "derived",
		"QOS_FAILPOINT_FILE":     readyFile,
		"QOS_FAILPOINT_SLEEP_MS": "30000",
	}
	crashSvc, err := startService(bin, workdir, crashEnv)
	if err != nil {
		return err
	}
	crashClient := &demoClient{base: crashSvc.base, http: &http.Client{Timeout: 45 * time.Second}}
	bulkCIR := int64(3_000_111)
	crashReq := qos.IncrementRequest{
		RequestID: "demo-crash-v3", BaseVersion: 2,
		QueuePatches: []qos.QueuePatch{{QueueID: "bulk", CIRBPS: &bulkCIR}},
	}
	requestErr := make(chan error, 1)
	go func() {
		var ignored qos.MutationResponse
		requestErr <- crashClient.post("/v1/policy/increment", crashReq, &ignored, 200)
	}()
	fmt.Printf("\n== forced termination ==\nwaiting for in-transaction failpoint: %s\n", readyFile)
	if err := waitForFile(readyFile, 20*time.Second); err != nil {
		stopService(crashSvc)
		return err
	}
	if err := crashSvc.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("kill service: %w", err)
	}
	<-crashSvc.done
	select {
	case <-requestErr:
	case <-time.After(5 * time.Second):
	}
	fmt.Printf("killed pid while derived rebuild was uncommitted\n")

	svc, err = startService(bin, workdir, nil)
	if err != nil {
		return err
	}
	client = &demoClient{base: svc.base, http: &http.Client{Timeout: 15 * time.Second}}
	var report qos.RecoveryReportResponse
	if err := client.get("/v1/recovery/report", &report, 200); err != nil {
		return err
	}
	fmt.Printf("recovery current_version=%d aborted=%d\n", report.CurrentVersion, len(report.Reports))
	for _, r := range report.Reports {
		fmt.Printf("  request=%s kind=%s phase=%s status=%s rollback_version=%d\n",
			r.RequestID, r.Kind, r.Phase, r.Status, r.RollbackVersion)
	}
	if report.CurrentVersion != 2 {
		return fmt.Errorf("recovery version = %d, want 2", report.CurrentVersion)
	}
	if len(report.Reports) != 1 || report.Reports[0].RequestID != crashReq.RequestID {
		return fmt.Errorf("recovery report missing aborted request: %+v", report.Reports)
	}
	var bulkView qos.QueueViewResponse
	if err := client.get("/v1/policy/queues/bulk?observe_version=2", &bulkView, 200); err != nil {
		return err
	}
	fmt.Printf("bulk@v2 after recovery eff_cir=%d calc=%d valid=%d (crashed CIR not visible)\n",
		bulkView.EffectiveRates.EffectiveCIRBPS, bulkView.CalculationVersion, bulkView.ValidThroughVersion)
	if bulkView.EffectiveRates.EffectiveCIRBPS == bulkCIR {
		return fmt.Errorf("crashed increment became visible")
	}
	var statusResp qos.IntentStatus
	if err := client.get("/v1/requests/demo-crash-v3", &statusResp, 200); err != nil {
		return err
	}
	fmt.Printf("request status: %s phase=%s committed_version=%d\n", statusResp.Status, statusResp.Phase, statusResp.CommittedVersion)
	if statusResp.Status != "aborted" {
		return fmt.Errorf("crashed request status = %s, want aborted", statusResp.Status)
	}
	fmt.Printf("\nDEMO PASS: versions, cascade, token bucket, stale read and crash recovery are consistent.\n")
	return nil
}

type demoClient struct {
	base string
	http *http.Client
}

func (c *demoClient) raw(method, path string, payload any) (int, []byte, error) {
	var body io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, c.base+path, body)
	if err != nil {
		return 0, nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return resp.StatusCode, data, err
}

func (c *demoClient) post(path string, payload any, out any, wantStatus int) error {
	status, body, err := c.raw(http.MethodPost, path, payload)
	if err != nil {
		return err
	}
	if status != wantStatus {
		return fmt.Errorf("POST %s: HTTP %d: %s", path, status, string(body))
	}
	return json.Unmarshal(body, out)
}

func (c *demoClient) get(path string, out any, wantStatus int) error {
	status, body, err := c.raw(http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	if status != wantStatus {
		return fmt.Errorf("GET %s: HTTP %d: %s", path, status, string(body))
	}
	return json.Unmarshal(body, out)
}

func printSimulation(label string, sim qos.SimulationSummaryResponse, focus string) {
	fmt.Printf("\n== %s ==\ncode=%s version=%d packets=%d\n", label, sim.Code, sim.PolicyVersion, sim.Packets)
	for _, q := range sim.Queues {
		if q.QueueID == focus || q.QueueID == "video-rtp" {
			fmt.Printf("queue=%s eff_cir=%d sent=%d dropped=%d green=%d yellow=%d red=%d final_tokens=%d carry=%d conclusion=%s\n",
				q.QueueID, q.EffectiveCIRBPS, q.SentBytes, q.DroppedBytes, q.GreenPackets, q.YellowPackets, q.RedPackets,
				q.FinalTokenBalanceBits, q.CumulativeErrorMilliBits, q.Conclusion)
		}
	}
}

func startService(bin, workdir string, extraEnv map[string]string) (*serviceProcess, error) {
	cmd := exec.Command(bin, "-addr", "127.0.0.1:0", "-workdir", workdir)
	cmd.Stderr = os.Stderr
	cmd.Env = append([]string(nil), os.Environ()...)
	for k, v := range extraEnv {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	lineCh := make(chan string, 32)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lineCh <- scanner.Text()
		}
		close(lineCh)
	}()
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	for {
		select {
		case line, ok := <-lineCh:
			if !ok {
				return nil, errors.New("service exited before reporting LISTEN")
			}
			fmt.Println(line)
			if strings.HasPrefix(line, "LISTEN=") {
				return &serviceProcess{cmd: cmd, base: strings.TrimPrefix(line, "LISTEN="), done: done}, nil
			}
		case <-timer.C:
			_ = cmd.Process.Kill()
			return nil, errors.New("timed out waiting for service LISTEN line")
		}
	}
}

func stopService(svc *serviceProcess) {
	if svc == nil || svc.cmd == nil || svc.cmd.Process == nil {
		return
	}
	_ = svc.cmd.Process.Kill()
	select {
	case <-svc.done:
	case <-time.After(5 * time.Second):
	}
}

func waitForFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", path)
}

func findModuleRoot() (string, error) {
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
			return "", errors.New("go.mod not found above current directory")
		}
		dir = parent
	}
}

func short(s string) string {
	if len(s) <= 16 {
		return s
	}
	return s[:16] + "..."
}
