package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"profilehotspot/internal/engine"
	"profilehotspot/internal/model"
	"profilehotspot/internal/service"
	"profilehotspot/internal/storage"
)

type demoState struct {
	FinalPolicyVersion uint64 `json:"final_policy_version"`
	FinalCommitSeq     int64  `json:"final_commit_seq"`
	PreCrashCommitSeq  int64  `json:"pre_crash_commit_seq"`
	CrashBatchID       string `json:"crash_batch_id"`
}

type client struct {
	baseURL string
	http    *http.Client
}

func main() {
	baseURL := flag.String("base-url", "http://127.0.0.1:18082", "running service base URL")
	phase := flag.String("phase", "full", "demo phase: full, crash, verify")
	statePath := flag.String("state-file", "demo-state.json", "cross-phase state file")
	flag.Parse()

	run := &runner{client: &client{baseURL: strings.TrimRight(*baseURL, "/"), http: &http.Client{Timeout: 10 * time.Second}}, statePath: *statePath}
	err := execute(run, *phase)
	if err != nil {
		fmt.Fprintln(os.Stderr, "demo failed:", err)
		os.Exit(1)
	}
}

func execute(run *runner, phase string) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if demoFailure, ok := recovered.(*demoError); ok {
				err = demoFailure
				return
			}
			panic(recovered)
		}
	}()
	switch phase {
	case "full":
		return run.full()
	case "crash":
		return run.crash()
	case "verify":
		return run.verify()
	default:
		return fmt.Errorf("unknown phase %q", phase)
	}
}

type runner struct {
	client    *client
	statePath string
	state     demoState
}

func (r *runner) full() error {
	var policy model.Policy
	if err := r.client.getJSON("/v1/demo/policy?version=1", &policy); err != nil {
		return err
	}
	var policyResponse service.PolicyCommitResponse
	if err := r.client.postJSON("/v1/policies", policy, http.StatusCreated, &policyResponse); err != nil {
		return err
	}
	fmt.Printf("policy v%d committed at commit_seq=%d\n", policyResponse.PolicyVersion, policyResponse.CommitSeq)

	smallChunk, err := r.demoChunk("demo-small", 3, 0, 0)
	if err != nil {
		return err
	}
	var smallResponse service.IngestResponse
	if err := r.client.postJSON("/v1/samples", smallChunk, http.StatusCreated, &smallResponse); err != nil {
		return err
	}
	smallMode, err := r.modeFor("checkout")
	if err != nil {
		return err
	}
	require(smallMode == engine.ModeExact, "small checkout batch should use exact path, got %s", smallMode)
	fmt.Printf("small batch: unique=%d mode(checkout)=%s commit_seq=%d\n", smallResponse.AcceptedUniqueSamples, smallMode, smallResponse.CommitSeq)

	largeChunk, err := r.demoChunk("demo-large", 40, 3, 11)
	if err != nil {
		return err
	}
	var largeResponse service.IngestResponse
	if err := r.client.postJSON("/v1/samples", largeChunk, http.StatusCreated, &largeResponse); err != nil {
		return err
	}
	var snapshot storage.VersionSnapshot
	if err := r.client.getJSON("/v1/versions/1/snapshot?limit=8", &snapshot); err != nil {
		return err
	}
	checkout := hotspotFor(snapshot.Hotspots, "checkout")
	require(checkout.Path.Mode == engine.ModeApproximate, "large checkout batch should use approximate path, got %s", checkout.Path.Mode)
	require(checkout.BiasUpperBound > 0 && checkout.BiasUpperBound <= 1, "approximate path must expose a positive bounded bias, got %v", checkout.BiasUpperBound)
	for _, frame := range checkout.Frames {
		require(frame.AbsoluteError <= frame.BiasUpperBound+1e-12, "frame %s error %.8f exceeds bound %.8f", frame.FrameID, frame.AbsoluteError, frame.BiasUpperBound)
	}
	fmt.Printf("large batch: observations=%d mode(checkout)=%s selected=%d population=%d bias_bound=%.6f\n",
		largeResponse.TotalObservations, checkout.Path.Mode, checkout.SelectedSamples, checkout.PopulationSamples, checkout.BiasUpperBound)
	fmt.Printf("hotspot conclusion: frame=%s tie_group=%d tie_size=%d ranking_tolerance_ppm=%d\n",
		checkout.Conclusion.LeaderFrameID, checkout.Conclusion.TopTieGroup, checkout.Conclusion.TopTieSize, checkout.Conclusion.TolerancePPM)

	capacity := 5
	exactLimit := 2
	increment := model.PolicyIncrement{
		BaseVersion: 1,
		Services: map[string]model.ServicePatch{
			"checkout": {ReservoirCapacity: &capacity, ExactLimit: &exactLimit},
		},
	}
	var incrementResponse service.PolicyCommitResponse
	if err := r.client.postJSON("/v1/policies/increments", increment, http.StatusCreated, &incrementResponse); err != nil {
		return err
	}
	require(incrementResponse.Rebuild.Atomic, "incremental policy rebuild must be atomic")
	var incrementSnapshot storage.VersionSnapshot
	if err := r.client.getJSON(fmt.Sprintf("/v1/versions/%d/snapshot?service=checkout&limit=5", incrementResponse.PolicyVersion), &incrementSnapshot); err != nil {
		return err
	}
	require(incrementSnapshot.Hotspots.CommitSeq == incrementSnapshot.CommitSeq, "hotspot and reservoir commit boundary diverged")
	require(incrementSnapshot.IsCurrent, "new policy version should be current")
	fmt.Printf("increment: policy v%d commit_seq=%d rebuilt_reservoirs=%d atomic=%v same_tx_boundary=%v\n",
		incrementResponse.PolicyVersion, incrementResponse.CommitSeq, incrementResponse.Rebuild.RebuiltReservoirCount, incrementResponse.Rebuild.Atomic,
		incrementSnapshot.Hotspots.CommitSeq == incrementSnapshot.CommitSeq)

	r.state = demoState{FinalPolicyVersion: incrementResponse.PolicyVersion, FinalCommitSeq: incrementResponse.CommitSeq, CrashBatchID: "demo-crash"}
	if err := r.saveState(); err != nil {
		return err
	}
	fmt.Printf("state saved to %s\n", r.statePath)
	return nil
}

func (r *runner) crash() error {
	if err := r.loadState(); err != nil {
		return err
	}
	var current struct {
		State storage.StateMeta `json:"state"`
	}
	if err := r.client.getJSON("/v1/current", &current); err != nil {
		return err
	}
	r.state.PreCrashCommitSeq = current.State.CommitSeq
	if err := r.saveState(); err != nil {
		return err
	}
	chunk, err := r.demoChunk(r.state.CrashBatchID, 12, 100, 0)
	if err != nil {
		return err
	}
	fmt.Printf("sending batch %s to the forced-termination fault point; expected connection loss\n", r.state.CrashBatchID)
	raw, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	status, body, err := r.client.postBytes("/v1/samples", raw)
	if err == nil {
		return fmt.Errorf("crash injection did not terminate server: status=%d body=%s", status, body)
	}
	fmt.Printf("server terminated as planned: %v\n", err)
	return nil
}

func (r *runner) verify() error {
	if err := r.loadState(); err != nil {
		return err
	}
	var current struct {
		State storage.StateMeta `json:"state"`
	}
	if err := r.client.getJSON("/v1/current", &current); err != nil {
		return err
	}
	require(current.State.CommitSeq == r.state.PreCrashCommitSeq, "commit seq changed across forced termination: before=%d after=%d", r.state.PreCrashCommitSeq, current.State.CommitSeq)
	var recovery storage.RecoveryReport
	if err := r.client.getJSON("/v1/recovery/report", &recovery); err != nil {
		return err
	}
	require(recovery.Complete, "recovery integrity check is not complete: %+v", recovery)
	require(recovery.ScannedIntents >= 1 && recovery.RolledBackIntents >= 1, "recovery did not scan and roll back the pending intent: %+v", recovery)
	fmt.Printf("recovery: integrity=%s scanned=%d rolled_back=%d repaired=%d last_commit=%d\n",
		recovery.IntegrityCheck, recovery.ScannedIntents, recovery.RolledBackIntents, recovery.RepairedCommitted, recovery.LastCompleteCommitSeq)

	status, _, err := r.client.getBytes("/v1/batches/" + r.state.CrashBatchID)
	if err != nil {
		return err
	}
	require(status == http.StatusNotFound, "uncommitted crash batch should be absent, status=%d", status)

	chunk, err := r.demoChunk(r.state.CrashBatchID, 12, 100, 0)
	if err != nil {
		return err
	}
	var retryResponse service.IngestResponse
	if err := r.client.postJSON("/v1/samples", chunk, http.StatusCreated, &retryResponse); err != nil {
		return err
	}
	require(retryResponse.CommitSeq == r.state.PreCrashCommitSeq+1, "retry commit seq=%d, want %d", retryResponse.CommitSeq, r.state.PreCrashCommitSeq+1)
	fmt.Printf("retry after recovery: batch=%s commit_seq=%d new_samples=%d\n", retryResponse.BatchID, retryResponse.CommitSeq, retryResponse.NewSamples)
	fmt.Println("DEMO_OK")
	return nil
}

func (r *runner) demoChunk(batchID string, count, start, duplicateEvery int) (model.SampleChunk, error) {
	path := fmt.Sprintf("/v1/demo/chunk?seed=42&batch_id=%s&count=%d&start=%d&services=checkout&duplicate_every=%d", batchID, count, start, duplicateEvery)
	var chunk model.SampleChunk
	if err := r.client.getJSON(path, &chunk); err != nil {
		return model.SampleChunk{}, err
	}
	return chunk, nil
}

func (r *runner) modeFor(serviceName string) (string, error) {
	var report service.ModeReport
	if err := r.client.getJSON("/v1/versions/1/modes", &report); err != nil {
		return "", err
	}
	for _, item := range report.Services {
		if item.Service == serviceName {
			return item.Path.Mode, nil
		}
	}
	return "", fmt.Errorf("service %s missing from mode report", serviceName)
}

func hotspotFor(report engine.HotspotReport, serviceName string) engine.ServiceHotspot {
	for _, item := range report.Services {
		if item.Service == serviceName {
			return item
		}
	}
	return engine.ServiceHotspot{Path: engine.PathDecision{Mode: "missing"}}
}

func (r *runner) saveState() error {
	raw, _ := json.MarshalIndent(r.state, "", "  ")
	return os.WriteFile(r.statePath, append(raw, '\n'), 0o644)
}

func (r *runner) loadState() error {
	raw, err := os.ReadFile(r.statePath)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, &r.state)
}

func (c *client) getJSON(path string, destination any) error {
	status, body, err := c.getBytes(path)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("GET %s status=%d body=%s", path, status, body)
	}
	return json.Unmarshal(body, destination)
}

func (c *client) getBytes(path string) (int, []byte, error) {
	response, err := c.http.Get(c.baseURL + path)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	return response.StatusCode, body, err
}

func (c *client) postJSON(path string, payload any, expectedStatus int, destination any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	status, body, err := c.postBytes(path, raw)
	if err != nil {
		return err
	}
	if status != expectedStatus {
		return fmt.Errorf("POST %s status=%d want=%d body=%s", path, status, expectedStatus, body)
	}
	if destination != nil {
		return json.Unmarshal(body, destination)
	}
	return nil
}

func (c *client) postBytes(path string, payload []byte) (int, []byte, error) {
	request, err := http.NewRequest(http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	return response.StatusCode, body, err
}

func require(condition bool, format string, args ...any) {
	if !condition {
		panic(&demoError{message: fmt.Sprintf(format, args...)})
	}
}

type demoError struct{ message string }

func (e *demoError) Error() string { return e.message }
