package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"ampcheckpoint/internal/domain"
	"ampcheckpoint/internal/sample"
)

type verifyError struct{ message string }

func (e verifyError) Error() string { return e.message }

type verifier struct {
	root      string
	tempDir   string
	binary    string
	dbPath    string
	sampleDir string
	addr      string
	baseURL   string
	client    *http.Client
	proc      *exec.Cmd
	logPath   string
	logFile   *os.File
	manifest  sample.Manifest
	summary   map[string]any
}

func main() {
	keep := flag.Bool("keep", false, "keep the temporary database, sample, binary, and server log")
	flag.Parse()
	summary, err := runVerifier(*keep)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verification failed: %v\n", err)
		os.Exit(1)
	}
	raw, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Println(string(raw))
}

func runVerifier(keep bool) (summary map[string]any, err error) {
	root, err := findRepoRoot()
	if err != nil {
		return nil, err
	}
	tempDir, err := os.MkdirTemp("", "amp-checkpoint-verify-")
	if err != nil {
		return nil, err
	}
	addr, err := freeAddr()
	if err != nil {
		return nil, err
	}
	v := &verifier{
		root:      root,
		tempDir:   tempDir,
		dbPath:    filepath.Join(tempDir, "checkpoints.db"),
		sampleDir: filepath.Join(tempDir, "sample"),
		addr:      addr,
		baseURL:   "http://" + addr,
		client:    &http.Client{Timeout: 10 * time.Second},
		logPath:   filepath.Join(tempDir, "server.log"),
		summary:   map[string]any{},
	}
	defer func() {
		v.stopServer()
		if v.logFile != nil {
			_ = v.logFile.Close()
		}
		if recovered := recover(); recovered != nil {
			if ve, ok := recovered.(verifyError); ok {
				err = fmt.Errorf("%s\nserver log:\n%s", ve.message, v.tailLog())
			} else {
				err = fmt.Errorf("panic: %v\nserver log:\n%s", recovered, v.tailLog())
			}
		}
		if keep {
			fmt.Fprintf(os.Stderr, "kept verification workspace: %s\n", tempDir)
		} else if err == nil {
			_ = os.RemoveAll(tempDir)
		}
	}()

	if err := v.buildServer(); err != nil {
		return nil, err
	}
	v.startServer()
	v.runScenario()
	return v.summary, nil
}

func (v *verifier) failf(format string, args ...any) {
	panic(verifyError{message: fmt.Sprintf(format, args...)})
}

func (v *verifier) buildServer() error {
	binary := filepath.Join(v.tempDir, "ampcheckpoint-server")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", binary, "./cmd/server")
	cmd.Dir = v.root
	raw, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("go build server: %w\n%s", err, raw)
	}
	v.binary = binary
	return nil
}

func (v *verifier) startServer() {
	if v.logFile == nil {
		file, err := os.OpenFile(v.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			v.failf("open server log: %v", err)
		}
		v.logFile = file
	}
	cmd := exec.Command(v.binary, "-addr", v.addr, "-db", v.dbPath, "-sample-dir", v.sampleDir)
	cmd.Dir = v.root
	cmd.Stdout = v.logFile
	cmd.Stderr = v.logFile
	if err := cmd.Start(); err != nil {
		v.failf("start server: %v", err)
	}
	v.proc = cmd
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := v.client.Get(v.baseURL + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	v.failf("server did not become healthy at %s", v.baseURL)
}

func (v *verifier) stopServer() {
	if v.proc == nil {
		return
	}
	_ = v.proc.Process.Kill()
	_ = v.proc.Wait()
	v.proc = nil
}

func (v *verifier) restartServer() {
	v.stopServer()
	time.Sleep(250 * time.Millisecond)
	v.startServer()
}

func (v *verifier) runScenario() {
	v.requestExpect(http.MethodGet, "/healthz", nil, http.StatusOK, "healthz")
	body := v.requestExpect(http.MethodGet, "/v1/sample", nil, http.StatusOK, "sample")
	if err := json.Unmarshal(body, &v.manifest); err != nil {
		v.failf("decode sample: %v", err)
	}
	if v.manifest.Seed != sample.DefaultSeed || len(v.manifest.Shards) != sample.ShardCount {
		v.failf("unexpected sample manifest: seed=%d shards=%d", v.manifest.Seed, len(v.manifest.Shards))
	}

	policyResponse := v.postJSON("/v1/policies", v.manifest.Policy, http.StatusCreated)
	assertPolicyVersion(v, policyResponse, 1)
	cp1 := v.commitCheckpoint(1, http.StatusCreated)
	if cp1.Version != 1 || len(cp1.Pools) != 2 {
		v.failf("unexpected initial checkpoint: %+v", cp1)
	}
	vram1 := poolByKind(v, cp1.Pools, domain.PoolVRAM)
	if vram1.TotalBytes > vram1.LimitBytes || vram1.CopyCount != sample.InitialResidentCount || len(cp1.Evictions) != 0 {
		v.failf("initial residency violated: %+v evictions=%d", vram1, len(cp1.Evictions))
	}
	if cp1.Writes.UsedBytes > cp1.Writes.BudgetBytes {
		v.failf("initial checkpoint exceeded write budget: %+v", cp1.Writes)
	}
	precision1 := domain.PrecisionReport{}
	v.getJSON("/v1/versions/1/precision", &precision1)
	if precision1.CheckedShardCount != sample.InitialResidentCount || precision1.CumulativeAbsDeviationFixed <= 0 {
		v.failf("unexpected initial precision report: %+v", precision1)
	}
	var allCumulative, allMaxAbs int64
	for _, shard := range precision1.Shards {
		if !shard.ConsistencyOK || len(shard.Elements) != shard.ElementCount {
			v.failf("precision contract failed for %s", shard.ShardID)
		}
		var cumulative, maxAbs int64
		for _, element := range shard.Elements {
			deviation := element.RestoredFixed - element.MasterFixed
			if element.DeviationFixed != deviation {
				v.failf("element deviation mismatch for %s: stored=%d computed=%d", shard.ShardID, element.DeviationFixed, deviation)
			}
			abs := deviation
			if abs < 0 {
				abs = -abs
			}
			cumulative += abs
			if abs > maxAbs {
				maxAbs = abs
			}
		}
		if cumulative != shard.CumulativeAbsDeviationFixed || maxAbs != shard.MaxAbsDeviationFixed {
			v.failf("precision aggregate mismatch for %s: cumulative=%d/%d max=%d/%d", shard.ShardID, cumulative, shard.CumulativeAbsDeviationFixed, maxAbs, shard.MaxAbsDeviationFixed)
		}
		allCumulative += cumulative
		if maxAbs > allMaxAbs {
			allMaxAbs = maxAbs
		}
	}
	if allCumulative != precision1.CumulativeAbsDeviationFixed || allMaxAbs != precision1.MaxAbsDeviationFixed {
		v.failf("global precision aggregate mismatch: cumulative=%d/%d max=%d/%d", allCumulative, precision1.CumulativeAbsDeviationFixed, allMaxAbs, precision1.MaxAbsDeviationFixed)
	}

	deltaResponse := v.postJSON("/v1/deltas", v.manifest.Delta, http.StatusCreated)
	assertPolicyVersion(v, deltaResponse, 2)
	cp2 := v.commitCheckpoint(2, http.StatusCreated)
	if cp2.Version != 2 || len(cp2.Evictions) < 1 {
		v.failf("incremental adjustment did not evict: %+v", cp2)
	}
	vram2 := poolByKind(v, cp2.Pools, domain.PoolVRAM)
	if vram2.TotalBytes > vram2.LimitBytes {
		v.failf("VRAM limit exceeded after eviction: %+v", vram2)
	}
	shards2 := domain.ShardsResponse{}
	v.getJSON("/v1/versions/2/shards", &shards2)
	for _, eviction := range cp2.Evictions {
		copyFound := false
		for _, copy := range shards2.Copies {
			if copy.CopyID == eviction.CopyID {
				copyFound = true
				if copy.Pool != domain.PoolPersistent || copy.Status != domain.StatusPersisted {
					v.failf("evicted copy %s did not reach PERSISTED: %+v", copy.CopyID, copy)
				}
			}
		}
		if !copyFound {
			v.failf("eviction references absent copy %s", eviction.CopyID)
		}
	}
	precision2 := domain.PrecisionReport{}
	v.getJSON("/v1/versions/2/precision", &precision2)
	if precision2.CheckedShardCount != sample.ShardCount {
		v.failf("precision report was not rebuilt for all FP16 copies: %+v", precision2)
	}

	v.postJSON("/v1/policies", v.manifest.TightPolicy, http.StatusCreated)
	cp3 := v.commitCheckpoint(3, http.StatusCreated)
	if cp3.Version != 3 || cp3.Writes.UsedBytes > cp3.Writes.BudgetBytes || len(cp3.Writes.Skipped) == 0 {
		v.failf("write budget check failed: %+v", cp3.Writes)
	}
	if cp3.Writes.RemainingBytes != cp3.Writes.BudgetBytes-cp3.Writes.UsedBytes {
		v.failf("write budget accounting mismatch: %+v", cp3.Writes)
	}
	if len(cp3.Writes.Queued) != len(cp3.Writes.Skipped) || len(cp3.Writes.Queued) > cp3.Writes.QueueCapacity {
		v.failf("bounded queue accounting mismatch: %+v", cp3.Writes)
	}

	v.postJSON("/v1/policies", v.manifest.QueueFullPolicy, http.StatusCreated)
	queueBody := v.requestExpect(http.MethodPost, "/v1/checkpoints", domain.CheckpointRequest{Round: 4}, http.StatusTooManyRequests, "queue full")
	assertErrorCode(v, queueBody, domain.ErrWriteQueueFull)
	latest := domain.CheckpointMeta{}
	v.getJSON("/v1/versions/latest/meta", &latest)
	if latest.Version != 3 {
		v.failf("E_WRITE_QUEUE_FULL committed a checkpoint: latest=%d", latest.Version)
	}

	staleBody := v.requestExpect(http.MethodPost, "/v1/policies", v.manifest.Policy, http.StatusConflict, "stale snapshot")
	assertErrorCode(v, staleBody, domain.ErrStaleSnapshot)
	staleDelta := domain.DeltaFragment{BaseVersion: 1, GeneratedAt: time.Now(), LossScale: &domain.LossScalePatch{Action: "grow"}}
	staleBaseBody := v.requestExpect(http.MethodPost, "/v1/deltas", staleDelta, http.StatusConflict, "stale base")
	assertErrorCode(v, staleBaseBody, domain.ErrStaleBase)
	badBatch := domain.PartitionBatch{Partitions: []domain.PartitionInput{{CopyID: "bad", ShardID: "shard-00", Round: 1, Precision: "BF16", SizeBytes: 4096, Digest: "bad"}}}
	badBody := v.requestExpect(http.MethodPost, "/v1/partitions/validate", badBatch, http.StatusUnprocessableEntity, "invalid partition")
	assertErrorCode(v, badBody, domain.ErrPartitionInvalid)

	overflowSuccesses := 0
	overflowBody := []byte(nil)
	for i := 0; i < 12; i++ {
		current := domain.Policy{}
		v.getJSON("/v1/policies/current", &current)
		delta := domain.DeltaFragment{BaseVersion: current.Version, GeneratedAt: time.Now(), LossScale: &domain.LossScalePatch{Action: "grow"}}
		status, body := v.request(http.MethodPost, "/v1/deltas", delta)
		if status == http.StatusCreated {
			overflowSuccesses++
			continue
		}
		if status == http.StatusUnprocessableEntity {
			assertErrorCode(v, body, domain.ErrLossScaleOverflow)
			overflowBody = body
			break
		}
		v.failf("unexpected loss scale response: HTTP %d %s", status, body)
	}
	if overflowBody == nil || overflowSuccesses == 0 {
		v.failf("loss scale growth did not exercise a successful advance followed by E_LOSS_SCALE_OVERFLOW")
	}
	afterOverflow := domain.Policy{}
	v.getJSON("/v1/policies/current", &afterOverflow)
	if afterOverflow.LossScaleFixed > afterOverflow.OverflowThresholdFixed {
		v.failf("loss scale overflow mutated committed policy: %+v", afterOverflow.PolicySnapshot)
	}

	current := domain.Policy{}
	v.getJSON("/v1/policies/current", &current)
	vramPolicy := v.manifest.VRAMFullPolicy
	vramPolicy.Version = current.Version + 10
	vramPolicy.GeneratedAt = time.Now().UTC()
	vramPolicy.Checksum = ""
	v.postJSON("/v1/policies", vramPolicy, http.StatusCreated)
	vramBody := v.requestExpect(http.MethodPost, "/v1/checkpoints", domain.CheckpointRequest{Round: 4}, http.StatusInsufficientStorage, "vram full")
	assertErrorCode(v, vramBody, domain.ErrVRAMPoolFull)
	v.getJSON("/v1/versions/latest/meta", &latest)
	if latest.Version != 3 {
		v.failf("E_VRAM_POOL_FULL committed a checkpoint: latest=%d", latest.Version)
	}

	v.getJSON("/v1/policies/current", &current)
	faultPolicy := v.manifest.TightPolicy
	faultPolicy.Version = current.Version + 1
	faultPolicy.GeneratedAt = time.Now().UTC()
	faultPolicy.WriteBudgetBytes = 80_000
	faultPolicy.RoundTimeBudgetMS = 500
	faultPolicy.WriteQueueCapacity = 32
	faultPolicy.Checksum = ""
	v.postJSON("/v1/policies", faultPolicy, http.StatusCreated)
	recoveryBefore := domain.RecoveryReport{}
	v.getJSON("/v1/versions/3/recovery", &recoveryBefore)
	precisionBefore := domain.PrecisionReport{}
	v.getJSON("/v1/versions/3/precision", &precisionBefore)
	faultResult := v.commitCheckpoint(4, http.StatusAccepted, `"fault_after_intent":"evict"`)
	if faultResult.IntentID == "" || faultResult.FaultInjected != "evict" {
		v.failf("fault injection did not return an intent: %+v", faultResult)
	}
	v.getJSON("/v1/versions/latest/meta", &latest)
	if latest.Version != 3 {
		v.failf("fault intent became visible before commit: latest=%d", latest.Version)
	}

	v.restartServer()
	status := map[string]any{}
	v.getJSON("/v1/status", &status)
	if intFromAny(status["recovered_intents"]) < 1 || intFromAny(status["running_intents"]) != 0 {
		v.failf("startup recovery status is wrong: %#v", status)
	}
	v.getJSON("/v1/versions/latest/meta", &latest)
	if latest.Version != 3 {
		v.failf("recovery changed latest committed checkpoint: %d", latest.Version)
	}
	recoveryAfter := domain.RecoveryReport{}
	v.getJSON("/v1/versions/3/recovery", &recoveryAfter)
	if len(recoveryAfter.AbortedOperations) != 1 {
		v.failf("expected one recovered aborted operation, got %+v", recoveryAfter.AbortedOperations)
	}
	aborted := recoveryAfter.AbortedOperations[0]
	if aborted.OperationID != faultResult.IntentID || len(aborted.PendingShards) == 0 || len(aborted.AbortedEvictions) == 0 {
		v.failf("aborted intent evidence is incomplete: %+v", aborted)
	}
	if recoveryAfter.VRAMTotalBytes != recoveryBefore.VRAMTotalBytes || recoveryAfter.PersistentTotalBytes != recoveryBefore.PersistentTotalBytes {
		v.failf("recovery report changed committed pool facts: before=%+v after=%+v", recoveryBefore, recoveryAfter)
	}
	precisionAfter := domain.PrecisionReport{}
	v.getJSON("/v1/versions/3/precision", &precisionAfter)
	if precisionAfter.CheckedShardCount != precisionBefore.CheckedShardCount || precisionAfter.CumulativeAbsDeviationFixed != precisionBefore.CumulativeAbsDeviationFixed {
		v.failf("precision facts changed during recovery: before=%+v after=%+v", precisionBefore, precisionAfter)
	}

	v.summary = map[string]any{
		"status":                     "ok",
		"sample_seed":                v.manifest.Seed,
		"sample_partition_batch_sha": v.manifest.PartitionBatchSHA256,
		"initial_residency": map[string]any{
			"checkpoint_version": 1,
			"vram_bytes":         vram1.TotalBytes,
			"vram_limit_bytes":   vram1.LimitBytes,
			"resident_copies":    vram1.CopyCount,
			"cumulative_abs_dev": precision1.CumulativeAbsDeviationFixed,
			"max_abs_dev":        precision1.MaxAbsDeviationFixed,
			"max_rel_ppm":        precision1.MaxRelativeDeviationPPM,
		},
		"eviction": map[string]any{
			"checkpoint_version": cp2.Version,
			"count":              len(cp2.Evictions),
			"vram_bytes":         vram2.TotalBytes,
			"vram_limit_bytes":   vram2.LimitBytes,
			"evicted":            cp2.Evictions,
		},
		"write_budget": map[string]any{
			"checkpoint_version": cp3.Version,
			"budget_bytes":       cp3.Writes.BudgetBytes,
			"used_bytes":         cp3.Writes.UsedBytes,
			"remaining_bytes":    cp3.Writes.RemainingBytes,
			"skipped_count":      len(cp3.Writes.Skipped),
			"queued_count":       len(cp3.Writes.Queued),
		},
		"error_codes_verified": []string{
			domain.ErrPartitionInvalid,
			domain.ErrVRAMPoolFull,
			domain.ErrLossScaleOverflow,
			domain.ErrStaleSnapshot,
			domain.ErrStaleBase,
			domain.ErrWriteQueueFull,
		},
		"loss_scale": map[string]any{
			"successful_growth_steps": overflowSuccesses,
			"overflow_error":          json.RawMessage(overflowBody),
		},
		"forced_termination_recovery": map[string]any{
			"intent_id":              aborted.OperationID,
			"intent_phase":           aborted.Phase,
			"latest_committed":       latest.Version,
			"pending_shards":         len(aborted.PendingShards),
			"aborted_evictions":      len(aborted.AbortedEvictions),
			"vram_bytes_before":      recoveryBefore.VRAMTotalBytes,
			"vram_bytes_after":       recoveryAfter.VRAMTotalBytes,
			"persistent_before":      recoveryBefore.PersistentTotalBytes,
			"persistent_after":       recoveryAfter.PersistentTotalBytes,
			"committed_only_report":  recoveryAfter.CommittedOnly,
			"recovered_intent_count": intFromAny(status["recovered_intents"]),
		},
	}
}

func (v *verifier) commitCheckpoint(round int64, wantStatus int, extra ...string) domain.CheckpointResult {
	body := fmt.Sprintf(`{"round":%d`, round)
	if len(extra) > 0 {
		body += "," + strings.Join(extra, ",")
	}
	body += "}"
	req, err := http.NewRequest(http.MethodPost, v.baseURL+"/v1/checkpoints", bytes.NewBufferString(body))
	if err != nil {
		v.failf("build checkpoint request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	status, raw := v.do(req)
	raw = v.expectStatus(status, raw, wantStatus, fmt.Sprintf("checkpoint round %d", round))
	var result domain.CheckpointResult
	if err := json.Unmarshal(raw, &result); err != nil {
		v.failf("decode checkpoint response: %v body=%s", err, raw)
	}
	return result
}

func (v *verifier) postJSON(path string, payload any, wantStatus int) []byte {
	raw, err := json.Marshal(payload)
	if err != nil {
		v.failf("encode %s request: %v", path, err)
	}
	req, err := http.NewRequest(http.MethodPost, v.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		v.failf("build %s request: %v", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	status, body := v.do(req)
	return v.expectStatus(status, body, wantStatus, path)
}

func (v *verifier) getJSON(path string, target any) {
	body := v.requestExpect(http.MethodGet, path, nil, http.StatusOK, path)
	if err := json.Unmarshal(body, target); err != nil {
		v.failf("decode %s: %v body=%s", path, err, body)
	}
}

func (v *verifier) requestExpect(method, path string, payload any, want int, label string) []byte {
	status, body := v.request(method, path, payload)
	return v.expectStatus(status, body, want, label)
}

func (v *verifier) request(method, path string, payload any) (int, []byte) {
	var reader io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			v.failf("encode %s %s: %v", method, path, err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, v.baseURL+path, reader)
	if err != nil {
		v.failf("build %s %s: %v", method, path, err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return v.do(req)
}

func (v *verifier) do(req *http.Request) (int, []byte) {
	resp, err := v.client.Do(req)
	if err != nil {
		v.failf("%s %s: %v", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		v.failf("read %s %s response: %v", req.Method, req.URL.Path, err)
	}
	return resp.StatusCode, raw
}

func (v *verifier) expectStatus(status int, body []byte, want int, label string) []byte {
	if status != want {
		v.failf("%s: HTTP %d want %d body=%s", label, status, want, body)
	}
	return body
}

func (v *verifier) tailLog() string {
	if v.logFile != nil {
		_ = v.logFile.Sync()
	}
	raw, err := os.ReadFile(v.logPath)
	if err != nil {
		return ""
	}
	const limit = 12_000
	if len(raw) > limit {
		raw = raw[len(raw)-limit:]
	}
	return string(raw)
}

func assertPolicyVersion(v *verifier, body []byte, version int64) {
	var response struct {
		PolicyVersion int64 `json:"policy_version"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		v.failf("decode policy response: %v body=%s", err, body)
	}
	if response.PolicyVersion != version {
		v.failf("policy version=%d want %d body=%s", response.PolicyVersion, version, body)
	}
}

func assertErrorCode(v *verifier, body []byte, code string) {
	var response struct {
		Error domain.BusinessError `json:"error"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		v.failf("decode error envelope: %v body=%s", err, body)
	}
	if response.Error.Code != code || response.Error.Message == "" {
		v.failf("error code=%q want %q body=%s", response.Error.Code, code, body)
	}
}

func poolByKind(v *verifier, pools []domain.PoolSummary, kind domain.Pool) domain.PoolSummary {
	for _, pool := range pools {
		if pool.Pool == kind {
			return pool
		}
	}
	v.failf("pool %s missing from checkpoint response", kind)
	return domain.PoolSummary{}
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		n, _ := typed.Int64()
		return int(n)
	default:
		return 0
	}
}

func findRepoRoot() (string, error) {
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

func freeAddr() (string, error) {
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
