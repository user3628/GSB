package verify

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"ciartifacts/internal/cache"
	"ciartifacts/internal/httpapi"
)

type Options struct {
	SamplesPath string
	WorkDir     string
	Executable  string
}

type Check struct {
	Name     string `json:"name"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	Passed   bool   `json:"passed"`
}

type Report struct {
	WorkDir       string  `json:"work_dir"`
	BaseURL       string  `json:"base_url"`
	Checks        []Check `json:"checks"`
	PassedCount   int     `json:"passed_count"`
	FailedCount   int     `json:"failed_count"`
	OverallPassed bool    `json:"overall_passed"`
}

type writeResponse struct {
	OK         bool `json:"ok"`
	Idempotent bool `json:"idempotent"`
	Entry      struct {
		EntryID string `json:"entry_id"`
		State   string `json:"state"`
		Hits    int64  `json:"hits"`
	} `json:"entry"`
}

type hitResponse struct {
	OK                    bool   `json:"ok"`
	Hit                   bool   `json:"hit"`
	Hits                  int64  `json:"hits"`
	ArtifactContentBase64 string `json:"artifact_content_base64"`
	Entry                 struct {
		State string `json:"state"`
		Hits  int64  `json:"hits"`
	} `json:"entry"`
}

type errorResponse struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context"`
}

type manifestResponse struct {
	OK       bool           `json:"ok"`
	Sequence int64          `json:"sequence"`
	Checksum string         `json:"checksum"`
	Entries  int            `json:"entries"`
	Path     string         `json:"path"`
	Snapshot cache.Snapshot `json:"snapshot"`
}

type evictionResponse struct {
	OK       bool                 `json:"ok"`
	Eviction cache.EvictionResult `json:"eviction"`
}

type switchResponse struct {
	OK     bool                       `json:"ok"`
	Switch cache.SnapshotSwitchResult `json:"switch"`
}

type recoveryResponse struct {
	OK     bool                 `json:"ok"`
	Report cache.RecoveryReport `json:"report"`
}

type sampleEntryRequest struct {
	StageID          string `json:"stage_id"`
	InputFingerprint string `json:"input_fingerprint"`
	ToolchainVersion string `json:"toolchain_version"`
	ArtifactContent  string `json:"artifact_content"`
	ArtifactDigest   string `json:"artifact_digest"`
	ArtifactSize     int64  `json:"artifact_size"`
	RetainUntil      string `json:"retain_until"`
}

func Run(ctx context.Context, options Options) (*Report, error) {
	if options.SamplesPath == "" {
		return nil, fmt.Errorf("samples path is required")
	}
	samplesPath, err := filepath.Abs(options.SamplesPath)
	if err != nil {
		return nil, err
	}
	inputs, err := cache.LoadSampleFile(samplesPath)
	if err != nil {
		return nil, err
	}
	workDir := options.WorkDir
	cleanup := false
	if workDir == "" {
		workDir, err = os.MkdirTemp("", "ciartifacts-verify-*")
		if err != nil {
			return nil, err
		}
		cleanup = true
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return nil, err
	}
	defer func() {
		if cleanup {
			_ = os.RemoveAll(workDir)
		}
	}()

	baseTime := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	clock := cache.NewManualClock(baseTime)
	store, err := cache.Open(ctx, cache.Config{
		Dir:           filepath.Join(workDir, "http-store"),
		CapacityBytes: 15,
		Clock:         clock,
	})
	if err != nil {
		return nil, err
	}
	defer store.Close()
	server := httptest.NewServer(httpapi.New(store))
	defer server.Close()
	report := &Report{WorkDir: workDir, BaseURL: server.URL}
	client := server.Client()

	build := mustSample(inputs, "build")
	testEntry := mustSample(inputs, "test")
	pkg := mustSample(inputs, "package")
	expired := mustSample(inputs, "lint")

	status, body, err := apiRequest(ctx, client, http.MethodPost, server.URL+"/v1/entries", sampleRequest(build))
	if err != nil {
		return nil, err
	}
	var write writeResponse
	_ = json.Unmarshal(body, &write)
	addCheck(report, "write-available",
		"HTTP 201, state=AVAILABLE",
		fmt.Sprintf("HTTP %d, state=%s", status, write.Entry.State),
		status == http.StatusCreated && write.Entry.State == string(cache.StateAvailable))

	clock.Advance(time.Minute)
	hitStatus, hitBody, err := apiRequest(ctx, client, http.MethodGet, hitURL(server.URL, build, build.ArtifactDigest), nil)
	if err != nil {
		return nil, err
	}
	var hit hitResponse
	_ = json.Unmarshal(hitBody, &hit)
	content, _ := base64.StdEncoding.DecodeString(hit.ArtifactContentBase64)
	addCheck(report, "write-then-hit",
		"HTTP 200, hit=true, hits=1, content=artifact-alpha\\n",
		fmt.Sprintf("HTTP %d, hit=%t, hits=%d, content=%q", hitStatus, hit.Hit, hit.Hits, content),
		hitStatus == http.StatusOK && hit.Hit && hit.Hits == 1 && bytes.Equal(content, build.ArtifactContent))

	transitionBody := map[string]any{
		"stage_id":          build.StageID,
		"input_fingerprint": build.InputFingerprint,
		"toolchain_version": build.ToolchainVersion,
		"to_state":          string(cache.StatePending),
	}
	transitionStatus, transitionRaw, err := apiRequest(ctx, client, http.MethodPost, server.URL+"/v1/entries/transition", transitionBody)
	if err != nil {
		return nil, err
	}
	transitionErr := decodeAPIError(transitionRaw)
	addCheck(report, "invalid-transition",
		"HTTP 409, E_INVALID_TRANSITION",
		fmt.Sprintf("HTTP %d, %s", transitionStatus, transitionErr.Code),
		transitionStatus == http.StatusConflict && transitionErr.Code == cache.CodeInvalidTransition)

	other := withContent(build, "artifact-omega\n")
	conflictStatus, conflictRaw, err := apiRequest(ctx, client, http.MethodPost, server.URL+"/v1/entries", sampleRequest(other))
	if err != nil {
		return nil, err
	}
	conflictErr := decodeAPIError(conflictRaw)
	addCheck(report, "conflicting-write",
		"HTTP 409, E_CONFLICTING_WRITE",
		fmt.Sprintf("HTTP %d, %s", conflictStatus, conflictErr.Code),
		conflictStatus == http.StatusConflict && conflictErr.Code == cache.CodeConflictingWrite)

	clock.Advance(time.Minute)
	if status, _, err = apiRequest(ctx, client, http.MethodPost, server.URL+"/v1/entries", sampleRequest(testEntry)); err != nil {
		return nil, err
	} else if status != http.StatusCreated {
		return nil, fmt.Errorf("write test sample failed with HTTP %d", status)
	}
	testPath := artifactPath(store.ArtifactDir(), testEntry.ArtifactDigest)
	if err := os.WriteFile(testPath, []byte("tampered-beta!\n"), 0o644); err != nil {
		return nil, err
	}
	mismatchStatus, mismatchRaw, err := apiRequest(ctx, client, http.MethodGet, hitURL(server.URL, testEntry, ""), nil)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(testPath, testEntry.ArtifactContent, 0o644); err != nil {
		return nil, err
	}
	mismatchErr := decodeAPIError(mismatchRaw)
	addCheck(report, "digest-mismatch",
		"HTTP 422, E_DIGEST_MISMATCH",
		fmt.Sprintf("HTTP %d, %s", mismatchStatus, mismatchErr.Code),
		mismatchStatus == http.StatusUnprocessableEntity && mismatchErr.Code == cache.CodeDigestMismatch)

	clock.Advance(time.Minute)
	if status, _, err = apiRequest(ctx, client, http.MethodPost, server.URL+"/v1/entries", sampleRequest(expired)); err != nil {
		return nil, err
	} else if status != http.StatusCreated {
		return nil, fmt.Errorf("write expired sample failed with HTTP %d", status)
	}
	clock.Advance(time.Minute)
	expiredStatus, expiredRaw, err := apiRequest(ctx, client, http.MethodGet, hitURL(server.URL, expired, ""), nil)
	if err != nil {
		return nil, err
	}
	expiredErr := decodeAPIError(expiredRaw)
	addCheck(report, "expired-rejected",
		"HTTP 410, E_ENTRY_EXPIRED",
		fmt.Sprintf("HTTP %d, %s", expiredStatus, expiredErr.Code),
		expiredStatus == http.StatusGone && expiredErr.Code == cache.CodeEntryExpired)

	clock.Advance(time.Minute)
	if status, _, err = apiRequest(ctx, client, http.MethodPost, server.URL+"/v1/entries", sampleRequest(pkg)); err != nil {
		return nil, err
	} else if status != http.StatusCreated {
		return nil, fmt.Errorf("write package sample failed with HTTP %d", status)
	}

	evictStatus, evictRaw, err := apiRequest(ctx, client, http.MethodPost, server.URL+"/v1/evict", map[string]any{})
	if err != nil {
		return nil, err
	}
	var eviction evictionResponse
	_ = json.Unmarshal(evictRaw, &eviction)
	order := make([]string, 0, len(eviction.Eviction.Evicted))
	for _, item := range eviction.Eviction.Evicted {
		order = append(order, item.StageID)
	}
	addCheck(report, "capacity-eviction",
		"HTTP 200, evicted=[build test lint], used<=15, released=46",
		fmt.Sprintf("HTTP %d, evicted=%v, used=%d, released=%d", evictStatus, order, eviction.Eviction.UsedBytesAfter, eviction.Eviction.ReleasedBytes),
		evictStatus == http.StatusOK && strings.Join(order, ",") == "build,test,lint" && eviction.Eviction.UsedBytesAfter <= 15 && eviction.Eviction.ReleasedBytes == 46)

	missStatus, missRaw, err := apiRequest(ctx, client, http.MethodGet, hitURL(server.URL, build, ""), nil)
	if err != nil {
		return nil, err
	}
	missErr := decodeAPIError(missRaw)
	reason, _ := missErr.Context["reason"].(string)
	addCheck(report, "evicted-miss-reason",
		"HTTP 404, E_CACHE_MISS, reason=EVICTED",
		fmt.Sprintf("HTTP %d, %s, reason=%s", missStatus, missErr.Code, reason),
		missStatus == http.StatusNotFound && missErr.Code == cache.CodeCacheMiss && reason == "EVICTED")

	snapshotPath := filepath.Join(workDir, "snapshot.json")
	manifestStatus, manifestRaw, err := apiRequest(ctx, client, http.MethodGet, server.URL+"/v1/manifest?path="+url.QueryEscape(snapshotPath), nil)
	if err != nil {
		return nil, err
	}
	var manifest manifestResponse
	_ = json.Unmarshal(manifestRaw, &manifest)
	if manifestStatus != http.StatusOK {
		return nil, fmt.Errorf("manifest export failed with HTTP %d: %s", manifestStatus, string(manifestRaw))
	}
	sameStatus, sameRaw, err := apiRequest(ctx, client, http.MethodPost, server.URL+"/v1/snapshots/switch", map[string]any{"path": snapshotPath})
	if err != nil {
		return nil, err
	}
	sameErr := decodeAPIError(sameRaw)
	addCheck(report, "snapshot-monotonic",
		"HTTP 409, E_SNAPSHOT_SEQUENCE",
		fmt.Sprintf("HTTP %d, %s", sameStatus, sameErr.Code),
		sameStatus == http.StatusConflict && sameErr.Code == cache.CodeSnapshotSequence)

	snapshot, err := cache.LoadSnapshotFile(snapshotPath)
	if err != nil {
		return nil, err
	}
	snapshot.Sequence += 5
	if err := cache.SealSnapshot(&snapshot); err != nil {
		return nil, err
	}
	if err := cache.WriteSnapshotFile(snapshotPath, snapshot); err != nil {
		return nil, err
	}
	switchStatus, switchRaw, err := apiRequest(ctx, client, http.MethodPost, server.URL+"/v1/snapshots/switch", map[string]any{"path": snapshotPath})
	if err != nil {
		return nil, err
	}
	var switched switchResponse
	_ = json.Unmarshal(switchRaw, &switched)
	addCheck(report, "snapshot-switch",
		"HTTP 200, sequence advanced",
		fmt.Sprintf("HTTP %d, sequence=%d", switchStatus, switched.Switch.Sequence),
		switchStatus == http.StatusOK && switched.Switch.Sequence == snapshot.Sequence)

	executable := options.Executable
	if executable == "" {
		executable, err = os.Executable()
		if err != nil {
			return nil, err
		}
	}
	writeRecovery, err := runCrashRecovery(ctx, executable, samplesPath, filepath.Join(workDir, "crash-write"), "write", "recovery")
	if err != nil {
		return nil, err
	}
	addCheck(report, "forced-kill-write-recovery",
		"state=PENDING, intent rolled back, HTTP report visible, artifact orphan removed",
		fmt.Sprintf("state=%s, report_entries=%d, orphans=%d", writeRecovery.State, len(writeRecovery.Report.EntriesRolledBack), len(writeRecovery.Report.OrphansRemoved)),
		writeRecovery.State == string(cache.StatePending) && len(writeRecovery.Report.EntriesRolledBack) == 1 && len(writeRecovery.Report.OrphansRemoved) > 0)

	evictRecovery, err := runCrashRecovery(ctx, executable, samplesPath, filepath.Join(workDir, "crash-evict"), "evict", "build")
	if err != nil {
		return nil, err
	}
	addCheck(report, "forced-kill-eviction-recovery",
		"state=AVAILABLE, eviction intent rolled back, HTTP report visible",
		fmt.Sprintf("state=%s, report_evictions=%d", evictRecovery.State, len(evictRecovery.Report.EvictionsRolledBack)),
		evictRecovery.State == string(cache.StateAvailable) && len(evictRecovery.Report.EvictionsRolledBack) == 1)

	report.PassedCount = 0
	report.FailedCount = 0
	for _, check := range report.Checks {
		if check.Passed {
			report.PassedCount++
		} else {
			report.FailedCount++
		}
	}
	report.OverallPassed = report.FailedCount == 0
	return report, nil
}

type crashRecoveryOutcome struct {
	State  string
	Report cache.RecoveryReport
}

func runCrashRecovery(ctx context.Context, executable, samplesPath, dir, scenario, stage string) (crashRecoveryOutcome, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return crashRecoveryOutcome{}, err
	}
	marker := filepath.Join(dir, "killed.marker")
	cmd := exec.Command(executable, "crash-helper", "--dir", dir, "--scenario", scenario, "--samples", samplesPath, "--stage", stage)
	cmd.Env = append(os.Environ(), "CIARTIFACTS_CRASH_MARKER="+marker)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		return crashRecoveryOutcome{}, fmt.Errorf("start crash helper: %w", err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			return crashRecoveryOutcome{}, fmt.Errorf("crash marker timeout; helper output: %s", output.String())
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		return crashRecoveryOutcome{}, err
	}
	_ = cmd.Wait()

	store, err := cache.Open(ctx, cache.Config{
		Dir:           dir,
		CapacityBytes: capacityForScenario(scenario),
	})
	if err != nil {
		return crashRecoveryOutcome{}, err
	}
	defer store.Close()
	inputs, err := cache.LoadSampleFile(samplesPath)
	if err != nil {
		return crashRecoveryOutcome{}, err
	}
	input := mustSample(inputs, stage)
	entry, ok := store.EntryByKey(input.Key())
	if !ok {
		return crashRecoveryOutcome{}, fmt.Errorf("recovered entry missing for stage %s", stage)
	}
	server := httptest.NewServer(httpapi.New(store))
	defer server.Close()
	status, raw, err := apiRequest(ctx, server.Client(), http.MethodGet, server.URL+"/v1/recovery/report", nil)
	if err != nil {
		return crashRecoveryOutcome{}, err
	}
	if status != http.StatusOK {
		return crashRecoveryOutcome{}, fmt.Errorf("recovery report endpoint returned HTTP %d: %s", status, string(raw))
	}
	var response recoveryResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return crashRecoveryOutcome{}, err
	}
	return crashRecoveryOutcome{State: string(entry.State), Report: response.Report}, nil
}

func capacityForScenario(scenario string) int64 {
	if scenario == "evict" {
		return 1
	}
	return 1024
}

func addCheck(report *Report, name, expected, actual string, passed bool) {
	report.Checks = append(report.Checks, Check{Name: name, Expected: expected, Actual: actual, Passed: passed})
}

func mustSample(inputs []cache.EntryInput, stage string) cache.EntryInput {
	input, err := cache.SampleByName(inputs, stage)
	if err != nil {
		panic(err)
	}
	return input
}

func withContent(input cache.EntryInput, content string) cache.EntryInput {
	input.ArtifactContent = []byte(content)
	input.ArtifactSize = int64(len(input.ArtifactContent))
	input.ArtifactDigest = cache.DigestBytes(input.ArtifactContent)
	return input
}

func sampleRequest(input cache.EntryInput) sampleEntryRequest {
	return sampleEntryRequest{
		StageID:          input.StageID,
		InputFingerprint: input.InputFingerprint,
		ToolchainVersion: input.ToolchainVersion,
		ArtifactContent:  string(input.ArtifactContent),
		ArtifactDigest:   input.ArtifactDigest,
		ArtifactSize:     input.ArtifactSize,
		RetainUntil:      input.RetainUntil.UTC().Format(time.RFC3339),
	}
}

func hitURL(base string, input cache.EntryInput, expectedDigest string) string {
	values := url.Values{}
	values.Set("stage_id", input.StageID)
	values.Set("input_fingerprint", input.InputFingerprint)
	values.Set("toolchain_version", input.ToolchainVersion)
	if expectedDigest != "" {
		values.Set("expected_digest", expectedDigest)
	}
	return base + "/v1/hit?" + values.Encode()
}

func artifactPath(dir, digest string) string {
	name, _ := cache.DigestPath(digest)
	return filepath.Join(dir, name)
}

func decodeAPIError(raw []byte) errorResponse {
	var value errorResponse
	_ = json.Unmarshal(raw, &value)
	return value
}

func apiRequest(ctx context.Context, client *http.Client, method, target string, body any) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 32<<20))
	if err != nil {
		return 0, nil, err
	}
	return response.StatusCode, data, nil
}
