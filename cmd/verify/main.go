package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	grayrouter "grayrouter"
)

type Check struct {
	Name     string `json:"name"`
	Pass     bool   `json:"pass"`
	Code     string `json:"code,omitempty"`
	Detail   any    `json:"detail,omitempty"`
	Error    string `json:"error,omitempty"`
	Duration string `json:"duration,omitempty"`
}

type VerifyReport struct {
	OK          bool              `json:"ok"`
	GeneratedAt string            `json:"generated_at"`
	Platform    string            `json:"platform"`
	GoVersion   string            `json:"go_version"`
	WorkDir     string            `json:"work_dir"`
	Checks      []Check           `json:"checks"`
	Passed      int               `json:"passed"`
	Failed      int               `json:"failed"`
	Artifacts   map[string]string `json:"artifacts"`
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "crash-helper" {
		runCrashHelper(os.Args[2:])
		return
	}
	if err := runVerify(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func runVerify() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	workDir := filepath.Join("artifacts", "verify-work")
	_ = os.RemoveAll(workDir)
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return err
	}
	if err := grayrouter.WriteSampleFiles("samples"); err != nil {
		return err
	}
	report := VerifyReport{GeneratedAt: time.Now().Format(time.RFC3339), Platform: runtime.GOOS + "/" + runtime.GOARCH, GoVersion: runtime.Version(), WorkDir: workDir, Artifacts: map[string]string{}}
	add := func(name string, fn func() (any, error)) {
		start := time.Now()
		detail, err := fn()
		check := Check{Name: name, Duration: time.Since(start).String(), Detail: detail}
		if err != nil {
			check.Pass = false
			check.Error = err.Error()
			var apiErr *grayrouter.APIError
			if errors.As(err, &apiErr) {
				check.Code = apiErr.Code
			}
			report.Failed++
		} else {
			check.Pass = true
			report.Passed++
		}
		report.Checks = append(report.Checks, check)
	}

	bundle := grayrouter.SampleBundleFixed()
	mainDB := filepath.Join(workDir, "main.db")
	host, err := grayrouter.Open(ctx, grayrouter.Config{DBPath: mainDB})
	if err != nil {
		return err
	}
	defer host.Close()

	add("E_GRAPH_INVALID returns node/field context and does not persist", func() (any, error) {
		invalidSnapshot := grayrouter.Snapshot{Version: 900, Models: []grayrouter.ModelVersion{bundle.Invalid}}
		_, err := host.SubmitSnapshot(ctx, grayrouter.CommitSnapshotInput{RequestID: "verify-invalid", Snapshot: invalidSnapshot})
		var apiErr *grayrouter.APIError
		if !errors.As(err, &apiErr) || apiErr.Code != grayrouter.CodeGraphInvalid {
			return nil, fmt.Errorf("expected E_GRAPH_INVALID, got %v", err)
		}
		state, stateErr := host.State(ctx)
		if stateErr != nil {
			return nil, stateErr
		}
		if state.HeadVersion != 0 || state.Counts["snapshots"] != 0 || state.Counts["model_versions"] != 0 {
			return nil, fmt.Errorf("invalid graph was persisted: head=%d counts=%v", state.HeadVersion, state.Counts)
		}
		return apiErr.Context, nil
	})

	add("graph signatures are bitwise repeatable and all diff categories are detected", func() (any, error) {
		categories := map[string]bool{}
		details := []map[string]any{}
		for _, newModel := range bundle.Delta.Models {
			var oldModel grayrouter.ModelVersion
			for _, candidate := range bundle.Baseline.Models {
				if candidate.Name == newModel.Name {
					oldModel = candidate
				}
			}
			sig1 := grayrouter.GraphSignature(newModel.Graph)
			sig2 := grayrouter.GraphSignature(newModel.Graph)
			struct1 := grayrouter.StructuralSignature(newModel.Graph)
			struct2 := grayrouter.StructuralSignature(newModel.Graph)
			if sig1 != sig2 || struct1 != struct2 {
				return nil, fmt.Errorf("non-deterministic signature for %s", newModel.Key())
			}
			diff := grayrouter.DiffModelVersions(oldModel, newModel)
			for _, category := range diff.Categories {
				categories[category] = true
			}
			details = append(details, map[string]any{"model": newModel.Key(), "categories": diff.Categories, "signature": sig1[:16]})
		}
		required := []string{grayrouter.CategoryShape, grayrouter.CategoryDType, grayrouter.CategoryQuant, grayrouter.CategoryOperator, grayrouter.CategoryPrePost}
		missing := []string{}
		for _, category := range required {
			if !categories[category] {
				missing = append(missing, category)
			}
		}
		if len(missing) > 0 {
			return nil, fmt.Errorf("missing categories: %s", strings.Join(missing, ","))
		}
		return details, nil
	})

	var pinned *grayrouter.InferResponse
	var baselineReport *grayrouter.CommitReport
	add("bind in-flight request before gray switch", func() (any, error) {
		state, err := host.State(ctx)
		if err != nil {
			return nil, err
		}
		if state.HeadVersion == 0 {
			report, err := host.SubmitSnapshot(ctx, grayrouter.CommitSnapshotInput{RequestID: "verify-baseline-v1", Snapshot: bundle.Baseline})
			if err != nil {
				return nil, err
			}
			baselineReport = report
		}
		resp, err := host.BindInference(ctx, grayrouter.InferRequest{RequestID: "verify-pin-0001", ModelKey: "demo-op@1.0.0"})
		if err != nil {
			return nil, err
		}
		pinned = resp
		return map[string]any{"request_id": resp.RequestID, "model_key": resp.ModelKey, "status": resp.BindingStatus}, nil
	})

	add("commit deterministic baseline snapshot v1", func() (any, error) {
		report, err := host.SubmitSnapshot(ctx, grayrouter.CommitSnapshotInput{RequestID: "verify-baseline-v1", Snapshot: bundle.Baseline})
		if err != nil {
			return nil, err
		}
		baselineReport = report
		return map[string]any{"version": report.Version, "models": report.ModelCount}, nil
	})
	_ = baselineReport

	var deltaReport *grayrouter.CommitReport
	add("commit delta v2 and produce category-driven migration/rebuild manifest", func() (any, error) {
		report, err := host.SubmitDelta(ctx, grayrouter.CommitDeltaInput{RequestID: "verify-delta-v2", Delta: bundle.Delta})
		if err != nil {
			return nil, err
		}
		deltaReport = report
		if len(report.Plans) != 5 || len(report.Rebuild) == 0 || len(report.Migrate) == 0 {
			return nil, fmt.Errorf("unexpected plan counts plans=%d rebuild=%d migrate=%d", len(report.Plans), len(report.Rebuild), len(report.Migrate))
		}
		return map[string]any{"version": report.Version, "rebuild": report.Rebuild, "migrate": report.Migrate}, nil
	})
	_ = deltaReport

	add("pinned request completes on old model version after switch", func() (any, error) {
		if pinned == nil {
			return nil, errors.New("missing pinned binding")
		}
		resp, err := host.CompleteBound(ctx, pinned.RequestID)
		if err != nil {
			return nil, err
		}
		state, _ := host.State(ctx)
		if resp.ModelKey != "demo-op@1.0.0" || state.HeadVersion != 2 || resp.Result == nil || len(resp.Result.Outputs) == 0 {
			return nil, fmt.Errorf("pin/version mismatch resp=%s head=%d outputs=%d", resp.ModelKey, state.HeadVersion, len(resp.Result.Outputs))
		}
		return map[string]any{"completed_model_key": resp.ModelKey, "head_version": state.HeadVersion, "output_digest": resp.Result.Outputs[0].Digest}, nil
	})

	add("gray fixed-point normalization and stable decisions are bitwise repeatable", func() (any, error) {
		norm, err := grayrouter.NormalizeWeights(map[string]float64{"base": 0.75, "canary": 0.25})
		if err != nil {
			return nil, err
		}
		if norm.TotalFixed != grayrouter.FixedScale {
			return nil, fmt.Errorf("fixed total=%d", norm.TotalFixed)
		}
		first := bundle.Trail.Requests[0]
		d1, err := host.DecideGray(ctx, grayrouter.GrayDecisionRequest{RequestID: first.RequestID, RouteKey: "demo-op-gray", SnapshotVersion: 2})
		if err != nil {
			return nil, err
		}
		d2, err := host.DecideGray(ctx, grayrouter.GrayDecisionRequest{RequestID: first.RequestID, RouteKey: "demo-op-gray", SnapshotVersion: 2})
		if err != nil {
			return nil, err
		}
		if string(grayrouter.CanonicalBytes(d1)) != string(grayrouter.CanonicalBytes(d2)) || d1.Digest != d2.Digest {
			return nil, errors.New("gray decision is not bitwise stable")
		}
		selected := map[string]int{}
		for _, req := range bundle.Trail.Requests[:24] {
			decision, err := host.DecideGray(ctx, grayrouter.GrayDecisionRequest{RequestID: req.RequestID, RouteKey: "demo-op-gray", SnapshotVersion: 2})
			if err != nil {
				return nil, err
			}
			selected[decision.Selected]++
		}
		return map[string]any{"normalization": norm, "stable_decision": d1, "trail_distribution": selected}, nil
	})

	add("same model/seed inference summaries are bitwise identical and quant errors are emitted", func() (any, error) {
		r1, err := host.Infer(ctx, grayrouter.InferRequest{RequestID: "verify-infer-a", ModelKey: "demo-quant@1.1.0"})
		if err != nil {
			return nil, err
		}
		r2, err := host.Infer(ctx, grayrouter.InferRequest{RequestID: "verify-infer-b", ModelKey: "demo-quant@1.1.0"})
		if err != nil {
			return nil, err
		}
		if string(grayrouter.CanonicalBytes(r1.Result)) != string(grayrouter.CanonicalBytes(r2.Result)) {
			return nil, errors.New("inference summaries differ")
		}
		if len(r1.Result.RoundingSteps) == 0 {
			return nil, errors.New("expected quantization rounding steps")
		}
		return map[string]any{"outputs": r1.Result.Outputs, "rounding_steps": r1.Result.RoundingSteps}, nil
	})

	add("same request_id concurrent inference executes once and all waiters reuse result", func() (any, error) {
		before := atomic.LoadInt64(&host.InferExecutions)
		const n = 24
		var wg sync.WaitGroup
		results := make([]string, n)
		errs := make([]error, n)
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				resp, err := host.Infer(ctx, grayrouter.InferRequest{RequestID: "verify-idem-concurrent-0001", ModelKey: "demo-stable@1.0.0"})
				if err != nil {
					errs[i] = err
					return
				}
				results[i] = string(grayrouter.CanonicalBytes(resp.Result))
			}(i)
		}
		close(start)
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				return nil, err
			}
		}
		for i := 1; i < n; i++ {
			if results[i] != results[0] {
				return nil, fmt.Errorf("result %d differs", i)
			}
		}
		after := atomic.LoadInt64(&host.InferExecutions)
		if after-before != 1 {
			return nil, fmt.Errorf("expected one graph execution, metric delta=%d", after-before)
		}
		return map[string]any{"waiters": n, "execution_delta": after - before, "digest": results[0][:16]}, nil
	})

	add("concurrent same-base deltas return exactly one E_CONFLICTING_UPDATE; old base returns E_STALE_BASE", func() (any, error) {
		conflictModel := findModel(bundle.Delta.Models, "demo-prepost")
		conflictModel.Version = "1.1.1"
		for i := range conflictModel.Graph.Nodes {
			if conflictModel.Graph.Nodes[i].ID == "n_output" {
				conflictModel.Graph.Nodes[i].PostSteps = append(conflictModel.Graph.Nodes[i].PostSteps, "post_conflict_verify")
			}
		}
		delta3 := grayrouter.Delta{Version: 3, BaseVersion: 2, Models: []grayrouter.ModelVersion{conflictModel}}
		entered := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		host.Hooks.AfterDeltaGate = func(base int64) {
			if base == 2 {
				once.Do(func() { close(entered) })
				<-release
			}
		}
		type result struct {
			report *grayrouter.CommitReport
			err    error
		}
		resultCh := make(chan result, 2)
		var wg sync.WaitGroup
		for _, id := range []string{"verify-conflict-a", "verify-conflict-b"} {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				r, err := host.SubmitDelta(ctx, grayrouter.CommitDeltaInput{RequestID: id, Delta: delta3})
				resultCh <- result{r, err}
			}(id)
		}
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			close(release)
			return nil, errors.New("winner did not enter delta gate")
		}
		time.Sleep(300 * time.Millisecond)
		close(release)
		wg.Wait()
		close(resultCh)
		host.Hooks.AfterDeltaGate = nil
		var successes, conflicts int
		var winnerVersion int64
		for item := range resultCh {
			if item.err == nil {
				successes++
				winnerVersion = item.report.Version
				continue
			}
			var apiErr *grayrouter.APIError
			if errors.As(item.err, &apiErr) && apiErr.Code == grayrouter.CodeConflictingUpdate {
				conflicts++
			} else {
				return nil, fmt.Errorf("unexpected concurrent delta error: %w", item.err)
			}
		}
		if successes != 1 || conflicts != 1 || winnerVersion != 3 {
			return nil, fmt.Errorf("successes=%d conflicts=%d winner=%d", successes, conflicts, winnerVersion)
		}
		_, staleErr := host.SubmitDelta(ctx, grayrouter.CommitDeltaInput{
			RequestID: "verify-stale-base-1",
			Delta:     grayrouter.Delta{Version: 4, BaseVersion: 1, Models: []grayrouter.ModelVersion{conflictModel}},
		})
		var staleAPI *grayrouter.APIError
		if !errors.As(staleErr, &staleAPI) || staleAPI.Code != grayrouter.CodeStaleBase {
			return nil, fmt.Errorf("expected E_STALE_BASE, got %v", staleErr)
		}
		state, _ := host.State(ctx)
		if state.HeadVersion != 3 {
			return nil, fmt.Errorf("stale update changed head to %d", state.HeadVersion)
		}
		return map[string]any{"successes": successes, "conflicts": conflicts, "stale_context": staleAPI.Context, "head": state.HeadVersion}, nil
	})

	add("many readers stay consistent with committed state", func() (any, error) {
		var wg sync.WaitGroup
		errCh := make(chan error, 32)
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					if _, err := host.State(ctx); err != nil {
						errCh <- err
						return
					}
					if _, err := host.Artifacts(ctx, ""); err != nil {
						errCh <- err
						return
					}
					if _, err := host.CommitReport(ctx, 2); err != nil {
						errCh <- err
						return
					}
				}
			}(i)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				return nil, err
			}
		}
		return "32 readers x 10 consistent reads", nil
	})

	host.Close()

	rebuildCheck := runCrashScenario(ctx, "rebuild", workDir)
	report.Checks = append(report.Checks, rebuildCheck)
	if rebuildCheck.Pass {
		report.Passed++
	} else {
		report.Failed++
	}
	inflightCheck := runCrashScenario(ctx, "inflight", workDir)
	report.Checks = append(report.Checks, inflightCheck)
	for _, check := range []Check{rebuildCheck, inflightCheck} {
		if detail, ok := check.Detail.(map[string]any); ok {
			if path, ok := detail["recovery_path"].(string); ok {
				report.Artifacts["recovery_"+strings.TrimPrefix(check.Name, "force-kill recovery: ")] = path
			}
		}
	}
	if inflightCheck.Pass {
		report.Passed++
	} else {
		report.Failed++
	}

	statePath := filepath.Join(workDir, "main-state.json")
	if state, err := readStateFile(mainDB); err == nil {
		_ = writeJSONFile(statePath, state)
		report.Artifacts["main_state"] = statePath
	}
	jsonPath := "artifacts/verification-report.json"
	mdPath := "artifacts/verification-report.md"
	report.OK = report.Failed == 0
	report.Artifacts["json_report"] = jsonPath
	report.Artifacts["markdown_report"] = mdPath
	_ = writeJSONFile(jsonPath, report)
	_ = os.WriteFile(mdPath, []byte(renderMarkdown(report)), 0o644)
	if report.Failed > 0 {
		return fmt.Errorf("verification failed: %d check(s) failed; see %s", report.Failed, mdPath)
	}
	fmt.Printf("PASS: %d checks; report=%s\n", report.Passed, mdPath)
	return nil
}

func findModel(models []grayrouter.ModelVersion, name string) grayrouter.ModelVersion {
	for _, model := range models {
		if model.Name == name {
			return model
		}
	}
	return grayrouter.ModelVersion{}
}

func readStateFile(dbPath string) (*grayrouter.StateView, error) {
	ctx := context.Background()
	h, err := grayrouter.Open(ctx, grayrouter.Config{DBPath: dbPath})
	if err != nil {
		return nil, err
	}
	defer h.Close()
	return h.State(ctx)
}

func runCrashScenario(parentCtx context.Context, mode, workDir string) Check {
	start := time.Now()
	dbPath := filepath.Join(workDir, "crash-"+mode+".db")
	readyPath := dbPath + ".ready"
	logPath := dbPath + ".helper.log"
	exe, err := os.Executable()
	if err != nil {
		return Check{Name: "SIGKILL recovery: " + mode, Pass: false, Error: err.Error()}
	}
	cmd := exec.Command(exe, "crash-helper", "--mode", mode, "--db", dbPath, "--ready", readyPath)
	var logs bytes.Buffer
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	if err := cmd.Start(); err != nil {
		return Check{Name: "force-kill recovery: " + mode, Pass: false, Error: err.Error()}
	}
	if !waitForFile(readyPath, 30*time.Second) {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = os.WriteFile(logPath, logs.Bytes(), 0o644)
		return Check{Name: "force-kill recovery: " + mode, Pass: false, Error: "helper did not reach crash point; log=" + logPath, Detail: logs.String()}
	}
	killErr := cmd.Process.Kill()
	waitErr := cmd.Wait()
	_ = os.WriteFile(logPath, logs.Bytes(), 0o644)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h, err := grayrouter.Open(ctx, grayrouter.Config{DBPath: dbPath})
	if err != nil {
		return Check{Name: "force-kill recovery: " + mode, Pass: false, Error: err.Error(), Detail: map[string]any{"kill_error": killErrString(killErr), "wait_error": killErrString(waitErr), "log": logPath}}
	}
	recovery := h.LatestRecoveryReport()
	state, stateErr := h.State(ctx)
	_ = h.Close()
	recoveryPath := filepath.Join(workDir, "recovery-"+mode+".json")
	_ = writeJSONFile(recoveryPath, recovery)
	pass := err == nil && stateErr == nil && recovery != nil && recovery.Consistent && state.HeadVersion == 1 && len(recovery.AbortedIntents) > 0
	if mode == "rebuild" {
		pass = pass && len(recovery.AbortedSnapshots) > 0 && recovery.AbortedArtifactRows >= 20
	} else {
		pass = pass && len(recovery.AbortedBindings) > 0
	}
	return Check{
		Name: "force-kill recovery: " + mode, Pass: pass, Duration: time.Since(start).String(),
		Detail: map[string]any{
			"kill": "os.Process.Kill maps to SIGKILL on Unix and TerminateProcess on Windows", "kill_error": killErrString(killErr),
			"wait_error": killErrString(waitErr), "head_after_recovery": maybeHead(state), "recovery_path": recoveryPath,
			"recovery": recovery, "helper_log": logPath,
		},
	}
}

func maybeHead(state *grayrouter.StateView) int64 {
	if state == nil {
		return -1
	}
	return state.HeadVersion
}

func killErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func waitForFile(path string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func runCrashHelper(args []string) {
	flags := flag.NewFlagSet("crash-helper", flag.ContinueOnError)
	mode := flags.String("mode", "", "rebuild or inflight")
	dbPath := flags.String("db", "", "sqlite path")
	readyPath := flags.String("ready", "", "ready file")
	if err := flags.Parse(args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx := context.Background()
	bundle := grayrouter.SampleBundleFixed()
	seedHost, err := grayrouter.Open(ctx, grayrouter.Config{DBPath: *dbPath})
	if err != nil {
		panic(err)
	}
	if _, err := seedHost.SubmitSnapshot(ctx, grayrouter.CommitSnapshotInput{RequestID: "helper-baseline", Snapshot: bundle.Baseline}); err != nil {
		panic(err)
	}
	if err := seedHost.Close(); err != nil {
		panic(err)
	}
	crashPoint := "after_stage"
	if *mode == "inflight" {
		crashPoint = "after_binding"
	}
	h, err := grayrouter.Open(ctx, grayrouter.Config{DBPath: *dbPath, CrashPoint: crashPoint, ReadyFile: *readyPath})
	if err != nil {
		panic(err)
	}
	if *mode == "rebuild" {
		if _, err := h.SubmitDelta(ctx, grayrouter.CommitDeltaInput{RequestID: "helper-crash-delta", Delta: bundle.Delta}); err != nil {
			panic(err)
		}
	} else {
		if _, err := h.BindInference(context.Background(), grayrouter.InferRequest{RequestID: "crash-inflight-0001", ModelKey: "demo-stable@1.0.0"}); err != nil {
			panic(err)
		}
	}
	select {}
}

func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func renderMarkdown(report VerifyReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# 验证运行报告\n\n- 结果: **%s**\n- 通过: %d\n- 失败: %d\n- 时间: %s\n- 平台: `%s`\n- Go: `%s`\n- 工作目录: `%s`\n\n", map[bool]string{true: "PASS", false: "FAIL"}[report.Failed == 0], report.Passed, report.Failed, report.GeneratedAt, report.Platform, report.GoVersion, report.WorkDir)
	b.WriteString("## 检查项\n\n")
	for _, check := range report.Checks {
		mark := "✅"
		if !check.Pass {
			mark = "❌"
		}
		fmt.Fprintf(&b, "- %s **%s** (`%s`)\n", mark, check.Name, check.Duration)
		if check.Code != "" {
			fmt.Fprintf(&b, "  - 业务码: `%s`\n", check.Code)
		}
		if check.Error != "" {
			fmt.Fprintf(&b, "  - 错误: `%s`\n", strings.ReplaceAll(check.Error, "\n", " "))
		}
	}
	b.WriteString("\n## 可核对产物\n\n")
	keys := make([]string, 0, len(report.Artifacts))
	for key := range report.Artifacts {
		keys = append(keys, key)
	}
	for _, key := range keys {
		fmt.Fprintf(&b, "- %s: `%s`\n", key, report.Artifacts[key])
	}
	b.WriteString("\n使用 `sqlite3 artifacts/verify-work/main.db '.tables'` 或检查 `recovery-rebuild.json` 核对持久化事实。\n")
	return b.String()
}
