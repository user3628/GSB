package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"hostacl-workbench/internal/apperr"
	"hostacl-workbench/internal/engine"
	"hostacl-workbench/internal/model"
)

type Options struct {
	WorkDir            string
	Keep               bool
	ReportPath         string
	RecoveryReportPath string
	MaxConcurrency     int
	QueueSize          int
	VerifyBudget       time.Duration
}

type Result struct {
	ReportPath         string
	RecoveryReportPath string
	Checks             int
	FinalVersion       int64
}

type Step struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Details any    `json:"details,omitempty"`
	Error   string `json:"error,omitempty"`
}

type CrashEvidence struct {
	RequestError    string `json:"request_error"`
	ExitCode        int    `json:"exit_code"`
	ExitError       string `json:"exit_error,omitempty"`
	PendingIntent   any    `json:"pending_intent,omitempty"`
	PostRecoverHead any    `json:"post_recover_head,omitempty"`
}

type Report struct {
	Product            string                    `json:"product"`
	Status             string                    `json:"status"`
	StartedAt          string                    `json:"started_at"`
	FinishedAt         string                    `json:"finished_at"`
	Environment        map[string]any            `json:"environment"`
	WorkDir            string                    `json:"work_dir"`
	ManagedRoot        string                    `json:"managed_root"`
	Database           string                    `json:"database"`
	Steps              []Step                    `json:"steps"`
	Checks             int                       `json:"checks"`
	InitialRecovery    engine.RecoveryReport     `json:"initial_recovery"`
	Cascades           []model.CascadeReport     `json:"cascades"`
	Reads              map[string]any            `json:"reads"`
	Crash              CrashEvidence             `json:"crash_recovery"`
	Recovery           json.RawMessage           `json:"recovery_report,omitempty"`
	Verification       *model.VerificationReport `json:"verification,omitempty"`
	ErrorCodesObserved []string                  `json:"error_codes_observed"`
	FinalHead          map[string]any            `json:"final_head"`
	Cleanup            map[string]any            `json:"cleanup"`
	Failure            string                    `json:"failure,omitempty"`
}

type runner struct {
	opts         Options
	report       *Report
	workDir      string
	root         string
	db           string
	reportPath   string
	recoveryPath string
}

type httpReply struct {
	Status int
	Body   []byte
	Err    error
}

func Run(ctx context.Context, opts Options) (result Result, runErr error) {
	if opts.MaxConcurrency <= 0 {
		opts.MaxConcurrency = 6
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = 256
	}
	if opts.VerifyBudget <= 0 {
		opts.VerifyBudget = 2 * time.Minute
	}
	if opts.ReportPath == "" {
		opts.ReportPath = "artifacts/acceptance-report.json"
	}
	if opts.RecoveryReportPath == "" {
		opts.RecoveryReportPath = "artifacts/recovery-report.json"
	}
	r := &runner{opts: opts}
	started := time.Now().UTC()
	r.report = &Report{
		Product: "hostacl-workbench", Status: "running", StartedAt: started.Format(time.RFC3339Nano),
		Steps: []Step{}, Cascades: []model.CascadeReport{}, Reads: map[string]any{},
		ErrorCodesObserved: []string{},
		Cleanup:            map[string]any{},
	}
	r.report.Environment = map[string]any{
		"os": runtime.GOOS, "arch": runtime.GOARCH, "go_version": runtime.Version(),
		"effective_uid": os.Geteuid(), "pid": os.Getpid(),
	}
	defer func() {
		r.report.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		r.report.Checks = countPassed(r.report.Steps)
		if runErr != nil {
			r.report.Status = "failed"
			r.report.Failure = runErr.Error()
			r.report.Steps = append(r.report.Steps, Step{Name: "acceptance", Status: "fail", Error: runErr.Error()})
		} else if r.report.Status == "running" {
			r.report.Status = "passed"
		}
		if r.reportPath != "" {
			if err := writeJSON(r.reportPath, r.report); err != nil && runErr == nil {
				runErr = err
			}
			result.ReportPath = r.reportPath
		}
		result.Checks = r.report.Checks
		switch head := r.report.FinalHead["version"].(type) {
		case float64:
			result.FinalVersion = int64(head)
		case int64:
			result.FinalVersion = head
		case int:
			result.FinalVersion = int64(head)
		}
	}()
	runErr = r.run(ctx)
	result.RecoveryReportPath = r.recoveryPath
	return
}

func (r *runner) run(ctx context.Context) error {
	if runtime.GOOS != "linux" && runtime.GOOS != "windows" {
		return fmt.Errorf("acceptance requires Linux or Windows, got %s", runtime.GOOS)
	}
	if runtime.GOOS == "linux" && os.Geteuid() != 0 {
		return fmt.Errorf("Linux acceptance requires root so real per-principal UID opens can be verified; run with sudo")
	}
	if r.opts.WorkDir == "" {
		wd, err := os.MkdirTemp("", "hostacl-workbench-acceptance-*")
		if err != nil {
			return err
		}
		if err := os.Chmod(wd, 0o711); err != nil {
			return err
		}
		r.workDir = wd
	} else {
		abs, err := filepath.Abs(r.opts.WorkDir)
		if err != nil {
			return err
		}
		r.workDir = abs
		if err := os.MkdirAll(r.workDir, 0o711); err != nil {
			return err
		}
		if err := os.Chmod(r.workDir, 0o711); err != nil {
			return err
		}
	}
	r.root = filepath.Join(r.workDir, "managed-root")
	r.db = filepath.Join(r.workDir, "workbench.db")
	if err := os.MkdirAll(r.root, 0o711); err != nil {
		return err
	}
	r.report.WorkDir = r.workDir
	r.report.ManagedRoot = r.root
	r.report.Database = r.db
	r.reportPath = absPath(r.opts.ReportPath)
	r.recoveryPath = absPath(r.opts.RecoveryReportPath)
	if !r.opts.Keep {
		defer func() {
			_ = engine.CleanupRoot(r.root)
			if err := os.RemoveAll(r.workDir); err != nil {
				r.report.Cleanup["error"] = err.Error()
			} else {
				r.report.Cleanup["removed_work_dir"] = r.workDir
			}
		}()
	}
	sample := model.DeterministicSample()
	eng, err := engine.New(engine.Config{
		Root: r.root, DBPath: r.db, MaxConcurrency: r.opts.MaxConcurrency,
		QueueSize: r.opts.QueueSize, VerifyBudget: r.opts.VerifyBudget,
	})
	if err != nil {
		return err
	}
	initialRecovery, err := eng.Recover(ctx)
	if err != nil {
		_ = eng.Close()
		return err
	}
	r.report.InitialRecovery = initialRecovery
	if err := r.check("startup recovery completes", initialRecovery.Status == "complete" && len(initialRecovery.PendingIntents) == 0, initialRecovery); err != nil {
		_ = eng.Close()
		return err
	}

	invalid := model.ClonePolicy(sample.SnapshotV1)
	invalid.Paths[0].Path = "../escape"
	invalid.Digest = model.SnapshotDigest(invalid)
	if _, err := eng.SubmitSnapshot(ctx, *invalid); err == nil || errorCode(err) != string(apperr.SpecInvalid) {
		_ = eng.Close()
		return fmt.Errorf("invalid path was not rejected with E_SPEC_INVALID: %v", err)
	}
	r.observeCode(apperr.SpecInvalid)
	if err := r.check("invalid specification rejected atomically", true, map[string]any{"code": apperr.SpecInvalid}); err != nil {
		_ = eng.Close()
		return err
	}

	commit1, err := eng.SubmitSnapshot(ctx, *sample.SnapshotV1)
	if err != nil {
		_ = eng.Close()
		return err
	}
	if err := r.check("snapshot v1 materialized with ACL readback", commit1.Version == 1 && commit1.Apply.ReadbackDigest != "", commit1); err != nil {
		_ = eng.Close()
		return err
	}
	read1, err := eng.Read(ctx, "u-api", "data", 1)
	if err != nil {
		_ = eng.Close()
		return err
	}
	r.report.Reads["v1_u-api_data"] = read1
	if err := r.check("v1 direct grant gives u-api write but not read on data", !read1.View.Rights.Read && read1.View.Rights.Write && read1.UsedVersion >= 1, read1); err != nil {
		_ = eng.Close()
		return err
	}
	deepBefore, err := eng.Read(ctx, "g-platform", "apps/api/deep/state.json", 1)
	if err != nil {
		_ = eng.Close()
		return err
	}
	r.report.Reads["v1_g-platform_deep"] = deepBefore
	if err := r.check("v1 nested group inherits read on deep file", deepBefore.View.Rights.Read, deepBefore); err != nil {
		_ = eng.Close()
		return err
	}

	commit2, err := eng.SubmitIncrement(ctx, *sample.IncrementV2)
	if err != nil {
		_ = eng.Close()
		return err
	}
	r.report.Cascades = append(r.report.Cascades, commit2.Cascade)
	if !contains(commit2.Cascade.AffectedSubjects, "u-api") || !contains(commit2.Cascade.AffectedPaths, "data") {
		_ = eng.Close()
		return fmt.Errorf("v2 cascade did not identify u-api/data: %+v", commit2.Cascade)
	}
	if err := r.check("group nesting change cascades to u-api/data", true, commit2.Cascade); err != nil {
		_ = eng.Close()
		return err
	}
	read2, err := eng.Read(ctx, "u-api", "data", 2)
	if err != nil {
		_ = eng.Close()
		return err
	}
	r.report.Reads["v2_u-api_data"] = read2
	if err := r.check("v2 nested group membership grants u-api read on data", read2.View.Rights.Read && read2.View.Rights.Write, read2); err != nil {
		_ = eng.Close()
		return err
	}

	commit3, err := eng.SubmitIncrement(ctx, *sample.IncrementV3)
	if err != nil {
		_ = eng.Close()
		return err
	}
	r.report.Cascades = append(r.report.Cascades, commit3.Cascade)
	deepAfter, err := eng.Read(ctx, "g-platform", "apps/api/deep/state.json", 3)
	if err != nil {
		_ = eng.Close()
		return err
	}
	r.report.Reads["v3_g-platform_deep"] = deepAfter
	if !contains(commit3.Cascade.AffectedSubjects, "g-platform") || deepAfter.View.Rights.Read {
		_ = eng.Close()
		return fmt.Errorf("v3 inheritance change did not invalidate g-platform deep view: cascade=%+v view=%+v", commit3.Cascade, deepAfter.View)
	}
	if err := r.check("inheritance change invalidates downstream view", true, map[string]any{"cascade": commit3.Cascade, "view": deepAfter.View}); err != nil {
		_ = eng.Close()
		return err
	}
	if err := eng.Close(); err != nil {
		return err
	}

	exe, err := os.Executable()
	if err != nil {
		return err
	}
	crashAddr, err := freeAddr()
	if err != nil {
		return err
	}
	crashLog := filepath.Join(r.workDir, "crash-server.log")
	crashCmd, crashLogFile, err := startServer(exe, r.root, r.db, crashAddr, r.opts, crashLog, "", true)
	if err != nil {
		return err
	}
	defer func() {
		if crashCmd.Process != nil {
			_ = crashCmd.Process.Kill()
		}
		_ = crashLogFile.Close()
	}()
	if err := waitHealth(crashAddr, crashCmd, 15*time.Second); err != nil {
		return fmt.Errorf("crash server did not start: %w\n%s", err, tailFile(crashLog))
	}
	crashURL := "http://" + crashAddr
	v4Reply := postJSON(httpURL(crashURL, "/v1/increments"), sample.IncrementV4, 8*time.Second)
	if v4Reply.Err == nil && v4Reply.Status == http.StatusOK {
		return fmt.Errorf("crash hook did not terminate the mutation before commit")
	}
	r.report.Crash.RequestError = replyError(v4Reply)
	crashDone := make(chan error, 1)
	go func() { crashDone <- crashCmd.Wait() }()
	select {
	case waitErr := <-crashDone:
		if waitErr == nil {
			r.report.Crash.ExitCode = 0
		} else {
			var exitErr *exec.ExitError
			if errors.As(waitErr, &exitErr) {
				r.report.Crash.ExitCode = exitErr.ExitCode()
				r.report.Crash.ExitError = waitErr.Error()
			} else {
				return waitErr
			}
		}
	case <-time.After(15 * time.Second):
		return fmt.Errorf("crash server did not exit")
	}
	if err := crashLogFile.Close(); err != nil {
		return err
	}
	if err := r.check("forced termination observed after materialization", r.report.Crash.ExitCode != 0, r.report.Crash); err != nil {
		return err
	}

	normalAddr, err := freeAddr()
	if err != nil {
		return err
	}
	normalLog := filepath.Join(r.workDir, "server.log")
	recoveryOut := filepath.Join(r.workDir, "startup-recovery.json")
	serverCmd, serverLogFile, err := startServer(exe, r.root, r.db, normalAddr, r.opts, normalLog, recoveryOut, false)
	if err != nil {
		return err
	}
	defer func() {
		if serverCmd.Process != nil {
			_ = serverCmd.Process.Kill()
		}
		_ = serverLogFile.Close()
	}()
	if err := waitHealth(normalAddr, serverCmd, 20*time.Second); err != nil {
		return fmt.Errorf("recovered server did not start: %w\n%s", err, tailFile(normalLog))
	}
	baseURL := "http://" + normalAddr
	stateAfterCrash := getJSON(httpURL(baseURL, "/v1/state"), 10*time.Second)
	if stateAfterCrash.Err != nil || stateAfterCrash.Status != 200 {
		return fmt.Errorf("state after recovery failed: %s", replyError(stateAfterCrash))
	}
	var stateDoc struct {
		Head storeHead `json:"head"`
	}
	_ = json.Unmarshal(stateAfterCrash.Body, &stateDoc)
	r.report.Crash.PostRecoverHead = stateDoc.Head
	if stateDoc.Head.Version != 3 {
		return fmt.Errorf("recovery exposed version %d, want last complete version 3", stateDoc.Head.Version)
	}
	recoveryReply := getJSON(httpURL(baseURL, "/v1/recovery"), 10*time.Second)
	if recoveryReply.Err != nil || recoveryReply.Status != 200 {
		return fmt.Errorf("recovery report endpoint failed: %s", replyError(recoveryReply))
	}
	var recovered engine.RecoveryReport
	if err := json.Unmarshal(recoveryReply.Body, &recovered); err != nil {
		return err
	}
	r.report.Recovery = append(json.RawMessage(nil), recoveryReply.Body...)
	if recovered.LastCompleteVersion != 3 || len(recovered.PendingIntents) == 0 || recovered.PendingIntents[0].Action != "abort_pending_intent" || len(recovered.Facts) == 0 {
		return fmt.Errorf("recovery report is incomplete: %+v", recovered)
	}
	if err := os.WriteFile(r.recoveryPath, append(pretty(recoveryReply.Body), '\n'), 0o644); err == nil {
		r.report.Crash.PendingIntent = recovered.PendingIntents
	} else {
		return err
	}
	if err := r.check("restart recovery aborts uncommitted intent and keeps v3", stateDoc.Head.Version == 3, map[string]any{"state": stateDoc.Head, "recovery": recovered}); err != nil {
		return err
	}

	invalidHTTP := postJSON(httpURL(baseURL, "/v1/snapshots"), invalid, 10*time.Second)
	if invalidHTTP.Status != http.StatusBadRequest || responseCode(invalidHTTP.Body) != string(apperr.SpecInvalid) {
		return fmt.Errorf("HTTP E_SPEC_INVALID contract failed: status=%d body=%s", invalidHTTP.Status, invalidHTTP.Body)
	}
	r.observeCode(apperr.SpecInvalid)
	if err := r.check("HTTP E_SPEC_INVALID response is structured JSON", true, json.RawMessage(invalidHTTP.Body)); err != nil {
		return err
	}

	v4Reply = postJSON(httpURL(baseURL, "/v1/increments"), sample.IncrementV4, 30*time.Second)
	if v4Reply.Err != nil || v4Reply.Status != 200 {
		return fmt.Errorf("v4 commit after recovery failed: %s", replyError(v4Reply))
	}
	stateV4 := getJSON(httpURL(baseURL, "/v1/state"), 10*time.Second)
	var v4Doc struct {
		Head storeHead `json:"head"`
	}
	_ = json.Unmarshal(stateV4.Body, &v4Doc)
	if v4Doc.Head.Version != 4 {
		return fmt.Errorf("v4 was not committed, head=%d", v4Doc.Head.Version)
	}
	if err := r.check("recovered workbench accepts the previously aborted v4 increment", true, v4Doc.Head); err != nil {
		return err
	}
	queueReq := map[string]any{"max_concurrency": 1, "queue_size": 1, "subject_ids": []string{"u-api", "svc-indexer"}, "paths": []string{"data", "data/private.json"}}
	queueReply := postJSON(httpURL(baseURL, "/v1/verify"), queueReq, 20*time.Second)
	if queueReply.Status != http.StatusTooManyRequests || responseCode(queueReply.Body) != string(apperr.VerifyQueueFull) {
		return fmt.Errorf("queue limit contract failed: status=%d body=%s", queueReply.Status, queueReply.Body)
	}
	r.observeCode(apperr.VerifyQueueFull)
	if err := r.check("bounded verification queue returns E_VERIFY_QUEUE_FULL", true, json.RawMessage(queueReply.Body)); err != nil {
		return err
	}
	degradedReply := postJSON(httpURL(baseURL, "/v1/verify"), map[string]any{"max_concurrency": 1, "queue_size": 256, "budget_ms": 1}, 30*time.Second)
	if degradedReply.Status != 200 {
		return fmt.Errorf("budget degradation call failed: %s", replyError(degradedReply))
	}
	var degraded model.VerificationReport
	if err := json.Unmarshal(degradedReply.Body, &degraded); err != nil {
		return err
	}
	if !degraded.Degraded || len(degraded.Skipped) == 0 || !stablePriorityOrder(degraded.Skipped) {
		return fmt.Errorf("time budget did not produce a stable degradation list: %+v", degraded)
	}
	if err := r.check("time budget produces deterministic skipped-path list", true, map[string]any{"skipped": len(degraded.Skipped), "status": degraded.Status}); err != nil {
		return err
	}
	staleReply := postJSON(httpURL(baseURL, "/v1/reads"), map[string]any{"subject_id": "u-api", "path": "data", "observed_version": 999}, 10*time.Second)
	if staleReply.Status != http.StatusConflict || responseCode(staleReply.Body) != string(apperr.StaleRead) {
		return fmt.Errorf("stale read contract failed: status=%d body=%s", staleReply.Status, staleReply.Body)
	}
	r.observeCode(apperr.StaleRead)
	r.report.Reads["stale_rejected"] = json.RawMessage(staleReply.Body)
	if err := r.check("version-bound read rejects E_STALE_READ", true, json.RawMessage(staleReply.Body)); err != nil {
		return err
	}
	verifyReply := postJSON(httpURL(baseURL, "/v1/verify"), map[string]any{}, r.opts.VerifyBudget+30*time.Second)
	if verifyReply.Status != 200 {
		return fmt.Errorf("full verification failed: status=%d body=%s", verifyReply.Status, verifyReply.Body)
	}
	var verification model.VerificationReport
	if err := json.Unmarshal(verifyReply.Body, &verification); err != nil {
		return err
	}
	r.report.Verification = &verification
	allowed, denied := false, false
	for _, result := range verification.Results {
		if result.Actual == "allowed" {
			allowed = true
		}
		if result.Actual == "denied" && result.KernelDenied {
			denied = true
		}
	}
	if verification.Mismatches != 0 || len(verification.Results) != 72 || !allowed || !denied {
		return fmt.Errorf("verification incomplete or inconsistent: results=%d mismatches=%d allowed=%t kernelDenied=%t skipped=%d", len(verification.Results), verification.Mismatches, allowed, denied, len(verification.Skipped))
	}
	if err := r.check("all principals opened every managed path for read and write with kernel decisions", true, map[string]any{
		"results": len(verification.Results), "allowed": allowed, "kernel_denied": denied, "run_id": verification.RunID,
	}); err != nil {
		return err
	}
	applyReply := postJSON(httpURL(baseURL, "/v1/apply"), map[string]any{}, 30*time.Second)
	if applyReply.Status != 200 {
		return fmt.Errorf("reapply failed: %s", replyError(applyReply))
	}
	if err := r.check("ACL apply and readback can be repeated", true, json.RawMessage(applyReply.Body)); err != nil {
		return err
	}

	stateForTamper := getJSON(httpURL(baseURL, "/v1/state"), 10*time.Second)
	var tamperState struct {
		Head storeHead `json:"head"`
	}
	_ = json.Unmarshal(stateForTamper.Body, &tamperState)
	tamperPath := filepath.Join(tamperState.Head.TreePath, "data", "private.json")
	if err := os.Chmod(tamperPath, 0); err != nil {
		return err
	}
	mismatchReply := postJSON(httpURL(baseURL, "/v1/verify"), map[string]any{"subject_ids": []string{"u-blocked"}, "paths": []string{"data/private.json"}}, 30*time.Second)
	if mismatchReply.Status != http.StatusConflict || responseCode(mismatchReply.Body) != string(apperr.ApplyMismatch) {
		return fmt.Errorf("ACL mismatch contract failed: status=%d body=%s", mismatchReply.Status, mismatchReply.Body)
	}
	r.observeCode(apperr.ApplyMismatch)
	if err := r.check("tampered ACL is detected as E_APPLY_MISMATCH by real opens", true, json.RawMessage(mismatchReply.Body)); err != nil {
		return err
	}
	repairReply := postJSON(httpURL(baseURL, "/v1/apply"), map[string]any{}, 30*time.Second)
	if repairReply.Status != 200 {
		return fmt.Errorf("ACL repair failed: %s", replyError(repairReply))
	}

	inc5 := v5Increment()
	type concurrentResult struct {
		status int
		body   []byte
		err    error
	}
	start := make(chan struct{})
	results := make(chan concurrentResult, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			reply := postJSON(httpURL(baseURL, "/v1/increments"), inc5, 30*time.Second)
			results <- concurrentResult{status: reply.Status, body: reply.Body, err: reply.Err}
		}()
	}
	close(start)
	concurrent := []concurrentResult{<-results, <-results}
	sort.Slice(concurrent, func(i, j int) bool { return concurrent[i].status < concurrent[j].status })
	if concurrent[0].status != 200 || concurrent[1].status != http.StatusConflict || responseCode(concurrent[1].body) != string(apperr.StaleBase) {
		return fmt.Errorf("concurrent stale-base contract failed: %+v", concurrent)
	}
	r.observeCode(apperr.StaleBase)
	if err := r.check("two increments on one base allow exactly one commit and reject the other", true, map[string]any{
		"winner_status": concurrent[0].status, "loser_status": concurrent[1].status,
		"loser_body": json.RawMessage(concurrent[1].body),
	}); err != nil {
		return err
	}
	finalState := getJSON(httpURL(baseURL, "/v1/state"), 10*time.Second)
	var finalDoc struct {
		Head storeHead `json:"head"`
	}
	_ = json.Unmarshal(finalState.Body, &finalDoc)
	if finalDoc.Head.Version != 5 {
		return fmt.Errorf("final head version=%d, want 5", finalDoc.Head.Version)
	}
	r.report.FinalHead = map[string]any{
		"version": finalDoc.Head.Version, "digest": finalDoc.Head.Digest,
		"view_digest": finalDoc.Head.ViewDigest, "tree_path": finalDoc.Head.TreePath,
	}
	if err := r.check("final committed state is version 5", true, r.report.FinalHead); err != nil {
		return err
	}
	verificationStatusReply := getJSON(httpURL(baseURL, "/v1/verifications?run_id="+verification.RunID), 10*time.Second)
	if verificationStatusReply.Status != 200 {
		return fmt.Errorf("verification status query failed: %s", replyError(verificationStatusReply))
	}
	var verificationStatus struct {
		Status  string `json:"status"`
		Version int64  `json:"version"`
	}
	if err := json.Unmarshal(verificationStatusReply.Body, &verificationStatus); err != nil {
		return err
	}
	if verificationStatus.Status != "superseded" || verificationStatus.Version != 4 {
		return fmt.Errorf("old verification run was not superseded atomically: %s", verificationStatusReply.Body)
	}
	if err := r.check("prior verification conclusions are superseded by the v5 commit", true, verificationStatus); err != nil {
		return err
	}
	if err := serverCmd.Process.Kill(); err != nil {
		return err
	}
	_ = serverCmd.Wait()
	if err := serverLogFile.Close(); err != nil {
		return err
	}
	r.report.Status = "passed"
	return nil
}

type storeHead struct {
	Version     int64  `json:"version"`
	Digest      string `json:"digest"`
	ViewDigest  string `json:"view_digest"`
	TreePath    string `json:"tree_path"`
	GeneratedAt string `json:"generated_at,omitempty"`
}

func (r *runner) check(name string, ok bool, details any) error {
	status := "pass"
	if !ok {
		status = "fail"
	}
	step := Step{Name: name, Status: status, Details: details}
	r.report.Steps = append(r.report.Steps, step)
	if !ok {
		return fmt.Errorf("check failed: %s", name)
	}
	return nil
}

func (r *runner) observeCode(code apperr.Code) {
	value := string(code)
	if !contains(r.report.ErrorCodesObserved, value) {
		r.report.ErrorCodesObserved = append(r.report.ErrorCodesObserved, value)
		sort.Strings(r.report.ErrorCodesObserved)
	}
}

func v5Increment() *model.Increment {
	inc := &model.Increment{
		BaseVersion: 4, Version: 5, GeneratedAt: "2026-11-25T00:00:05Z",
		PathChanges: []model.PathChange{{Op: "upsert", Path: &model.ManagedPath{
			Path: "data", Kind: "directory", ACL: []model.ACE{
				{SubjectID: "u-api", Effect: "allow", Rights: []string{"read", "write"}},
				{SubjectID: "g-platform", Effect: "allow", Rights: []string{"read"}},
				{SubjectID: "u-blocked", Effect: "allow", Rights: []string{"read"}},
			},
		}}},
	}
	inc.Digest = model.IncrementDigest(inc)
	return inc
}

func startServer(exe, root, db, addr string, opts Options, logPath, recoveryOut string, crash bool) (*exec.Cmd, *os.File, error) {
	args := []string{
		"serve", "--root", root, "--db", db, "--addr", addr,
		"--max-concurrency", fmt.Sprint(opts.MaxConcurrency),
		"--queue-size", fmt.Sprint(opts.QueueSize),
		"--budget-ms", fmt.Sprint(opts.VerifyBudget.Milliseconds()),
	}
	if recoveryOut != "" {
		args = append(args, "--recovery-out", recoveryOut)
	}
	if crash {
		args = append(args, "--crash-after-materialize")
	}
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, nil, err
	}
	cmd := exec.Command(exe, args...)
	cmd.Stdout = file
	cmd.Stderr = file
	if err := cmd.Start(); err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	return cmd, file, nil
}

func waitHealth(addr string, cmd *exec.Cmd, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	url := "http://" + addr + "/healthz"
	for time.Now().Before(deadline) {
		reply := getJSON(url, 700*time.Millisecond)
		if reply.Err == nil && reply.Status == 200 {
			return nil
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			return fmt.Errorf("server exited early: %v", cmd.ProcessState)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("health check timed out at %s", url)
}

func freeAddr() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := listener.Addr().String()
	return addr, listener.Close()
}

func getJSON(url string, timeout time.Duration) httpReply {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(url)
	if err != nil {
		return httpReply{Err: err}
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	return httpReply{Status: resp.StatusCode, Body: body, Err: readErr}
}

func postJSON(url string, value any, timeout time.Duration) httpReply {
	raw, err := json.Marshal(value)
	if err != nil {
		return httpReply{Err: err}
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		return httpReply{Err: err}
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	return httpReply{Status: resp.StatusCode, Body: body, Err: readErr}
}

func httpURL(base, path string) string { return strings.TrimRight(base, "/") + path }

func responseCode(body []byte) string {
	var doc struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(body, &doc)
	return doc.Code
}

func errorCode(err error) string {
	var appErr *apperr.Error
	if errors.As(err, &appErr) {
		return string(appErr.Code)
	}
	return ""
}

func replyError(reply httpReply) string {
	if reply.Err != nil {
		return reply.Err.Error()
	}
	return fmt.Sprintf("status=%d body=%s", reply.Status, string(reply.Body))
}

func tailFile(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	if len(raw) > 8000 {
		raw = raw[len(raw)-8000:]
	}
	return string(raw)
}

func stablePriorityOrder(skipped []model.SkippedTask) bool {
	for i := 1; i < len(skipped); i++ {
		a, b := skipped[i-1], skipped[i]
		if a.Priority < b.Priority || (a.Priority == b.Priority && a.SubjectID > b.SubjectID) || (a.Priority == b.Priority && a.SubjectID == b.SubjectID && a.Path > b.Path) {
			return false
		}
	}
	return true
}

func contains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func countPassed(steps []Step) int {
	count := 0
	for _, step := range steps {
		if step.Status == "pass" {
			count++
		}
	}
	return count
}

func writeJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

func pretty(raw []byte) []byte {
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return raw
	}
	return out.Bytes()
}

func absPath(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}
