package scenario

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	mfaengine "mfaengine"
)

type Options struct {
	WorkDir    string
	Executable string
}

type runner struct {
	ctx       context.Context
	options   Options
	scenarios []mfaengine.ScenarioResult
}

func Run(ctx context.Context, options Options) (mfaengine.DiagnosticReport, error) {
	if options.WorkDir == "" {
		return mfaengine.DiagnosticReport{}, fmt.Errorf("scenario work directory is required")
	}
	if err := os.MkdirAll(options.WorkDir, 0o755); err != nil {
		return mfaengine.DiagnosticReport{}, err
	}
	mainDB := filepath.Join(options.WorkDir, "core.db")
	if err := resetDatabase(mainDB); err != nil {
		return mfaengine.DiagnosticReport{}, err
	}
	engine, err := mfaengine.NewEngine(ctx, mainDB, mfaengine.EngineOptions{})
	if err != nil {
		return mfaengine.DiagnosticReport{}, err
	}
	defer engine.Close()
	if err := engine.Register(ctx, DemoConfig()); err != nil {
		return mfaengine.DiagnosticReport{}, err
	}
	r := &runner{ctx: ctx, options: options, scenarios: make([]mfaengine.ScenarioResult, 0)}
	r.runAuthenticationScenarios(engine)
	if err := r.runRebindScenario(engine); err != nil {
		return mfaengine.DiagnosticReport{}, err
	}
	r.runBudgetScenario(engine)
	r.runPolicyValidationScenario(engine)
	r.runActivationScenario(engine)

	faults, err := runSnapshotFaults(ctx, options.WorkDir)
	if err != nil {
		return mfaengine.DiagnosticReport{}, err
	}
	recovery, err := runCrashRecovery(ctx, options)
	if err != nil {
		return mfaengine.DiagnosticReport{}, err
	}
	return engine.BuildDiagnosticReport(ctx, r.scenarios, faults, recovery)
}

func resetDatabase(path string) error {
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if err := os.Remove(path + suffix); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (r *runner) add(name string, expected mfaengine.Code, actual mfaengine.Code, passed bool, tenantID, userID string, factorIDs []string, before, after int64, attemptID, detail string) {
	if factorIDs == nil {
		factorIDs = []string{}
	}
	r.scenarios = append(r.scenarios, mfaengine.ScenarioResult{
		Name:           name,
		ExpectedCode:   expected,
		ActualCode:     actual,
		Passed:         passed,
		TenantID:       tenantID,
		UserID:         userID,
		FactorIDs:      append([]string(nil), factorIDs...),
		SnapshotBefore: before,
		SnapshotAfter:  after,
		AttemptID:      attemptID,
		Detail:         detail,
	})
}

func currentVersion(engine *mfaengine.Engine) int64 {
	return engine.SnapshotStatus(context.Background()).LatestValid
}

func factorState(engine *mfaengine.Engine, factorID string) (mfaengine.FactorReport, bool) {
	reports, err := engine.ListFactorReports(context.Background())
	if err != nil {
		return mfaengine.FactorReport{}, false
	}
	for _, report := range reports {
		if report.ID == factorID {
			return report, true
		}
	}
	return mfaengine.FactorReport{}, false
}

func (r *runner) runAuthenticationScenarios(engine *mfaengine.Engine) {
	successBefore := currentVersion(engine)
	success, successErr := engine.Authenticate(r.ctx, mfaengine.AuthRequest{
		TenantID: "acme", UserID: "alice", SessionID: "session-acme-success", AttemptID: "attempt-acme-success",
	})
	successCode := mfaengine.ErrorCode(successErr)
	recovery, _ := factorState(engine, "f-alice-recovery")
	successPass := successCode == mfaengine.CodeOK && success.SnapshotVersion == successBefore+1 && recovery.RecoveryCodesConsumed == 1
	r.add("01_challenge_response_success", mfaengine.CodeOK, successCode, successPass, "acme", "alice", success.FactorIDs, successBefore, success.SnapshotVersion, "attempt-acme-success", "TOTP and one-time recovery factor both verified")

	replayBefore := currentVersion(engine)
	index := 0
	replay, replayErr := engine.Authenticate(r.ctx, mfaengine.AuthRequest{
		TenantID: "acme", UserID: "alice", SessionID: "session-acme-replay", AttemptID: "attempt-acme-replay",
		RecoveryCodeIndex: &index,
	})
	replayCode := mfaengine.ErrorCode(replayErr)
	recovery, _ = factorState(engine, "f-alice-recovery")
	replayPass := replayCode == mfaengine.CodeConsumed && replay.SnapshotVersion == replayBefore+1 && recovery.RecoveryCodesConsumed == 1
	r.add("02_recovery_code_duplicate_consumption", mfaengine.CodeConsumed, replayCode, replayPass, "acme", "alice", replay.FactorIDs, replayBefore, replay.SnapshotVersion, "attempt-acme-replay", "same recovery-code index rejected after first use")

	windowBefore := currentVersion(engine)
	window, windowErr := engine.Authenticate(r.ctx, mfaengine.AuthRequest{
		TenantID: "theta", UserID: "tina", SessionID: "session-window", AttemptID: "attempt-window-offset-2", TOTPStepOffset: 2,
	})
	windowCode := mfaengine.ErrorCode(windowErr)
	windowFactor, _ := factorState(engine, "f-tina-totp")
	windowPass := windowCode == mfaengine.CodeChallengeFailed && windowFactor.ConsecutiveFailures == 1 && window.SnapshotVersion == windowBefore+1
	r.add("03_totp_outside_window", mfaengine.CodeChallengeFailed, windowCode, windowPass, "theta", "tina", window.FactorIDs, windowBefore, window.SnapshotVersion, "attempt-window-offset-2", "TOTP step offset 2 is outside configured window 1")

	webBefore := currentVersion(engine)
	web, webErr := engine.Authenticate(r.ctx, mfaengine.AuthRequest{
		TenantID: "iota", UserID: "ian", SessionID: "session-webauthn", AttemptID: "attempt-webauthn-success",
	})
	webCode := mfaengine.ErrorCode(webErr)
	webPass := webCode == mfaengine.CodeOK && web.SnapshotVersion == webBefore+1
	r.add("04_webauthn_assertion_success", mfaengine.CodeOK, webCode, webPass, "iota", "ian", web.FactorIDs, webBefore, web.SnapshotVersion, "attempt-webauthn-success", "deterministic WebAuthn assertion accepted")

	degradeBefore := currentVersion(engine)
	_, firstErr := engine.Authenticate(r.ctx, mfaengine.AuthRequest{TenantID: "gamma", UserID: "gina", AttemptID: "attempt-gina-fail-1", AnswerMode: "invalid"})
	_, secondErr := engine.Authenticate(r.ctx, mfaengine.AuthRequest{TenantID: "gamma", UserID: "gina", AttemptID: "attempt-gina-fail-2", AnswerMode: "invalid"})
	thirdFail, thirdErr := engine.Authenticate(r.ctx, mfaengine.AuthRequest{TenantID: "gamma", UserID: "gina", AttemptID: "attempt-gina-fail-3", AnswerMode: "invalid"})
	degraded, _ := factorState(engine, "f-gina-totp")
	degradePass := mfaengine.ErrorCode(firstErr) == mfaengine.CodeChallengeFailed &&
		mfaengine.ErrorCode(secondErr) == mfaengine.CodeChallengeFailed &&
		mfaengine.ErrorCode(thirdErr) == mfaengine.CodeChallengeFailed &&
		degraded.Status == mfaengine.StatusDegraded && degraded.ConsecutiveFailures == 3
	r.add("05_failure_threshold_degrades_factor", mfaengine.CodeChallengeFailed, mfaengine.ErrorCode(thirdErr), degradePass, "gamma", "gina", thirdFail.FactorIDs, degradeBefore, thirdFail.SnapshotVersion, "attempt-gina-fail-3", "three consecutive failures exceed threshold 2")

	noViableBefore := currentVersion(engine)
	noViable, noViableErr := engine.Authenticate(r.ctx, mfaengine.AuthRequest{TenantID: "gamma", UserID: "gina", AttemptID: "attempt-gina-no-viable"})
	noViableCode := mfaengine.ErrorCode(noViableErr)
	degraded, _ = factorState(engine, "f-gina-totp")
	noViablePass := noViableCode == mfaengine.CodeNoViableFactor && noViable.SnapshotVersion == noViableBefore && degraded.Status == mfaengine.StatusDegraded
	r.add("06_no_viable_factor_after_degradation", mfaengine.CodeNoViableFactor, noViableCode, noViablePass, "gamma", "gina", nil, noViableBefore, noViable.SnapshotVersion, "attempt-gina-no-viable", "degraded factor is excluded and no snapshot is polluted")

	restoreBefore := currentVersion(engine)
	restored, restoreErr := engine.RestoreFactor(r.ctx, mfaengine.RestoreRequest{TenantID: "gamma", UserID: "gina", FactorID: "f-gina-totp", AttemptID: "attempt-gina-restore", Reason: "risk review accepted deterministic sample"})
	afterRestore, restoreAuthErr := engine.Authenticate(r.ctx, mfaengine.AuthRequest{TenantID: "gamma", UserID: "gina", AttemptID: "attempt-gina-after-restore"})
	restorePass := mfaengine.ErrorCode(restoreErr) == mfaengine.CodeOK && mfaengine.ErrorCode(restoreAuthErr) == mfaengine.CodeOK && afterRestore.SnapshotVersion == restoreBefore+2
	r.add("07_factor_restore_trace", mfaengine.CodeOK, mfaengine.ErrorCode(restoreAuthErr), restorePass, "gamma", "gina", afterRestore.FactorIDs, restoreBefore, afterRestore.SnapshotVersion, "attempt-gina-restore", fmt.Sprintf("restore snapshot=%d, reason recorded", restored.SnapshotVersion))

	expireBefore := currentVersion(engine)
	expired, expireErr := engine.ExpireFactors(r.ctx, "eta", "eva", "attempt-eva-expire")
	expiredAuth, expiredAuthErr := engine.Authenticate(r.ctx, mfaengine.AuthRequest{TenantID: "eta", UserID: "eva", AttemptID: "attempt-eva-after-expire"})
	evaFactor, _ := factorState(engine, "f-eva-totp")
	expirePass := mfaengine.ErrorCode(expireErr) == mfaengine.CodeOK && mfaengine.ErrorCode(expiredAuthErr) == mfaengine.CodeNoViableFactor &&
		evaFactor.Status == mfaengine.StatusExpired && expired.SnapshotVersion == expireBefore+1 && expiredAuth.SnapshotVersion == expired.SnapshotVersion
	r.add("08_expired_factor_identification", mfaengine.CodeNoViableFactor, mfaengine.ErrorCode(expiredAuthErr), expirePass, "eta", "eva", expired.FactorIDs, expireBefore, expired.SnapshotVersion, "attempt-eva-expire", "expired factor status and risk trace committed before no-viable decision")

	missingBefore := currentVersion(engine)
	missing, missingErr := engine.Authenticate(r.ctx, mfaengine.AuthRequest{
		TenantID: "epsilon", UserID: "erin", AttemptID: "attempt-missing-factor", FactorHints: []string{"f-does-not-exist"},
	})
	missingCode := mfaengine.ErrorCode(missingErr)
	missingPass := missingCode == mfaengine.CodeNoViableFactor && missing.SnapshotVersion == missingBefore
	r.add("09_missing_factor_identifier", mfaengine.CodeNoViableFactor, missingCode, missingPass, "epsilon", "erin", nil, missingBefore, missing.SnapshotVersion, "attempt-missing-factor", "unknown factor hint rejected without state mutation")
}

func (r *runner) runRebindScenario(engine *mfaengine.Engine) error {
	beforeFactors, err := engine.ListFactors(r.ctx)
	if err != nil {
		return err
	}
	beforeVersion := currentVersion(engine)
	winnerCh := make(chan struct {
		result mfaengine.RebindResult
		err    error
	}, 1)
	go func() {
		result, err := engine.Rebind(r.ctx, mfaengine.RebindRequest{
			TenantID: "delta", UserID: "dave", FactorType: mfaengine.FactorTOTP,
			NewFactorID: "f-dave-totp-v2", AttemptID: "attempt-rebind-winner",
			Sensitivity: mfaengine.SensitivityHigh, HoldMillis: 250,
		})
		winnerCh <- struct {
			result mfaengine.RebindResult
			err    error
		}{result, err}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for !engine.RebindInFlight("delta", "dave", mfaengine.FactorTOTP) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !engine.RebindInFlight("delta", "dave", mfaengine.FactorTOTP) {
		return fmt.Errorf("rebind winner did not enter in-flight state")
	}
	oldUsable, err := engine.UsableFactor(r.ctx, "delta", "dave", "f-dave-totp")
	if err != nil {
		return err
	}
	_, loserErr := engine.Rebind(r.ctx, mfaengine.RebindRequest{
		TenantID: "delta", UserID: "dave", FactorType: mfaengine.FactorTOTP,
		NewFactorID: "f-dave-totp-v2", AttemptID: "attempt-rebind-loser",
		Sensitivity: mfaengine.SensitivityHigh,
	})
	winner := <-winnerCh
	if winner.err != nil {
		return fmt.Errorf("rebind winner: %w", winner.err)
	}
	loserCode := mfaengine.ErrorCode(loserErr)
	afterFactors, err := engine.ListFactors(r.ctx)
	if err != nil {
		return err
	}
	newFactor, newFound := factorState(engine, "f-dave-totp-v2")
	oldPresent := false
	for _, factor := range afterFactors {
		if factor.ID == "f-dave-totp" {
			oldPresent = true
		}
	}
	pass := loserCode == mfaengine.CodeRebindContention && winner.result.Code == mfaengine.CodeOK && oldUsable &&
		len(afterFactors) == len(beforeFactors) && !oldPresent && newFound && newFactor.Status == mfaengine.StatusActive
	detail := fmt.Sprintf("winner=%s loser=%s old_usable_during_flight=%t factor_count=%d", winner.result.Code, loserCode, oldUsable, len(afterFactors))
	r.add("10_rebind_mutex_and_replacement", mfaengine.CodeRebindContention, loserCode, pass, "delta", "dave", []string{"f-dave-totp-v2"}, beforeVersion, winner.result.SnapshotVersion, "attempt-rebind-loser", detail)
	return nil
}

func (r *runner) runBudgetScenario(engine *mfaengine.Engine) {
	beforeVersion := currentVersion(engine)
	firstCh := make(chan error, 1)
	go func() {
		_, err := engine.Authenticate(r.ctx, mfaengine.AuthRequest{
			TenantID: "zeta", UserID: "zoe", SessionID: "session-budget-1", AttemptID: "attempt-budget-1", HoldMillis: 250,
		})
		firstCh <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for engine.PendingAuth("zeta", "zoe") == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	second, secondErr := engine.Authenticate(r.ctx, mfaengine.AuthRequest{
		TenantID: "zeta", UserID: "zoe", SessionID: "session-budget-2", AttemptID: "attempt-budget-2",
	})
	firstErr := <-firstCh
	zoeFactor, _ := factorState(engine, "f-zoe-totp")
	code := mfaengine.ErrorCode(secondErr)
	pass := code == mfaengine.CodeAuthBudget && mfaengine.ErrorCode(firstErr) == mfaengine.CodeOK &&
		second.SnapshotVersion == beforeVersion && zoeFactor.SuccessCount == 1
	r.add("11_per_user_auth_budget", mfaengine.CodeAuthBudget, code, pass, "zeta", "zoe", nil, beforeVersion, currentVersion(engine), "attempt-budget-2", "rejected request did not change factor state or snapshot")
}

func (r *runner) runPolicyValidationScenario(engine *mfaengine.Engine) {
	beforeFactorCount := 0
	if factors, err := engine.ListFactors(r.ctx); err == nil {
		beforeFactorCount = len(factors)
	}
	beforeVersion := currentVersion(engine)
	cfg := DemoConfig()
	cfg.Factors = append(cfg.Factors, mfaengine.FactorDeclaration{
		ID: "f-alice-totp", TenantID: "acme", UserID: "alice", Type: mfaengine.FactorTOTP,
		Sensitivity: mfaengine.SensitivityHigh, Status: mfaengine.StatusPending,
	})
	_, validationErr := mfaengine.ValidateConfig(cfg)
	afterFactors, _ := engine.ListFactors(r.ctx)
	afterVersion := currentVersion(engine)
	code := mfaengine.ErrorCode(validationErr)
	pass := code == mfaengine.CodePolicyMalformed && beforeFactorCount == len(afterFactors) && beforeVersion == afterVersion
	r.add("12_policy_malformed_atomic", mfaengine.CodePolicyMalformed, code, pass, "acme", "alice", nil, beforeVersion, afterVersion, "", "duplicate factor identifier rejected before any database write")
}

func (r *runner) runActivationScenario(engine *mfaengine.Engine) {
	beforeVersion := currentVersion(engine)
	activation, activationErr := engine.ActivateFactor(r.ctx, mfaengine.ActivationRequest{
		TenantID: "kappa", UserID: "kay", FactorID: "f-kay-totp", AttemptID: "attempt-kay-activate",
	})
	auth, authErr := engine.Authenticate(r.ctx, mfaengine.AuthRequest{
		TenantID: "kappa", UserID: "kay", AttemptID: "attempt-kay-after-activate",
	})
	activeFactor, found := factorState(engine, "f-kay-totp")
	code := mfaengine.ErrorCode(authErr)
	pass := mfaengine.ErrorCode(activationErr) == mfaengine.CodeOK && code == mfaengine.CodeOK &&
		activation.SnapshotVersion == beforeVersion+1 && auth.SnapshotVersion == beforeVersion+2 &&
		found && activeFactor.Status == mfaengine.StatusActive && activeFactor.LastActivated
	r.add("13_factor_activation_snapshot", mfaengine.CodeOK, code, pass, "kappa", "kay", auth.FactorIDs, beforeVersion, auth.SnapshotVersion, "attempt-kay-activate", "pending factor activated and snapshot committed before authentication")
}

func runSnapshotFaults(ctx context.Context, workDir string) ([]mfaengine.FaultScenarioResult, error) {
	kinds := []mfaengine.FaultKind{
		mfaengine.FaultSnapshotVersionGap,
		mfaengine.FaultSnapshotWholesaleReplacement,
	}
	results := make([]mfaengine.FaultScenarioResult, 0, len(kinds))
	for index, kind := range kinds {
		path := filepath.Join(workDir, fmt.Sprintf("fault-%d.db", index))
		if err := resetDatabase(path); err != nil {
			return nil, err
		}
		engine, err := mfaengine.NewEngine(ctx, path, mfaengine.EngineOptions{})
		if err != nil {
			return nil, err
		}
		if err := engine.Register(ctx, DemoConfig()); err != nil {
			_ = engine.Close()
			return nil, err
		}
		if _, err := engine.Authenticate(ctx, mfaengine.AuthRequest{TenantID: "gamma", UserID: "gina", AttemptID: "attempt-fault-baseline"}); err != nil {
			_ = engine.Close()
			return nil, err
		}
		baseline := engine.SnapshotStatus(ctx).LatestValid
		if err := engine.ApplyFault(ctx, mfaengine.FaultInjection{Kind: kind}); err != nil {
			_ = engine.Close()
			return nil, err
		}
		status, verifyErr := engine.VerifySnapshots(ctx)
		actual := mfaengine.ErrorCode(verifyErr)
		invalidVersion := int64(0)
		for _, entry := range status.Entries {
			if !entry.Valid && entry.Version > invalidVersion {
				invalidVersion = entry.Version
			}
		}
		factor, found := factorState(engine, "f-gina-totp")
		_, replayErr := engine.ReadSnapshot(ctx, status.FallbackVersion)
		expectedFallback := baseline
		expectedInvalidVersion := baseline + 2
		if kind == mfaengine.FaultSnapshotWholesaleReplacement {
			expectedFallback = baseline - 1
			expectedInvalidVersion = baseline
		}
		pass := actual == mfaengine.CodeSnapshotVersion && status.FallbackUsed && status.FallbackVersion == expectedFallback &&
			invalidVersion == expectedInvalidVersion && found && factor.Status == mfaengine.StatusActive && replayErr == nil
		results = append(results, mfaengine.FaultScenarioResult{
			Name:            string(kind),
			Kind:            kind,
			ExpectedCode:    mfaengine.CodeSnapshotVersion,
			ActualCode:      actual,
			Passed:          pass,
			InvalidVersion:  invalidVersion,
			FallbackVersion: status.FallbackVersion,
			Reason:          status.Reason,
			SnapshotStatus:  status,
		})
		_ = engine.Close()
	}
	return results, nil
}

func runCrashRecovery(ctx context.Context, options Options) (*mfaengine.CrashRecoveryReport, error) {
	if options.Executable == "" {
		return nil, nil
	}
	report := &mfaengine.CrashRecoveryReport{}
	beforeDB := filepath.Join(options.WorkDir, "crash-before.db")
	afterDB := filepath.Join(options.WorkDir, "crash-after.db")
	for _, path := range []string{beforeDB, afterDB} {
		if err := resetDatabase(path); err != nil {
			return nil, err
		}
		engine, err := mfaengine.NewEngine(ctx, path, mfaengine.EngineOptions{})
		if err != nil {
			return nil, err
		}
		if err := engine.Register(ctx, DemoConfig()); err != nil {
			_ = engine.Close()
			return nil, err
		}
		_ = engine.Close()
	}

	runCrash := func(path, point, attempt string) error {
		cmd := exec.Command(options.Executable, "crash-auth", "--db", path, "--point", point,
			"--tenant", "gamma", "--user", "gina", "--attempt", attempt)
		cmd.Stdout = nil
		cmd.Stderr = nil
		return cmd.Run()
	}
	beforeBaseline := readCrashState(ctx, beforeDB)
	beforeErr := runCrash(beforeDB, "before-commit", "attempt-crash-before")
	beforeAfter := readCrashState(ctx, beforeDB)
	afterBaseline := readCrashState(ctx, afterDB)
	afterErr := runCrash(afterDB, "after-commit", "attempt-crash-after")
	afterAfter := readCrashState(ctx, afterDB)

	report.BeforeCommitCode = beforeErrCode(beforeErr)
	report.AfterCommitCode = beforeErrCode(afterErr)
	report.BeforeUsageCount = beforeAfter.usage
	report.AfterUsageCount = afterAfter.usage
	report.BeforeSnapshotVersion = beforeAfter.version
	report.AfterSnapshotVersion = afterAfter.version
	report.Passed = beforeBaseline.usage == beforeAfter.usage && beforeBaseline.version == beforeAfter.version &&
		afterBaseline.usage+1 == afterAfter.usage && afterBaseline.version+1 == afterAfter.version &&
		beforeErr != nil && afterErr != nil
	if report.Passed {
		report.Detail = "before-commit transaction rolled back; after-commit transaction and snapshot persisted"
	} else {
		report.Detail = "crash recovery state mismatch"
	}
	return report, nil
}

type crashState struct {
	usage   int
	version int64
}

func readCrashState(ctx context.Context, path string) crashState {
	engine, err := mfaengine.NewEngine(ctx, path, mfaengine.EngineOptions{})
	if err != nil {
		return crashState{}
	}
	defer engine.Close()
	state := crashState{version: engine.SnapshotStatus(ctx).LatestValid}
	if factor, ok := factorState(engine, "f-gina-totp"); ok {
		state.usage = factor.SuccessCount
	}
	return state
}

func beforeErrCode(err error) mfaengine.Code {
	if err == nil {
		return mfaengine.CodeOK
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return mfaengine.Code(fmt.Sprintf("EXIT_%d", exitErr.ExitCode()))
	}
	return mfaengine.CodeInternal
}
