package mfa

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const testClock int64 = 1_700_000_000

func testConfig() Config {
	return Config{
		ClockUnix: testClock,
		Limits: Limits{
			MaxConcurrency:    2,
			MaxPendingPerUser: 4,
			FailureThreshold:  2,
			TOTPStepSeconds:   30,
			TOTPWindow:        1,
		},
		Tenants: []TenantDeclaration{
			{ID: "acme", Name: "Acme"},
			{ID: "beta", Name: "Beta"},
			{ID: "delta", Name: "Delta"},
			{ID: "epsilon", Name: "Epsilon"},
			{ID: "gamma", Name: "Gamma"},
		},
		Users: []UserDeclaration{
			{ID: "alice", TenantID: "acme", Name: "Alice"},
			{ID: "bea", TenantID: "beta", Name: "Bea"},
			{ID: "dave", TenantID: "delta", Name: "Dave"},
			{ID: "erin", TenantID: "epsilon", Name: "Erin"},
			{ID: "gina", TenantID: "gamma", Name: "Gina"},
		},
		Policies: []PolicyDeclaration{
			{TenantID: "acme", MinStrength: 3, RequiredFactors: []PolicyRequiredFactor{
				{Type: FactorTOTP, MinSensitivity: SensitivityHigh},
				{Type: FactorRecovery, MinSensitivity: SensitivityHigh},
			}},
			{TenantID: "beta", MinStrength: 2, RequiredFactors: []PolicyRequiredFactor{
				{Type: FactorRecovery, MinSensitivity: SensitivityMedium},
			}},
			{TenantID: "delta", MinStrength: 2, RequiredFactors: []PolicyRequiredFactor{
				{Type: FactorTOTP, MinSensitivity: SensitivityMedium},
			}},
			{TenantID: "epsilon", MinStrength: 2, RequiredFactors: []PolicyRequiredFactor{
				{Type: FactorWebAuthn, MinSensitivity: SensitivityHigh},
			}},
			{TenantID: "gamma", MinStrength: 2, RequiredFactors: []PolicyRequiredFactor{
				{Type: FactorTOTP, MinSensitivity: SensitivityMedium},
			}},
		},
		Factors: []FactorDeclaration{
			{ID: "f-alice-totp", TenantID: "acme", UserID: "alice", Type: FactorTOTP, Sensitivity: SensitivityHigh, Status: StatusActive, Rebindable: true, LastActivated: true},
			{ID: "f-alice-recovery", TenantID: "acme", UserID: "alice", Type: FactorRecovery, Sensitivity: SensitivityHigh, Status: StatusActive, Rebindable: true, LastActivated: true, RecoveryCodeCount: 3},
			{ID: "f-alice-webauthn", TenantID: "acme", UserID: "alice", Type: FactorWebAuthn, Sensitivity: SensitivityHigh, Status: StatusActive, Rebindable: true, LastActivated: true},
			{ID: "f-bea-recovery", TenantID: "beta", UserID: "bea", Type: FactorRecovery, Sensitivity: SensitivityMedium, Status: StatusActive, Rebindable: true, LastActivated: true, RecoveryCodeCount: 2},
			{ID: "f-dave-totp", TenantID: "delta", UserID: "dave", Type: FactorTOTP, Sensitivity: SensitivityMedium, Status: StatusActive, Rebindable: true, LastActivated: true},
			{ID: "f-erin-webauthn", TenantID: "epsilon", UserID: "erin", Type: FactorWebAuthn, Sensitivity: SensitivityHigh, Status: StatusActive, Rebindable: true, LastActivated: true},
			{ID: "f-gina-totp", TenantID: "gamma", UserID: "gina", Type: FactorTOTP, Sensitivity: SensitivityMedium, Status: StatusActive, Rebindable: true, LastActivated: true},
		},
	}
}

func openTestEngine(t *testing.T, cfg Config) *Engine {
	t.Helper()
	engine, err := NewEngine(context.Background(), filepath.Join(t.TempDir(), "mfa.db"), EngineOptions{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	if err := engine.Register(context.Background(), cfg); err != nil {
		t.Fatalf("register: %v", err)
	}
	return engine
}

func factorByID(t *testing.T, engine *Engine, id string) Factor {
	t.Helper()
	factors, err := engine.ListFactors(context.Background())
	if err != nil {
		t.Fatalf("list factors: %v", err)
	}
	for _, factor := range factors {
		if factor.ID == id {
			return factor
		}
	}
	t.Fatalf("factor %s not found", id)
	return Factor{}
}

func TestRegistrationValidationIsAtomic(t *testing.T) {
	ctx := context.Background()
	engine, err := NewEngine(ctx, filepath.Join(t.TempDir(), "mfa.db"), EngineOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	cfg := testConfig()
	cfg.Factors = append(cfg.Factors, FactorDeclaration{
		ID: "f-alice-totp", TenantID: "acme", UserID: "alice", Type: FactorTOTP,
		Sensitivity: SensitivityHigh, Status: StatusPending,
	})
	err = engine.Register(ctx, cfg)
	if got := ErrorCode(err); got != CodePolicyMalformed {
		t.Fatalf("expected %s, got %s (%v)", CodePolicyMalformed, got, err)
	}
	factors, err := engine.ListFactors(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(factors) != 0 {
		t.Fatalf("atomicity failed, got %d factors", len(factors))
	}
	if status := engine.SnapshotStatus(ctx); status.LatestValid != 0 {
		t.Fatalf("atomicity failed, snapshot=%d", status.LatestValid)
	}
}

func TestChallengeSuccessReplayAndWindow(t *testing.T) {
	ctx := context.Background()
	engine := openTestEngine(t, testConfig())
	before := engine.SnapshotStatus(ctx).LatestValid

	success, err := engine.Authenticate(ctx, AuthRequest{
		TenantID: "acme", UserID: "alice", SessionID: "session-a1", AttemptID: "attempt-a1",
	})
	if err != nil {
		t.Fatalf("expected successful multi-factor auth: %v", err)
	}
	if success.Code != CodeOK || success.SnapshotVersion != before+1 {
		t.Fatalf("unexpected success result: %+v", success)
	}
	recovery := factorByID(t, engine, "f-alice-recovery")
	if recovery.SuccessCount != 1 {
		t.Fatalf("recovery success count=%d", recovery.SuccessCount)
	}
	reports, err := engine.ListFactorReports(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, report := range reports {
		if report.ID == "f-alice-recovery" && report.RecoveryCodesConsumed != 1 {
			t.Fatalf("consumed recovery codes=%d", report.RecoveryCodesConsumed)
		}
	}

	index := 0
	replay, err := engine.Authenticate(ctx, AuthRequest{
		TenantID: "acme", UserID: "alice", SessionID: "session-a2", AttemptID: "attempt-a2",
		RecoveryCodeIndex: &index,
	})
	if got := ErrorCode(err); got != CodeConsumed {
		t.Fatalf("expected %s, got %s (%v), result=%+v", CodeConsumed, got, err, replay)
	}
	if replay.Code != CodeConsumed || replay.SnapshotVersion <= success.SnapshotVersion {
		t.Fatalf("unexpected replay result: %+v", replay)
	}

	window, err := engine.Authenticate(ctx, AuthRequest{
		TenantID: "acme", UserID: "alice", SessionID: "session-a3", AttemptID: "attempt-a3",
		TOTPStepOffset: 2,
	})
	if got := ErrorCode(err); got != CodeChallengeFailed {
		t.Fatalf("expected %s, got %s (%v), result=%+v", CodeChallengeFailed, got, err, window)
	}
	if len(window.Challenges) != 2 {
		t.Fatalf("expected two factor challenges, got %+v", window.Challenges)
	}
}

func TestDegradationRestorationAndNoViableFactor(t *testing.T) {
	ctx := context.Background()
	engine := openTestEngine(t, testConfig())
	for _, attemptID := range []string{"gina-fail-1", "gina-fail-2", "gina-fail-3"} {
		result, err := engine.Authenticate(ctx, AuthRequest{
			TenantID: "gamma", UserID: "gina", AttemptID: attemptID, AnswerMode: "invalid",
		})
		if got := ErrorCode(err); got != CodeChallengeFailed {
			t.Fatalf("%s expected %s, got %s (%v) result=%+v", attemptID, CodeChallengeFailed, got, err, result)
		}
	}
	factor := factorByID(t, engine, "f-gina-totp")
	if factor.Status != StatusDegraded || factor.ConsecutiveFailures != 3 || factor.FailureCount != 3 {
		t.Fatalf("unexpected degraded state: %+v", factor)
	}
	risk, err := engine.ListRiskTraces(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(risk) != 1 || risk[0].Kind != "degraded" || risk[0].AttemptID != "gina-fail-3" {
		t.Fatalf("unexpected risk trace: %+v", risk)
	}
	beforeNoViable := engine.SnapshotStatus(ctx).LatestValid
	noViable, err := engine.Authenticate(ctx, AuthRequest{
		TenantID: "gamma", UserID: "gina", AttemptID: "gina-no-viable",
	})
	if got := ErrorCode(err); got != CodeNoViableFactor {
		t.Fatalf("expected %s, got %s (%v)", CodeNoViableFactor, got, err)
	}
	if noViable.SnapshotVersion != beforeNoViable {
		t.Fatalf("no viable factor must not commit a snapshot")
	}

	restored, err := engine.RestoreFactor(ctx, RestoreRequest{
		TenantID: "gamma", UserID: "gina", FactorID: "f-gina-totp",
		AttemptID: "gina-restore", Reason: "manual risk review passed",
	})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restored.Code != CodeOK {
		t.Fatalf("unexpected restore result: %+v", restored)
	}
	risk, err = engine.ListRiskTraces(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(risk) != 2 || risk[1].Kind != "restored" {
		t.Fatalf("unexpected restore trace: %+v", risk)
	}
	if _, err := engine.Authenticate(ctx, AuthRequest{TenantID: "gamma", UserID: "gina", AttemptID: "gina-after-restore"}); err != nil {
		t.Fatalf("auth after restore: %v", err)
	}
}

func TestConcurrentRebindContentionAndInvariant(t *testing.T) {
	ctx := context.Background()
	engine := openTestEngine(t, testConfig())
	beforeFactors, err := engine.ListFactors(ctx)
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan RebindResult, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, attemptID := range []string{"rebind-a", "rebind-b"} {
		wg.Add(1)
		go func(attemptID string) {
			defer wg.Done()
			<-start
			result, err := engine.Rebind(ctx, RebindRequest{
				TenantID: "delta", UserID: "dave", FactorType: FactorTOTP,
				NewFactorID: "f-dave-totp-v2", AttemptID: attemptID,
				Sensitivity: SensitivityHigh, HoldMillis: 150,
			})
			results <- result
			errs <- err
		}(attemptID)
	}
	close(start)
	time.Sleep(25 * time.Millisecond)
	usable, err := engine.UsableFactor(ctx, "delta", "dave", "f-dave-totp")
	if err != nil {
		t.Fatal(err)
	}
	if !usable {
		t.Fatal("old factor must remain usable while rebind is in flight before activation")
	}
	wg.Wait()
	close(results)
	close(errs)

	codes := map[Code]int{}
	for err := range errs {
		if err != nil {
			t.Logf("rebind error: %v", err)
		}
		codes[ErrorCode(err)]++
	}
	if codes[CodeOK] != 1 || codes[CodeRebindContention] != 1 {
		t.Fatalf("unexpected rebind codes: %+v", codes)
	}
	afterFactors, err := engine.ListFactors(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterFactors) != len(beforeFactors) {
		t.Fatalf("factor count changed: before=%d after=%d", len(beforeFactors), len(afterFactors))
	}
	newFactor := factorByID(t, engine, "f-dave-totp-v2")
	if newFactor.Status != StatusActive || !newFactor.LastActivated {
		t.Fatalf("new factor was not activated: %+v", newFactor)
	}
	for _, factor := range afterFactors {
		if factor.ID == "f-dave-totp" {
			t.Fatalf("old factor still present after atomic replacement: %+v", factor)
		}
	}
	traces, err := engine.ListRebindTraces(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(traces) != 2 {
		t.Fatalf("expected two rebind traces, got %+v", traces)
	}
	foundOldID := false
	for _, trace := range traces {
		if trace.OldFactorID == "f-dave-totp" && trace.NewFactorID == "f-dave-totp-v2" {
			foundOldID = true
		}
	}
	if !foundOldID {
		t.Fatalf("rebind trace did not preserve old factor identity: %+v", traces)
	}
}

func TestAuthBudgetAndGlobalQueue(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	cfg.Limits.MaxConcurrency = 1
	cfg.Limits.MaxPendingPerUser = 1
	engine := openTestEngine(t, cfg)

	firstDone := make(chan error, 1)
	go func() {
		_, err := engine.Authenticate(ctx, AuthRequest{
			TenantID: "gamma", UserID: "gina", AttemptID: "budget-1", HoldMillis: 150,
		})
		firstDone <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for engine.PendingAuth("gamma", "gina") == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	second, err := engine.Authenticate(ctx, AuthRequest{
		TenantID: "gamma", UserID: "gina", AttemptID: "budget-2",
	})
	if got := ErrorCode(err); got != CodeAuthBudget {
		t.Fatalf("expected %s, got %s (%v), result=%+v", CodeAuthBudget, got, err, second)
	}
	if err := <-firstDone; err != nil {
		t.Fatalf("first auth: %v", err)
	}

	// With only the global slot contended and a larger per-user budget, requests wait rather than fail.
	cfg2 := testConfig()
	cfg2.Limits.MaxConcurrency = 1
	cfg2.Limits.MaxPendingPerUser = 4
	engine2 := openTestEngine(t, cfg2)
	var wg sync.WaitGroup
	queueCodes := make(chan Code, 3)
	for _, attemptID := range []string{"queue-1", "queue-2", "queue-3"} {
		wg.Add(1)
		go func(attemptID string) {
			defer wg.Done()
			_, err := engine2.Authenticate(ctx, AuthRequest{
				TenantID: "gamma", UserID: "gina", AttemptID: attemptID, HoldMillis: 40,
			})
			queueCodes <- ErrorCode(err)
		}(attemptID)
	}
	wg.Wait()
	close(queueCodes)
	for code := range queueCodes {
		if code != CodeOK {
			t.Fatalf("queued request was rejected: %s", code)
		}
	}
}

func TestSnapshotGapAndWholesaleReplacement(t *testing.T) {
	ctx := context.Background()
	engine := openTestEngine(t, testConfig())
	if _, err := engine.Authenticate(ctx, AuthRequest{TenantID: "gamma", UserID: "gina", AttemptID: "snapshot-auth"}); err != nil {
		t.Fatal(err)
	}
	validHead := engine.SnapshotStatus(ctx).LatestValid
	if validHead != 2 {
		t.Fatalf("expected snapshot head 2, got %d", validHead)
	}
	if err := engine.ApplyFault(ctx, FaultInjection{Kind: FaultSnapshotVersionGap}); err != nil {
		t.Fatalf("inject gap: %v", err)
	}
	status, err := engine.VerifySnapshots(ctx)
	if got := ErrorCode(err); got != CodeSnapshotVersion {
		t.Fatalf("expected %s, got %s (%v)", CodeSnapshotVersion, got, err)
	}
	if !status.FallbackUsed || status.FallbackVersion != validHead {
		t.Fatalf("unexpected fallback: %+v", status)
	}
	if _, err := engine.ReadSnapshot(ctx, validHead); err != nil {
		t.Fatalf("valid snapshot replay failed: %v", err)
	}

	engine2 := openTestEngine(t, testConfig())
	if _, err := engine2.Authenticate(ctx, AuthRequest{TenantID: "gamma", UserID: "gina", AttemptID: "snapshot-auth-2"}); err != nil {
		t.Fatal(err)
	}
	if err := engine2.ApplyFault(ctx, FaultInjection{Kind: FaultSnapshotWholesaleReplacement}); err != nil {
		t.Fatalf("inject replacement: %v", err)
	}
	status2, err := engine2.VerifySnapshots(ctx)
	if got := ErrorCode(err); got != CodeSnapshotVersion {
		t.Fatalf("expected %s for content replacement, got %s (%v)", CodeSnapshotVersion, got, err)
	}
	if status2.FallbackVersion != 1 || status2.Reason == "" {
		t.Fatalf("unexpected replacement status: %+v", status2)
	}
	if _, err := engine2.ReadSnapshot(ctx, 1); err != nil {
		t.Fatalf("old snapshot replay failed: %v", err)
	}
	if _, err := engine2.ReadSnapshot(ctx, 2); ErrorCode(err) != CodeSnapshotVersion {
		t.Fatalf("replaced snapshot should fail integrity check: %v", err)
	}
}

func TestCommittedAndUncommittedHooksRecoverFromSQLite(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "recover.db")
	engine, err := NewEngine(ctx, dbPath, EngineOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Register(ctx, testConfig()); err != nil {
		t.Fatal(err)
	}
	beforeVersion := engine.SnapshotStatus(ctx).LatestValid
	beforeFactor := factorByID(t, engine, "f-gina-totp")

	var afterCommit bool
	engine.options.AfterCommit = func(operation string) {
		if operation == "authenticate" && !afterCommit {
			afterCommit = true
			panic("simulated forced termination after commit")
		}
	}
	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("expected simulated termination")
			}
		}()
		_, _ = engine.Authenticate(ctx, AuthRequest{TenantID: "gamma", UserID: "gina", AttemptID: "committed-before-crash"})
	}()
	_ = engine.Close()

	reopened, err := NewEngine(ctx, dbPath, EngineOptions{})
	if err != nil {
		t.Fatal(err)
	}
	committedFactor := factorByID(t, reopened, "f-gina-totp")
	committedVersion := reopened.SnapshotStatus(ctx).LatestValid
	if committedFactor.SuccessCount != beforeFactor.SuccessCount+1 || committedVersion != beforeVersion+1 {
		t.Fatalf("committed operation rolled back: factor=%+v version=%d", committedFactor, committedVersion)
	}

	reopened.options.BeforeCommit = func(operation string) {
		if operation == "authenticate" {
			panic("simulated forced termination before commit")
		}
	}
	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("expected simulated termination")
			}
		}()
		_, _ = reopened.Authenticate(ctx, AuthRequest{TenantID: "gamma", UserID: "gina", AttemptID: "uncommitted-before-crash"})
	}()
	_ = reopened.Close()

	reopenedAgain, err := NewEngine(ctx, dbPath, EngineOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedAgain.Close()
	finalFactor := factorByID(t, reopenedAgain, "f-gina-totp")
	finalVersion := reopenedAgain.SnapshotStatus(ctx).LatestValid
	if finalFactor.SuccessCount != committedFactor.SuccessCount || finalVersion != committedVersion {
		t.Fatalf("uncommitted operation survived: factor=%+v version=%d", finalFactor, finalVersion)
	}
}

func TestValidatedConfigNormalizesDeterministically(t *testing.T) {
	cfg := testConfig()
	normalized, err := ValidateConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		t.Fatal(err)
	}
	normalized2, err := ValidateConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	encoded2, err := json.Marshal(normalized2)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != string(encoded2) {
		t.Fatal("normalization is not deterministic")
	}
	if len(normalized.Factors) != len(cfg.Factors) {
		t.Fatalf("factor count changed during normalization")
	}
}

func TestMalformedPolicyReferenceReturnsBusinessCode(t *testing.T) {
	cfg := testConfig()
	cfg.Policies[0].RequiredFactors[0].Type = "sms"
	_, err := ValidateConfig(cfg)
	if got := ErrorCode(err); got != CodePolicyMalformed {
		t.Fatalf("expected %s, got %s (%v)", CodePolicyMalformed, got, err)
	}
	if !errors.Is(errors.Unwrap(err), err) && err == nil {
		t.Fatal("unreachable")
	}
}

func TestRebindInterruptionRollsBackOrCommitsAtomically(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "rebind-crash.db")
	engine, err := NewEngine(ctx, dbPath, EngineOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Register(ctx, testConfig()); err != nil {
		t.Fatal(err)
	}
	versionBefore := engine.SnapshotStatus(ctx).LatestValid

	engine.options.BeforeCommit = func(operation string) {
		if operation == "rebind" {
			panic("simulated termination before rebind commit")
		}
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected simulated termination")
			}
		}()
		_, _ = engine.Rebind(ctx, RebindRequest{
			TenantID: "delta", UserID: "dave", FactorType: FactorTOTP,
			NewFactorID: "f-dave-uncommitted", AttemptID: "rebind-before-commit",
			Sensitivity: SensitivityHigh,
		})
	}()
	if usable, err := engine.UsableFactor(ctx, "delta", "dave", "f-dave-totp"); err != nil || !usable {
		t.Fatalf("old factor must remain active after uncommitted rebind: usable=%t err=%v", usable, err)
	}
	if engine.SnapshotStatus(ctx).LatestValid != versionBefore {
		t.Fatal("uncommitted rebind changed snapshot version")
	}
	if _, found := factorStateT(t, engine, "f-dave-uncommitted"); found {
		t.Fatal("uncommitted rebind leaked new factor")
	}

	engine.options.BeforeCommit = nil
	engine.options.AfterCommit = func(operation string) {
		if operation == "rebind" {
			panic("simulated termination after rebind commit")
		}
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected simulated termination after commit")
			}
		}()
		_, _ = engine.Rebind(ctx, RebindRequest{
			TenantID: "delta", UserID: "dave", FactorType: FactorTOTP,
			NewFactorID: "f-dave-committed", AttemptID: "rebind-after-commit",
			Sensitivity: SensitivityHigh,
		})
	}()
	_ = engine.Close()
	reopened, err := NewEngine(ctx, dbPath, EngineOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	newFactor, found := factorStateT(t, reopened, "f-dave-committed")
	if !found || newFactor.Status != StatusActive || !newFactor.LastActivated {
		t.Fatalf("committed rebind was not recovered: %+v found=%t", newFactor, found)
	}
	if reopened.SnapshotStatus(ctx).LatestValid != versionBefore+1 {
		t.Fatalf("committed rebind snapshot version mismatch: %d", reopened.SnapshotStatus(ctx).LatestValid)
	}
}

func factorStateT(t *testing.T, engine *Engine, id string) (Factor, bool) {
	t.Helper()
	factors, err := engine.ListFactors(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, factor := range factors {
		if factor.ID == id {
			return factor, true
		}
	}
	return Factor{}, false
}
