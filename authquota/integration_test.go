package authquota_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"openplatform/authquota"
	"openplatform/internal/sample"
)

type testEnv struct {
	engine  *authquota.Engine
	fixture sample.File
	tenant  authquota.TenantSeed
}

func loadFixture(t *testing.T) sample.File {
	t.Helper()
	fixture, err := sample.Load(filepath.Join("..", "testdata", "sample.json"))
	if err != nil {
		t.Fatalf("load sample: %v", err)
	}
	return fixture
}

func newTestEngine(t *testing.T, mutate func(*authquota.Config)) testEnv {
	t.Helper()
	fixture := loadFixture(t)
	fixedTime, err := fixture.Time()
	if err != nil {
		t.Fatal(err)
	}
	cfg := authquota.Config{
		DBPath:           filepath.Join(t.TempDir(), "integration.db"),
		RecoveryStrategy: authquota.RecoveryStrict,
		MaxInFlight:      4,
		MaxQueue:         8,
		QueueTimeout:     200 * time.Millisecond,
		TokenSecret:      []byte(fixture.TokenSecret),
		Clock:            func() time.Time { return fixedTime },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	engine, err := authquota.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	for _, tenant := range fixture.Tenants {
		if err := engine.Seed(context.Background(), tenant); err != nil {
			t.Fatalf("seed tenant: %v", err)
		}
	}
	return testEnv{engine: engine, fixture: fixture, tenant: fixture.Primary()}
}

func issue(t *testing.T, env testEnv, nonce string) authquota.IssueCodeResult {
	t.Helper()
	result, err := env.engine.IssueAuthorizationCode(context.Background(), authquota.IssueCodeInput{
		TenantID: env.tenant.ID, ClientID: env.tenant.ClientID, RedirectURI: env.tenant.RedirectURI,
		CodeChallenge: env.fixture.PKCE.Challenge, Nonce: nonce, RequestID: "req-issue",
	})
	if err != nil {
		t.Fatalf("issue code: %v", err)
	}
	return result
}

func exchange(t *testing.T, env testEnv, code, verifier, idem string) authquota.TokenResponse {
	t.Helper()
	result, err := env.engine.ExchangeAuthorizationCode(context.Background(), authquota.ExchangeInput{
		Code: code, ClientID: env.tenant.ClientID, RedirectURI: env.tenant.RedirectURI,
		CodeVerifier: verifier, IdempotencyKey: idem, RequestID: "req-exchange",
	})
	if err != nil {
		t.Fatalf("exchange code: %v", err)
	}
	return result
}

func issueAndExchange(t *testing.T, env testEnv) authquota.TokenResponse {
	t.Helper()
	code := issue(t, env, env.fixture.Nonces[0])
	return exchange(t, env, code.Code, env.fixture.PKCE.Verifier, "exchange-test-0001")
}

func mustStatus(t *testing.T, engine *authquota.Engine, tenantID string) authquota.SnapshotView {
	t.Helper()
	view, err := engine.Status(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	return view
}

func TestFixedPointChargeOverflowAndTruncation(t *testing.T) {
	quote, err := authquota.ComputeCharge(1, authquota.Fixed(100_000_000), 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if quote.Charged != authquota.Fixed(33_333_333) || quote.TruncNum != 1 || quote.TruncDen != 3 {
		t.Fatalf("unexpected quote: %+v", quote)
	}
	quote, err = authquota.ComputeCharge(2, authquota.Fixed(100_000_000), 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if quote.Charged != authquota.Fixed(66_666_666) || quote.TruncNum != 2 {
		t.Fatalf("unexpected multi-request quote: %+v", quote)
	}
	_, err = authquota.ComputeCharge(math.MaxInt64, authquota.Fixed(math.MaxInt64), 1, 1)
	if !authquota.IsCode(err, authquota.CodePrecisionOverflow) {
		t.Fatalf("expected precision overflow, got %v", err)
	}
	amount, err := authquota.ParseFixed("1.23456789")
	if err != nil || amount.String() != "1.234567890" {
		t.Fatalf("fixed parse/format mismatch: %v %q", err, amount.String())
	}
	if _, err := authquota.ParseFixed("0.0000000001"); !authquota.IsCode(err, authquota.CodePrecisionOverflow) {
		t.Fatalf("expected sub-nano parse to overflow, got %v", err)
	}
}

func TestAuthorizationCodeValidationDoesNotConsumeCode(t *testing.T) {
	env := newTestEngine(t, nil)
	ctx := context.Background()
	code := issue(t, env, env.fixture.Nonces[0])

	_, err := env.engine.ExchangeAuthorizationCode(ctx, authquota.ExchangeInput{
		Code: code.Code, ClientID: env.tenant.ClientID, RedirectURI: env.tenant.RedirectURI,
		CodeVerifier: "wrong-verifier", IdempotencyKey: "bad-pkce",
	})
	if !authquota.IsCode(err, authquota.CodePKCEMismatch) {
		t.Fatalf("expected PKCE mismatch, got %v", err)
	}
	_, err = env.engine.ExchangeAuthorizationCode(ctx, authquota.ExchangeInput{
		Code: code.Code, ClientID: env.tenant.ClientID, RedirectURI: "https://evil.example.test/callback",
		CodeVerifier: env.fixture.PKCE.Verifier, IdempotencyKey: "bad-redirect",
	})
	if !authquota.IsCode(err, authquota.CodeInvalidGrant) {
		t.Fatalf("expected invalid grant for redirect, got %v", err)
	}
	tokens := exchange(t, env, code.Code, env.fixture.PKCE.Verifier, "good-exchange")
	if tokens.AccessToken == "" || tokens.RefreshToken == "" {
		t.Fatal("exchange did not return tokens")
	}
	principal, err := env.engine.Authenticate(ctx, tokens.AccessToken)
	if err != nil || principal.TenantID != env.tenant.ID {
		t.Fatalf("authenticate access token: %+v %v", principal, err)
	}
	_, err = env.engine.ExchangeAuthorizationCode(ctx, authquota.ExchangeInput{
		Code: code.Code, ClientID: env.tenant.ClientID, RedirectURI: env.tenant.RedirectURI,
		CodeVerifier: env.fixture.PKCE.Verifier, IdempotencyKey: "second-exchange",
	})
	if !authquota.IsCode(err, authquota.CodeInvalidGrant) {
		t.Fatalf("expected consumed code to be invalid, got %v", err)
	}
}

func TestRefreshRotationBillingAndReplayRevokesFamily(t *testing.T) {
	env := newTestEngine(t, nil)
	ctx := context.Background()
	tokens := issueAndExchange(t, env)
	initial := mustStatus(t, env.engine, env.tenant.ID)

	rotated, err := env.engine.Refresh(ctx, authquota.RefreshInput{
		RefreshToken: tokens.RefreshToken, ClientID: env.tenant.ClientID,
		IdempotencyKey: "refresh-rotation-0001", RequestCount: 2, RequestID: "req-rotate",
	})
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if rotated.LedgerVersion != 1 || rotated.SnapshotVersion != initial.Version+1 {
		t.Fatalf("unexpected versions: %+v", rotated)
	}
	wantCharge := authquota.Fixed(66_666_666)
	afterRotation := mustStatus(t, env.engine, env.tenant.ID)
	if afterRotation.State.QuotaRemaining != initial.State.QuotaRemaining-wantCharge {
		t.Fatalf("quota=%d, want %d", afterRotation.State.QuotaRemaining, initial.State.QuotaRemaining-wantCharge)
	}
	ledger, err := env.engine.Ledger(ctx, env.tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.Entries) != 1 || ledger.Entries[0].ChargedNano != wantCharge || ledger.Entries[0].TruncationNum != 2 {
		t.Fatalf("unexpected ledger: %+v", ledger.Entries)
	}

	_, err = env.engine.Refresh(ctx, authquota.RefreshInput{
		RefreshToken: tokens.RefreshToken, ClientID: env.tenant.ClientID,
		IdempotencyKey: "refresh-replay-old", RequestCount: 1, RequestID: "req-replay",
	})
	if !authquota.IsCode(err, authquota.CodeTokenReuse) {
		t.Fatalf("expected token reuse, got %v", err)
	}
	_, err = env.engine.Authenticate(ctx, rotated.AccessToken)
	if !authquota.IsCode(err, authquota.CodeInvalidGrant) {
		t.Fatalf("family access token should be revoked, got %v", err)
	}
	afterReplay := mustStatus(t, env.engine, env.tenant.ID)
	if afterReplay.Version != afterRotation.Version+1 {
		t.Fatalf("replay should add one snapshot: before=%d after=%d", afterRotation.Version, afterReplay.Version)
	}
	if afterReplay.State.QuotaRemaining != afterRotation.State.QuotaRemaining || afterReplay.State.LedgerVersion != 1 {
		t.Fatalf("replay polluted quota/ledger: %+v", afterReplay.State)
	}
	events, err := env.engine.Errors(ctx, env.tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Code != authquota.CodeTokenReuse || events[0].LedgerVersion != 1 ||
		events[0].SnapshotVersion != afterReplay.Version || events[0].RequestID != "req-replay" {
		t.Fatalf("unexpected replay audit event: %+v", events)
	}
}

func TestRefreshIdempotencyReturnsFirstResultAndVersion(t *testing.T) {
	env := newTestEngine(t, nil)
	ctx := context.Background()
	tokens := issueAndExchange(t, env)
	input := authquota.RefreshInput{
		RefreshToken: tokens.RefreshToken, ClientID: env.tenant.ClientID,
		IdempotencyKey: "same-refresh-key", RequestCount: 1, RequestID: "req-idem",
	}
	first, err := env.engine.Refresh(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := env.engine.Refresh(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("idempotent responses differ:\n%+v\n%+v", first, second)
	}
	view := mustStatus(t, env.engine, env.tenant.ID)
	if view.State.LedgerVersion != 1 || view.Version != first.SnapshotVersion {
		t.Fatalf("duplicate changed state: %+v", view.State)
	}
	ledger, err := env.engine.Ledger(ctx, env.tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.Entries) != 1 {
		t.Fatalf("idempotent duplicate created %d ledger rows", len(ledger.Entries))
	}
	var payload []byte
	if err := env.engine.DB().QueryRow(`
SELECT response_payload FROM idempotency_records WHERE tenant_id=? AND operation='refresh' AND idem_key=?`,
		env.tenant.ID, input.IdempotencyKey).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte(first.RefreshToken)) || bytes.Contains(payload, []byte(first.AccessToken)) {
		t.Fatal("idempotency cache leaked a plaintext token")
	}
	conflict := input
	conflict.RequestCount = 2
	_, err = env.engine.Refresh(ctx, conflict)
	if !authquota.IsCode(err, authquota.CodeIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
}

func TestExpiredAndPKCEMismatchFaultsPersistAuditWithoutConsumption(t *testing.T) {
	t.Run("expired auth code", func(t *testing.T) {
		env := newTestEngine(t, func(cfg *authquota.Config) { cfg.Fault = authquota.FaultAuthCodeExpired })
		code := issue(t, env, env.fixture.Nonces[0])
		_, err := env.engine.ExchangeAuthorizationCode(context.Background(), authquota.ExchangeInput{
			Code: code.Code, ClientID: env.tenant.ClientID, RedirectURI: env.tenant.RedirectURI,
			CodeVerifier: env.fixture.PKCE.Verifier, IdempotencyKey: "expired-exchange",
		})
		if !authquota.IsCode(err, authquota.CodeInvalidGrant) {
			t.Fatalf("expected invalid grant, got %v", err)
		}
		var consumedAt any
		if err := env.engine.DB().QueryRow(`
SELECT consumed_at FROM auth_codes WHERE tenant_id=? AND nonce=?`, env.tenant.ID, env.fixture.Nonces[0]).Scan(&consumedAt); err != nil {
			t.Fatal(err)
		}
		if consumedAt != nil {
			t.Fatalf("expired validation consumed code: %v", consumedAt)
		}
		view := mustStatus(t, env.engine, env.tenant.ID)
		if view.Version != 1 {
			t.Fatalf("expired code should not change snapshot: %d", view.Version)
		}
	})
	t.Run("forced pkce mismatch", func(t *testing.T) {
		env := newTestEngine(t, func(cfg *authquota.Config) { cfg.Fault = authquota.FaultPKCEMismatch })
		code := issue(t, env, env.fixture.Nonces[0])
		_, err := env.engine.ExchangeAuthorizationCode(context.Background(), authquota.ExchangeInput{
			Code: code.Code, ClientID: env.tenant.ClientID, RedirectURI: env.tenant.RedirectURI,
			CodeVerifier: env.fixture.PKCE.Verifier, IdempotencyKey: "bad-pkce-fault",
		})
		if !authquota.IsCode(err, authquota.CodePKCEMismatch) {
			t.Fatalf("expected PKCE mismatch, got %v", err)
		}
		var consumedAt any
		if err := env.engine.DB().QueryRow(`
SELECT consumed_at FROM auth_codes WHERE tenant_id=? AND nonce=?`, env.tenant.ID, env.fixture.Nonces[0]).Scan(&consumedAt); err != nil {
			t.Fatal(err)
		}
		if consumedAt != nil {
			t.Fatal("PKCE failure consumed authorization code")
		}
	})
}

func TestPrecisionOverflowRollsBackTokenAndLedger(t *testing.T) {
	env := newTestEngine(t, func(cfg *authquota.Config) { cfg.Fault = authquota.FaultPrecisionOverflow })
	ctx := context.Background()
	tokens := issueAndExchange(t, env)
	before := mustStatus(t, env.engine, env.tenant.ID)
	_, err := env.engine.Refresh(ctx, authquota.RefreshInput{
		RefreshToken: tokens.RefreshToken, ClientID: env.tenant.ClientID,
		IdempotencyKey: "overflow-refresh", RequestCount: 1, RequestID: "req-overflow",
	})
	if !authquota.IsCode(err, authquota.CodePrecisionOverflow) {
		t.Fatalf("expected precision overflow, got %v", err)
	}
	after := mustStatus(t, env.engine, env.tenant.ID)
	if after.Version != before.Version || after.State.QuotaRemaining != before.State.QuotaRemaining || after.State.LedgerVersion != 0 {
		t.Fatalf("overflow changed state: before=%+v after=%+v", before.State, after.State)
	}
	if _, err := env.engine.Authenticate(ctx, tokens.AccessToken); err != nil {
		t.Fatalf("overflow should leave old access token active: %v", err)
	}
	ledger, err := env.engine.Ledger(ctx, env.tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.Entries) != 0 {
		t.Fatalf("overflow wrote ledger entries: %+v", ledger.Entries)
	}
	events, err := env.engine.Errors(ctx, env.tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Code != authquota.CodePrecisionOverflow {
		t.Fatalf("unexpected overflow audit: %+v", events)
	}
}

func TestQuotaExhaustionChargesNothingAndAdvancesSnapshot(t *testing.T) {
	env := newTestEngine(t, func(cfg *authquota.Config) { cfg.Fault = authquota.FaultQuotaExhausted })
	ctx := context.Background()
	tokens := issueAndExchange(t, env)
	before := mustStatus(t, env.engine, env.tenant.ID)
	_, err := env.engine.Refresh(ctx, authquota.RefreshInput{
		RefreshToken: tokens.RefreshToken, ClientID: env.tenant.ClientID,
		IdempotencyKey: "quota-refresh", RequestCount: 1, RequestID: "req-quota",
	})
	if !authquota.IsCode(err, authquota.CodeQuotaExhausted) {
		t.Fatalf("expected quota exhausted, got %v", err)
	}
	after := mustStatus(t, env.engine, env.tenant.ID)
	if after.Version != before.Version+1 {
		t.Fatalf("quota exhaustion should create one snapshot: %d -> %d", before.Version, after.Version)
	}
	if after.State.QuotaRemaining != before.State.QuotaRemaining || after.State.LedgerVersion != 0 {
		t.Fatalf("quota failure changed quota/ledger: %+v", after.State)
	}
	ledger, err := env.engine.Ledger(ctx, env.tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.Entries) != 0 {
		t.Fatalf("quota failure wrote ledger: %+v", ledger.Entries)
	}
	if _, err := env.engine.Authenticate(ctx, tokens.AccessToken); err != nil {
		t.Fatalf("quota failure revoked access token: %v", err)
	}
}

func TestSnapshotJumpStrictAndConsistentRecovery(t *testing.T) {
	fixture := loadFixture(t)
	dbPath := filepath.Join(t.TempDir(), "jump.db")
	fixedTime, _ := fixture.Time()
	base := authquota.Config{
		DBPath: dbPath, RecoveryStrategy: authquota.RecoveryStrict,
		TokenSecret: []byte(fixture.TokenSecret), Clock: func() time.Time { return fixedTime },
		Fault: authquota.FaultSnapshotJump,
	}
	engine, err := authquota.Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	for _, tenant := range fixture.Tenants {
		if err := engine.Seed(context.Background(), tenant); err != nil {
			t.Fatal(err)
		}
	}
	env := testEnv{engine: engine, fixture: fixture, tenant: fixture.Primary()}
	tokens := issueAndExchange(t, env)
	rotated, err := engine.Refresh(context.Background(), authquota.RefreshInput{
		RefreshToken: tokens.RefreshToken, ClientID: env.tenant.ClientID,
		IdempotencyKey: "jump-refresh", RequestCount: 1, RequestID: "req-jump",
	})
	if err != nil {
		t.Fatalf("refresh with jump injection: %v", err)
	}
	if rotated.SnapshotVersion != 4 {
		t.Fatalf("expected skipped snapshot version 4, got %d", rotated.SnapshotVersion)
	}
	view, err := engine.Status(context.Background(), env.tenant.ID)
	if !authquota.IsCode(err, authquota.CodeSnapshotVersion) {
		t.Fatalf("expected snapshot version error, got %v", err)
	}
	if !view.ReadOnly || !view.Recovered || view.Version != 2 || view.LatestVersion != 4 {
		t.Fatalf("unexpected recovery view: %+v", view)
	}
	if _, err := engine.Ledger(context.Background(), env.tenant.ID); !authquota.IsCode(err, authquota.CodeSnapshotVersion) {
		t.Fatalf("strict ledger should refuse: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	base.RecoveryStrategy = authquota.RecoveryConsistent
	recovered, err := authquota.Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	ledger, err := recovered.Ledger(context.Background(), env.tenant.ID)
	if err != nil {
		t.Fatalf("consistent ledger recovery: %v", err)
	}
	if !ledger.ReadOnly || ledger.SnapshotVersion != 2 || len(ledger.Entries) != 0 {
		t.Fatalf("consistent export was not isolated to last good snapshot: %+v", ledger)
	}
	if ledger.Session.QuotaRemaining != env.tenant.Quota {
		t.Fatalf("read-only session quota should be pre-jump: %d", ledger.Session.QuotaRemaining)
	}
}

func TestConcurrentQueueOverflowReturnsContentionAndRecordsAudit(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	env := newTestEngine(t, func(cfg *authquota.Config) {
		cfg.MaxInFlight = 1
		cfg.MaxQueue = 0
		cfg.QueueTimeout = 80 * time.Millisecond
		cfg.Hooks.BeforeBilling = func(_ context.Context, _, _ string) error {
			once.Do(func() {
				close(entered)
				<-release
			})
			return nil
		}
	})
	ctx := context.Background()
	tokens := issueAndExchange(t, env)
	firstDone := make(chan error, 1)
	go func() {
		_, err := env.engine.Refresh(ctx, authquota.RefreshInput{
			RefreshToken: tokens.RefreshToken, ClientID: env.tenant.ClientID,
			IdempotencyKey: "contention-first", RequestCount: 1, RequestID: "req-first",
		})
		firstDone <- err
	}()
	<-entered
	_, err := env.engine.Refresh(ctx, authquota.RefreshInput{
		RefreshToken: tokens.RefreshToken, ClientID: env.tenant.ClientID,
		IdempotencyKey: "contention-second", RequestCount: 1, RequestID: "req-blocked",
	})
	if !authquota.IsCode(err, authquota.CodeQuotaContention) {
		t.Fatalf("expected contention, got %v", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first refresh failed: %v", err)
	}
	events, err := env.engine.Errors(ctx, env.tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Code != authquota.CodeQuotaContention ||
		events[0].TenantID != env.tenant.ID || events[0].RequestID != "req-blocked" || events[0].LedgerVersion != 0 {
		t.Fatalf("contention audit mismatch: %+v", events)
	}
}

func TestBeforeCommitFailureRollsBackRotationAndBilling(t *testing.T) {
	var shouldFail atomic.Bool
	env := newTestEngine(t, func(cfg *authquota.Config) {
		cfg.Hooks.BeforeCommit = func(_ context.Context, _, requestID string) error {
			if shouldFail.Load() && requestID == "req-atomic" {
				return errors.New("injected commit failure")
			}
			return nil
		}
	})
	ctx := context.Background()
	tokens := issueAndExchange(t, env)
	before := mustStatus(t, env.engine, env.tenant.ID)
	shouldFail.Store(true)
	_, err := env.engine.Refresh(ctx, authquota.RefreshInput{
		RefreshToken: tokens.RefreshToken, ClientID: env.tenant.ClientID,
		IdempotencyKey: "atomic-refresh", RequestCount: 1, RequestID: "req-atomic",
	})
	if err == nil {
		t.Fatal("expected injected commit failure")
	}
	after := mustStatus(t, env.engine, env.tenant.ID)
	if after.Version != before.Version || after.State.QuotaRemaining != before.State.QuotaRemaining || after.State.LedgerVersion != 0 {
		t.Fatalf("failed transaction was partially applied: before=%+v after=%+v", before.State, after.State)
	}
	if _, err := env.engine.Authenticate(ctx, tokens.AccessToken); err != nil {
		t.Fatalf("old access token should remain valid: %v", err)
	}
	rotated, err := env.engine.Refresh(ctx, authquota.RefreshInput{
		RefreshToken: tokens.RefreshToken, ClientID: env.tenant.ClientID,
		IdempotencyKey: "atomic-refresh-retry", RequestCount: 1, RequestID: "req-atomic-retry",
	})
	if err != nil {
		t.Fatalf("old refresh token should remain usable after rollback: %v", err)
	}
	if rotated.LedgerVersion != 1 {
		t.Fatalf("retry ledger version=%d, want 1", rotated.LedgerVersion)
	}
}

func TestMultiTenantIsolationAndExplicitRevoke(t *testing.T) {
	env := newTestEngine(t, nil)
	ctx := context.Background()
	other := env.fixture.Tenants[1]
	acmeTokens := issueAndExchange(t, env)
	otherCode, err := env.engine.IssueAuthorizationCode(ctx, authquota.IssueCodeInput{
		TenantID: other.ID, ClientID: other.ClientID, RedirectURI: other.RedirectURI,
		CodeChallenge: env.fixture.PKCE.Challenge, Nonce: "nonce-beta-0001", RequestID: "req-beta-issue",
	})
	if err != nil {
		t.Fatal(err)
	}
	betaTokens, err := env.engine.ExchangeAuthorizationCode(ctx, authquota.ExchangeInput{
		Code: otherCode.Code, ClientID: other.ClientID, RedirectURI: other.RedirectURI,
		CodeVerifier: env.fixture.PKCE.Verifier, IdempotencyKey: "beta-exchange", RequestID: "req-beta-exchange",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.engine.Refresh(ctx, authquota.RefreshInput{
		RefreshToken: acmeTokens.RefreshToken, ClientID: env.tenant.ClientID,
		IdempotencyKey: "acme-refresh-isolation", RequestCount: 1, RequestID: "req-acme-isolation",
	}); err != nil {
		t.Fatal(err)
	}
	betaView := mustStatus(t, env.engine, other.ID)
	if betaView.State.QuotaRemaining != other.Quota || betaView.State.LedgerVersion != 0 {
		t.Fatalf("acme charge leaked into beta: %+v", betaView.State)
	}
	beforeRevoke := betaView.Version
	revoked, err := env.engine.Revoke(ctx, authquota.RevokeInput{
		TenantID: other.ID, Token: betaTokens.RefreshToken, RequestID: "req-beta-revoke",
	})
	if err != nil {
		t.Fatal(err)
	}
	if revoked.SnapshotVersion != beforeRevoke+1 || revoked.RevokedFamily != betaTokens.FamilyID {
		t.Fatalf("unexpected revoke result: %+v", revoked)
	}
	if _, err := env.engine.Authenticate(ctx, betaTokens.AccessToken); !authquota.IsCode(err, authquota.CodeInvalidGrant) {
		t.Fatalf("revoked family access token still valid: %v", err)
	}
	acmeView := mustStatus(t, env.engine, env.tenant.ID)
	if acmeView.State.LedgerVersion != 1 {
		t.Fatalf("beta revoke affected acme ledger: %+v", acmeView.State)
	}
}

func TestHTTPMiddleware(t *testing.T) {
	env := newTestEngine(t, nil)
	tokens := issueAndExchange(t, env)
	handler := env.engine.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := authquota.PrincipalFromContext(r.Context())
		if !ok || principal.TenantID != env.tenant.ID {
			t.Fatalf("principal missing from middleware context: %+v", principal)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	server := httptest.NewServer(handler)
	defer server.Close()

	request, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized || !bytes.Contains(body, []byte(authquota.CodeInvalidGrant)) {
		t.Fatalf("missing bearer response: %d %s", response.StatusCode, body)
	}

	request, _ = http.NewRequest(http.MethodGet, server.URL, nil)
	request.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("valid bearer status=%d", response.StatusCode)
	}
}

func TestDeterministicSampleExportIsByteIdentical(t *testing.T) {
	first := runDeterministicScenario(t, filepath.Join(t.TempDir(), "first.db"))
	second := runDeterministicScenario(t, filepath.Join(t.TempDir(), "second.db"))
	if !bytes.Equal(first, second) {
		t.Fatalf("deterministic exports differ\nfirst=%s\nsecond=%s", first, second)
	}
}

func runDeterministicScenario(t *testing.T, dbPath string) []byte {
	t.Helper()
	fixture := loadFixture(t)
	fixedTime, _ := fixture.Time()
	engine, err := authquota.Open(context.Background(), authquota.Config{
		DBPath: dbPath, RecoveryStrategy: authquota.RecoveryStrict,
		TokenSecret: []byte(fixture.TokenSecret), Clock: func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	for _, tenant := range fixture.Tenants {
		if err := engine.Seed(context.Background(), tenant); err != nil {
			t.Fatal(err)
		}
	}
	env := testEnv{engine: engine, fixture: fixture, tenant: fixture.Primary()}
	tokens := issueAndExchange(t, env)
	for _, step := range fixture.Billing {
		tokens, err = engine.Refresh(context.Background(), authquota.RefreshInput{
			RefreshToken: tokens.RefreshToken, ClientID: env.tenant.ClientID,
			IdempotencyKey: step.IdempotencyKey, RequestCount: step.RequestCount, RequestID: step.RequestID,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	export, err := engine.ExportAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	body, err := authquota.CanonicalJSON(export)
	if err != nil {
		t.Fatal(err)
	}
	return body
}
