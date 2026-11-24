// Package experiment runs deterministic, local-only authentication anomaly
// injections and produces byte-reproducible evidence reports.
package experiment

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"example.com/authlab"
	"example.com/authlab/sample"
)

const ReportSchema = "authlab.experiment.v1"

var InjectionOrder = []string{
	"expired",
	"cross-tenant",
	"session-fixation",
	"privilege-escalation",
	"refresh-race",
}

type Options struct {
	Injection      string
	WorkDir        string
	Concurrency    int
	ContentionWait time.Duration
}

type KV struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type CodeCount struct {
	Code  string `json:"code"`
	Count int    `json:"count"`
}

type StepReport struct {
	Name                 string      `json:"name"`
	ExpectedCode         string      `json:"expected_code"`
	ActualCode           string      `json:"actual_code"`
	ExpectedVersionDelta int64       `json:"expected_version_delta"`
	VersionBefore        int64       `json:"version_before"`
	VersionAfter         int64       `json:"version_after"`
	VersionDelta         int64       `json:"version_delta"`
	DigestBefore         string      `json:"digest_before"`
	DigestAfter          string      `json:"digest_after"`
	StateUnchanged       bool        `json:"state_unchanged"`
	Passed               bool        `json:"passed"`
	LoadError            string      `json:"load_error,omitempty"`
	Attributes           []KV        `json:"attributes"`
	CodeCounts           []CodeCount `json:"code_counts"`
}

type Assertion struct {
	Name     string `json:"name"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	Passed   bool   `json:"passed"`
}

type TenantReport struct {
	Scope         string                 `json:"scope"`
	TenantID      string                 `json:"tenant_id"`
	Version       int64                  `json:"version"`
	Digest        string                 `json:"digest"`
	State         authlab.TenantState    `json:"state"`
	History       []authlab.SnapshotMeta `json:"history"`
	Audit         authlab.SnapshotAudit  `json:"audit"`
	IntegrityPass bool                   `json:"integrity_pass"`
}

type CaseReport struct {
	Name          string         `json:"name"`
	Injection     string         `json:"injection"`
	Description   string         `json:"description"`
	Passed        bool           `json:"passed"`
	ObservedCodes []string       `json:"observed_codes"`
	Steps         []StepReport   `json:"steps"`
	Assertions    []Assertion    `json:"assertions"`
	Tenants       []TenantReport `json:"tenants"`
}

type Summary struct {
	Passed      bool `json:"passed"`
	CaseCount   int  `json:"case_count"`
	FailedCases int  `json:"failed_cases"`
	StepCount   int  `json:"step_count"`
	FailedSteps int  `json:"failed_steps"`
}

type Report struct {
	Schema    string                `json:"schema"`
	Engine    string                `json:"engine"`
	Injection string                `json:"injection"`
	BaseTime  string                `json:"base_time"`
	Fixture   sample.FixtureSummary `json:"fixture"`
	Cases     []CaseReport          `json:"cases"`
	Summary   Summary               `json:"summary"`
}

func Marshal(report *Report) ([]byte, error) {
	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(payload, '\n'), nil
}

func Run(ctx context.Context, options Options) (*Report, error) {
	injections, err := selectedInjections(options.Injection)
	if err != nil {
		return nil, err
	}
	if options.Concurrency < 2 {
		options.Concurrency = 8
	}
	if options.ContentionWait <= 0 {
		options.ContentionWait = 40 * time.Millisecond
	}
	workDir := options.WorkDir
	cleanup := func() {}
	if workDir == "" {
		workDir, err = os.MkdirTemp("", "authlab-experiment-*")
		if err != nil {
			return nil, fmt.Errorf("create experiment workspace: %w", err)
		}
		cleanup = func() { _ = os.RemoveAll(workDir) }
	}
	defer cleanup()
	if err := os.MkdirAll(filepath.Join(workDir, "db"), 0o755); err != nil {
		return nil, fmt.Errorf("create experiment db directory: %w", err)
	}

	report := &Report{
		Schema:    ReportSchema,
		Engine:    "authlab-local",
		Injection: normalizeInjection(options.Injection),
		BaseTime:  sample.BaseTime.Format(time.RFC3339),
		Fixture:   sample.Summary(),
		Cases:     []CaseReport{},
	}
	runner := caseRunner{ctx: ctx, workDir: workDir, options: options, report: report}
	for _, injection := range injections {
		var caseReport CaseReport
		switch injection {
		case "expired":
			caseReport, err = runner.expired()
		case "cross-tenant":
			caseReport, err = runner.crossTenant()
		case "session-fixation":
			caseReport, err = runner.sessionFixation()
		case "privilege-escalation":
			caseReport, err = runner.privilegeEscalation()
		case "refresh-race":
			caseReport, err = runner.refreshRace()
		}
		if err != nil {
			return nil, fmt.Errorf("run %s: %w", injection, err)
		}
		report.Cases = append(report.Cases, caseReport)
	}
	report.Summary = summarize(report.Cases)
	return report, nil
}

func normalizeInjection(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "_", "-")
	if value == "" || value == "all" {
		return "all"
	}
	return value
}

func selectedInjections(value string) ([]string, error) {
	value = normalizeInjection(value)
	if value == "all" {
		return append([]string(nil), InjectionOrder...), nil
	}
	for _, supported := range InjectionOrder {
		if value == supported {
			return []string{value}, nil
		}
	}
	return nil, fmt.Errorf("unsupported injection %q (supported: %s)", value, strings.Join(InjectionOrder, ", "))
}

func summarize(cases []CaseReport) Summary {
	summary := Summary{Passed: true, CaseCount: len(cases)}
	for _, caseReport := range cases {
		if !caseReport.Passed {
			summary.Passed = false
			summary.FailedCases++
		}
		for _, step := range caseReport.Steps {
			summary.StepCount++
			if !step.Passed {
				summary.FailedSteps++
			}
		}
	}
	return summary
}

type caseRunner struct {
	ctx     context.Context
	workDir string
	options Options
	report  *Report
}

type caseEnv struct {
	engine  *authlab.Engine
	clock   *authlab.ManualClock
	fixture authlab.Fixture
	codec   *authlab.TokenCodec
}

func (r *caseRunner) openEnv(name string, options authlab.Options) (*caseEnv, error) {
	clock := authlab.NewManualClock(sample.BaseTime)
	fixture := sample.Fixture()
	options.Clock = clock
	options.IDs = authlab.NewDeterministicIDSource(fixture.IDSeed)
	if options.AccessTTL <= 0 {
		options.AccessTTL = 5 * time.Minute
	}
	if options.RefreshTTL <= 0 {
		options.RefreshTTL = time.Hour
	}
	if options.MaxInFlight <= 0 {
		options.MaxInFlight = 16
	}
	if options.ContentionWait <= 0 {
		options.ContentionWait = r.options.ContentionWait
	}
	path := filepath.Join(r.workDir, "db", name+".sqlite")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(path + suffix)
	}
	engine, err := authlab.OpenSQLiteEngine(r.ctx, path, fixture, options)
	if err != nil {
		return nil, err
	}
	return &caseEnv{
		engine: engine, clock: clock, fixture: fixture,
		codec: authlab.NewTokenCodec(fixture.SigningKey),
	}, nil
}

func loginAs(ctx context.Context, env *caseEnv, tenantID, userID string) (*authlab.LoginResult, error) {
	password, ok := sample.PasswordFor(tenantID, userID)
	if !ok {
		return nil, fmt.Errorf("unknown sample user %s/%s", tenantID, userID)
	}
	username := ""
	for _, credential := range sample.Credentials {
		if credential.TenantID == tenantID && credential.UserID == userID {
			username = credential.Username
			break
		}
	}
	if username == "" {
		return nil, fmt.Errorf("unknown sample username %s/%s", tenantID, userID)
	}
	return env.engine.Login(ctx, authlab.LoginRequest{TenantID: tenantID, Username: username, Password: password})
}

func mustSnapshot(ctx context.Context, env *caseEnv, tenantID string) (authlab.Snapshot, error) {
	return env.engine.CurrentSnapshot(ctx, tenantID)
}

func buildStep(name, expected string, expectedDelta int64, before, after authlab.Snapshot, actionErr error, loadErr error, attrs ...KV) StepReport {
	actual := string(authlab.CodeOf(actionErr))
	delta := after.Version - before.Version
	unchanged := before.Version == after.Version && before.Digest == after.Digest
	passed := actual == expected && delta == expectedDelta && loadErr == nil
	if expectedDelta == 0 {
		passed = passed && unchanged
	}
	if expectedDelta > 0 {
		passed = passed && !unchanged
	}
	if attrs == nil {
		attrs = []KV{}
	}
	step := StepReport{
		Name: name, ExpectedCode: expected, ActualCode: actual,
		ExpectedVersionDelta: expectedDelta,
		VersionBefore:        before.Version, VersionAfter: after.Version, VersionDelta: delta,
		DigestBefore: before.Digest, DigestAfter: after.Digest,
		StateUnchanged: unchanged, Passed: passed, Attributes: attrs, CodeCounts: []CodeCount{},
	}
	if loadErr != nil {
		step.LoadError = loadErr.Error()
	}
	return step
}

func assertion(name, expected, actual string) Assertion {
	return Assertion{Name: name, Expected: expected, Actual: actual, Passed: expected == actual}
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func captureTenant(ctx context.Context, env *caseEnv, scope, tenantID string) (TenantReport, error) {
	snapshot, err := env.engine.CurrentSnapshot(ctx, tenantID)
	if err != nil {
		return TenantReport{}, err
	}
	history, err := env.engine.SnapshotHistory(ctx, tenantID)
	if err != nil {
		return TenantReport{}, err
	}
	audit, err := env.engine.AuditSnapshots(ctx, tenantID)
	if err != nil {
		return TenantReport{}, err
	}
	return TenantReport{
		Scope: scope, TenantID: tenantID, Version: snapshot.Version, Digest: snapshot.Digest,
		State: snapshot.State, History: history, Audit: audit, IntegrityPass: audit.IntegrityOK,
	}, nil
}

func finalizeCase(caseReport *CaseReport) {
	passed := true
	for _, step := range caseReport.Steps {
		if !step.Passed {
			passed = false
		}
	}
	for _, item := range caseReport.Assertions {
		if !item.Passed {
			passed = false
		}
	}
	for _, tenant := range caseReport.Tenants {
		if !tenant.IntegrityPass {
			passed = false
		}
	}
	observed := map[string]bool{}
	for _, step := range caseReport.Steps {
		if step.ActualCode != "" {
			observed[step.ActualCode] = true
		}
		for _, count := range step.CodeCounts {
			observed[count.Code] = true
		}
	}
	caseReport.ObservedCodes = make([]string, 0, len(observed))
	for code := range observed {
		caseReport.ObservedCodes = append(caseReport.ObservedCodes, code)
	}
	sort.Strings(caseReport.ObservedCodes)
	caseReport.Passed = passed
}

func (r *caseRunner) expired() (CaseReport, error) {
	env, err := r.openEnv("expired", authlab.Options{})
	if err != nil {
		return CaseReport{}, err
	}
	defer env.engine.Close()
	ctx := r.ctx
	report := CaseReport{
		Name: "expired-token", Injection: "expired",
		Description: "Expired access and refresh tokens fail with E_TOKEN_INVALID while the tenant snapshot remains unchanged.",
		Steps:       []StepReport{}, Assertions: []Assertion{}, Tenants: []TenantReport{},
	}
	before, err := mustSnapshot(ctx, env, sample.TenantA)
	if err != nil {
		return report, err
	}
	login, err := loginAs(ctx, env, sample.TenantA, sample.UserAlice)
	after, loadErr := mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("login-alice", "OK", 1, before, after, err, loadErr,
		KV{Name: "access_token", Value: authlab.TokenFingerprint(login.AccessToken)},
		KV{Name: "refresh_token", Value: authlab.TokenFingerprint(login.RefreshToken)}))
	if err != nil {
		return report, err
	}

	env.clock.Advance(6 * time.Minute)
	before, err = mustSnapshot(ctx, env, sample.TenantA)
	if err != nil {
		return report, err
	}
	_, validationErr := env.engine.ValidateAccess(ctx, login.AccessToken)
	after, loadErr = mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("expired-access-token", string(authlab.CodeTokenInvalid), 0, before, after, validationErr, loadErr,
		KV{Name: "clock_offset", Value: "6m0s"}))

	env.clock.Advance(2 * time.Hour)
	before, err = mustSnapshot(ctx, env, sample.TenantA)
	if err != nil {
		return report, err
	}
	_, refreshErr := env.engine.Refresh(ctx, login.RefreshToken)
	after, loadErr = mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("expired-refresh-token", string(authlab.CodeTokenInvalid), 0, before, after, refreshErr, loadErr,
		KV{Name: "clock_offset", Value: "2h6m0s"}))

	tenant, err := captureTenant(ctx, env, "main", sample.TenantA)
	if err != nil {
		return report, err
	}
	report.Tenants = append(report.Tenants, tenant)
	report.Assertions = append(report.Assertions,
		assertion("final-snapshot-version", "1", fmt.Sprintf("%d", tenant.Version)),
		assertion("session-still-active", "active", sessionStatus(tenant.State, login.SessionID)),
	)
	finalizeCase(&report)
	return report, nil
}

func (r *caseRunner) crossTenant() (CaseReport, error) {
	env, err := r.openEnv("cross-tenant", authlab.Options{})
	if err != nil {
		return CaseReport{}, err
	}
	defer env.engine.Close()
	ctx := r.ctx
	report := CaseReport{
		Name: "cross-tenant-token-use", Injection: "cross-tenant",
		Description: "A valid tenant A token presented to tenant B returns E_CROSS_TENANT and mutates neither tenant.",
		Steps:       []StepReport{}, Assertions: []Assertion{}, Tenants: []TenantReport{},
	}
	beforeA, err := mustSnapshot(ctx, env, sample.TenantA)
	if err != nil {
		return report, err
	}
	loginA, err := loginAs(ctx, env, sample.TenantA, sample.UserAlice)
	afterA, loadErr := mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("login-tenant-a", "OK", 1, beforeA, afterA, err, loadErr,
		KV{Name: "token_tenant", Value: sample.TenantA}))
	if err != nil {
		return report, err
	}
	beforeB, err := mustSnapshot(ctx, env, sample.TenantB)
	if err != nil {
		return report, err
	}
	loginB, err := loginAs(ctx, env, sample.TenantB, sample.UserCarol)
	afterB, loadErr := mustSnapshot(ctx, env, sample.TenantB)
	report.Steps = append(report.Steps, buildStep("login-tenant-b", "OK", 1, beforeB, afterB, err, loadErr,
		KV{Name: "token_tenant", Value: sample.TenantB}))
	if err != nil {
		return report, err
	}
	beforeA, beforeB = afterA, afterB

	_, err = env.engine.ValidateAccessForTenant(ctx, sample.TenantB, loginA.AccessToken)
	afterA, loadErrA := mustSnapshot(ctx, env, sample.TenantA)
	afterB, loadErrB := mustSnapshot(ctx, env, sample.TenantB)
	if loadErrB != nil {
		return report, loadErrB
	}
	report.Steps = append(report.Steps, buildStep("cross-tenant-validate", string(authlab.CodeCrossTenant), 0, beforeA, afterA, err, loadErrA,
		KV{Name: "signed_tenant", Value: sample.TenantA}, KV{Name: "endpoint_tenant", Value: sample.TenantB}))
	report.Assertions = append(report.Assertions, assertion("tenant-b-unchanged-by-validate", beforeB.Digest, afterB.Digest))

	beforeA, beforeB = afterA, afterB
	_, err = env.engine.RefreshForTenant(ctx, sample.TenantB, loginA.RefreshToken)
	afterA, loadErrA = mustSnapshot(ctx, env, sample.TenantA)
	afterB, loadErrB = mustSnapshot(ctx, env, sample.TenantB)
	if loadErrB != nil {
		return report, loadErrB
	}
	report.Steps = append(report.Steps, buildStep("cross-tenant-refresh", string(authlab.CodeCrossTenant), 0, beforeA, afterA, err, loadErrA,
		KV{Name: "signed_tenant", Value: sample.TenantA}, KV{Name: "endpoint_tenant", Value: sample.TenantB}))
	report.Assertions = append(report.Assertions,
		assertion("tenant-a-unchanged-by-refresh", beforeA.Digest, afterA.Digest),
		assertion("tenant-b-unchanged-by-refresh", beforeB.Digest, afterB.Digest),
	)

	beforeA = afterA
	_, err = env.engine.ValidateAccessForTenant(ctx, sample.TenantA, loginA.AccessToken)
	afterA, loadErr = mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("same-tenant-validate", "OK", 0, beforeA, afterA, err, loadErr,
		KV{Name: "signed_tenant", Value: sample.TenantA}, KV{Name: "endpoint_tenant", Value: sample.TenantA}))

	tenantA, err := captureTenant(ctx, env, "main", sample.TenantA)
	if err != nil {
		return report, err
	}
	tenantB, err := captureTenant(ctx, env, "main", sample.TenantB)
	if err != nil {
		return report, err
	}
	report.Tenants = append(report.Tenants, tenantA, tenantB)
	report.Assertions = append(report.Assertions,
		assertion("tenant-a-version", "1", fmt.Sprintf("%d", tenantA.Version)),
		assertion("tenant-b-version", "1", fmt.Sprintf("%d", tenantB.Version)),
		assertion("tenant-a-session", "active", sessionStatus(tenantA.State, loginA.SessionID)),
		assertion("tenant-b-session", "active", sessionStatus(tenantB.State, loginB.SessionID)),
	)
	finalizeCase(&report)
	return report, nil
}

func (r *caseRunner) sessionFixation() (CaseReport, error) {
	env, err := r.openEnv("session-fixation", authlab.Options{})
	if err != nil {
		return CaseReport{}, err
	}
	defer env.engine.Close()
	ctx := r.ctx
	report := CaseReport{
		Name: "session-fixation", Injection: "session-fixation",
		Description: "A client-supplied pre-auth session id is rejected with E_SESSION_FIXATION; a later clean login rotates the id.",
		Steps:       []StepReport{}, Assertions: []Assertion{}, Tenants: []TenantReport{},
	}
	before, err := mustSnapshot(ctx, env, sample.TenantA)
	if err != nil {
		return report, err
	}
	first, err := loginAs(ctx, env, sample.TenantA, sample.UserAlice)
	after, loadErr := mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("initial-login", "OK", 1, before, after, err, loadErr))
	if err != nil {
		return report, err
	}
	password, _ := sample.PasswordFor(sample.TenantA, sample.UserAlice)
	before = after
	_, err = env.engine.Login(ctx, authlab.LoginRequest{
		TenantID: sample.TenantA, Username: sample.UsernameAlice, Password: password,
		PriorSessionID: first.SessionID,
	})
	after, loadErr = mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("fixation-attempt", string(authlab.CodeSessionFixation), 0, before, after, err, loadErr,
		KV{Name: "prior_session_id", Value: first.SessionID}))

	before = after
	second, err := loginAs(ctx, env, sample.TenantA, sample.UserAlice)
	after, loadErr = mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("clean-login", "OK", 1, before, after, err, loadErr,
		KV{Name: "first_session_id", Value: first.SessionID}, KV{Name: "second_session_id", Value: second.SessionID}))
	if err != nil {
		return report, err
	}
	tenant, err := captureTenant(ctx, env, "main", sample.TenantA)
	if err != nil {
		return report, err
	}
	report.Tenants = append(report.Tenants, tenant)
	report.Assertions = append(report.Assertions,
		assertion("session-rotated", "true", boolString(first.SessionID != second.SessionID)),
		assertion("final-snapshot-version", "2", fmt.Sprintf("%d", tenant.Version)),
		assertion("session-count", "2", fmt.Sprintf("%d", len(tenant.State.Sessions))),
	)
	finalizeCase(&report)
	return report, nil
}

func (r *caseRunner) privilegeEscalation() (CaseReport, error) {
	env, err := r.openEnv("privilege-escalation", authlab.Options{})
	if err != nil {
		return CaseReport{}, err
	}
	defer env.engine.Close()
	ctx := r.ctx
	report := CaseReport{
		Name: "privilege-escalation", Injection: "privilege-escalation",
		Description: "Locally signed extra roles/permissions, cross-tenant resource access, and a revoked permission all fail with E_PRIVILEGE_ESCALATION.",
		Steps:       []StepReport{}, Assertions: []Assertion{}, Tenants: []TenantReport{},
	}
	before, err := mustSnapshot(ctx, env, sample.TenantA)
	if err != nil {
		return report, err
	}
	loginA, err := loginAs(ctx, env, sample.TenantA, sample.UserAlice)
	after, loadErr := mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("login-tenant-a", "OK", 1, before, after, err, loadErr))
	if err != nil {
		return report, err
	}
	beforeB, err := mustSnapshot(ctx, env, sample.TenantB)
	if err != nil {
		return report, err
	}
	loginB, err := loginAs(ctx, env, sample.TenantB, sample.UserCarol)
	afterB, loadErr := mustSnapshot(ctx, env, sample.TenantB)
	report.Steps = append(report.Steps, buildStep("login-tenant-b", "OK", 1, beforeB, afterB, err, loadErr))
	if err != nil {
		return report, err
	}
	claims, err := env.codec.Parse(loginA.AccessToken, env.clock.Now())
	if err != nil {
		return report, err
	}
	claims.Roles = append(claims.Roles, "admin")
	claims.Permissions = append(claims.Permissions, "admin:all")
	escalatedToken, err := env.codec.Sign(claims)
	if err != nil {
		return report, err
	}

	before = after
	_, err = env.engine.ValidateAccess(ctx, escalatedToken)
	after, loadErr = mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("forged-extra-role-and-permission", string(authlab.CodePrivilegeEscalation), 0, before, after, err, loadErr,
		KV{Name: "escalated_token", Value: authlab.TokenFingerprint(escalatedToken)},
		KV{Name: "added_role", Value: "admin"}, KV{Name: "added_permission", Value: "admin:all"}))

	before = after
	_, err = env.engine.AuthorizeResource(ctx, loginA.AccessToken, sample.TenantB, "doc:read")
	after, loadErr = mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("cross-tenant-resource-authorization", string(authlab.CodePrivilegeEscalation), 0, before, after, err, loadErr,
		KV{Name: "signed_tenant", Value: sample.TenantA}, KV{Name: "resource_tenant", Value: sample.TenantB}))

	before = after
	authorizationErr := env.engine.UpdateAuthorization(ctx, sample.TenantA, sample.UserAlice,
		[]string{"member"}, []string{"doc:read", "profile:read"})
	after, loadErr = mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("revoke-doc-write", "OK", 0, before, after, authorizationErr, loadErr,
		KV{Name: "revoked_permission", Value: "doc:write"}))

	before = after
	_, err = env.engine.ValidateAccess(ctx, loginA.AccessToken)
	after, loadErr = mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("old-token-after-permission-revocation", string(authlab.CodePrivilegeEscalation), 0, before, after, err, loadErr))

	beforeB = afterB
	_, err = env.engine.ValidateAccess(ctx, loginB.AccessToken)
	afterB, loadErr = mustSnapshot(ctx, env, sample.TenantB)
	report.Steps = append(report.Steps, buildStep("unrelated-tenant-session-still-valid", "OK", 0, beforeB, afterB, err, loadErr))

	tenantA, err := captureTenant(ctx, env, "main", sample.TenantA)
	if err != nil {
		return report, err
	}
	tenantB, err := captureTenant(ctx, env, "main", sample.TenantB)
	if err != nil {
		return report, err
	}
	report.Tenants = append(report.Tenants, tenantA, tenantB)
	report.Assertions = append(report.Assertions,
		assertion("tenant-a-snapshot-version", "1", fmt.Sprintf("%d", tenantA.Version)),
		assertion("tenant-b-snapshot-version", "1", fmt.Sprintf("%d", tenantB.Version)),
		assertion("tenant-a-session", "active", sessionStatus(tenantA.State, loginA.SessionID)),
		assertion("tenant-b-session", "active", sessionStatus(tenantB.State, loginB.SessionID)),
	)
	finalizeCase(&report)
	return report, nil
}

type refreshGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newRefreshGate() *refreshGate {
	return &refreshGate{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *refreshGate) hook(_ context.Context, operation authlab.Operation) error {
	if operation.Kind != authlab.OperationRefresh {
		return nil
	}
	g.once.Do(func() {
		close(g.entered)
		<-g.release
	})
	return nil
}

func (r *caseRunner) refreshRace() (CaseReport, error) {
	report := CaseReport{
		Name: "concurrent-refresh-race", Injection: "refresh-race",
		Description: "Concurrent use of one refresh token yields one winner; waiting overflow returns E_AUTH_CONTENTION, replay revokes the family, and no version ever has two active refresh tokens.",
		Steps:       []StepReport{}, Assertions: []Assertion{}, Tenants: []TenantReport{},
	}
	contentionCase, err := r.refreshContentionScenario()
	if err != nil {
		return report, err
	}
	report.Steps = append(report.Steps, contentionCase.Steps...)
	report.Assertions = append(report.Assertions, contentionCase.Assertions...)
	report.Tenants = append(report.Tenants, contentionCase.Tenants...)

	stampedeCase, err := r.refreshStampedeScenario()
	if err != nil {
		return report, err
	}
	report.Steps = append(report.Steps, stampedeCase.Steps...)
	report.Assertions = append(report.Assertions, stampedeCase.Assertions...)
	report.Tenants = append(report.Tenants, stampedeCase.Tenants...)
	finalizeCase(&report)
	return report, nil
}

func (r *caseRunner) refreshContentionScenario() (CaseReport, error) {
	gate := newRefreshGate()
	env, err := r.openEnv("refresh-race-contention", authlab.Options{
		MaxInFlight: 1, ContentionWait: r.options.ContentionWait, TransactionHook: gate.hook,
	})
	if err != nil {
		return CaseReport{}, err
	}
	defer env.engine.Close()
	ctx := r.ctx
	report := CaseReport{Name: "refresh-contention", Steps: []StepReport{}, Assertions: []Assertion{}, Tenants: []TenantReport{}}
	before, err := mustSnapshot(ctx, env, sample.TenantA)
	if err != nil {
		return report, err
	}
	login, err := loginAs(ctx, env, sample.TenantA, sample.UserAlice)
	after, loadErr := mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("login", "OK", 1, before, after, err, loadErr))
	if err != nil {
		return report, err
	}
	raceBefore := after

	type refreshResult struct {
		rotation *authlab.RotationResult
		err      error
	}
	winnerCh := make(chan refreshResult, 1)
	go func() {
		rotation, err := env.engine.Refresh(ctx, login.RefreshToken)
		winnerCh <- refreshResult{rotation: rotation, err: err}
	}()
	select {
	case <-gate.entered:
	case <-time.After(2 * time.Second):
		return report, fmt.Errorf("contention winner did not enter transaction hook")
	}
	losers := r.options.Concurrency - 1
	loserResults := make(chan error, losers)
	var wg sync.WaitGroup
	for i := 0; i < losers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := env.engine.Refresh(ctx, login.RefreshToken)
			loserResults <- err
		}()
	}
	wg.Wait()
	close(loserResults)
	close(gate.release)
	winner := <-winnerCh
	if winner.err != nil {
		return report, fmt.Errorf("contention winner failed: %w", winner.err)
	}

	contentionCount := 0
	unexpected := []string{}
	for err := range loserResults {
		if authlab.CodeOf(err) == authlab.CodeAuthContention {
			contentionCount++
		} else {
			unexpected = append(unexpected, string(authlab.CodeOf(err)))
		}
	}
	sort.Strings(unexpected)
	after, loadErr = mustSnapshot(ctx, env, sample.TenantA)
	raceStep := buildStep("bounded-concurrent-refresh", "OK", 1, raceBefore, after, nil, loadErr,
		KV{Name: "winner_count", Value: "1"},
		KV{Name: "contention_count", Value: fmt.Sprintf("%d", contentionCount)},
		KV{Name: "max_in_flight", Value: "1"},
		KV{Name: "unexpected_codes", Value: strings.Join(unexpected, ",")},
	)
	if contentionCount != losers || len(unexpected) != 0 {
		raceStep.Passed = false
	}
	raceStep.CodeCounts = []CodeCount{{Code: "OK", Count: 1}, {Code: string(authlab.CodeAuthContention), Count: contentionCount}}
	report.Steps = append(report.Steps, raceStep)

	before = after
	_, replayErr := env.engine.Refresh(ctx, login.RefreshToken)
	after, loadErr = mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("replay-old-refresh-token", string(authlab.CodeTokenReuse), 1, before, after, replayErr, loadErr))

	before = after
	_, accessErr := env.engine.ValidateAccess(ctx, winner.rotation.AccessToken)
	after, loadErr = mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("winner-access-after-family-revocation", string(authlab.CodeTokenInvalid), 0, before, after, accessErr, loadErr))

	before = after
	_, currentRefreshErr := env.engine.Refresh(ctx, winner.rotation.RefreshToken)
	after, loadErr = mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("winner-refresh-after-family-revocation", string(authlab.CodeTokenReuse), 0, before, after, currentRefreshErr, loadErr))

	tenant, err := captureTenant(ctx, env, "contention", sample.TenantA)
	if err != nil {
		return report, err
	}
	report.Tenants = append(report.Tenants, tenant)
	maxActive, err := maxActiveRefreshAcrossHistory(ctx, env, sample.TenantA)
	if err != nil {
		return report, err
	}
	report.Assertions = append(report.Assertions,
		assertion("contention-single-winner", "1", "1"),
		assertion("contention-other-requests", fmt.Sprintf("%d", losers), fmt.Sprintf("%d", contentionCount)),
		assertion("contention-final-version", "3", fmt.Sprintf("%d", tenant.Version)),
		assertion("contention-max-active-refresh", "1", fmt.Sprintf("%d", maxActive)),
		assertion("contention-final-active-refresh", "0", fmt.Sprintf("%d", activeRefreshCount(tenant.State))),
	)
	return report, nil
}

func (r *caseRunner) refreshStampedeScenario() (CaseReport, error) {
	env, err := r.openEnv("refresh-race-stampede", authlab.Options{
		MaxInFlight: r.options.Concurrency, ContentionWait: 5 * time.Second,
	})
	if err != nil {
		return CaseReport{}, err
	}
	defer env.engine.Close()
	ctx := r.ctx
	report := CaseReport{Name: "refresh-stampede", Steps: []StepReport{}, Assertions: []Assertion{}, Tenants: []TenantReport{}}
	before, err := mustSnapshot(ctx, env, sample.TenantA)
	if err != nil {
		return report, err
	}
	login, err := loginAs(ctx, env, sample.TenantA, sample.UserAlice)
	after, loadErr := mustSnapshot(ctx, env, sample.TenantA)
	report.Steps = append(report.Steps, buildStep("login", "OK", 1, before, after, err, loadErr))
	if err != nil {
		return report, err
	}
	raceBefore := after

	type result struct {
		rotation *authlab.RotationResult
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, r.options.Concurrency)
	var wg sync.WaitGroup
	for i := 0; i < r.options.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rotation, err := env.engine.Refresh(ctx, login.RefreshToken)
			results <- result{rotation: rotation, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	successes := 0
	reuse := 0
	contention := 0
	unexpected := []string{}
	for result := range results {
		switch authlab.CodeOf(result.err) {
		case "OK":
			successes++
		case authlab.CodeTokenReuse:
			reuse++
		case authlab.CodeAuthContention:
			contention++
		default:
			unexpected = append(unexpected, string(authlab.CodeOf(result.err)))
		}
	}
	sort.Strings(unexpected)
	after, loadErr = mustSnapshot(ctx, env, sample.TenantA)
	stampedeStep := buildStep("unbounded-concurrent-refresh", "OK", 2, raceBefore, after, nil, loadErr,
		KV{Name: "success_count", Value: fmt.Sprintf("%d", successes)},
		KV{Name: "reuse_count", Value: fmt.Sprintf("%d", reuse)},
		KV{Name: "contention_count", Value: fmt.Sprintf("%d", contention)},
		KV{Name: "unexpected_codes", Value: strings.Join(unexpected, ",")},
	)
	if successes != 1 || reuse != r.options.Concurrency-1 || contention != 0 || len(unexpected) != 0 {
		stampedeStep.Passed = false
	}
	stampedeStep.CodeCounts = []CodeCount{
		{Code: "OK", Count: successes},
		{Code: string(authlab.CodeTokenReuse), Count: reuse},
		{Code: string(authlab.CodeAuthContention), Count: contention},
	}
	report.Steps = append(report.Steps, stampedeStep)

	tenant, err := captureTenant(ctx, env, "stampede", sample.TenantA)
	if err != nil {
		return report, err
	}
	report.Tenants = append(report.Tenants, tenant)
	maxActive, err := maxActiveRefreshAcrossHistory(ctx, env, sample.TenantA)
	if err != nil {
		return report, err
	}
	report.Assertions = append(report.Assertions,
		assertion("stampede-single-winner", "1", fmt.Sprintf("%d", successes)),
		assertion("stampede-reuse-losers", fmt.Sprintf("%d", r.options.Concurrency-1), fmt.Sprintf("%d", reuse)),
		assertion("stampede-final-version", "3", fmt.Sprintf("%d", tenant.Version)),
		assertion("stampede-max-active-refresh", "1", fmt.Sprintf("%d", maxActive)),
		assertion("stampede-final-active-refresh", "0", fmt.Sprintf("%d", activeRefreshCount(tenant.State))),
	)
	return report, nil
}

func sessionStatus(state authlab.TenantState, sessionID string) string {
	for _, session := range state.Sessions {
		if session.SessionID == sessionID {
			return session.Status
		}
	}
	return "missing"
}

func activeRefreshCount(state authlab.TenantState) int {
	count := 0
	for _, record := range state.RefreshTokens {
		if record.Status == authlab.RefreshActive {
			count++
		}
	}
	return count
}

func maxActiveRefreshAcrossHistory(ctx context.Context, env *caseEnv, tenantID string) (int, error) {
	history, err := env.engine.SnapshotHistory(ctx, tenantID)
	if err != nil {
		return 0, err
	}
	maxActive := 0
	for _, meta := range history {
		snapshot, err := env.engine.SnapshotAt(ctx, tenantID, meta.Version)
		if err != nil {
			return 0, err
		}
		if count := activeRefreshCount(snapshot.State); count > maxActive {
			maxActive = count
		}
	}
	return maxActive, nil
}
