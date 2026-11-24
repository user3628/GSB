package tokenforge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

type sequentialIDs struct {
	mu       sync.Mutex
	families int64
}

func (s *sequentialIDs) NewFamilyID(tenantID, _ string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.families++
	return fmt.Sprintf("fam_%s_%04d", tenantID, s.families), nil
}

func (s *sequentialIDs) NewTokenID(_ string, familyID string, sequence int64, kind TokenKind) (string, error) {
	return fmt.Sprintf("tok_%s_%d_%s", familyID, sequence, kind), nil
}

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		DatabasePath:               filepath.Join(t.TempDir(), "tokenforge.db"),
		SigningKey:                 []byte("deterministic-test-key-0123456789"),
		AccessTTL:                  30 * time.Minute,
		RefreshTTL:                 24 * time.Hour,
		DefaultFamilyScanThreshold: 4,
		MaxInFlight:                8,
		AdmissionWait:              2 * time.Second,
		Clock:                      func() time.Time { return testNow },
		IDs:                        &sequentialIDs{},
	}
}

func newTestService(t *testing.T, cfg Config) *Service {
	t.Helper()
	svc, err := NewService(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func createTestTenant(t *testing.T, svc *Service, id string) {
	t.Helper()
	err := svc.CreateTenant(context.Background(), TenantSpec{
		TenantID: id, DefaultUserID: "user_" + id,
		InitialQuotaFixed: 10 * Scale,
		UnitCostFixed:     333_333_333,
		UsageFixed:        1_500_000_000,
	})
	if err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
}

func issueTestPair(t *testing.T, svc *Service, tenantID string) IssueResult {
	t.Helper()
	res, err := svc.Issue(context.Background(), IssueRequest{TenantID: tenantID})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	return res
}

func TestFixedPointAndOverflow(t *testing.T) {
	product, charge, remainder, err := MulDivScale(333_333_333, 1_500_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if product != 499_999_999_500_000_000 || charge != 499_999_999 || remainder != 500_000_000 {
		t.Fatalf("unexpected fixed result: product=%d charge=%d remainder=%d", product, charge, remainder)
	}
	bound, err := ErrorBoundFixedUpper(remainder)
	if err != nil || bound != 1 {
		t.Fatalf("unexpected bound=%d err=%v", bound, err)
	}
	if _, err := CheckedMul(1<<62, 4); CodeOf(err) != CodeQuotaOverflow {
		t.Fatalf("expected overflow, got %v", err)
	}
	f, err := ParseFixed("-1.000000001")
	if err != nil || f.String() != "-1.000000001" {
		t.Fatalf("parse/format fixed: %v %s", err, f.String())
	}
}

func TestRefreshRotationReplayAndSnapshots(t *testing.T) {
	svc := newTestService(t, testConfig(t))
	ctx := context.Background()
	createTestTenant(t, svc, "tenant_small")
	first := issueTestPair(t, svc, "tenant_small")
	if first.SnapshotVersion != 1 {
		t.Fatalf("issue snapshot=%d", first.SnapshotVersion)
	}
	rotated, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: first.Pair.Refresh.Value})
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if rotated.ChargedFixed != 499_999_999 || rotated.RemainderUnits != 500_000_000 || rotated.SnapshotVersion != 2 {
		t.Fatalf("unexpected refresh result: %+v", rotated)
	}
	accessCheck, err := svc.ValidateAccess(ctx, rotated.Pair.Access.Value)
	if err != nil || accessCheck.Code != CodeOK {
		t.Fatalf("new access invalid: %+v %v", accessCheck, err)
	}
	oldAccess, err := svc.ValidateAccess(ctx, first.Pair.Access.Value)
	if CodeOf(err) != CodeTokenConsumed || oldAccess.Code != CodeTokenConsumed {
		t.Fatalf("old access should be consumed: %+v %v", oldAccess, err)
	}
	replay, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: first.Pair.Refresh.Value})
	if CodeOf(err) != CodeTokenReuse || replay.Code != CodeTokenReuse || replay.SnapshotVersion != 3 {
		t.Fatalf("replay should revoke family: %+v %v", replay, err)
	}
	revoked, err := svc.ValidateAccess(ctx, rotated.Pair.Access.Value)
	if CodeOf(err) != CodeTokenRevoked || revoked.Code != CodeTokenRevoked {
		t.Fatalf("family access should be revoked: %+v %v", revoked, err)
	}
	chain, err := svc.ReplaySnapshots(ctx, "tenant_small")
	if err != nil || chain.Code != CodeOK || len(chain.Snapshots) != 3 || chain.LatestContiguousVersion != 3 {
		t.Fatalf("snapshot chain: %+v %v", chain, err)
	}
	if len(chain.Comparisons) != 2 || chain.Comparisons[0].ContentChanged != true {
		t.Fatalf("snapshot comparison missing: %+v", chain.Comparisons)
	}
}

func TestSingleAndFamilyRevoke(t *testing.T) {
	svc := newTestService(t, testConfig(t))
	ctx := context.Background()
	createTestTenant(t, svc, "tenant_revoke")
	first := issueTestPair(t, svc, "tenant_revoke")
	single, err := svc.Revoke(ctx, RevokeRequest{Token: first.Pair.Refresh.Value, Scope: RevokeSingle})
	if err != nil || single.SnapshotVersion != 2 {
		t.Fatalf("single revoke: %+v %v", single, err)
	}
	_, err = svc.Refresh(ctx, RefreshRequest{RefreshToken: first.Pair.Refresh.Value})
	if CodeOf(err) != CodeTokenRevoked {
		t.Fatalf("single revoked refresh should fail with revoked, got %v", err)
	}
	check, err := svc.ValidateAccess(ctx, first.Pair.Access.Value)
	if err != nil || check.Code != CodeOK {
		t.Fatalf("sibling access should remain usable: %+v %v", check, err)
	}
	second := issueTestPair(t, svc, "tenant_revoke")
	if _, err := svc.Revoke(ctx, RevokeRequest{Token: second.Pair.Access.Value, Scope: RevokeSingle}); err != nil {
		t.Fatalf("single access revoke: %v", err)
	}
	check, err = svc.ValidateAccess(ctx, second.Pair.Access.Value)
	if CodeOf(err) != CodeTokenRevoked || check.Code != CodeTokenRevoked {
		t.Fatalf("single revoked access should fail: %+v %v", check, err)
	}
	if _, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: second.Pair.Refresh.Value}); err != nil {
		t.Fatalf("sibling refresh should survive single access revoke: %v", err)
	}
	family, err := svc.Revoke(ctx, RevokeRequest{Token: first.Pair.Access.Value, Scope: RevokeFamily})
	if err != nil || family.SnapshotVersion != 6 || family.RevokedCount != 2 {
		t.Fatalf("family revoke: %+v %v", family, err)
	}
	check, err = svc.ValidateAccess(ctx, first.Pair.Access.Value)
	if CodeOf(err) != CodeTokenRevoked || check.Code != CodeTokenRevoked {
		t.Fatalf("family access should be revoked: %+v %v", check, err)
	}
	if _, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: first.Pair.Refresh.Value}); CodeOf(err) != CodeTokenRevoked {
		t.Fatalf("family refresh should be revoked, got %v", err)
	}
}

func TestTokenInvalidExpiredSequence(t *testing.T) {
	svc := newTestService(t, testConfig(t))
	ctx := context.Background()
	createTestTenant(t, svc, "tenant_errors")
	first := issueTestPair(t, svc, "tenant_errors")

	tampered := tamperToken(first.Pair.Refresh.Value)
	_, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: tampered})
	if CodeOf(err) != CodeTokenInvalid {
		t.Fatalf("tampered signature code=%v", CodeOf(err))
	}
	expiredClaims := first.Pair.Refresh.Claims
	expiredClaims.IssuedAt = testNow.Add(-2 * time.Hour).UnixNano()
	expiredClaims.ExpiresAt = testNow.Add(-time.Second).UnixNano()
	expired, err := svc.signClaims(expiredClaims)
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.Refresh(ctx, RefreshRequest{RefreshToken: expired.Value})
	if CodeOf(err) != CodeTokenExpired {
		t.Fatalf("expired code=%v", CodeOf(err))
	}
	futureClaims := first.Pair.Refresh.Claims
	futureClaims.Sequence = 2
	futureClaims.TokenID = "tok_signed_future"
	future, err := svc.signClaims(futureClaims)
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.Refresh(ctx, RefreshRequest{RefreshToken: future.Value})
	if CodeOf(err) != CodeTokenSequence {
		t.Fatalf("sequence code=%v", CodeOf(err))
	}
}

func TestQuotaOverflowLeavesNoLedger(t *testing.T) {
	cfg := testConfig(t)
	svc := newTestService(t, cfg)
	ctx := context.Background()
	if err := svc.CreateTenant(ctx, TenantSpec{
		TenantID: "tenant_overflow", DefaultUserID: "user_overflow",
		InitialQuotaFixed: 1 << 62, UnitCostFixed: 1 << 62, UsageFixed: Scale,
	}); err != nil {
		t.Fatal(err)
	}
	first := issueTestPair(t, svc, "tenant_overflow")
	res, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: first.Pair.Refresh.Value})
	if CodeOf(err) != CodeQuotaOverflow || res.Code != CodeQuotaOverflow {
		t.Fatalf("expected quota overflow: %+v %v", res, err)
	}
	chain, err := svc.ReplaySnapshots(ctx, "tenant_overflow")
	if err != nil || chain.LatestContiguousVersion != 1 {
		t.Fatalf("snapshot changed after overflow: %+v %v", chain, err)
	}
	if len(chain.Snapshots) != 1 || len(chain.Snapshots[0].State.Charges) != 0 {
		t.Fatalf("error ledger was written: %+v", chain.Snapshots)
	}
	tenant, err := svc.GetTenant(ctx, "tenant_overflow")
	if err != nil || tenant.Quota.SpentFixed != 0 || tenant.Quota.ErrorBook.EntryCount != 0 {
		t.Fatalf("quota changed after overflow: %+v %v", tenant, err)
	}
}

func TestBeforeCommitRollbackThenRetry(t *testing.T) {
	cfg := testConfig(t)
	var fail atomic.Bool
	fail.Store(true)
	cfg.BeforeCommit = func(_ context.Context, info CommitInfo) error {
		if info.Operation == "refresh" && fail.Load() {
			return errors.New("injected abort")
		}
		return nil
	}
	svc := newTestService(t, cfg)
	ctx := context.Background()
	createTestTenant(t, svc, "tenant_abort")
	first := issueTestPair(t, svc, "tenant_abort")
	_, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: first.Pair.Refresh.Value})
	if CodeOf(err) != CodeAborted {
		t.Fatalf("expected abort, got %v", err)
	}
	chain, err := svc.ReplaySnapshots(ctx, "tenant_abort")
	if err != nil || chain.LatestContiguousVersion != 1 {
		t.Fatalf("snapshot changed after abort: %+v %v", chain, err)
	}
	tenant, _ := svc.GetTenant(ctx, "tenant_abort")
	if tenant.Quota.SpentFixed != 0 {
		t.Fatalf("quota changed after abort: +%v", tenant.Quota)
	}
	fail.Store(false)
	rotated, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: first.Pair.Refresh.Value})
	if err != nil || rotated.SnapshotVersion != 2 {
		t.Fatalf("retry after abort failed: %+v %v", rotated, err)
	}
}

func TestSnapshotGapFallback(t *testing.T) {
	svc := newTestService(t, testConfig(t))
	ctx := context.Background()
	createTestTenant(t, svc, "tenant_gap")
	r1 := issueTestPair(t, svc, "tenant_gap")
	r2, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: r1.Pair.Refresh.Value})
	if err != nil {
		t.Fatal(err)
	}
	r3, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: r2.Pair.Refresh.Value})
	if err != nil {
		t.Fatal(err)
	}
	svc.InjectNextSnapshotGap()
	r4, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: r3.Pair.Refresh.Value})
	if err != nil || r4.SnapshotVersion != 5 {
		t.Fatalf("gap refresh: %+v %v", r4, err)
	}
	chain, err := svc.ReplaySnapshots(ctx, "tenant_gap")
	if CodeOf(err) != CodeSnapshotVersion {
		t.Fatalf("expected snapshot version error, got %v", err)
	}
	if chain.Code != CodeSnapshotVersion || chain.LatestContiguousVersion != 3 || chain.StoredVersion != 5 {
		t.Fatalf("unexpected fallback: %+v", chain)
	}
}

func TestFamilyPathParity(t *testing.T) {
	svc := newTestService(t, testConfig(t))
	ctx := context.Background()
	createTestTenant(t, svc, "tenant_parity")
	for i := 0; i < 5; i++ {
		issueTestPair(t, svc, "tenant_parity")
	}
	precise, err := svc.AuditFamilies(ctx, "tenant_parity", FamilyQueryOptions{Threshold: 10})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := svc.AuditFamilies(ctx, "tenant_parity", FamilyQueryOptions{Threshold: 10, ForcePath: PathStream})
	if err != nil {
		t.Fatal(err)
	}
	if precise.Path != PathPrecise || stream.Path != PathStream || !stream.IndexReady {
		t.Fatalf("paths not observed: precise=%s stream=%s", precise.Path, stream.Path)
	}
	preciseJSON, _ := json.Marshal(precise.Families)
	streamJSON, _ := json.Marshal(stream.Families)
	if string(preciseJSON) != string(streamJSON) || precise.MembersDigest != stream.MembersDigest {
		t.Fatalf("path conclusions differ")
	}
}

func TestConcurrentRefreshOneWinner(t *testing.T) {
	cfg := testConfig(t)
	var firstHook atomic.Bool
	firstHook.Store(true)
	cfg.BeforeCommit = func(ctx context.Context, info CommitInfo) error {
		if info.Operation == "refresh" && firstHook.CompareAndSwap(true, false) {
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	}
	svc := newTestService(t, cfg)
	ctx := context.Background()
	createTestTenant(t, svc, "tenant_race")
	first := issueTestPair(t, svc, "tenant_race")
	const workers = 8
	results := make(chan Code, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: first.Pair.Refresh.Value})
			results <- CodeOf(err)
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	counts := map[Code]int{}
	for code := range results {
		counts[code]++
	}
	if counts[CodeOK] != 1 || counts[CodeTokenConsumed] != workers-1 {
		t.Fatalf("unexpected race codes: %+v", counts)
	}
	replay, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: first.Pair.Refresh.Value})
	if CodeOf(err) != CodeTokenReuse || replay.Code != CodeTokenReuse {
		t.Fatalf("post-race replay should revoke family: %+v %v", replay, err)
	}
}

func TestQuotaContention(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxInFlight = 1
	cfg.AdmissionWait = 40 * time.Millisecond
	entered := make(chan struct{})
	var once sync.Once
	cfg.BeforeCommit = func(ctx context.Context, info CommitInfo) error {
		if info.Operation == "refresh" {
			once.Do(func() { close(entered) })
			time.Sleep(200 * time.Millisecond)
		}
		return nil
	}
	svc := newTestService(t, cfg)
	ctx := context.Background()
	createTestTenant(t, svc, "tenant_contend")
	a := issueTestPair(t, svc, "tenant_contend")
	b := issueTestPair(t, svc, "tenant_contend")
	errCh := make(chan error, 1)
	go func() {
		_, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: a.Pair.Refresh.Value})
		errCh <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first refresh did not enter transaction")
	}
	contendedResult, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: b.Pair.Refresh.Value})
	if CodeOf(err) != CodeQuotaContention || contendedResult.QueueNanos <= 0 {
		t.Fatalf("expected contention, got %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("first refresh failed: %v", err)
	}
	metrics := svc.Metrics()
	if metrics.Contended < 1 || metrics.PeakInFlight != 1 || metrics.Admitted < 1 || metrics.QueueObservations == 0 || metrics.QueueTotalNanos <= 0 || metrics.QueueMaxNanos <= 0 {
		t.Fatalf("metrics not observable: %+v", metrics)
	}
}

func TestConcurrentDifferentTenants(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxInFlight = 8
	cfg.AdmissionWait = 5 * time.Second
	svc := newTestService(t, cfg)
	ctx := context.Background()
	const tenants = 6
	pairs := make([]IssueResult, tenants)
	for i := 0; i < tenants; i++ {
		tenantID := fmt.Sprintf("tenant_multi_%d", i)
		createTestTenant(t, svc, tenantID)
		pairs[i] = issueTestPair(t, svc, tenantID)
	}
	start := make(chan struct{})
	errs := make(chan error, tenants)
	var wg sync.WaitGroup
	for i := 0; i < tenants; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			_, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: pairs[index].Pair.Refresh.Value})
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent different-tenant refresh failed: %v", err)
		}
	}
}

func TestCrashRecovery(t *testing.T) {
	if os.Getenv("TOKENFORGE_CRASH_HELPER") == "1" {
		t.Skip("helper process is driven by TestCrashRecovery")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "crash.db")
	tokenPath := filepath.Join(dir, "token.txt")
	markerPath := filepath.Join(dir, "marker.txt")
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashRecoveryHelper")
	cmd.Env = append(os.Environ(),
		"TOKENFORGE_CRASH_HELPER=1",
		"TOKENFORGE_DB="+dbPath,
		"TOKENFORGE_TOKEN="+tokenPath,
		"TOKENFORGE_MARKER="+markerPath,
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(markerPath); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := os.Stat(markerPath); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("helper did not reach pre-commit marker: %v", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill helper: %v", err)
	}
	_ = cmd.Wait()
	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatalf("read token: %v", err)
	}
	cfg := crashTestConfig(dbPath)
	svc, err := NewService(context.Background(), cfg)
	if err != nil {
		t.Fatalf("reopen after kill: %v", err)
	}
	defer svc.Close()
	chain, err := svc.ReplaySnapshots(context.Background(), "tenant_crash")
	if err != nil || chain.LatestContiguousVersion != 1 {
		t.Fatalf("committed state not recovered: %+v %v", chain, err)
	}
	rotated, err := svc.Refresh(context.Background(), RefreshRequest{RefreshToken: strings.TrimSpace(string(tokenBytes))})
	if err != nil || rotated.SnapshotVersion != 2 {
		t.Fatalf("token not refreshable after crash: %+v %v", rotated, err)
	}
	svc.InjectNextSnapshotGap()
	gapped, err := svc.Refresh(context.Background(), RefreshRequest{RefreshToken: rotated.Pair.Refresh.Value})
	if err != nil || gapped.SnapshotVersion != 4 {
		t.Fatalf("post-recovery gap refresh: %+v %v", gapped, err)
	}
	gapChain, err := svc.ReplaySnapshots(context.Background(), "tenant_crash")
	if CodeOf(err) != CodeSnapshotVersion || gapChain.LatestContiguousVersion != 2 {
		t.Fatalf("post-recovery gap not rejected with fallback: %+v %v", gapChain, err)
	}
}

func TestCrashRecoveryHelper(t *testing.T) {
	if os.Getenv("TOKENFORGE_CRASH_HELPER") != "1" {
		t.Skip("not helper process")
	}
	dbPath := os.Getenv("TOKENFORGE_DB")
	tokenPath := os.Getenv("TOKENFORGE_TOKEN")
	markerPath := os.Getenv("TOKENFORGE_MARKER")
	cfg := crashTestConfig(dbPath)
	cfg.BeforeCommit = func(ctx context.Context, info CommitInfo) error {
		if info.Operation == "refresh" {
			if err := os.WriteFile(markerPath, []byte("pre-commit"), 0o600); err != nil {
				return err
			}
			time.Sleep(30 * time.Second)
		}
		return nil
	}
	svc, err := NewService(context.Background(), cfg)
	if err != nil {
		t.Fatalf("helper NewService: %v", err)
	}
	createTestTenant(t, svc, "tenant_crash")
	first := issueTestPair(t, svc, "tenant_crash")
	if err := os.WriteFile(tokenPath, []byte(first.Pair.Refresh.Value), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	_, _ = svc.Refresh(context.Background(), RefreshRequest{RefreshToken: first.Pair.Refresh.Value})
}

func crashTestConfig(dbPath string) Config {
	return Config{
		DatabasePath:               dbPath,
		SigningKey:                 []byte("deterministic-test-key-0123456789"),
		AccessTTL:                  30 * time.Minute,
		RefreshTTL:                 24 * time.Hour,
		DefaultFamilyScanThreshold: 4,
		MaxInFlight:                4,
		AdmissionWait:              2 * time.Second,
		Clock:                      func() time.Time { return testNow },
		IDs:                        &sequentialIDs{},
	}
}

func tamperToken(raw string) string {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 || parts[2] == "" {
		return raw + "x"
	}
	repl := byte('A')
	if parts[2][0] == 'A' {
		repl = 'B'
	}
	parts[2] = string(repl) + parts[2][1:]
	return strings.Join(parts, ".")
}

func TestSnapshotComparisonDetectsContentReplacement(t *testing.T) {
	base := SnapshotRecord{
		Version: 4, ContentHash: "hash-a",
		State: SnapshotState{
			TenantID: "t", Version: 4,
			Families: []SnapshotFamily{{FamilyID: "f1", Status: familyStatusActive}},
			Tokens:   []SnapshotToken{{TokenID: "tok1", FamilyID: "f1", Status: tokenStatusActive}},
		},
	}
	replacedSameVersion := base
	replacedSameVersion.ContentHash = "hash-b"
	replacedSameVersion.State.Families = []SnapshotFamily{{FamilyID: "f2", Status: familyStatusActive}}
	replacedSameVersion.State.Tokens = []SnapshotToken{{TokenID: "tok2", FamilyID: "f2", Status: tokenStatusActive}}
	cmp := CompareSnapshots(base, replacedSameVersion)
	if !cmp.SameVersion || !cmp.ContentChanged || !cmp.WholeReplacement || cmp.VersionOnlyChange {
		t.Fatalf("same-version replacement not detected: %+v", cmp)
	}
	versionOnly := replacedSameVersion
	versionOnly.Version = 5
	versionOnly.ContentHash = "hash-b"
	cmp = CompareSnapshots(replacedSameVersion, versionOnly)
	if !cmp.VersionOnlyChange || cmp.ContentChanged {
		t.Fatalf("version-only change not detected: %+v", cmp)
	}
}
