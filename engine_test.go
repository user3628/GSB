package authlab_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"example.com/authlab"
	"example.com/authlab/sample"
	_ "modernc.org/sqlite"
)

type testEnv struct {
	engine  *authlab.Engine
	clock   *authlab.ManualClock
	fixture authlab.Fixture
	dbPath  string
}

func newTestEnv(t *testing.T, options authlab.Options) *testEnv {
	t.Helper()
	ctx := context.Background()
	clock := authlab.NewManualClock(sample.BaseTime)
	fixture := sample.Fixture()
	options.Clock = clock
	options.IDs = authlab.NewDeterministicIDSource(fixture.IDSeed)
	if options.MaxInFlight == 0 {
		options.MaxInFlight = 16
	}
	if options.ContentionWait == 0 {
		options.ContentionWait = time.Second
	}
	dbPath := filepath.Join(t.TempDir(), "authlab.sqlite")
	store, err := authlab.OpenSQLiteStore(ctx, dbPath, clock.Now)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := store.Init(ctx, fixture); err != nil {
		t.Fatalf("init store: %v", err)
	}
	engine, err := authlab.NewEngine(store, fixture, options)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	return &testEnv{engine: engine, clock: clock, fixture: fixture, dbPath: dbPath}
}

func (env *testEnv) login(t *testing.T, tenantID, username, password string) *authlab.LoginResult {
	t.Helper()
	result, err := env.engine.Login(context.Background(), authlab.LoginRequest{
		TenantID: tenantID, Username: username, Password: password,
	})
	if err != nil {
		t.Fatalf("login %s/%s: %v", tenantID, username, err)
	}
	return result
}

func requireCode(t *testing.T, err error, want authlab.Code) {
	t.Helper()
	if got := authlab.CodeOf(err); got != want {
		t.Fatalf("code = %s, want %s (err=%v)", got, want, err)
	}
}

func currentSnapshot(t *testing.T, env *testEnv, tenantID string) authlab.Snapshot {
	t.Helper()
	snapshot, err := env.engine.CurrentSnapshot(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("current snapshot: %v", err)
	}
	return snapshot
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

func TestPBKDF2SHA256KnownVector(t *testing.T) {
	got := authlab.HashPassword("password", []byte("salt"), 1, 32)
	if hex.EncodeToString(got) != "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b" {
		t.Fatalf("PBKDF2 vector mismatch: %x", got)
	}
}

func TestLoginTokenValidationAndExpiry(t *testing.T) {
	env := newTestEnv(t, authlab.Options{AccessTTL: 5 * time.Minute, RefreshTTL: time.Hour})
	ctx := context.Background()
	login := env.login(t, sample.TenantA, sample.UsernameAlice, sample.PasswordAlice)
	if login.SnapshotVersion != 1 {
		t.Fatalf("login snapshot version = %d, want 1", login.SnapshotVersion)
	}
	if _, err := env.engine.ValidateAccess(ctx, login.AccessToken); err != nil {
		t.Fatalf("validate freshly issued token: %v", err)
	}

	tampered := login.AccessToken[:len(login.AccessToken)-1] + "A"
	if tampered == login.AccessToken {
		tampered = login.AccessToken[:len(login.AccessToken)-1] + "B"
	}
	_, err := env.engine.ValidateAccess(ctx, tampered)
	requireCode(t, err, authlab.CodeTokenInvalid)

	codec := authlab.NewTokenCodec(env.fixture.SigningKey)
	missingTenant, err := codec.Sign(authlab.Claims{
		TokenID: "at_missing_tenant", Type: authlab.AccessToken, UserID: sample.UserAlice,
		SessionID: "sess_missing", ExpiresAt: env.clock.Now().Add(time.Minute).Unix(),
	})
	if err != nil {
		t.Fatalf("sign tenant-less token: %v", err)
	}
	_, err = codec.Parse(missingTenant, env.clock.Now())
	requireCode(t, err, authlab.CodeTokenInvalid)

	env.clock.Advance(6 * time.Minute)
	_, err = env.engine.ValidateAccess(ctx, login.AccessToken)
	requireCode(t, err, authlab.CodeTokenInvalid)
	env.clock.Advance(2 * time.Hour)
	_, err = env.engine.Refresh(ctx, login.RefreshToken)
	requireCode(t, err, authlab.CodeTokenInvalid)

	snapshot := currentSnapshot(t, env, sample.TenantA)
	if snapshot.Version != 1 || snapshot.Digest == "" {
		t.Fatalf("failed validations changed snapshot: version=%d digest=%q", snapshot.Version, snapshot.Digest)
	}
}

func TestLoginDoesNotRevealUserExistence(t *testing.T) {
	env := newTestEnv(t, authlab.Options{})
	ctx := context.Background()
	_, errUnknown := env.engine.Login(ctx, authlab.LoginRequest{
		TenantID: sample.TenantA, Username: "not-a-user", Password: "irrelevant",
	})
	_, errWrong := env.engine.Login(ctx, authlab.LoginRequest{
		TenantID: sample.TenantA, Username: sample.UsernameAlice, Password: "wrong-password",
	})
	requireCode(t, errUnknown, authlab.CodeAuthInvalid)
	requireCode(t, errWrong, authlab.CodeAuthInvalid)
}

func TestCrossTenantOperationsDoNotMutateEitherTenant(t *testing.T) {
	env := newTestEnv(t, authlab.Options{})
	ctx := context.Background()
	loginA := env.login(t, sample.TenantA, sample.UsernameAlice, sample.PasswordAlice)
	loginB := env.login(t, sample.TenantB, sample.UsernameCarol, sample.PasswordCarol)
	beforeA := currentSnapshot(t, env, sample.TenantA)
	beforeB := currentSnapshot(t, env, sample.TenantB)

	_, err := env.engine.ValidateAccessForTenant(ctx, sample.TenantB, loginA.AccessToken)
	requireCode(t, err, authlab.CodeCrossTenant)
	_, err = env.engine.RefreshForTenant(ctx, sample.TenantB, loginA.RefreshToken)
	requireCode(t, err, authlab.CodeCrossTenant)
	if _, err := env.engine.ValidateAccessForTenant(ctx, sample.TenantA, loginA.AccessToken); err != nil {
		t.Fatalf("same-tenant validation: %v", err)
	}
	if _, err := env.engine.ValidateAccessForTenant(ctx, sample.TenantB, loginB.AccessToken); err != nil {
		t.Fatalf("tenant B validation: %v", err)
	}

	afterA := currentSnapshot(t, env, sample.TenantA)
	afterB := currentSnapshot(t, env, sample.TenantB)
	if beforeA.Version != afterA.Version || beforeA.Digest != afterA.Digest {
		t.Fatalf("tenant A changed: before v%d %s after v%d %s", beforeA.Version, beforeA.Digest, afterA.Version, afterA.Digest)
	}
	if beforeB.Version != afterB.Version || beforeB.Digest != afterB.Digest {
		t.Fatalf("tenant B changed: before v%d %s after v%d %s", beforeB.Version, beforeB.Digest, afterB.Version, afterB.Digest)
	}
}

func TestRefreshRotationAndReuseRevokesFamily(t *testing.T) {
	env := newTestEnv(t, authlab.Options{})
	ctx := context.Background()
	login := env.login(t, sample.TenantA, sample.UsernameAlice, sample.PasswordAlice)
	rotated, err := env.engine.Refresh(ctx, login.RefreshToken)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if rotated.SnapshotVersion != 2 {
		t.Fatalf("rotation version = %d, want 2", rotated.SnapshotVersion)
	}
	if _, err := env.engine.ValidateAccess(ctx, rotated.AccessToken); err != nil {
		t.Fatalf("new access token: %v", err)
	}

	_, err = env.engine.Refresh(ctx, login.RefreshToken)
	requireCode(t, err, authlab.CodeTokenReuse)
	afterReuse := currentSnapshot(t, env, sample.TenantA)
	if afterReuse.Version != 3 {
		t.Fatalf("reuse version = %d, want 3", afterReuse.Version)
	}
	if activeRefreshCount(afterReuse.State) != 0 {
		t.Fatalf("refresh family still active after reuse")
	}
	if afterReuse.State.Sessions[0].Status != authlab.SessionRevoked {
		t.Fatalf("session not revoked after reuse")
	}
	_, err = env.engine.Refresh(ctx, rotated.RefreshToken)
	requireCode(t, err, authlab.CodeTokenReuse)
	_, err = env.engine.ValidateAccess(ctx, rotated.AccessToken)
	requireCode(t, err, authlab.CodeTokenInvalid)
	still := currentSnapshot(t, env, sample.TenantA)
	if still.Version != 3 {
		t.Fatalf("repeated replay changed version to %d", still.Version)
	}
}

func TestRevokeSessionCreatesNewVersionAndRevokesFamily(t *testing.T) {
	env := newTestEnv(t, authlab.Options{})
	ctx := context.Background()
	login := env.login(t, sample.TenantA, sample.UsernameAlice, sample.PasswordAlice)
	rotated, err := env.engine.Refresh(ctx, login.RefreshToken)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if rotated.SnapshotVersion != 2 {
		t.Fatalf("refresh version = %d, want 2", rotated.SnapshotVersion)
	}
	revoked, err := env.engine.RevokeSession(ctx, rotated.AccessToken)
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if revoked.Version != 3 {
		t.Fatalf("revoke version = %d, want 3", revoked.Version)
	}
	if revoked.State.Sessions[0].Status != authlab.SessionRevoked || activeRefreshCount(revoked.State) != 0 {
		t.Fatalf("revoke did not terminate session/family: %+v", revoked.State)
	}
	_, err = env.engine.ValidateAccess(ctx, rotated.AccessToken)
	requireCode(t, err, authlab.CodeTokenInvalid)
	_, err = env.engine.Refresh(ctx, rotated.RefreshToken)
	requireCode(t, err, authlab.CodeTokenReuse)
	if current := currentSnapshot(t, env, sample.TenantA); current.Version != 3 {
		t.Fatalf("replay after revoke changed version to %d", current.Version)
	}
}

func TestInFlightContentionReturnsStableCode(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseGate()
	env := newTestEnv(t, authlab.Options{
		MaxInFlight: 1, ContentionWait: 10 * time.Millisecond,
		TransactionHook: func(_ context.Context, operation authlab.Operation) error {
			if operation.Kind == authlab.OperationRefresh {
				enterOnce.Do(func() {
					close(entered)
					<-release
				})
			}
			return nil
		},
	})
	ctx := context.Background()
	login := env.login(t, sample.TenantA, sample.UsernameAlice, sample.PasswordAlice)
	firstResult := make(chan error, 1)
	go func() {
		_, err := env.engine.Refresh(ctx, login.RefreshToken)
		firstResult <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first refresh did not enter hook")
	}
	_, err := env.engine.Refresh(ctx, login.RefreshToken)
	requireCode(t, err, authlab.CodeAuthContention)
	releaseGate()
	if err := <-firstResult; err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	snapshot := currentSnapshot(t, env, sample.TenantA)
	if snapshot.Version != 2 || activeRefreshCount(snapshot.State) != 1 {
		t.Fatalf("unexpected state after contention: version=%d active=%d", snapshot.Version, activeRefreshCount(snapshot.State))
	}
}

func TestPrivilegeEscalationAndAuthorizationRevocation(t *testing.T) {
	env := newTestEnv(t, authlab.Options{})
	ctx := context.Background()
	loginA := env.login(t, sample.TenantA, sample.UsernameAlice, sample.PasswordAlice)
	loginB := env.login(t, sample.TenantB, sample.UsernameCarol, sample.PasswordCarol)
	codec := authlab.NewTokenCodec(env.fixture.SigningKey)
	claims, err := codec.Parse(loginA.AccessToken, env.clock.Now())
	if err != nil {
		t.Fatalf("parse token: %v", err)
	}
	claims.Roles = append(claims.Roles, "admin")
	claims.Permissions = append(claims.Permissions, "admin:all")
	escalated, err := codec.Sign(claims)
	if err != nil {
		t.Fatalf("sign escalated token: %v", err)
	}
	before := currentSnapshot(t, env, sample.TenantA)
	_, err = env.engine.ValidateAccess(ctx, escalated)
	requireCode(t, err, authlab.CodePrivilegeEscalation)
	_, err = env.engine.AuthorizeResource(ctx, loginA.AccessToken, sample.TenantB, "doc:read")
	requireCode(t, err, authlab.CodePrivilegeEscalation)
	if after := currentSnapshot(t, env, sample.TenantA); after.Version != before.Version || after.Digest != before.Digest {
		t.Fatalf("failed privilege checks changed state")
	}

	err = env.engine.UpdateAuthorization(ctx, sample.TenantA, sample.UserAlice,
		[]string{"member"}, []string{"doc:read", "profile:read"})
	if err != nil {
		t.Fatalf("update authorization: %v", err)
	}
	_, err = env.engine.ValidateAccess(ctx, loginA.AccessToken)
	requireCode(t, err, authlab.CodePrivilegeEscalation)
	if _, err := env.engine.ValidateAccess(ctx, loginB.AccessToken); err != nil {
		t.Fatalf("unrelated tenant session was affected: %v", err)
	}
	if after := currentSnapshot(t, env, sample.TenantA); after.Version != 1 {
		t.Fatalf("authorization update unexpectedly changed session snapshot version: %d", after.Version)
	}
}

func TestSessionFixationIsRejectedAndCleanLoginRotates(t *testing.T) {
	env := newTestEnv(t, authlab.Options{})
	ctx := context.Background()
	first := env.login(t, sample.TenantA, sample.UsernameAlice, sample.PasswordAlice)
	before := currentSnapshot(t, env, sample.TenantA)
	_, err := env.engine.Login(ctx, authlab.LoginRequest{
		TenantID: sample.TenantA, Username: sample.UsernameAlice, Password: sample.PasswordAlice,
		PriorSessionID: first.SessionID,
	})
	requireCode(t, err, authlab.CodeSessionFixation)
	after := currentSnapshot(t, env, sample.TenantA)
	if after.Version != before.Version || after.Digest != before.Digest {
		t.Fatalf("fixation rejection mutated state")
	}
	second := env.login(t, sample.TenantA, sample.UsernameAlice, sample.PasswordAlice)
	if second.SessionID == first.SessionID {
		t.Fatalf("clean login did not rotate session id")
	}
	if second.SnapshotVersion != 2 {
		t.Fatalf("second login version = %d, want 2", second.SnapshotVersion)
	}
}

func TestConcurrentRefreshHasSingleActiveWinner(t *testing.T) {
	const requests = 8
	env := newTestEnv(t, authlab.Options{MaxInFlight: requests, ContentionWait: 2 * time.Second})
	ctx := context.Background()
	login := env.login(t, sample.TenantA, sample.UsernameAlice, sample.PasswordAlice)

	type result struct {
		rotation *authlab.RotationResult
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, requests)
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
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
	var winner *authlab.RotationResult
	for result := range results {
		if result.err == nil {
			successes++
			winner = result.rotation
			continue
		}
		if authlab.CodeOf(result.err) == authlab.CodeTokenReuse {
			reuse++
			continue
		}
		t.Fatalf("unexpected concurrent refresh code: %s (%v)", authlab.CodeOf(result.err), result.err)
	}
	if successes != 1 || reuse != requests-1 || winner == nil {
		t.Fatalf("successes=%d reuse=%d, want 1/%d", successes, reuse, requests-1)
	}
	final := currentSnapshot(t, env, sample.TenantA)
	if final.Version != 3 {
		t.Fatalf("final version = %d, want 3", final.Version)
	}
	version2, err := env.engine.SnapshotAt(ctx, sample.TenantA, 2)
	if err != nil {
		t.Fatalf("snapshot v2: %v", err)
	}
	if got := activeRefreshCount(version2.State); got != 1 {
		t.Fatalf("v2 active refresh count = %d, want 1", got)
	}
	if got := activeRefreshCount(final.State); got != 0 {
		t.Fatalf("final active refresh count = %d, want 0", got)
	}
	_, err = env.engine.ValidateAccess(ctx, winner.AccessToken)
	requireCode(t, err, authlab.CodeTokenInvalid)
}

func TestSnapshotSameVersionReplacementRejectedAndRepaired(t *testing.T) {
	env := newTestEnv(t, authlab.Options{})
	ctx := context.Background()
	login := env.login(t, sample.TenantA, sample.UsernameAlice, sample.PasswordAlice)
	rotated, err := env.engine.Refresh(ctx, login.RefreshToken)
	if err != nil {
		t.Fatalf("refresh to v2: %v", err)
	}
	if _, err := env.engine.Refresh(ctx, rotated.RefreshToken); err != nil {
		t.Fatalf("refresh to v3: %v", err)
	}
	db, err := sql.Open("sqlite", env.dbPath)
	if err != nil {
		t.Fatalf("open raw sqlite: %v", err)
	}
	defer db.Close()
	var content []byte
	if err := db.QueryRow(`SELECT content FROM snapshots WHERE tenant_id=? AND version=3`, sample.TenantA).Scan(&content); err != nil {
		t.Fatalf("read v3 content: %v", err)
	}
	replacement := bytes.Replace(content, []byte(`"status":"active"`), []byte(`"status":"replaced"`), 1)
	if bytes.Equal(replacement, content) {
		t.Fatalf("test replacement did not alter content")
	}
	digest := sha256.Sum256(replacement)
	if _, err := db.Exec(`UPDATE snapshots SET content=?, digest=? WHERE tenant_id=? AND version=3`,
		replacement, hex.EncodeToString(digest[:]), sample.TenantA); err != nil {
		t.Fatalf("tamper v3: %v", err)
	}

	fallback, err := env.engine.CurrentSnapshot(ctx, sample.TenantA)
	requireCode(t, err, authlab.CodeSnapshotVersion)
	if fallback.Version != 2 {
		t.Fatalf("fallback version = %d, want 2", fallback.Version)
	}
	audit, err := env.engine.AuditSnapshots(ctx, sample.TenantA)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if audit.IntegrityOK || audit.ValidVersion != 2 || audit.HeadVersion != 3 {
		t.Fatalf("unexpected audit: %+v", audit)
	}
	repaired, changed, err := env.engine.RepairSnapshots(ctx, sample.TenantA)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if !changed || repaired.Version != 2 {
		t.Fatalf("repair result version=%d changed=%v", repaired.Version, changed)
	}
	if _, err := env.engine.CurrentSnapshot(ctx, sample.TenantA); err != nil {
		t.Fatalf("snapshot after repair: %v", err)
	}
	if _, err := env.engine.Login(ctx, authlab.LoginRequest{TenantID: sample.TenantA, Username: sample.UsernameAlice, Password: sample.PasswordAlice}); err != nil {
		t.Fatalf("login after repair: %v", err)
	}
	if version := currentSnapshot(t, env, sample.TenantA).Version; version != 3 {
		t.Fatalf("post-repair version = %d, want 3", version)
	}
}

func TestSnapshotVersionGapFallsBackAndRepairs(t *testing.T) {
	env := newTestEnv(t, authlab.Options{})
	ctx := context.Background()
	login := env.login(t, sample.TenantA, sample.UsernameAlice, sample.PasswordAlice)
	rotated, err := env.engine.Refresh(ctx, login.RefreshToken)
	if err != nil {
		t.Fatalf("refresh to v2: %v", err)
	}
	if _, err := env.engine.Refresh(ctx, rotated.RefreshToken); err != nil {
		t.Fatalf("refresh to v3: %v", err)
	}
	db, err := sql.Open("sqlite", env.dbPath)
	if err != nil {
		t.Fatalf("open raw sqlite: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`DELETE FROM snapshots WHERE tenant_id=? AND version=2`, sample.TenantA); err != nil {
		t.Fatalf("delete v2: %v", err)
	}
	fallback, err := env.engine.CurrentSnapshot(ctx, sample.TenantA)
	requireCode(t, err, authlab.CodeSnapshotVersion)
	if fallback.Version != 1 {
		t.Fatalf("fallback version = %d, want 1", fallback.Version)
	}
	audit, err := env.engine.AuditSnapshots(ctx, sample.TenantA)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if audit.IntegrityOK || audit.ValidVersion != 1 {
		t.Fatalf("unexpected gap audit: %+v", audit)
	}
	repaired, changed, err := env.engine.RepairSnapshots(ctx, sample.TenantA)
	if err != nil || !changed || repaired.Version != 1 {
		t.Fatalf("repair version=%d changed=%v err=%v", repaired.Version, changed, err)
	}
	history, err := env.engine.SnapshotHistory(ctx, sample.TenantA)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 1 || history[0].Version != 1 {
		t.Fatalf("history after repair = %+v", history)
	}
}

func TestAuthenticationMiddlewareTenantBoundary(t *testing.T) {
	env := newTestEnv(t, authlab.Options{})
	login := env.login(t, sample.TenantA, sample.UsernameAlice, sample.PasswordAlice)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authlab.ClaimsFromContext(r.Context())
		if !ok || claims.TenantID != sample.TenantA {
			t.Fatalf("claims unavailable in request context")
		}
		w.WriteHeader(http.StatusNoContent)
	})
	handler := env.engine.AuthenticationMiddleware(next)

	okRequest := httptest.NewRequest(http.MethodGet, "/session", nil)
	okRequest.Header.Set("Authorization", "Bearer "+login.AccessToken)
	okRequest.Header.Set(authlab.TenantHeader, sample.TenantA)
	okResponse := httptest.NewRecorder()
	handler.ServeHTTP(okResponse, okRequest)
	if okResponse.Code != http.StatusNoContent {
		t.Fatalf("same tenant status = %d", okResponse.Code)
	}

	crossRequest := httptest.NewRequest(http.MethodGet, "/session", nil)
	crossRequest.Header.Set("Authorization", "Bearer "+login.AccessToken)
	crossRequest.Header.Set(authlab.TenantHeader, sample.TenantB)
	crossResponse := httptest.NewRecorder()
	handler.ServeHTTP(crossResponse, crossRequest)
	if crossResponse.Code != http.StatusForbidden {
		t.Fatalf("cross tenant status = %d, want 403", crossResponse.Code)
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(crossResponse.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Error.Code != string(authlab.CodeCrossTenant) {
		t.Fatalf("cross tenant body code = %q", body.Error.Code)
	}
}

func TestBusinessErrorDoesNotExposeDetails(t *testing.T) {
	err := authlab.ErrNotFound
	if errors.Is(err, context.Canceled) {
		t.Fatal("unexpected error chain")
	}
	if authlab.CodeOf(err) != authlab.CodeNotFound {
		t.Fatalf("ErrNotFound code = %s", authlab.CodeOf(err))
	}
}
