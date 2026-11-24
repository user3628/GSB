package authlab

import (
	"context"
	"errors"
	"sync"
	"time"
)

// OperationKind is exposed to transaction observers for metrics and local
// deterministic contention tests.
type OperationKind string

const (
	OperationLogin         OperationKind = "login"
	OperationRefresh       OperationKind = "refresh"
	OperationValidate      OperationKind = "validate"
	OperationRevoke        OperationKind = "revoke"
	OperationAuthorization OperationKind = "authorization_update"
)

// Operation describes an authentication transaction. TenantID is populated for
// login and authorization updates; token-driven operations learn the signed
// tenant only after the bearer token is parsed.
type Operation struct {
	Kind     OperationKind
	TenantID string
}

// TransactionHook runs after an in-flight slot is acquired but before tenant
// state is read or changed. It is intended for observation. It must not call
// back into the same Engine operation.
type TransactionHook func(ctx context.Context, operation Operation) error

// Options configures the engine. Defaults are applied by NewEngine.
type Options struct {
	Clock           Clock
	IDs             IDSource
	AccessTTL       time.Duration
	RefreshTTL      time.Duration
	MaxInFlight     int
	ContentionWait  time.Duration
	TransactionHook TransactionHook
}

// Engine is a single-process multi-tenant authentication/session engine.
type Engine struct {
	store          SessionStore
	codec          *TokenCodec
	clock          Clock
	ids            IDSource
	accessTTL      time.Duration
	refreshTTL     time.Duration
	contentionWait time.Duration
	permits        chan struct{}
	hook           TransactionHook

	gatesMu sync.Mutex
	gates   map[string]chan struct{}
}

func NewEngine(store SessionStore, fixture Fixture, options Options) (*Engine, error) {
	if store == nil {
		return nil, newError(CodeInternal, "new_engine", "store is nil")
	}
	if err := ValidateFixture(fixture); err != nil {
		return nil, err
	}
	if options.Clock == nil {
		options.Clock = SystemClock{}
	}
	if options.IDs == nil {
		options.IDs = randomIDSource{}
	}
	if options.AccessTTL <= 0 {
		options.AccessTTL = 5 * time.Minute
	}
	if options.RefreshTTL <= 0 {
		options.RefreshTTL = time.Hour
	}
	if options.MaxInFlight <= 0 {
		options.MaxInFlight = 32
	}
	if options.ContentionWait <= 0 {
		options.ContentionWait = 250 * time.Millisecond
	}
	return &Engine{
		store:          store,
		codec:          NewTokenCodec(fixture.SigningKey),
		clock:          options.Clock,
		ids:            options.IDs,
		accessTTL:      options.AccessTTL,
		refreshTTL:     options.RefreshTTL,
		contentionWait: options.ContentionWait,
		permits:        make(chan struct{}, options.MaxInFlight),
		hook:           options.TransactionHook,
		gates:          make(map[string]chan struct{}),
	}, nil
}

// OpenSQLiteEngine creates and initializes a local pure-Go SQLite environment.
func OpenSQLiteEngine(ctx context.Context, path string, fixture Fixture, options Options) (*Engine, error) {
	now := time.Now
	if options.Clock != nil {
		now = options.Clock.Now
	}
	store, err := OpenSQLiteStore(ctx, path, now)
	if err != nil {
		return nil, err
	}
	if err := store.Init(ctx, fixture); err != nil {
		_ = store.Close()
		return nil, err
	}
	engine, err := NewEngine(store, fixture, options)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return engine, nil
}

func (e *Engine) Close() error { return e.store.Close() }

func (e *Engine) acquirePermit(ctx context.Context) (func(), error) {
	if err := waitFor(ctx, e.permits, e.contentionWait); err != nil {
		return nil, newError(CodeAuthContention, "acquire_transaction", "in-flight limit")
	}
	return func() { <-e.permits }, nil
}

func (e *Engine) tenantGate(tenantID string) chan struct{} {
	e.gatesMu.Lock()
	defer e.gatesMu.Unlock()
	gate, ok := e.gates[tenantID]
	if !ok {
		gate = make(chan struct{}, 1)
		e.gates[tenantID] = gate
	}
	return gate
}

func (e *Engine) acquireTenant(ctx context.Context, tenantID string) (func(), error) {
	gate := e.tenantGate(tenantID)
	if err := waitFor(ctx, gate, e.contentionWait); err != nil {
		return nil, newError(CodeAuthContention, "acquire_tenant", "tenant serialization timeout")
	}
	return func() { <-gate }, nil
}

func waitFor(ctx context.Context, gate chan struct{}, wait time.Duration) error {
	select {
	case gate <- struct{}{}:
		return nil
	default:
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return context.DeadlineExceeded
	}
}

func (e *Engine) beginTransaction(ctx context.Context, operation Operation) (func(), error) {
	releasePermit, err := e.acquirePermit(ctx)
	if err != nil {
		return nil, err
	}
	release := releasePermit
	if e.hook != nil {
		if err := e.hook(ctx, operation); err != nil {
			release()
			return nil, err
		}
	}
	return release, nil
}

// LoginRequest authenticates against the local fixture. PriorSessionID models a
// pre-authentication identifier supplied by a client. The engine refuses it
// rather than allowing a login to preserve a pre-auth session identifier.
type LoginRequest struct {
	TenantID        string
	Username        string
	Password        string
	PriorSessionID  string
	ClientSessionID string
	AccessTTL       time.Duration
	RefreshTTL      time.Duration
}

type LoginResult struct {
	TenantID         string    `json:"tenant_id"`
	UserID           string    `json:"user_id"`
	SessionID        string    `json:"session_id"`
	AccessToken      string    `json:"access_token"`
	RefreshToken     string    `json:"refresh_token"`
	AccessExpiresAt  time.Time `json:"access_expires_at"`
	RefreshExpiresAt time.Time `json:"refresh_expires_at"`
	SnapshotVersion  int64     `json:"snapshot_version"`
}

func (e *Engine) Login(ctx context.Context, request LoginRequest) (*LoginResult, error) {
	if request.TenantID == "" || request.Username == "" || request.Password == "" {
		return nil, newError(CodeAuthInvalid, "login", "invalid credentials")
	}
	release, err := e.beginTransaction(ctx, Operation{Kind: OperationLogin, TenantID: request.TenantID})
	if err != nil {
		return nil, err
	}
	defer release()
	releaseTenant, err := e.acquireTenant(ctx, request.TenantID)
	if err != nil {
		return nil, err
	}
	defer releaseTenant()

	user, err := e.store.FindUserByUsername(ctx, request.TenantID, request.Username)
	if err != nil {
		if errors.Is(err, ErrNotFound) || IsCode(err, CodeNotFound) {
			dummyPasswordWork(request.Password)
			return nil, newError(CodeAuthInvalid, "login", "invalid credentials")
		}
		return nil, err
	}
	if !verifyPassword(request.Password, user) {
		return nil, newError(CodeAuthInvalid, "login", "invalid credentials")
	}
	clientSessionID := request.PriorSessionID
	if clientSessionID == "" {
		clientSessionID = request.ClientSessionID
	}
	if clientSessionID != "" {
		return nil, newError(CodeSessionFixation, "login", "client supplied session id")
	}

	now := e.clock.Now().UTC()
	accessTTL := request.AccessTTL
	if accessTTL <= 0 {
		accessTTL = e.accessTTL
	}
	refreshTTL := request.RefreshTTL
	if refreshTTL <= 0 {
		refreshTTL = e.refreshTTL
	}
	accessExpiry := now.Add(accessTTL)
	refreshExpiry := now.Add(refreshTTL)

	sessionID := e.ids.Next("sess")
	familyID := e.ids.Next("fam")
	refreshID := e.ids.Next("rt")
	accessID := e.ids.Next("at")
	if sessionID == clientSessionID {
		return nil, newError(CodeSessionFixation, "login", "session id was not rotated")
	}
	accessToken, err := e.codec.Sign(Claims{
		TokenID: accessID, Type: AccessToken, TenantID: user.TenantID, UserID: user.UserID,
		SessionID: sessionID, FamilyID: familyID, Roles: user.Roles, Permissions: user.Permissions,
		IssuedAt: now.Unix(), ExpiresAt: accessExpiry.Unix(),
	})
	if err != nil {
		return nil, err
	}
	refreshToken, err := e.codec.Sign(Claims{
		TokenID: refreshID, Type: RefreshToken, TenantID: user.TenantID, UserID: user.UserID,
		SessionID: sessionID, FamilyID: familyID, Serial: 1, Roles: user.Roles, Permissions: user.Permissions,
		IssuedAt: now.Unix(), ExpiresAt: refreshExpiry.Unix(),
	})
	if err != nil {
		return nil, err
	}

	snapshot, _, err := e.store.MutateSnapshot(ctx, user.TenantID, func(state *TenantState) (bool, error) {
		state.Sessions = append(state.Sessions, SessionRecord{
			SessionID: sessionID, UserID: user.UserID, FamilyID: familyID,
			Status: SessionActive, CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
		})
		state.Families = append(state.Families, TokenFamily{
			FamilyID: familyID, SessionID: sessionID, UserID: user.UserID,
			CurrentSerial: 1, Status: FamilyActive, CreatedAt: now.Unix(),
		})
		state.RefreshTokens = append(state.RefreshTokens, RefreshTokenRecord{
			TokenID: refreshID, FamilyID: familyID, SessionID: sessionID, UserID: user.UserID,
			Serial: 1, Status: RefreshActive, ExpiresAt: refreshExpiry.Unix(),
		})
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return &LoginResult{
		TenantID: user.TenantID, UserID: user.UserID, SessionID: sessionID,
		AccessToken: accessToken, RefreshToken: refreshToken,
		AccessExpiresAt: accessExpiry, RefreshExpiresAt: refreshExpiry,
		SnapshotVersion: snapshot.Version,
	}, nil
}

type RotationResult struct {
	TenantID         string    `json:"tenant_id"`
	UserID           string    `json:"user_id"`
	SessionID        string    `json:"session_id"`
	AccessToken      string    `json:"access_token"`
	RefreshToken     string    `json:"refresh_token"`
	AccessExpiresAt  time.Time `json:"access_expires_at"`
	RefreshExpiresAt time.Time `json:"refresh_expires_at"`
	SnapshotVersion  int64     `json:"snapshot_version"`
}

func (e *Engine) Refresh(ctx context.Context, refreshToken string) (*RotationResult, error) {
	return e.refresh(ctx, "", refreshToken)
}

// RefreshForTenant enforces a caller-provided resource tenant after parsing and
// verifying the signed token tenant. State lookup still uses only the signed
// tenant, never the caller parameter.
func (e *Engine) RefreshForTenant(ctx context.Context, expectedTenant, refreshToken string) (*RotationResult, error) {
	return e.refresh(ctx, expectedTenant, refreshToken)
}

func (e *Engine) refresh(ctx context.Context, expectedTenant, refreshToken string) (*RotationResult, error) {
	release, err := e.beginTransaction(ctx, Operation{Kind: OperationRefresh})
	if err != nil {
		return nil, err
	}
	defer release()
	claims, err := e.codec.Parse(refreshToken, e.clock.Now().UTC())
	if err != nil {
		return nil, err
	}
	if claims.Type != RefreshToken || claims.FamilyID == "" || claims.Serial < 1 {
		return nil, newError(CodeTokenInvalid, "refresh", "not a refresh token")
	}
	if expectedTenant != "" && claims.TenantID != expectedTenant {
		return nil, newError(CodeCrossTenant, "refresh", "signed tenant does not match endpoint tenant")
	}
	releaseTenant, err := e.acquireTenant(ctx, claims.TenantID)
	if err != nil {
		return nil, err
	}
	defer releaseTenant()

	now := e.clock.Now().UTC()
	snapshot, err := e.store.LoadSnapshot(ctx, claims.TenantID)
	if err != nil {
		return nil, err
	}
	record := snapshot.State.refresh(claims.TokenID)
	if record == nil || record.FamilyID != claims.FamilyID || record.SessionID != claims.SessionID ||
		record.UserID != claims.UserID || record.Serial != claims.Serial {
		return nil, newError(CodeTokenInvalid, "refresh", "refresh token not found")
	}
	family := snapshot.State.family(claims.FamilyID)
	session := snapshot.State.session(claims.SessionID)
	if family == nil || session == nil || family.UserID != claims.UserID || session.UserID != claims.UserID {
		return nil, newError(CodeTokenInvalid, "refresh", "session lineage missing")
	}
	if record.Status != RefreshActive || family.Status != FamilyActive || session.Status != SessionActive {
		if family.Status == FamilyActive || session.Status == SessionActive || record.Status != RefreshRevoked {
			_, _, revokeErr := e.store.MutateSnapshot(ctx, claims.TenantID, func(state *TenantState) (bool, error) {
				changed := revokeFamilyState(state, claims.FamilyID, claims.SessionID, now, "refresh_reuse")
				return changed, nil
			})
			if revokeErr != nil {
				return nil, revokeErr
			}
		}
		return nil, newError(CodeTokenReuse, "refresh", "refresh token was already consumed or revoked")
	}
	if family.CurrentSerial != record.Serial {
		return nil, newError(CodeTokenInvalid, "refresh", "active serial mismatch")
	}
	user, err := e.store.FindUserByID(ctx, claims.TenantID, claims.UserID)
	if err != nil {
		if errors.Is(err, ErrNotFound) || IsCode(err, CodeNotFound) {
			return nil, newError(CodeTokenInvalid, "refresh", "subject unavailable")
		}
		return nil, err
	}

	accessTTL := e.accessTTL
	refreshTTL := e.refreshTTL
	accessExpiry := now.Add(accessTTL)
	refreshExpiry := now.Add(refreshTTL)
	newSerial := record.Serial + 1
	accessID := e.ids.Next("at")
	newRefreshID := e.ids.Next("rt")
	newAccessToken, err := e.codec.Sign(Claims{
		TokenID: accessID, Type: AccessToken, TenantID: user.TenantID, UserID: user.UserID,
		SessionID: claims.SessionID, FamilyID: claims.FamilyID,
		Roles: user.Roles, Permissions: user.Permissions,
		IssuedAt: now.Unix(), ExpiresAt: accessExpiry.Unix(),
	})
	if err != nil {
		return nil, err
	}
	newRefreshToken, err := e.codec.Sign(Claims{
		TokenID: newRefreshID, Type: RefreshToken, TenantID: user.TenantID, UserID: user.UserID,
		SessionID: claims.SessionID, FamilyID: claims.FamilyID, Serial: newSerial,
		Roles: user.Roles, Permissions: user.Permissions,
		IssuedAt: now.Unix(), ExpiresAt: refreshExpiry.Unix(),
	})
	if err != nil {
		return nil, err
	}

	updated, changed, err := e.store.MutateSnapshot(ctx, claims.TenantID, func(state *TenantState) (bool, error) {
		oldRecord := state.refresh(claims.TokenID)
		family := state.family(claims.FamilyID)
		session := state.session(claims.SessionID)
		if oldRecord == nil || family == nil || session == nil || oldRecord.Status != RefreshActive ||
			family.Status != FamilyActive || session.Status != SessionActive || family.CurrentSerial != oldRecord.Serial {
			return false, newError(CodeTokenReuse, "refresh", "refresh state changed concurrently")
		}
		oldRecord.Status = RefreshUsed
		oldRecord.UsedAt = now.Unix()
		family.CurrentSerial = newSerial
		session.UpdatedAt = now.Unix()
		session.LastRotationAt = now.Unix()
		state.RefreshTokens = append(state.RefreshTokens, RefreshTokenRecord{
			TokenID: newRefreshID, FamilyID: claims.FamilyID, SessionID: claims.SessionID,
			UserID: user.UserID, Serial: newSerial, Status: RefreshActive, ExpiresAt: refreshExpiry.Unix(),
		})
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if !changed {
		return nil, newError(CodeInternal, "refresh", "snapshot mutation was not committed")
	}
	return &RotationResult{
		TenantID: user.TenantID, UserID: user.UserID, SessionID: claims.SessionID,
		AccessToken: newAccessToken, RefreshToken: newRefreshToken,
		AccessExpiresAt: accessExpiry, RefreshExpiresAt: refreshExpiry,
		SnapshotVersion: updated.Version,
	}, nil
}

func revokeFamilyState(state *TenantState, familyID, sessionID string, now time.Time, reason string) bool {
	changed := false
	if family := state.family(familyID); family != nil && family.Status != FamilyRevoked {
		family.Status = FamilyRevoked
		family.RevokedAt = now.Unix()
		family.RevokedReason = reason
		changed = true
	}
	if session := state.session(sessionID); session != nil && session.Status != SessionRevoked {
		session.Status = SessionRevoked
		session.UpdatedAt = now.Unix()
		session.RevokedAt = now.Unix()
		session.RevokedReason = reason
		changed = true
	}
	for i := range state.RefreshTokens {
		record := &state.RefreshTokens[i]
		if record.FamilyID != familyID || record.Status == RefreshRevoked {
			continue
		}
		record.Status = RefreshRevoked
		record.RevokedAt = now.Unix()
		changed = true
	}
	return changed
}

func (e *Engine) ValidateAccess(ctx context.Context, accessToken string) (Claims, error) {
	return e.validateAccess(ctx, "", accessToken, false)
}

// ValidateAccessForTenant returns E_CROSS_TENANT when the signed token tenant
// differs from the endpoint tenant. It performs no session-state mutation.
func (e *Engine) ValidateAccessForTenant(ctx context.Context, expectedTenant, accessToken string) (Claims, error) {
	return e.validateAccess(ctx, expectedTenant, accessToken, true)
}

func (e *Engine) validateAccess(ctx context.Context, expectedTenant, accessToken string, enforceTenant bool) (Claims, error) {
	release, err := e.beginTransaction(ctx, Operation{Kind: OperationValidate})
	if err != nil {
		return Claims{}, err
	}
	defer release()
	claims, err := e.codec.Parse(accessToken, e.clock.Now().UTC())
	if err != nil {
		return Claims{}, err
	}
	if claims.Type != AccessToken {
		return Claims{}, newError(CodeTokenInvalid, "validate_access", "not an access token")
	}
	if enforceTenant {
		if expectedTenant == "" || claims.TenantID != expectedTenant {
			return Claims{}, newError(CodeCrossTenant, "validate_access", "signed tenant does not match endpoint tenant")
		}
	}
	snapshot, err := e.store.LoadSnapshot(ctx, claims.TenantID)
	if err != nil {
		return Claims{}, err
	}
	session := snapshot.State.session(claims.SessionID)
	family := snapshot.State.family(claims.FamilyID)
	if session == nil || family == nil || session.Status != SessionActive || family.Status != FamilyActive ||
		session.UserID != claims.UserID || family.UserID != claims.UserID || session.FamilyID != claims.FamilyID {
		return Claims{}, newError(CodeTokenInvalid, "validate_access", "session unavailable")
	}
	user, err := e.store.FindUserByID(ctx, claims.TenantID, claims.UserID)
	if err != nil {
		if errors.Is(err, ErrNotFound) || IsCode(err, CodeNotFound) {
			return Claims{}, newError(CodeTokenInvalid, "validate_access", "subject unavailable")
		}
		return Claims{}, err
	}
	if !equalStringSets(claims.Roles, user.Roles) || !equalStringSets(claims.Permissions, user.Permissions) {
		return Claims{}, newError(CodePrivilegeEscalation, "validate_access", "token authorization differs from current grant")
	}
	return claims, nil
}

// AuthorizeResource checks a resource tenant and permission after authentication.
// A cross-tenant resource request at this authorization layer is a privilege
// escalation, distinct from the session-boundary E_CROSS_TENANT check.
func (e *Engine) AuthorizeResource(ctx context.Context, accessToken, resourceTenant, permission string) (Claims, error) {
	claims, err := e.ValidateAccess(ctx, accessToken)
	if err != nil {
		return Claims{}, err
	}
	if resourceTenant == "" || claims.TenantID != resourceTenant {
		return Claims{}, newError(CodePrivilegeEscalation, "authorize_resource", "cross-tenant resource")
	}
	if permission != "" && !hasString(claims.Permissions, permission) {
		return Claims{}, newError(CodePrivilegeEscalation, "authorize_resource", "permission not granted")
	}
	return claims, nil
}

func (e *Engine) RevokeSession(ctx context.Context, accessToken string) (Snapshot, error) {
	release, err := e.beginTransaction(ctx, Operation{Kind: OperationRevoke})
	if err != nil {
		return Snapshot{}, err
	}
	defer release()
	claims, err := e.codec.Parse(accessToken, e.clock.Now().UTC())
	if err != nil {
		return Snapshot{}, err
	}
	if claims.Type != AccessToken {
		return Snapshot{}, newError(CodeTokenInvalid, "revoke", "not an access token")
	}
	releaseTenant, err := e.acquireTenant(ctx, claims.TenantID)
	if err != nil {
		return Snapshot{}, err
	}
	defer releaseTenant()
	snapshot, err := e.store.LoadSnapshot(ctx, claims.TenantID)
	if err != nil {
		return Snapshot{}, err
	}
	session := snapshot.State.session(claims.SessionID)
	if session == nil || session.UserID != claims.UserID || session.Status != SessionActive {
		return Snapshot{}, newError(CodeTokenInvalid, "revoke", "session unavailable")
	}
	now := e.clock.Now().UTC()
	updated, _, err := e.store.MutateSnapshot(ctx, claims.TenantID, func(state *TenantState) (bool, error) {
		changed := revokeFamilyState(state, claims.FamilyID, claims.SessionID, now, "logout")
		return changed, nil
	})
	if err != nil {
		return Snapshot{}, err
	}
	return updated, nil
}

func (e *Engine) UpdateAuthorization(ctx context.Context, tenantID, userID string, roles, permissions []string) error {
	release, err := e.beginTransaction(ctx, Operation{Kind: OperationAuthorization, TenantID: tenantID})
	if err != nil {
		return err
	}
	defer release()
	releaseTenant, err := e.acquireTenant(ctx, tenantID)
	if err != nil {
		return err
	}
	defer releaseTenant()
	err = e.store.UpdateAuthorization(ctx, tenantID, userID, roles, permissions)
	if err != nil {
		if errors.Is(err, ErrNotFound) || IsCode(err, CodeNotFound) {
			return newError(CodeNotFound, "update_authorization", "user unavailable")
		}
		return err
	}
	return nil
}

func (e *Engine) CurrentSnapshot(ctx context.Context, tenantID string) (Snapshot, error) {
	return e.store.LoadSnapshot(ctx, tenantID)
}

func (e *Engine) SnapshotAt(ctx context.Context, tenantID string, version int64) (Snapshot, error) {
	return e.store.SnapshotAt(ctx, tenantID, version)
}

func (e *Engine) SnapshotHistory(ctx context.Context, tenantID string) ([]SnapshotMeta, error) {
	return e.store.SnapshotHistory(ctx, tenantID)
}

func (e *Engine) AuditSnapshots(ctx context.Context, tenantID string) (SnapshotAudit, error) {
	return e.store.AuditSnapshots(ctx, tenantID)
}

func (e *Engine) RepairSnapshots(ctx context.Context, tenantID string) (Snapshot, bool, error) {
	return e.store.RepairSnapshots(ctx, tenantID)
}

func (e *Engine) FindUser(ctx context.Context, tenantID, userID string) (UserRecord, error) {
	return e.store.FindUserByID(ctx, tenantID, userID)
}
