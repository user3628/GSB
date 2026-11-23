package authquota

import (
	"context"
	"sync"
	"time"
)

const (
	DefaultAuthCodeTTL     = 5 * time.Minute
	DefaultAccessTokenTTL  = 15 * time.Minute
	DefaultRefreshTokenTTL = 30 * 24 * time.Hour
	DefaultMaxInFlight     = 4
	DefaultMaxQueue        = 16
	DefaultQueueTimeout    = 250 * time.Millisecond
)

type RecoveryStrategy string

const (
	RecoveryStrict     RecoveryStrategy = "strict"
	RecoveryConsistent RecoveryStrategy = "consistent"
)

type FaultMode string

const (
	FaultNone              FaultMode = "none"
	FaultAuthCodeExpired   FaultMode = "auth_code_expired"
	FaultPKCEMismatch      FaultMode = "pkce_mismatch"
	FaultRefreshReplay     FaultMode = "refresh_token_replay"
	FaultPrecisionOverflow FaultMode = "precision_overflow"
	FaultSnapshotJump      FaultMode = "snapshot_version_jump"
	FaultQuotaExhausted    FaultMode = "quota_exhausted"
)

// Hooks are optional integration/test seams. They are called at deterministic
// points and must not modify the database themselves.
type Hooks struct {
	BeforeBilling func(ctx context.Context, tenantID, requestID string) error
	BeforeCommit  func(ctx context.Context, tenantID, requestID string) error
	CrashPoint    func(ctx context.Context, point string)
}

type Config struct {
	DBPath           string
	RecoveryStrategy RecoveryStrategy
	MaxInFlight      int
	MaxQueue         int
	QueueTimeout     time.Duration
	AuthCodeTTL      time.Duration
	AccessTokenTTL   time.Duration
	RefreshTokenTTL  time.Duration
	TokenSecret      []byte
	Fault            FaultMode
	Clock            func() time.Time
	Hooks            Hooks
}

func normalizeConfig(cfg Config) Config {
	if cfg.RecoveryStrategy == "" {
		cfg.RecoveryStrategy = RecoveryStrict
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = DefaultMaxInFlight
	}
	if cfg.MaxQueue < 0 {
		cfg.MaxQueue = 0
	}
	if cfg.QueueTimeout <= 0 {
		cfg.QueueTimeout = DefaultQueueTimeout
	}
	if cfg.AuthCodeTTL <= 0 {
		cfg.AuthCodeTTL = DefaultAuthCodeTTL
	}
	if cfg.AccessTokenTTL <= 0 {
		cfg.AccessTokenTTL = DefaultAccessTokenTTL
	}
	if cfg.RefreshTokenTTL <= 0 {
		cfg.RefreshTokenTTL = DefaultRefreshTokenTTL
	}
	if cfg.Fault == "" {
		cfg.Fault = FaultNone
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if len(cfg.TokenSecret) == 0 {
		cfg.TokenSecret = []byte("local-deterministic-openplatform-secret-v1")
	}
	return cfg
}

// Gate serializes work per tenant and limits billing transactions in flight.
type Gate struct {
	slots    chan struct{}
	timeout  time.Duration
	maxQueue int

	mu    sync.Mutex
	lanes map[string]*tenantLane
}

type tenantLane struct {
	sem     chan struct{}
	mu      sync.Mutex
	waiting int
}

func NewGate(maxInFlight, maxQueue int, timeout time.Duration) *Gate {
	if maxInFlight <= 0 {
		maxInFlight = DefaultMaxInFlight
	}
	if maxQueue < 0 {
		maxQueue = 0
	}
	if timeout <= 0 {
		timeout = DefaultQueueTimeout
	}
	return &Gate{
		slots:    make(chan struct{}, maxInFlight),
		timeout:  timeout,
		maxQueue: maxQueue,
		lanes:    make(map[string]*tenantLane),
	}
}

func (g *Gate) lane(tenantID string) *tenantLane {
	g.mu.Lock()
	defer g.mu.Unlock()
	lane := g.lanes[tenantID]
	if lane == nil {
		lane = &tenantLane{sem: make(chan struct{}, 1)}
		lane.sem <- struct{}{}
		g.lanes[tenantID] = lane
	}
	return lane
}

// Acquire reserves a global in-flight slot and then the tenant's exclusive
// lane. Waiting more than QueueTimeout returns E_QUOTA_CONTENTION.
func (g *Gate) Acquire(ctx context.Context, tenantID string) error {
	timer := time.NewTimer(g.timeout)
	defer timer.Stop()
	select {
	case g.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return bizError(CodeQuotaContention, "global billing transaction queue exceeded threshold", nil)
	}

	lane := g.lane(tenantID)
	lane.mu.Lock()
	select {
	case <-lane.sem:
		lane.mu.Unlock()
		return nil
	default:
		if lane.waiting >= g.maxQueue {
			lane.mu.Unlock()
			<-g.slots
			return bizError(CodeQuotaContention, "tenant billing queue exceeded threshold", nil)
		}
		lane.waiting++
		lane.mu.Unlock()
	}

	select {
	case <-lane.sem:
		lane.mu.Lock()
		lane.waiting--
		lane.mu.Unlock()
		return nil
	case <-ctx.Done():
		g.cancelLaneWait(lane)
		<-g.slots
		return ctx.Err()
	case <-timer.C:
		// Resolve a simultaneous wake-up deterministically in favour of the
		// available lane so a completed operation is never aged out.
		select {
		case <-lane.sem:
			lane.mu.Lock()
			lane.waiting--
			lane.mu.Unlock()
			return nil
		default:
			g.cancelLaneWait(lane)
			<-g.slots
			return bizError(CodeQuotaContention, "tenant billing queue wait exceeded threshold", nil)
		}
	}
}

func (g *Gate) cancelLaneWait(lane *tenantLane) {
	lane.mu.Lock()
	lane.waiting--
	lane.mu.Unlock()
}

func (g *Gate) Release(tenantID string) {
	lane := g.lane(tenantID)
	lane.sem <- struct{}{}
	<-g.slots
}
