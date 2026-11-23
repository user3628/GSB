package store

import "time"

// LeaseState enumerates the lifecycle of a row write lease.
type LeaseState string

const (
	LeaseGranted  LeaseState = "granted"  // currently held until expires_at
	LeaseExpired  LeaseState = "expired"  // TTL elapsed without renewal
	LeaseReleased LeaseState = "released" // explicitly released or consumed by a commit
	LeaseStolen   LeaseState = "stolen"   // superseded by a higher lease_version
)

// IntentKind enumerates the mutating operations that require a write-ahead
// intent record before their transaction commits.
type IntentKind string

const (
	KindAcquire IntentKind = "acquire"
	KindRenew   IntentKind = "renew"
	KindCommit  IntentKind = "commit"
	KindRelease IntentKind = "release"
	KindExpire  IntentKind = "expire"
	KindRecycle IntentKind = "recycle"
)

// IntentState is the lifecycle of an intent record.
type IntentState string

const (
	IntentPrepared   IntentState = "prepared"    // written, applying transaction not confirmed
	IntentCompleted  IntentState = "completed"   // applying transaction committed
	IntentRolledBack IntentState = "rolled_back" // reverted by crash recovery
)

// WriteIntent is the durable intent row persisted ahead of every mutation.
type WriteIntent struct {
	ID            int64
	Kind          IntentKind
	RowID         string
	RequestID     string
	LeaseVersion  int64
	BaseRowVer    int64
	Payload       string
	State         IntentState
	CreatedAtMs   int64
	CompletedAtMs int64
}

// RowVersion is one immutable version of a business row.
type RowVersion struct {
	RowID       string
	RowVersion  int64
	CommitVer   int64
	ContentHash string
	Content     string
	Deleted     bool
	CreatedAtMs int64
	RetiredAtMs int64
	IntentID    int64
}

// Lease is the durable row write-lease record.
type Lease struct {
	RowID        string
	LeaseVersion int64
	Holder       string
	State        LeaseState
	GrantedAtMs  int64
	ExpiresAtMs  int64
	RenewedAtMs  int64
}

// ActiveSnapshot tracks a reader snapshot that may still need old versions.
type ActiveSnapshot struct {
	SnapshotVer int64
	Holder      string
	OpenedAtMs  int64
	Orphaned    bool
}

// RequestRecord implements idempotent request deduplication.
type RequestRecord struct {
	RequestID     string
	Kind          string
	RequestKey    string
	PayloadHash   string
	Status        string
	ResponseJSON  string
	CreatedAtMs   int64
	CompletedAtMs int64
}

// RecoveryEvent is one fact-based action taken during crash recovery.
type RecoveryEvent struct {
	IntentID  int64
	Kind      IntentKind
	RowID     string
	Action    string
	Detail    string
	CreatedMs int64
}

// Milli converts a time to UTC milliseconds.
func Milli(t time.Time) int64 { return t.UTC().UnixMilli() }
