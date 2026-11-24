package blockpool

// Physical block states. pending_reclaim and reclaimed are distinct so that a
// reader can never observe a zero-retained block as live while its content is
// being removed.
const (
	BlockLive           = "live"
	BlockPendingReclaim = "pending_reclaim"
	BlockReclaimed      = "reclaimed"
)

const (
	LeaseActive    = "active"
	LeaseExpired   = "expired"
	LeaseUnmounted = "unmounted"
)

// Block is the public metadata view of a content-addressed block.
type Block struct {
	Digest      string `json:"digest"`
	SizeBytes   int64  `json:"size_bytes"`
	StoredAtMS  int64  `json:"stored_at_ms"`
	RetentionMS int64  `json:"retention_ms"`
	State       string `json:"state"`
	RefCount    int64  `json:"ref_count"`
}

// Lease is one persisted reference lease. Version is monotonic for the
// (namespace, path, digest) tuple and is never reused.
type Lease struct {
	Namespace   string `json:"namespace"`
	Path        string `json:"path"`
	Digest      string `json:"digest"`
	Version     int64  `json:"version"`
	MountedAtMS int64  `json:"mounted_at_ms"`
	ExpiresAtMS int64  `json:"expires_at_ms"`
	State       string `json:"state"`
}

// BlockDetails includes the block metadata and all currently effective leases.
type BlockDetails struct {
	Block
	ActiveLeaseCount int64   `json:"active_lease_count"`
	Leases           []Lease `json:"leases"`
}

type IngestRequest struct {
	RequestID   string `json:"request_id,omitempty"`
	Digest      string `json:"digest"`
	Data        []byte `json:"-"`
	SizeBytes   *int64 `json:"size_bytes,omitempty"`
	RetentionMS int64  `json:"retention_ms,omitempty"`
}

type MountRequest struct {
	RequestID  string `json:"request_id,omitempty"`
	Namespace  string `json:"namespace"`
	Path       string `json:"path"`
	Digest     string `json:"digest"`
	LeaseTTLMS int64  `json:"lease_ms,omitempty"`
}

type RenewRequest struct {
	RequestID  string `json:"request_id,omitempty"`
	Namespace  string `json:"namespace"`
	Path       string `json:"path"`
	Digest     string `json:"digest"`
	Version    int64  `json:"version"`
	LeaseTTLMS int64  `json:"lease_ms,omitempty"`
}

type UnmountRequest struct {
	RequestID string `json:"request_id,omitempty"`
	Namespace string `json:"namespace"`
	Path      string `json:"path"`
	Digest    string `json:"digest"`
}

type UnmountResult struct {
	Namespace string `json:"namespace"`
	Path      string `json:"path"`
	Digest    string `json:"digest"`
	Removed   bool   `json:"removed"`
	Expired   bool   `json:"expired"`
	RefCount  int64  `json:"ref_count"`
}

type BatchBlock struct {
	Digest      string `json:"digest"`
	Data        []byte `json:"-"`
	SizeBytes   *int64 `json:"size_bytes,omitempty"`
	RetentionMS int64  `json:"retention_ms,omitempty"`
}

// ReplaceRequest atomically replaces the complete desired reference set of one
// logical node.
type ReplaceRequest struct {
	RequestID  string       `json:"request_id,omitempty"`
	Namespace  string       `json:"namespace"`
	Path       string       `json:"path"`
	Blocks     []BatchBlock `json:"blocks"`
	LeaseTTLMS int64        `json:"lease_ms,omitempty"`
}

type ReplaceResult struct {
	Namespace string   `json:"namespace"`
	Path      string   `json:"path"`
	Added     []string `json:"added"`
	Removed   []string `json:"removed"`
	Unchanged []string `json:"unchanged"`
	Leases    []Lease  `json:"leases"`
}

type ReclaimRequest struct {
	RequestID     string `json:"request_id,omitempty"`
	Digest        string `json:"digest,omitempty"`
	CapacityBytes *int64 `json:"capacity_bytes,omitempty"`
}

type ReclaimedBlock struct {
	Digest      string `json:"digest"`
	SizeBytes   int64  `json:"size_bytes"`
	ReclaimedAt int64  `json:"reclaimed_at_ms"`
}

type ReclaimResult struct {
	// Reclaimed is ordered exactly as scheduled: size descending, digest
	// ascending for equal sizes.
	Reclaimed       []ReclaimedBlock `json:"reclaimed"`
	ReleasedBytes   int64            `json:"released_bytes"`
	LiveBytesAfter  int64            `json:"live_bytes_after"`
	BudgetBytes     *int64           `json:"budget_bytes,omitempty"`
	OverBudgetBytes int64            `json:"over_budget_bytes"`
	Blocked         []ReclaimBlocked `json:"blocked,omitempty"`
}

type ReclaimBlocked struct {
	Digest               string   `json:"digest"`
	RefCount             int64    `json:"ref_count"`
	EffectiveLeaseCount  int64    `json:"effective_lease_count"`
	RemainingRetentionMS int64    `json:"remaining_retention_ms"`
	Reasons              []string `json:"reasons"`
}

type RecoveryDiff struct {
	Digest              string `json:"digest"`
	PersistedRefCount   int64  `json:"persisted_ref_count"`
	RebuiltRefCount     int64  `json:"rebuilt_ref_count"`
	EffectiveLeaseCount int64  `json:"effective_lease_count"`
	Detail              string `json:"detail"`
}

// RecoveryReport is produced only at a successful Pool.Open. A process kill
// cannot produce an exit report; the next opener produces this report from
// committed database facts and pending intent rows.
type RecoveryReport struct {
	ID                int64          `json:"id"`
	CreatedAtMS       int64          `json:"created_at_ms"`
	PendingIntents    int64          `json:"pending_intents"`
	AbortedIntents    int64          `json:"aborted_intents"`
	RecoveredRequests int64          `json:"recovered_requests"`
	DiffCount         int64          `json:"diff_count"`
	Diffs             []RecoveryDiff `json:"diffs"`
	Note              string         `json:"note"`
}

// NodeRef identifies a logical node, which is namespace plus path.
type NodeRef struct {
	Namespace string `json:"namespace"`
	Path      string `json:"path"`
}

type Namespace struct {
	Name string `json:"name"`
}
