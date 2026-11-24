package walgroup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// TransactionState is persisted in SQLite.
type TransactionState string

const (
	TxnAppending TransactionState = "APPENDING"
	TxnGrouped   TransactionState = "GROUPED"
	TxnDurable   TransactionState = "DURABLE"
	TxnAborted   TransactionState = "ABORTED"
)

// SegmentState is persisted in SQLite. The only legal forward paths are
// OPEN -> SEALED -> RECLAIMABLE -> RECLAIMED.
type SegmentState string

const (
	SegmentOpen        SegmentState = "OPEN"
	SegmentSealed      SegmentState = "SEALED"
	SegmentReclaimable SegmentState = "RECLAIMABLE"
	SegmentReclaimed   SegmentState = "RECLAIMED"
)

var txnTransitions = map[TransactionState]map[TransactionState]bool{
	TxnAppending: {TxnGrouped: true, TxnAborted: true},
	TxnGrouped:   {TxnDurable: true, TxnAborted: true},
	TxnDurable:   {},
	TxnAborted:   {},
}

var segmentTransitions = map[SegmentState]map[SegmentState]bool{
	SegmentOpen:        {SegmentSealed: true},
	SegmentSealed:      {SegmentReclaimable: true},
	SegmentReclaimable: {SegmentReclaimed: true},
	SegmentReclaimed:   {},
}

// CanTransitionTxn reports whether from -> to is in the declared state machine.
func CanTransitionTxn(from, to TransactionState) bool {
	return txnTransitions[from][to]
}

// CanTransitionSegment reports whether from -> to is in the declared state machine.
func CanTransitionSegment(from, to SegmentState) bool {
	return segmentTransitions[from][to]
}

// PageUpdate is one post-image in a transaction. Version may be zero, in which
// case the engine allocates the next version for the page. Data and Value are
// alternative ways to provide the same bytes; if both are set they must match.
type PageUpdate struct {
	PageNo  int64  `json:"page_no"`
	Version uint64 `json:"version,omitempty"`
	Data    []byte `json:"data,omitempty"`
	Value   string `json:"value,omitempty"`
}

func (u PageUpdate) bytes() ([]byte, error) {
	if len(u.Data) != 0 && u.Value != "" {
		if string(u.Data) != u.Value {
			return nil, newError(CodeInvalidArgument, "page %d supplies both data and a different value", u.PageNo)
		}
	}
	if len(u.Data) != 0 {
		return append([]byte(nil), u.Data...), nil
	}
	if u.Value != "" {
		return []byte(u.Value), nil
	}
	// An empty byte string is a valid deterministic page value.
	return []byte{}, nil
}

// TransactionInput is a complete, self-contained transaction submission.
type TransactionInput struct {
	ID      string       `json:"id"`
	Records []PageUpdate `json:"records"`
}

func (in TransactionInput) validate() error {
	if strings.TrimSpace(in.ID) == "" {
		return newError(CodeInvalidArgument, "transaction id must not be empty")
	}
	if len(in.Records) == 0 {
		return newError(CodeInvalidArgument, "transaction %q has no records", in.ID)
	}
	for i, r := range in.Records {
		if r.PageNo < 0 {
			return newError(CodeInvalidArgument, "transaction %q record %d has negative page number", in.ID, i)
		}
		if _, err := r.bytes(); err != nil {
			return err
		}
	}
	return nil
}

// Fingerprint identifies transaction content independently of engine-assigned
// page versions. It is also persisted for duplicate/conflict detection.
func (in TransactionInput) Fingerprint() (string, error) {
	h := sha256.New()
	h.Write([]byte("txn-v1\x00"))
	h.Write([]byte(in.ID))
	h.Write([]byte{0})
	for _, r := range in.Records {
		data, err := r.bytes()
		if err != nil {
			return "", err
		}
		var fixed [24]byte
		putU64(fixed[0:8], uint64(r.PageNo))
		putU64(fixed[8:16], r.Version)
		putU32(fixed[16:20], uint32(len(data)))
		putU32(fixed[20:24], uint32(crc32c(data)))
		h.Write(fixed[:])
		h.Write(data)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Transaction is the persisted transaction view returned to callers.
type Transaction struct {
	ID          string           `json:"id"`
	Fingerprint string           `json:"fingerprint"`
	State       TransactionState `json:"state"`
	SegmentNo   uint64           `json:"segment_no,omitempty"`
	StartLSN    uint64           `json:"start_lsn,omitempty"`
	EndLSN      uint64           `json:"end_lsn,omitempty"`
	StartOffset int64            `json:"start_offset,omitempty"`
	EndOffset   int64            `json:"end_offset,omitempty"`
	RecordCount int              `json:"record_count,omitempty"`
}

// CommitResult is one transaction's result within a group commit.
type CommitResult struct {
	Transaction
	Duplicate bool `json:"duplicate"`
}

// GroupResult reports the single fsync barrier and all per-transaction spans.
type GroupResult struct {
	Transactions []CommitResult `json:"transactions"`
	StartLSN     uint64         `json:"start_lsn,omitempty"`
	EndLSN       uint64         `json:"end_lsn,omitempty"`
	FsyncCount   int            `json:"fsync_count"`
}

// Record is one decoded WAL record. Offsets are byte offsets in the segment.
type Record struct {
	LSN       uint64 `json:"lsn"`
	TxnID     string `json:"txn_id"`
	PageNo    int64  `json:"page_no"`
	Version   uint64 `json:"version"`
	Digest    string `json:"digest"`
	Data      []byte `json:"data,omitempty"`
	Offset    int64  `json:"offset"`
	Length    int    `json:"length"`
	Checksum  uint32 `json:"checksum"`
	SegmentNo uint64 `json:"segment_no"`
	Ordinal   int    `json:"ordinal"`
}

// SegmentHeader is the fixed on-disk header. PrevDigest is the SHA-256 digest
// of the preceding segment when this segment was opened.
type SegmentHeader struct {
	Magic          string `json:"magic"`
	Version        uint32 `json:"version"`
	HeaderSize     uint32 `json:"header_size"`
	SegmentNo      uint64 `json:"segment_no"`
	StartLSN       uint64 `json:"start_lsn"`
	PrevDigest     string `json:"prev_digest"`
	RecordCount    uint64 `json:"record_count"`
	TxnCount       uint64 `json:"txn_count"`
	LastLSN        uint64 `json:"last_lsn"`
	Flags          uint32 `json:"flags"`
	HeaderChecksum uint32 `json:"header_checksum"`
}

// SegmentInfo is the SQLite segment-catalog view.
type SegmentInfo struct {
	SegmentNo   uint64       `json:"segment_no"`
	FileName    string       `json:"file_name"`
	StartLSN    uint64       `json:"start_lsn"`
	LastLSN     uint64       `json:"last_lsn"`
	PrevDigest  string       `json:"prev_digest"`
	RecordCount uint64       `json:"record_count"`
	TxnCount    uint64       `json:"txn_count"`
	ByteSize    int64        `json:"byte_size"`
	State       SegmentState `json:"state"`
	Digest      string       `json:"digest"`
}

// ParsedSegment is the strict result of parsing a segment file.
type ParsedSegment struct {
	Header  SegmentHeader `json:"header"`
	Records []Record      `json:"records"`
	Digest  string        `json:"digest"`
}

// PageVersionRef is one page version in a checkpoint metadata set.
type PageVersionRef struct {
	PageNo  int64  `json:"page_no"`
	Version uint64 `json:"version"`
	LSN     uint64 `json:"lsn"`
	Digest  string `json:"digest"`
}

// Checkpoint is the atomically active replay base.
type Checkpoint struct {
	Sequence       uint64           `json:"sequence"`
	ReplayStartLSN uint64           `json:"replay_start_lsn"`
	Pages          []PageVersionRef `json:"pages"`
	PagesDigest    string           `json:"pages_digest"`
	Digest         string           `json:"digest"`
	Active         bool             `json:"active"`
}

// CheckpointRequest asks the engine to derive a checkpoint from durable page
// versions at ReplayStartLSN. ExpectedPages/ExpectedDigest, when supplied, must
// match exactly or E_CHECKPOINT_INVALID is returned.
type CheckpointRequest struct {
	Sequence       uint64           `json:"sequence"`
	ReplayStartLSN uint64           `json:"replay_start_lsn"`
	ExpectedPages  []PageVersionRef `json:"expected_pages,omitempty"`
	ExpectedDigest string           `json:"expected_digest,omitempty"`
}

// ReclaimRequest requests physical reclaim of named segments. An empty list
// means all currently eligible sealed segments.
type ReclaimRequest struct {
	SegmentNos []uint64 `json:"segment_nos,omitempty"`
}

// ReclaimResult lists state transitions performed by a reclaim call.
type ReclaimResult struct {
	Segments []SegmentInfo `json:"segments"`
}

// PageResult is a read satisfying snapshot/checkpoint rules.
type PageResult struct {
	PageNo            int64  `json:"page_no"`
	Version           uint64 `json:"version"`
	LSN               uint64 `json:"lsn"`
	Digest            string `json:"digest"`
	Data              []byte `json:"data,omitempty"`
	Value             string `json:"value,omitempty"`
	SnapshotLSN       uint64 `json:"snapshot_lsn"`
	CheckpointSeq     uint64 `json:"checkpoint_sequence,omitempty"`
	MinServiceableLSN uint64 `json:"min_serviceable_lsn"`
}

// ReplayPage is one page after deterministic redo from the active checkpoint.
type ReplayPage struct {
	PageNo  int64  `json:"page_no"`
	Version uint64 `json:"version"`
	LSN     uint64 `json:"lsn"`
	Digest  string `json:"digest"`
	Data    []byte `json:"data,omitempty"`
}

// ReplayResult is the deterministic replay from the checkpoint start.
type ReplayResult struct {
	CheckpointSequence uint64       `json:"checkpoint_sequence"`
	ReplayStartLSN     uint64       `json:"replay_start_lsn"`
	LastReplayedLSN    uint64       `json:"last_replayed_lsn"`
	RecordsReplayed    int          `json:"records_replayed"`
	Pages              []ReplayPage `json:"pages"`
}

// IntentReport is one persisted intent's recovery disposition.
type IntentReport struct {
	IntentID   int64    `json:"intent_id"`
	Kind       string   `json:"kind"`
	PriorState string   `json:"prior_state"`
	Action     string   `json:"action"`
	TxnIDs     []string `json:"txn_ids,omitempty"`
	SegmentNos []uint64 `json:"segment_nos,omitempty"`
}

// SegmentDisposition explains what recovery did with a segment.
type SegmentDisposition struct {
	SegmentNo   uint64       `json:"segment_no"`
	Action      string       `json:"action"`
	State       SegmentState `json:"state"`
	StartLSN    uint64       `json:"start_lsn"`
	LastLSN     uint64       `json:"last_lsn"`
	RecordCount uint64       `json:"record_count"`
	Reason      string       `json:"reason,omitempty"`
}

// RecoveryReport is derived only from persisted segments, SQLite metadata and
// intents. It intentionally contains no speculative in-memory state.
type RecoveryReport struct {
	Status                   string               `json:"status"`
	FactsOnly                bool                 `json:"facts_only"`
	Intents                  []IntentReport       `json:"intents"`
	AbortedTxnIDs            []string             `json:"aborted_txn_ids"`
	Segments                 []SegmentDisposition `json:"segments"`
	NextLSN                  uint64               `json:"next_lsn"`
	MinServiceableLSN        uint64               `json:"min_serviceable_lsn"`
	ActiveCheckpointSequence uint64               `json:"active_checkpoint_sequence,omitempty"`
}

// Options controls segment limits and provides deterministic fault hooks used
// by tests and by the bundled crash demonstrator.
type Options struct {
	SegmentMaxBytes   int64
	MaxTxnsPerSegment int

	// Fault is called before mutating a resource. Returning an error aborts the
	// operation through its normal rollback path.
	Fault func(FaultPoint) error
	// CrashPoint is called at a durable-ordering boundary. A process may be
	// killed while it is blocked here; the bundled demo does exactly that.
	CrashPoint func(point string, details map[string]any)
	// TestSyncCounter, when non-nil, receives one call for each successful file
	// fsync. It is useful for asserting group-commit barriers.
	TestSyncCounter func(segmentNo uint64)
}

// FaultPoint identifies an injectable pre-mutation failure.
type FaultPoint struct {
	Point     string
	SegmentNo uint64
	TxnIDs    []string
}

const (
	FaultBeforeRotate        = "rotate.before_create"
	FaultBeforeGroupWrite    = "group.before_file_write"
	FaultBeforeGroupSync     = "group.before_fsync"
	FaultBeforeGroupMetadata = "group.before_metadata_commit"
	FaultBeforeCheckpoint    = "checkpoint.before_metadata_commit"
	FaultBeforeReclaimDelete = "reclaim.before_delete"

	CrashAfterGroupSync       = "group.after_sync"
	CrashBeforeCheckpointMeta = "checkpoint.before_metadata_commit"
)

func defaultOptions(opts Options) Options {
	if opts.SegmentMaxBytes <= 0 {
		opts.SegmentMaxBytes = 1 << 20
	}
	if opts.MaxTxnsPerSegment <= 0 {
		opts.MaxTxnsPerSegment = 128
	}
	return opts
}

// CheckpointDigest computes the canonical overall checkpoint digest. Pages are
// sorted by page number before hashing.
func CheckpointDigest(sequence, replayStart uint64, pages []PageVersionRef) (pagesDigest, overallDigest string) {
	refs := append([]PageVersionRef(nil), pages...)
	sort.Slice(refs, func(i, j int) bool { return refs[i].PageNo < refs[j].PageNo })
	canonical, _ := json.Marshal(refs)
	pd := sha256.Sum256(canonical)
	pagesDigest = hex.EncodeToString(pd[:])
	sum := sha256.Sum256([]byte(fmt.Sprintf("walgroup-checkpoint-v1\x00%d\x00%d\x00%s", sequence, replayStart, pagesDigest)))
	return pagesDigest, hex.EncodeToString(sum[:])
}

func canonicalPageRefs(pages []PageVersionRef) []PageVersionRef {
	out := append([]PageVersionRef(nil), pages...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].PageNo == out[j].PageNo {
			return out[i].Version < out[j].Version
		}
		return out[i].PageNo < out[j].PageNo
	})
	return out
}
