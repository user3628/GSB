package mvcc

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
)

type Code string

const (
	CodeTxnInvalid          Code = "E_TXN_INVALID"
	CodeVersionInvalid      Code = "E_VERSION_INVALID"
	CodeChainBroken         Code = "E_VERSION_CHAIN_BROKEN"
	CodeSnapshotInvalid     Code = "E_SNAPSHOT_INVALID"
	CodeReclaimUnsafe        Code = "E_RECLAIM_UNSAFE"
	CodeConflictingRequest  Code = "E_CONFLICTING_REQUEST"
	CodeWriteConflict       Code = "E_WRITE_CONFLICT"
	CodeNotFound            Code = "E_NOT_FOUND"
	CodeInterrupted         Code = "E_INTERRUPTED"
	CodeInternal            Code = "E_INTERNAL"
)

type Error struct {
	Code    Code           `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewError(code Code, message string, context map[string]any) *Error {
	return &Error{Code: code, Message: message, Context: context}
}

func AsError(err error) *Error {
	if err == nil {
		return nil
	}
	if me, ok := err.(*Error); ok {
		return me
	}
	return &Error{Code: CodeInternal, Message: err.Error()}
}

type TxnStatus string

const (
	TxnActive    TxnStatus = "ACTIVE"
	TxnCommitted TxnStatus = "COMMITTED"
	TxnAborted   TxnStatus = "ABORTED"
)

type CommitState string

const (
	VersionUncommitted CommitState = "UNCOMMITTED"
	VersionCommitted   CommitState = "COMMITTED"
	VersionAborted     CommitState = "ABORTED"
)

const (
	ReasonVisible                 = "VISIBLE_COMMITTED_IN_SNAPSHOT"
	ReasonSelfUncommitted         = "SELF_UNCOMMITTED"
	ReasonSnapshotOutsideCommit   = "SNAPSHOT_OUTSIDE_COMMIT"
	ReasonRolledBack              = "ROLLED_BACK"
	ReasonDeletedInSnapshot       = "DELETED_IN_SNAPSHOT"
	ReasonNoVisibleVersion        = "NO_VISIBLE_VERSION"
	ReasonNotFound                = "NOT_FOUND"
)

type Snapshot struct {
	SnapshotSeq    int64  `json:"snapshot_seq"`
	GeneratedAtMS  int64  `json:"generated_at_ms"`
	TxnUpperBound  int64  `json:"txn_upper_bound"`
	Checksum       string `json:"checksum"`
}

func (s Snapshot) ChecksumPayload() string {
	return fmt.Sprintf("mvcc-snapshot-v1\n%d\n%d\n%d", s.SnapshotSeq, s.GeneratedAtMS, s.TxnUpperBound)
}

func (s Snapshot) ComputeChecksum() string {
	sum := sha256.Sum256([]byte(s.ChecksumPayload()))
	return hex.EncodeToString(sum[:])
}

func (s Snapshot) WithChecksum() Snapshot {
	s.Checksum = s.ComputeChecksum()
	return s
}

func (s Snapshot) ValidateChecksum() error {
	expected := s.ComputeChecksum()
	if s.Checksum != expected {
		return NewError(CodeSnapshotInvalid, "snapshot checksum mismatch", map[string]any{
			"snapshot_seq":      s.SnapshotSeq,
			"expected_checksum": expected,
			"actual_checksum":   s.Checksum,
		})
	}
	return nil
}

type Txn struct {
	Seq           int64     `json:"txn_seq"`
	Status        TxnStatus `json:"status"`
	StartedAtMS   int64     `json:"started_at_ms"`
	SnapshotSeq   int64     `json:"snapshot_seq"`
	UpperBound    int64     `json:"upper_bound"`
	RequestID     string    `json:"request_id,omitempty"`
}

type RowVersion struct {
	RowKey                string      `json:"row_key"`
	VersionSeq            int64       `json:"version_seq"`
	CreateTxnSeq          int64       `json:"create_txn_seq"`
	CommitState           CommitState `json:"commit_state"`
	Digest                string      `json:"digest"`
	Content               string      `json:"content,omitempty"`
	PrevVersionSeq        *int64      `json:"prev_version_seq,omitempty"`
	IsDelete              bool        `json:"is_delete"`
	SupersedingCommitSeq  int64       `json:"superseding_commit_seq,omitempty"`
	Reclaimed             bool        `json:"reclaimed,omitempty"`
	Detached              bool        `json:"detached,omitempty"`
}

var digestPattern = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

func ValidDigest(digest string) bool {
	return digestPattern.MatchString(digest)
}

func NormalizeDigest(digest string) (string, bool) {
	if !ValidDigest(digest) {
		return "", false
	}
	return lowerHex(digest), true
}

func lowerHex(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'F' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

func DigestContent(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

type Candidate struct {
	VersionSeq     int64  `json:"version_seq"`
	PrevVersionSeq *int64 `json:"prev_version_seq,omitempty"`
	CreateTxnSeq   int64  `json:"create_txn_seq"`
	CommitState    string `json:"commit_state"`
	IsDelete       bool   `json:"is_delete"`
	Eligible       bool   `json:"eligible"`
	Reason         string `json:"reason"`
	Detail         string `json:"detail"`
}

type ReadResult struct {
	RowKey             string      `json:"row_key"`
	ReaderTxnSeq       int64       `json:"reader_txn_seq"`
	SnapshotSeq        int64       `json:"snapshot_seq"`
	UpperBound         int64       `json:"upper_bound"`
	Visible            bool        `json:"visible"`
	Outcome            string      `json:"outcome"`
	Reason             string      `json:"reason"`
	HitVersionSeq      *int64      `json:"hit_version_seq,omitempty"`
	ShadowVersionSeq   *int64      `json:"shadow_version_seq,omitempty"`
	HitDigest          string      `json:"hit_digest,omitempty"`
	HitContent         string      `json:"hit_content,omitempty"`
	Basis              []string    `json:"basis"`
	Candidates         []Candidate `json:"candidates"`
}

func (r ReadResult) CanonicalJSON() []byte {
	b, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return b
}

type Sample struct {
	Name         string          `json:"name"`
	Transactions []SampleTxn     `json:"transactions"`
	Versions     []SampleVersion `json:"versions"`
	ExpectedReads []SampleRead   `json:"expected_reads,omitempty"`
}

type SampleTxn struct {
	TxnSeq      int64     `json:"txn_seq"`
	Status      TxnStatus `json:"status"`
	StartedAtMS int64     `json:"started_at_ms"`
}

type SampleVersion struct {
	RowKey         string      `json:"row_key"`
	VersionSeq     int64       `json:"version_seq"`
	CreateTxnSeq   int64       `json:"create_txn_seq"`
	CommitState    CommitState `json:"commit_state"`
	Digest         string      `json:"digest"`
	Content        string      `json:"content,omitempty"`
	PrevVersionSeq *int64      `json:"prev_version_seq,omitempty"`
	IsDelete       bool        `json:"is_delete,omitempty"`
}

type SampleRead struct {
	Name          string `json:"name"`
	ReaderTxnSeq  int64  `json:"reader_txn_seq"`
	RowKey        string `json:"row_key"`
	Visible       bool   `json:"visible"`
	Reason        string `json:"reason"`
	HitVersionSeq *int64 `json:"hit_version_seq,omitempty"`
}
