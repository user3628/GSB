package dnsdetect

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "modernc.org/sqlite"
)

const zeroDigest = "0000000000000000000000000000000000000000000000000000000000000000"

type SourcePriority struct {
	Kind     SourceKind `json:"kind"`
	Priority int        `json:"priority"`
}

type SnapshotHead struct {
	Version       int64  `json:"version"`
	ContentDigest string `json:"content_digest"`
}

type Snapshot struct {
	Version           int64            `json:"version"`
	CreatedTicks      int64            `json:"created_ticks"`
	FormatVersions    []int            `json:"format_versions"`
	SourcePriorities  []SourcePriority `json:"source_priorities"`
	LedgerDigest      string           `json:"ledger_digest"`
	ContentDigest     string           `json:"content_digest"`
	PrevContentDigest string           `json:"prev_content_digest"`
	WindowProgress    []WindowProgress `json:"window_progress"`
	Content           []byte           `json:"-"`
}

func (s Snapshot) Head() SnapshotHead {
	return SnapshotHead{Version: s.Version, ContentDigest: s.ContentDigest}
}

type SnapshotDraft struct {
	CreatedTicks     int64
	FormatVersions   []int
	SourcePriorities []SourcePriority
	LedgerDigest     string
	WindowProgress   []WindowProgress
	Content          []byte
}

type SnapshotLoadResult struct {
	Snapshots       []Snapshot `json:"snapshots"`
	Latest          *Snapshot  `json:"latest,omitempty"`
	RejectedVersion int64      `json:"rejected_version,omitempty"`
}

type SnapshotStore struct {
	db   *sql.DB
	path string
}

func OpenSnapshotStore(path string) (*SnapshotStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, newBusinessError(CodeIO, path, -1, "snapshot database path is empty", nil)
	}
	if path != ":memory:" && !strings.HasPrefix(path, "file:") {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, newBusinessError(CodeIO, path, -1, "cannot create snapshot database directory", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, newBusinessError(CodeIO, path, -1, "cannot open SQLite database", err)
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA busy_timeout=5000",
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, newBusinessError(CodeIO, path, -1, "cannot configure SQLite database", err)
		}
	}
	store := &SnapshotStore{db: db, path: path}
	if err := store.initialize(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SnapshotStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *SnapshotStore) initialize(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS snapshots (
    version INTEGER PRIMARY KEY CHECK(version >= 1),
    created_ticks INTEGER NOT NULL,
    format_versions TEXT NOT NULL,
    source_priorities TEXT NOT NULL,
    ledger_digest TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    prev_content_digest TEXT NOT NULL,
    window_progress TEXT NOT NULL,
    content BLOB NOT NULL
);`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return newBusinessError(CodeIO, s.path, -1, "cannot initialize snapshot schema", err)
	}
	return nil
}

type snapshotQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func contentDigest(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func normalizeFormatVersions(values []int) []int {
	set := make(map[int]bool)
	for _, value := range values {
		set[value] = true
	}
	versions := make([]int, 0, len(set))
	for value := range set {
		versions = append(versions, value)
	}
	sort.Ints(versions)
	return versions
}

func normalizePriorities(values []SourcePriority) []SourcePriority {
	set := make(map[SourceKind]int)
	for _, value := range values {
		set[value.Kind] = value.Priority
	}
	out := make([]SourcePriority, 0, len(set))
	for kind, priority := range set {
		out = append(out, SourcePriority{Kind: kind, Priority: priority})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Kind < out[j].Kind })
	return out
}

// Commit appends one immutable snapshot inside a single SQLite transaction.
// If the latest content digest is identical, the existing snapshot is returned
// without advancing the version; this makes restart and re-run idempotent.
func (s *SnapshotStore) Commit(ctx context.Context, draft SnapshotDraft) (*Snapshot, error) {
	if len(draft.Content) == 0 {
		return nil, newBusinessError(CodeSnapshotFormat, s.path, -1, "snapshot content is empty", nil)
	}
	digest := contentDigest(draft.Content)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, newBusinessError(CodeIO, s.path, -1, "cannot begin snapshot transaction", err)
	}
	defer tx.Rollback()
	existing, err := readSnapshots(ctx, tx, s.path)
	if err != nil {
		return nil, err
	}
	version := int64(1)
	previousDigest := zeroDigest
	if len(existing) > 0 {
		latest := existing[len(existing)-1]
		if latest.ContentDigest == digest && snapshotMetadataEqual(latest, draft) {
			if err := tx.Commit(); err != nil {
				return nil, newBusinessError(CodeIO, s.path, -1, "cannot finish idempotent snapshot transaction", err)
			}
			return &latest, nil
		}
		version = latest.Version + 1
		previousDigest = latest.ContentDigest
	}
	formatVersions, err := json.Marshal(normalizeFormatVersions(draft.FormatVersions))
	if err != nil {
		return nil, newBusinessError(CodeSnapshotFormat, s.path, -1, "cannot encode format versions", err)
	}
	priorities, err := json.Marshal(normalizePriorities(draft.SourcePriorities))
	if err != nil {
		return nil, newBusinessError(CodeSnapshotFormat, s.path, -1, "cannot encode source priorities", err)
	}
	progress := append([]WindowProgress(nil), draft.WindowProgress...)
	sort.Slice(progress, func(i, j int) bool { return progress[i].Index < progress[j].Index })
	progressJSON, err := json.Marshal(progress)
	if err != nil {
		return nil, newBusinessError(CodeSnapshotFormat, s.path, -1, "cannot encode window progress", err)
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO snapshots(version, created_ticks, format_versions, source_priorities, ledger_digest, content_digest, prev_content_digest, window_progress, content)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		version, draft.CreatedTicks, string(formatVersions), string(priorities), draft.LedgerDigest,
		digest, previousDigest, string(progressJSON), draft.Content)
	if err != nil {
		return nil, newBusinessError(CodeIO, s.path, -1, "cannot insert snapshot", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, newBusinessError(CodeIO, s.path, -1, "cannot commit snapshot transaction", err)
	}
	return &Snapshot{
		Version:           version,
		CreatedTicks:      draft.CreatedTicks,
		FormatVersions:    normalizeFormatVersions(draft.FormatVersions),
		SourcePriorities:  normalizePriorities(draft.SourcePriorities),
		LedgerDigest:      draft.LedgerDigest,
		ContentDigest:     digest,
		PrevContentDigest: previousDigest,
		WindowProgress:    progress,
		Content:           append([]byte(nil), draft.Content...),
	}, nil
}

// Load validates version continuity, per-row content digests, and the backward
// content-digest chain. On failure it returns all valid snapshots before the
// fault, so the caller can fall back to Latest.
func (s *SnapshotStore) Load(ctx context.Context) (*SnapshotLoadResult, error) {
	snapshots, err := readSnapshots(ctx, s.db, s.path)
	result := &SnapshotLoadResult{}
	for _, snapshot := range snapshots {
		result.Snapshots = append(result.Snapshots, snapshot)
		result.Latest = &result.Snapshots[len(result.Snapshots)-1]
	}
	if err != nil {
		if business, ok := err.(*BusinessError); ok && business.Offset >= 0 {
			result.RejectedVersion = business.Offset
		}
		return result, err
	}
	return result, nil
}

func (s *SnapshotStore) Latest(ctx context.Context) (*Snapshot, error) {
	result, err := s.Load(ctx)
	if err != nil {
		return result.Latest, err
	}
	return result.Latest, nil
}

func readSnapshots(ctx context.Context, queryer snapshotQueryer, path string) ([]Snapshot, error) {
	rows, err := queryer.QueryContext(ctx, `
SELECT version, created_ticks, format_versions, source_priorities, ledger_digest,
       content_digest, prev_content_digest, window_progress, content
FROM snapshots ORDER BY version ASC`)
	if err != nil {
		return nil, newBusinessError(CodeIO, path, -1, "cannot query snapshots", err)
	}
	defer rows.Close()
	snapshots := make([]Snapshot, 0)
	expectedVersion := int64(1)
	previousDigest := zeroDigest
	for rows.Next() {
		var snapshot Snapshot
		var formatJSON, priorityJSON, progressJSON string
		if err := rows.Scan(&snapshot.Version, &snapshot.CreatedTicks, &formatJSON, &priorityJSON,
			&snapshot.LedgerDigest, &snapshot.ContentDigest, &snapshot.PrevContentDigest, &progressJSON, &snapshot.Content); err != nil {
			return snapshots, newBusinessError(CodeIO, path, -1, "cannot scan snapshot", err)
		}
		if snapshot.Version != expectedVersion {
			return snapshots, newBusinessError(CodeSnapshotVersion, path, expectedVersion,
				fmt.Sprintf("snapshot chain expected version %d, found %d", expectedVersion, snapshot.Version), nil)
		}
		actualDigest := contentDigest(snapshot.Content)
		if actualDigest != snapshot.ContentDigest {
			return snapshots, newBusinessError(CodeSnapshotReplaced, path, snapshot.Version,
				fmt.Sprintf("snapshot %d content digest mismatch", snapshot.Version), nil)
		}
		if snapshot.PrevContentDigest != previousDigest {
			return snapshots, newBusinessError(CodeSnapshotReplaced, path, snapshot.Version,
				fmt.Sprintf("snapshot %d previous content digest does not match chain", snapshot.Version), nil)
		}
		if err := json.Unmarshal([]byte(formatJSON), &snapshot.FormatVersions); err != nil {
			return snapshots, newBusinessError(CodeSnapshotFormat, path, snapshot.Version, "cannot decode format versions", err)
		}
		if err := json.Unmarshal([]byte(priorityJSON), &snapshot.SourcePriorities); err != nil {
			return snapshots, newBusinessError(CodeSnapshotFormat, path, snapshot.Version, "cannot decode source priorities", err)
		}
		if err := json.Unmarshal([]byte(progressJSON), &snapshot.WindowProgress); err != nil {
			return snapshots, newBusinessError(CodeSnapshotFormat, path, snapshot.Version, "cannot decode window progress", err)
		}
		snapshot.FormatVersions = normalizeFormatVersions(snapshot.FormatVersions)
		snapshot.SourcePriorities = normalizePriorities(snapshot.SourcePriorities)
		sort.Slice(snapshot.WindowProgress, func(i, j int) bool { return snapshot.WindowProgress[i].Index < snapshot.WindowProgress[j].Index })
		snapshots = append(snapshots, snapshot)
		previousDigest = snapshot.ContentDigest
		expectedVersion++
	}
	if err := rows.Err(); err != nil {
		return snapshots, newBusinessError(CodeIO, path, -1, "snapshot row iteration failed", err)
	}
	return snapshots, nil
}

// VerifyHead provides the external trust anchor needed to detect whole-file or
// whole-table replacement. A version number alone is never sufficient.
func (s *SnapshotStore) VerifyHead(ctx context.Context, expected SnapshotHead) error {
	if expected.Version <= 0 || expected.ContentDigest == "" {
		return newBusinessError(CodeSnapshotVersion, s.path, expected.Version, "invalid expected snapshot head", nil)
	}
	result, loadErr := s.Load(ctx)
	if result.Latest == nil || result.Latest.Version < expected.Version {
		if loadErr != nil {
			return loadErr
		}
		return newBusinessError(CodeSnapshotVersion, s.path, expected.Version, "expected snapshot version is not present", nil)
	}
	if result.Latest.Version != expected.Version {
		if loadErr != nil {
			return loadErr
		}
		return newBusinessError(CodeSnapshotVersion, s.path, expected.Version, "expected content anchor is not the current snapshot head", nil)
	}
	for _, snapshot := range result.Snapshots {
		if snapshot.Version == expected.Version {
			if snapshot.ContentDigest != expected.ContentDigest {
				return newBusinessError(CodeSnapshotReplaced, s.path, expected.Version, "snapshot head content was replaced", nil)
			}
			if loadErr != nil {
				return loadErr
			}
			return nil
		}
	}
	if loadErr != nil {
		return loadErr
	}
	return newBusinessError(CodeSnapshotVersion, s.path, expected.Version, "expected snapshot version is absent", nil)
}

func snapshotMetadataEqual(snapshot Snapshot, draft SnapshotDraft) bool {
	if snapshot.CreatedTicks != draft.CreatedTicks || snapshot.LedgerDigest != draft.LedgerDigest {
		return false
	}
	formats := normalizeFormatVersions(draft.FormatVersions)
	if len(snapshot.FormatVersions) != len(formats) {
		return false
	}
	for index := range formats {
		if snapshot.FormatVersions[index] != formats[index] {
			return false
		}
	}
	priorities := normalizePriorities(draft.SourcePriorities)
	if len(snapshot.SourcePriorities) != len(priorities) {
		return false
	}
	for index := range priorities {
		if snapshot.SourcePriorities[index] != priorities[index] {
			return false
		}
	}
	progress := append([]WindowProgress(nil), draft.WindowProgress...)
	sort.Slice(progress, func(i, j int) bool { return progress[i].Index < progress[j].Index })
	if len(snapshot.WindowProgress) != len(progress) {
		return false
	}
	for index := range progress {
		if snapshot.WindowProgress[index] != progress[index] {
			return false
		}
	}
	return true
}

// Replay decodes a stored snapshot. Version zero selects the latest valid
// snapshot. On a chain fault the latest contiguous snapshot and the fault are
// returned together.
func (s *SnapshotStore) Replay(ctx context.Context, version int64) (*Report, error) {
	result, loadErr := s.Load(ctx)
	var selected *Snapshot
	if version <= 0 {
		selected = result.Latest
	} else {
		for index := range result.Snapshots {
			if result.Snapshots[index].Version == version {
				selected = &result.Snapshots[index]
				break
			}
		}
	}
	if selected == nil {
		if loadErr != nil {
			return nil, loadErr
		}
		return nil, newBusinessError(CodeSnapshotVersion, s.path, version, "snapshot version is not present in the contiguous chain", nil)
	}
	var report Report
	if err := json.Unmarshal(selected.Content, &report); err != nil {
		return nil, newBusinessError(CodeSnapshotFormat, s.path, selected.Version, "cannot decode snapshot report", err)
	}
	report.Snapshot = &SnapshotInfo{
		Version:           selected.Version,
		CreatedTicks:      selected.CreatedTicks,
		ContentDigest:     selected.ContentDigest,
		PrevContentDigest: selected.PrevContentDigest,
		LedgerDigest:      selected.LedgerDigest,
	}
	return &report, loadErr
}

func IsSnapshotChainError(err error) bool {
	code := CodeOf(err)
	return code == CodeSnapshotVersion || code == CodeSnapshotReplaced
}
