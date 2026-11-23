package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"protocolfuzzlab/internal/codes"
	"protocolfuzzlab/internal/model"
)

type SnapshotRecord struct {
	Version          int             `json:"version"`
	ParentVersion    int             `json:"parent_version"`
	CaseSetVersion   int             `json:"caseset_version"`
	BatchID          string          `json:"batch_id"`
	LogicalBatch     string          `json:"logical_batch"`
	ReplayKey        string          `json:"replay_key"`
	Digest           string          `json:"digest"`
	Content          json.RawMessage `json:"content"`
	CreatedMarker    string          `json:"sampled_at"`
	WholeReplacement bool            `json:"whole_replacement"`
	ContentChanged   bool            `json:"content_changed"`
	PreviousVersion  int             `json:"previous_version"`
}

type SnapshotCommit struct {
	CaseSetVersion   int
	BatchID          string
	LogicalBatch     string
	ReplayKey        string
	Digest           string
	Content          []byte
	WholeReplacement bool
	ContentChanged   bool
	PreviousVersion  int
}

// CommitSnapshot appends one snapshot and its batch-completion marker in a
// single transaction. Reusing a replay key is idempotent and returns the already
// committed snapshot without advancing the version.
func (s *Store) CommitSnapshot(commit SnapshotCommit) (SnapshotRecord, bool, error) {
	if commit.CaseSetVersion <= 0 || commit.BatchID == "" || commit.LogicalBatch == "" || commit.ReplayKey == "" {
		return SnapshotRecord{}, false, codes.New(codes.BadRequest, "snapshot commit is missing required metadata")
	}
	if len(commit.Content) == 0 {
		return SnapshotRecord{}, false, codes.New(codes.BadRequest, "snapshot content is empty")
	}
	if commit.Digest == "" {
		commit.Digest = model.DigestBytes(commit.Content)
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SnapshotRecord{}, false, err
	}
	defer tx.Rollback()

	existing, err := loadSnapshotByReplayKeyTx(ctx, tx, commit.ReplayKey)
	if err == nil {
		if existing.Digest != commit.Digest {
			return SnapshotRecord{}, false, codes.New(codes.BadRequest, "replay key %q was reused with different content", commit.ReplayKey)
		}
		if err := tx.Commit(); err != nil {
			return SnapshotRecord{}, false, err
		}
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return SnapshotRecord{}, false, err
	}

	var caseSetExists int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM caseset_versions WHERE version=?`, commit.CaseSetVersion).Scan(&caseSetExists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SnapshotRecord{}, false, codes.New(codes.CasesetVersion, "case set version %d does not exist", commit.CaseSetVersion)
		}
		return SnapshotRecord{}, false, err
	}

	chain, chainErr := loadSnapshotChainTx(ctx, tx)
	if chainErr != nil {
		return SnapshotRecord{}, false, chainErr
	}
	next := len(chain) + 1
	parent := 0
	if len(chain) > 0 {
		parent = chain[len(chain)-1].Version
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO execution_snapshots(version,parent_version,caseset_version,batch_id,logical_batch,replay_key,digest,content,created_marker,whole_replacement,content_changed,previous_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		next, parent, commit.CaseSetVersion, commit.BatchID, commit.LogicalBatch, commit.ReplayKey, commit.Digest, commit.Content, SampleMarker, boolInt(commit.WholeReplacement), boolInt(commit.ContentChanged), commit.PreviousVersion); err != nil {
		return SnapshotRecord{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO batch_commits(replay_key,batch_id,caseset_version,snapshot_version,digest) VALUES(?,?,?,?,?)`,
		commit.ReplayKey, commit.BatchID, commit.CaseSetVersion, next, commit.Digest); err != nil {
		return SnapshotRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return SnapshotRecord{}, false, err
	}
	return SnapshotRecord{
		Version: next, ParentVersion: parent, CaseSetVersion: commit.CaseSetVersion,
		BatchID: commit.BatchID, LogicalBatch: commit.LogicalBatch, ReplayKey: commit.ReplayKey,
		Digest: commit.Digest, Content: append(json.RawMessage(nil), commit.Content...), CreatedMarker: SampleMarker,
		WholeReplacement: commit.WholeReplacement, ContentChanged: commit.ContentChanged, PreviousVersion: commit.PreviousVersion,
	}, true, nil
}

func (s *Store) Snapshot(version int) (SnapshotRecord, error) {
	return loadSnapshot(context.Background(), s.db, version)
}

func (s *Store) SnapshotByReplayKey(key string) (SnapshotRecord, bool, error) {
	rec, err := loadSnapshotByReplayKey(context.Background(), s.db, key)
	if errors.Is(err, sql.ErrNoRows) {
		return SnapshotRecord{}, false, nil
	}
	if err != nil {
		return SnapshotRecord{}, false, err
	}
	return rec, true, nil
}

// SnapshotChain returns the longest contiguous chain. If a version or parent
// link is missing it returns E_SNAPSHOT_VERSION and the records before the gap.
func (s *Store) SnapshotChain() ([]SnapshotRecord, error) {
	return s.snapshotChainRows(context.Background())
}

func (s *Store) LatestSnapshotForLogical(logicalBatch string) (SnapshotRecord, bool, error) {
	rec, err := loadLatestSnapshotForLogical(context.Background(), s.db, logicalBatch)
	if errors.Is(err, sql.ErrNoRows) {
		return SnapshotRecord{}, false, nil
	}
	if err != nil {
		return SnapshotRecord{}, false, err
	}
	return rec, true, nil
}

func (s *Store) SnapshotCount() (int, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM execution_snapshots`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// InjectSnapshotGap deliberately removes a middle snapshot for the external
// E_SNAPSHOT_VERSION verification sample. It never advances any head version.
func (s *Store) InjectSnapshotGap(version int) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var next sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MIN(version) FROM execution_snapshots WHERE version>?`, version).Scan(&next); err != nil {
		return err
	}
	if !next.Valid {
		return codes.New(codes.SnapshotVersion, "snapshot %d has no successor to expose a gap", version)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM execution_snapshots WHERE version=?`, version); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM batch_commits WHERE snapshot_version=?`, version); err != nil {
		return err
	}
	return tx.Commit()
}

func loadSnapshot(ctx context.Context, q queryer, version int) (SnapshotRecord, error) {
	return scanSnapshot(q.QueryRowContext(ctx, `SELECT version,parent_version,caseset_version,batch_id,logical_batch,replay_key,digest,content,created_marker,whole_replacement,content_changed,previous_version FROM execution_snapshots WHERE version=?`, version))
}

func loadSnapshotByReplayKey(ctx context.Context, q queryer, key string) (SnapshotRecord, error) {
	return scanSnapshot(q.QueryRowContext(ctx, `SELECT version,parent_version,caseset_version,batch_id,logical_batch,replay_key,digest,content,created_marker,whole_replacement,content_changed,previous_version FROM execution_snapshots WHERE replay_key=?`, key))
}

func loadSnapshotByReplayKeyTx(ctx context.Context, tx *sql.Tx, key string) (SnapshotRecord, error) {
	return loadSnapshotByReplayKey(ctx, tx, key)
}

func loadLatestSnapshotForLogical(ctx context.Context, q queryer, logical string) (SnapshotRecord, error) {
	return scanSnapshot(q.QueryRowContext(ctx, `SELECT version,parent_version,caseset_version,batch_id,logical_batch,replay_key,digest,content,created_marker,whole_replacement,content_changed,previous_version FROM execution_snapshots WHERE logical_batch=? ORDER BY version DESC LIMIT 1`, logical))
}

type rowScanner interface {
	Scan(...any) error
}

func scanSnapshot(row rowScanner) (SnapshotRecord, error) {
	var (
		rec            SnapshotRecord
		content        []byte
		whole, changed int
	)
	if err := row.Scan(&rec.Version, &rec.ParentVersion, &rec.CaseSetVersion, &rec.BatchID, &rec.LogicalBatch, &rec.ReplayKey, &rec.Digest, &content, &rec.CreatedMarker, &whole, &changed, &rec.PreviousVersion); err != nil {
		return SnapshotRecord{}, err
	}
	rec.Content = append(json.RawMessage(nil), content...)
	rec.WholeReplacement = whole != 0
	rec.ContentChanged = changed != 0
	return rec, nil
}

func loadSnapshotChainTx(ctx context.Context, tx *sql.Tx) ([]SnapshotRecord, error) {
	rows, err := tx.QueryContext(ctx, `SELECT version,parent_version,caseset_version,batch_id,logical_batch,replay_key,digest,content,created_marker,whole_replacement,content_changed,previous_version FROM execution_snapshots ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectSnapshotChain(rows)
}

func (s *Store) snapshotChainRows(ctx context.Context) ([]SnapshotRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT version,parent_version,caseset_version,batch_id,logical_batch,replay_key,digest,content,created_marker,whole_replacement,content_changed,previous_version FROM execution_snapshots ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectSnapshotChain(rows)
}

func collectSnapshotChain(rows *sql.Rows) ([]SnapshotRecord, error) {
	var records []SnapshotRecord
	for rows.Next() {
		rec, err := scanSnapshot(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	expected := 1
	parent := 0
	contiguous := make([]SnapshotRecord, 0, len(records))
	for _, rec := range records {
		if rec.Version != expected || rec.ParentVersion != parent {
			fallback := 0
			if len(contiguous) > 0 {
				fallback = contiguous[len(contiguous)-1].Version
			}
			return contiguous, &codes.Error{
				Code:            codes.SnapshotVersion,
				Message:         fmt.Sprintf("snapshot chain break at version %d (expected version %d, parent %d)", rec.Version, expected, parent),
				FallbackVersion: fallback,
			}
		}
		contiguous = append(contiguous, rec)
		parent = rec.Version
		expected++
	}
	return contiguous, nil
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// SnapshotVersions returns all physical snapshot version numbers, including
// versions beyond a chain gap. It is used for diagnostics and verification.
func (s *Store) SnapshotVersions() ([]int, error) {
	rows, err := s.db.Query(`SELECT version FROM execution_snapshots ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		out = append(out, version)
	}
	return out, rows.Err()
}

func (s *Store) MaxSnapshotVersion() (int, error) {
	var version sql.NullInt64
	if err := s.db.QueryRow(`SELECT MAX(version) FROM execution_snapshots`).Scan(&version); err != nil {
		return 0, err
	}
	if !version.Valid {
		return 0, nil
	}
	return int(version.Int64), nil
}
