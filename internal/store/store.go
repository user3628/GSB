package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"logagg/internal/agg"
	"logagg/internal/errs"
	"logagg/internal/model"
)

type Store struct {
	db   *sql.DB
	path string
	now  func() time.Time
}

func Open(path string) (*Store, error) {
	if path != ":memory:" && path != "" {
		dir := filepath.Dir(path)
		if dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
		}
	}
	dsn := path
	if path != ":memory:" {
		dsn = fmt.Sprintf("file:%s?_foreign_keys=on&_journal_mode=DELETE&_synchronous=FULL&_busy_timeout=5000", path)
	} else {
		dsn = ":memory:?_foreign_keys=on&_journal_mode=MEMORY&_synchronous=FULL"
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db, path: path, now: func() time.Time { return time.Now().UTC() }}
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Path() string { return s.path }

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, schemaSQL)
	return err
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
INSERT OR IGNORE INTO meta(key,value) VALUES ('schema_version','1');

CREATE TABLE IF NOT EXISTS intents (
	intent_id TEXT PRIMARY KEY,
	kind TEXT NOT NULL,
	status TEXT NOT NULL,
	session_id TEXT NOT NULL DEFAULT '',
	batch_id TEXT NOT NULL DEFAULT '',
	shard_id TEXT NOT NULL DEFAULT '',
	version INTEGER,
	block_no INTEGER,
	partition INTEGER,
	segment_start INTEGER,
	segment_end INTEGER,
	payload_json TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status, created_at);

CREATE TABLE IF NOT EXISTS segment_work (
	session_id TEXT NOT NULL,
	shard_id TEXT NOT NULL,
	version INTEGER NOT NULL,
	block_no INTEGER NOT NULL,
	segment_start INTEGER NOT NULL,
	segment_end INTEGER NOT NULL,
	partition INTEGER NOT NULL DEFAULT 0,
	status TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	PRIMARY KEY(session_id, shard_id, version, block_no, segment_start)
);
CREATE INDEX IF NOT EXISTS idx_segment_work_status ON segment_work(status, session_id);

CREATE TABLE IF NOT EXISTS partial_sums (
	session_id TEXT NOT NULL,
	shard_id TEXT NOT NULL,
	version INTEGER NOT NULL,
	block_no INTEGER NOT NULL,
	segment_start INTEGER NOT NULL,
	segment_end INTEGER NOT NULL,
	partition INTEGER NOT NULL,
	definition_hash TEXT NOT NULL,
	sum_json TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	PRIMARY KEY(session_id, shard_id, version, block_no, segment_start)
);

CREATE TABLE IF NOT EXISTS batches (
	batch_id TEXT PRIMARY KEY,
	session_id TEXT NOT NULL,
	request_hash TEXT NOT NULL,
	status TEXT NOT NULL,
	result_json TEXT NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_session ON batches(session_id, batch_id);

CREATE TABLE IF NOT EXISTS shards (
	shard_id TEXT NOT NULL,
	version INTEGER NOT NULL,
	manifest_json TEXT NOT NULL,
	index_hash TEXT NOT NULL,
	committed_at TEXT NOT NULL,
	PRIMARY KEY(shard_id, version)
);

CREATE TABLE IF NOT EXISTS current_shard (
	shard_id TEXT PRIMARY KEY,
	version INTEGER NOT NULL,
	intent_id TEXT NOT NULL DEFAULT '',
	committed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shard_block_data (
	shard_id TEXT NOT NULL,
	version INTEGER NOT NULL,
	block_no INTEGER NOT NULL,
	partition INTEGER NOT NULL,
	meta_json TEXT NOT NULL,
	group_payload BLOB NOT NULL,
	metric_payload BLOB NOT NULL,
	PRIMARY KEY(shard_id, version, block_no)
);
`

type Intent struct {
	ID           string `json:"intent_id"`
	Kind         string `json:"kind"`
	Status       string `json:"status"`
	SessionID    string `json:"session_id,omitempty"`
	BatchID      string `json:"batch_id,omitempty"`
	ShardID      string `json:"shard_id,omitempty"`
	Version      int64  `json:"version,omitempty"`
	BlockNo      int    `json:"block_no,omitempty"`
	Partition    int    `json:"partition,omitempty"`
	SegmentStart int    `json:"segment_start,omitempty"`
	SegmentEnd   int    `json:"segment_end,omitempty"`
	PayloadJSON  string `json:"payload_json,omitempty"`
}

func (s *Store) InsertIntent(ctx context.Context, intent Intent) error {
	now := s.now().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
INSERT OR IGNORE INTO intents
(intent_id,kind,status,session_id,batch_id,shard_id,version,block_no,partition,segment_start,segment_end,payload_json,created_at,updated_at)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		intent.ID, intent.Kind, defaultString(intent.Status, "pending"), intent.SessionID, intent.BatchID, intent.ShardID,
		nullableInt64(intent.Version), nullableInt(intent.BlockNo), nullableInt(intent.Partition),
		nullableInt(intent.SegmentStart), nullableInt(intent.SegmentEnd), intent.PayloadJSON, now, now)
	return err
}

func (s *Store) MarkIntentCommitted(ctx context.Context, intentID string) error {
	if intentID == "" {
		return nil
	}
	now := s.now().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET status='committed',updated_at=? WHERE intent_id=?`, now, intentID)
	return err
}

func (s *Store) BeginSegment(ctx context.Context, intent Intent, partition int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := s.now().Format(time.RFC3339Nano)
	intent.Status = "pending"
	if _, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO intents
(intent_id,kind,status,session_id,batch_id,shard_id,version,block_no,partition,segment_start,segment_end,payload_json,created_at,updated_at)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		intent.ID, intent.Kind, intent.Status, intent.SessionID, intent.BatchID, intent.ShardID,
		intent.Version, intent.BlockNo, partition, intent.SegmentStart, intent.SegmentEnd, intent.PayloadJSON, now, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO segment_work
(session_id,shard_id,version,block_no,segment_start,segment_end,partition,status,updated_at)
VALUES (?,?,?,?,?,?,?, 'pending_decode', ?)`,
		intent.SessionID, intent.ShardID, intent.Version, intent.BlockNo, intent.SegmentStart, intent.SegmentEnd, partition, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CommitPartialSum(ctx context.Context, sum *agg.SegmentSum, intentID string) error {
	payload, err := json.Marshal(sum)
	if err != nil {
		return err
	}
	now := s.now().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO partial_sums
(session_id,shard_id,version,block_no,segment_start,segment_end,partition,definition_hash,sum_json,updated_at)
VALUES (?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(session_id,shard_id,version,block_no,segment_start) DO UPDATE SET
	segment_end=excluded.segment_end,
	partition=excluded.partition,
	definition_hash=excluded.definition_hash,
	sum_json=excluded.sum_json,
	updated_at=excluded.updated_at`,
		sum.SessionID, sum.ShardID, sum.BaseVersion, sum.BlockNo, sum.SegmentStart, sum.SegmentEnd,
		sum.Partition, sum.DefinitionHash, string(payload), now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE segment_work SET status='committed', segment_end=?, partition=?, updated_at=?
WHERE session_id=? AND shard_id=? AND version=? AND block_no=? AND segment_start=?`,
		sum.SegmentEnd, sum.Partition, now, sum.SessionID, sum.ShardID, sum.BaseVersion, sum.BlockNo, sum.SegmentStart); err != nil {
		return err
	}
	if intentID != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed', updated_at=? WHERE intent_id=?`, now, intentID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) LoadPartialSum(ctx context.Context, sessionID, shardID string, version int64, blockNo, segmentStart int) (*agg.SegmentSum, error) {
	var payload string
	err := s.db.QueryRowContext(ctx, `
SELECT sum_json FROM partial_sums
WHERE session_id=? AND shard_id=? AND version=? AND block_no=? AND segment_start=?`,
		sessionID, shardID, version, blockNo, segmentStart).Scan(&payload)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var sum agg.SegmentSum
	if err := json.Unmarshal([]byte(payload), &sum); err != nil {
		return nil, err
	}
	return &sum, nil
}

func (s *Store) LoadSessionPartialSums(ctx context.Context, sessionID string) ([]*agg.SegmentSum, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT sum_json FROM partial_sums WHERE session_id=? ORDER BY shard_id, version, block_no, segment_start`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*agg.SegmentSum{}
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var sum agg.SegmentSum
		if err := json.Unmarshal([]byte(payload), &sum); err != nil {
			return nil, err
		}
		out = append(out, &sum)
	}
	return out, rows.Err()
}

type BatchRecord struct {
	BatchID     string `json:"batch_id"`
	SessionID   string `json:"session_id"`
	RequestHash string `json:"request_hash"`
	Status      string `json:"status"`
	ResultJSON  string `json:"result_json"`
}

func (s *Store) GetBatch(ctx context.Context, batchID string) (*BatchRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT batch_id,session_id,request_hash,status,result_json FROM batches WHERE batch_id=?`, batchID)
	var record BatchRecord
	err := row.Scan(&record.BatchID, &record.SessionID, &record.RequestHash, &record.Status, &record.ResultJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *Store) SaveBatch(ctx context.Context, record BatchRecord) error {
	now := s.now().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO batches(batch_id,session_id,request_hash,status,result_json,created_at,updated_at)
VALUES(?,?,?,?,?,?,?)
ON CONFLICT(batch_id) DO UPDATE SET
	session_id=excluded.session_id,
	request_hash=excluded.request_hash,
	status=excluded.status,
	result_json=excluded.result_json,
	updated_at=excluded.updated_at`,
		record.BatchID, record.SessionID, record.RequestHash, record.Status, record.ResultJSON, now, now)
	return err
}

func (s *Store) CommitShard(ctx context.Context, pkg *model.ShardPackage, indexHash, intentID string) error {
	manifestJSON, err := json.Marshal(pkg.Manifest)
	if err != nil {
		return err
	}
	now := s.now().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO shards(shard_id,version,manifest_json,index_hash,committed_at)
VALUES(?,?,?,?,?)
ON CONFLICT(shard_id,version) DO UPDATE SET manifest_json=excluded.manifest_json,index_hash=excluded.index_hash`,
		pkg.Manifest.ShardID, pkg.Manifest.Version, string(manifestJSON), indexHash, now); err != nil {
		return err
	}
	for _, block := range pkg.Blocks {
		metaJSON, err := json.Marshal(block.Meta)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO shard_block_data(shard_id,version,block_no,partition,meta_json,group_payload,metric_payload)
VALUES(?,?,?,?,?,?,?)
ON CONFLICT(shard_id,version,block_no) DO UPDATE SET
	partition=excluded.partition,meta_json=excluded.meta_json,
	group_payload=excluded.group_payload,metric_payload=excluded.metric_payload`,
			pkg.Manifest.ShardID, pkg.Manifest.Version, block.Meta.BlockNo, block.Meta.Partition,
			string(metaJSON), block.GroupPayload, block.MetricPayload); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO current_shard(shard_id,version,intent_id,committed_at)
VALUES(?,?,?,?)
ON CONFLICT(shard_id) DO UPDATE SET version=excluded.version,intent_id=excluded.intent_id,committed_at=excluded.committed_at`,
		pkg.Manifest.ShardID, pkg.Manifest.Version, intentID, now); err != nil {
		return err
	}
	if intentID != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed',updated_at=? WHERE intent_id=?`, now, intentID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) CurrentShardVersion(ctx context.Context, shardID string) (int64, bool, error) {
	var version int64
	err := s.db.QueryRowContext(ctx, `SELECT version FROM current_shard WHERE shard_id=?`, shardID).Scan(&version)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return version, true, nil
}

func (s *Store) LoadShardPackage(ctx context.Context, shardID string, version int64) (*model.ShardPackage, error) {
	if version == 0 {
		var ok bool
		var err error
		version, ok, err = s.CurrentShardVersion(ctx, shardID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
	}
	var manifestJSON string
	err := s.db.QueryRowContext(ctx, `SELECT manifest_json FROM shards WHERE shard_id=? AND version=?`, shardID, version).Scan(&manifestJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest model.Shard
	if err := json.Unmarshal([]byte(manifestJSON), &manifest); err != nil {
		return nil, err
	}
	pkg := &model.ShardPackage{Manifest: manifest}
	rows, err := s.db.QueryContext(ctx, `
SELECT meta_json,group_payload,metric_payload FROM shard_block_data
WHERE shard_id=? AND version=? ORDER BY block_no`, shardID, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var metaJSON string
		var group, metric []byte
		if err := rows.Scan(&metaJSON, &group, &metric); err != nil {
			return nil, err
		}
		var meta model.BlockMeta
		if err := json.Unmarshal([]byte(metaJSON), &meta); err != nil {
			return nil, err
		}
		pkg.Blocks = append(pkg.Blocks, &model.Block{Meta: meta, GroupPayload: group, MetricPayload: metric})
	}
	return pkg, rows.Err()
}

func nullableInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullableInt(v int) any {
	if v == 0 {
		return nil
	}
	return v
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func codedInternal(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errs.As(err); ok {
		return err
	}
	return errs.New(errs.EInternal, "%v", err)
}
