package rewardpack

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS config_snapshots (
  version INTEGER PRIMARY KEY,
  digest TEXT NOT NULL,
  content BLOB NOT NULL,
  applied_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sources (
  source TEXT PRIMARY KEY,
  revision INTEGER NOT NULL,
  digest TEXT NOT NULL,
  pair_count INTEGER NOT NULL,
  replaced_generation INTEGER NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS annotations (
  pair_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  raw_json BLOB NOT NULL,
  content_digest TEXT NOT NULL,
  revision INTEGER NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS annotations_source_idx ON annotations(source);
CREATE TABLE IF NOT EXISTS pending_changes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,
  source TEXT,
  pair_id TEXT,
  old_digest TEXT,
  new_digest TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stage_state (
  stage TEXT PRIMARY KEY,
  seq INTEGER NOT NULL,
  input_fingerprint TEXT NOT NULL,
  config_version INTEGER NOT NULL,
  artifact_digest TEXT NOT NULL,
  payload BLOB NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS checkpoints (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  stage TEXT NOT NULL,
  stage_seq INTEGER NOT NULL,
  input_fingerprint TEXT NOT NULL,
  config_version INTEGER NOT NULL,
  artifact_digest TEXT NOT NULL,
  valid INTEGER NOT NULL,
  created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS checkpoints_one_valid_input
  ON checkpoints(stage, input_fingerprint) WHERE valid = 1;
CREATE INDEX IF NOT EXISTS checkpoints_stage_seq_idx ON checkpoints(stage, stage_seq);
CREATE TABLE IF NOT EXISTS pair_artifacts (
  stage TEXT NOT NULL,
  pair_id TEXT NOT NULL,
  source TEXT NOT NULL,
  input_digest TEXT NOT NULL,
  artifact_digest TEXT NOT NULL,
  payload BLOB NOT NULL,
  PRIMARY KEY(stage, pair_id)
);
CREATE INDEX IF NOT EXISTS pair_artifacts_stage_source_idx ON pair_artifacts(stage, source);
CREATE TABLE IF NOT EXISTS invalidations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fingerprint TEXT NOT NULL UNIQUE,
  change_kind TEXT NOT NULL,
  first_diff_pair_id TEXT,
  affected_sources BLOB NOT NULL,
  earliest_stage TEXT NOT NULL,
  invalidated_stages BLOB NOT NULL,
  affected_pairs BLOB NOT NULL,
  all_pairs INTEGER NOT NULL,
  previous_manifest_digest TEXT NOT NULL,
  current_manifest_digest TEXT NOT NULL,
  previous_config_version INTEGER NOT NULL,
  current_config_version INTEGER NOT NULL,
  reason TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS manifest_history (
  digest TEXT PRIMARY KEY,
  manifest BLOB NOT NULL,
  config_version INTEGER NOT NULL,
  created_at TEXT NOT NULL
);
`

// Store is a local SQLite repository. All durable state is committed through
// SQLite transactions; cross-process publication is additionally serialized by
// a real OS file lock.
type Store struct {
	db       *sql.DB
	path     string
	lockPath string
}

// OpenStore opens or creates a local SQLite database and applies the schema.
func OpenStore(path string) (*Store, error) {
	return OpenStoreContext(context.Background(), path)
}

func OpenStoreContext(ctx context.Context, path string) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, errorWith(CodeInvalidInput, err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, errorWith(CodeInvalidInput, fmt.Errorf("create database directory: %w", err))
	}
	dsn := "file:" + filepath.ToSlash(abs) + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, errorWith(CodeInternal, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, errorWith(CodeInternal, err)
	}
	if err := execRetry(ctx, db.ExecContext, schemaSQL); err != nil {
		_ = db.Close()
		return nil, errorWith(CodeInternal, fmt.Errorf("apply schema: %w", err))
	}
	if err := execRetry(ctx, db.ExecContext, `PRAGMA busy_timeout=10000`); err != nil {
		_ = db.Close()
		return nil, errorWith(CodeInternal, fmt.Errorf("set busy timeout: %w", err))
	}
	return &Store{db: db, path: abs, lockPath: abs + ".lock"}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Path() string     { return s.path }
func (s *Store) LockPath() string { return s.lockPath }

func nowUTC() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (s *Store) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	const maxBusyWait = 15 * time.Second
	deadline := time.Now().Add(maxBusyWait)
	for {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			if isSQLiteBusy(err) && time.Now().Before(deadline) {
				if sleepErr := sleepContext(ctx, 20*time.Millisecond); sleepErr != nil {
					return sleepErr
				}
				continue
			}
			if isSQLiteBusy(err) {
				return &BusinessError{Code: CodeLockTimeout, Context: "SQLite transaction begin", Message: err.Error()}
			}
			return errorWith(CodeInternal, err)
		}
		if err := fn(tx); err != nil {
			_ = tx.Rollback()
			if isSQLiteBusy(err) && time.Now().Before(deadline) {
				if sleepErr := sleepContext(ctx, 20*time.Millisecond); sleepErr != nil {
					return sleepErr
				}
				continue
			}
			if isSQLiteBusy(err) {
				return &BusinessError{Code: CodeLockTimeout, Context: "SQLite transaction body", Message: err.Error()}
			}
			return err
		}
		if err := tx.Commit(); err != nil {
			if isSQLiteBusy(err) && time.Now().Before(deadline) {
				if sleepErr := sleepContext(ctx, 20*time.Millisecond); sleepErr != nil {
					return sleepErr
				}
				continue
			}
			if isSQLiteBusy(err) {
				return &BusinessError{Code: CodeLockTimeout, Context: "SQLite transaction commit", Message: err.Error()}
			}
			return errorWith(CodeInternal, err)
		}
		return nil
	}
}

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "database is locked") || strings.Contains(text, "sqlite_busy") || strings.Contains(text, "sqlite_locked")
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type execContextFunc func(context.Context, string, ...any) (sql.Result, error)

func execRetry(ctx context.Context, exec execContextFunc, query string, args ...any) error {
	const maxBusyWait = 15 * time.Second
	deadline := time.Now().Add(maxBusyWait)
	for {
		_, err := exec(ctx, query, args...)
		if err == nil {
			return nil
		}
		if !isSQLiteBusy(err) {
			return err
		}
		if time.Now().After(deadline) {
			return &BusinessError{Code: CodeLockTimeout, Context: "SQLite exec", Message: err.Error()}
		}
		if err := sleepContext(ctx, 20*time.Millisecond); err != nil {
			return err
		}
	}
}

func validateAnnotation(a Annotation) error {
	if strings.TrimSpace(a.PairID) == "" {
		return &BusinessError{Code: CodeInvalidInput, Field: "pair_id", Message: "pair_id must not be empty"}
	}
	if strings.TrimSpace(a.Source) == "" {
		return &BusinessError{Code: CodeInvalidInput, PairID: a.PairID, Field: "source", Message: "source must not be empty"}
	}
	if _, err := json.Marshal(a); err != nil {
		if strings.Contains(err.Error(), "NaN") || strings.Contains(err.Error(), "Inf") {
			return &BusinessError{Code: CodeNumeric, PairID: a.PairID, Factor: factorConsistency, Message: "annotation contains a non-finite number"}
		}
		return &BusinessError{Code: CodeInvalidInput, PairID: a.PairID, Message: err.Error()}
	}
	return nil
}

// ApplyConfig persists a new immutable configuration snapshot. Version
// regression and same-version mutation are rejected.
func (s *Store) ApplyConfig(ctx context.Context, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	content, err := yamlMarshal(cfg)
	if err != nil {
		return errorWith(CodeInvalidConfig, err)
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		var currentVersion int
		var currentDigest string
		err := tx.QueryRowContext(ctx, `SELECT version, digest FROM config_snapshots ORDER BY version DESC LIMIT 1`).Scan(&currentVersion, &currentDigest)
		if err != nil && err != sql.ErrNoRows {
			return errorWith(CodeInternal, err)
		}
		digest := cfg.Digest()
		if err == nil {
			if cfg.Version < currentVersion {
				return &BusinessError{Code: CodeVersionRollback, Expected: currentVersion, Actual: cfg.Version, Context: "config snapshot version regression", Message: fmt.Sprintf("config version %d is older than active %d", cfg.Version, currentVersion)}
			}
			if cfg.Version == currentVersion {
				if digest == currentDigest {
					return nil
				}
				return &BusinessError{Code: CodeVersionConflict, Expected: currentDigest, Actual: digest, Context: "same config version with different contents", Message: fmt.Sprintf("config version %d already has different contents", cfg.Version)}
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO config_snapshots(version,digest,content,applied_at) VALUES(?,?,?,?)`, cfg.Version, digest, content, nowUTC()); err != nil {
			return errorWith(CodeInternal, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO pending_changes(kind,created_at) VALUES('config_change',?)`, nowUTC()); err != nil {
			return errorWith(CodeInternal, err)
		}
		return nil
	})
}

// ActiveConfig loads the latest immutable config snapshot.
func (s *Store) ActiveConfig(ctx context.Context) (Config, error) {
	var content []byte
	err := s.db.QueryRowContext(ctx, `SELECT content FROM config_snapshots ORDER BY version DESC LIMIT 1`).Scan(&content)
	if err == sql.ErrNoRows {
		return Config{}, &BusinessError{Code: CodeMissingArtifact, Stage: StageScore, Context: "no config snapshot", Message: "no config snapshot has been applied"}
	}
	if err != nil {
		return Config{}, errorWith(CodeInternal, err)
	}
	cfg, err := ParseConfig(content)
	if err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// UpsertAnnotation inserts an append or edits one existing pair.
func (s *Store) UpsertAnnotation(ctx context.Context, a Annotation) error {
	if err := validateAnnotation(a); err != nil {
		return err
	}
	raw, _ := json.Marshal(a)
	digest := digestBytes(raw)
	return s.withTx(ctx, func(tx *sql.Tx) error {
		var oldSource, oldDigest string
		err := tx.QueryRowContext(ctx, `SELECT source, content_digest FROM annotations WHERE pair_id=?`, a.PairID).Scan(&oldSource, &oldDigest)
		kind := "append"
		if err == nil {
			kind = "edit"
			if oldSource == a.Source && oldDigest == digest {
				return nil
			}
		} else if err != sql.ErrNoRows {
			return errorWith(CodeInternal, err)
		}
		if err == nil {
			if _, err := tx.ExecContext(ctx, `UPDATE annotations SET source=?,raw_json=?,content_digest=?,revision=revision+1,updated_at=? WHERE pair_id=?`, a.Source, raw, digest, nowUTC(), a.PairID); err != nil {
				return errorWith(CodeInternal, err)
			}
		} else {
			if _, err := tx.ExecContext(ctx, `INSERT INTO annotations(pair_id,source,raw_json,content_digest,revision,updated_at) VALUES(?,?,?,?,1,?)`, a.PairID, a.Source, raw, digest, nowUTC()); err != nil {
				return errorWith(CodeInternal, err)
			}
		}
		if err == nil && oldSource != a.Source {
			if err := refreshSourceTx(ctx, tx, oldSource); err != nil {
				return err
			}
		}
		if err := refreshSourceTx(ctx, tx, a.Source); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO pending_changes(kind,source,pair_id,old_digest,new_digest,created_at) VALUES(?,?,?,?,?,?)`, kind, a.Source, a.PairID, nullableString(oldDigest), digest, nowUTC()); err != nil {
			return errorWith(CodeInternal, err)
		}
		return nil
	})
}

// ReplaceSource atomically replaces one source shard. Pair artifacts from other
// sources remain untouched.
func (s *Store) ReplaceSource(ctx context.Context, source string, annotations []Annotation) error {
	if strings.TrimSpace(source) == "" {
		return &BusinessError{Code: CodeInvalidInput, Field: "source", Message: "source must not be empty"}
	}
	seen := map[string]bool{}
	copyAnn := make([]Annotation, len(annotations))
	for i, a := range annotations {
		if a.Source == "" {
			a.Source = source
		}
		if a.Source != source {
			return &BusinessError{Code: CodeInvalidInput, PairID: a.PairID, Field: "source", Expected: source, Actual: a.Source, Message: "all replacement annotations must belong to the replaced source"}
		}
		if err := validateAnnotation(a); err != nil {
			return err
		}
		if seen[a.PairID] {
			return &BusinessError{Code: CodeInvalidInput, PairID: a.PairID, Field: "pair_id", Message: "duplicate pair_id in replacement shard"}
		}
		seen[a.PairID] = true
		copyAnn[i] = a
	}
	sort.Slice(copyAnn, func(i, j int) bool { return copyAnn[i].PairID < copyAnn[j].PairID })
	return s.withTx(ctx, func(tx *sql.Tx) error {
		var oldDigest string
		err := tx.QueryRowContext(ctx, `SELECT digest FROM sources WHERE source=?`, source).Scan(&oldDigest)
		if err != nil && err != sql.ErrNoRows {
			return errorWith(CodeInternal, err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM annotations WHERE source=?`, source); err != nil {
			return errorWith(CodeInternal, err)
		}
		for _, a := range copyAnn {
			raw, _ := json.Marshal(a)
			digest := digestBytes(raw)
			if _, err := tx.ExecContext(ctx, `INSERT INTO annotations(pair_id,source,raw_json,content_digest,revision,updated_at) VALUES(?,?,?,?,1,?)`, a.PairID, source, raw, digest, nowUTC()); err != nil {
				return errorWith(CodeInternal, err)
			}
		}
		newDigest := sourceDigest(copyAnn)
		if _, err := tx.ExecContext(ctx, `INSERT INTO sources(source,revision,digest,pair_count,replaced_generation,updated_at) VALUES(?,1,?,?,1,?)
ON CONFLICT(source) DO UPDATE SET revision=revision+1,digest=excluded.digest,pair_count=excluded.pair_count,replaced_generation=replaced_generation+1,updated_at=excluded.updated_at`,
			source, newDigest, len(copyAnn), nowUTC()); err != nil {
			return errorWith(CodeInternal, err)
		}
		kind := "source_replace"
		if err == nil && oldDigest == newDigest {
			kind = "source_replace_identical"
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO pending_changes(kind,source,old_digest,new_digest,created_at) VALUES(?,?,?,?,?)`, kind, source, nullableString(oldDigest), newDigest, nowUTC()); err != nil {
			return errorWith(CodeInternal, err)
		}
		return nil
	})
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func refreshSourceTx(ctx context.Context, tx *sql.Tx, source string) error {
	rows, err := tx.QueryContext(ctx, `SELECT raw_json FROM annotations WHERE source=? ORDER BY pair_id`, source)
	if err != nil {
		return errorWith(CodeInternal, err)
	}
	defer rows.Close()
	var digests []string
	var annotations []Annotation
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return errorWith(CodeInternal, err)
		}
		digests = append(digests, digestBytes(raw))
		var a Annotation
		if err := json.Unmarshal(raw, &a); err != nil {
			return errorWith(CodeInternal, err)
		}
		annotations = append(annotations, a)
	}
	if err := rows.Err(); err != nil {
		return errorWith(CodeInternal, err)
	}
	digest := hashBytes([]byte("source\x00"), []byte(CanonicalString(digests)))
	if _, err := tx.ExecContext(ctx, `INSERT INTO sources(source,revision,digest,pair_count,replaced_generation,updated_at) VALUES(?,1,?,?,0,?)
ON CONFLICT(source) DO UPDATE SET revision=revision+1,digest=excluded.digest,pair_count=excluded.pair_count,updated_at=excluded.updated_at`,
		source, digest, len(annotations), nowUTC()); err != nil {
		return errorWith(CodeInternal, err)
	}
	return nil
}

func sourceDigest(annotations []Annotation) string {
	ordered := append([]Annotation(nil), annotations...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].PairID < ordered[j].PairID })
	digests := make([]string, 0, len(ordered))
	for _, a := range ordered {
		raw, _ := json.Marshal(a)
		digests = append(digests, a.PairID+":"+digestBytes(raw))
	}
	return hashBytes([]byte("source\x00"), []byte(CanonicalString(digests)))
}

// LoadAnnotations returns all current annotations sorted by pair_id.
func (s *Store) LoadAnnotations(ctx context.Context) ([]Annotation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT raw_json FROM annotations ORDER BY pair_id`)
	if err != nil {
		return nil, errorWith(CodeInternal, err)
	}
	defer rows.Close()
	var annotations []Annotation
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, errorWith(CodeInternal, err)
		}
		var a Annotation
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, errorWith(CodeInternal, err)
		}
		annotations = append(annotations, a)
	}
	return annotations, errorWith(CodeInternal, rows.Err())
}

// LoadSources returns current source metadata sorted by source.
func (s *Store) LoadSources(ctx context.Context) ([]SourceRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT source,revision,digest,pair_count,replaced_generation FROM sources ORDER BY source`)
	if err != nil {
		return nil, errorWith(CodeInternal, err)
	}
	defer rows.Close()
	var records []SourceRecord
	for rows.Next() {
		var record SourceRecord
		if err := rows.Scan(&record.Source, &record.Revision, &record.Digest, &record.PairCount, &record.ReplacedGeneration); err != nil {
			return nil, errorWith(CodeInternal, err)
		}
		records = append(records, record)
	}
	return records, errorWith(CodeInternal, rows.Err())
}

type SourceRecord struct {
	Source             string `json:"source"`
	Revision           int64  `json:"revision"`
	Digest             string `json:"digest"`
	PairCount          int    `json:"pair_count"`
	ReplacedGeneration int64  `json:"replaced_generation"`
}

type PendingChange struct {
	ID        int64  `json:"id"`
	Kind      string `json:"kind"`
	Source    string `json:"source,omitempty"`
	PairID    string `json:"pair_id,omitempty"`
	OldDigest string `json:"old_digest,omitempty"`
	NewDigest string `json:"new_digest,omitempty"`
}

func (s *Store) PendingChanges(ctx context.Context) ([]PendingChange, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,kind,COALESCE(source,''),COALESCE(pair_id,''),COALESCE(old_digest,''),COALESCE(new_digest,'') FROM pending_changes ORDER BY id`)
	if err != nil {
		return nil, errorWith(CodeInternal, err)
	}
	defer rows.Close()
	var result []PendingChange
	for rows.Next() {
		var change PendingChange
		if err := rows.Scan(&change.ID, &change.Kind, &change.Source, &change.PairID, &change.OldDigest, &change.NewDigest); err != nil {
			return nil, errorWith(CodeInternal, err)
		}
		result = append(result, change)
	}
	return result, errorWith(CodeInternal, rows.Err())
}

func consumePendingChangesTx(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM pending_changes`)
	return errorWith(CodeInternal, err)
}

// Manifest is the committed input snapshot from the last complete build.
type Manifest struct {
	ConfigVersion       int                 `json:"config_version"`
	ConfigDigest        string              `json:"config_digest"`
	NormalizationDigest string              `json:"normalization_digest"`
	ScoringDigest       string              `json:"scoring_digest"`
	FeaturesDigest      string              `json:"features_digest"`
	BudgetDigest        string              `json:"budget_digest"`
	PairDigests         map[string]string   `json:"pair_digests"`
	PairSources         map[string]string   `json:"pair_sources"`
	SourceDigests       map[string]string   `json:"source_digests"`
	SourceGenerations   map[string]int64    `json:"source_generations"`
	SourcePairs         map[string][]string `json:"source_pairs"`
}

func (m Manifest) Digest() string {
	// Replacement generation counters are diagnostic metadata, not semantic
	// content. Identical whole-shard replacements must remain a no-op.
	semantic := m
	semantic.SourceGenerations = nil
	return digestJSON(semantic)
}

func newManifest(cfg Config, annotations []Annotation, sources []SourceRecord) (Manifest, error) {
	m := Manifest{
		ConfigVersion:       cfg.Version,
		ConfigDigest:        cfg.Digest(),
		NormalizationDigest: cfg.NormalizationDigest(),
		ScoringDigest:       cfg.ScoringDigest(),
		FeaturesDigest:      cfg.FeaturesDigest(),
		BudgetDigest:        cfg.BudgetDigest(),
		PairDigests:         map[string]string{},
		PairSources:         map[string]string{},
		SourceDigests:       map[string]string{},
		SourceGenerations:   map[string]int64{},
		SourcePairs:         map[string][]string{},
	}
	for _, a := range annotations {
		raw, err := json.Marshal(a)
		if err != nil {
			return Manifest{}, &BusinessError{Code: CodeNumeric, PairID: a.PairID, Factor: factorConsistency, Message: err.Error()}
		}
		m.PairDigests[a.PairID] = digestBytes(raw)
		m.PairSources[a.PairID] = a.Source
		m.SourcePairs[a.Source] = append(m.SourcePairs[a.Source], a.PairID)
	}
	for _, source := range m.SourcePairs {
		sort.Strings(source)
	}
	for _, source := range sources {
		m.SourceDigests[source.Source] = source.Digest
		m.SourceGenerations[source.Source] = source.ReplacedGeneration
	}
	return m, nil
}

func (s *Store) CurrentManifest(ctx context.Context) (Manifest, bool, error) {
	var payload []byte
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='build_manifest'`).Scan(&payload)
	if err == sql.ErrNoRows {
		return Manifest{}, false, nil
	}
	if err != nil {
		return Manifest{}, false, errorWith(CodeInternal, err)
	}
	var m Manifest
	if err := json.Unmarshal(payload, &m); err != nil {
		return Manifest{}, false, errorWith(CodeInternal, err)
	}
	return m, true, nil
}

// ManifestWasCommitted reports whether an identical manifest was successfully
// published by an earlier build. It is used to distinguish content rollback
// from an ordinary annotation edit.
func (s *Store) ManifestWasCommitted(ctx context.Context, digest string) (bool, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM manifest_history WHERE digest=?`, digest).Scan(&count); err != nil {
		return false, errorWith(CodeInternal, err)
	}
	return count > 0, nil
}

func saveManifestTx(ctx context.Context, tx *sql.Tx, m Manifest) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return errorWith(CodeInternal, err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES('build_manifest',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, payload)
	if err != nil {
		return errorWith(CodeInternal, err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO manifest_history(digest,manifest,config_version,created_at) VALUES(?,?,?,?) ON CONFLICT(digest) DO NOTHING`,
		m.Digest(), payload, m.ConfigVersion, nowUTC())
	return errorWith(CodeInternal, err)
}

// Invalidation records the minimum rebuild scope and first differing pair.
type Invalidation struct {
	Fingerprint            string   `json:"fingerprint"`
	ChangeKind             string   `json:"change_kind"`
	FirstDiffPairID        string   `json:"first_diff_pair_id,omitempty"`
	AffectedSources        []string `json:"affected_sources"`
	EarliestStage          string   `json:"earliest_stage"`
	InvalidatedStages      []string `json:"invalidated_stages"`
	AffectedPairs          []string `json:"affected_pairs"`
	AllPairs               bool     `json:"all_pairs"`
	PreviousManifestDigest string   `json:"previous_manifest_digest"`
	CurrentManifestDigest  string   `json:"current_manifest_digest"`
	PreviousConfigVersion  int      `json:"previous_config_version"`
	CurrentConfigVersion   int      `json:"current_config_version"`
	Reason                 string   `json:"reason"`
}

func (s *Store) recordInvalidation(ctx context.Context, inv Invalidation) error {
	affectedSources, _ := json.Marshal(inv.AffectedSources)
	stages, _ := json.Marshal(inv.InvalidatedStages)
	pairs, _ := json.Marshal(inv.AffectedPairs)
	err := execRetry(ctx, s.db.ExecContext, `INSERT OR IGNORE INTO invalidations(
fingerprint,change_kind,first_diff_pair_id,affected_sources,earliest_stage,invalidated_stages,affected_pairs,all_pairs,
previous_manifest_digest,current_manifest_digest,previous_config_version,current_config_version,reason,created_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		inv.Fingerprint, inv.ChangeKind, inv.FirstDiffPairID, affectedSources, inv.EarliestStage, stages, pairs,
		boolInt(inv.AllPairs),
		inv.PreviousManifestDigest, inv.CurrentManifestDigest, inv.PreviousConfigVersion, inv.CurrentConfigVersion, inv.Reason, nowUTC())
	return errorWith(CodeInternal, err)
}

func (s *Store) Invalidations(ctx context.Context) ([]Invalidation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT fingerprint,change_kind,COALESCE(first_diff_pair_id,''),affected_sources,earliest_stage,
invalidated_stages,affected_pairs,all_pairs,previous_manifest_digest,current_manifest_digest,previous_config_version,current_config_version,reason
FROM invalidations ORDER BY id`)
	if err != nil {
		return nil, errorWith(CodeInternal, err)
	}
	defer rows.Close()
	var result []Invalidation
	for rows.Next() {
		var inv Invalidation
		var sourcesJSON, stagesJSON, pairsJSON []byte
		var allPairs int
		if err := rows.Scan(&inv.Fingerprint, &inv.ChangeKind, &inv.FirstDiffPairID, &sourcesJSON, &inv.EarliestStage,
			&stagesJSON, &pairsJSON, &allPairs, &inv.PreviousManifestDigest, &inv.CurrentManifestDigest,
			&inv.PreviousConfigVersion, &inv.CurrentConfigVersion, &inv.Reason); err != nil {
			return nil, errorWith(CodeInternal, err)
		}
		_ = json.Unmarshal(sourcesJSON, &inv.AffectedSources)
		_ = json.Unmarshal(stagesJSON, &inv.InvalidatedStages)
		_ = json.Unmarshal(pairsJSON, &inv.AffectedPairs)
		inv.AllPairs = allPairs != 0
		result = append(result, inv)
	}
	return result, errorWith(CodeInternal, rows.Err())
}
