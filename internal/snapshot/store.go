package snapshot

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	_ "modernc.org/sqlite"

	"svcworkbench/internal/domain"
)

const chainID = "service-control-adjudication-chain-v1"

type Config struct {
	SchemaVersion  string
	Marker         string
	TargetFormat   string
	SourceVersions map[string]string
	Priorities     []string
}

type SnapshotMeta struct {
	Version        int               `json:"version"`
	ChainID        string            `json:"chain_id"`
	ParentHash     string            `json:"parent_hash"`
	ContentHash    string            `json:"content_hash"`
	ConfigHash     string            `json:"config_hash"`
	Marker         string            `json:"marker"`
	TargetFormat   string            `json:"target_format"`
	SourceVersions map[string]string `json:"source_versions"`
	Priorities     []string          `json:"priorities"`
	Payload        []byte            `json:"-"`
}

type ChainState struct {
	ChainID          string `json:"chain_id"`
	Versions         []int  `json:"versions"`
	ActiveVersion    int    `json:"active_version"`
	HeadVersion      int    `json:"head_version"`
	FallbackVersion  int    `json:"fallback_version,omitempty"`
	HeadHash         string `json:"head_hash,omitempty"`
	ConfigCompatible bool   `json:"config_compatible"`
	Detail           string `json:"detail,omitempty"`
}

type LoadResult struct {
	Meta    SnapshotMeta
	Payload []byte
	Chain   ChainState
}

type SnapshotError struct {
	Code   domain.Code
	Chain  ChainState
	Detail string
}

func (e *SnapshotError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Detail)
}

type Progress struct {
	SourceID      string
	Fingerprint   string
	Kind          string
	FormatVersion string
	Accepted      int
	Status        string
}

type ProgressRow struct {
	SourceID        string `json:"source_id"`
	Fingerprint     string `json:"fingerprint"`
	Kind            string `json:"kind"`
	FormatVersion   string `json:"format_version"`
	Accepted        int    `json:"accepted"`
	Status          string `json:"status"`
	SnapshotVersion int    `json:"snapshot_version"`
}

type Comparison struct {
	SameVersion      bool `json:"same_version"`
	SameChain        bool `json:"same_chain"`
	SameConfig       bool `json:"same_config"`
	SameContent      bool `json:"same_content"`
	WholeReplacement bool `json:"whole_replacement"`
}

type Store struct {
	db            *sql.DB
	mu            sync.Mutex
	schemaWarning string
}

func Open(path string) (*Store, error) {
	if path != "" {
		dir := filepath.Dir(path)
		if dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, err
		}
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS snapshot_meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  chain_id TEXT NOT NULL,
  tip_version INTEGER NOT NULL,
  tip_hash TEXT NOT NULL,
  schema_version TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots (
  version INTEGER PRIMARY KEY,
  chain_id TEXT NOT NULL,
  parent_hash TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  config_hash TEXT NOT NULL,
  marker TEXT NOT NULL,
  target_format TEXT NOT NULL,
  source_versions TEXT NOT NULL,
  priorities TEXT NOT NULL,
  payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS source_progress (
  source_id TEXT PRIMARY KEY,
  fingerprint TEXT NOT NULL,
  kind TEXT NOT NULL,
  format_version TEXT NOT NULL,
  accepted INTEGER NOT NULL,
  status TEXT NOT NULL,
  snapshot_version INTEGER NOT NULL
);
`); err != nil {
		db.Close()
		return nil, err
	}
	store := &Store{db: db}
	if _, err := db.Exec(`INSERT OR IGNORE INTO snapshot_meta (id, chain_id, tip_version, tip_hash, schema_version)
VALUES (1, '` + chainID + `', 0, '` + domain.GenesisHash + `', '` + domain.SnapshotSchemaVersion + `')`); err != nil {
		store.schemaWarning = "snapshot metadata schema is incompatible with the current workbench"
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Load(ctx context.Context, requestedVersion int, config Config) (*LoadResult, ChainState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.schemaWarning != "" {
		state := ChainState{ChainID: chainID, Versions: []int{}, ConfigCompatible: false, Detail: s.schemaWarning}
		return nil, state, snapshotError(state, s.schemaWarning)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, ChainState{}, err
	}
	defer tx.Rollback()
	return s.loadTx(ctx, tx, requestedVersion, config)
}

func (s *Store) Commit(ctx context.Context, config Config, payload []byte, progress []Progress, hook func(string)) (SnapshotMeta, ChainState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.schemaWarning != "" {
		state := ChainState{ChainID: chainID, Versions: []int{}, ConfigCompatible: false, Detail: s.schemaWarning}
		return SnapshotMeta{}, state, snapshotError(state, s.schemaWarning)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return SnapshotMeta{}, ChainState{}, err
	}
	defer tx.Rollback()
	load, state, err := s.loadTx(ctx, tx, 0, config)
	if err != nil {
		return SnapshotMeta{}, state, err
	}
	headVersion := 0
	parentHash := domain.GenesisHash
	if load != nil {
		headVersion = load.Meta.Version
		parentHash = load.Meta.ContentHash
	}
	version := headVersion + 1
	sourceVersionsJSON, prioritiesJSON, configHash, err := canonicalConfig(config)
	if err != nil {
		return SnapshotMeta{}, state, err
	}
	contentHash := computeContentHash(version, parentHash, config.SchemaVersion, config.Marker, config.TargetFormat, configHash, payload)
	meta := SnapshotMeta{
		Version:        version,
		ChainID:        chainID,
		ParentHash:     parentHash,
		ContentHash:    contentHash,
		ConfigHash:     configHash,
		Marker:         config.Marker,
		TargetFormat:   config.TargetFormat,
		SourceVersions: cloneMap(config.SourceVersions),
		Priorities:     append([]string(nil), config.Priorities...),
		Payload:        append([]byte(nil), payload...),
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO snapshots
  (version, chain_id, parent_hash, content_hash, config_hash, marker, target_format, source_versions, priorities, payload)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		meta.Version, meta.ChainID, meta.ParentHash, meta.ContentHash, meta.ConfigHash, meta.Marker, meta.TargetFormat,
		sourceVersionsJSON, prioritiesJSON, meta.Payload,
	); err != nil {
		state.Detail = "snapshot write schema is incompatible with the current workbench"
		return SnapshotMeta{}, state, snapshotError(state, state.Detail)
	}
	for _, item := range progress {
		status := item.Status
		if status == "" {
			status = "committed"
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_progress (source_id, fingerprint, kind, format_version, accepted, status, snapshot_version)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(source_id) DO UPDATE SET
  fingerprint=excluded.fingerprint,
  kind=excluded.kind,
  format_version=excluded.format_version,
  accepted=excluded.accepted,
  status=excluded.status,
  snapshot_version=excluded.snapshot_version`,
			item.SourceID, item.Fingerprint, item.Kind, item.FormatVersion, item.Accepted, status, meta.Version,
		); err != nil {
			state.Detail = "source progress schema is incompatible with the current workbench"
			return SnapshotMeta{}, state, snapshotError(state, state.Detail)
		}
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE snapshot_meta SET chain_id=?, tip_version=?, tip_hash=?, schema_version=? WHERE id=1`,
		meta.ChainID, meta.Version, meta.ContentHash, config.SchemaVersion,
	); err != nil {
		state.Detail = "snapshot metadata write schema is incompatible with the current workbench"
		return SnapshotMeta{}, state, snapshotError(state, state.Detail)
	}
	if hook != nil {
		hook("snapshot-tx")
	}
	if err := tx.Commit(); err != nil {
		return SnapshotMeta{}, state, err
	}
	versions := append([]int(nil), state.Versions...)
	versions = append(versions, meta.Version)
	state = ChainState{
		ChainID:          chainID,
		Versions:         versions,
		ActiveVersion:    meta.Version,
		HeadVersion:      meta.Version,
		HeadHash:         meta.ContentHash,
		ConfigCompatible: true,
	}
	return meta, state, nil
}

func (s *Store) loadTx(ctx context.Context, tx *sql.Tx, requestedVersion int, config Config) (*LoadResult, ChainState, error) {
	state := ChainState{ChainID: chainID, Versions: []int{}, ConfigCompatible: true}
	var metaChain, metaHash, metaSchema string
	var metaTip int
	metaErr := tx.QueryRowContext(ctx, `SELECT chain_id, tip_version, tip_hash, schema_version FROM snapshot_meta WHERE id=1`).
		Scan(&metaChain, &metaTip, &metaHash, &metaSchema)
	if metaErr != nil && !errors.Is(metaErr, sql.ErrNoRows) {
		if ctx.Err() != nil {
			return nil, state, metaErr
		}
		state.Detail = "snapshot metadata cannot be read by the current schema"
		state.ConfigCompatible = false
		return nil, state, snapshotError(state, state.Detail)
	}
	state.ChainID = metaChain
	if state.ChainID == "" {
		state.ChainID = chainID
	}
	state.HeadVersion = metaTip
	state.HeadHash = metaHash
	rows, err := tx.QueryContext(ctx, `
SELECT version, chain_id, parent_hash, content_hash, config_hash, marker, target_format, source_versions, priorities, payload
FROM snapshots ORDER BY version ASC`)
	if err != nil {
		if ctx.Err() != nil {
			return nil, state, err
		}
		state.Detail = "snapshot table cannot be read by the current schema"
		state.ConfigCompatible = false
		return nil, state, snapshotError(state, state.Detail)
	}
	defer rows.Close()
	allRows := make([]SnapshotMeta, 0)
	for rows.Next() {
		var item SnapshotMeta
		var sourceVersionsJSON, prioritiesJSON string
		if err := rows.Scan(
			&item.Version, &item.ChainID, &item.ParentHash, &item.ContentHash, &item.ConfigHash,
			&item.Marker, &item.TargetFormat, &sourceVersionsJSON, &prioritiesJSON, &item.Payload,
		); err != nil {
			state.Detail = "snapshot row cannot be decoded by the current schema"
			state.ConfigCompatible = false
			return nil, state, snapshotError(state, state.Detail)
		}
		if err := json.Unmarshal([]byte(sourceVersionsJSON), &item.SourceVersions); err != nil {
			return nil, state, snapshotError(state, fmt.Sprintf("snapshot %d has invalid source version JSON", item.Version))
		}
		if err := json.Unmarshal([]byte(prioritiesJSON), &item.Priorities); err != nil {
			return nil, state, snapshotError(state, fmt.Sprintf("snapshot %d has invalid priority JSON", item.Version))
		}
		allRows = append(allRows, item)
		state.Versions = append(state.Versions, item.Version)
	}
	if err := rows.Err(); err != nil {
		if ctx.Err() != nil {
			return nil, state, err
		}
		state.Detail = "snapshot table iteration failed"
		state.ConfigCompatible = false
		return nil, state, snapshotError(state, state.Detail)
	}
	if len(allRows) == 0 {
		if metaTip != 0 || (metaHash != "" && metaHash != domain.GenesisHash) {
			return nil, state, snapshotError(state, "snapshot metadata references a missing chain")
		}
		if requestedVersion > 0 {
			state.Detail = fmt.Sprintf("snapshot %d does not exist", requestedVersion)
			return nil, state, snapshotError(state, state.Detail)
		}
		return nil, state, nil
	}
	if errors.Is(metaErr, sql.ErrNoRows) {
		state.Detail = "snapshot table exists without committed chain metadata"
		state.ConfigCompatible = false
		return nil, state, snapshotError(state, state.Detail)
	}
	if metaSchema != config.SchemaVersion {
		state.Detail = fmt.Sprintf("snapshot schema %q is incompatible with %q", metaSchema, config.SchemaVersion)
		state.ConfigCompatible = false
		return nil, state, snapshotError(state, state.Detail)
	}
	previousHash := domain.GenesisHash
	validRows := make([]SnapshotMeta, 0, len(allRows))
	structuralDetail := ""
	for index, item := range allRows {
		expectedVersion := index + 1
		if item.Version != expectedVersion {
			structuralDetail = fmt.Sprintf("snapshot version gap: expected %d, found %d", expectedVersion, item.Version)
			break
		}
		if item.ChainID != state.ChainID {
			structuralDetail = fmt.Sprintf("snapshot %d chain id mismatch", item.Version)
			break
		}
		if item.ParentHash != previousHash {
			structuralDetail = fmt.Sprintf("snapshot %d parent hash mismatch", item.Version)
			break
		}
		sourceVersionsJSON, prioritiesJSON, expectedConfigHash, err := canonicalConfig(Config{
			SchemaVersion:  config.SchemaVersion,
			Marker:         item.Marker,
			TargetFormat:   item.TargetFormat,
			SourceVersions: item.SourceVersions,
			Priorities:     item.Priorities,
		})
		if err != nil {
			structuralDetail = fmt.Sprintf("snapshot %d config cannot be canonicalized", item.Version)
			break
		}
		_ = sourceVersionsJSON
		_ = prioritiesJSON
		if item.ConfigHash != expectedConfigHash {
			structuralDetail = fmt.Sprintf("snapshot %d config hash mismatch", item.Version)
			break
		}
		expectedContentHash := computeContentHash(item.Version, item.ParentHash, config.SchemaVersion, item.Marker, item.TargetFormat, item.ConfigHash, item.Payload)
		if item.ContentHash != expectedContentHash {
			structuralDetail = fmt.Sprintf("snapshot %d content hash mismatch or replacement detected", item.Version)
			break
		}
		validRows = append(validRows, item)
		previousHash = item.ContentHash
	}
	if structuralDetail == "" {
		head := allRows[len(allRows)-1]
		if metaTip != head.Version || metaHash != head.ContentHash {
			structuralDetail = "snapshot table tip does not match committed chain metadata"
			fallback := highestCompatibleBefore(validRows, config, metaTip)
			for _, item := range allRows {
				if item.Version == metaTip && item.ContentHash == metaHash && configCompatible(item, config) {
					fallback = metaTip
					break
				}
			}
			state.ActiveVersion = fallback
			state.FallbackVersion = fallback
			state.ConfigCompatible = false
			state.Detail = structuralDetail
			return nil, state, snapshotError(state, structuralDetail)
		}
	}
	if structuralDetail != "" {
		state.ActiveVersion = highestCompatible(validRows, config)
		state.FallbackVersion = state.ActiveVersion
		state.ConfigCompatible = false
		state.Detail = structuralDetail
		return nil, state, snapshotError(state, structuralDetail)
	}
	if requestedVersion < 0 {
		state.Detail = "snapshot version must be non-negative"
		return nil, state, snapshotError(state, state.Detail)
	}
	targetVersion := requestedVersion
	if targetVersion == 0 {
		targetVersion = state.Versions[len(state.Versions)-1]
	}
	var selected *SnapshotMeta
	for index := range allRows {
		if allRows[index].Version == targetVersion {
			copy := allRows[index]
			selected = &copy
			break
		}
	}
	if selected == nil {
		state.ActiveVersion = state.Versions[len(state.Versions)-1]
		state.FallbackVersion = state.ActiveVersion
		state.ConfigCompatible = false
		state.Detail = fmt.Sprintf("snapshot %d does not exist", targetVersion)
		return nil, state, snapshotError(state, state.Detail)
	}
	if !configCompatible(*selected, config) {
		state.ActiveVersion = 0
		state.ConfigCompatible = false
		state.Detail = fmt.Sprintf("snapshot %d format versions or source priorities are incompatible", selected.Version)
		return nil, state, snapshotError(state, state.Detail)
	}
	state.ActiveVersion = selected.Version
	state.HeadVersion = allRows[len(allRows)-1].Version
	state.HeadHash = allRows[len(allRows)-1].ContentHash
	state.ConfigCompatible = true
	return &LoadResult{Meta: *selected, Payload: append([]byte(nil), selected.Payload...), Chain: state}, state, nil
}

func (s *Store) Progress(ctx context.Context) ([]ProgressRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `
SELECT source_id, fingerprint, kind, format_version, accepted, status, snapshot_version
FROM source_progress ORDER BY source_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ProgressRow, 0)
	for rows.Next() {
		var row ProgressRow
		if err := rows.Scan(&row.SourceID, &row.Fingerprint, &row.Kind, &row.FormatVersion, &row.Accepted, &row.Status, &row.SnapshotVersion); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) InjectSnapshotGap(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var maxVersion int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM snapshots`).Scan(&maxVersion); err != nil {
		return err
	}
	if maxVersion == 0 {
		return errors.New("snapshot gap injection requires a committed baseline snapshot")
	}
	var row SnapshotMeta
	var sourceVersionsJSON, prioritiesJSON string
	if err := tx.QueryRowContext(ctx, `
SELECT version, chain_id, parent_hash, content_hash, config_hash, marker, target_format, source_versions, priorities, payload
FROM snapshots WHERE version=?`, maxVersion).Scan(
		&row.Version, &row.ChainID, &row.ParentHash, &row.ContentHash, &row.ConfigHash,
		&row.Marker, &row.TargetFormat, &sourceVersionsJSON, &prioritiesJSON, &row.Payload,
	); err != nil {
		return err
	}
	version := maxVersion + 2
	parentHash := row.ContentHash
	contentHash := computeContentHash(version, parentHash, domain.SnapshotSchemaVersion, row.Marker, row.TargetFormat, row.ConfigHash, row.Payload)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO snapshots
  (version, chain_id, parent_hash, content_hash, config_hash, marker, target_format, source_versions, priorities, payload)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		version, row.ChainID, parentHash, contentHash, row.ConfigHash, row.Marker, row.TargetFormat, sourceVersionsJSON, prioritiesJSON, row.Payload,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE snapshot_meta SET tip_version=?, tip_hash=? WHERE id=1`, version, contentHash); err != nil {
		return err
	}
	return tx.Commit()
}

func canonicalConfig(config Config) (string, string, string, error) {
	sourceVersionsJSON, err := json.Marshal(config.SourceVersions)
	if err != nil {
		return "", "", "", err
	}
	prioritiesJSON, err := json.Marshal(config.Priorities)
	if err != nil {
		return "", "", "", err
	}
	hashInput := strings.Join([]string{
		config.SchemaVersion,
		config.Marker,
		config.TargetFormat,
		string(sourceVersionsJSON),
		string(prioritiesJSON),
	}, "\n")
	sum := sha256.Sum256([]byte(hashInput))
	return string(sourceVersionsJSON), string(prioritiesJSON), hex.EncodeToString(sum[:]), nil
}

func computeContentHash(version int, parentHash, schemaVersion, marker, targetFormat, configHash string, payload []byte) string {
	hash := sha256.New()
	hash.Write([]byte(strconv.Itoa(version)))
	hash.Write([]byte{'\n'})
	hash.Write([]byte(parentHash))
	hash.Write([]byte{'\n'})
	hash.Write([]byte(schemaVersion))
	hash.Write([]byte{'\n'})
	hash.Write([]byte(marker))
	hash.Write([]byte{'\n'})
	hash.Write([]byte(targetFormat))
	hash.Write([]byte{'\n'})
	hash.Write([]byte(configHash))
	hash.Write([]byte{'\n'})
	hash.Write(payload)
	return hex.EncodeToString(hash.Sum(nil))
}

func configCompatible(meta SnapshotMeta, config Config) bool {
	if meta.Marker != config.Marker || meta.TargetFormat != config.TargetFormat || len(meta.SourceVersions) != len(config.SourceVersions) || len(meta.Priorities) != len(config.Priorities) {
		return false
	}
	for key, value := range config.SourceVersions {
		if meta.SourceVersions[key] != value {
			return false
		}
	}
	for index := range config.Priorities {
		if meta.Priorities[index] != config.Priorities[index] {
			return false
		}
	}
	_, _, expectedConfigHash, err := canonicalConfig(Config{
		SchemaVersion:  domain.SnapshotSchemaVersion,
		Marker:         meta.Marker,
		TargetFormat:   meta.TargetFormat,
		SourceVersions: meta.SourceVersions,
		Priorities:     meta.Priorities,
	})
	return err == nil && meta.ConfigHash == expectedConfigHash
}

func highestCompatible(rows []SnapshotMeta, config Config) int {
	for index := len(rows) - 1; index >= 0; index-- {
		if configCompatible(rows[index], config) {
			return rows[index].Version
		}
	}
	return 0
}

func highestCompatibleBefore(rows []SnapshotMeta, config Config, version int) int {
	for index := len(rows) - 1; index >= 0; index-- {
		if rows[index].Version < version && configCompatible(rows[index], config) {
			return rows[index].Version
		}
	}
	return 0
}

func snapshotError(state ChainState, detail string) error {
	return &SnapshotError{Code: domain.CodeSnapshotVersion, Chain: state, Detail: detail}
}

func Compare(left, right SnapshotMeta) Comparison {
	comparison := Comparison{
		SameVersion: left.Version == right.Version,
		SameChain:   left.ChainID == right.ChainID,
		SameConfig:  left.ConfigHash == right.ConfigHash,
		SameContent: left.ContentHash == right.ContentHash,
	}
	comparison.WholeReplacement = !comparison.SameChain || (comparison.SameVersion && !comparison.SameContent)
	return comparison
}

func cloneMap(source map[string]string) map[string]string {
	out := make(map[string]string, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
}

func SortedVersions(versions []int) []int {
	out := make([]int, len(versions))
	copy(out, versions)
	sort.Ints(out)
	return out
}
