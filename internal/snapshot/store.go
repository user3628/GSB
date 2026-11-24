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
	"strings"

	_ "modernc.org/sqlite"

	"supplyaudit/internal/model"
)

const (
	SchemaVersion = 1
	Marker        = "deterministic-local-sample"
)

type Meta struct {
	Version           int      `json:"version"`
	ParentVersion     int      `json:"parent_version"`
	SchemaVersion     int      `json:"schema_version"`
	ConfigFingerprint string   `json:"config_fingerprint"`
	ContentHash       string   `json:"content_hash"`
	PreviousHash      string   `json:"previous_hash"`
	SnapshotHash      string   `json:"snapshot_hash"`
	RequiredFormats   []string `json:"required_formats"`
	Priority          []string `json:"priority"`
	Marker            string   `json:"marker"`
}

type Snapshot struct {
	Meta
	Payload []byte `json:"-"`
}

type Compatibility struct {
	SchemaVersion   int      `json:"schema_version"`
	AcceptedFormats []string `json:"accepted_formats"`
	Priority        []string `json:"priority"`
}

type storedConfig struct {
	SchemaVersion   int      `json:"schema_version"`
	RequiredFormats []string `json:"required_formats"`
	Priority        []string `json:"priority"`
	AcceptedFormats []string `json:"accepted_formats"`
}

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, model.NewCodeError(model.CodeInvalidConfig, "snapshot database path is empty", 0, 0)
	}
	if directory := filepath.Dir(path); directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	store := &Store{db: db}
	pragmas := []string{
		"PRAGMA busy_timeout=5000",
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
	}
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS snapshots (
		version INTEGER PRIMARY KEY,
		parent_version INTEGER NOT NULL,
		schema_version INTEGER NOT NULL,
		config_json TEXT NOT NULL,
		config_fingerprint TEXT NOT NULL,
		content_hash TEXT NOT NULL,
		previous_hash TEXT NOT NULL,
		snapshot_hash TEXT NOT NULL,
		required_formats TEXT NOT NULL,
		priority TEXT NOT NULL,
		marker TEXT NOT NULL,
		payload BLOB NOT NULL
	)`); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Append(ctx context.Context, payload []byte, requiredFormats, priority, acceptedFormats []string) (Meta, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Meta{}, err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := loadRows(ctx, tx)
	if err != nil {
		return Meta{}, err
	}
	if err := validateRows(rows); err != nil {
		return Meta{}, err
	}
	nextVersion := 1
	previousHash := ""
	if len(rows) > 0 {
		theLast := rows[len(rows)-1]
		nextVersion = theLast.Meta.Version + 1
		previousHash = theLast.Meta.SnapshotHash
	}
	meta, configJSON, err := buildMeta(nextVersion, nextVersion-1, previousHash, payload, requiredFormats, priority, acceptedFormats)
	if err != nil {
		return Meta{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots
		(version, parent_version, schema_version, config_json, config_fingerprint, content_hash, previous_hash, snapshot_hash, required_formats, priority, marker, payload)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		meta.Version, meta.ParentVersion, meta.SchemaVersion, configJSON, meta.ConfigFingerprint,
		meta.ContentHash, meta.PreviousHash, meta.SnapshotHash, joinJSON(meta.RequiredFormats),
		joinJSON(meta.Priority), meta.Marker, payload,
	); err != nil {
		return Meta{}, err
	}
	if err := tx.Commit(); err != nil {
		return Meta{}, err
	}
	return meta, nil
}

func (s *Store) Chain(ctx context.Context) ([]Meta, error) {
	rows, err := loadRows(ctx, s.db)
	if err != nil {
		return nil, err
	}
	if err := validateRows(rows); err != nil {
		return nil, err
	}
	metas := make([]Meta, len(rows))
	for index := range rows {
		metas[index] = rows[index].Meta
	}
	return metas, nil
}

func (s *Store) LatestVersion(ctx context.Context) (int, error) {
	var version int
	err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM snapshots").Scan(&version)
	return version, err
}

func (s *Store) Load(ctx context.Context, version int, compatibility Compatibility) (Snapshot, error) {
	if version <= 0 {
		return Snapshot{}, model.NewCodeError(model.CodeSnapshotVersion, "snapshot version must be positive", 0, 0)
	}
	rows, err := loadRows(ctx, s.db)
	if err != nil {
		return Snapshot{}, err
	}
	if len(rows) == 0 {
		return Snapshot{}, model.NewCodeError(model.CodeSnapshotVersion, "snapshot database is empty", 0, 0)
	}
	if err := validateRows(rows); err != nil {
		return Snapshot{}, err
	}
	var selected *row
	for index := range rows {
		if rows[index].Meta.Version == version {
			selected = &rows[index]
			break
		}
	}
	if selected == nil {
		return Snapshot{}, model.NewCodeError(model.CodeSnapshotVersion, fmt.Sprintf("snapshot version %d is missing from the chain", version), 0, 0)
	}
	if compatibility.SchemaVersion != 0 && selected.Meta.SchemaVersion != compatibility.SchemaVersion {
		return Snapshot{}, model.NewCodeError(model.CodeSnapshotVersion, fmt.Sprintf("snapshot schema %d is incompatible with current schema %d", selected.Meta.SchemaVersion, compatibility.SchemaVersion), 0, 0)
	}
	if !equalStrings(selected.Meta.Priority, compatibility.Priority) {
		return Snapshot{}, model.NewCodeError(model.CodeSnapshotVersion, "snapshot source priority is incompatible with current priority", 0, 0)
	}
	accepted := make(map[string]struct{}, len(compatibility.AcceptedFormats))
	for _, format := range compatibility.AcceptedFormats {
		accepted[format] = struct{}{}
	}
	for _, required := range selected.Meta.RequiredFormats {
		if _, ok := accepted[required]; !ok {
			return Snapshot{}, model.NewCodeError(model.CodeSnapshotVersion, fmt.Sprintf("snapshot requires unsupported format %s", required), 0, 0)
		}
	}
	return Snapshot{Meta: selected.Meta, Payload: selected.Payload}, nil
}

// AppendGapFixture constructs an intentionally discontinuous fixture. It is
// used only by the verification subcommand to prove that gap detection rejects
// a chain; production append never calls it.
func (s *Store) AppendGapFixture(ctx context.Context, payload []byte, requiredFormats, priority, acceptedFormats []string) (Meta, error) {
	base, err := s.Append(ctx, payload, requiredFormats, priority, acceptedFormats)
	if err != nil {
		return Meta{}, err
	}
	if base.Version != 1 {
		return Meta{}, model.NewCodeError(model.CodeSnapshotVersion, "gap fixture requires an empty snapshot store", 0, 0)
	}
	gapVersion := 3
	meta, configJSON, err := buildMeta(gapVersion, 2, "missing-version-2", payload, requiredFormats, priority, acceptedFormats)
	if err != nil {
		return Meta{}, err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO snapshots
		(version, parent_version, schema_version, config_json, config_fingerprint, content_hash, previous_hash, snapshot_hash, required_formats, priority, marker, payload)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		meta.Version, meta.ParentVersion, meta.SchemaVersion, configJSON, meta.ConfigFingerprint,
		meta.ContentHash, meta.PreviousHash, meta.SnapshotHash, joinJSON(meta.RequiredFormats),
		joinJSON(meta.Priority), meta.Marker, payload,
	)
	if err != nil {
		return Meta{}, err
	}
	return meta, nil
}

// ReplacePayloadFixture mutates a stored payload without updating hashes. It is
// used by the verification subcommand to prove that whole-row replacement is
// detected rather than accepted because the version number still matches.
func (s *Store) ReplacePayloadFixture(ctx context.Context, version int, payload []byte) error {
	result, err := s.db.ExecContext(ctx, "UPDATE snapshots SET payload = ? WHERE version = ?", payload, version)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return model.NewCodeError(model.CodeSnapshotVersion, fmt.Sprintf("snapshot version %d is missing", version), 0, 0)
	}
	return nil
}

type row struct {
	Meta       Meta
	ConfigJSON string
	Payload    []byte
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func loadRows(ctx context.Context, db queryer) ([]row, error) {
	rows, err := db.QueryContext(ctx, `SELECT version, parent_version, schema_version, config_json, config_fingerprint,
		content_hash, previous_hash, snapshot_hash, required_formats, priority, marker, payload
		FROM snapshots ORDER BY version ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]row, 0)
	for rows.Next() {
		var current row
		var requiredJSON, priorityJSON string
		if err := rows.Scan(
			&current.Meta.Version,
			&current.Meta.ParentVersion,
			&current.Meta.SchemaVersion,
			&current.ConfigJSON,
			&current.Meta.ConfigFingerprint,
			&current.Meta.ContentHash,
			&current.Meta.PreviousHash,
			&current.Meta.SnapshotHash,
			&requiredJSON,
			&priorityJSON,
			&current.Meta.Marker,
			&current.Payload,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(requiredJSON), &current.Meta.RequiredFormats); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(priorityJSON), &current.Meta.Priority); err != nil {
			return nil, err
		}
		out = append(out, current)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func validateRows(rows []row) error {
	if len(rows) == 0 {
		return nil
	}
	previousHash := ""
	for index, current := range rows {
		expectedVersion := index + 1
		if current.Meta.Version != expectedVersion {
			return model.NewCodeError(model.CodeSnapshotVersion, fmt.Sprintf("snapshot version chain gap: expected %d, found %d", expectedVersion, current.Meta.Version), 0, 0)
		}
		if current.Meta.ParentVersion != expectedVersion-1 {
			return model.NewCodeError(model.CodeSnapshotVersion, fmt.Sprintf("snapshot %d has parent %d", current.Meta.Version, current.Meta.ParentVersion), 0, 0)
		}
		if current.Meta.PreviousHash != previousHash {
			return model.NewCodeError(model.CodeSnapshotVersion, fmt.Sprintf("snapshot %d previous hash does not match its parent", current.Meta.Version), 0, 0)
		}
		var config storedConfig
		if err := json.Unmarshal([]byte(current.ConfigJSON), &config); err != nil {
			return model.NewCodeError(model.CodeSnapshotVersion, fmt.Sprintf("snapshot %d config is not canonical JSON", current.Meta.Version), 0, 0)
		}
		if config.SchemaVersion != current.Meta.SchemaVersion || !equalStrings(config.RequiredFormats, current.Meta.RequiredFormats) || !equalStrings(config.Priority, current.Meta.Priority) {
			return model.NewCodeError(model.CodeSnapshotVersion, fmt.Sprintf("snapshot %d config was replaced", current.Meta.Version), 0, 0)
		}
		configFingerprint := fingerprintConfig(config)
		if configFingerprint != current.Meta.ConfigFingerprint {
			return model.NewCodeError(model.CodeSnapshotVersion, fmt.Sprintf("snapshot %d config fingerprint mismatch", current.Meta.Version), 0, 0)
		}
		contentHash := hashBytes(current.Payload)
		if contentHash != current.Meta.ContentHash {
			return model.NewCodeError(model.CodeSnapshotVersion, fmt.Sprintf("snapshot %d payload was replaced", current.Meta.Version), 0, 0)
		}
		expectedSnapshotHash := hashSnapshot(current.Meta.Version, current.Meta.ParentVersion, previousHash, contentHash, configFingerprint)
		if expectedSnapshotHash != current.Meta.SnapshotHash {
			return model.NewCodeError(model.CodeSnapshotVersion, fmt.Sprintf("snapshot %d hash mismatch", current.Meta.Version), 0, 0)
		}
		previousHash = current.Meta.SnapshotHash
	}
	return nil
}

func buildMeta(version, parent int, previousHash string, payload []byte, requiredFormats, priority, acceptedFormats []string) (Meta, string, error) {
	required := sortedUnique(requiredFormats)
	priority = append([]string(nil), priority...)
	accepted := sortedUnique(acceptedFormats)
	config := storedConfig{
		SchemaVersion:   SchemaVersion,
		RequiredFormats: required,
		Priority:        priority,
		AcceptedFormats: accepted,
	}
	configBytes, err := json.Marshal(config)
	if err != nil {
		return Meta{}, "", err
	}
	configFingerprint := fingerprintConfig(config)
	contentHash := hashBytes(payload)
	meta := Meta{
		Version:           version,
		ParentVersion:     parent,
		SchemaVersion:     SchemaVersion,
		ConfigFingerprint: configFingerprint,
		ContentHash:       contentHash,
		PreviousHash:      previousHash,
		SnapshotHash:      hashSnapshot(version, parent, previousHash, contentHash, configFingerprint),
		RequiredFormats:   required,
		Priority:          priority,
		Marker:            Marker,
	}
	return meta, string(configBytes), nil
}

func fingerprintConfig(config storedConfig) string {
	copyValue := config
	copyValue.RequiredFormats = sortedUnique(config.RequiredFormats)
	copyValue.AcceptedFormats = sortedUnique(config.AcceptedFormats)
	copyValue.Priority = append([]string(nil), config.Priority...)
	data, _ := json.Marshal(copyValue)
	return hashBytes(data)
}

func hashSnapshot(version, parent int, previousHash, contentHash, configFingerprint string) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "supplyaudit-snapshot-v1\n%d\n%d\n%s\n%s\n%s", version, parent, previousHash, contentHash, configFingerprint)
	return hex.EncodeToString(h.Sum(nil))
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func joinJSON(values []string) string {
	data, _ := json.Marshal(values)
	return string(data)
}

func sortedUnique(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func IsSnapshotVersionError(err error) bool {
	if err == nil {
		return false
	}
	var codeErr *model.CodeError
	return errors.As(err, &codeErr) && codeErr.Code == model.CodeSnapshotVersion
}
