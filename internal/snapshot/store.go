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
	"time"

	"baselineworkbench/internal/codes"
	"baselineworkbench/internal/engine"
	"baselineworkbench/internal/ledger"
	"baselineworkbench/internal/policy"

	_ "modernc.org/sqlite"
)

type Diagnostic struct {
	ScoreMicros      int64               `json:"deviation_score_micros"`
	ErrorUpperMicros int64               `json:"error_upper_micros"`
	ScoreInterval    engine.Interval     `json:"score_interval"`
	Comparisons      []engine.Comparison `json:"comparisons"`
	Deterministic    int                 `json:"deterministic_deviation_count"`
	Uncertain        int                 `json:"uncertain_interval_count"`
	Codes            []codes.Code        `json:"codes"`
	Complete         bool                `json:"complete"`
	Ledger           ledger.Ledger       `json:"ledger"`
}

type Content struct {
	Schema       string        `json:"schema"`
	PackagePath  string        `json:"package_path"`
	SampleMarker string        `json:"sample_marker"`
	Items        []policy.Item `json:"items"`
	Diagnostic   *Diagnostic   `json:"diagnostic,omitempty"`
}

func EncodeContent(packagePath, sampleMarker string, items []policy.Item) ([]byte, error) {
	content := Content{
		Schema:       "baseline.workbench.snapshot-content/v1",
		PackagePath:  packagePath,
		SampleMarker: sampleMarker,
		Items:        make([]policy.Item, 0, len(items)),
	}
	content.Items = append(content.Items, items...)
	sort.Slice(content.Items, func(i, j int) bool { return content.Items[i].ID < content.Items[j].ID })
	return json.Marshal(content)
}

func EncodeResult(packagePath, sampleMarker string, result engine.Result) ([]byte, error) {
	payload, err := EncodeContent(packagePath, sampleMarker, result.Items)
	if err != nil {
		return nil, err
	}
	var content Content
	if err := json.Unmarshal(payload, &content); err != nil {
		return nil, err
	}
	comparisons := append([]engine.Comparison{}, result.Comparisons...)
	if comparisons == nil {
		comparisons = []engine.Comparison{}
	}
	codeList := append([]codes.Code{}, result.Codes...)
	if codeList == nil {
		codeList = []codes.Code{}
	}
	content.Diagnostic = &Diagnostic{
		ScoreMicros:      result.ScoreMicros,
		ErrorUpperMicros: result.ErrorUpperMicros,
		ScoreInterval:    result.ScoreInterval,
		Comparisons:      comparisons,
		Deterministic:    result.Deterministic,
		Uncertain:        result.Uncertain,
		Codes:            codeList,
		Complete:         result.Complete,
		Ledger:           result.Ledger.Clone(),
	}
	return json.Marshal(content)
}

func DecodeContent(payload []byte) (Content, error) {
	var content Content
	err := json.Unmarshal(payload, &content)
	return content, err
}

type Snapshot struct {
	PackagePath      string `json:"package_path"`
	Version          int64  `json:"version"`
	ParentVersion    int64  `json:"parent_version"`
	ContentDigest    string `json:"content_digest"`
	ParentDigest     string `json:"parent_digest"`
	ContentChanged   bool   `json:"content_changed"`
	WholeReplacement bool   `json:"whole_replacement"`
	SampleMarker     string `json:"sample_marker"`
	Payload          []byte `json:"-"`
}

type Chain struct {
	Snapshots []Snapshot `json:"snapshots"`
	Active    *Snapshot  `json:"active,omitempty"`
	ErrorCode codes.Code `json:"error_code,omitempty"`
	GapAt     int64      `json:"gap_at,omitempty"`
	Message   string     `json:"message,omitempty"`
}

type AppendOptions struct {
	ForcedVersion int64
}

type Store struct {
	db   *sql.DB
	path string
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	store := &Store{db: db, path: path}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	for _, pragma := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA journal_mode = DELETE",
		"PRAGMA synchronous = FULL",
	} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if err := store.init(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) init(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS snapshots (
    package_path TEXT NOT NULL,
    version INTEGER NOT NULL,
    parent_version INTEGER NOT NULL,
    content_digest TEXT NOT NULL,
    parent_digest TEXT NOT NULL,
    content_changed INTEGER NOT NULL,
    whole_replacement INTEGER NOT NULL,
    sample_marker TEXT NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (package_path, version)
);
`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

func (s *Store) Append(ctx context.Context, packagePath string, payload []byte, sampleMarker string, options AppendOptions) (Snapshot, *codes.BusinessError) {
	if packagePath == "" {
		return Snapshot{}, codes.New(codes.PolicyMalformed, "snapshot package path is empty")
	}
	var content Content
	if err := json.Unmarshal(payload, &content); err != nil {
		return Snapshot{}, codes.New(codes.PolicyMalformed, "snapshot payload is invalid JSON: %v", err)
	}
	if content.Schema != "baseline.workbench.snapshot-content/v1" {
		return Snapshot{}, codes.New(codes.PolicyMalformed, "snapshot payload schema is invalid")
	}
	if content.PackagePath != packagePath {
		return Snapshot{}, codes.New(codes.PolicyMalformed, "snapshot payload package path does not match append path")
	}
	if content.SampleMarker != "" && sampleMarker != "" && content.SampleMarker != sampleMarker {
		return Snapshot{}, codes.New(codes.PolicyMalformed, "snapshot sample marker does not match payload")
	}
	digest := digestBytes(payload)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, codes.New(codes.SnapshotVersion, "begin snapshot transaction: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	latest, hasLatest, err := latestInTx(ctx, tx, packagePath)
	if err != nil {
		return Snapshot{}, codes.New(codes.SnapshotVersion, "read latest snapshot: %v", err)
	}
	expected := int64(1)
	parentVersion := int64(0)
	parentDigest := ""
	if hasLatest {
		expected = latest.Version + 1
		parentVersion = latest.Version
		parentDigest = latest.ContentDigest
	}
	if options.ForcedVersion > 0 && options.ForcedVersion != expected {
		return Snapshot{}, &codes.BusinessError{
			Code:    codes.SnapshotVersion,
			Message: fmt.Sprintf("snapshot version gap: expected %d, forced %d", expected, options.ForcedVersion),
		}
	}
	contentChanged := hasLatest && latest.ContentDigest != digest
	wholeReplacement := false
	if contentChanged {
		wholeReplacement = isWholeReplacement(latest.Payload, payload)
	}
	version := expected
	_, err = tx.ExecContext(ctx, `
INSERT INTO snapshots (package_path, version, parent_version, content_digest, parent_digest,
                       content_changed, whole_replacement, sample_marker, payload)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		packagePath, version, parentVersion, digest, parentDigest,
		boolInt(contentChanged), boolInt(wholeReplacement), sampleMarker, payload)
	if err != nil {
		return Snapshot{}, codes.New(codes.SnapshotVersion, "insert snapshot: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, codes.New(codes.SnapshotVersion, "commit snapshot: %v", err)
	}
	committed = true
	return Snapshot{
		PackagePath:      packagePath,
		Version:          version,
		ParentVersion:    parentVersion,
		ContentDigest:    digest,
		ParentDigest:     parentDigest,
		ContentChanged:   contentChanged,
		WholeReplacement: wholeReplacement,
		SampleMarker:     sampleMarker,
		Payload:          append([]byte(nil), payload...),
	}, nil
}

func (s *Store) LoadChain(ctx context.Context, packagePath string) (Chain, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT package_path, version, parent_version, content_digest, parent_digest,
       content_changed, whole_replacement, sample_marker, payload
FROM snapshots WHERE package_path = ? ORDER BY version ASC`, packagePath)
	if err != nil {
		return Chain{}, err
	}
	defer rows.Close()
	chain := Chain{Snapshots: []Snapshot{}}
	expected := int64(1)
	parentDigest := ""
	for rows.Next() {
		var row Snapshot
		var contentChanged, wholeReplacement int64
		if err := rows.Scan(&row.PackagePath, &row.Version, &row.ParentVersion,
			&row.ContentDigest, &row.ParentDigest, &contentChanged, &wholeReplacement,
			&row.SampleMarker, &row.Payload); err != nil {
			return Chain{}, err
		}
		row.ContentChanged = contentChanged != 0
		row.WholeReplacement = wholeReplacement != 0
		if row.Version != expected {
			chain.ErrorCode = codes.SnapshotVersion
			chain.GapAt = row.Version
			chain.Message = fmt.Sprintf("snapshot version gap: expected %d, found %d", expected, row.Version)
			break
		}
		if row.ParentVersion != expected-1 {
			chain.ErrorCode = codes.SnapshotVersion
			chain.GapAt = row.Version
			chain.Message = fmt.Sprintf("snapshot parent version mismatch at version %d", row.Version)
			break
		}
		if expected > 1 && row.ParentDigest != parentDigest {
			chain.ErrorCode = codes.SnapshotVersion
			chain.GapAt = row.Version
			chain.Message = fmt.Sprintf("snapshot parent digest mismatch at version %d", row.Version)
			break
		}
		if digestBytes(row.Payload) != row.ContentDigest {
			chain.ErrorCode = codes.SnapshotVersion
			chain.GapAt = row.Version
			chain.Message = fmt.Sprintf("snapshot payload digest mismatch at version %d", row.Version)
			break
		}
		chain.Snapshots = append(chain.Snapshots, row)
		parentDigest = row.ContentDigest
		expected++
	}
	if err := rows.Err(); err != nil {
		return Chain{}, err
	}
	if len(chain.Snapshots) > 0 {
		active := chain.Snapshots[len(chain.Snapshots)-1]
		chain.Active = &active
	}
	return chain, nil
}

func (s *Store) Version(ctx context.Context, packagePath string) (int64, error) {
	var version sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT MAX(version) FROM snapshots WHERE package_path = ?`, packagePath).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) || !version.Valid {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return version.Int64, nil
}

type latestRow struct {
	Version       int64
	ContentDigest string
	Payload       []byte
}

func latestInTx(ctx context.Context, tx *sql.Tx, packagePath string) (latestRow, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT version, content_digest, payload FROM snapshots
WHERE package_path = ? ORDER BY version DESC LIMIT 1`, packagePath)
	var latest latestRow
	if err := row.Scan(&latest.Version, &latest.ContentDigest, &latest.Payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return latestRow{}, false, nil
		}
		return latestRow{}, false, err
	}
	return latest, true, nil
}

func isWholeReplacement(previousPayload, nextPayload []byte) bool {
	var previous, next Content
	if json.Unmarshal(previousPayload, &previous) != nil || json.Unmarshal(nextPayload, &next) != nil {
		return true
	}
	if previous.Schema != next.Schema || previous.PackagePath != next.PackagePath {
		return true
	}
	previousItems := make(map[string]string, len(previous.Items))
	for _, item := range previous.Items {
		previousItems[item.ID] = itemFingerprint(item)
	}
	if len(previousItems) == 0 || len(next.Items) == 0 {
		return true
	}
	common := 0
	changed := 0
	for _, item := range next.Items {
		old, ok := previousItems[item.ID]
		if !ok {
			continue
		}
		common++
		if old != itemFingerprint(item) {
			changed++
		}
	}
	if common == 0 {
		return true
	}
	return common == len(previousItems) && common == len(next.Items) && changed == common
}

func itemFingerprint(item policy.Item) string {
	encoded, _ := json.Marshal(item)
	return digestBytes(encoded)
}

func digestBytes(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func boolInt(value bool) int64 {
	if value {
		return 1
	}
	return 0
}
