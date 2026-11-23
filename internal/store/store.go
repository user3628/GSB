package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	_ "modernc.org/sqlite"

	"vpnbench/internal/model"
)

type Store struct {
	db      *sql.DB
	writeMu sync.Mutex
}

type SnapshotInput struct {
	Kind          string
	Content       []byte
	IdentityKeys  []string
	SummaryDigest string
	PolicyVersion int64
	WindowID      string
	SessionID     string
}

type Snapshot struct {
	Kind             string   `json:"kind"`
	Version          int64    `json:"version"`
	ContentHash      string   `json:"content_hash"`
	IdentityKeys     []string `json:"identity_keys"`
	ChangeType       string   `json:"change_type"`
	WholeReplacement bool     `json:"whole_replacement"`
	BeforeHash       string   `json:"before_hash,omitempty"`
	AfterHash        string   `json:"after_hash"`
	BeforeSummary    Summary  `json:"before_summary"`
	AfterSummary     Summary  `json:"after_summary"`
	PolicyVersion    int64    `json:"policy_version,omitempty"`
	WindowID         string   `json:"window_id,omitempty"`
	SessionID        string   `json:"session_id,omitempty"`
	Content          []byte   `json:"-"`
}

type Summary struct {
	Version       int64    `json:"version"`
	Digest        string   `json:"digest"`
	IdentityCount int      `json:"identity_count"`
	IdentityKeys  []string `json:"identity_keys"`
}

type Chain struct {
	Kind            string     `json:"kind"`
	LatestVersion   int64      `json:"latest_version"`
	MissingVersions []int64    `json:"missing_versions"`
	Snapshots       []Snapshot `json:"snapshots"`
	Fallback        *Snapshot  `json:"fallback,omitempty"`
}

type WindowResult struct {
	StreamID   string
	RequestSeq int64
	Code       model.Code
	ResultJSON []byte
}

type SettlementInput struct {
	SessionID     string
	WindowID      string
	Status        string
	PolicyVersion int64
	OutcomeJSON   []byte
	Results       []WindowResult
	Corrections   []model.Correction
	Ledger        []model.LedgerEntry
}

type WindowRecord struct {
	WindowID          string          `json:"window_id"`
	SessionID         string          `json:"session_id"`
	Status            string          `json:"status"`
	PolicyVersion     int64           `json:"policy_version"`
	SettlementVersion int64           `json:"settlement_version"`
	RequestCount      int             `json:"request_count"`
	OutcomeJSON       json.RawMessage `json:"outcome"`
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, model.NewError(model.CodeInternal, "database path is empty", 0)
	}
	if path != ":memory:" {
		dir := filepath.Dir(path)
		if dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, model.NewError(model.CodeInternal, "create database directory: "+err.Error(), 0)
			}
		}
	}
	dsn := path
	if path == ":memory:" {
		dsn = "file:vpnbench?mode=memory&cache=shared"
	} else {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, model.NewError(model.CodeInternal, "resolve sqlite path: "+err.Error(), 0)
		}
		escapedPath := strings.ReplaceAll(url.PathEscape(filepath.ToSlash(absolute)), "%2F", "/")
		dsn = "file:" + escapedPath + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, model.NewError(model.CodeInternal, "open sqlite: "+err.Error(), 0)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, model.NewError(model.CodeInternal, "ping sqlite: "+err.Error(), 0)
	}
	store := &Store{db: db}
	if err := store.migrate(context.Background()); err != nil {
		db.Close()
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

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS snapshot_sequences (
			kind TEXT PRIMARY KEY,
			next_version INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			kind TEXT NOT NULL,
			version INTEGER NOT NULL,
			content_json BLOB NOT NULL,
			content_hash TEXT NOT NULL,
			identity_keys_json TEXT NOT NULL,
			identity_count INTEGER NOT NULL,
			change_type TEXT NOT NULL,
			whole_replacement INTEGER NOT NULL,
			before_hash TEXT NOT NULL,
			before_summary_json TEXT NOT NULL,
			after_summary_json TEXT NOT NULL,
			policy_version INTEGER NOT NULL DEFAULT 0,
			window_id TEXT NOT NULL DEFAULT '',
			session_id TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (kind, version)
		)`,
		`CREATE TABLE IF NOT EXISTS windows (
			window_id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL,
			status TEXT NOT NULL,
			policy_version INTEGER NOT NULL,
			settlement_version INTEGER NOT NULL,
			request_count INTEGER NOT NULL,
			outcome_json BLOB NOT NULL,
			content_hash TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_windows_session ON windows(session_id, settlement_version)`,
		`CREATE TABLE IF NOT EXISTS window_results (
			settlement_version INTEGER NOT NULL,
			session_id TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			request_seq INTEGER NOT NULL,
			code TEXT NOT NULL,
			result_json BLOB NOT NULL,
			PRIMARY KEY (settlement_version, session_id, stream_id, request_seq)
		)`,
		`CREATE TABLE IF NOT EXISTS corrections (
			settlement_version INTEGER NOT NULL,
			session_id TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			request_seq INTEGER NOT NULL,
			code TEXT NOT NULL,
			correction_json BLOB NOT NULL,
			PRIMARY KEY (settlement_version, session_id, stream_id, request_seq, code)
		)`,
		`CREATE TABLE IF NOT EXISTS ledger_entries (
			settlement_version INTEGER NOT NULL,
			sequence INTEGER NOT NULL,
			session_id TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			request_seq INTEGER NOT NULL,
			tunnel_id TEXT NOT NULL,
			entry_json BLOB NOT NULL,
			PRIMARY KEY (settlement_version, sequence)
		)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return model.NewError(model.CodeInternal, "migrate sqlite: "+err.Error(), 0)
		}
	}
	return nil
}

func (s *Store) SavePolicySnapshot(ctx context.Context, input SnapshotInput) (Snapshot, error) {
	input.Kind = "policy"
	return s.saveSnapshot(ctx, input)
}

func (s *Store) SaveSettlementSnapshot(ctx context.Context, input SettlementInput) (Snapshot, bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, false, model.NewError(model.CodeInternal, "begin settlement transaction: "+err.Error(), 0)
	}
	defer tx.Rollback()

	var existingVersion int64
	var existingHash string
	err = tx.QueryRowContext(ctx,
		`SELECT settlement_version, content_hash FROM windows WHERE window_id = ? AND session_id = ?`,
		input.WindowID, input.SessionID,
	).Scan(&existingVersion, &existingHash)
	if err == nil {
		contentHash := hashBytes(input.OutcomeJSON)
		if existingHash != contentHash {
			return Snapshot{}, false, model.NewError(model.CodeResultMismatch, "window already committed with different content", 0).
				Locate(input.SessionID, "", 0)
		}
		snapshot, snapshotErr := snapshotByVersion(ctx, tx, "settlement", existingVersion)
		if snapshotErr != nil {
			return Snapshot{}, false, snapshotErr
		}
		return snapshot, true, nil
	}
	if err != sql.ErrNoRows {
		return Snapshot{}, false, model.NewError(model.CodeInternal, "query committed window: "+err.Error(), 0)
	}

	version, err := allocateVersion(ctx, tx, "settlement")
	if err != nil {
		return Snapshot{}, false, err
	}
	previous, err := latestSnapshot(ctx, tx, "settlement")
	if err != nil {
		return Snapshot{}, false, err
	}
	settlementInput := SnapshotInput{
		Kind:          "settlement",
		Content:       input.OutcomeJSON,
		IdentityKeys:  make([]string, 0, len(input.Results)),
		PolicyVersion: input.PolicyVersion,
		WindowID:      input.WindowID,
		SessionID:     input.SessionID,
	}
	for _, result := range input.Results {
		settlementInput.IdentityKeys = append(settlementInput.IdentityKeys,
			fmt.Sprintf("%s/%s/%d", input.SessionID, result.StreamID, result.RequestSeq))
	}
	snapshot, err := buildSnapshot(version, settlementInput, previous, settlementInput)
	if err != nil {
		return Snapshot{}, false, err
	}
	if err := insertSnapshot(ctx, tx, snapshot); err != nil {
		return Snapshot{}, false, err
	}
	contentHash := hashBytes(input.OutcomeJSON)
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO windows(window_id, session_id, status, policy_version, settlement_version, request_count, outcome_json, content_hash)
		 VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
		input.WindowID, input.SessionID, input.Status, input.PolicyVersion, version, len(input.Results), input.OutcomeJSON, contentHash,
	); err != nil {
		return Snapshot{}, false, model.NewError(model.CodeInternal, "insert settled window: "+err.Error(), 0)
	}
	for _, result := range input.Results {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO window_results(settlement_version, session_id, stream_id, request_seq, code, result_json)
			 VALUES(?, ?, ?, ?, ?, ?)`,
			version, input.SessionID, result.StreamID, result.RequestSeq, string(result.Code), result.ResultJSON,
		); err != nil {
			return Snapshot{}, false, model.NewError(model.CodeInternal, "insert window result: "+err.Error(), 0)
		}
	}
	for _, correction := range input.Corrections {
		encoded, _ := json.Marshal(correction)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO corrections(settlement_version, session_id, stream_id, request_seq, code, correction_json)
			 VALUES(?, ?, ?, ?, ?, ?)`,
			version, correction.SessionID, correction.StreamID, correction.RequestSeq, string(correction.Code), encoded,
		); err != nil {
			return Snapshot{}, false, model.NewError(model.CodeInternal, "insert correction: "+err.Error(), 0)
		}
	}
	for _, entry := range input.Ledger {
		encoded, _ := json.Marshal(entry)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO ledger_entries(settlement_version, sequence, session_id, stream_id, request_seq, tunnel_id, entry_json)
			 VALUES(?, ?, ?, ?, ?, ?, ?)`,
			version, entry.Sequence, entry.SessionID, entry.StreamID, entry.RequestSeq, entry.TunnelID, encoded,
		); err != nil {
			return Snapshot{}, false, model.NewError(model.CodeInternal, "insert ledger entry: "+err.Error(), 0)
		}
	}
	maybeCrash("settlement_after_write")
	if err := tx.Commit(); err != nil {
		return Snapshot{}, false, model.NewError(model.CodeInternal, "commit settlement transaction: "+err.Error(), 0)
	}
	return snapshot, false, nil
}

func (s *Store) saveSnapshot(ctx context.Context, input SnapshotInput) (Snapshot, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, model.NewError(model.CodeInternal, "begin snapshot transaction: "+err.Error(), 0)
	}
	defer tx.Rollback()
	version, err := allocateVersion(ctx, tx, input.Kind)
	if err != nil {
		return Snapshot{}, err
	}
	previous, err := latestSnapshot(ctx, tx, input.Kind)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot, err := buildSnapshot(version, input, previous, input)
	if err != nil {
		return Snapshot{}, err
	}
	if err := insertSnapshot(ctx, tx, snapshot); err != nil {
		return Snapshot{}, err
	}
	maybeCrash("policy_after_write")
	if err := tx.Commit(); err != nil {
		return Snapshot{}, model.NewError(model.CodeInternal, "commit snapshot transaction: "+err.Error(), 0)
	}
	return snapshot, nil
}

func allocateVersion(ctx context.Context, tx *sql.Tx, kind string) (int64, error) {
	if _, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO snapshot_sequences(kind, next_version) VALUES(?, 1)`, kind,
	); err != nil {
		return 0, model.NewError(model.CodeInternal, "initialize snapshot sequence: "+err.Error(), 0)
	}
	var version int64
	if err := tx.QueryRowContext(ctx,
		`SELECT next_version FROM snapshot_sequences WHERE kind = ?`, kind,
	).Scan(&version); err != nil {
		return 0, model.NewError(model.CodeInternal, "read snapshot sequence: "+err.Error(), 0)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE snapshot_sequences SET next_version = ? WHERE kind = ?`, version+1, kind,
	); err != nil {
		return 0, model.NewError(model.CodeInternal, "advance snapshot sequence: "+err.Error(), 0)
	}
	return version, nil
}

func latestSnapshot(ctx context.Context, tx *sql.Tx, kind string) (*Snapshot, error) {
	row := tx.QueryRowContext(ctx,
		`SELECT kind, version, content_json, content_hash, identity_keys_json, change_type,
		        whole_replacement, before_hash, before_summary_json, after_summary_json,
		        policy_version, window_id, session_id
		   FROM snapshots WHERE kind = ? ORDER BY version DESC LIMIT 1`, kind)
	snapshot, err := scanSnapshot(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, model.NewError(model.CodeInternal, "read latest snapshot: "+err.Error(), 0)
	}
	return &snapshot, nil
}

func snapshotByVersion(ctx context.Context, tx *sql.Tx, kind string, version int64) (Snapshot, error) {
	row := tx.QueryRowContext(ctx,
		`SELECT kind, version, content_json, content_hash, identity_keys_json, change_type,
		        whole_replacement, before_hash, before_summary_json, after_summary_json,
		        policy_version, window_id, session_id
		   FROM snapshots WHERE kind = ? AND version = ?`, kind, version)
	snapshot, err := scanSnapshot(row)
	if err != nil {
		return Snapshot{}, model.NewError(model.CodeInternal, "read snapshot: "+err.Error(), 0)
	}
	return snapshot, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanSnapshot(row rowScanner) (Snapshot, error) {
	var snapshot Snapshot
	var identityJSON, beforeSummaryJSON, afterSummaryJSON []byte
	var wholeReplacement int
	err := row.Scan(
		&snapshot.Kind, &snapshot.Version, &snapshot.Content, &snapshot.ContentHash, &identityJSON,
		&snapshot.ChangeType, &wholeReplacement, &snapshot.BeforeHash, &beforeSummaryJSON,
		&afterSummaryJSON, &snapshot.PolicyVersion, &snapshot.WindowID, &snapshot.SessionID,
	)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot.WholeReplacement = wholeReplacement != 0
	snapshot.AfterHash = snapshot.ContentHash
	_ = json.Unmarshal(identityJSON, &snapshot.IdentityKeys)
	_ = json.Unmarshal(beforeSummaryJSON, &snapshot.BeforeSummary)
	_ = json.Unmarshal(afterSummaryJSON, &snapshot.AfterSummary)
	return snapshot, nil
}

func buildSnapshot(version int64, input SnapshotInput, previous *Snapshot, detectionInput SnapshotInput) (Snapshot, error) {
	identityKeys := append([]string(nil), detectionInput.IdentityKeys...)
	if identityKeys == nil {
		identityKeys = []string{}
	}
	sort.Strings(identityKeys)
	contentHash := hashBytes(input.Content)
	summaryDigest := contentHash
	if input.SummaryDigest != "" {
		summaryDigest = input.SummaryDigest
	}
	after := Summary{
		Version:       version,
		Digest:        summaryDigest,
		IdentityCount: len(identityKeys),
		IdentityKeys:  identityKeys,
	}
	snapshot := Snapshot{
		Kind:             input.Kind,
		Version:          version,
		Content:          append([]byte(nil), input.Content...),
		ContentHash:      contentHash,
		IdentityKeys:     identityKeys,
		AfterHash:        contentHash,
		AfterSummary:     after,
		BeforeSummary:    Summary{IdentityKeys: []string{}},
		PolicyVersion:    input.PolicyVersion,
		WindowID:         input.WindowID,
		SessionID:        input.SessionID,
		ChangeType:       "INITIAL",
		WholeReplacement: false,
	}
	if previous == nil {
		return snapshot, nil
	}
	snapshot.BeforeHash = previous.ContentHash
	snapshot.BeforeSummary = previous.AfterSummary
	if previous.ContentHash == contentHash {
		snapshot.ChangeType = "SAME_CONTENT"
		return snapshot, nil
	}
	intersection := identityIntersection(previous.IdentityKeys, identityKeys)
	if intersection == 0 {
		snapshot.ChangeType = "WHOLE_REPLACEMENT"
		snapshot.WholeReplacement = true
	} else if sameStringSet(previous.IdentityKeys, identityKeys) {
		snapshot.ChangeType = "IN_PLACE_REPLACEMENT"
	} else {
		snapshot.ChangeType = "PARTIAL_REPLACEMENT"
	}
	return snapshot, nil
}

func insertSnapshot(ctx context.Context, tx *sql.Tx, snapshot Snapshot) error {
	identityJSON, _ := json.Marshal(snapshot.IdentityKeys)
	beforeSummaryJSON, _ := json.Marshal(snapshot.BeforeSummary)
	afterSummaryJSON, _ := json.Marshal(snapshot.AfterSummary)
	whole := 0
	if snapshot.WholeReplacement {
		whole = 1
	}
	_, err := tx.ExecContext(ctx,
		`INSERT INTO snapshots(kind, version, content_json, content_hash, identity_keys_json,
		 identity_count, change_type, whole_replacement, before_hash, before_summary_json,
		 after_summary_json, policy_version, window_id, session_id)
		 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshot.Kind, snapshot.Version, snapshot.Content, snapshot.ContentHash, identityJSON,
		len(snapshot.IdentityKeys), snapshot.ChangeType, whole, snapshot.BeforeHash,
		beforeSummaryJSON, afterSummaryJSON, snapshot.PolicyVersion, snapshot.WindowID, snapshot.SessionID,
	)
	if err != nil {
		return model.NewError(model.CodeInternal, "insert snapshot: "+err.Error(), 0)
	}
	return nil
}

func (s *Store) Chain(ctx context.Context, kind string) (Chain, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT kind, version, content_json, content_hash, identity_keys_json, change_type,
		        whole_replacement, before_hash, before_summary_json, after_summary_json,
		        policy_version, window_id, session_id
		   FROM snapshots WHERE kind = ? ORDER BY version ASC`, kind)
	if err != nil {
		return Chain{}, model.NewError(model.CodeInternal, "query snapshot chain: "+err.Error(), 0)
	}
	defer rows.Close()
	chain := Chain{Kind: kind, MissingVersions: []int64{}, Snapshots: []Snapshot{}}
	byVersion := make(map[int64]Snapshot)
	for rows.Next() {
		snapshot, err := scanSnapshot(rows)
		if err != nil {
			return Chain{}, model.NewError(model.CodeInternal, "scan snapshot chain: "+err.Error(), 0)
		}
		chain.Snapshots = append(chain.Snapshots, snapshot)
		byVersion[snapshot.Version] = snapshot
		if snapshot.Version > chain.LatestVersion {
			chain.LatestVersion = snapshot.Version
		}
	}
	if err := rows.Err(); err != nil {
		return Chain{}, model.NewError(model.CodeInternal, "iterate snapshot chain: "+err.Error(), 0)
	}
	contiguous := true
	for version := int64(1); version <= chain.LatestVersion; version++ {
		snapshot, exists := byVersion[version]
		if !exists {
			chain.MissingVersions = append(chain.MissingVersions, version)
			contiguous = false
			continue
		}
		if contiguous {
			copy := snapshot
			chain.Fallback = &copy
		}
	}
	return chain, nil
}

func (s *Store) LatestContiguous(ctx context.Context, kind string) (Chain, error) {
	chain, err := s.Chain(ctx, kind)
	if err != nil {
		return chain, err
	}
	if len(chain.MissingVersions) > 0 || len(chain.Snapshots) == 0 || chain.Fallback == nil {
		message := "snapshot version chain has a gap"
		if len(chain.Snapshots) == 0 {
			message = "snapshot version chain is empty"
		}
		issue := model.NewError(model.CodeSnapshotVersion, message, 0)
		issue.Expected = "contiguous versions starting at 1"
		issue.Actual = fmt.Sprintf("latest=%d missing=%v", chain.LatestVersion, chain.MissingVersions)
		if len(chain.MissingVersions) > 0 && chain.Fallback != nil {
			issue.Message = fmt.Sprintf("%s; fell back to version %d", message, chain.Fallback.Version)
		}
		return chain, issue
	}
	return chain, nil
}

func (s *Store) Windows(ctx context.Context) ([]WindowRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT window_id, session_id, status, policy_version, settlement_version, request_count, outcome_json
		   FROM windows ORDER BY session_id ASC, settlement_version ASC, window_id ASC`)
	if err != nil {
		return nil, model.NewError(model.CodeInternal, "query windows: "+err.Error(), 0)
	}
	defer rows.Close()
	var records []WindowRecord
	for rows.Next() {
		var record WindowRecord
		var outcome []byte
		if err := rows.Scan(&record.WindowID, &record.SessionID, &record.Status, &record.PolicyVersion,
			&record.SettlementVersion, &record.RequestCount, &outcome); err != nil {
			return nil, model.NewError(model.CodeInternal, "scan window: "+err.Error(), 0)
		}
		record.OutcomeJSON = append(json.RawMessage(nil), outcome...)
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, model.NewError(model.CodeInternal, "iterate windows: "+err.Error(), 0)
	}
	return records, nil
}

func (s *Store) QueryCorrections(ctx context.Context, sessionID, streamID string, requestSeq int64) ([]model.Correction, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT correction_json FROM corrections
		  WHERE session_id = ? AND stream_id = ? AND request_seq = ?
		  ORDER BY settlement_version ASC, code ASC`, sessionID, streamID, requestSeq)
	if err != nil {
		return nil, model.NewError(model.CodeInternal, "query corrections: "+err.Error(), 0)
	}
	defer rows.Close()
	var corrections []model.Correction
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return nil, model.NewError(model.CodeInternal, "scan correction: "+err.Error(), 0)
		}
		var correction model.Correction
		if err := json.Unmarshal(encoded, &correction); err != nil {
			return nil, model.NewError(model.CodeInternal, "decode correction: "+err.Error(), 0)
		}
		corrections = append(corrections, correction)
	}
	return corrections, rows.Err()
}

// InjectSnapshotGap is an explicitly invoked acceptance helper. It creates a
// committed snapshot at latest+2, leaving latest+1 missing. It never mutates or
// deletes existing snapshots.
func (s *Store) InjectSnapshotGap(ctx context.Context, kind string, payload []byte) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, model.NewError(model.CodeInternal, "begin gap injection: "+err.Error(), 0)
	}
	defer tx.Rollback()
	previous, err := latestSnapshot(ctx, tx, kind)
	if err != nil {
		return 0, err
	}
	var version int64 = 1
	if previous != nil {
		version = previous.Version + 2
	}
	hash := hashBytes(payload)
	snapshot := Snapshot{
		Kind:         kind,
		Version:      version,
		Content:      append([]byte(nil), payload...),
		ContentHash:  hash,
		IdentityKeys: []string{},
		ChangeType:   "INJECTED_GAP_HEAD",
		AfterHash:    hash,
		AfterSummary: Summary{Version: version, Digest: hash},
	}
	if previous != nil {
		snapshot.BeforeHash = previous.ContentHash
		snapshot.BeforeSummary = previous.AfterSummary
	}
	if err := insertSnapshot(ctx, tx, snapshot); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO snapshot_sequences(kind, next_version) VALUES(?, ?)
		 ON CONFLICT(kind) DO UPDATE SET next_version = excluded.next_version`, kind, version+1,
	); err != nil {
		return 0, model.NewError(model.CodeInternal, "update sequence after gap injection: "+err.Error(), 0)
	}
	if err := tx.Commit(); err != nil {
		return 0, model.NewError(model.CodeInternal, "commit gap injection: "+err.Error(), 0)
	}
	return version, nil
}

func maybeCrash(point string) {
	if strings.TrimSpace(os.Getenv("VPNBENCH_CRASH_POINT")) == point {
		os.Exit(86)
	}
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func identityIntersection(left, right []string) int {
	rightSet := make(map[string]struct{}, len(right))
	for _, item := range right {
		rightSet[item] = struct{}{}
	}
	count := 0
	for _, item := range left {
		if _, ok := rightSet[item]; ok {
			count++
		}
	}
	return count
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)
	sort.Strings(leftCopy)
	sort.Strings(rightCopy)
	for i := range leftCopy {
		if leftCopy[i] != rightCopy[i] {
			return false
		}
	}
	return true
}
