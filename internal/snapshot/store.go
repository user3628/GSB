package snapshot

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	_ "modernc.org/sqlite"

	"secverify/internal/model"
)

const PayloadSchema = "secverify.snapshot/v1"

type Payload struct {
	Schema     string       `json:"schema"`
	Namespace  string       `json:"namespace"`
	Version    int64        `json:"version"`
	ParentHash string       `json:"parent_hash,omitempty"`
	ChangeHash string       `json:"change_hash"`
	StateHash  string       `json:"state_hash"`
	Marker     string       `json:"marker"`
	Items      []model.Item `json:"items"`
}

type Snapshot struct {
	Namespace   string
	Version     int64
	Content     []byte
	ContentHash string
	ParentHash  string
	ReportJSON  []byte
	Payload     Payload
}

type Chain struct {
	Snapshots       []Snapshot
	Versions        []int64
	Latest          *Snapshot
	FallbackVersion int64
}

type Store struct {
	db      *sql.DB
	path    string
	writeMu sync.Mutex
}

func Open(path string) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o700); err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}
	query := u.Query()
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "journal_mode(WAL)")
	query.Add("_pragma", "synchronous(FULL)")
	query.Add("_pragma", "foreign_keys(ON)")
	u.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	store := &Store{db: db, path: abs}
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

func (s *Store) Path() string { return s.path }

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS snapshots (
			namespace TEXT NOT NULL,
			version INTEGER NOT NULL,
			parent_hash TEXT NOT NULL,
			content TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			created_marker TEXT NOT NULL,
			PRIMARY KEY (namespace, version)
		)`,
		`CREATE TABLE IF NOT EXISTS round_reports (
			namespace TEXT NOT NULL,
			version INTEGER NOT NULL,
			report_json TEXT NOT NULL,
			PRIMARY KEY (namespace, version)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func NewGenesisPayload(namespace string, items []model.Item) (Payload, error) {
	sorted := append([]model.Item(nil), items...)
	model.SortItems(sorted)
	stateHash, err := StateHash(sorted)
	if err != nil {
		return Payload{}, err
	}
	return Payload{
		Schema:     PayloadSchema,
		Namespace:  namespace,
		Version:    0,
		ChangeHash: "GENESIS",
		StateHash:  stateHash,
		Marker:     model.SampleMarker,
		Items:      sorted,
	}, nil
}

func NewRoundPayload(namespace string, version int64, parentHash, changeHash string, items []model.Item) (Payload, error) {
	sorted := append([]model.Item(nil), items...)
	model.SortItems(sorted)
	stateHash, err := StateHash(sorted)
	if err != nil {
		return Payload{}, err
	}
	return Payload{
		Schema:     PayloadSchema,
		Namespace:  namespace,
		Version:    version,
		ParentHash: parentHash,
		ChangeHash: changeHash,
		StateHash:  stateHash,
		Marker:     model.SampleMarker,
		Items:      sorted,
	}, nil
}

func StateHash(items []model.Item) (string, error) {
	sorted := append([]model.Item(nil), items...)
	model.SortItems(sorted)
	var builder strings.Builder
	for _, item := range sorted {
		builder.WriteString(item.ID)
		builder.WriteByte('=')
		builder.WriteString(model.ItemHash(item))
		builder.WriteByte('\n')
	}
	return model.HashBytes([]byte(builder.String())), nil
}

func EncodePayload(p Payload) ([]byte, error) {
	if p.Items == nil {
		p.Items = []model.Item{}
	}
	return json.Marshal(p)
}

func (s *Store) Initialize(ctx context.Context, namespace string, items []model.Item) (*Snapshot, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots WHERE namespace = ?`, namespace).Scan(&count); err != nil {
		return nil, err
	}
	if count > 0 {
		return nil, fmt.Errorf("namespace %s is already initialized", namespace)
	}
	payload, err := NewGenesisPayload(namespace, items)
	if err != nil {
		return nil, err
	}
	content, err := EncodePayload(payload)
	if err != nil {
		return nil, err
	}
	hash := model.HashBytes(content)
	if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(namespace, version, parent_hash, content, content_hash, created_marker) VALUES(?,?,?,?,?,?)`,
		namespace, int64(0), "", string(content), hash, model.SampleMarker); err != nil {
		return nil, err
	}
	report := []byte(`{"schema":"secverify.round/v1","version":0,"marker":"` + model.SampleMarker + `"}`)
	if _, err := tx.ExecContext(ctx, `INSERT INTO round_reports(namespace, version, report_json) VALUES(?,?,?)`, namespace, int64(0), string(report)); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Snapshot{Namespace: namespace, Version: 0, Content: content, ContentHash: hash, ReportJSON: report, Payload: payload}, nil
}

// AppendRound inserts the new snapshot and report in one transaction. The
// beforeCommit hook is used only by the external crash-injection harness and
// runs after both INSERTs but before COMMIT.
func (s *Store) AppendRound(ctx context.Context, namespace string, expectedVersion int64, parentHash, changeHash string, items []model.Item, reportJSON []byte, beforeCommit func()) (*Snapshot, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var actualMax int64
	var actualCount int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MAX(version), -1) FROM snapshots WHERE namespace = ?`, namespace).Scan(&actualCount, &actualMax); err != nil {
		return nil, err
	}
	if actualCount == 0 {
		return nil, fmt.Errorf("namespace is not initialized")
	}
	if actualMax != expectedVersion {
		return nil, fmt.Errorf("snapshot version changed concurrently: expected %d, got %d", expectedVersion, actualMax)
	}
	version := expectedVersion + 1
	payload, err := NewRoundPayload(namespace, version, parentHash, changeHash, items)
	if err != nil {
		return nil, err
	}
	content, err := EncodePayload(payload)
	if err != nil {
		return nil, err
	}
	hash := model.HashBytes(content)
	if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(namespace, version, parent_hash, content, content_hash, created_marker) VALUES(?,?,?,?,?,?)`,
		namespace, version, parentHash, string(content), hash, model.SampleMarker); err != nil {
		return nil, err
	}
	if reportJSON == nil {
		reportJSON = []byte(`{"schema":"secverify.round/v1","version":` + fmt.Sprintf("%d", version) + `,"marker":"` + model.SampleMarker + `"}`)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO round_reports(namespace, version, report_json) VALUES(?,?,?)`, namespace, version, string(reportJSON)); err != nil {
		return nil, err
	}
	if beforeCommit != nil {
		beforeCommit()
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Snapshot{Namespace: namespace, Version: version, Content: content, ContentHash: hash, ParentHash: parentHash, ReportJSON: append([]byte(nil), reportJSON...), Payload: payload}, nil
}

// LoadChain validates continuity, payload schema/hash, and parent hashes. On a
// gap it returns the contiguous prefix, a fallback version, and
// E_SNAPSHOT_VERSION so callers must refuse to advance the chain.
func (s *Store) LoadChain(ctx context.Context, namespace string) (*Chain, *model.BizError) {
	rows, err := s.db.QueryContext(ctx, `SELECT version, parent_hash, content, content_hash, created_marker FROM snapshots WHERE namespace = ? ORDER BY version`, namespace)
	if err != nil {
		return nil, model.NewError(model.CodeStore, "", -1, err.Error())
	}
	defer rows.Close()
	chain := &Chain{FallbackVersion: -1}
	expected := int64(0)
	previousHash := ""
	for rows.Next() {
		var version int64
		var parentHash, content, contentHash, marker string
		if err := rows.Scan(&version, &parentHash, &content, &contentHash, &marker); err != nil {
			return chain, model.NewError(model.CodeStore, "", -1, err.Error())
		}
		if version != expected {
			return chain, &model.BizError{
				Code:            model.CodeSnapshotVersion,
				SourceOffset:    -1,
				Message:         fmt.Sprintf("snapshot version gap: expected %d, found %d", expected, version),
				Items:           []string{namespace},
				FallbackVersion: chain.FallbackVersion,
			}
		}
		computed := model.HashBytes([]byte(content))
		if computed != contentHash {
			return chain, &model.BizError{
				Code:            model.CodeSnapshotCorrupt,
				SourceOffset:    -1,
				Message:         fmt.Sprintf("snapshot %d content hash mismatch", version),
				Items:           []string{namespace},
				FallbackVersion: chain.FallbackVersion,
			}
		}
		var payload Payload
		if err := json.Unmarshal([]byte(content), &payload); err != nil {
			return chain, &model.BizError{Code: model.CodeSnapshotCorrupt, SourceOffset: -1, Message: "snapshot payload cannot be decoded", Items: []string{namespace}, FallbackVersion: chain.FallbackVersion}
		}
		if payload.Schema != PayloadSchema || payload.Namespace != namespace || payload.Version != version {
			return chain, &model.BizError{Code: model.CodeSnapshotCorrupt, SourceOffset: -1, Message: "snapshot payload identity mismatch", Items: []string{namespace}, FallbackVersion: chain.FallbackVersion}
		}
		if version > 0 && parentHash != previousHash {
			return chain, &model.BizError{Code: model.CodeSnapshotCorrupt, SourceOffset: -1, Message: fmt.Sprintf("snapshot %d parent hash mismatch", version), Items: []string{namespace}, FallbackVersion: chain.FallbackVersion}
		}
		snap := Snapshot{Namespace: namespace, Version: version, ParentHash: parentHash, Content: []byte(content), ContentHash: contentHash, Payload: payload}
		chain.Snapshots = append(chain.Snapshots, snap)
		chain.Versions = append(chain.Versions, version)
		chain.FallbackVersion = version
		previousHash = contentHash
		expected++
	}
	if err := rows.Err(); err != nil {
		return chain, model.NewError(model.CodeStore, "", -1, err.Error())
	}
	if len(chain.Snapshots) > 0 {
		latest := chain.Snapshots[len(chain.Snapshots)-1]
		chain.Latest = &latest
	}
	return chain, nil
}

func (s *Store) RoundReport(ctx context.Context, namespace string, version int64) ([]byte, error) {
	var report string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM round_reports WHERE namespace = ? AND version = ?`, namespace, version).Scan(&report)
	if err != nil {
		return nil, err
	}
	return []byte(report), nil
}

// DeleteSnapshotForInjection deliberately corrupts the version chain. It is
// only called by an explicit external injection flag or test API.
func (s *Store) DeleteSnapshotForInjection(ctx context.Context, namespace string, version int64) error {
	if version <= 0 {
		return fmt.Errorf("version 0 cannot be removed by injection")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `DELETE FROM snapshots WHERE namespace = ? AND version = ?`, namespace, version)
	return err
}

type Comparison struct {
	LeftVersion      int64
	RightVersion     int64
	SameVersion      bool
	ContentEqual     bool
	Different        bool
	WholeReplacement bool
	CommonItems      []string
	AddedItems       []string
	RemovedItems     []string
}

// CompareSnapshots intentionally compares content and item identity, not just
// version numbers. A same-version content change is always different and is
// classified as a whole replacement.
func CompareSnapshots(left, right Snapshot) Comparison {
	common := make([]string, 0)
	added := make([]string, 0)
	removed := make([]string, 0)
	leftItems := model.ItemMap(left.Payload.Items)
	rightItems := model.ItemMap(right.Payload.Items)
	for id := range leftItems {
		if _, ok := rightItems[id]; ok {
			common = append(common, id)
		} else {
			removed = append(removed, id)
		}
	}
	for id := range rightItems {
		if _, ok := leftItems[id]; !ok {
			added = append(added, id)
		}
	}
	sort.Strings(common)
	sort.Strings(added)
	sort.Strings(removed)
	contentEqual := false
	if len(left.Content) > 0 || len(right.Content) > 0 {
		contentEqual = bytes.Equal(left.Content, right.Content)
	} else {
		contentEqual = left.ContentHash != "" && left.ContentHash == right.ContentHash
	}
	sameVersion := left.Version == right.Version
	replacement := !contentEqual && (sameVersion || len(common) == 0)
	return Comparison{
		LeftVersion:      left.Version,
		RightVersion:     right.Version,
		SameVersion:      sameVersion,
		ContentEqual:     contentEqual,
		Different:        !contentEqual,
		WholeReplacement: replacement,
		CommonItems:      common,
		AddedItems:       added,
		RemovedItems:     removed,
	}
}
