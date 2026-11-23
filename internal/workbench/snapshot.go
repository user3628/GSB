package workbench

import (
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

type Store struct {
	db *sql.DB
}

type SnapshotDescriptor struct {
	ChainEntry
	Compatible bool   `json:"compatible"`
	Code       string `json:"code,omitempty"`
}

type snapshotRow struct {
	ChainEntry
	ReportJSON []byte
}

func OpenStore(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("snapshot database path is required")
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS snapshots (
			version INTEGER PRIMARY KEY,
			parent_version INTEGER NOT NULL,
			parent_chain_hash TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			chain_hash TEXT NOT NULL,
			required_parser_version TEXT NOT NULL,
			format_versions TEXT NOT NULL,
			target_parser_version TEXT NOT NULL,
			sample_clock TEXT NOT NULL,
			report_json BLOB NOT NULL
		)
	`); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) CommitReport(report Report, versionJump int) (Report, error) {
	if versionJump < 0 {
		return report, NewBusinessError(CodeSnapshotVersion, "", 0, "snapshot version jump must be non-negative")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return report, err
	}
	defer tx.Rollback()

	rows, err := readSnapshotRows(tx, 0)
	if err != nil {
		return report, err
	}
	if err := validateRows(rows, 0); err != nil {
		return report, err
	}
	nextVersion := 1
	parentVersion := 0
	parentHash := ""
	if len(rows) > 0 {
		last := rows[len(rows)-1]
		nextVersion = last.Version + 1
		parentVersion = last.Version
		parentHash = last.ChainHash
	}
	if versionJump > 0 {
		requested := nextVersion + versionJump
		return report, &BusinessError{
			Code:   CodeSnapshotVersion,
			Detail: fmt.Sprintf("snapshot version jump rejected: expected next version %d, requested %d; transaction rolled back", nextVersion, requested),
		}
	}
	contentHash, err := reportContentHash(report)
	if err != nil {
		return report, err
	}
	required := report.Compatibility.RequiredParserVersion
	formats := append([]string(nil), report.Compatibility.FormatVersions...)
	sort.Strings(formats)
	chainHash := computeChainHash(parentHash, nextVersion, contentHash, required, formats, report.TargetParserVersion, SampleClock)
	if _, err := tx.Exec(`
		INSERT INTO snapshots
			(version, parent_version, parent_chain_hash, content_hash, chain_hash,
			 required_parser_version, format_versions, target_parser_version, sample_clock, report_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, nextVersion, parentVersion, parentHash, contentHash, chainHash,
		required, mustJSON(formats), report.TargetParserVersion, SampleClock, []byte{}); err != nil {
		return report, err
	}
	chain := make([]ChainEntry, 0, len(rows)+1)
	for _, row := range rows {
		chain = append(chain, row.ChainEntry)
	}
	current := ChainEntry{
		Version:               nextVersion,
		ParentVersion:         parentVersion,
		ParentChainHash:       parentHash,
		ContentHash:           contentHash,
		ChainHash:             chainHash,
		RequiredParserVersion: required,
		FormatVersions:        formats,
		TargetParserVersion:   report.TargetParserVersion,
		SampleClock:           SampleClock,
	}
	chain = append(chain, current)
	report.Snapshot = &SnapshotMetadata{
		Version:               current.Version,
		ParentVersion:         current.ParentVersion,
		ParentChainHash:       current.ParentChainHash,
		ContentHash:           current.ContentHash,
		ChainHash:             current.ChainHash,
		RequiredParserVersion: current.RequiredParserVersion,
		FormatVersions:        current.FormatVersions,
		TargetParserVersion:   current.TargetParserVersion,
		SampleClock:           current.SampleClock,
		Chain:                 chain,
	}
	stored, err := canonicalReportBytes(report)
	if err != nil {
		return report, err
	}
	if _, err := tx.Exec(`UPDATE snapshots SET report_json = ? WHERE version = ?`, stored, nextVersion); err != nil {
		return report, err
	}
	if err := tx.Commit(); err != nil {
		return report, err
	}
	return report, nil
}

func (s *Store) LoadSnapshot(version int, currentParser string) (Report, error) {
	if currentParser != "v1" && currentParser != "v2" {
		return Report{}, NewBusinessError(CodeSnapshotVersion, "", 0, fmt.Sprintf("unknown current parser version %q", currentParser))
	}
	rows, err := readSnapshotRows(s.db, version)
	if err != nil {
		return Report{}, err
	}
	if err := validateRows(rows, version); err != nil {
		return Report{}, err
	}
	target := rows[len(rows)-1]
	if !ParserCompatible(currentParser, target.RequiredParserVersion) {
		return Report{}, &BusinessError{
			Code:   CodeSnapshotVersion,
			Detail: fmt.Sprintf("snapshot version %d requires parser %s; current parser %s is incompatible", version, target.RequiredParserVersion, currentParser),
		}
	}
	var report Report
	if err := json.Unmarshal(target.ReportJSON, &report); err != nil {
		return Report{}, NewBusinessError(CodeSnapshotVersion, "", 0, "snapshot report is not valid JSON")
	}
	chain := make([]ChainEntry, 0, len(rows))
	for _, row := range rows {
		chain = append(chain, row.ChainEntry)
	}
	report.Snapshot = &SnapshotMetadata{
		Version:               target.Version,
		ParentVersion:         target.ParentVersion,
		ParentChainHash:       target.ParentChainHash,
		ContentHash:           target.ContentHash,
		ChainHash:             target.ChainHash,
		RequiredParserVersion: target.RequiredParserVersion,
		FormatVersions:        target.FormatVersions,
		TargetParserVersion:   target.TargetParserVersion,
		SampleClock:           target.SampleClock,
		Chain:                 chain,
	}
	report.TargetParserVersion = target.TargetParserVersion
	report.Compatibility.CurrentParserVersion = currentParser
	report.Compatibility.Compatible = ParserCompatible(currentParser, target.RequiredParserVersion)
	if report.Compatibility.Compatible {
		report.Compatibility.Code = ""
		report.Compatibility.Detail = fmt.Sprintf("snapshot version %d replayed with compatible parser %s", target.Version, currentParser)
	}
	return report, nil
}

func (s *Store) LoadLatest(currentParser string) (Report, error) {
	rows, err := readSnapshotRows(s.db, 0)
	if err != nil {
		return Report{}, err
	}
	if len(rows) == 0 {
		return Report{}, NewBusinessError(CodeSnapshotVersion, "", 0, "no committed snapshots")
	}
	return s.LoadSnapshot(rows[len(rows)-1].Version, currentParser)
}

func (s *Store) ListSnapshots(currentParser string) ([]SnapshotDescriptor, error) {
	rows, err := readSnapshotRows(s.db, 0)
	if err != nil {
		return nil, err
	}
	if err := validateRows(rows, 0); err != nil {
		return nil, err
	}
	list := make([]SnapshotDescriptor, 0, len(rows))
	for _, row := range rows {
		compatible := ParserCompatible(currentParser, row.RequiredParserVersion)
		descriptor := SnapshotDescriptor{ChainEntry: row.ChainEntry, Compatible: compatible}
		if !compatible {
			descriptor.Code = CodeSnapshotVersion
		}
		list = append(list, descriptor)
	}
	return list, nil
}

func readSnapshotRows(queryer interface {
	Query(string, ...any) (*sql.Rows, error)
}, upTo int) ([]snapshotRow, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if upTo > 0 {
		rows, err = queryer.Query(`
			SELECT version, parent_version, parent_chain_hash, content_hash, chain_hash,
			       required_parser_version, format_versions, target_parser_version, sample_clock, report_json
			FROM snapshots WHERE version <= ? ORDER BY version
		`, upTo)
	} else {
		rows, err = queryer.Query(`
			SELECT version, parent_version, parent_chain_hash, content_hash, chain_hash,
			       required_parser_version, format_versions, target_parser_version, sample_clock, report_json
			FROM snapshots ORDER BY version
		`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	list := make([]snapshotRow, 0)
	for rows.Next() {
		var row snapshotRow
		var formats string
		if err := rows.Scan(
			&row.Version, &row.ParentVersion, &row.ParentChainHash, &row.ContentHash, &row.ChainHash,
			&row.RequiredParserVersion, &formats, &row.TargetParserVersion, &row.SampleClock, &row.ReportJSON,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(formats), &row.FormatVersions); err != nil {
			return nil, NewBusinessError(CodeSnapshotVersion, "", 0, "snapshot format version set is invalid")
		}
		sort.Strings(row.FormatVersions)
		list = append(list, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return list, nil
}

func validateRows(rows []snapshotRow, requested int) error {
	if requested > 0 {
		if len(rows) != requested {
			return &BusinessError{
				Code:   CodeSnapshotVersion,
				Detail: fmt.Sprintf("snapshot version chain has a gap before requested version %d", requested),
			}
		}
	}
	parentHash := ""
	for index, row := range rows {
		expectedVersion := index + 1
		if row.Version != expectedVersion {
			return &BusinessError{
				Code:   CodeSnapshotVersion,
				Detail: fmt.Sprintf("snapshot version chain gap: expected version %d, found %d", expectedVersion, row.Version),
			}
		}
		expectedParent := 0
		if index > 0 {
			expectedParent = rows[index-1].Version
		}
		if row.ParentVersion != expectedParent || row.ParentChainHash != parentHash {
			return &BusinessError{
				Code:   CodeSnapshotVersion,
				Detail: fmt.Sprintf("snapshot version %d parent link was replaced or reordered", row.Version),
			}
		}
		var report Report
		if err := json.Unmarshal(row.ReportJSON, &report); err != nil {
			return &BusinessError{Code: CodeSnapshotVersion, Detail: fmt.Sprintf("snapshot version %d content is not valid JSON", row.Version)}
		}
		contentHash, err := reportContentHash(report)
		if err != nil {
			return err
		}
		if contentHash != row.ContentHash {
			return &BusinessError{
				Code:   CodeSnapshotVersion,
				Detail: fmt.Sprintf("snapshot version %d content was wholesale-replaced: stored hash %s, observed %s", row.Version, row.ContentHash, contentHash),
			}
		}
		expectedChain := computeChainHash(parentHash, row.Version, row.ContentHash, row.RequiredParserVersion, row.FormatVersions, row.TargetParserVersion, row.SampleClock)
		if expectedChain != row.ChainHash {
			return &BusinessError{
				Code:   CodeSnapshotVersion,
				Detail: fmt.Sprintf("snapshot version %d chain hash mismatch", row.Version),
			}
		}
		parentHash = row.ChainHash
	}
	return nil
}

func reportContentHash(report Report) (string, error) {
	base := report
	base.Snapshot = nil
	encoded, err := canonicalReportBytes(base)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func canonicalReportBytes(report Report) ([]byte, error) {
	return json.Marshal(report)
}

func PrettyReportBytes(report Report) ([]byte, error) {
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func computeChainHash(parentHash string, version int, contentHash, required string, formats []string, target, clock string) string {
	formatsCopy := append([]string(nil), formats...)
	sort.Strings(formatsCopy)
	payload := strings.Join([]string{
		parentHash,
		fmt.Sprintf("%d", version),
		contentHash,
		required,
		strings.Join(formatsCopy, ","),
		target,
		clock,
	}, "\x00")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func ParserCompatible(current, required string) bool {
	switch current {
	case "v1":
		return required == "v1"
	case "v2":
		return required == "v1" || required == "v2"
	default:
		return false
	}
}

func mustJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}
