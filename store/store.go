package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
	_ "modernc.org/sqlite"

	"spmmopt/spmm"
)

type Store struct {
	db       *sql.DB
	path     string
	lockPath string
}

type PlanRef struct {
	PlanKey              string `json:"plan_key"`
	Version              int64  `json:"version"`
	Digest               string `json:"digest"`
	ShapeFingerprint     string `json:"shape_fingerprint,omitempty"`
	StructureFingerprint string `json:"structure_fingerprint,omitempty"`
	DenseFingerprint     string `json:"dense_fingerprint,omitempty"`
}

type PlanExpectation struct {
	Version int64
	Digest  string
}

type PlanRecord struct {
	PlanKey        string    `json:"plan_key"`
	Version        int64     `json:"version"`
	Digest         string    `json:"digest"`
	PreviousDigest string    `json:"previous_digest"`
	PayloadHash    string    `json:"payload_hash"`
	Plan           spmm.Plan `json:"plan"`
	CreatedAt      string    `json:"created_at"`
}

type BlockRecord struct {
	PlanKey           string           `json:"plan_key"`
	Version           int64            `json:"version"`
	PlanDigest        string           `json:"plan_digest"`
	ExecutionRowIndex int              `json:"execution_row_index"`
	PlanRowBlockIndex int              `json:"plan_row_block_index"`
	RowStart          int              `json:"row_start"`
	RowEnd            int              `json:"row_end"`
	ColStart          int              `json:"col_start"`
	ColEnd            int              `json:"col_end"`
	InputFingerprint  string           `json:"input_fingerprint"`
	OutputDigest      string           `json:"output_digest"`
	TraceDigest       string           `json:"trace_digest"`
	ErrorDigest       string           `json:"error_digest"`
	Values            []float64        `json:"values"`
	Trace             spmm.BlockTrace  `json:"trace"`
	Error             spmm.ErrorReport `json:"error"`
	CreatedAt         string           `json:"created_at"`
}

type OperatorState struct {
	PlanKey          string `json:"plan_key"`
	Version          int64  `json:"version"`
	PlanDigest       string `json:"plan_digest"`
	LastExecutionRow int    `json:"last_execution_row_index"`
	LastRowStart     int    `json:"last_row_start"`
	LastRowEnd       int    `json:"last_row_end"`
	State            string `json:"state"`
	UpdatedAt        string `json:"updated_at"`
}

type IntegrityStatus string

const (
	IntegrityOK             IntegrityStatus = "ok"
	IntegrityAdvance        IntegrityStatus = "normal_advance"
	IntegrityRollback       IntegrityStatus = "rollback"
	IntegrityReplacement    IntegrityStatus = "replacement"
	IntegrityMissing        IntegrityStatus = "missing"
	IntegrityDigestMismatch IntegrityStatus = "digest_mismatch"
)

type IntegrityReport struct {
	PlanKey               string          `json:"plan_key"`
	Status                IntegrityStatus `json:"status"`
	Reason                string          `json:"reason"`
	Reasons               []string        `json:"reasons,omitempty"`
	HeadVersion           int64           `json:"head_version"`
	HistoryMaxVersion     int64           `json:"history_max_version"`
	HeadDigest            string          `json:"head_digest"`
	ExpectedVersion       int64           `json:"expected_version,omitempty"`
	ExpectedDigest        string          `json:"expected_digest,omitempty"`
	DownstreamInvalidated bool            `json:"downstream_invalidated"`
	HeadRestored          bool            `json:"head_restored"`
}

func Open(ctx context.Context, path string) (*Store, error) {
	if path == "" {
		return nil, spmm.NewError(spmm.CodeInput, "database path is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	dsn := "file:" + filepath.ToSlash(abs) + "?_txlock=immediate&_pragma=busy_timeout(15000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA busy_timeout=15000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("configure sqlite (%s): %w", stmt, err)
		}
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}
	return &Store{db: db, path: abs, lockPath: abs + ".lock"}, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Path() string { return s.path }

func (s *Store) acquire(ctx context.Context) (*flock.Flock, error) {
	f := flock.New(s.lockPath)
	ok, err := f.TryLockContext(ctx, 5*time.Millisecond)
	if err != nil {
		return nil, spmm.NewError(spmm.CodeStalePublish, "file lock failed: %v", err)
	}
	if !ok {
		return nil, &spmm.Error{Code: spmm.CodeStalePublish, Message: "publication file lock timed out", Detail: "file_lock_timeout"}
	}
	return f, nil
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func versionDigest(planKey string, seq int64, payloadHash, prev string) string {
	type version struct {
		PlanKey     string `json:"plan_key"`
		Version     int64  `json:"version"`
		PayloadHash string `json:"payload_hash"`
		PrevDigest  string `json:"prev_digest"`
	}
	return spmm.HashJSON(version{PlanKey: planKey, Version: seq, PayloadHash: payloadHash, PrevDigest: prev})
}

func staleError(ref PlanRef, expectation string, cause error) *spmm.Error {
	e := &spmm.Error{
		Code:    spmm.CodeStalePublish,
		Message: "plan publication rejected because the version chain advanced or diverged",
		PlanKey: ref.PlanKey, PlanVersion: ref.Version, Expectation: expectation,
		Cause: cause,
	}
	return e
}

func encodeJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}

func decodePlan(b []byte) (spmm.Plan, error) {
	var p spmm.Plan
	err := json.Unmarshal(b, &p)
	return p, err
}

func isNotFound(err error) bool { return errors.Is(err, sql.ErrNoRows) }
