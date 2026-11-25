package checkpoint

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"regworkbench/internal/canonical"
	"regworkbench/internal/filelock"
	"regworkbench/internal/workerr"
)

type Record struct {
	Seq               int64           `json:"seq"`
	Stage             string          `json:"stage"`
	Fold              int             `json:"fold"`
	InputFingerprint  string          `json:"input_fingerprint"`
	ConfigVersion     string          `json:"config_version"`
	CoefficientDigest string          `json:"coefficient_digest"`
	Payload           json.RawMessage `json:"payload,omitempty"`
}

type Manager struct {
	dir      string
	dbPath   string
	lockPath string
	db       *sql.DB
	kFolds   int
}

var stageRank = map[string]int{
	"preprocess": 10,
	"split":      20,
	"fold":       30,
	"selection":  40,
	"final":      50,
}

func OpenManager(dir string, kFolds int) (*Manager, error) {
	if err := os.MkdirAll(filepath.Join(dir, "checkpoints"), 0o755); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dir, "workbench.db")
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL&_foreign_keys=on")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	m := &Manager{dir: dir, dbPath: dbPath, lockPath: filepath.Join(dir, ".publish.lock"), db: db, kFolds: kFolds}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS checkpoints (
		seq INTEGER PRIMARY KEY,
		stage TEXT NOT NULL,
		fold INTEGER NOT NULL,
		input_fingerprint TEXT NOT NULL,
		config_version TEXT NOT NULL,
		coefficient_digest TEXT NOT NULL,
		canonical_json TEXT NOT NULL
	)`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := m.reconcileFiles(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return m, nil
}

func (m *Manager) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

func (m *Manager) SetKFolds(k int) { m.kFolds = k }

func (m *Manager) WithLock(ctx context.Context, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	lock, err := filelock.Acquire(ctx, m.lockPath)
	if err != nil {
		return workerr.Wrap(workerr.EConflict, fmt.Errorf("acquire publish lock: %w", err))
	}
	defer lock.Release()
	return fn()
}

func (m *Manager) Publish(ctx context.Context, rec Record) (*Record, error) {
	err := m.WithLock(ctx, func() error {
		records, err := m.recordsLocked()
		if err != nil {
			return err
		}
		last := int64(0)
		if len(records) > 0 {
			last = records[len(records)-1].Seq
		}
		if rec.Seq != last+1 {
			expected := last + 1
			e := workerr.New(workerr.EStaleCheckpoint, fmt.Sprintf("expected seq %d, got %d", expected, rec.Seq))
			e.ExpectedSeq = expected
			e.ActualSeq = rec.Seq
			return e
		}
		if err := validateStage(records, rec, m.kFolds); err != nil {
			return err
		}
		b, err := canonical.JSON(rec)
		if err != nil {
			return err
		}
		path := filepath.Join(m.dir, "checkpoints", fmt.Sprintf("%012d-%s-%d.json", rec.Seq, rec.Stage, rec.Fold))
		if err := canonical.AtomicWriteFile(path, append(b, '\n'), 0o644); err != nil {
			return err
		}
		_, err = m.db.Exec(`INSERT INTO checkpoints(seq,stage,fold,input_fingerprint,config_version,coefficient_digest,canonical_json)
			VALUES(?,?,?,?,?,?,?)`, rec.Seq, rec.Stage, rec.Fold, rec.InputFingerprint, rec.ConfigVersion, rec.CoefficientDigest, string(b))
		if err != nil {
			return workerr.Wrap(workerr.EStaleCheckpoint, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func validateStage(previous []Record, rec Record, kFolds int) error {
	rank, ok := stageRank[rec.Stage]
	if !ok {
		e := workerr.New(workerr.EInvalidStage, "unknown stage "+rec.Stage)
		e.Stage = rec.Stage
		return e
	}
	has := func(stage string) bool {
		for _, r := range previous {
			if r.Stage == stage {
				return true
			}
		}
		return false
	}
	lastRank := 0
	for _, r := range previous {
		if stageRank[r.Stage] > lastRank {
			lastRank = stageRank[r.Stage]
		}
	}
	if rank < lastRank {
		e := workerr.New(workerr.EInvalidStage, "stage "+rec.Stage+" cannot be published after later stage")
		e.Stage = rec.Stage
		return e
	}

	switch rec.Stage {
	case "preprocess":
		if len(previous) != 0 || rec.Fold != -1 {
			return invalidStage(rec.Stage, "session_start", "preprocess must be first global record")
		}
	case "split":
		if !has("preprocess") || rec.Fold != -1 {
			return invalidStage(rec.Stage, "preprocess", "split requires preprocess")
		}
	case "fold":
		if !has("split") || rec.Fold < 0 {
			return invalidStage(rec.Stage, "split", "fold record requires split")
		}
		expectedFold := 0
		for _, r := range previous {
			if r.Stage == "fold" {
				expectedFold++
			}
		}
		if rec.Fold != expectedFold {
			e := workerr.New(workerr.EStaleCheckpoint, fmt.Sprintf("cross-fold publish mismatch: expected fold %d, got %d", expectedFold, rec.Fold))
			e.Fold = &rec.Fold
			e.ExpectedSeq = int64(expectedFold)
			return e
		}
	case "selection":
		foldCount := 0
		for _, r := range previous {
			if r.Stage == "fold" {
				foldCount++
			}
		}
		if foldCount == 0 {
			return invalidStage(rec.Stage, "fold", "selection requires fold products")
		}
		if kFolds > 0 && foldCount != kFolds {
			return invalidStage(rec.Stage, fmt.Sprintf("all_%d_folds", kFolds), fmt.Sprintf("selection requires %d folds, found %d", kFolds, foldCount))
		}
		if rec.Fold != -1 {
			return invalidStage(rec.Stage, "global", "selection must be global fold=-1")
		}
	case "final":
		if !has("selection") || rec.Fold != -1 {
			return invalidStage(rec.Stage, "selection", "final requires selection")
		}
	}
	return nil
}

func invalidStage(stage, required, message string) *workerr.Error {
	e := workerr.New(workerr.EInvalidStage, message)
	e.Stage = stage
	e.RequiredStage = required
	return e
}

func (m *Manager) Records() ([]Record, error) {
	return m.recordsLocked()
}

func (m *Manager) recordsLocked() ([]Record, error) {
	rows, err := m.db.Query(`SELECT canonical_json FROM checkpoints ORDER BY seq ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var rec Record
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (m *Manager) LoadPayload(seq int64, target any) error {
	var raw string
	err := m.db.QueryRow(`SELECT canonical_json FROM checkpoints WHERE seq=?`, seq).Scan(&raw)
	if err != nil {
		return err
	}
	var envelope struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return err
	}
	return json.Unmarshal(envelope.Payload, target)
}

func (m *Manager) reconcileFiles(ctx context.Context) error {
	return m.WithLock(ctx, func() error {
		paths, err := filepath.Glob(filepath.Join(m.dir, "checkpoints", "*.json"))
		if err != nil {
			return err
		}
		sort.Strings(paths)
		for _, path := range paths {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var rec Record
			if err := json.Unmarshal(b, &rec); err != nil {
				return fmt.Errorf("corrupt checkpoint %s: %w", path, err)
			}
			canonicalBytes, err := canonical.JSON(rec)
			if err != nil {
				return err
			}
			_, err = m.db.Exec(`INSERT OR IGNORE INTO checkpoints(seq,stage,fold,input_fingerprint,config_version,coefficient_digest,canonical_json)
				VALUES(?,?,?,?,?,?,?)`, rec.Seq, rec.Stage, rec.Fold, rec.InputFingerprint, rec.ConfigVersion, rec.CoefficientDigest, string(canonicalBytes))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (m *Manager) StopFilePath() string { return filepath.Join(m.dir, "stop.requested") }

func (m *Manager) RequestStop() error {
	return canonical.AtomicWriteFile(m.StopFilePath(), []byte("stop\n"), 0o644)
}

func (m *Manager) StopRequested() bool {
	_, err := os.Stat(m.StopFilePath())
	return err == nil
}

func (m *Manager) ClearStop() error {
	if err := os.Remove(m.StopFilePath()); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
