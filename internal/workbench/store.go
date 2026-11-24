package workbench

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	_ "github.com/mattn/go-sqlite3"
)

type Store struct {
	db   *sql.DB
	path string
}

func OpenStore(path string) (*Store, error) {
	if path != ":memory:" {
		if dir := filepath.Dir(path); dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
		}
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db, path: path}
	if err := store.initialize(); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) initialize() error {
	statements := []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=FULL`,
		`PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			version INTEGER PRIMARY KEY,
			kind TEXT NOT NULL,
			parent_digest TEXT NOT NULL,
			state_json BLOB NOT NULL,
			state_sha256 TEXT NOT NULL,
			chain_digest TEXT NOT NULL,
			marker TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := s.db.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Path() string {
	return s.path
}

func (s *Store) Load(repair bool) (ChainLoad, error) {
	rows, err := s.db.Query(`SELECT version, kind, parent_digest, state_json, state_sha256, chain_digest FROM snapshots ORDER BY version ASC`)
	if err != nil {
		return ChainLoad{}, err
	}
	defer rows.Close()

	var all []Snapshot
	for rows.Next() {
		var snapshot Snapshot
		if err := rows.Scan(&snapshot.Version, &snapshot.Kind, &snapshot.ParentDigest, &snapshot.StateJSON, &snapshot.StateSHA256, &snapshot.ChainDigest); err != nil {
			return ChainLoad{}, err
		}
		all = append(all, snapshot)
	}
	if err := rows.Err(); err != nil {
		return ChainLoad{}, err
	}

	load := ChainLoad{}
	expected := int64(1)
	prevDigest := GenesisDigest
	var previousState *State
	for _, snapshot := range all {
		if snapshot.Version != expected {
			load.WarningCode = CodeSnapshotVersion
			load.WarningDetail = fmt.Sprintf("snapshot version chain gap: expected %d, found %d", expected, snapshot.Version)
			load.InvalidFromVersion = expected
			break
		}
		stateHash := sha256.Sum256(snapshot.StateJSON)
		if hex.EncodeToString(stateHash[:]) != snapshot.StateSHA256 {
			load.WarningCode = CodeSnapshotVersion
			load.WarningDetail = fmt.Sprintf("snapshot %d state hash does not match its serialized state", snapshot.Version)
			load.InvalidFromVersion = expected
			break
		}
		if snapshot.ParentDigest != prevDigest {
			load.WarningCode = CodeSnapshotVersion
			load.WarningDetail = fmt.Sprintf("snapshot %d parent digest does not match the preceding chain", snapshot.Version)
			load.InvalidFromVersion = expected
			break
		}
		chainHash := sha256.Sum256([]byte(prevDigest + "\n" + snapshot.StateSHA256))
		if hex.EncodeToString(chainHash[:]) != snapshot.ChainDigest {
			load.WarningCode = CodeSnapshotVersion
			load.WarningDetail = fmt.Sprintf("snapshot %d chain digest is invalid", snapshot.Version)
			load.InvalidFromVersion = expected
			break
		}
		var state State
		if err := json.Unmarshal(snapshot.StateJSON, &state); err != nil {
			load.WarningCode = CodeSnapshotVersion
			load.WarningDetail = fmt.Sprintf("snapshot %d state cannot be decoded", snapshot.Version)
			load.InvalidFromVersion = expected
			break
		}
		if err := validateState(state, snapshot.Version); err != nil {
			load.WarningCode = CodeSnapshotVersion
			load.WarningDetail = fmt.Sprintf("snapshot %d state is inconsistent: %v", snapshot.Version, err)
			load.InvalidFromVersion = expected
			break
		}
		if err := validateTransition(previousState, state, snapshot.Kind); err != nil {
			load.WarningCode = CodeSnapshotVersion
			load.WarningDetail = fmt.Sprintf("snapshot %d transition is inconsistent: %v", snapshot.Version, err)
			load.InvalidFromVersion = expected
			break
		}
		snapshot.State = state
		load.Valid = append(load.Valid, snapshot)
		load.State = state
		load.HasState = true
		prevDigest = snapshot.ChainDigest
		stateCopy := state
		previousState = &stateCopy
		expected++
	}
	if load.WarningCode != "" && repair {
		if err := s.deleteFrom(load.InvalidFromVersion); err != nil {
			return ChainLoad{}, err
		}
	}
	return load, nil
}

func validateTransition(previous *State, current State, kind string) error {
	if previous == nil {
		if kind != "policy" {
			return fmt.Errorf("first snapshot must be a policy snapshot")
		}
		if len(current.Results) != 0 {
			return fmt.Errorf("initial policy snapshot contains task results")
		}
		return nil
	}
	switch kind {
	case "policy", "policy-edit":
		if len(current.Results) != 0 {
			return fmt.Errorf("policy snapshot contains task results")
		}
		return nil
	case "task":
		if !reflect.DeepEqual(previous.Config, current.Config) || !reflect.DeepEqual(previous.Tasks, current.Tasks) {
			return fmt.Errorf("task snapshot changed policy or task definitions")
		}
		if len(current.Results) != len(previous.Results)+1 {
			return fmt.Errorf("task snapshot must append exactly one result")
		}
		for index, result := range previous.Results {
			if !reflect.DeepEqual(result, current.Results[index]) {
				return fmt.Errorf("task snapshot modified committed result %q", result.TaskID)
			}
		}
		added := current.Results[len(current.Results)-1]
		if added.SnapshotVersion != current.Version {
			return fmt.Errorf("new result %q has snapshot version %d, want %d", added.TaskID, added.SnapshotVersion, current.Version)
		}
		return nil
	default:
		return fmt.Errorf("unknown snapshot kind %q", kind)
	}
}

func validateState(state State, version int64) error {
	if state.Format != StateFormat {
		return fmt.Errorf("unexpected state format %q", state.Format)
	}
	if state.Version != version {
		return fmt.Errorf("embedded version %d does not match row version %d", state.Version, version)
	}
	if err := state.Config.Validate(); err != nil {
		return err
	}
	taskIDs := make(map[string]struct{}, len(state.Tasks))
	for _, task := range state.Tasks {
		if cleanTaskID(task.TaskID) == "" {
			return fmt.Errorf("empty task identifier")
		}
		if _, exists := taskIDs[task.TaskID]; exists {
			return fmt.Errorf("duplicate task identifier %q", task.TaskID)
		}
		taskIDs[task.TaskID] = struct{}{}
	}
	resultIDs := make(map[string]struct{}, len(state.Results))
	for _, result := range state.Results {
		if _, exists := taskIDs[result.TaskID]; !exists {
			return fmt.Errorf("result references unknown task %q", result.TaskID)
		}
		if _, exists := resultIDs[result.TaskID]; exists {
			return fmt.Errorf("duplicate result for task %q", result.TaskID)
		}
		resultIDs[result.TaskID] = struct{}{}
		if result.SnapshotVersion < 1 || result.SnapshotVersion > version {
			return fmt.Errorf("result snapshot version for %q is out of range", result.TaskID)
		}
	}
	return nil
}

func (s *Store) Commit(state State, kind string, expectedVersion int64) (Snapshot, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Snapshot{}, err
	}
	defer tx.Rollback()

	var maxVersion int64
	var previousDigest string
	err = tx.QueryRow(`SELECT version, chain_digest FROM snapshots ORDER BY version DESC LIMIT 1`).Scan(&maxVersion, &previousDigest)
	if err != nil && err != sql.ErrNoRows {
		return Snapshot{}, err
	}
	if err == sql.ErrNoRows {
		maxVersion = 0
		previousDigest = GenesisDigest
	}
	nextVersion := maxVersion + 1
	if expectedVersion > 0 && expectedVersion != nextVersion {
		return Snapshot{}, fmt.Errorf("snapshot version conflict: expected %d, next is %d", expectedVersion, nextVersion)
	}
	state.Version = nextVersion
	state.Format = StateFormat
	sort.Slice(state.Tasks, func(i, j int) bool { return state.Tasks[i].TaskID < state.Tasks[j].TaskID })
	sort.Slice(state.Results, func(i, j int) bool { return state.Results[i].TaskID < state.Results[j].TaskID })
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return Snapshot{}, err
	}
	stateHash := sha256.Sum256(stateJSON)
	stateSHA := hex.EncodeToString(stateHash[:])
	chainHash := sha256.Sum256([]byte(previousDigest + "\n" + stateSHA))
	chainDigest := hex.EncodeToString(chainHash[:])
	if _, err := tx.Exec(`INSERT INTO snapshots(version, kind, parent_digest, state_json, state_sha256, chain_digest, marker) VALUES(?, ?, ?, ?, ?, ?, ?)`,
		nextVersion, kind, previousDigest, stateJSON, stateSHA, chainDigest, DefaultSnapshot); err != nil {
		return Snapshot{}, err
	}
	if _, err := tx.Exec(`INSERT INTO meta(key, value) VALUES('head_version', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, fmt.Sprintf("%d", nextVersion)); err != nil {
		return Snapshot{}, err
	}
	if _, err := tx.Exec(`INSERT INTO meta(key, value) VALUES('head_digest', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, chainDigest); err != nil {
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, err
	}
	return Snapshot{
		Version:      nextVersion,
		Kind:         kind,
		ParentDigest: previousDigest,
		StateSHA256:  stateSHA,
		ChainDigest:  chainDigest,
		StateJSON:    stateJSON,
		State:        state,
	}, nil
}

func (s *Store) DeleteSnapshot(version int64) error {
	_, err := s.db.Exec(`DELETE FROM snapshots WHERE version = ?`, version)
	return err
}

func (s *Store) deleteFrom(version int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM snapshots WHERE version >= ?`, version); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM meta WHERE key IN ('head_version', 'head_digest')`); err != nil {
		return err
	}
	var maxVersion int64
	var headDigest string
	err = tx.QueryRow(`SELECT version, chain_digest FROM snapshots ORDER BY version DESC LIMIT 1`).Scan(&maxVersion, &headDigest)
	if err == sql.ErrNoRows {
		maxVersion = 0
		headDigest = GenesisDigest
	} else if err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO meta(key, value) VALUES('head_version', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, fmt.Sprintf("%d", maxVersion)); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO meta(key, value) VALUES('head_digest', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, headDigest); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Reset() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM snapshots`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM meta`); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) SnapshotCount(version int64) (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM snapshots WHERE version = ?`, version).Scan(&count)
	return count, err
}
