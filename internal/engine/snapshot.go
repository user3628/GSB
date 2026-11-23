package engine

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"sort"

	_ "modernc.org/sqlite"
)

// DeviceState is the committed state of one device inside a snapshot.
type DeviceState struct {
	TenantID      string   `json:"tenant_id"`
	DeviceID      string   `json:"device_id"`
	Fingerprint   string   `json:"fingerprint"`
	Capabilities  []string `json:"capabilities"`
	Score         int64    `json:"score"`
	ScoreVersion  int64    `json:"score_version"`
	CredDigest    string   `json:"cred_digest"`
	PolicyVersion int64    `json:"policy_version"`
}

// Snapshot is a whole-replacement, monotonically versioned authorization
// snapshot. Hash chains the previous snapshot hash so any wholesale
// replacement of stored content is detectable.
type Snapshot struct {
	Version  int64         `json:"version"`
	PrevHash string        `json:"prev_hash"`
	Devices  []DeviceState `json:"devices"`
}

// Hash computes the content hash over version, previous hash and the full
// device set. Comparing snapshots by Hash detects whole-content
// replacement, not just version-number drift.
func (s *Snapshot) Hash() string {
	payload, _ := json.Marshal(struct {
		Version  int64         `json:"version"`
		PrevHash string        `json:"prev_hash"`
		Devices  []DeviceState `json:"devices"`
	}{s.Version, s.PrevHash, s.Devices})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// SnapshotEqual reports whether two snapshots have identical content.
func SnapshotEqual(a, b *Snapshot) bool { return a.Hash() == b.Hash() }

// SnapshotRecord is one entry of the snapshot version chain in reports.
type SnapshotRecord struct {
	Version     int64  `json:"version"`
	PrevHash    string `json:"prev_hash"`
	ContentHash string `json:"content_hash"`
	Continuous  bool   `json:"continuous"`
}

// Store persists snapshots and device states in SQLite. Committed
// transactions survive process kills; uncommitted ones are discarded.
type Store struct {
	db *sql.DB
}

// OpenStore opens (and lazily creates) the store at path.
func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS snapshots(
		version INTEGER PRIMARY KEY,
		prev_hash TEXT NOT NULL,
		content_hash TEXT NOT NULL,
		payload TEXT NOT NULL
	)`); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS device_state(
		tenant_id TEXT NOT NULL,
		device_id TEXT NOT NULL,
		state TEXT NOT NULL,
		PRIMARY KEY(tenant_id, device_id)
	)`); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

// DB exposes the raw handle for crash-injection tests.
func (s *Store) DB() *sql.DB { return s.db }

// CommitSnapshot writes the snapshot row and replaces the whole device
// state table inside a single SQLite transaction.
func (s *Store) CommitSnapshot(snap *Snapshot) error {
	payload, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO snapshots(version, prev_hash, content_hash, payload) VALUES(?,?,?,?)`,
		snap.Version, snap.PrevHash, snap.Hash(), string(payload)); err != nil {
		tx.Rollback()
		return err
	}
	if _, err := tx.Exec(`DELETE FROM device_state`); err != nil {
		tx.Rollback()
		return err
	}
	for _, dev := range snap.Devices {
		raw, err := json.Marshal(dev)
		if err != nil {
			tx.Rollback()
			return err
		}
		if _, err := tx.Exec(`INSERT INTO device_state(tenant_id, device_id, state) VALUES(?,?,?)`,
			dev.TenantID, dev.DeviceID, string(raw)); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// InjectRawSnapshot writes an unverified snapshot row. It exists solely so
// that fault-injection scenarios (driven by external parameters) can plant
// version gaps or wholesale-replaced content.
func (s *Store) InjectRawSnapshot(version int64, prevHash, contentHash, payload string) error {
	_, err := s.db.Exec(`INSERT INTO snapshots(version, prev_hash, content_hash, payload) VALUES(?,?,?,?)`,
		version, prevHash, contentHash, payload)
	return err
}

// LoadChain verifies the version chain: versions must be contiguous from 1
// and every stored content hash must match the recomputed hash of the
// stored payload (whole-replacement detection). On the first break it
// returns E_SNAPSHOT_VERSION together with the latest continuous version.
func (s *Store) LoadChain() (records []SnapshotRecord, latestContinuous int64, headHash string, ferr *Error) {
	rows, err := s.db.Query(`SELECT version, prev_hash, content_hash, payload FROM snapshots ORDER BY version ASC`)
	if err != nil {
		return nil, 0, "", errf(CodeSnapshotVersion, "load chain: %v", err)
	}
	defer rows.Close()
	expected := int64(1)
	prevHash := ""
	for rows.Next() {
		var version int64
		var storedPrev, storedHash, payload string
		if err := rows.Scan(&version, &storedPrev, &storedHash, &payload); err != nil {
			return records, latestContinuous, prevHash, errf(CodeSnapshotVersion, "scan: %v", err)
		}
		rec := SnapshotRecord{Version: version, PrevHash: storedPrev, ContentHash: storedHash}
		var snap Snapshot
		if version != expected {
			records = append(records, rec)
			return records, latestContinuous, prevHash, errf(CodeSnapshotVersion,
				"version gap: expected %d got %d, latest continuous %d", expected, version, latestContinuous)
		}
		if err := json.Unmarshal([]byte(payload), &snap); err != nil || snap.Hash() != storedHash || storedPrev != prevHash {
			records = append(records, rec)
			return records, latestContinuous, prevHash, errf(CodeSnapshotVersion,
				"content replaced at version %d, latest continuous %d", version, latestContinuous)
		}
		rec.Continuous = true
		records = append(records, rec)
		prevHash = storedHash
		latestContinuous = version
		expected++
	}
	return records, latestContinuous, prevHash, nil
}

// LoadStates returns the committed device states.
func (s *Store) LoadStates() ([]DeviceState, error) {
	rows, err := s.db.Query(`SELECT state FROM device_state ORDER BY tenant_id ASC, device_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeviceState
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var st DeviceState
		if err := json.Unmarshal([]byte(raw), &st); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// SortedStates flattens the state map into a deterministic slice.
func SortedStates(states map[string]*DeviceState) []DeviceState {
	out := make([]DeviceState, 0, len(states))
	for _, st := range states {
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TenantID != out[j].TenantID {
			return out[i].TenantID < out[j].TenantID
		}
		return out[i].DeviceID < out[j].DeviceID
	})
	return out
}
