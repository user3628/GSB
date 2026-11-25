package engine

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/gofrs/flock"
	_ "modernc.org/sqlite"
)

type Engine struct {
	db       *sql.DB
	cfg      Config
	path     string
	lockPath string
}

type querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
	batch_id TEXT PRIMARY KEY,
	batch_index INTEGER NOT NULL,
	seq_start INTEGER NOT NULL,
	seq_end INTEGER NOT NULL,
	digest TEXT NOT NULL,
	status TEXT NOT NULL,
	created_logical INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS samples (
	seq INTEGER PRIMARY KEY,
	id TEXT NOT NULL UNIQUE,
	batch_id TEXT NOT NULL,
	batch_index INTEGER NOT NULL,
	label INTEGER NOT NULL,
	text TEXT NOT NULL,
	fields_json TEXT NOT NULL,
	revision INTEGER NOT NULL,
	content_digest TEXT NOT NULL,
	vector_json TEXT NOT NULL,
	vector_digest TEXT NOT NULL,
	active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS sample_revisions (
	id TEXT NOT NULL,
	revision INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	label INTEGER NOT NULL,
	text TEXT NOT NULL,
	fields_json TEXT NOT NULL,
	content_digest TEXT NOT NULL,
	vector_json TEXT NOT NULL,
	vector_digest TEXT NOT NULL,
	change_kind TEXT NOT NULL,
	PRIMARY KEY(id, revision)
);
CREATE TABLE IF NOT EXISTS sample_units (
	sample_seq INTEGER NOT NULL,
	unit_id INTEGER NOT NULL,
	dims_json TEXT NOT NULL,
	PRIMARY KEY(sample_seq, unit_id)
);
CREATE TABLE IF NOT EXISTS units (
	unit_id INTEGER PRIMARY KEY,
	current_version INTEGER NOT NULL,
	state_digest TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS unit_versions (
	unit_id INTEGER NOT NULL,
	version INTEGER NOT NULL,
	model_version INTEGER NOT NULL,
	sample_seq INTEGER NOT NULL,
	batch_id TEXT NOT NULL,
	state_json TEXT NOT NULL,
	update_json TEXT NOT NULL,
	state_digest TEXT NOT NULL,
	valid INTEGER NOT NULL DEFAULT 1,
	is_current INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY(unit_id, version)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unit_version_valid
	ON unit_versions(unit_id, version) WHERE valid=1;
CREATE UNIQUE INDEX IF NOT EXISTS idx_unit_current
	ON unit_versions(unit_id) WHERE is_current=1;
CREATE TABLE IF NOT EXISTS model_versions (
	version INTEGER PRIMARY KEY,
	generation INTEGER NOT NULL,
	event_sequence INTEGER NOT NULL,
	batch_id TEXT NOT NULL DEFAULT '',
	sample_id TEXT NOT NULL DEFAULT '',
	kind TEXT NOT NULL,
	fingerprint TEXT NOT NULL,
	unit_map_json TEXT NOT NULL,
	dataset_digest TEXT NOT NULL,
	config_digest TEXT NOT NULL,
	parent_version INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS inference_cache (
	cache_key TEXT PRIMARY KEY,
	request_id TEXT NOT NULL,
	model_version INTEGER NOT NULL,
	unit_id INTEGER NOT NULL DEFAULT -1,
	unit_version INTEGER NOT NULL DEFAULT 0,
	score_q INTEGER NOT NULL,
	score REAL NOT NULL,
	result_json TEXT NOT NULL,
	fingerprint TEXT NOT NULL,
	valid INTEGER NOT NULL DEFAULT 1,
	created_logical INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS inference_dependencies (
	cache_key TEXT NOT NULL,
	unit_id INTEGER NOT NULL,
	unit_version INTEGER NOT NULL,
	model_version INTEGER NOT NULL,
	PRIMARY KEY(cache_key, unit_id)
);
CREATE TABLE IF NOT EXISTS version_events (
	event_seq INTEGER PRIMARY KEY,
	kind TEXT NOT NULL,
	generation INTEGER NOT NULL,
	dataset_digest TEXT NOT NULL,
	config_digest TEXT NOT NULL,
	payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_reports (
	report_id TEXT PRIMARY KEY,
	kind TEXT NOT NULL,
	status TEXT NOT NULL,
	affected_units_json TEXT NOT NULL,
	report_json TEXT NOT NULL,
	first_diff_byte INTEGER NOT NULL DEFAULT -1,
	created_logical INTEGER NOT NULL
);
`

func OpenDB(path string, cfg Config) (*Engine, error) {
	cfg = cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	e := &Engine{db: db, cfg: cfg, path: path, lockPath: path + ".lock"}
	if filepath.Base(path) == ":memory:" || path == ":memory:" {
		e.lockPath = filepath.Join(".", "memory-engine.lock")
	}
	if err := e.configure(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate sqlite: %w", err)
	}
	if err := e.initMeta(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return e, nil
}

func (e *Engine) configure() error {
	pragmas := []string{
		"PRAGMA busy_timeout=5000",
		"PRAGMA journal_mode=DELETE",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
	}
	for _, p := range pragmas {
		if _, err := e.db.Exec(p); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
	}
	return nil
}

func (e *Engine) Close() error { return e.db.Close() }

// DB exposes the database handle to integration tests and advanced callers.
func (e *Engine) DB() *sql.DB { return e.db }

func (e *Engine) Config() Config { return e.cfg }

func (e *Engine) withLock(fn func() error) error {
	fl := flock.New(e.lockPath)
	if err := fl.Lock(); err != nil {
		return &Error{Code: CodeLockUnavailable, Message: err.Error()}
	}
	defer func() { _ = fl.Unlock() }()
	return fn()
}

func getMetaTx(q querier, key string) (string, bool, error) {
	var value string
	err := q.QueryRow("SELECT value FROM meta WHERE key=?", key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func setMetaTx(q querier, key, value string) error {
	_, err := q.Exec(`INSERT INTO meta(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

func (e *Engine) initMeta() error {
	tx, err := e.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	existing, ok, err := getMetaTx(tx, "config_json")
	if err != nil {
		return err
	}
	if ok {
		var stored Config
		if err := json.Unmarshal([]byte(existing), &stored); err != nil {
			return err
		}
		e.cfg = stored.Normalize()
	} else {
		b, err := CanonicalJSON(e.cfg)
		if err != nil {
			return err
		}
		digest, err := e.cfg.Digest()
		if err != nil {
			return err
		}
		if err := setMetaTx(tx, "config_json", string(b)); err != nil {
			return err
		}
		if err := setMetaTx(tx, "config_digest", digest); err != nil {
			return err
		}
		if err := setMetaTx(tx, "generation", "1"); err != nil {
			return err
		}
		if err := setMetaTx(tx, "current_model_version", "0"); err != nil {
			return err
		}
		if err := setMetaTx(tx, "event_sequence", "0"); err != nil {
			return err
		}
		if err := setMetaTx(tx, "logical_clock", "0"); err != nil {
			return err
		}
		cp, err := NewCheckpoint(e.cfg, "", 1)
		if err != nil {
			return err
		}
		cpb, err := json.Marshal(cp)
		if err != nil {
			return err
		}
		if err := setMetaTx(tx, "checkpoint_json", string(cpb)); err != nil {
			return err
		}
		for _, u := range cp.Units {
			if _, err := tx.Exec("INSERT OR IGNORE INTO units(unit_id,current_version,state_digest) VALUES(?,?,?)", u.UnitID, 0, u.StateDigest); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func loadCheckpointTx(q querier) (*Checkpoint, error) {
	raw, ok, err := getMetaTx(q, "checkpoint_json")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, newError(CodeNotFound, "checkpoint_json is not initialized")
	}
	var cp Checkpoint
	if err := json.Unmarshal([]byte(raw), &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

func saveCheckpointTx(q querier, cp *Checkpoint) error {
	if err := cp.RefreshFingerprint(); err != nil {
		return err
	}
	b, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	return setMetaTx(q, "checkpoint_json", string(b))
}

func nextLogicalTx(tx *sql.Tx) (int64, error) {
	raw, ok, err := getMetaTx(tx, "logical_clock")
	if err != nil {
		return 0, err
	}
	var v int64
	if ok {
		_, _ = fmt.Sscan(raw, &v)
	}
	v++
	return v, setMetaTx(tx, "logical_clock", fmt.Sprint(v))
}

type ImportReport struct {
	BatchID          string `json:"batch_id"`
	BatchIndex       int    `json:"batch_index"`
	SampleCount      int    `json:"sample_count"`
	StartSeq         int64  `json:"start_seq"`
	EndSeq           int64  `json:"end_seq"`
	ModelVersion     int64  `json:"model_version"`
	Fingerprint      string `json:"fingerprint"`
	AlreadyCommitted bool   `json:"already_committed"`
}

func (e *Engine) ImportBatch(b Batch) (*ImportReport, error) {
	var report *ImportReport
	err := e.withLock(func() error {
		tx, err := e.db.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		var existingSeqStart, existingSeqEnd int64
		var existingDigest string
		err = tx.QueryRow("SELECT seq_start,seq_end,digest FROM batches WHERE batch_id=?", b.ID).Scan(&existingSeqStart, &existingSeqEnd, &existingDigest)
		if err == nil {
			var fp string
			_ = tx.QueryRow("SELECT value FROM meta WHERE key='current_fingerprint'").Scan(&fp)
			report = &ImportReport{BatchID: b.ID, BatchIndex: b.Index, SampleCount: len(b.Samples), StartSeq: existingSeqStart, EndSeq: existingSeqEnd, ModelVersion: existingSeqEnd, Fingerprint: fp, AlreadyCommitted: true}
			return nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		cp, err := loadCheckpointTx(tx)
		if err != nil {
			return err
		}
		startSeq := cp.EventSequence + 1
		seq := cp.EventSequence
		for _, in := range b.Samples {
			seq++
			modelVersion := seq
			contentDigest, err := SampleContentDigest(in)
			if err != nil {
				return err
			}
			updates, fv, err := TrainSample(cp, in, TrainingMeta{
				Seq:          seq,
				BatchID:      b.ID,
				ModelVersion: modelVersion,
				Generation:   cp.Generation,
			})
			if err != nil {
				return err
			}
			vectorJSON, err := CanonicalJSON(fv)
			if err != nil {
				return err
			}
			fieldsJSON, err := CanonicalJSON(in.Fields)
			if err != nil {
				return err
			}
			_, err = tx.Exec(`INSERT INTO samples
				(seq,id,batch_id,batch_index,label,text,fields_json,revision,content_digest,vector_json,vector_digest,active)
				VALUES(?,?,?,?,?,?,?,1,?,?,?,1)`,
				seq, in.ID, b.ID, b.Index, in.Label, in.Text, string(fieldsJSON), contentDigest, string(vectorJSON), fv.Digest)
			if err != nil {
				return err
			}
			_, err = tx.Exec(`INSERT INTO sample_revisions
				(id,revision,seq,label,text,fields_json,content_digest,vector_json,vector_digest,change_kind)
				VALUES(?,1,?,?,?,?,?,?,?,'arrival')`,
				in.ID, seq, in.Label, in.Text, string(fieldsJSON), contentDigest, string(vectorJSON), fv.Digest)
			if err != nil {
				return err
			}

			unitDims := map[int][]int{}
			for _, f := range fv.Features {
				u := UnitOfDim(f.Dim, cp.Config.Units)
				unitDims[u] = append(unitDims[u], f.Dim)
			}
			unitIDs := make([]int, 0, len(unitDims))
			for u := range unitDims {
				unitIDs = append(unitIDs, u)
			}
			sortInts(unitIDs)
			for _, u := range unitIDs {
				dims := unitDims[u]
				dimsJSON, _ := CanonicalJSON(dims)
				if _, err := tx.Exec("INSERT INTO sample_units(sample_seq,unit_id,dims_json) VALUES(?,?,?)", seq, u, string(dimsJSON)); err != nil {
					return err
				}
			}

			unitMap := make([]map[string]any, 0, len(updates))
			for _, up := range updates {
				ui := cp.unitIndex(up.UnitID)
				unitCP := cp.Units[ui]
				stateJSON, err := CanonicalJSON(unitCP)
				if err != nil {
					return err
				}
				updateJSON, err := CanonicalJSON(up)
				if err != nil {
					return err
				}
				if _, err := tx.Exec("UPDATE unit_versions SET is_current=0 WHERE unit_id=? AND is_current=1", up.UnitID); err != nil {
					return err
				}
				_, err = tx.Exec(`INSERT INTO unit_versions
					(unit_id,version,model_version,sample_seq,batch_id,state_json,update_json,state_digest,valid,is_current)
					VALUES(?,?,?,?,?,?,?,?,1,1)`,
					up.UnitID, up.UnitVersion, up.ModelVersion, up.SampleSeq, up.BatchID, string(stateJSON), string(updateJSON), unitCP.StateDigest)
				if err != nil {
					return err
				}
				if _, err := tx.Exec(`INSERT INTO units(unit_id,current_version,state_digest) VALUES(?,?,?)
					ON CONFLICT(unit_id) DO UPDATE SET current_version=excluded.current_version,state_digest=excluded.state_digest`,
					up.UnitID, up.UnitVersion, unitCP.StateDigest); err != nil {
					return err
				}
				unitMap = append(unitMap, map[string]any{"unit_id": up.UnitID, "version": up.UnitVersion, "digest": unitCP.StateDigest})
			}
			currentSamples, err := listSamplesTx(tx)
			if err != nil {
				return err
			}
			cp.DatasetDigest = DatasetDigest(currentSamples)
			if err := cp.RefreshFingerprint(); err != nil {
				return err
			}
			unitMapJSON, _ := CanonicalJSON(unitMap)
			_, err = tx.Exec(`INSERT INTO model_versions
				(version,generation,event_sequence,batch_id,sample_id,kind,fingerprint,unit_map_json,dataset_digest,config_digest,parent_version)
				VALUES(?,?,?,?,?,'sample',?,?,?,?,?)`,
				modelVersion, cp.Generation, seq, b.ID, in.ID, cp.Fingerprint, string(unitMapJSON), cp.DatasetDigest, cp.ConfigDigest, modelVersion-1)
			if err != nil {
				return err
			}
		}

		samples, err := listSamplesTx(tx)
		if err != nil {
			return err
		}
		datasetDigest := DatasetDigest(samples)
		cp.DatasetDigest = datasetDigest
		if err := cp.RefreshFingerprint(); err != nil {
			return err
		}
		if err := saveCheckpointTx(tx, cp); err != nil {
			return err
		}
		endSeq := cp.EventSequence
		batchDigest := DigestBytes([]byte(b.ID + ":" + fmt.Sprint(startSeq) + ":" + fmt.Sprint(endSeq) + ":" + datasetDigest))
		logical, err := nextLogicalTx(tx)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT INTO batches(batch_id,batch_index,seq_start,seq_end,digest,status,created_logical)
			VALUES(?,?,?,?,?,'committed',?)`, b.ID, b.Index, startSeq, endSeq, batchDigest, logical)
		if err != nil {
			return err
		}
		if err := setMetaTx(tx, "dataset_digest", datasetDigest); err != nil {
			return err
		}
		if err := setMetaTx(tx, "current_model_version", fmt.Sprint(cp.ModelVersion)); err != nil {
			return err
		}
		if err := setMetaTx(tx, "current_fingerprint", cp.Fingerprint); err != nil {
			return err
		}
		if err := setMetaTx(tx, "event_sequence", fmt.Sprint(cp.EventSequence)); err != nil {
			return err
		}
		if err := insertVersionEventTx(tx, cp.EventSequence, "normal_progress", cp.Generation, datasetDigest, cp.ConfigDigest, map[string]any{"batch_id": b.ID}); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		report = &ImportReport{BatchID: b.ID, BatchIndex: b.Index, SampleCount: len(b.Samples), StartSeq: startSeq, EndSeq: endSeq, ModelVersion: cp.ModelVersion, Fingerprint: cp.Fingerprint}
		return nil
	})
	return report, err
}

func sortInts(in []int) {
	for i := 1; i < len(in); i++ {
		for j := i; j > 0 && in[j-1] > in[j]; j-- {
			in[j-1], in[j] = in[j], in[j-1]
		}
	}
}

func insertVersionEventTx(tx querier, seq int64, kind string, generation int64, datasetDigest, configDigest string, payload any) error {
	raw, err := CanonicalJSON(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT OR REPLACE INTO version_events(event_seq,kind,generation,dataset_digest,config_digest,payload_json)
		VALUES(?,?,?,?,?,?)`, seq, kind, generation, datasetDigest, configDigest, string(raw))
	return err
}

func scanSample(scanner interface{ Scan(...any) error }) (StoredSample, error) {
	var s StoredSample
	var fieldsJSON string
	var vectorJSON string
	err := scanner.Scan(&s.Seq, &s.ID, &s.BatchID, &s.BatchIndex, &s.Label, &s.Text, &fieldsJSON, &s.Revision, &s.ContentDigest, &vectorJSON, &s.VectorDigest, new(int))
	if err != nil {
		return s, err
	}
	if fieldsJSON != "" && fieldsJSON != "null" {
		if err := json.Unmarshal([]byte(fieldsJSON), &s.Fields); err != nil {
			return s, err
		}
	}
	return s, nil
}

const sampleColumns = `seq,id,batch_id,batch_index,label,text,fields_json,revision,content_digest,vector_json,vector_digest,active`

func listSamplesTx(q querier) ([]StoredSample, error) {
	rows, err := q.Query("SELECT " + sampleColumns + " FROM samples WHERE active=1 ORDER BY seq ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StoredSample
	for rows.Next() {
		s, err := scanSample(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (e *Engine) ListSamples() ([]StoredSample, error) {
	return listSamplesTx(e.db)
}

func (e *Engine) LoadCheckpoint() (*Checkpoint, error) {
	return loadCheckpointTx(e.db)
}

func (e *Engine) CommittedBatches() ([]string, error) {
	rows, err := e.db.Query("SELECT batch_id FROM batches WHERE status='committed' ORDER BY batch_index ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

type RecoveryReport struct {
	CommittedBatches []string `json:"committed_batches"`
	LastSequence     int64    `json:"last_sequence"`
	ModelVersion     int64    `json:"model_version"`
	Fingerprint      string   `json:"fingerprint"`
	Generation       int64    `json:"generation"`
}

func (e *Engine) Recover() (*RecoveryReport, error) {
	batches, err := e.CommittedBatches()
	if err != nil {
		return nil, err
	}
	cp, err := e.LoadCheckpoint()
	if err != nil {
		return nil, err
	}
	return &RecoveryReport{CommittedBatches: batches, LastSequence: cp.EventSequence, ModelVersion: cp.ModelVersion, Fingerprint: cp.Fingerprint, Generation: cp.Generation}, nil
}
