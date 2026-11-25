package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"alarmengine/internal/errcode"
	"alarmengine/internal/model"
	"alarmengine/internal/sample"
	"alarmengine/internal/storage"
)

// IngestRequest is an idempotent observation batch.
type IngestRequest struct {
	RequestID    string              `json:"request_id"`
	BaseVersion  int64               `json:"base_version"`
	Observations []model.Observation `json:"observations"`
}

// IngestResult is the committed result returned (and cached) per request id.
type IngestResult struct {
	RequestID     string `json:"request_id"`
	CommitVersion int64  `json:"commit_version"`
	Inserted      int    `json:"inserted"`
	Alerts        int    `json:"alerts"`
	Suppressed    int    `json:"suppressed"`
	RootCauses    int    `json:"root_causes"`
	BatchID       string `json:"batch_id"`
}

// IngestObservations is the single-writer incremental commit path. It:
//  1. validates the whole batch (failure -> E_SAMPLE_INVALID, nothing stored),
//  2. checks the base version (mismatch -> E_STALE_BASE, nothing changed),
//  3. enforces idempotency by request id (conflicting payload ->
//     E_CONFLICTING_REQUEST),
//  4. writes a durable STARTED intent before domain rows,
//  5. atomically inserts observations and rebuilds derived structures,
//  6. marks the intent COMMITTED and caches the result.
func (e *Engine) IngestObservations(req IngestRequest) (IngestResult, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	if req.RequestID == "" {
		return IngestResult{}, errcode.New(errcode.ESampleInvalid, "missing request_id")
	}
	payloadHash := hashPayload(req)
	if cached, conflict, err := e.checkIdempotent(req.RequestID, "ingest", payloadHash); err != nil {
		return IngestResult{}, err
	} else if conflict != nil {
		return IngestResult{}, conflict
	} else if cached != nil {
		var res IngestResult
		if err := json.Unmarshal([]byte(*cached), &res); err == nil {
			return res, nil
		}
	}

	current := e.Version()
	if req.BaseVersion != current {
		return IngestResult{}, errcode.New(errcode.EStaleBase,
			"base version %d != committed version %d", req.BaseVersion, current)
	}

	known := map[string]bool{}
	for id := range e.services {
		known[id] = true
	}
	if err := model.ValidateObservations(req.Observations, known); err != nil {
		return IngestResult{}, err
	}

	now := e.cfg.Now()
	intentID := e.nextIntentID("intent-ingest-" + req.RequestID)
	batchID := "batch-" + intentID
	tx, err := e.store.BeginWrite()
	if err != nil {
		return IngestResult{}, err
	}
	if err := storage.InsertIntent(tx, intentID, storage.IntentIngestObservations, current, req, now); err != nil {
		_ = tx.Rollback()
		return IngestResult{}, err
	}
	if err := storage.InsertRequest(tx, req.RequestID, "ingest", payloadHash, now); err != nil {
		_ = tx.Rollback()
		return IngestResult{}, err
	}
	if err := tx.Commit(); err != nil { // flush durable intent before domain writes
		return IngestResult{}, err
	}
	e.maybeCrash(CrashAfterIntentIngest, intentID)

	// Second transaction: domain rows + atomic derived rebuild under one
	// version bump.
	newVersion := current + 1
	tx, err = e.store.BeginWrite()
	if err != nil {
		return IngestResult{}, err
	}
	commitErr := func() error {
		var seqs []int64
		for _, o := range req.Observations {
			seq, err := storage.InsertObservation(tx, o, batchID, req.BaseVersion, newVersion)
			if err != nil {
				return err
			}
			seqs = append(seqs, seq)
		}
		if err := e.rebuildDerivedInTx(tx, newVersion); err != nil {
			return err
		}
		if err := storage.SetMeta(tx, storage.MetaCommittedVersion, fmt.Sprintf("%d", newVersion)); err != nil {
			return err
		}
		if err := storage.SetMeta(tx, storage.MetaLastCompleteVer, fmt.Sprintf("%d", newVersion)); err != nil {
			return err
		}
		if err := storage.CompleteIntent(tx, intentID); err != nil {
			return err
		}
		res := IngestResult{
			RequestID:     req.RequestID,
			CommitVersion: newVersion,
			Inserted:      len(req.Observations),
			BatchID:       batchID,
		}
		// Fill counts from freshly rebuilt tables.
		alerts, err := storage.LoadAlerts(tx, newVersion)
		if err != nil {
			return err
		}
		res.Alerts = len(alerts)
		for _, a := range alerts {
			if a.State == model.StateSuppressed {
				res.Suppressed++
			}
		}
		scores, err := storage.LoadRootCauseScores(tx, newVersion)
		if err != nil {
			return err
		}
		res.RootCauses = len(scores)
		raw, _ := json.Marshal(res)
		return storage.CompleteRequest(tx, req.RequestID, string(raw))
	}()
	if commitErr != nil {
		_ = tx.Rollback()
		return IngestResult{}, commitErr
	}
	if err := tx.Commit(); err != nil {
		return IngestResult{}, err
	}

	e.mu.Lock()
	e.version = newVersion
	e.lastGood = newVersion
	e.mu.Unlock()
	if err := e.reloadDerivedCache(); err != nil {
		return IngestResult{}, err
	}
	return IngestResult{
		RequestID:     req.RequestID,
		CommitVersion: newVersion,
		Inserted:      len(req.Observations),
		BatchID:       batchID,
		Alerts:        e.countAlerts(newVersion),
		Suppressed:    e.countSuppressed(newVersion),
		RootCauses:    e.countScores(newVersion),
	}, nil
}

func (e *Engine) checkIdempotent(requestID, kind, payloadHash string) (*string, error, error) {
	rec, exists, err := storage.LookupRequest(e.store.ReadDB(), requestID)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, nil
	}
	if rec.PayloadHash != payloadHash {
		return nil, errcode.New(errcode.EConflictingRequest,
			"request id %s reused with different payload", requestID), nil
	}
	if rec.Status == storage.RequestCompleted {
		return &rec.ResultJSON, nil, nil
	}
	// A STARTED request left by a crash will be reconciled by recovery; treat
	// replay while unfinished as conflict to preserve single execution.
	return nil, errcode.New(errcode.EConflictingRequest,
		"request id %s is still in flight", requestID), nil
}

func hashPayload(v any) string {
	raw, _ := json.Marshal(v)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (e *Engine) countAlerts(version int64) int {
	rows, err := storage.LoadAlerts(e.store.ReadDB(), version)
	if err != nil {
		return 0
	}
	return len(rows)
}

func (e *Engine) countSuppressed(version int64) int {
	rows, err := storage.LoadAlerts(e.store.ReadDB(), version)
	if err != nil {
		return 0
	}
	n := 0
	for _, a := range rows {
		if a.State == model.StateSuppressed {
			n++
		}
	}
	return n
}

func (e *Engine) countScores(version int64) int {
	rows, err := storage.LoadRootCauseScores(e.store.ReadDB(), version)
	if err != nil {
		return 0
	}
	return len(rows)
}

func (e *Engine) reloadDerivedCache() error {
	// Derived data is read directly from SQLite by version; the in-memory
	// cache only needs policy/services which change through their own methods.
	return nil
}

// BootstrapServices validates and stores the initial service catalog as one
// batch. Any validation failure returns E_SAMPLE_INVALID and stores nothing.
func (e *Engine) BootstrapServices(services []model.Service) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if err := model.ValidateServices(services); err != nil {
		return err
	}
	if existing, _ := storage.LoadServices(e.store.ReadDB()); len(existing) > 0 {
		return nil // idempotent bootstrap on reopen
	}
	now := e.cfg.Now()
	intentID := e.nextIntentID("intent-services")
	tx, err := e.store.BeginWrite()
	if err != nil {
		return err
	}
	if err := storage.InsertIntent(tx, intentID, storage.IntentRebuildDerived, 0, services, now); err != nil {
		_ = tx.Rollback()
		return err
	}
	for _, s := range services {
		if err := storage.UpsertService(tx, s, 0); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err := storage.CompleteIntent(tx, intentID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	e.mu.Lock()
	e.services = map[string]model.Service{}
	for _, s := range services {
		e.services[s.ID] = s
	}
	e.topology = model.NewTopology(services)
	e.mu.Unlock()
	return nil
}

// BootstrapPolicy validates checksum/monotonicity and installs the first
// snapshot without going through the replacement path (no prior snapshot).
func (e *Engine) BootstrapPolicy(p model.PolicySnapshot) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if err := model.ValidatePolicySnapshot(p); err != nil {
		return err
	}
	if got := sample.ComputeChecksum(p); got != p.Checksum {
		return errcode.New(errcode.ESnapshotInvalid, "checksum mismatch")
	}
	maxVer, _ := storage.MaxPolicyVersion(e.store.ReadDB())
	if maxVer > 0 {
		// Idempotent bootstrap: an engine reopened on an existing database must
		// not reject its own fixed-seed baseline snapshot.
		row, err := storage.GetPolicyRow(e.store.ReadDB(), maxVer)
		if err == nil && row.Checksum == p.Checksum && maxVer == p.Version {
			return nil
		}
		return errcode.New(errcode.ESnapshotInvalid,
			"snapshot version %d not greater than max %d", p.Version, maxVer)
	}
	return e.installSnapshotLocked(p)
}
