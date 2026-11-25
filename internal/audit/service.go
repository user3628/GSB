package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"

	"configaudit/internal/fixedpoint"
	"configaudit/internal/parser"
	"configaudit/internal/store"
)

// Service serializes audit/snapshot writers while allowing concurrent readers.
type Service struct {
	store *store.Store
	ds    *parser.Dataset

	writeMu sync.Mutex // single writer for audits and snapshot advancement

	// inFlight deduplicates concurrent identical request ids.
	inflightMu sync.Mutex
	inflight   map[string]*inflightCall
}

type inflightCall struct {
	done   chan struct{}
	result *store.Snapshot
	err    error
}

// NewService wires the store and the parsed deterministic dataset.
func NewService(st *store.Store, ds *parser.Dataset) *Service {
	return &Service{store: st, ds: ds, inflight: map[string]*inflightCall{}}
}

// Store exposes the persistence layer for read-only query endpoints.
func (s *Service) Store() *store.Store { return s.store }

// hashRequest produces a stable content hash for idempotency conflict checks.
func hashRequest(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// FullAudit runs a full audit and atomically advances the snapshot version.
// Repeated identical request ids execute once; differing content conflicts.
func (s *Service) FullAudit(ctx context.Context, requestID string, payload any) (*store.Snapshot, error) {
	reqHash, err := hashRequest(payload)
	if err != nil {
		return nil, err
	}

	// Fast path: already persisted.
	if existing, err := s.store.GetAudit(ctx, requestID); err != nil {
		return nil, err
	} else if existing != nil {
		if existing.RequestHash != reqHash {
			return nil, fixedpoint.NewError(fixedpoint.EConflictingRequest,
				"request id %s reused with different content", requestID)
		}
		return s.waitOrLoad(ctx, requestID, existing)
	}

	// Coalesce concurrent duplicates.
	s.inflightMu.Lock()
	if call, ok := s.inflight[requestID]; ok {
		s.inflightMu.Unlock()
		return s.joinCall(ctx, call, requestID, reqHash)
	}
	call := &inflightCall{done: make(chan struct{})}
	s.inflight[requestID] = call
	s.inflightMu.Unlock()

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	defer func() {
		s.inflightMu.Lock()
		delete(s.inflight, requestID)
		s.inflightMu.Unlock()
		close(call.done)
	}()

	// Re-check after acquiring the writer lock (a prior writer may have finished).
	if existing, err := s.store.GetAudit(ctx, requestID); err != nil {
		call.err = err
		return nil, err
	} else if existing != nil {
		if existing.RequestHash != reqHash {
			err := fixedpoint.NewError(fixedpoint.EConflictingRequest, "request id %s reused with different content", requestID)
			call.err = err
			return nil, err
		}
		snap, err := s.store.LoadSnapshot(ctx, existing.Version)
		call.result, call.err = snap, err
		return snap, err
	}

	if err := s.store.CreateAuditIntent(ctx, requestID, reqHash, "full", 0, 0); err != nil {
		call.err = err
		return nil, err
	}
	report, err := NewEngine(s.ds).Run()
	if err != nil {
		_ = s.store.FailAudit(ctx, requestID, string(fixedpoint.CodeOf(err)))
		call.err = err
		return nil, err
	}
	version, err := s.store.LatestSnapshotVersion(ctx)
	if err != nil {
		call.err = err
		return nil, err
	}
	snap := &store.Snapshot{
		Version:   version + 1,
		CreatedAt: nowRFC3339(),
		Report:    report,
	}
	if err := s.store.SaveSnapshot(ctx, snap); err != nil {
		_ = s.store.FailAudit(ctx, requestID, string(fixedpoint.CodeOf(err)))
		call.err = err
		return nil, err
	}
	if err := s.store.CompleteAudit(ctx, requestID, report, snap.Version); err != nil {
		call.err = err
		return nil, err
	}
	call.result = snap
	return snap, nil
}

// Incremental applies a fragment that must target the current base version.
func (s *Service) Incremental(ctx context.Context, requestID string, baseVersion int64, affectedKeys []string, payload any) (*store.Snapshot, error) {
	reqHash, err := hashRequest(payload)
	if err != nil {
		return nil, err
	}
	if existing, err := s.store.GetAudit(ctx, requestID); err != nil {
		return nil, err
	} else if existing != nil {
		if existing.RequestHash != reqHash {
			return nil, fixedpoint.NewError(fixedpoint.EConflictingRequest, "request id %s reused with different content", requestID)
		}
		return s.waitOrLoad(ctx, requestID, existing)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	current, err := s.store.LatestSnapshotVersion(ctx)
	if err != nil {
		return nil, err
	}
	if baseVersion != current {
		return nil, fixedpoint.NewError(fixedpoint.EStaleBase,
			"base version %d is not current %d", baseVersion, current)
	}
	report, err := s.runFragment(affectedKeys)
	if err != nil {
		return nil, err
	}
	newVersion, err := s.store.SaveIncremental(ctx, requestID, reqHash, baseVersion, report)
	if err != nil {
		return nil, err
	}
	return s.store.LoadSnapshot(ctx, newVersion)
}

// runFragment audits only the affected keys but keeps the full reference map.
func (s *Service) runFragment(keys []string) (*Report, error) {
	allow := map[string]bool{}
	for _, k := range keys {
		allow[k] = true
	}
	fragmentDS := *s.ds
	fragmentDS.Sources = nil
	for _, sl := range s.ds.Sources {
		if allow[sl.KeyPath] {
			fragmentDS.Sources = append(fragmentDS.Sources, sl)
		}
	}
	return NewEngine(&fragmentDS).Run()
}

// QuerySnapshot is a read-only operation and never takes the writer lock.
func (s *Service) QuerySnapshot(ctx context.Context, version int64) (*store.Snapshot, error) {
	if version == 0 {
		v, err := s.store.LatestSnapshotVersion(ctx)
		if err != nil {
			return nil, err
		}
		if v == 0 {
			return nil, fixedpoint.NewError(fixedpoint.ENotFound, "no snapshot available")
		}
		version = v
	}
	return s.store.LoadSnapshot(ctx, version)
}

// Recover delegates crash recovery to the store and may be called on startup.
func (s *Service) Recover(ctx context.Context) (*store.RecoveryReport, error) {
	return s.store.Recover(ctx)
}

func (s *Service) waitOrLoad(ctx context.Context, requestID string, row *store.AuditRow) (*store.Snapshot, error) {
	if row.Status == store.StatusRunning {
		s.inflightMu.Lock()
		call := s.inflight[requestID]
		s.inflightMu.Unlock()
		if call != nil {
			return s.joinCall(ctx, call, requestID, row.RequestHash)
		}
		// Running row without an in-process call means a crashed prior process.
		if err := s.store.FailAudit(ctx, requestID, store.StatusAborted); err != nil {
			return nil, err
		}
		return nil, fixedpoint.NewError(fixedpoint.ENotFound, "prior audit %s was interrupted and is aborted", requestID)
	}
	if row.Status == store.StatusAborted {
		return nil, fixedpoint.NewError(fixedpoint.ENotFound, "audit %s previously aborted; use a new request id", requestID)
	}
	return s.store.LoadSnapshot(ctx, row.Version)
}

func (s *Service) joinCall(ctx context.Context, call *inflightCall, requestID, reqHash string) (*store.Snapshot, error) {
	select {
	case <-call.done:
		if call.err != nil {
			return nil, call.err
		}
		return call.result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
