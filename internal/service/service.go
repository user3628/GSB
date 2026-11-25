// Package service orchestrates manifest loading, replica report intake,
// full and incremental verification, idempotent request handling and
// crash recovery on top of the SQLite intent store.
package service

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"blockinspect/internal/cerr"
	"blockinspect/internal/manifest"
	"blockinspect/internal/sample"
	"blockinspect/internal/store"
	"blockinspect/internal/trust"
	"blockinspect/internal/verify"
)

// blockState is the in-memory, single-writer state of one block.
type blockState struct {
	mu        sync.Mutex // single writer: serializes verification per block
	manifest  *manifest.Manifest
	sampleDir string
	reports   map[string]verify.Report
	lastInc   *verify.IncrementalResult
}

// Service is the domain entry point used by the HTTP API.
type Service struct {
	st     *store.Store
	mu     sync.Mutex
	blocks map[string]*blockState
	// inflight tracks running idempotent requests so duplicates wait and
	// reuse the result instead of re-verifying.
	inflight map[string]*inflightRequest
}

type inflightRequest struct {
	done   chan struct{}
	result []byte
	err    error
}

// New creates a Service on top of the store.
func New(st *store.Store) *Service {
	return &Service{st: st, blocks: map[string]*blockState{}, inflight: map[string]*inflightRequest{}}
}

// Recover runs startup recovery and returns the persisted-facts report.
func (s *Service) Recover() (*store.RecoveryReport, error) { return s.st.Recover() }

func (s *Service) block(id string) *blockState {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blocks[id]
	if !ok {
		b = &blockState{reports: map[string]verify.Report{}}
		s.blocks[id] = b
	}
	return b
}

// LoadManifest parses and validates a manifest file. On E_MANIFEST_INVALID
// the previously loaded manifest (if any) stays in effect.
func (s *Service) LoadManifest(path string) (*manifest.Manifest, error) {
	m, err := manifest.LoadFile(path)
	if err != nil {
		return nil, err
	}
	b := s.block(m.BlockID)
	b.mu.Lock()
	b.manifest = m
	b.sampleDir = filepath.Dir(path)
	b.mu.Unlock()
	return m, nil
}

// Manifest returns the currently loaded manifest of a block.
func (s *Service) Manifest(blockID string) (*manifest.Manifest, error) {
	b := s.block(blockID)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.manifest == nil {
		return nil, cerr.New(cerr.NotFound, "no manifest loaded for block", "block_id", blockID)
	}
	return b.manifest, nil
}

// SubmitReport registers one replica's digest report for a block.
func (s *Service) SubmitReport(rep *sample.ReplicaReport) error {
	b := s.block(rep.BlockID)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.manifest == nil {
		return cerr.New(cerr.NotFound, "no manifest loaded for block", "block_id", rep.BlockID)
	}
	if len(rep.PageDigests) != b.manifest.PageCount {
		return cerr.New(cerr.PageMismatch, "report page digest count differs from manifest",
			"replica", rep.ReplicaID,
			"expected_length", b.manifest.PageCount, "actual_length", len(rep.PageDigests))
	}
	vr := verify.Report{ReplicaID: rep.ReplicaID}
	for i, h := range rep.PageDigests {
		d, err := manifest.ParseDigest(h)
		if err != nil {
			return cerr.New(cerr.PageMismatch, "report page digest is not 32 bytes",
				"replica", rep.ReplicaID, "page", i, "detail", err.Error())
		}
		vr.PageDigests = append(vr.PageDigests, d)
	}
	root, err := manifest.ParseDigest(rep.RootDigest)
	if err != nil {
		return cerr.New(cerr.PageMismatch, "report root digest is not 32 bytes",
			"replica", rep.ReplicaID, "detail", err.Error())
	}
	vr.RootDigest = root
	b.reports[rep.ReplicaID] = vr
	return nil
}

// VerifyRequest triggers one idempotent verification.
type VerifyRequest struct {
	RequestID    string `json:"request_id"`
	BlockID      string `json:"block_id"`
	Mode         string `json:"mode"` // "full" or "incremental"
	CheckpointID *int64 `json:"checkpoint_id,omitempty"`
	ChangedPages []int  `json:"changed_pages,omitempty"`
	// DebugCrashAfterIntent, when true, kills the process right after the
	// intent record is committed. Used only by the local verification
	// tool to rehearse hard-kill recovery.
	DebugCrashAfterIntent bool `json:"debug_crash_after_intent,omitempty"`
}

// VerifyResult is the full JSON-able outcome of one verification.
type VerifyResult struct {
	RequestID    string                    `json:"request_id"`
	BlockID      string                    `json:"block_id"`
	Mode         string                    `json:"mode"`
	CheckpointID int64                     `json:"checkpoint_id"`
	Full         *verify.FullResult        `json:"full,omitempty"`
	Consensus    *verify.ConsensusResult   `json:"consensus,omitempty"`
	Incremental  *verify.IncrementalResult `json:"incremental,omitempty"`
	Trust        *trust.TrustReport        `json:"trust,omitempty"`
	Damage       *trust.DamageRange        `json:"damage,omitempty"`
}

func hashPayload(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Verify runs one verification under the block's single-writer lock with
// intent-first persistence. Duplicate request IDs with identical payloads
// wait for and reuse the in-flight or persisted result; conflicting
// payloads fail with E_CONFLICTING_REQUEST.
func (s *Service) Verify(req *VerifyRequest) (*VerifyResult, error) {
	if req.RequestID == "" || req.BlockID == "" {
		return nil, cerr.New(cerr.BadRequest, "request_id and block_id are required")
	}
	if req.Mode != "full" && req.Mode != "incremental" {
		return nil, cerr.New(cerr.BadRequest, "mode must be full or incremental", "mode", req.Mode)
	}
	payloadHash := hashPayload(struct {
		BlockID      string `json:"block_id"`
		Mode         string `json:"mode"`
		CheckpointID *int64 `json:"checkpoint_id,omitempty"`
		ChangedPages []int  `json:"changed_pages,omitempty"`
	}{req.BlockID, req.Mode, req.CheckpointID, req.ChangedPages})

	// idempotency: existing request with the same ID?
	if prev, err := s.st.GetRequest(req.RequestID); err == nil {
		if prev.PayloadHash != payloadHash {
			return nil, cerr.New(cerr.ConflictingRequest,
				"request_id reused with different payload",
				"request_id", req.RequestID)
		}
		if prev.Status == "done" {
			var res VerifyResult
			if err := json.Unmarshal([]byte(prev.Result), &res); err != nil {
				return nil, err
			}
			return &res, nil
		}
		if prev.Status == "failed" {
			var e cerr.Error
			if err := json.Unmarshal([]byte(prev.Result), &e); err == nil {
				return nil, &e
			}
			return nil, cerr.New(cerr.Internal, prev.Result)
		}
		// status running: wait for the in-flight request to finish
		return s.waitInflight(req.RequestID)
	}

	b := s.block(req.BlockID)
	b.mu.Lock() // single writer per block
	defer b.mu.Unlock()

	// Re-check inside the writer lock: another writer may have completed
	// the same request while we waited for the lock.
	if prev, err := s.st.GetRequest(req.RequestID); err == nil {
		if prev.PayloadHash != payloadHash {
			return nil, cerr.New(cerr.ConflictingRequest,
				"request_id reused with different payload",
				"request_id", req.RequestID)
		}
		if prev.Status == "done" {
			var res VerifyResult
			if err := json.Unmarshal([]byte(prev.Result), &res); err != nil {
				return nil, err
			}
			return &res, nil
		}
	}

	slot := &inflightRequest{done: make(chan struct{})}
	s.mu.Lock()
	s.inflight[req.RequestID] = slot
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.inflight, req.RequestID)
		s.mu.Unlock()
		close(slot.done)
	}()

	if _, err := s.st.RegisterRequest(req.RequestID, req.BlockID, payloadHash); err != nil {
		return nil, err
	}

	res, err := s.runVerify(b, req)
	if err != nil {
		slot.err = err
		b, _ := json.Marshal(err)
		_ = s.st.FailRequest(req.RequestID, b)
		return nil, err
	}
	rb, _ := json.Marshal(res)
	slot.result = rb
	return res, nil
}

func (s *Service) waitInflight(requestID string) (*VerifyResult, error) {
	s.mu.Lock()
	slot, ok := s.inflight[requestID]
	s.mu.Unlock()
	if !ok {
		// request recorded as running but no in-flight owner: it crashed
		// or recovery has not completed for it.
		return nil, cerr.New(cerr.RecoveryIncomplete,
			"request is not committed; run recovery or resubmit",
			"request_id", requestID)
	}
	<-slot.done
	if slot.err != nil {
		return nil, slot.err
	}
	var res VerifyResult
	if err := json.Unmarshal(slot.result, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (s *Service) pageReader(b *blockState) verify.PageReader {
	return func(p int) ([]byte, error) {
		return os.ReadFile(sample.PagePath(b.sampleDir, p))
	}
}

func (s *Service) runVerify(b *blockState, req *VerifyRequest) (*VerifyResult, error) {
	if b.manifest == nil {
		return nil, cerr.New(cerr.NotFound, "no manifest loaded for block", "block_id", req.BlockID)
	}
	m := b.manifest

	// determine the checkpoint to build on, and allocate the ID of the
	// new checkpoint this verification will commit
	var prevTree *manifest.Tree
	var baseCheckpointID int64 = -1
	if req.Mode == "incremental" {
		if req.CheckpointID == nil {
			return nil, cerr.New(cerr.BadRequest, "incremental mode requires checkpoint_id")
		}
		baseCheckpointID = *req.CheckpointID
		t, err := s.st.LoadTree(req.BlockID, baseCheckpointID)
		if err != nil {
			return nil, cerr.New(cerr.CheckpointNotFound, "checkpoint not found",
				"block_id", req.BlockID, "checkpoint_id", baseCheckpointID)
		}
		prevTree = t
	}
	var newCheckpointID int64
	latest, err := s.st.LatestCheckpoint(req.BlockID)
	if err == sql.ErrNoRows {
		newCheckpointID = 0
	} else if err != nil {
		return nil, err
	} else {
		newCheckpointID = latest.CheckpointID + 1
	}

	// 1. write the intent record before mutating any state
	intentID := fmt.Sprintf("intent-%s", req.RequestID)
	if err := s.st.WriteIntent(intentID, req.BlockID, "verify-"+req.Mode, req); err != nil {
		return nil, err
	}
	if req.DebugCrashAfterIntent {
		fmt.Fprintf(os.Stderr, "debug: crashing after intent %s\n", intentID)
		os.Exit(2)
	}

	// 2. compute
	res := &VerifyResult{
		RequestID: req.RequestID, BlockID: req.BlockID,
		Mode: req.Mode, CheckpointID: newCheckpointID,
	}
	var newTree *manifest.Tree
	var consensusResult []byte
	switch req.Mode {
	case "full":
		full, err := verify.Full(m, s.pageReader(b), manifest.BuildTree(m.LeafDigests))
		if err != nil {
			return nil, err
		}
		res.Full = full
		newTree = full.RecomputedTree
		var reports []verify.Report
		for _, r := range b.reports {
			reports = append(reports, r)
		}
		sort.Slice(reports, func(i, j int) bool { return reports[i].ReplicaID < reports[j].ReplicaID })
		if len(reports) > 0 {
			cons, err := verify.Consensus(reports, m.PageCount)
			if err != nil {
				return nil, err
			}
			res.Consensus = cons
			res.Trust = trust.ComputeScores(cons.TrustInputs(), m.PageCount,
				cons.ConsensusTree.MaxLevel(), len(cons.QuorumReplicas))
			dr := trust.ComputeDamageRange(cons.DivergentPageSet(), m)
			res.Damage = &dr
			consensusResult, _ = json.Marshal(cons)
		}
	case "incremental":
		inc, err := verify.Incremental(m, s.pageReader(b), prevTree, int(baseCheckpointID), req.ChangedPages)
		if err != nil {
			return nil, err
		}
		res.Incremental = inc
		newTree = inc.NewTree
		b.lastInc = inc
	}

	// 3. commit checkpoint + consensus + request result in one transaction
	rb, _ := json.Marshal(res)
	if err := s.st.CommitCheckpoint(intentID, req.BlockID, newCheckpointID, newTree,
		consensusResult, req.RequestID, rb); err != nil {
		return nil, err
	}
	return res, nil
}

// Result returns the persisted result of a previously submitted request.
func (s *Service) Result(requestID string) (*VerifyResult, error) {
	prev, err := s.st.GetRequest(requestID)
	if err == sql.ErrNoRows {
		return nil, cerr.New(cerr.NotFound, "unknown request_id", "request_id", requestID)
	}
	if err != nil {
		return nil, err
	}
	switch prev.Status {
	case "done":
		var res VerifyResult
		if err := json.Unmarshal([]byte(prev.Result), &res); err != nil {
			return nil, err
		}
		return &res, nil
	case "failed":
		var e cerr.Error
		if err := json.Unmarshal([]byte(prev.Result), &e); err == nil {
			return nil, &e
		}
		return nil, cerr.New(cerr.Internal, prev.Result)
	default:
		return nil, cerr.New(cerr.RecoveryIncomplete,
			"request was never committed (aborted or crashed)",
			"request_id", requestID)
	}
}

// IncrementalStats returns the stats of the last incremental verification
// of a block.
func (s *Service) IncrementalStats(blockID string) (*verify.IncrementalResult, error) {
	b := s.block(blockID)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.lastInc == nil {
		return nil, cerr.New(cerr.NotFound, "no incremental verification recorded",
			"block_id", blockID)
	}
	return b.lastInc, nil
}
