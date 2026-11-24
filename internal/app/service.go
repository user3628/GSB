package app

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"preprocesspipeline/internal/domain"
	"preprocesspipeline/internal/store"
)

type Service struct {
	store   *store.Store
	dataDir string

	writeMu sync.Mutex
	callsMu sync.Mutex
	calls   map[string]*batchCall
}

type batchCall struct {
	done chan struct{}
	hash string
	resp *BatchResponse
	err  error
}

type BatchRequest struct {
	BatchID        string               `json:"batch_id"`
	PolicyVersion  int64                `json:"policy_version,omitempty"`
	RawInput       string               `json:"raw_input,omitempty"`
	Generate       *domain.GenerateSpec `json:"generate,omitempty"`
	Inject         string               `json:"inject,omitempty"`
	Injection      string               `json:"injection,omitempty"`
	Fault          string               `json:"fault,omitempty"`
	InterruptStage string               `json:"interrupt_stage,omitempty"`
	Retry          bool                 `json:"retry,omitempty"`
}

type BatchResponse struct {
	BatchID       string               `json:"batch_id"`
	Status        string               `json:"status"`
	PolicyVersion int64                `json:"policy_version"`
	IntentID      string               `json:"intent_id"`
	RawDigest     string               `json:"raw_digest"`
	Stages        []domain.StageResult `json:"stages"`
	Shards        []store.ShardRecord  `json:"shards"`
	Warnings      []string             `json:"warnings"`
	Reused        bool                 `json:"reused,omitempty"`
}

type IntentPlan struct {
	TempDir        string   `json:"temp_dir"`
	ArtifactPaths  []string `json:"artifact_paths,omitempty"`
	ShardCount     int      `json:"shard_count"`
	InterruptStage string   `json:"interrupt_stage,omitempty"`
	Fault          string   `json:"fault,omitempty"`
	AffectedShards []string `json:"affected_shards,omitempty"`
}

func NewService(st *store.Store) *Service {
	return &Service{store: st, dataDir: st.DataDir, calls: make(map[string]*batchCall)}
}

func (s *Service) Store() *store.Store { return s.store }

func (s *Service) DataDir() string { return s.dataDir }

func newID(prefix string) string {
	var raw [12]byte
	_, _ = rand.Read(raw[:])
	return prefix + "-" + hex.EncodeToString(raw[:])
}

func nowISO() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func normalizeInject(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.TrimPrefix(value, "e_")
	switch value {
	case "label_invalid", "invalidlabel", "invalid_label":
		return "invalid_label"
	case "shard_key_missing", "missingshardkey", "missing_shard_key":
		return "missing_shard_key"
	case "sample_ratio_invalid", "bad_ratio", "ratio_invalid", "sample_ratio_out_of_range":
		return "sample_ratio_invalid"
	case "id_conflict", "duplicate_id", "duplicate_identifier_conflict":
		return "id_conflict"
	case "stage_interrupted", "interrupt", "interrupted", "stage_interrupt":
		return "stage_interrupted"
	case "record_invalid", "invalid_record":
		return "record_invalid"
	case "digest_mismatch", "shard_digest_mismatch":
		return "digest_mismatch"
	case "force_kill", "kill":
		return "force_kill"
	default:
		return value
	}
}

func (r BatchRequest) normalized() BatchRequest {
	if r.Inject == "" {
		r.Inject = r.Injection
	}
	if r.Inject == "" {
		r.Inject = r.Fault
	}
	r.Inject = normalizeInject(r.Inject)
	if r.InterruptStage == "" && r.Inject == "stage_interrupted" {
		r.InterruptStage = domain.StageTensorize
	}
	return r
}

func (s *Service) SubmitBatch(ctx context.Context, req BatchRequest) (resp *BatchResponse, err error) {
	req = req.normalized()
	if strings.TrimSpace(req.BatchID) == "" {
		req.BatchID = newID("batch")
	}
	raw, _, requestHash, err := s.prepareRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	s.callsMu.Lock()
	if call, ok := s.calls[req.BatchID]; ok {
		if call.hash != requestHash {
			s.callsMu.Unlock()
			return nil, domain.NewError(domain.CodeIDConflict,
				"batch identifier was already used with a different completed request",
				map[string]any{"batch_id": req.BatchID})
		}
		s.callsMu.Unlock()
		select {
		case <-call.done:
			if call.err != nil {
				return nil, call.err
			}
			respCopy := *call.resp
			respCopy.Reused = true
			return &respCopy, nil
		case <-ctx.Done():
			return nil, domain.NewError(domain.CodeInternal, "context cancelled while waiting for batch leader")
		}
	}
	call := &batchCall{done: make(chan struct{}), hash: requestHash}
	s.calls[req.BatchID] = call
	s.callsMu.Unlock()

	defer close(call.done)

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	defer func() {
		if recovered := recover(); recovered != nil {
			berr := domain.NewError(domain.CodeInternal, fmt.Sprintf("batch leader panic: %v", recovered))
			call.err = berr
			err = berr
		}
	}()

	policy, err := s.resolvePolicy(ctx, req.PolicyVersion)
	if err != nil {
		call.err = err
		return nil, err
	}
	if req.Inject == "sample_ratio_invalid" {
		err = domain.NewError(domain.CodeSampleRatioInvalid,
			"injected sample ratio 1.5000 is outside [0,1]",
			map[string]any{"sample_ratio": 1.5})
		call.err = err
		return nil, err
	}
	resp, err = s.executeBatchLocked(ctx, req, raw, policy, requestHash)
	call.resp = resp
	call.err = err
	return resp, err
}

func (s *Service) prepareRequest(ctx context.Context, req BatchRequest) (string, *domain.Policy, string, error) {
	policy, err := s.resolvePolicy(ctx, req.PolicyVersion)
	if err != nil {
		return "", nil, "", err
	}
	raw := req.RawInput
	if strings.TrimSpace(raw) == "" {
		spec := domain.DefaultGenerateSpec()
		if req.Generate != nil {
			spec = *req.Generate
		}
		if spec.Namespace == "" {
			spec.Namespace = req.BatchID
		}
		raw = domain.GenerateRawInput(spec)
	}
	switch req.Inject {
	case "invalid_label":
		raw = domain.InjectInvalidLabel(raw)
	case "missing_shard_key":
		raw = domain.InjectMissingShardKey(raw)
	case "record_invalid":
		raw = domain.InjectMalformedLine(raw)
	}
	hash := domain.DigestValue(map[string]any{
		"batch_id":     req.BatchID,
		"retry":        req.Retry,
		"request":      req,
		"resolved_raw": raw,
	})
	return raw, policy, hash, nil
}

func (s *Service) resolvePolicy(ctx context.Context, version int64) (*domain.Policy, error) {
	if version > 0 {
		return s.store.GetPolicy(ctx, version)
	}
	return s.store.CurrentPolicy(ctx)
}

func (s *Service) executeBatchLocked(
	ctx context.Context,
	req BatchRequest,
	raw string,
	policy *domain.Policy,
	requestHash string,
) (*BatchResponse, error) {
	tempRel := ""
	tempAbs := ""
	cleanupTemp := true
	defer func() {
		if cleanupTemp && tempAbs != "" {
			_ = os.RemoveAll(tempAbs)
		}
	}()

	if existing, err := s.store.GetBatch(ctx, req.BatchID); err == nil {
		if existing.RequestHash != requestHash {
			return nil, domain.NewError(domain.CodeIDConflict,
				"batch identifier was already committed with a different request",
				map[string]any{"batch_id": req.BatchID, "existing_status": existing.Status})
		}
		switch existing.Status {
		case "committed":
			var resp BatchResponse
			if existing.ResultJSON != "" {
				if err := json.Unmarshal([]byte(existing.ResultJSON), &resp); err != nil {
					return nil, err
				}
				resp.Reused = true
				return &resp, nil
			}
		case "aborted":
			if !req.Retry {
				return nil, domain.NewError(domain.CodeBatchAborted,
					"batch has an aborted intent; resubmit with retry=true after reviewing recovery report",
					map[string]any{"batch_id": req.BatchID, "error_code": existing.ErrorCode})
			}
		default:
			return nil, domain.NewError(domain.CodeBatchRunning, "batch is already running",
				map[string]any{"batch_id": req.BatchID, "status": existing.Status})
		}
	} else if berr := domain.AsBusinessError(err); berr == nil || berr.Code != domain.CodeBatchNotFound {
		return nil, err
	}

	if req.Inject == "id_conflict" {
		if err := s.ensureConflictFixtureLocked(ctx); err != nil {
			return nil, err
		}
		raw = withConflictRecord(raw)
	}

	intentID := newID("intent")
	tempRel = filepath.ToSlash(filepath.Join("tmp", intentID))
	tempAbs = filepath.Join(s.dataDir, filepath.FromSlash(tempRel))
	if err := os.MkdirAll(tempAbs, 0o755); err != nil {
		return nil, domain.NewError(domain.CodeInternal, "create temp directory: "+err.Error())
	}
	plan := IntentPlan{
		TempDir:        tempRel,
		ShardCount:     policy.Sharding.Count,
		InterruptStage: req.InterruptStage,
		Fault:          req.Inject,
		ArtifactPaths:  plannedBatchArtifactPaths(intentID, req.BatchID, policy.Version, policy.Sharding.Count),
	}
	planJSON, _ := json.Marshal(plan)
	if err := s.store.CreateIntent(ctx, store.Intent{
		IntentID: intentID, BatchID: req.BatchID, PolicyVersion: policy.Version,
		Kind: "batch_run", Status: "running", PlanJSON: string(planJSON),
	}); err != nil {
		return nil, err
	}

	if req.Inject == "force_kill" && (req.InterruptStage == "" || req.InterruptStage == domain.StageParseClean) {
		os.Exit(137)
	}

	interruptBefore := func(stage string) error {
		if req.InterruptStage == stage && (req.Inject == "stage_interrupted" || req.Inject == "force_kill") {
			if req.Inject == "force_kill" {
				os.Exit(137)
			}
			return domain.NewError(domain.CodeStageInterrupted,
				"stage interruption marker accepted; intent remains unfinished for recovery scan",
				map[string]any{"batch_id": req.BatchID, "intent_id": intentID, "stage": stage})
		}
		return nil
	}

	if err := interruptBefore(domain.StageParseClean); err != nil {
		cleanupTemp = false
		return nil, err
	}
	parseOut, err := domain.RunParseClean(raw, *policy)
	if err != nil {
		_ = s.finishRejected(ctx, intentID, err)
		return nil, err
	}
	if conflict := s.checkIdentityConflict(ctx, req.BatchID, parseOut.Records); conflict != nil {
		_ = s.finishRejected(ctx, intentID, conflict)
		return nil, conflict
	}

	rawDigest := domain.DigestString("raw-lines-v1", raw)
	batchRecord := store.BatchRecord{
		BatchID: req.BatchID, RequestHash: requestHash, PolicyVersion: policy.Version,
		RawInput: raw, RawDigest: rawDigest,
	}
	if err := s.saveBoundary(ctx, req.BatchID, policy.Version, intentID, 1, parseOut.Result, &batchRecord); err != nil {
		return nil, s.failExecution(ctx, intentID, req.BatchID, tempAbs, err)
	}

	if err := interruptBefore(domain.StageFilter); err != nil {
		cleanupTemp = false
		return nil, err
	}
	filterOut, err := domain.RunFilter(parseOut, *policy)
	if err != nil {
		return nil, s.failExecution(ctx, intentID, req.BatchID, tempAbs, err)
	}
	if err := s.saveBoundary(ctx, req.BatchID, policy.Version, intentID, 2, filterOut.Result, nil); err != nil {
		return nil, s.failExecution(ctx, intentID, req.BatchID, tempAbs, err)
	}

	if err := interruptBefore(domain.StageShard); err != nil {
		cleanupTemp = false
		return nil, err
	}
	shardOut, err := domain.RunShard(filterOut, *policy)
	if err != nil {
		return nil, s.failExecution(ctx, intentID, req.BatchID, tempAbs, err)
	}
	if err := s.saveBoundary(ctx, req.BatchID, policy.Version, intentID, 3, shardOut.Result, nil); err != nil {
		return nil, s.failExecution(ctx, intentID, req.BatchID, tempAbs, err)
	}

	if err := interruptBefore(domain.StageSample); err != nil {
		cleanupTemp = false
		return nil, err
	}
	sampleOut, err := domain.RunSample(shardOut, *policy)
	if err != nil {
		return nil, s.failExecution(ctx, intentID, req.BatchID, tempAbs, err)
	}
	if err := s.saveBoundary(ctx, req.BatchID, policy.Version, intentID, 4, sampleOut.Result, nil); err != nil {
		return nil, s.failExecution(ctx, intentID, req.BatchID, tempAbs, err)
	}

	corruptShard := -1
	if req.Inject == "digest_mismatch" {
		corruptShard = 0
	}
	tensorOut, err := domain.RunTensorize(sampleOut, shardOut.Stats, *policy, corruptShard)
	if err != nil {
		return nil, s.failExecution(ctx, intentID, req.BatchID, tempAbs, err)
	}

	tempPaths, err := writeArtifactsToTemp(tempAbs, tensorOut.Artifacts)
	if err != nil {
		return nil, s.failExecution(ctx, intentID, req.BatchID, tempAbs, err)
	}
	if req.Inject == "force_kill" && req.InterruptStage == domain.StageTensorize {
		os.Exit(137)
	}
	if req.Inject == "stage_interrupted" && req.InterruptStage == domain.StageTensorize {
		cleanupTemp = false
		return nil, domain.NewError(domain.CodeStageInterrupted,
			"tensorize interrupted after temp shard writes and before atomic publication",
			map[string]any{"batch_id": req.BatchID, "intent_id": intentID, "stage": domain.StageTensorize,
				"temp_dir": tempRel})
	}

	relPaths, shardRecords, err := s.publishArtifacts(intentID, req.BatchID, policy.Version, tensorOut, tempPaths)
	if err != nil {
		return nil, s.failExecution(ctx, intentID, req.BatchID, tempAbs, err)
	}
	plan.ArtifactPaths = relPaths
	planJSON, _ = json.Marshal(plan)
	_ = s.store.UpdateIntentPlan(ctx, intentID, string(planJSON))

	response := &BatchResponse{
		BatchID: req.BatchID, Status: "committed", PolicyVersion: policy.Version,
		IntentID: intentID, RawDigest: rawDigest,
		Stages: []domain.StageResult{parseOut.Result, filterOut.Result, shardOut.Result, sampleOut.Result, tensorOut.Result},
		Shards: shardRecords, Warnings: shardOut.Warnings,
	}
	resultJSON, _ := json.Marshal(response)
	err = s.store.WithTx(ctx, func(tx *sql.Tx) error {
		if err := store.InsertStageSummaryTx(ctx, tx, req.BatchID, policy.Version, 5, tensorOut.Result); err != nil {
			return err
		}
		if _, err := store.InvalidateOlderShardsTx(ctx, tx, req.BatchID, policy.Version); err != nil {
			return err
		}
		for _, rec := range shardRecords {
			if err := store.UpsertShardTx(ctx, tx, rec); err != nil {
				return err
			}
		}
		for _, record := range parseOut.Records {
			if err := store.AddSampleIdentityTx(ctx, tx, record.ID, req.BatchID, policy.Version, record.RecordDigest); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE batches SET status='committed', policy_version=?, result_json=?, error_code='', completed_at=?
WHERE batch_id=?`, policy.Version, string(resultJSON), nowISO(), req.BatchID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE intents SET status='committed', boundary_stage=?, error_code='', updated_at=? WHERE intent_id=?`,
			domain.StageTensorize, nowISO(), intentID)
		return err
	})
	if err != nil {
		return nil, s.failExecution(ctx, intentID, req.BatchID, tempAbs,
			domain.NewError(domain.CodeInternal, "commit batch transaction failed: "+err.Error()))
	}
	cleanupTemp = true
	_ = os.RemoveAll(tempAbs)
	return response, nil
}

func (s *Service) saveBoundary(ctx context.Context, batchID string, policyVersion int64, intentID string, seq int, stage domain.StageResult, batch *store.BatchRecord) error {
	return s.store.WithTx(ctx, func(tx *sql.Tx) error {
		if batch != nil {
			if err := store.InsertRunningBatchTx(ctx, tx, *batch); err != nil {
				return err
			}
		}
		if err := store.InsertStageSummaryTx(ctx, tx, batchID, policyVersion, seq, stage); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE intents SET boundary_stage=?, updated_at=? WHERE intent_id=?`,
			stage.Stage, nowISO(), intentID)
		return err
	})
}

func (s *Service) failExecution(ctx context.Context, intentID, batchID, tempAbs string, cause error) error {
	code := domain.CodeInternal
	if berr := domain.AsBusinessError(cause); berr != nil {
		code = berr.Code
	}
	_ = s.store.FinishIntent(ctx, intentID, "failed", code, "")
	_, _ = s.store.DB().ExecContext(ctx,
		`UPDATE batches SET status='aborted', error_code=?, completed_at=? WHERE batch_id=? AND status='running'`,
		code, nowISO(), batchID)
	if tempAbs != "" {
		_ = os.RemoveAll(tempAbs)
	}
	return cause
}

func (s *Service) finishRejected(ctx context.Context, intentID string, cause error) error {
	code := domain.CodeInternal
	if berr := domain.AsBusinessError(cause); berr != nil {
		code = berr.Code
	}
	return s.store.FinishIntent(ctx, intentID, "rejected", code, "")
}

func (s *Service) checkIdentityConflict(ctx context.Context, currentBatch string, records []domain.CleanRecord) error {
	for _, record := range records {
		identity, err := s.store.GetIdentity(ctx, record.ID)
		if err != nil {
			return domain.NewError(domain.CodeInternal, err.Error())
		}
		if identity != nil && identity.BatchID != currentBatch {
			return domain.NewError(domain.CodeIDConflict,
				"sample identifier already committed in another batch",
				map[string]any{
					"sample_id":         record.ID,
					"existing_batch_id": identity.BatchID,
					"existing_digest":   identity.RecordDigest,
					"incoming_digest":   record.RecordDigest,
				})
		}
	}
	return nil
}

func (s *Service) ensureConflictFixtureLocked(ctx context.Context) error {
	const fixtureBatch = "id-conflict-base"
	const sampleID = "conflict-sample-fixed"
	if identity, err := s.store.GetIdentity(ctx, sampleID); err != nil {
		return err
	} else if identity != nil {
		return nil
	}
	policy, err := s.store.CurrentPolicy(ctx)
	if err != nil {
		return err
	}
	raw := fmt.Sprintf("%d|%s|18|alpha|north\n", policy.Filter.MinTimestampMS+60000, sampleID)
	req := BatchRequest{BatchID: fixtureBatch, RawInput: raw}
	hash := domain.DigestValue(map[string]any{"raw": raw, "policy": policy.Version})
	_, err = s.executeBatchLocked(ctx, req, raw, policy, hash)
	return err
}

func withConflictRecord(raw string) string {
	lines := strings.Split(strings.TrimRight(raw, "\n"), "\n")
	conflictLine := "1767225660000|conflict-sample-fixed|22|beta|south"
	if len(lines) > 6 {
		lines[6] = conflictLine
	} else if len(lines) > 0 {
		lines[0] = conflictLine
	} else {
		lines = []string{conflictLine}
	}
	return strings.Join(lines, "\n") + "\n"
}
