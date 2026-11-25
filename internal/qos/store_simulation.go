package qos

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type storedSimulation struct {
	ID            string
	RequestID     string
	PolicyVersion int64
	Seed          int64
	SHA           string
	Packets       int
	CreatedAt     string
}

func (s *Store) trafficFile() string { return filepath.Join(s.opts.DataDir, "traffic_sample.jsonl") }

func (s *Store) ensureTrafficFile() error {
	if _, err := os.Stat(s.trafficFile()); err == nil {
		return nil
	}
	_, err := EnsureSamples(s.opts.DataDir, s.opts.SampleSeed)
	return err
}

func (s *Store) SubmitSimulation(ctx context.Context, req SimulationRequest) (*SimulationSummaryResponse, *BusinessError) {
	if be := validateRequestID(req.RequestID); be != nil {
		return nil, be
	}
	if req.PolicyVersion == 0 {
		req.PolicyVersion = req.BaseVersion
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if it, err := s.replayIntent(ctx, req.RequestID); err != nil {
		return nil, NewError(CodeInternal, "cannot check request idempotency", 500, map[string]any{"error": err.Error()})
	} else if it != nil {
		if it.Kind != "simulation" {
			return nil, NewError(CodeRequestConflict, "request_id belongs to another operation kind", 409, map[string]any{
				"request_id": req.RequestID, "existing_kind": it.Kind,
			})
		}
		if be := intentReplayError(it); be != nil {
			return nil, be
		}
		var out SimulationSummaryResponse
		if err := json.Unmarshal(it.ResponseJSON, &out); err != nil {
			return nil, NewError(CodeInternal, "cannot decode idempotent simulation response", 500, map[string]any{"error": err.Error()})
		}
		return &out, nil
	}
	if err := s.insertIntent(ctx, req.RequestID, "simulation", req.PolicyVersion, req.PolicyVersion, req); err != nil {
		return nil, NewError(CodeRequestConflict, "request_id already exists", 409, map[string]any{"request_id": req.RequestID})
	}
	if err := s.ensureTrafficFile(); err != nil {
		be := NewError(CodeSimulationFailed, "cannot prepare deterministic traffic sample", 422, map[string]any{"error": err.Error()})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	traffic, sha, err := LoadTrafficFile(s.trafficFile())
	if err != nil {
		be := NewError(CodeSimulationFailed, "cannot load deterministic traffic sample", 422, map[string]any{"error": err.Error()})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	if err := s.setIntentPhase(ctx, req.RequestID, "simulating"); err != nil {
		be := NewError(CodeInternal, "cannot record intent phase", 500, map[string]any{"error": err.Error()})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		be := NewError(CodeInternal, "cannot begin simulation transaction", 500, map[string]any{"error": err.Error()})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	current, err := currentVersionTx(ctx, tx)
	if err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot read current version", err)
	}
	if current == 0 {
		_ = tx.Rollback()
		be := NewError(CodeNoPolicy, "no policy has been committed", 404, map[string]any{"policy_version": req.PolicyVersion})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	if req.PolicyVersion != current {
		_ = tx.Rollback()
		be := staleRead(req.PolicyVersion, current, current, "simulation_policy_version_replaced", map[string]any{"simulation_id": req.RequestID})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	policy, err := loadPolicyTx(ctx, tx)
	if err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot load policy for simulation", err)
	}
	views, err := loadAllViews(ctx, tx)
	if err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot load derived views for simulation", err)
	}
	if len(policy) == 0 {
		_ = tx.Rollback()
		be := NewError(CodeNoPolicy, "policy contains no queues", 404, map[string]any{"policy_version": req.PolicyVersion})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	effective := make(map[string]EffectiveRates, len(views))
	stamps := make(map[string]QueueVersionStamp, len(views))
	for _, v := range views {
		if v.ValidThrough < current {
			_ = tx.Rollback()
			be := staleRead(current, v.CalcVersion, current, "derived_view_lags_observed_version", map[string]any{
				"queue_id": v.Queue.ID, "valid_through_version": v.ValidThrough,
			})
			s.failIntent(ctx, req.RequestID, be)
			return nil, be
		}
		effective[v.Queue.ID] = v.Effective
		stamps[v.Queue.ID] = QueueVersionStamp{CalculationVersion: v.CalcVersion, ValidThroughVersion: v.ValidThrough}
	}
	results, packetCount, biz := RunSimulation(SimulationInput{
		PolicyVersion: current, Policy: policy, Effective: effective, QueueVersions: stamps,
		Samples: traffic, SelectedQueueIDs: req.QueueIDs,
	})
	if biz != nil {
		_ = tx.Rollback()
		s.failIntent(ctx, req.RequestID, biz)
		return nil, biz
	}
	now := nowRFC3339()
	if _, err := tx.ExecContext(ctx, `INSERT INTO simulations(simulation_id,request_id,policy_version,sample_seed,
		sample_sha256,packets,status,created_at) VALUES(?,?,?,?,?,?,?,?)`,
		req.RequestID, req.RequestID, current, s.opts.SampleSeed, sha, packetCount, "complete", now); err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot insert simulation", err)
	}
	insertedAny := false
	for _, result := range results {
		summary, _ := json.Marshal(result.Summary)
		if _, err := tx.ExecContext(ctx, `INSERT INTO simulation_results(simulation_id,queue_id,summary_json)
			VALUES(?,?,?)`, req.RequestID, result.Summary.QueueID, string(summary)); err != nil {
			return nil, s.txFailure(ctx, req.RequestID, tx, "cannot insert simulation summary", err)
		}
		for _, packet := range result.Packets {
			packetJSON, _ := json.Marshal(packet)
			if _, err := tx.ExecContext(ctx, `INSERT INTO simulation_packets(simulation_id,queue_id,sequence,packet_json)
				VALUES(?,?,?,?)`, req.RequestID, packet.QueueID, packet.Sequence, string(packetJSON)); err != nil {
				return nil, s.txFailure(ctx, req.RequestID, tx, "cannot insert simulation packet", err)
			}
		}
		if !insertedAny {
			insertedAny = true
			s.maybeFailpoint("simulation", req.RequestID)
		}
	}
	summaries := make([]QueueSimulationSummary, 0, len(results))
	for _, result := range results {
		summaries = append(summaries, result.Summary)
	}
	resp := &SimulationSummaryResponse{
		Code: CodeOK, SimulationID: req.RequestID, PolicyVersion: current, ObservedVersion: current,
		UsedVersion: current, SampleSeed: s.opts.SampleSeed, SampleSHA256: sha, Packets: packetCount,
		Queues: summaries, RoundingContract: RoundingContract, CreatedAt: now,
	}
	if err := completeIntentTx(ctx, tx, req.RequestID, current, resp, 200); err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot complete simulation intent", err)
	}
	if err := tx.Commit(); err != nil {
		be := NewError(CodeInternal, "simulation commit failed", 500, map[string]any{"error": err.Error()})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	rollback = false
	return resp, nil
}

func loadSimulation(ctx context.Context, tx *sql.Tx, simulationID string) (storedSimulation, error) {
	var sim storedSimulation
	err := tx.QueryRowContext(ctx, `SELECT simulation_id,request_id,policy_version,sample_seed,sample_sha256,packets,created_at
		FROM simulations WHERE simulation_id=?`, simulationID).Scan(&sim.ID, &sim.RequestID, &sim.PolicyVersion,
		&sim.Seed, &sim.SHA, &sim.Packets, &sim.CreatedAt)
	return sim, err
}

func (s *Store) GetSimulation(ctx context.Context, simulationID string, observed int64) (*SimulationSummaryResponse, *BusinessError) {
	if observed <= 0 {
		return nil, policyInvalid("observe_version must be positive", map[string]any{"observe_version": observed})
	}
	tx, current, err := s.readTx(ctx)
	if err != nil {
		return nil, NewError(CodeInternal, "cannot begin simulation read", 500, map[string]any{"error": err.Error()})
	}
	defer tx.Rollback()
	sim, err := loadSimulation(ctx, tx, simulationID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewError(CodeNotFound, "simulation does not exist", 404, map[string]any{"simulation_id": simulationID})
	}
	if err != nil {
		return nil, NewError(CodeInternal, "cannot load simulation", 500, map[string]any{"error": err.Error()})
	}
	if observed != current {
		return nil, staleRead(observed, sim.PolicyVersion, current, "observed_version_replaced", map[string]any{"simulation_id": simulationID})
	}
	if sim.PolicyVersion != observed {
		return nil, staleRead(observed, sim.PolicyVersion, current, "simulation_version_replaced", map[string]any{"simulation_id": simulationID})
	}
	rows, err := tx.QueryContext(ctx, `SELECT queue_id,summary_json FROM simulation_results WHERE simulation_id=? ORDER BY queue_id`, simulationID)
	if err != nil {
		return nil, NewError(CodeInternal, "cannot read simulation summaries", 500, map[string]any{"error": err.Error()})
	}
	defer rows.Close()
	var summaries []QueueSimulationSummary
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, NewError(CodeInternal, "cannot scan simulation summary", 500, map[string]any{"error": err.Error()})
		}
		var summary QueueSimulationSummary
		if err := json.Unmarshal([]byte(raw), &summary); err != nil {
			return nil, NewError(CodeInternal, "cannot decode simulation summary", 500, map[string]any{"error": err.Error()})
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, NewError(CodeInternal, "cannot iterate simulation summaries", 500, map[string]any{"error": err.Error()})
	}
	return &SimulationSummaryResponse{
		Code: CodeOK, SimulationID: simulationID, PolicyVersion: sim.PolicyVersion,
		ObservedVersion: observed, UsedVersion: sim.PolicyVersion, SampleSeed: sim.Seed,
		SampleSHA256: sim.SHA, Packets: sim.Packets, Queues: summaries,
		RoundingContract: RoundingContract, CreatedAt: sim.CreatedAt,
	}, nil
}

func (s *Store) GetQueueDiagnostic(ctx context.Context, simulationID, queueID string, observed int64) (*QueueDiagnosticResponse, *BusinessError) {
	if observed <= 0 {
		return nil, policyInvalid("observe_version must be positive", map[string]any{"observe_version": observed})
	}
	tx, current, err := s.readTx(ctx)
	if err != nil {
		return nil, NewError(CodeInternal, "cannot begin diagnostic read", 500, map[string]any{"error": err.Error()})
	}
	defer tx.Rollback()
	sim, err := loadSimulation(ctx, tx, simulationID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewError(CodeNotFound, "simulation does not exist", 404, map[string]any{"simulation_id": simulationID})
	}
	if err != nil {
		return nil, NewError(CodeInternal, "cannot load simulation", 500, map[string]any{"error": err.Error()})
	}
	var viewVersion, validThrough int64
	err = tx.QueryRowContext(ctx, `SELECT calculation_version,valid_through_version FROM queue_views WHERE queue_id=?`, queueID).
		Scan(&viewVersion, &validThrough)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewError(CodeNotFound, "queue view does not exist", 404, map[string]any{"queue_id": queueID})
	}
	if err != nil {
		return nil, NewError(CodeInternal, "cannot read queue view version", 500, map[string]any{"error": err.Error()})
	}
	if observed != current {
		return nil, staleRead(observed, viewVersion, current, "observed_version_replaced", map[string]any{
			"simulation_id": simulationID, "queue_id": queueID,
		})
	}
	if sim.PolicyVersion != observed || validThrough < observed || viewVersion > observed {
		return nil, staleRead(observed, viewVersion, current, "derived_view_lags_observed_version", map[string]any{
			"simulation_id": simulationID, "queue_id": queueID, "valid_through_version": validThrough,
			"simulation_version": sim.PolicyVersion,
		})
	}
	var summaryRaw string
	err = tx.QueryRowContext(ctx, `SELECT summary_json FROM simulation_results WHERE simulation_id=? AND queue_id=?`,
		simulationID, queueID).Scan(&summaryRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewError(CodeNotFound, "queue was not part of this simulation", 404, map[string]any{
			"simulation_id": simulationID, "queue_id": queueID,
		})
	}
	if err != nil {
		return nil, NewError(CodeInternal, "cannot read queue simulation summary", 500, map[string]any{"error": err.Error()})
	}
	var summary QueueSimulationSummary
	if err := json.Unmarshal([]byte(summaryRaw), &summary); err != nil {
		return nil, NewError(CodeInternal, "cannot decode queue simulation summary", 500, map[string]any{"error": err.Error()})
	}
	rows, err := tx.QueryContext(ctx, `SELECT packet_json FROM simulation_packets
		WHERE simulation_id=? AND queue_id=? ORDER BY sequence`, simulationID, queueID)
	if err != nil {
		return nil, NewError(CodeInternal, "cannot read diagnostic packets", 500, map[string]any{"error": err.Error()})
	}
	defer rows.Close()
	var packets []PacketResult
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, NewError(CodeInternal, "cannot scan diagnostic packet", 500, map[string]any{"error": err.Error()})
		}
		var p PacketResult
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			return nil, NewError(CodeInternal, "cannot decode diagnostic packet", 500, map[string]any{"error": err.Error()})
		}
		packets = append(packets, p)
	}
	if packets == nil {
		packets = []PacketResult{}
	}
	return &QueueDiagnosticResponse{
		Code: CodeOK, SimulationID: simulationID, PolicyVersion: sim.PolicyVersion,
		ObservedVersion: observed, UsedVersion: observed,
		CalculationVersion: viewVersion, ValidThroughVersion: validThrough,
		Summary: summary, TokenBalanceBits: summary.FinalTokenBalanceBits,
		PeakTokenBalanceBits: summary.FinalPeakBalanceBits, SentBytes: summary.SentBytes,
		DroppedBytes: summary.DroppedBytes, Conclusion: summary.Conclusion, Packets: packets,
		CumulativeErrorMilliBits: summary.CumulativeErrorMilliBits, RoundingContract: RoundingContract,
	}, nil
}

func (s *Store) LatestQueueDiagnostic(ctx context.Context, queueID string, observed int64) (*QueueDiagnosticResponse, *BusinessError) {
	if observed <= 0 {
		return nil, policyInvalid("observe_version must be positive", map[string]any{"observe_version": observed})
	}
	tx, current, err := s.readTx(ctx)
	if err != nil {
		return nil, NewError(CodeInternal, "cannot begin latest diagnostic read", 500, map[string]any{"error": err.Error()})
	}
	if current != observed {
		_ = tx.Rollback()
		return nil, staleRead(observed, current, current, "observed_version_replaced", map[string]any{"queue_id": queueID})
	}
	var simulationID string
	err = tx.QueryRowContext(ctx, `SELECT simulation_id FROM simulations WHERE policy_version=?
		ORDER BY rowid DESC LIMIT 1`, observed).Scan(&simulationID)
	_ = tx.Rollback()
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewError(CodeNotFound, "no simulation exists at the observed version", 404, map[string]any{
			"queue_id": queueID, "observed_version": observed,
		})
	}
	if err != nil {
		return nil, NewError(CodeInternal, "cannot find latest simulation", 500, map[string]any{"error": err.Error()})
	}
	return s.GetQueueDiagnostic(ctx, simulationID, queueID, observed)
}

func (s *Store) SampleMeta(ctx context.Context) (SampleMeta, *BusinessError) {
	if err := s.ensureTrafficFile(); err != nil {
		return SampleMeta{}, NewError(CodeInternal, "cannot prepare samples", 500, map[string]any{"error": err.Error()})
	}
	raw, err := os.ReadFile(filepath.Join(s.opts.DataDir, "sample_meta.json"))
	if err != nil {
		return SampleMeta{}, NewError(CodeInternal, "cannot read sample metadata", 500, map[string]any{"error": err.Error()})
	}
	var meta SampleMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return SampleMeta{}, NewError(CodeInternal, "cannot decode sample metadata", 500, map[string]any{"error": err.Error()})
	}
	return meta, nil
}
