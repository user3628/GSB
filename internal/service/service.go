package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"upstreampool/internal/client"
	"upstreampool/internal/config"
	"upstreampool/internal/model"
	"upstreampool/internal/sim"
	"upstreampool/internal/store"
)

type Service struct {
	store      *store.Store
	cfg        model.Config
	endpoints  map[string]model.EndpointSpec
	trackLocks sync.Map
	corrupt    sync.Map
	physicalMu sync.Mutex
	physical   map[string]net.Conn
	crashPause time.Duration
}

type Outcome struct {
	RequestID    string           `json:"request_id"`
	RequestKey   string           `json:"request_key"`
	BatchID      string           `json:"batch_id"`
	ClientID     string           `json:"client_id"`
	EndpointID   string           `json:"endpoint_id"`
	Status       string           `json:"status"`
	Error        *model.ErrorBody `json:"error,omitempty"`
	ConnectionID string           `json:"connection_id,omitempty"`
	WaitedMS     int64            `json:"waited_ms,omitempty"`
	CompletedMS  int64            `json:"completed_ms"`
}

type SubmitResponse struct {
	TrackID   string        `json:"track_id"`
	BatchID   string        `json:"batch_id"`
	Advanced  bool          `json:"advanced"`
	Requests  []Outcome     `json:"requests"`
	Snapshot  *sim.Snapshot `json:"snapshot"`
	HorizonMS int64         `json:"horizon_ms"`
	FinalSeq  int64         `json:"final_seq"`
}

func New(ctx context.Context, st *store.Store, cfg model.Config) (*Service, error) {
	byID := make(map[string]model.EndpointSpec, len(cfg.Endpoints))
	for _, ep := range cfg.Endpoints {
		byID[ep.ID] = ep
	}
	if err := st.SaveEndpoints(ctx, cfg.Endpoints); err != nil {
		return nil, err
	}
	s := &Service{store: st, cfg: cfg, endpoints: byID, physical: make(map[string]net.Conn)}
	if raw := strings.TrimSpace(os.Getenv("DIAG_CRASH_AFTER_PLAN_INTENT_MS")); raw != "" {
		ms, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || ms < 0 {
			return nil, fmt.Errorf("invalid DIAG_CRASH_AFTER_PLAN_INTENT_MS")
		}
		s.crashPause = time.Duration(ms) * time.Millisecond
	}
	return s, nil
}

func (s *Service) Close() error {
	s.physicalMu.Lock()
	for key, conn := range s.physical {
		_ = conn.Close()
		delete(s.physical, key)
	}
	s.physicalMu.Unlock()
	return nil
}

func (s *Service) Endpoints() []model.EndpointSpec {
	out := make([]model.EndpointSpec, 0, len(s.endpoints))
	for _, ep := range s.endpoints {
		out = append(out, ep)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *Service) Config() model.Config { return s.cfg }

func (s *Service) trackLock(trackID string) *sync.Mutex {
	value, _ := s.trackLocks.LoadOrStore(trackID, &sync.Mutex{})
	return value.(*sync.Mutex)
}

func (s *Service) Submit(ctx context.Context, in model.SubmitRequest) (*SubmitResponse, error) {
	if strings.TrimSpace(in.TrackID) == "" || strings.TrimSpace(in.BatchID) == "" {
		return nil, model.NewError(400, model.ErrSpecInvalid, "track_id and batch_id are required", nil)
	}
	if len(in.Requests) == 0 {
		return nil, model.NewError(400, model.ErrSpecInvalid, "a batch must contain at least one request", nil)
	}
	in.TrackID = strings.TrimSpace(in.TrackID)
	in.BatchID = strings.TrimSpace(in.BatchID)
	if len(in.TrackID) > 128 || len(in.BatchID) > 128 {
		return nil, model.NewError(400, model.ErrSpecInvalid, "track_id or batch_id is too long", nil)
	}
	reqs := append([]model.RequestInput(nil), in.Requests...)
	for i := range reqs {
		reqs[i].RequestID = strings.TrimSpace(reqs[i].RequestID)
		reqs[i].ClientID = strings.TrimSpace(reqs[i].ClientID)
		reqs[i].EndpointID = strings.TrimSpace(reqs[i].EndpointID)
		if err := config.ValidateRequest(&reqs[i], s.endpoints); err != nil {
			return nil, err
		}
	}
	for i := range reqs {
		if reqs[i].RequestID == "" {
			reqs[i].RequestID = automaticRequestID(reqs[i])
		}
	}
	if err := ensureUniqueRequestIDs(reqs); err != nil {
		return nil, err
	}
	digest, err := store.NormalizeBatchDigest(in.TrackID, in.BatchID, reqs)
	if err != nil {
		return nil, model.NewError(400, model.ErrSpecInvalid, "cannot canonicalize batch", map[string]any{"error": err.Error()})
	}

	lock := s.trackLock(in.TrackID)
	lock.Lock()
	defer lock.Unlock()

	existing, err := s.store.FindBatch(ctx, in.TrackID, in.BatchID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.Digest != digest {
			return nil, model.NewError(409, model.ErrConflictingBatch, "batch id was already used with different content", map[string]any{
				"track_id": in.TrackID, "batch_id": in.BatchID,
			})
		}
		if existing.Status == store.BatchPending {
			if _, err := s.processTrack(ctx, in.TrackID); err != nil {
				return nil, err
			}
			existing, err = s.store.FindBatch(ctx, in.TrackID, in.BatchID)
			if err != nil {
				return nil, err
			}
		}
		if existing.ResponseJSON == "" {
			return nil, model.NewError(409, model.ErrRecoveryPending, "batch is accepted but not yet applied", map[string]any{"track_id": in.TrackID, "batch_id": in.BatchID})
		}
		var out SubmitResponse
		if err := json.Unmarshal([]byte(existing.ResponseJSON), &out); err != nil {
			return nil, model.NewError(500, model.ErrCheckpointCorrupt, "persisted batch response is invalid", map[string]any{"batch_id": in.BatchID})
		}
		out.Advanced = false
		return &out, nil
	}

	requestRows := make([]model.Request, 0, len(reqs))
	for i, req := range reqs {
		requestRows = append(requestRows, model.Request{
			TrackID: in.TrackID, BatchID: in.BatchID, RequestKey: req.RequestID, RequestID: req.RequestID,
			TimestampMS: req.TimestampMS, ClientID: req.ClientID, EndpointID: req.EndpointID,
			RequestBytes: req.RequestBytes, UploadRateBPS: req.UploadRateBPS, ResponseBytes: req.ResponseBytes, Ordinal: i,
		})
	}
	intentID, err := s.store.CreateIntent(ctx, in.TrackID, "INGEST_BATCH", map[string]any{
		"batch_id": in.BatchID, "digest": digest, "request_count": len(reqs),
	})
	if err != nil {
		return nil, err
	}
	if err := s.store.InsertBatch(ctx, in.TrackID, in.BatchID, digest, requestRows, intentID); err != nil {
		return nil, err
	}
	_, err = s.processTrack(ctx, in.TrackID)
	if err != nil {
		return nil, err
	}
	applied, err := s.store.FindBatch(ctx, in.TrackID, in.BatchID)
	if err != nil {
		return nil, err
	}
	if applied == nil || applied.Status != store.BatchApplied || applied.ResponseJSON == "" {
		return nil, model.NewError(500, model.ErrRecoveryPending, "batch was ingested but not yet applied", nil)
	}
	var resp SubmitResponse
	if err := json.Unmarshal([]byte(applied.ResponseJSON), &resp); err != nil {
		return nil, model.NewError(500, model.ErrCheckpointCorrupt, "persisted batch response is invalid", map[string]any{"batch_id": in.BatchID})
	}
	return &resp, nil
}

func (s *Service) processTrack(ctx context.Context, trackID string) (*SubmitResponse, error) {
	pending, err := s.store.PendingBatchIDsForTrack(ctx, trackID)
	if err != nil {
		return nil, err
	}
	if len(pending) == 0 {
		return s.latestResponse(ctx, trackID)
	}
	if _, corrupt := s.corrupt.Load(trackID); corrupt {
		return nil, model.NewError(500, model.ErrCheckpointCorrupt, "track has a corrupt checkpoint; recovery is blocked", map[string]any{"track_id": trackID})
	}
	requests, err := s.store.Requests(ctx, trackID)
	if err != nil {
		return nil, err
	}
	if len(requests) == 0 {
		return nil, model.NewError(500, model.ErrInternal, "pending batch has no persisted requests", nil)
	}
	requestByKey := make(map[string]model.Request, len(requests))
	for _, req := range requests {
		requestByKey[req.RequestKey] = req
	}
	ioFacts, err := s.store.LoadIO(ctx, trackID)
	if err != nil {
		return nil, err
	}
	overrides := make(map[string]string)
	knownIO := make(map[string]store.IOFact)
	for _, fact := range ioFacts {
		knownIO[fact.RequestKey] = fact
		if fact.Status == sim.StatusError && fact.ErrorCode != "" {
			overrides[fact.RequestKey] = fact.ErrorCode
		}
	}
	pendingSet := make(map[string]struct{})
	for _, batchID := range pending {
		for _, req := range requests {
			if req.BatchID == batchID {
				pendingSet[req.RequestKey] = struct{}{}
			}
		}
	}
	planIntent, err := s.store.CreateIntent(ctx, trackID, "APPLY_PLAN", map[string]any{
		"pending_batches": pending,
		"request_count":   len(requests),
	})
	if err != nil {
		return nil, err
	}
	if s.crashPause > 0 {
		log.Printf("DIAG_PLAN_INTENT_READY intent=%s track=%s", planIntent, trackID)
		time.Sleep(s.crashPause)
	}

	attempted := make(map[string]bool)
	newFacts := make([]store.IOFact, 0)
	var finalPlan *sim.Plan
	for iteration := 0; iteration < 8; iteration++ {
		plan, err := sim.Run(sim.Input{TrackID: trackID, Requests: requests, Endpoints: s.endpoints, Defaults: s.cfg.Defaults, Overrides: overrides})
		if err != nil {
			return nil, err
		}
		targets := make([]*sim.RequestResult, 0)
		for i := range plan.Requests {
			res := &plan.Requests[i]
			if _, pendingReq := pendingSet[res.RequestKey]; !pendingReq || res.Status != sim.StatusDone || attempted[res.RequestKey] {
				continue
			}
			if _, hasFact := knownIO[res.RequestKey]; hasFact {
				attempted[res.RequestKey] = true
				continue
			}
			targets = append(targets, res)
		}
		if len(targets) == 0 {
			finalPlan = plan
			break
		}
		changed, facts := s.executeNetwork(ctx, trackID, plan, targets, attempted, requestByKey)
		newFacts = append(newFacts, facts...)
		for _, fact := range facts {
			knownIO[fact.RequestKey] = fact
			if fact.Status == sim.StatusError && fact.ErrorCode != "" {
				overrides[fact.RequestKey] = fact.ErrorCode
			}
		}
		if !changed {
			finalPlan = plan
			break
		}
	}
	if finalPlan == nil {
		return nil, model.NewError(500, model.ErrInternal, "actual network outcomes did not converge with the deterministic plan", nil)
	}
	if len(newFacts) > 0 {
		finalPlan, err = sim.Run(sim.Input{TrackID: trackID, Requests: requests, Endpoints: s.endpoints, Defaults: s.cfg.Defaults, Overrides: overrides})
		if err != nil {
			return nil, err
		}
	}
	s.closeNonIdlePhysical(trackID, finalPlan)

	outcomesByBatch := make(map[string][]Outcome)
	for _, res := range finalPlan.Requests {
		outcomesByBatch[res.BatchID] = append(outcomesByBatch[res.BatchID], outcomeFromResult(res))
	}
	batchOutcomes := make([]store.BatchOutcome, 0, len(pending))
	for _, batchID := range pending {
		out := SubmitResponse{
			TrackID: trackID, BatchID: batchID, Advanced: true,
			Requests: outcomesByBatch[batchID], Snapshot: sim.FinalSnapshot(finalPlan),
			HorizonMS: finalPlan.HorizonMS, FinalSeq: finalPlan.FinalSeq,
		}
		raw, err := json.Marshal(out)
		if err != nil {
			return nil, err
		}
		batchOutcomes = append(batchOutcomes, store.BatchOutcome{BatchID: batchID, JSON: string(raw)})
	}
	if err := s.store.ApplyPlan(ctx, store.PlanWrites{
		TrackID: trackID, HorizonMS: finalPlan.HorizonMS, FinalSeq: finalPlan.FinalSeq,
		CheckpointInterval: s.cfg.CheckpointInterval,
		Snapshots:          finalPlan.Snapshots, Transitions: finalPlan.Transitions,
		Results: finalPlan.Requests, Connections: finalPlan.Connections, Uses: finalPlan.Uses,
		IO: newFacts, Batches: batchOutcomes, IntentID: planIntent,
	}); err != nil {
		return nil, err
	}
	raw := ""
	for _, b := range batchOutcomes {
		if b.BatchID == pending[len(pending)-1] {
			raw = b.JSON
		}
	}
	var out SubmitResponse
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Service) executeNetwork(ctx context.Context, trackID string, plan *sim.Plan, targets []*sim.RequestResult, attempted map[string]bool, requestByKey map[string]model.Request) (bool, []store.IOFact) {
	byConn := make(map[string][]*sim.RequestResult)
	for _, res := range targets {
		if res.ConnectionID == "" {
			continue
		}
		byConn[res.ConnectionID] = append(byConn[res.ConnectionID], res)
	}
	connIDs := make([]string, 0, len(byConn))
	for id := range byConn {
		connIDs = append(connIDs, id)
	}
	sort.Strings(connIDs)
	var facts []store.IOFact
	changed := false
	for _, connID := range connIDs {
		group := byConn[connID]
		sort.SliceStable(group, func(i, j int) bool {
			if group[i].AcquiredAtMS != group[j].AcquiredAtMS {
				return group[i].AcquiredAtMS < group[j].AcquiredAtMS
			}
			return group[i].RequestKey < group[j].RequestKey
		})
		ep := s.endpoints[group[0].EndpointID]
		conn, result := s.physicalFor(trackID, connID, ep)
		if result.ErrorCode != "" {
			for _, target := range group {
				attempted[target.RequestKey] = true
				fact := store.IOFact{RequestKey: target.RequestKey, Status: sim.StatusError, ErrorCode: result.ErrorCode, Message: result.Message}
				facts = append(facts, fact)
				changed = true
			}
			continue
		}
		for _, target := range group {
			if ctx.Err() != nil {
				attempted[target.RequestKey] = true
				facts = append(facts, store.IOFact{RequestKey: target.RequestKey, Status: sim.StatusError, ErrorCode: model.ErrSendFailed, Message: ctx.Err().Error()})
				changed = true
				break
			}
			attempted[target.RequestKey] = true
			req, ok := requestByKey[target.RequestKey]
			if !ok {
				continue
			}
			result := client.RoundTrip(conn, req.RequestID, req.RequestBytes, req.UploadRateBPS, req.ResponseBytes, client.Options{
				DialTimeout: msDuration(ep.DialTimeoutMS), WriteTimeout: msDuration(s.cfg.Defaults.WriteTimeoutMS),
				ReadTimeout: msDuration(s.cfg.Defaults.ReadTimeoutMS), RateChunkBytes: s.cfg.Defaults.RateChunkBytes,
			})
			if result.ErrorCode != "" {
				facts = append(facts, store.IOFact{RequestKey: target.RequestKey, Status: sim.StatusError, ErrorCode: result.ErrorCode, Message: result.Message})
				changed = true
				s.dropPhysical(trackID, connID)
				break
			}
			facts = append(facts, store.IOFact{RequestKey: target.RequestKey, Status: sim.StatusDone})
		}
	}
	return changed, facts
}

func (s *Service) physicalFor(trackID, connID string, ep model.EndpointSpec) (net.Conn, client.Result) {
	key := trackID + "\x00" + connID
	s.physicalMu.Lock()
	if conn := s.physical[key]; conn != nil {
		s.physicalMu.Unlock()
		return conn, client.Result{}
	}
	s.physicalMu.Unlock()
	conn, result := client.Dial(ep.Address, msDuration(ep.DialTimeoutMS))
	if result.ErrorCode != "" {
		return nil, result
	}
	s.physicalMu.Lock()
	s.physical[key] = conn
	s.physicalMu.Unlock()
	return conn, client.Result{}
}

func (s *Service) dropPhysical(trackID, connID string) {
	key := trackID + "\x00" + connID
	s.physicalMu.Lock()
	if conn := s.physical[key]; conn != nil {
		_ = conn.Close()
		delete(s.physical, key)
	}
	s.physicalMu.Unlock()
}

func (s *Service) closeNonIdlePhysical(trackID string, plan *sim.Plan) {
	finalState := make(map[string]sim.FinalConnection)
	for _, c := range plan.Connections {
		finalState[c.ID] = c
	}
	prefix := trackID + "\x00"
	s.physicalMu.Lock()
	defer s.physicalMu.Unlock()
	for key, conn := range s.physical {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		connID := strings.TrimPrefix(key, prefix)
		c, ok := finalState[connID]
		if !ok || string(c.Stage) != "IDLE" {
			_ = conn.Close()
			delete(s.physical, key)
		}
	}
}

func (s *Service) Snapshot(ctx context.Context, trackID string, atMS *int64) (*sim.Snapshot, int64, int64, error) {
	if _, corrupt := s.corrupt.Load(trackID); corrupt {
		return nil, 0, 0, model.NewError(500, model.ErrCheckpointCorrupt, "track has a corrupt checkpoint", map[string]any{"track_id": trackID})
	}
	snap, horizon, finalSeq, err := s.store.SnapshotView(ctx, trackID, atMS)
	if err != nil {
		return nil, 0, 0, err
	}
	if snap == nil {
		return nil, 0, 0, model.NewError(404, model.ErrNotFound, "no snapshot exists for this track", map[string]any{"track_id": trackID})
	}
	return snap, horizon, finalSeq, nil
}

func (s *Service) Trace(ctx context.Context, trackID, connectionID string) ([]sim.Transition, error) {
	tr, err := s.store.Transitions(ctx, trackID, connectionID)
	if err != nil {
		return nil, err
	}
	if len(tr) == 0 {
		return nil, model.NewError(404, model.ErrNotFound, "connection trace not found", map[string]any{"track_id": trackID, "connection_id": connectionID})
	}
	return tr, nil
}

func (s *Service) LatestRecovery(ctx context.Context) (*store.RecoveryReport, error) {
	return s.store.LatestRecoveryReport(ctx)
}

func (s *Service) Recover(ctx context.Context) (*store.RecoveryReport, error) {
	started := time.Now().UTC().Format(time.RFC3339Nano)
	aborted, err := s.store.AbortPendingIntents(ctx)
	if err != nil {
		return nil, err
	}
	for i := range aborted {
		aborted[i].Status = store.IntentAborted
	}
	pendingBefore, err := s.store.PendingBatches(ctx)
	if err != nil {
		return nil, err
	}
	report := &store.RecoveryReport{StartedAt: started, AbortedIntents: aborted}
	for _, pending := range pendingBefore {
		report.PendingBatches = append(report.PendingBatches, pending.TrackID+":"+pending.BatchID)
	}

	tracks, err := s.store.ListTracks(ctx)
	if err != nil {
		return nil, err
	}
	for _, trackID := range tracks {
		if err := s.store.ValidateTrackCheckpoints(ctx, trackID); err != nil {
			s.corrupt.Store(trackID, true)
			var be *model.BusinessError
			if ok := asBusiness(err, &be); ok {
				report.ErrorCode = be.Body.Code
				report.Message = be.Body.Message
			} else {
				report.ErrorCode = model.ErrCheckpointCorrupt
				report.Message = err.Error()
			}
			log.Printf("recovery blocked for %s: %s", trackID, report.Message)
			break
		}
	}
	if report.ErrorCode == "" {
		for _, pending := range pendingBefore {
			fullID := pending.TrackID + ":" + pending.BatchID
			lock := s.trackLock(pending.TrackID)
			lock.Lock()
			_, err := s.processTrack(ctx, pending.TrackID)
			lock.Unlock()
			if err != nil {
				report.ErrorCode = model.ErrRecoveryPending
				report.Message = fmt.Sprintf("resume %s: %v", fullID, err)
				log.Printf("recovery could not resume %s: %v", fullID, err)
				break
			}
			report.ResumedBatches = append(report.ResumedBatches, fullID)
		}
	}
	if err := s.store.SaveRecoveryReport(ctx, *report); err != nil {
		return nil, err
	}
	if report.ErrorCode != "" {
		return report, model.NewError(500, report.ErrorCode, report.Message, nil)
	}
	return report, nil
}

func (s *Service) latestResponse(ctx context.Context, trackID string) (*SubmitResponse, error) {
	tracks, err := s.store.ListTracks(ctx)
	if err != nil {
		return nil, err
	}
	found := false
	for _, id := range tracks {
		if id == trackID {
			found = true
		}
	}
	if !found {
		return nil, model.NewError(404, model.ErrNotFound, "track not found", map[string]any{"track_id": trackID})
	}
	snap, horizon, finalSeq, err := s.store.SnapshotView(ctx, trackID, nil)
	if err != nil || snap == nil {
		return nil, err
	}
	return &SubmitResponse{TrackID: trackID, Advanced: false, Snapshot: snap, HorizonMS: horizon, FinalSeq: finalSeq}, nil
}

func outcomeFromResult(res sim.RequestResult) Outcome {
	out := Outcome{
		RequestID: res.RequestID, RequestKey: res.RequestKey, BatchID: res.BatchID,
		ClientID: res.ClientID, EndpointID: res.EndpointID, Status: res.Status,
		ConnectionID: res.ConnectionID, WaitedMS: res.WaitedMS, CompletedMS: res.CompletedMS,
	}
	if res.Status == sim.StatusError {
		out.Error = &model.ErrorBody{Code: res.ErrorCode, Message: res.ErrorMessage}
		if res.ErrorCode == model.ErrPoolAcquireTimeout {
			out.Error.Details = map[string]any{"waited_ms": res.WaitedMS}
		}
	}
	return out
}

func ensureUniqueRequestIDs(reqs []model.RequestInput) error {
	seen := make(map[string]struct{}, len(reqs))
	for _, req := range reqs {
		if _, ok := seen[req.RequestID]; ok {
			return model.NewError(400, model.ErrSpecInvalid, "request_id must be unique and is required for duplicate request content", map[string]any{"request_id": req.RequestID})
		}
		seen[req.RequestID] = struct{}{}
	}
	return nil
}

func automaticRequestID(req model.RequestInput) string {
	raw := fmt.Sprintf("%d\x00%s\x00%s\x00%d\x00%d\x00%d", req.TimestampMS, req.ClientID, req.EndpointID, req.RequestBytes, req.UploadRateBPS, req.ResponseBytes)
	sum := sha256.Sum256([]byte(raw))
	return "auto-" + hex.EncodeToString(sum[:12])
}

func msDuration(ms int64) time.Duration {
	if ms <= 0 {
		return time.Millisecond
	}
	return time.Duration(ms) * time.Millisecond
}

func asBusiness(err error, target **model.BusinessError) bool {
	be, ok := err.(*model.BusinessError)
	if ok {
		*target = be
	}
	return ok
}
