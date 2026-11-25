package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"layoutread/internal/engine"
	"layoutread/internal/model"
	"layoutread/internal/store"
)

type Service struct {
	store *store.Store

	mu            sync.RWMutex
	strategy      model.Strategy
	docs          map[string]model.Document
	results       map[string]model.DocumentResult
	queryCache    map[string][]byte
	running       bool
	stopRequested bool
	generation    uint64
	hasImported   bool

	queryCounter atomic.Uint64
}

type RunOptions struct {
	StopFilePath string
	StageDelay   time.Duration
}

type Invalidation struct {
	DocID         string `json:"doc_id"`
	EarliestStage string `json:"earliest_stage"`
	Reason        string `json:"reason"`
}

type DocumentRunSummary struct {
	DocID                string   `json:"doc_id"`
	Changed              bool     `json:"changed"`
	EarliestInvalidStage string   `json:"earliest_invalid_stage,omitempty"`
	ReusedStages         []string `json:"reused_stages"`
	ResultFingerprint    string   `json:"result_fingerprint"`
}

type RunReport struct {
	StrategyVersion     int64                `json:"strategy_version"`
	StrategyFingerprint string               `json:"strategy_fingerprint"`
	Documents           []DocumentRunSummary `json:"documents"`
	Invalidations       []Invalidation       `json:"invalidations"`
	EarliestStage       string               `json:"earliest_stage,omitempty"`
	Stopped             bool                 `json:"stopped"`
	Processed           int                  `json:"processed"`
	ResultFingerprint   string               `json:"result_fingerprint"`
}

type StageStatus struct {
	Stage               string `json:"stage"`
	StageOrdinal        int64  `json:"stage_ordinal"`
	Sequence            int64  `json:"sequence"`
	InputFingerprint    string `json:"input_fingerprint"`
	ArtifactFingerprint string `json:"artifact_fingerprint"`
	StrategyVersion     int64  `json:"strategy_version"`
}

type DocumentStatus struct {
	DocID              string        `json:"doc_id"`
	ContentFingerprint string        `json:"content_fingerprint"`
	StrategyVersion    int64         `json:"strategy_version"`
	HeadSequence       int64         `json:"head_sequence"`
	Stages             []StageStatus `json:"stages"`
}

type StatusReport struct {
	StrategyVersion     int64            `json:"strategy_version"`
	StrategyFingerprint string           `json:"strategy_fingerprint"`
	Documents           []DocumentStatus `json:"documents"`
	Running             bool             `json:"running"`
	StopRequested       bool             `json:"stop_requested"`
	SnapshotDigest      string           `json:"snapshot_digest"`
	AnalysisPending     bool             `json:"analysis_pending"`
}

type QueryRequest struct {
	DocID            string
	Scope            string
	QueryID          string
	BudgetMS         int
	HasBudget        bool
	SimulatedDelayMS int
}

type QueryResponse struct {
	QueryID           string          `json:"query_id"`
	DocID             string          `json:"doc_id"`
	Scope             string          `json:"scope"`
	CacheKey          string          `json:"cache_key"`
	CacheHit          bool            `json:"cache_hit"`
	ResultFingerprint string          `json:"result_fingerprint"`
	Data              json.RawMessage `json:"data"`
}

type ExportReport struct {
	StrategyVersion     int64                  `json:"strategy_version"`
	StrategyFingerprint string                 `json:"strategy_fingerprint"`
	Documents           []model.DocumentResult `json:"documents"`
	ResultFingerprint   string                 `json:"result_fingerprint"`
}

type StopReport struct {
	Running       bool `json:"running"`
	StopRequested bool `json:"stop_requested"`
}

func New(st *store.Store) *Service {
	return &Service{
		store:      st,
		docs:       make(map[string]model.Document),
		results:    make(map[string]model.DocumentResult),
		queryCache: make(map[string][]byte),
	}
}

func (s *Service) LoadPersistedStrategy(ctx context.Context) error {
	strategy, found, err := s.store.LoadStrategy(ctx)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	s.mu.Lock()
	if s.strategy.Version == 0 {
		s.strategy = strategy
	}
	s.mu.Unlock()
	return nil
}

func (s *Service) Import(ctx context.Context, bundle model.Bundle) error {
	if err := bundle.Strategy.Validate(); err != nil {
		return err
	}
	if len(bundle.Documents) == 0 {
		return model.NewError(model.CodeInvalidRequest, "at least one document is required", "")
	}
	docs := make(map[string]model.Document, len(bundle.Documents))
	for _, doc := range bundle.Documents {
		if err := doc.Validate(); err != nil {
			return err
		}
		if _, exists := docs[doc.DocID]; exists {
			return model.NewError(model.CodeDocumentInvalid, "duplicate doc_id", doc.DocID)
		}
		docs[doc.DocID] = doc
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return model.NewError(model.CodeAlreadyRunning, "cannot import while an analysis run is active", "")
	}
	if err := s.store.SaveStrategy(ctx, bundle.Strategy); err != nil {
		return err
	}
	if err := s.store.SetAnalysisPending(ctx, true); err != nil {
		return err
	}
	s.strategy = bundle.Strategy
	s.docs = docs
	s.results = make(map[string]model.DocumentResult)
	s.queryCache = make(map[string][]byte)
	s.generation++
	s.hasImported = true
	return nil
}

func (s *Service) Strategy() model.Strategy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.strategy
}

func (s *Service) Documents() []model.Document {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]model.Document, 0, len(s.docs))
	for _, doc := range s.docs {
		result = append(result, doc)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].DocID < result[j].DocID })
	return result
}

func (s *Service) Run(ctx context.Context, options RunOptions) (RunReport, error) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return RunReport{}, model.NewError(model.CodeAlreadyRunning, "an analysis run is already active", "")
	}
	strategy := s.strategy
	if err := strategy.Validate(); err != nil {
		s.mu.Unlock()
		return RunReport{}, err
	}
	docs := make([]model.Document, 0, len(s.docs))
	for _, doc := range s.docs {
		docs = append(docs, doc)
	}
	if len(docs) == 0 {
		s.mu.Unlock()
		return RunReport{}, model.NewError(model.CodeInvalidRequest, "no documents have been imported", "")
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].DocID < docs[j].DocID })
	s.running = true
	s.stopRequested = false
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	report := RunReport{
		StrategyVersion:     strategy.Version,
		StrategyFingerprint: strategy.Fingerprint(),
		Documents:           make([]DocumentRunSummary, 0, len(docs)),
		Invalidations:       make([]Invalidation, 0),
	}
	runCtx := context.WithoutCancel(ctx)
	for _, doc := range docs {
		if s.shouldStop(options.StopFilePath) {
			report.Stopped = true
			break
		}
		result, reused, earliest, err := s.runDocument(runCtx, doc, strategy, options.StageDelay)
		if err != nil {
			return report, err
		}
		s.mu.Lock()
		s.results[doc.DocID] = result
		s.mu.Unlock()
		report.Processed++
		summary := DocumentRunSummary{
			DocID:                doc.DocID,
			Changed:              earliest != "",
			EarliestInvalidStage: earliest,
			ReusedStages:         reused,
			ResultFingerprint:    result.ResultFingerprint,
		}
		report.Documents = append(report.Documents, summary)
		if earliest != "" {
			report.Invalidations = append(report.Invalidations, Invalidation{
				DocID: doc.DocID, EarliestStage: earliest, Reason: invalidationReason(earliest),
			})
			if report.EarliestStage == "" || model.StageNumber(earliest) < model.StageNumber(report.EarliestStage) {
				report.EarliestStage = earliest
			}
		}
	}
	results := make([]model.DocumentResult, 0, len(report.Documents))
	s.mu.RLock()
	for _, summary := range report.Documents {
		if result, ok := s.results[summary.DocID]; ok {
			results = append(results, result)
		}
	}
	s.mu.RUnlock()
	report.ResultFingerprint = resultSetFingerprint(strategy.Fingerprint(), results)
	if !report.Stopped && report.Processed == len(docs) {
		if err := s.store.SetAnalysisPending(runCtx, false); err != nil {
			return report, err
		}
	}
	return report, nil
}

func (s *Service) runDocument(ctx context.Context, doc model.Document, strategy model.Strategy, stageDelay time.Duration) (model.DocumentResult, []string, string, error) {
	contentFingerprint := doc.ContentFingerprint()
	normalizeInput := engine.NormalizeInputFingerprint(doc, strategy)
	normalized, normalizeReused, normalizeSeq, err := prepareStage(ctx, s.store, doc.DocID, model.StageNormalize, normalizeInput, contentFingerprint, strategy, false, stageDelay, func() (*model.NormalizedArtifact, error) {
		return engine.Normalize(doc, strategy)
	})
	if err != nil {
		return model.DocumentResult{}, nil, "", err
	}
	forceFrom := 2
	earliest := ""
	reused := make([]string, 0, 4)
	if normalizeReused {
		reused = append(reused, model.StageNormalize)
		forceFrom = 0
	} else {
		earliest = model.StageNormalize
	}

	columnInput := engine.ColumnInputFingerprint(normalized, strategy)
	columns, columnReused, columnSeq, err := prepareStage(ctx, s.store, doc.DocID, model.StageColumns, columnInput, contentFingerprint, strategy, forceFrom > 0 && forceFrom <= 2, stageDelay, func() (*model.ColumnArtifact, error) {
		return engine.InferColumns(normalized, strategy)
	})
	if err != nil {
		return model.DocumentResult{}, reused, earliest, err
	}
	if columnReused {
		reused = append(reused, model.StageColumns)
	} else {
		if earliest == "" {
			earliest = model.StageColumns
		}
		forceFrom = 3
	}

	orderInput := engine.OrderInputFingerprint(columns, strategy)
	order, orderReused, orderSeq, err := prepareStage(ctx, s.store, doc.DocID, model.StageOrder, orderInput, contentFingerprint, strategy, forceFrom > 0 && forceFrom <= 3, stageDelay, func() (*model.OrderArtifact, error) {
		return engine.GenerateOrder(columns, strategy)
	})
	if err != nil {
		return model.DocumentResult{}, reused, earliest, err
	}
	if orderReused {
		reused = append(reused, model.StageOrder)
	} else {
		if earliest == "" {
			earliest = model.StageOrder
		}
		forceFrom = 4
	}

	summaryInput := engine.SummaryInputFingerprint(order)
	summary, summaryReused, summarySeq, err := prepareStage(ctx, s.store, doc.DocID, model.StageSummary, summaryInput, contentFingerprint, strategy, forceFrom > 0 && forceFrom <= 4, stageDelay, func() (*model.SummaryArtifact, error) {
		return engine.Summarize(columns, order)
	})
	if err != nil {
		return model.DocumentResult{}, reused, earliest, err
	}
	if summaryReused {
		reused = append(reused, model.StageSummary)
	} else if earliest == "" {
		earliest = model.StageSummary
	}
	stages := []model.StageFingerprint{
		{Stage: model.StageNormalize, InputFingerprint: normalizeInput, ArtifactFingerprint: normalized.Fingerprint(), Sequence: normalizeSeq, Reused: normalizeReused},
		{Stage: model.StageColumns, InputFingerprint: columnInput, ArtifactFingerprint: columns.Fingerprint(), Sequence: columnSeq, Reused: columnReused},
		{Stage: model.StageOrder, InputFingerprint: orderInput, ArtifactFingerprint: order.Fingerprint(), Sequence: orderSeq, Reused: orderReused},
		{Stage: model.StageSummary, InputFingerprint: summaryInput, ArtifactFingerprint: summary.Fingerprint(), Sequence: summarySeq, Reused: summaryReused},
	}
	result := engine.BuildDocumentResult(doc, strategy, stages, order, summary)
	return result, reused, earliest, nil
}

func prepareStage[T any](ctx context.Context, st *store.Store, docID, stage, inputFingerprint, contentFingerprint string, strategy model.Strategy, force bool, stageDelay time.Duration, compute func() (T, error)) (T, bool, int64, error) {
	var zero T
	checkpoint, found, err := st.GetCheckpoint(ctx, docID, stage)
	if err != nil {
		return zero, false, 0, err
	}
	if found && !force && checkpoint.InputFingerprint == inputFingerprint {
		var artifact T
		if err := json.Unmarshal([]byte(checkpoint.ArtifactJSON), &artifact); err != nil {
			return zero, false, 0, &model.Error{Code: model.CodeCheckpointCorrupt, Message: "stored artifact JSON is invalid", DocID: docID, Stage: stage}
		}
		if got := model.Digest(artifact); got != checkpoint.ArtifactFingerprint {
			return zero, false, 0, &model.Error{Code: model.CodeCheckpointCorrupt, Message: "stored artifact digest mismatch", DocID: docID, Stage: stage}
		}
		return artifact, true, checkpoint.Seq, nil
	}
	if stageDelay > 0 {
		time.Sleep(stageDelay)
	}
	artifact, err := compute()
	if err != nil {
		return zero, false, 0, err
	}
	artifactJSON, err := json.Marshal(artifact)
	if err != nil {
		return zero, false, 0, err
	}
	artifactFingerprint := model.Digest(artifact)
	seq, err := st.NextSequence(ctx, docID)
	if err != nil {
		return zero, false, 0, err
	}
	published, err := st.Publish(ctx, store.Checkpoint{
		DocID:               docID,
		Stage:               stage,
		StageOrdinal:        int64(model.StageNumber(stage)),
		Seq:                 seq,
		InputFingerprint:    inputFingerprint,
		StrategyVersion:     strategy.Version,
		ArtifactSummary:     artifactFingerprint,
		ArtifactFingerprint: artifactFingerprint,
		ArtifactJSON:        string(artifactJSON),
		ContentFingerprint:  contentFingerprint,
	})
	if err != nil {
		return zero, false, 0, err
	}
	return artifact, false, published.Seq, nil
}

func (s *Service) shouldStop(stopFilePath string) bool {
	s.mu.RLock()
	requested := s.stopRequested
	s.mu.RUnlock()
	if requested {
		return true
	}
	if stopFilePath == "" {
		return false
	}
	_, err := os.Stat(stopFilePath)
	return err == nil
}

func (s *Service) RequestStop() StopReport {
	s.mu.Lock()
	s.stopRequested = true
	report := StopReport{Running: s.running, StopRequested: true}
	s.mu.Unlock()
	return report
}

func (s *Service) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

func (s *Service) Results() ([]model.DocumentResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]model.DocumentResult, 0, len(s.results))
	for _, doc := range s.docs {
		value, ok := s.results[doc.DocID]
		if !ok {
			return nil, model.NewError(model.CodeNotFound, "analysis result is not available for document", doc.DocID)
		}
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].DocID < result[j].DocID })
	return result, nil
}

func (s *Service) Query(ctx context.Context, request QueryRequest) (QueryResponse, error) {
	start := time.Now()
	queryID := request.QueryID
	if queryID == "" {
		queryID = fmt.Sprintf("q-%06d", s.queryCounter.Add(1))
	}
	if request.Scope != "blocks" && request.Scope != "confidence" && request.Scope != "columns" {
		err := model.NewError(model.CodeInvalidRequest, "scope must be blocks, confidence or columns", request.DocID)
		err.QueryID = queryID
		return QueryResponse{}, err
	}
	s.mu.RLock()
	strategy := s.strategy
	generation := s.generation
	s.mu.RUnlock()
	budgetMS := strategy.LatencyBudgetMS
	if request.HasBudget {
		budgetMS = request.BudgetMS
	}
	if budgetMS <= 0 {
		return QueryResponse{}, latencyError(queryID, request.DocID, start, budgetMS)
	}
	budget := time.Duration(budgetMS) * time.Millisecond

	s.mu.RLock()
	result, ok := s.results[request.DocID]
	hasImported := s.hasImported
	s.mu.RUnlock()
	if !ok {
		if hasImported {
			err := model.NewError(model.CodeNotFound, "analysis result is not available; run the imported bundle first", request.DocID)
			err.QueryID = queryID
			return QueryResponse{}, err
		}
		pending, err := s.store.AnalysisPending(ctx)
		if err != nil {
			modelErr := model.AsError(err)
			modelErr.QueryID = queryID
			return QueryResponse{}, modelErr
		}
		if pending {
			err := model.NewError(model.CodeNotFound, "analysis checkpoints are pending rebuild; run/resume first", request.DocID)
			err.QueryID = queryID
			return QueryResponse{}, err
		}
		loaded, loadErr := s.loadStoredResult(ctx, request.DocID)
		if loadErr != nil {
			err := model.AsError(loadErr)
			err.QueryID = queryID
			return QueryResponse{}, err
		}
		result = loaded
		s.mu.Lock()
		if s.generation != generation {
			s.mu.Unlock()
			err := model.NewError(model.CodeNotFound, "analysis state changed while the query was loading", request.DocID)
			err.QueryID = queryID
			return QueryResponse{}, err
		}
		s.results[request.DocID] = loaded
		s.mu.Unlock()
	}
	contentFP := result.ContentFingerprint
	cacheKey := queryCacheKey(contentFP, strategy, request.Scope, result.ResultFingerprint)
	s.mu.RLock()
	cached, cacheHit := s.queryCache[cacheKey]
	if cacheHit {
		cached = append([]byte(nil), cached...)
	}
	s.mu.RUnlock()
	if cacheHit {
		if time.Since(start) > budget {
			return QueryResponse{}, latencyError(queryID, request.DocID, start, budgetMS)
		}
		if s.generationChanged(generation) {
			err := model.NewError(model.CodeNotFound, "analysis result was replaced during query", request.DocID)
			err.QueryID = queryID
			return QueryResponse{}, err
		}
		return QueryResponse{
			QueryID: queryID, DocID: request.DocID, Scope: request.Scope, CacheKey: cacheKey,
			CacheHit: true, ResultFingerprint: result.ResultFingerprint, Data: json.RawMessage(cached),
		}, nil
	}

	type computeResult struct {
		data []byte
		err  error
	}
	channel := make(chan computeResult, 1)
	go func() {
		if request.SimulatedDelayMS > 0 {
			time.Sleep(time.Duration(request.SimulatedDelayMS) * time.Millisecond)
		}
		data, err := computeQueryData(result, request.Scope)
		if err == nil {
			s.mu.Lock()
			if s.generation == generation {
				s.queryCache[cacheKey] = append([]byte(nil), data...)
			}
			s.mu.Unlock()
		}
		channel <- computeResult{data: data, err: err}
	}()
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case computed := <-channel:
		if time.Since(start) > budget {
			return QueryResponse{}, latencyError(queryID, request.DocID, start, budgetMS)
		}
		if computed.err != nil {
			e := model.AsError(computed.err)
			e.QueryID = queryID
			return QueryResponse{}, e
		}
		if s.generationChanged(generation) {
			err := model.NewError(model.CodeNotFound, "analysis result was replaced during query", request.DocID)
			err.QueryID = queryID
			return QueryResponse{}, err
		}
		return QueryResponse{
			QueryID: queryID, DocID: request.DocID, Scope: request.Scope, CacheKey: cacheKey,
			CacheHit: false, ResultFingerprint: result.ResultFingerprint, Data: json.RawMessage(computed.data),
		}, nil
	case <-timer.C:
		return QueryResponse{}, latencyError(queryID, request.DocID, start, budgetMS)
	case <-ctx.Done():
		e := model.AsError(ctx.Err())
		e.QueryID = queryID
		return QueryResponse{}, e
	}
}

func (s *Service) generationChanged(expected uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.generation != expected
}

func (s *Service) loadStoredResult(ctx context.Context, docID string) (model.DocumentResult, error) {
	states, err := s.store.ListDocuments(ctx)
	if err != nil {
		return model.DocumentResult{}, err
	}
	for _, state := range states {
		if state.DocID != docID {
			continue
		}
		s.mu.RLock()
		strategy := s.strategy
		s.mu.RUnlock()
		if strategy.Version == 0 {
			if err := s.LoadPersistedStrategy(ctx); err != nil {
				return model.DocumentResult{}, err
			}
			s.mu.RLock()
			strategy = s.strategy
			s.mu.RUnlock()
		}
		return loadResultFromCheckpoints(ctx, s.store, state, strategy)
	}
	return model.DocumentResult{}, model.NewError(model.CodeNotFound, "analysis result is not available for document", docID)
}

func computeQueryData(result model.DocumentResult, scope string) ([]byte, error) {
	switch scope {
	case "blocks":
		return json.Marshal(struct {
			DocID             string                    `json:"doc_id"`
			ResultFingerprint string                    `json:"result_fingerprint"`
			Blocks            []model.BlockSequenceItem `json:"blocks"`
		}{
			DocID: result.DocID, ResultFingerprint: result.ResultFingerprint, Blocks: blockSequence(result),
		})
	case "confidence":
		return json.Marshal(struct {
			DocID             string                  `json:"doc_id"`
			ResultFingerprint string                  `json:"result_fingerprint"`
			Confidences       []model.OrderConfidence `json:"confidences"`
		}{DocID: result.DocID, ResultFingerprint: result.ResultFingerprint, Confidences: result.Confidences})
	case "columns":
		return json.Marshal(struct {
			DocID             string             `json:"doc_id"`
			ResultFingerprint string             `json:"result_fingerprint"`
			Columns           []model.Column     `json:"columns"`
			ColumnStats       []model.ColumnStat `json:"column_stats"`
		}{DocID: result.DocID, ResultFingerprint: result.ResultFingerprint, Columns: result.Columns, ColumnStats: result.ColumnStats})
	default:
		return nil, model.NewError(model.CodeInvalidRequest, "unsupported query scope", result.DocID)
	}
}

func blockSequence(result model.DocumentResult) []model.BlockSequenceItem {
	items := make([]model.BlockSequenceItem, len(result.Blocks))
	for i, block := range result.Blocks {
		items[i] = model.BlockSequenceItem{
			Order: block.Order, BlockID: block.BlockID, Column: block.Column, Kind: block.Kind,
			FullSpan: block.FullSpan, X: block.X, Y: block.Y, W: block.W, H: block.H,
			Text: block.Text, Lang: block.Lang, FontTier: block.FontTier,
			SourceIndices: append([]int(nil), block.SourceIndices...),
		}
	}
	return items
}

func latencyError(queryID, docID string, start time.Time, budgetMS int) *model.Error {
	elapsedMS := model.Round6(float64(time.Since(start).Microseconds()) / 1000.0)
	return &model.Error{
		Code:      model.CodeLatencyBudget,
		Message:   fmt.Sprintf("query exceeded latency budget of %d ms", budgetMS),
		DocID:     docID,
		QueryID:   queryID,
		ElapsedMS: elapsedMS,
	}
}

func queryCacheKey(contentFingerprint string, strategy model.Strategy, scope, resultFingerprint string) string {
	return contentFingerprint + "|" + strconv.FormatInt(strategy.Version, 10) + "|" + strategy.Fingerprint() + "|" + scope + "|" + resultFingerprint
}

func invalidationReason(stage string) string {
	switch stage {
	case model.StageNormalize:
		return "document content or normalization policy changed"
	case model.StageColumns:
		return "normalized artifact or column policy changed"
	case model.StageOrder:
		return "column artifact, cross-column priority or confidence weights changed"
	case model.StageSummary:
		return "reading order artifact changed"
	default:
		return "stage input changed"
	}
}

func resultSetFingerprint(strategyFingerprint string, results []model.DocumentResult) string {
	sorted := append([]model.DocumentResult(nil), results...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].DocID < sorted[j].DocID })
	return model.Digest(struct {
		Strategy string                 `json:"strategy"`
		Results  []model.DocumentResult `json:"results"`
	}{Strategy: strategyFingerprint, Results: sorted})
}

func (s *Service) Status(ctx context.Context) (StatusReport, error) {
	s.mu.RLock()
	strategy := s.strategy
	running := s.running
	stopRequested := s.stopRequested
	imported := make(map[string]model.Document, len(s.docs))
	for id, doc := range s.docs {
		imported[id] = doc
	}
	s.mu.RUnlock()
	docStates, err := s.store.ListDocuments(ctx)
	if err != nil {
		return StatusReport{}, err
	}
	checkpoints, err := s.store.ListCheckpoints(ctx, "")
	if err != nil {
		return StatusReport{}, err
	}
	byDoc := make(map[string][]StageStatus)
	for _, checkpoint := range checkpoints {
		byDoc[checkpoint.DocID] = append(byDoc[checkpoint.DocID], StageStatus{
			Stage: checkpoint.Stage, StageOrdinal: checkpoint.StageOrdinal, Sequence: checkpoint.Seq,
			InputFingerprint: checkpoint.InputFingerprint, ArtifactFingerprint: checkpoint.ArtifactFingerprint,
			StrategyVersion: checkpoint.StrategyVersion,
		})
	}
	report := StatusReport{
		StrategyVersion: strategy.Version, StrategyFingerprint: strategy.Fingerprint(),
		Running: running, StopRequested: stopRequested,
		Documents: make([]DocumentStatus, 0, len(docStates)+len(imported)),
	}
	pending, err := s.store.AnalysisPending(ctx)
	if err != nil {
		return StatusReport{}, err
	}
	report.AnalysisPending = pending
	snapshotDigest, _, err := s.store.Snapshot(ctx)
	if err != nil {
		return StatusReport{}, err
	}
	report.SnapshotDigest = snapshotDigest
	seen := make(map[string]struct{}, len(docStates))
	for _, state := range docStates {
		seen[state.DocID] = struct{}{}
		stages := byDoc[state.DocID]
		sort.Slice(stages, func(i, j int) bool { return stages[i].StageOrdinal < stages[j].StageOrdinal })
		report.Documents = append(report.Documents, DocumentStatus{
			DocID: state.DocID, ContentFingerprint: state.ContentFingerprint,
			StrategyVersion: state.StrategyVersion, HeadSequence: state.Seq, Stages: stages,
		})
	}
	for docID, doc := range imported {
		if _, ok := seen[docID]; ok {
			continue
		}
		report.Documents = append(report.Documents, DocumentStatus{
			DocID: docID, ContentFingerprint: doc.ContentFingerprint(), HeadSequence: 0, Stages: []StageStatus{},
		})
	}
	sort.Slice(report.Documents, func(i, j int) bool { return report.Documents[i].DocID < report.Documents[j].DocID })
	return report, nil
}

func (s *Service) Export(ctx context.Context) (ExportReport, error) {
	s.mu.RLock()
	strategy := s.strategy
	cachedResults := make(map[string]model.DocumentResult, len(s.results))
	for id, result := range s.results {
		cachedResults[id] = result
	}
	hasImported := s.hasImported
	importedDocs := make([]model.Document, 0, len(s.docs))
	for _, doc := range s.docs {
		importedDocs = append(importedDocs, doc)
	}
	s.mu.RUnlock()
	if hasImported {
		sort.Slice(importedDocs, func(i, j int) bool { return importedDocs[i].DocID < importedDocs[j].DocID })
		results := make([]model.DocumentResult, 0, len(importedDocs))
		for _, doc := range importedDocs {
			result, ok := cachedResults[doc.DocID]
			if !ok {
				return ExportReport{}, model.NewError(model.CodeNotFound, "analysis result is not available; run the imported bundle first", doc.DocID)
			}
			results = append(results, result)
		}
		return ExportReport{
			StrategyVersion: strategy.Version, StrategyFingerprint: strategy.Fingerprint(),
			Documents: results, ResultFingerprint: resultSetFingerprint(strategy.Fingerprint(), results),
		}, nil
	}
	pending, err := s.store.AnalysisPending(ctx)
	if err != nil {
		return ExportReport{}, err
	}
	if pending {
		return ExportReport{}, model.NewError(model.CodeNotFound, "analysis checkpoints are pending rebuild; run/resume first", "")
	}
	docStates, err := s.store.ListDocuments(ctx)
	if err != nil {
		return ExportReport{}, err
	}
	results := make([]model.DocumentResult, 0, len(docStates))
	for _, state := range docStates {
		if result, ok := cachedResults[state.DocID]; ok {
			results = append(results, result)
			continue
		}
		result, err := loadResultFromCheckpoints(ctx, s.store, state, strategy)
		if err != nil {
			return ExportReport{}, err
		}
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].DocID < results[j].DocID })
	report := ExportReport{
		StrategyVersion: strategy.Version, StrategyFingerprint: strategy.Fingerprint(),
		Documents: results, ResultFingerprint: resultSetFingerprint(strategy.Fingerprint(), results),
	}
	return report, nil
}

func loadResultFromCheckpoints(ctx context.Context, st *store.Store, state store.DocumentState, strategy model.Strategy) (model.DocumentResult, error) {
	normalizedCP, ok, err := st.GetCheckpoint(ctx, state.DocID, model.StageNormalize)
	if err != nil || !ok {
		if err == nil {
			err = model.NewError(model.CodeNotFound, "normalization checkpoint is missing", state.DocID)
		}
		return model.DocumentResult{}, err
	}
	columnCP, ok, err := st.GetCheckpoint(ctx, state.DocID, model.StageColumns)
	if err != nil || !ok {
		if err == nil {
			err = model.NewError(model.CodeNotFound, "column checkpoint is missing", state.DocID)
		}
		return model.DocumentResult{}, err
	}
	orderCP, ok, err := st.GetCheckpoint(ctx, state.DocID, model.StageOrder)
	if err != nil || !ok {
		if err == nil {
			err = model.NewError(model.CodeNotFound, "order checkpoint is missing", state.DocID)
		}
		return model.DocumentResult{}, err
	}
	summaryCP, ok, err := st.GetCheckpoint(ctx, state.DocID, model.StageSummary)
	if err != nil || !ok {
		if err == nil {
			err = model.NewError(model.CodeNotFound, "summary checkpoint is missing", state.DocID)
		}
		return model.DocumentResult{}, err
	}
	var order model.OrderArtifact
	var summary model.SummaryArtifact
	if err := json.Unmarshal([]byte(orderCP.ArtifactJSON), &order); err != nil {
		return model.DocumentResult{}, err
	}
	if err := json.Unmarshal([]byte(summaryCP.ArtifactJSON), &summary); err != nil {
		return model.DocumentResult{}, err
	}
	stages := []model.StageFingerprint{
		{Stage: model.StageNormalize, InputFingerprint: normalizedCP.InputFingerprint, ArtifactFingerprint: normalizedCP.ArtifactFingerprint, Sequence: normalizedCP.Seq, Reused: true},
		{Stage: model.StageColumns, InputFingerprint: columnCP.InputFingerprint, ArtifactFingerprint: columnCP.ArtifactFingerprint, Sequence: columnCP.Seq, Reused: true},
		{Stage: model.StageOrder, InputFingerprint: orderCP.InputFingerprint, ArtifactFingerprint: orderCP.ArtifactFingerprint, Sequence: orderCP.Seq, Reused: true},
		{Stage: model.StageSummary, InputFingerprint: summaryCP.InputFingerprint, ArtifactFingerprint: summaryCP.ArtifactFingerprint, Sequence: summaryCP.Seq, Reused: true},
	}
	result := model.DocumentResult{
		DocID: state.DocID, ContentFingerprint: state.ContentFingerprint,
		StrategyVersion: strategy.Version, StrategyFingerprint: strategy.Fingerprint(),
		StageFingerprints: stages, Blocks: order.Blocks, Confidences: order.Confidences,
		ColumnStats: summary.Columns, Diagnostics: summary.Diagnostics,
	}
	for _, stat := range summary.Columns {
		result.Columns = append(result.Columns, model.Column{Index: stat.ColumnIndex, X0: stat.X0, X1: stat.X1, BoxCount: stat.BoxCount})
	}
	result.ResultFingerprint = model.ResultFingerprint(result)
	return result, nil
}

func (s *Service) CacheSize() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.queryCache)
}

func (s *Service) ClearQueryCache() {
	s.mu.Lock()
	s.queryCache = make(map[string][]byte)
	s.mu.Unlock()
}

func IsStale(err error) bool {
	var modelErr *model.Error
	return errors.As(err, &modelErr) && modelErr.Code == model.CodeStaleCheckpoint
}
