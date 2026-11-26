package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"logagg/internal/agg"
	"logagg/internal/codec"
	"logagg/internal/errs"
	"logagg/internal/model"
	"logagg/internal/plan"
	"logagg/internal/sample"
	"logagg/internal/sched"
	"logagg/internal/shard"
	"logagg/internal/store"
)

type Config struct {
	MemoryLimitBytes int64
	MaxConcurrency   int
	QueueCapacity    int
	MinRowWidthBytes int64
}

func DefaultConfig() Config {
	return Config{
		MemoryLimitBytes: 16 << 20,
		MaxConcurrency:   2,
		QueueCapacity:    128,
		MinRowWidthBytes: plan.DefaultInt64PairRowWidth,
	}
}

type Engine struct {
	store     *store.Store
	registry  *shard.Registry
	generator *sample.Generator
	cfg       Config
	pool      *sched.Pool

	sessions sync.Map
	shardMu  sync.Mutex
}

func New(st *store.Store, cfg Config) *Engine {
	if cfg.MemoryLimitBytes <= 0 {
		cfg.MemoryLimitBytes = DefaultConfig().MemoryLimitBytes
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 2
	}
	if cfg.QueueCapacity < 0 {
		cfg.QueueCapacity = 128
	}
	if cfg.MinRowWidthBytes <= 0 {
		cfg.MinRowWidthBytes = plan.DefaultInt64PairRowWidth
	}
	return &Engine{
		store:     st,
		registry:  shard.NewRegistry(),
		generator: sample.NewGenerator(),
		cfg:       cfg,
		pool:      sched.NewPool(cfg.MaxConcurrency, cfg.QueueCapacity),
	}
}

func (e *Engine) Close() { e.pool.Close() }

func (e *Engine) Store() *store.Store { return e.store }

func (e *Engine) Registry() *shard.Registry { return e.registry }

func (e *Engine) Generator() *sample.Generator { return e.generator }

type Session struct {
	id string

	writeMu sync.Mutex
	stateMu sync.RWMutex

	loadOnce sync.Once
	loadErr  error

	definitionHash string
	groupColumn    string
	metricColumn   string
	inputScale     int
	total          *agg.TotalSum
	segments       map[string]*agg.SegmentSum
	version        int64
}

func newSession(id string) *Session {
	return &Session{id: id, total: agg.NewTotalSum(), segments: map[string]*agg.SegmentSum{}}
}

func (s *Session) ensureLoaded(ctx context.Context, st *store.Store) error {
	s.loadOnce.Do(func() {
		sums, err := st.LoadSessionPartialSums(ctx, s.id)
		if err != nil {
			s.loadErr = err
			return
		}
		for _, sum := range sums {
			key := segmentKey(sum.ShardID, sum.BaseVersion, sum.BlockNo, sum.SegmentStart)
			if _, exists := s.segments[key]; exists {
				continue
			}
			s.segments[key] = sum
			if s.definitionHash == "" {
				s.definitionHash = sum.DefinitionHash
			}
			if err := s.total.MergeSegment(sum); err != nil {
				s.loadErr = err
				return
			}
			s.version++
		}
	})
	return s.loadErr
}

func (e *Engine) session(id string) *Session {
	actual, _ := e.sessions.LoadOrStore(id, newSession(id))
	return actual.(*Session)
}

type Item struct {
	ShardID string `json:"shard_id"`
	Version int64  `json:"version"`
	BlockNo int    `json:"block_no"`
}

type AdvanceRequest struct {
	SessionID        string        `json:"session_id"`
	BatchID          string        `json:"batch_id"`
	Items            []Item        `json:"items"`
	GroupColumn      string        `json:"group_column"`
	MetricColumn     string        `json:"metric_column"`
	InputScale       int           `json:"input_scale"`
	Precision        agg.Precision `json:"precision"`
	ByteBudget       int64         `json:"byte_budget"`
	MemoryLimitBytes int64         `json:"memory_limit_bytes,omitempty"`
}

type SegmentReport struct {
	Key            string `json:"key"`
	ShardID        string `json:"shard_id"`
	Version        int64  `json:"version"`
	Partition      int    `json:"partition"`
	BlockNo        int    `json:"block_no"`
	StartRow       int    `json:"start_row"`
	EndRow         int    `json:"end_row"`
	Status         string `json:"status"`
	RowCount       int    `json:"row_count"`
	GroupCount     int    `json:"group_count"`
	ProcessedBytes int64  `json:"processed_bytes"`
	IntentID       string `json:"intent_id,omitempty"`
}

type AdvanceResponse struct {
	SessionID      string               `json:"session_id"`
	BatchID        string               `json:"batch_id"`
	Reused         bool                 `json:"reused"`
	Continuation   bool                 `json:"continuation,omitempty"`
	RequestHash    string               `json:"request_hash"`
	DefinitionHash string               `json:"definition_hash"`
	Status         string               `json:"status"`
	Processed      []SegmentReport      `json:"processed"`
	ReusedSegments []SegmentReport      `json:"reused_segments"`
	Deferred       []sched.DeferredTask `json:"deferred"`
	Plan           *sched.RoundPlan     `json:"plan,omitempty"`
	Result         *agg.QueryResult     `json:"result,omitempty"`
	SessionVersion int64                `json:"session_version"`
}

type preparedTask struct {
	task     sched.Task
	item     Item
	block    *model.Block
	plan     plan.Segment
	intentID string
}

type segmentOutcome struct {
	prepared preparedTask
	groups   []int64
	metrics  []int64
	sum      *agg.SegmentSum
	report   SegmentReport
}

func (e *Engine) Advance(ctx context.Context, req AdvanceRequest) (*AdvanceResponse, error) {
	return e.runAdvance(ctx, req, false)
}

func (e *Engine) Continue(ctx context.Context, req AdvanceRequest) (*AdvanceResponse, error) {
	return e.runAdvance(ctx, req, true)
}

func (e *Engine) runAdvance(ctx context.Context, req AdvanceRequest, continuation bool) (*AdvanceResponse, error) {
	if req.SessionID == "" || req.BatchID == "" {
		return nil, errs.New(errs.EInvalidRequest, "session_id and batch_id are required")
	}
	if req.GroupColumn == "" || req.MetricColumn == "" {
		return nil, errs.New(errs.EInvalidRequest, "group_column and metric_column are required by query contract")
	}
	if len(req.Items) == 0 {
		return nil, errs.New(errs.EInvalidRequest, "at least one block item is required")
	}
	if req.ByteBudget <= 0 {
		return nil, errs.New(errs.EInvalidRequest, "byte_budget must be positive for a scheduling round")
	}
	requestHash, err := hashBatchContent(req)
	if err != nil {
		return nil, err
	}

	sess := e.session(req.SessionID)
	sess.writeMu.Lock()
	defer sess.writeMu.Unlock()
	if err := sess.ensureLoaded(ctx, e.store); err != nil {
		return nil, errs.New(errs.EInternal, "load session: %v", err)
	}

	existing, err := e.store.GetBatch(ctx, req.BatchID)
	if err != nil {
		return nil, errs.New(errs.EInternal, "get batch: %v", err)
	}
	if existing != nil {
		if existing.RequestHash != requestHash {
			return nil, errs.New(errs.EConflictBatch, "batch %s was already submitted with different content", req.BatchID).
				WithDetail("batch_id", req.BatchID).
				WithDetail("existing_request_hash", existing.RequestHash).
				WithDetail("conflicting_request_hash", requestHash)
		}
		var stored AdvanceResponse
		if err := json.Unmarshal([]byte(existing.ResultJSON), &stored); err != nil {
			return nil, errs.New(errs.EInternal, "decode stored batch result: %v", err)
		}
		if !continuation {
			stored.Reused = true
			return &stored, nil
		}
		if existing.Status == "completed" {
			stored.Reused = true
			stored.Continuation = true
			return &stored, nil
		}
	} else if continuation {
		return nil, errs.New(errs.ENotFound, "cannot continue unknown batch %s", req.BatchID)
	}

	definitionHash := hashDefinition(req.GroupColumn, req.MetricColumn, req.InputScale)
	sess.stateMu.Lock()
	if sess.definitionHash == "" {
		sess.definitionHash = definitionHash
		sess.groupColumn = req.GroupColumn
		sess.metricColumn = req.MetricColumn
		sess.inputScale = req.InputScale
	} else if sess.definitionHash != definitionHash {
		sess.stateMu.Unlock()
		return nil, errs.New(errs.EConflictQuery, "session %s already has query definition %s", req.SessionID, sess.definitionHash).
			WithDetail("existing_definition_hash", sess.definitionHash).
			WithDetail("requested_definition_hash", definitionHash)
	}
	sess.stateMu.Unlock()

	memoryLimit := req.MemoryLimitBytes
	if memoryLimit <= 0 {
		memoryLimit = e.cfg.MemoryLimitBytes
	}

	prepared := []preparedTask{}
	reused := []SegmentReport{}
	for _, item := range req.Items {
		pkg, block, err := e.loadBlock(ctx, item)
		if err != nil {
			return nil, err
		}
		resolvedVersion := item.Version
		if resolvedVersion == 0 {
			resolvedVersion = pkg.Manifest.Version
		}
		item.Version = resolvedVersion
		if err := validateColumns(pkg.Manifest, req.GroupColumn, req.MetricColumn); err != nil {
			return nil, err
		}
		reserved := int64(len(block.GroupPayload)+len(block.MetricPayload)) + 64
		blockPlan, err := plan.PlanBlock(plan.Request{
			BlockNo:          block.Meta.BlockNo,
			Partition:        block.Meta.Partition,
			RowCount:         block.Meta.RowCount,
			DeclaredBytes:    block.Meta.UncompressedBytes,
			MemoryLimitBytes: memoryLimit,
			ReservedBytes:    reserved,
			MinRowWidthBytes: e.cfg.MinRowWidthBytes,
		})
		if err != nil {
			return nil, err
		}
		for _, segment := range blockPlan.Segments {
			key := segmentKey(item.ShardID, item.Version, item.BlockNo, segment.StartRow)
			sess.stateMu.RLock()
			existingSum, exists := sess.segments[key]
			sess.stateMu.RUnlock()
			if exists {
				if existingSum.DefinitionHash != definitionHash {
					return nil, errs.New(errs.EConflictQuery, "segment %s has different definition", key)
				}
				reused = append(reused, SegmentReport{
					Key: key, ShardID: item.ShardID, Version: item.Version, Partition: block.Meta.Partition,
					BlockNo: item.BlockNo, StartRow: segment.StartRow, EndRow: segment.EndRow,
					Status: "reused_committed_partial_sum", RowCount: segment.RowCount,
					GroupCount: len(existingSum.Groups), ProcessedBytes: segment.EstimatedBytes,
				})
				continue
			}
			blockStarted := segment.StartRow > 0 && e.hasEarlierCommittedSegment(sess, item, segment.StartRow)
			task := sched.Task{
				ID: key, SessionID: req.SessionID, ShardID: item.ShardID, Version: item.Version,
				Partition: block.Meta.Partition, BlockNo: item.BlockNo,
				SegmentStart: segment.StartRow, SegmentEnd: segment.EndRow,
				Bytes: segment.EstimatedBytes, BlockStarted: blockStarted,
			}
			prepared = append(prepared, preparedTask{task: task, item: item, block: block, plan: segment})
		}
	}

	tasks := make([]sched.Task, 0, len(prepared))
	for _, candidate := range prepared {
		tasks = append(tasks, candidate.task)
	}
	roundPlan, err := sched.PlanRound(tasks, e.pool.Active(), e.cfg.MaxConcurrency, e.cfg.QueueCapacity, req.ByteBudget)
	if err != nil {
		return nil, err
	}

	admittedTasks := append([]sched.PlannedTask{}, roundPlan.Run...)
	admittedTasks = append(admittedTasks, roundPlan.Queued...)
	admittedByID := make(map[string]sched.PlannedTask, len(admittedTasks))
	for _, task := range admittedTasks {
		admittedByID[task.ID] = task
	}

	admitted := []preparedTask{}
	for _, candidate := range prepared {
		if _, ok := admittedByID[candidate.task.ID]; !ok {
			continue
		}
		intentID := "segment-start:" + candidate.task.ID + ":" + strconv.FormatInt(time.Now().UnixNano(), 36)
		intent := store.Intent{
			ID: intentID, Kind: "segment_start", Status: "pending",
			SessionID: req.SessionID, BatchID: req.BatchID, ShardID: candidate.task.ShardID,
			Version: candidate.task.Version, BlockNo: candidate.task.BlockNo,
			SegmentStart: candidate.task.SegmentStart, SegmentEnd: candidate.task.SegmentEnd,
		}
		if err := e.store.BeginSegment(ctx, intent, candidate.task.Partition); err != nil {
			return nil, errs.New(errs.EInternal, "write segment intent: %v", err)
		}
		candidate.intentID = intentID
		admitted = append(admitted, candidate)
	}

	outcomes := make([]segmentOutcome, len(admitted))
	var decodeWG sync.WaitGroup
	var outcomeMu sync.Mutex
	var firstErr error
	for index, candidate := range admitted {
		index := index
		candidate := candidate
		decodeWG.Add(1)
		job := sched.Job{Task: candidate.task, Run: func(jobCtx context.Context) error {
			defer decodeWG.Done()
			groups, err := codec.DecodeColumn(candidate.block, codec.ColumnGroup, candidate.plan.StartRow, candidate.plan.RowCount)
			if err != nil {
				outcomeMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				outcomeMu.Unlock()
				return err
			}
			metrics, err := codec.DecodeColumn(candidate.block, codec.ColumnMetric, candidate.plan.StartRow, candidate.plan.RowCount)
			if err != nil {
				outcomeMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				outcomeMu.Unlock()
				return err
			}
			sum := agg.NewSegmentSum(req.SessionID, candidate.task.ShardID, candidate.task.Version,
				candidate.task.Partition, candidate.task.BlockNo, candidate.task.SegmentStart, candidate.task.SegmentEnd, definitionHash)
			if err := sum.AddRows(groups, metrics); err != nil {
				outcomeMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				outcomeMu.Unlock()
				return err
			}
			outcomes[index] = segmentOutcome{
				prepared: candidate, groups: groups, metrics: metrics, sum: sum,
				report: SegmentReport{
					Key: candidate.task.ID, ShardID: candidate.task.ShardID, Version: candidate.task.Version,
					Partition: candidate.task.Partition, BlockNo: candidate.task.BlockNo,
					StartRow: candidate.task.SegmentStart, EndRow: candidate.task.SegmentEnd,
					Status: "decoded_and_committed", RowCount: len(groups), GroupCount: len(sum.Groups),
					ProcessedBytes: candidate.task.Bytes, IntentID: candidate.intentID,
				},
			}
			return nil
		}}
		if err := e.pool.Submit(ctx, job); err != nil {
			decodeWG.Done()
			decodeWG.Wait()
			return nil, err
		}
	}
	decodeWG.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	sort.Slice(outcomes, func(i, j int) bool { return outcomes[i].prepared.task.ID < outcomes[j].prepared.task.ID })
	processed := []SegmentReport{}
	for _, outcome := range outcomes {
		partialIntentID := "partial-commit:" + outcome.prepared.task.ID + ":" + strconv.FormatInt(time.Now().UnixNano(), 36)
		if err := e.store.InsertIntent(ctx, store.Intent{
			ID: partialIntentID, Kind: "partial_commit", Status: "pending",
			SessionID: req.SessionID, BatchID: req.BatchID, ShardID: outcome.sum.ShardID,
			Version: outcome.sum.BaseVersion, BlockNo: outcome.sum.BlockNo,
			SegmentStart: outcome.sum.SegmentStart, SegmentEnd: outcome.sum.SegmentEnd,
		}); err != nil {
			return nil, errs.New(errs.EInternal, "write partial commit intent: %v", err)
		}
		if err := e.store.CommitPartialSum(ctx, outcome.sum, partialIntentID); err != nil {
			return nil, errs.New(errs.EInternal, "commit partial sum: %v", err)
		}
		if err := e.store.MarkIntentCommitted(ctx, outcome.prepared.intentID); err != nil {
			return nil, err
		}

		key := outcome.prepared.task.ID
		sess.stateMu.Lock()
		if _, exists := sess.segments[key]; !exists {
			sess.segments[key] = outcome.sum
			if err := sess.total.MergeSegment(outcome.sum); err != nil {
				sess.stateMu.Unlock()
				return nil, err
			}
			sess.version++
		}
		sessionVersion := sess.version
		sess.stateMu.Unlock()
		outcome.report.Status = "decoded_and_committed"
		_ = sessionVersion
		processed = append(processed, outcome.report)
	}

	sess.stateMu.RLock()
	precision := req.Precision
	if precision.InputScale == 0 && req.InputScale != 0 {
		precision.InputScale = req.InputScale
	}
	if precision.Mode == "" {
		precision.Mode = agg.RoundHalfUp
	}
	result, err := agg.Finalize(sess.total, precision)
	sessionVersion := sess.version
	sess.stateMu.RUnlock()
	if err != nil {
		return nil, err
	}

	status := "completed"
	if len(roundPlan.Deferred) > 0 {
		status = "deferred"
	}
	response := &AdvanceResponse{
		SessionID: req.SessionID, BatchID: req.BatchID, Continuation: continuation,
		RequestHash: requestHash, DefinitionHash: definitionHash, Status: status,
		Processed: processed, ReusedSegments: reused, Deferred: roundPlan.Deferred,
		Plan: roundPlan, Result: result, SessionVersion: sessionVersion,
	}
	resultJSON, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	if err := e.store.SaveBatch(ctx, store.BatchRecord{
		BatchID: req.BatchID, SessionID: req.SessionID, RequestHash: requestHash,
		Status: status, ResultJSON: string(resultJSON),
	}); err != nil {
		return nil, errs.New(errs.EInternal, "save batch idempotency record: %v", err)
	}
	return response, nil
}

func (e *Engine) Query(ctx context.Context, sessionID string, precision agg.Precision) (*agg.QueryResult, int64, error) {
	sess := e.session(sessionID)
	if err := sess.ensureLoaded(ctx, e.store); err != nil {
		return nil, 0, errs.New(errs.EInternal, "load session: %v", err)
	}
	sess.stateMu.RLock()
	defer sess.stateMu.RUnlock()
	if precision.InputScale == 0 {
		precision.InputScale = sess.inputScale
	}
	if precision.Mode == "" {
		precision.Mode = agg.RoundHalfUp
	}
	result, err := agg.Finalize(sess.total, precision)
	if err != nil {
		return nil, 0, err
	}
	return result, sess.version, nil
}

func (e *Engine) CommitGeneratedShard(ctx context.Context, shardID string, version int64) (*shard.Committed, error) {
	pkg := e.generator.Shard(version)
	if shardID != "" {
		pkg.Manifest.ShardID = shardID
		for i := range pkg.Manifest.Blocks {
			_ = i
		}
	}
	return e.CommitShard(ctx, pkg)
}

func (e *Engine) CommitShard(ctx context.Context, pkg *model.ShardPackage) (*shard.Committed, error) {
	e.shardMu.Lock()
	defer e.shardMu.Unlock()
	if err := shard.Validate(pkg); err != nil {
		return nil, err
	}
	currentVersion, exists, err := e.store.CurrentShardVersion(ctx, pkg.Manifest.ShardID)
	if err != nil {
		return nil, errs.New(errs.EInternal, "read current shard: %v", err)
	}
	if exists && pkg.Manifest.Version <= currentVersion {
		return nil, errs.New(errs.EShardInvalid,
			"version not monotonic in SQLite for shard %s: committed=%d submitted=%d",
			pkg.Manifest.ShardID, currentVersion, pkg.Manifest.Version).
			WithDetail("committed_version", currentVersion).
			WithDetail("submitted_version", pkg.Manifest.Version)
	}
	intentID := "shard-switch:" + pkg.Manifest.ShardID + ":v" + strconv.FormatInt(pkg.Manifest.Version, 10)
	if err := e.store.InsertIntent(ctx, store.Intent{
		ID: intentID, Kind: "shard_switch", Status: "pending",
		ShardID: pkg.Manifest.ShardID, Version: pkg.Manifest.Version,
	}); err != nil {
		return nil, errs.New(errs.EInternal, "write shard switch intent: %v", err)
	}
	indexHash := hashShardIndex(pkg)
	if err := e.store.CommitShard(ctx, pkg, indexHash, intentID); err != nil {
		return nil, errs.New(errs.EInternal, "commit shard: %v", err)
	}
	committed, err := e.registry.Commit(pkg)
	if err != nil {
		return nil, err
	}
	return committed, nil
}

func (e *Engine) Recover(ctx context.Context) (*store.RecoveryReport, error) {
	return e.store.Recover(ctx)
}

func (e *Engine) loadBlock(ctx context.Context, item Item) (*model.ShardPackage, *model.Block, error) {
	committed, ok := e.registry.Get(item.ShardID, item.Version)
	var pkg *model.ShardPackage
	if ok {
		pkg = committed.Package
	} else {
		loaded, err := e.store.LoadShardPackage(ctx, item.ShardID, item.Version)
		if err != nil {
			return nil, nil, errs.New(errs.EInternal, "load shard %s v%d: %v", item.ShardID, item.Version, err)
		}
		pkg = loaded
	}
	if pkg == nil {
		current, ok := e.registry.Current(item.ShardID)
		if ok && item.Version == 0 {
			pkg = current.Package
		} else if item.Version == 0 {
			loaded, err := e.store.LoadShardPackage(ctx, item.ShardID, 0)
			if err != nil {
				return nil, nil, errs.New(errs.EInternal, "load current shard: %v", err)
			}
			pkg = loaded
		}
	}
	if pkg == nil {
		return nil, nil, errs.New(errs.ENotFound, "shard %s version %d not found", item.ShardID, item.Version)
	}
	if err := shard.Validate(pkg); err != nil {
		return nil, nil, err
	}
	version := item.Version
	if version == 0 {
		version = pkg.Manifest.Version
		item.Version = version
	}
	block := pkg.BlockByNo(item.BlockNo)
	if block == nil {
		return nil, nil, errs.New(errs.ENotFound, "block %d not found in shard %s v%d", item.BlockNo, item.ShardID, version)
	}
	return pkg, block, nil
}

func (e *Engine) hasEarlierCommittedSegment(sess *Session, item Item, startRow int) bool {
	sess.stateMu.RLock()
	defer sess.stateMu.RUnlock()
	for key, sum := range sess.segments {
		if sum.ShardID == item.ShardID && sum.BaseVersion == item.Version && sum.BlockNo == item.BlockNo && sum.SegmentEnd <= startRow && sum.SegmentEnd > 0 {
			_ = key
			return true
		}
	}
	return false
}

func validateColumns(shard model.Shard, groupColumn, metricColumn string) error {
	foundGroup, foundMetric := false, false
	for _, column := range shard.Columns {
		if column.Name == groupColumn {
			foundGroup = true
		}
		if column.Name == metricColumn {
			foundMetric = true
		}
	}
	if !foundGroup || !foundMetric {
		return errs.New(errs.EInvalidRequest, "query columns group=%q metric=%q not in shard columns", groupColumn, metricColumn).
			WithDetail("group_found", foundGroup).WithDetail("metric_found", foundMetric)
	}
	return nil
}

func segmentKey(shardID string, version int64, blockNo, start int) string {
	return fmt.Sprintf("%s:v%d:b%d:s%d", shardID, version, blockNo, start)
}

func hashDefinition(groupColumn, metricColumn string, inputScale int) string {
	h := sha256.New()
	fmt.Fprintf(h, "group=%s\nmetric=%s\nscale=%d\ncontract=int64-fixed-v1", groupColumn, metricColumn, inputScale)
	return hex.EncodeToString(h.Sum(nil))
}

func DefinitionHash(groupColumn, metricColumn string, inputScale int) string {
	return hashDefinition(groupColumn, metricColumn, inputScale)
}

func hashCanonical(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func hashBatchContent(req AdvanceRequest) (string, error) {
	content := struct {
		SessionID        string        `json:"session_id"`
		BatchID          string        `json:"batch_id"`
		Items            []Item        `json:"items"`
		GroupColumn      string        `json:"group_column"`
		MetricColumn     string        `json:"metric_column"`
		InputScale       int           `json:"input_scale"`
		Precision        agg.Precision `json:"precision"`
		MemoryLimitBytes int64         `json:"memory_limit_bytes"`
	}{
		SessionID: req.SessionID, BatchID: req.BatchID, Items: req.Items,
		GroupColumn: req.GroupColumn, MetricColumn: req.MetricColumn,
		InputScale: req.InputScale, Precision: req.Precision,
		MemoryLimitBytes: req.MemoryLimitBytes,
	}
	return hashCanonical(content)
}

func hashShardIndex(pkg *model.ShardPackage) string {
	metas := append([]model.BlockMeta(nil), pkg.Manifest.Blocks...)
	sort.Slice(metas, func(i, j int) bool { return metas[i].BlockNo < metas[j].BlockNo })
	h := sha256.New()
	fmt.Fprintf(h, "%s|%d", pkg.Manifest.ShardID, pkg.Manifest.Version)
	for _, meta := range metas {
		fmt.Fprintf(h, "|%d:%s:%s", meta.BlockNo, meta.Encoding, meta.Digest)
	}
	return hex.EncodeToString(h.Sum(nil))
}
