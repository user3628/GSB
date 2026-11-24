package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"driftconverge/internal/apperr"
	"driftconverge/internal/batch"
	"driftconverge/internal/detect"
	"driftconverge/internal/model"
	"driftconverge/internal/plan"
	"driftconverge/internal/store"
)

type AnalysisReport struct {
	SnapshotVersion        int64          `json:"snapshotVersion"`
	SnapshotChecksum       string         `json:"snapshotChecksum"`
	GeneratedAt            string         `json:"generatedAt"`
	ObjectsPath            string         `json:"objectsPath"`
	PolicyPath             string         `json:"policyPath"`
	ValuesPath             string         `json:"valuesPath"`
	ObjectCount            int            `json:"objectCount"`
	DriftObjectCount       int            `json:"driftObjectCount"`
	DetectionPath          string         `json:"detectionPath"`
	ExactBatchSignature    string         `json:"exactBatchSignature"`
	BucketedBatchSignature string         `json:"bucketedBatchSignature"`
	BatchPathReconciled    bool           `json:"batchPathReconciled"`
	BatchVersionID         int64          `json:"batchVersionId"`
	BatchSignature         string         `json:"batchSignature"`
	PlanSignature          string         `json:"planSignature"`
	Drift                  *detect.Report `json:"drift"`
	Batches                *batch.Report  `json:"batches"`
	Plan                   *plan.Report   `json:"plan"`
}

type Session struct {
	Store   *store.Store
	Samples *model.Samples

	Analysis *AnalysisReport

	objectMu sync.RWMutex
	roundMu  sync.Mutex
	rounds   map[string]*roundCall
}

type roundCall struct {
	done   chan struct{}
	report *ExecutionReport
	err    error
}

func NewSession(st *store.Store, samples *model.Samples) *Session {
	return &Session{Store: st, Samples: samples, rounds: make(map[string]*roundCall)}
}

func (s *Session) Analyze(ctx context.Context) (*AnalysisReport, error) {
	if s.Store == nil || s.Samples == nil {
		return nil, apperr.New(apperr.InvalidArgument, "store and samples are required")
	}
	current, err := s.Store.CurrentPolicyVersion(ctx)
	if err != nil {
		return nil, err
	}
	if s.Samples.Policy.Version <= current {
		return nil, apperr.WithDetails(apperr.StaleSnapshot, map[string]int64{
			"currentVersion": current, "incomingVersion": s.Samples.Policy.Version,
		}, "snapshot version %d is not greater than current version %d", s.Samples.Policy.Version, current)
	}
	driftReport, err := detect.Analyze(s.Samples.Cluster, s.Samples.Policy, s.Samples.Values)
	if err != nil {
		return &AnalysisReport{
			SnapshotVersion:  s.Samples.Cluster.SnapshotVersion,
			SnapshotChecksum: s.Samples.Policy.Checksum,
			GeneratedAt:      s.Samples.Policy.GeneratedAt,
			ObjectsPath:      s.Samples.ObjectsPath,
			PolicyPath:       s.Samples.PolicyPath,
			ValuesPath:       s.Samples.ValuesPath,
			ObjectCount:      len(s.Samples.Cluster.Objects),
			Drift:            driftReport,
		}, err
	}
	batchReport, batchErr := batch.Organize(s.Samples.Cluster, driftReport.Results, s.Samples.Policy)
	if batchReport == nil {
		return nil, batchErr
	}
	exactBatches, _ := batch.Organize(s.Samples.Cluster, driftReport.ExactResults, s.Samples.Policy)
	bucketedBatches, _ := batch.Organize(s.Samples.Cluster, driftReport.BucketedResults, s.Samples.Policy)
	batchPathReconciled := sameBatchPartition(exactBatches, bucketedBatches) && sameBatchPartition(batchReport, exactBatches)
	if !batchPathReconciled {
		report := &AnalysisReport{
			SnapshotVersion:        s.Samples.Cluster.SnapshotVersion,
			SnapshotChecksum:       s.Samples.Policy.Checksum,
			GeneratedAt:            s.Samples.Policy.GeneratedAt,
			ObjectsPath:            s.Samples.ObjectsPath,
			PolicyPath:             s.Samples.PolicyPath,
			ValuesPath:             s.Samples.ValuesPath,
			ObjectCount:            len(s.Samples.Cluster.Objects),
			DetectionPath:          driftReport.SelectedPath,
			ExactBatchSignature:    batchSignature(exactBatches),
			BucketedBatchSignature: batchSignature(bucketedBatches),
			BatchPathReconciled:    false,
			Drift:                  driftReport,
			Batches:                batchReport,
		}
		return report, apperr.WithDetails(apperr.PathDivergence, map[string]string{
			"exactBatchSignature":    report.ExactBatchSignature,
			"bucketedBatchSignature": report.BucketedBatchSignature,
			"selectedBatchSignature": report.Batches.StableSignature,
		}, "exact and bucketed paths produced different batch partitions")
	}
	costPlan, planErr := plan.Build(s.Samples.Cluster, driftReport, batchReport, s.Samples.Policy)
	if planErr != nil {
		return nil, planErr
	}
	if err := s.Store.CommitAnalysis(ctx, s.Samples.Cluster, s.Samples.Policy, driftReport); err != nil {
		return nil, err
	}
	batchVersionID, err := s.Store.CommitBatchPlan(ctx, s.Samples.Cluster.SnapshotVersion, "", batchReport, costPlan)
	if err != nil {
		return nil, err
	}
	driftCount := 0
	for _, result := range driftReport.Results {
		if result.HasDrift {
			driftCount++
		}
	}
	report := &AnalysisReport{
		SnapshotVersion:        s.Samples.Cluster.SnapshotVersion,
		SnapshotChecksum:       s.Samples.Policy.Checksum,
		GeneratedAt:            s.Samples.Policy.GeneratedAt,
		ObjectsPath:            s.Samples.ObjectsPath,
		PolicyPath:             s.Samples.PolicyPath,
		ValuesPath:             s.Samples.ValuesPath,
		ObjectCount:            len(s.Samples.Cluster.Objects),
		DriftObjectCount:       driftCount,
		DetectionPath:          driftReport.SelectedPath,
		ExactBatchSignature:    batchSignature(exactBatches),
		BucketedBatchSignature: batchSignature(bucketedBatches),
		BatchPathReconciled:    true,
		BatchVersionID:         batchVersionID,
		BatchSignature:         batchReport.StableSignature,
		PlanSignature:          costPlan.StableSignature,
		Drift:                  driftReport,
		Batches:                batchReport,
		Plan:                   costPlan,
	}
	s.Analysis = report
	if batchErr != nil {
		return report, batchErr
	}
	return report, nil
}

func sameBatchPartition(left, right *batch.Report) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.StableSignature == right.StableSignature && left.ErrorCode == right.ErrorCode
}

func batchSignature(report *batch.Report) string {
	if report == nil {
		return ""
	}
	return report.StableSignature
}

type ExecuteOptions struct {
	RoundID  string
	Endpoint string
}

type ObjectWriteResult struct {
	ObjectID           string            `json:"objectId"`
	State              string            `json:"state"`
	Attempts           int               `json:"attempts"`
	ErrorCode          string            `json:"errorCode,omitempty"`
	ErrorMessage       string            `json:"errorMessage,omitempty"`
	ResidualCategories []detect.Category `json:"residualCategories,omitempty"`
	ResidualFields     []string          `json:"residualFields,omitempty"`
	DurationMicros     int64             `json:"durationMicros"`
	WriteRecordIDs     []string          `json:"writeRecordIds,omitempty"`
}

type BatchExecutionResult struct {
	BatchID         string              `json:"batchId"`
	Index           int                 `json:"index"`
	State           string              `json:"state"`
	ObjectIDs       []string            `json:"objectIds"`
	MaxSeverityRank int                 `json:"maxSeverityRank"`
	StartedAt       string              `json:"startedAt"`
	DurationMicros  int64               `json:"durationMicros"`
	Objects         []ObjectWriteResult `json:"objects"`
	ErrorCode       string              `json:"errorCode,omitempty"`
}

type SkippedBatch struct {
	BatchID string `json:"batchId"`
	Index   int    `json:"index"`
	Order   int    `json:"skipOrder"`
	Reason  string `json:"reason"`
	Detail  string `json:"detail"`
}

type BudgetDecision struct {
	BatchID          string `json:"batchId"`
	Index            int    `json:"index"`
	DecisionOrder    int    `json:"decisionOrder"`
	PrioritySeverity int    `json:"prioritySeverity"`
	CandidateMicros  int64  `json:"candidateMicros"`
	Candidate        string `json:"candidate"`
	LimitMicros      int64  `json:"limitMicros"`
	Limit            string `json:"limit"`
	TolerancePPM     int64  `json:"tolerancePpm"`
	ToleranceMicros  int64  `json:"toleranceMicros"`
	Tolerance        string `json:"tolerance"`
	Decision         string `json:"decision"`
	WithinTolerance  bool   `json:"withinTolerance"`
	Explanation      string `json:"explanation"`
}

type BudgetReport struct {
	RoundTimeBudgetMs     int64            `json:"roundTimeBudgetMs"`
	RoundTimeBudgetMicros int64            `json:"roundTimeBudgetMicros"`
	TolerancePPM          int64            `json:"tolerancePpm"`
	OrderingRule          string           `json:"orderingRule"`
	EstimatedAcceptedCost string           `json:"estimatedAcceptedCost"`
	EstimatedRemaining    string           `json:"estimatedRemaining"`
	Decisions             []BudgetDecision `json:"decisions"`
}

type ExecutionReport struct {
	RoundID           string                 `json:"roundId"`
	SnapshotVersion   int64                  `json:"snapshotVersion"`
	BatchVersionID    int64                  `json:"batchVersionId"`
	Endpoint          string                 `json:"endpoint"`
	State             string                 `json:"state"`
	Reused            bool                   `json:"reused"`
	ErrorCode         string                 `json:"errorCode,omitempty"`
	StartedAt         string                 `json:"startedAt"`
	DurationMicros    int64                  `json:"durationMicros"`
	Budget            BudgetReport           `json:"budget"`
	ExecutedBatches   []BatchExecutionResult `json:"executedBatches"`
	SkippedBatches    []SkippedBatch         `json:"skippedBatches"`
	SkippedOrder      []string               `json:"skippedOrder"`
	ConvergedObjects  int                    `json:"convergedObjects"`
	UnresolvedObjects int                    `json:"unresolvedObjects"`
	FailedObjects     int                    `json:"failedObjects"`
}

func (s *Session) ExecuteRound(ctx context.Context, opts ExecuteOptions) (*ExecutionReport, error) {
	if s.Analysis == nil {
		return nil, apperr.New(apperr.InvalidArgument, "analyze a snapshot before executing a round")
	}
	if opts.RoundID == "" {
		return nil, apperr.New(apperr.InvalidArgument, "round id is required")
	}
	endpoint, err := validateLoopbackEndpoint(opts.Endpoint)
	if err != nil {
		return nil, err
	}

	s.roundMu.Lock()
	if call, ok := s.rounds[opts.RoundID]; ok {
		s.roundMu.Unlock()
		<-call.done
		if call.report != nil {
			copy := *call.report
			copy.Reused = true
			return &copy, call.err
		}
		return nil, call.err
	}
	call := &roundCall{done: make(chan struct{})}
	s.rounds[opts.RoundID] = call
	s.roundMu.Unlock()
	defer func() {
		close(call.done)
		s.roundMu.Lock()
		s.rounds[opts.RoundID] = call
		s.roundMu.Unlock()
	}()

	existing, err := s.Store.GetRound(ctx, opts.RoundID)
	if err != nil {
		call.err = err
		return nil, err
	}
	if existing != nil {
		report, existingErr := reportFromRound(existing)
		if existing.State != "running" {
			report.Reused = true
			call.report, call.err = report, existingErr
			return report, existingErr
		}
		// A running row without a local owner is either another process or a
		// crash artifact. Wait briefly for another process; recovery is explicit.
		waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		record, waitErr := waitForRoundTerminal(waitCtx, s.Store, opts.RoundID)
		if waitErr != nil {
			call.err = waitErr
			return nil, waitErr
		}
		report, existingErr = reportFromRound(record)
		report.Reused = true
		call.report, call.err = report, existingErr
		return report, existingErr
	}
	roundRecord, created, err := s.Store.CreateRound(ctx, opts.RoundID, s.Analysis.SnapshotVersion, s.Analysis.BatchVersionID)
	if err != nil {
		call.err = err
		return nil, err
	}
	if !created {
		if roundRecord.State == "running" {
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			roundRecord, err = waitForRoundTerminal(waitCtx, s.Store, opts.RoundID)
			if err != nil {
				call.err = err
				return nil, err
			}
		}
		report, existingErr := reportFromRound(roundRecord)
		report.Reused = true
		call.report, call.err = report, existingErr
		return report, existingErr
	}
	report, execErr := s.executePlannedRound(ctx, opts.RoundID, endpoint)
	report.Reused = false
	state := "completed"
	errorCode := ""
	if execErr != nil {
		state = "failed"
		errorCode = string(apperr.CodeOf(execErr))
	}
	if saveErr := s.Store.SaveRoundResult(context.Background(), opts.RoundID, state, errorCode, report); saveErr != nil && execErr == nil {
		execErr = saveErr
	}
	call.report, call.err = report, execErr
	return report, execErr
}

func waitForRoundTerminal(ctx context.Context, st *store.Store, roundID string) (*store.RoundRecord, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		record, err := st.GetRound(ctx, roundID)
		if err != nil {
			return nil, err
		}
		if record != nil && record.State != "running" {
			return record, nil
		}
		select {
		case <-ctx.Done():
			return nil, apperr.New(apperr.RoundInProgress, "round %s is still running after wait deadline", roundID)
		case <-ticker.C:
		}
	}
}

func reportFromRound(record *store.RoundRecord) (*ExecutionReport, error) {
	if record == nil {
		return nil, apperr.New(apperr.PersistenceFailed, "round record is nil")
	}
	var report ExecutionReport
	if record.ResultJSON != "" {
		if err := json.Unmarshal([]byte(record.ResultJSON), &report); err != nil {
			return nil, apperr.New(apperr.PersistenceFailed, "decode persisted round result: %v", err)
		}
	}
	report.RoundID = record.RoundID
	report.SnapshotVersion = record.SnapshotVersion
	report.BatchVersionID = record.BatchVersionID
	if record.State == "aborted" || record.State == "failed" {
		code := apperr.Code(record.ErrorCode)
		if code == "" {
			code = apperr.Internal
		}
		return &report, apperr.WithDetails(code, map[string]any{"roundId": record.RoundID, "state": record.State}, "round %s finished with state %s", record.RoundID, record.State)
	}
	return &report, nil
}

func (s *Session) objectByID(id string) *model.Object {
	for i := range s.Samples.Cluster.Objects {
		if s.Samples.Cluster.Objects[i].ID == id {
			return &s.Samples.Cluster.Objects[i]
		}
	}
	return nil
}

func (s *Session) setObjectActual(id string, actual map[string]model.FieldValue, sources map[string]model.FieldSource) {
	s.objectMu.Lock()
	defer s.objectMu.Unlock()
	obj := s.objectByID(id)
	if obj == nil {
		return
	}
	obj.Actual = cloneFieldValues(actual)
	if sources != nil {
		obj.Sources = cloneFieldSources(sources)
	}
}

func cloneFieldValues(input map[string]model.FieldValue) map[string]model.FieldValue {
	out := make(map[string]model.FieldValue, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func cloneFieldSources(input map[string]model.FieldSource) map[string]model.FieldSource {
	out := make(map[string]model.FieldSource, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func sortedUnique(values []string) []string {
	seen := map[string]struct{}{}
	result := []string{}
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

var _ = fmt.Sprintf
