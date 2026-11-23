// Package joinshuffle exposes the integrable in-process facade for the local
// columnar join-shuffle aggregation engine.
package joinshuffle

import (
	"context"
	"net/http"
	"time"

	"example.com/joinshuffle/internal/httpapi"
	"example.com/joinshuffle/internal/model"
	"example.com/joinshuffle/internal/sample"
	"example.com/joinshuffle/internal/service"
	"example.com/joinshuffle/internal/snapshot"
	"example.com/joinshuffle/internal/store"
	"example.com/joinshuffle/internal/sum"
)

type BusinessError = model.BusinessError
type LoadSnapshotsRequest = model.LoadSnapshotsRequest
type SnapshotLoadInput = model.SnapshotLoadInput
type SnapshotMeta = model.SnapshotMeta
type SnapshotPairMeta = model.SnapshotPairMeta
type AggregateRequest = model.AggregateRequest
type AggregateResponse = model.AggregateResponse
type RouteDecision = model.RouteDecision
type PartitionStat = model.PartitionStat
type SkewReport = model.SkewReport
type GroupResult = model.GroupResult
type ErrorDeclaration = model.ErrorDeclaration
type PartitionInvariance = model.PartitionInvariance
type RecoveryReport = model.RecoveryReport
type RecoveryEvent = model.RecoveryEvent
type SnapshotStatus = service.SnapshotStatus
type PartialSum = sum.Partial
type Snapshot = snapshot.Snapshot

// DefaultSampleGeneratedAt is the fixed timestamp used by the reference sample generator.
var DefaultSampleGeneratedAt = sample.DefaultGeneratedAt

type JoinType = model.JoinType
type SumAlgorithm = model.SumAlgorithm
type PrecisionPath = model.PrecisionPath

const (
	JoinInner JoinType = model.JoinInner
	JoinLeft  JoinType = model.JoinLeft

	SumNaive           SumAlgorithm = model.SumNaive
	SumKahan           SumAlgorithm = model.SumKahan
	SumDoubleDoubleFix SumAlgorithm = model.SumDoubleDoubleFix
	SumAuto            SumAlgorithm = model.SumAuto

	PathFast PrecisionPath = model.PathFast
	PathHigh PrecisionPath = model.PathHigh

	CodeSnapshotInvalid            = model.CodeSnapshotInvalid
	CodePrecisionToleranceExceeded = model.CodePrecisionToleranceExceeded
	CodeConflictingRequest         = model.CodeConflictingRequest
	CodeExecutionAborted           = model.CodeExecutionAborted
	CodeInvalidRequest             = model.CodeInvalidRequest
	CodeNotFound                   = model.CodeNotFound
	CodeNotReady                   = model.CodeNotReady
	CodeExecutionInProgress        = model.CodeExecutionInProgress
	CodeInternal                   = model.CodeInternal
)

// Engine owns a SQLite store and an in-process service. Call Close when done.
type Engine struct {
	service *service.Service
	store   *store.Store
	handler http.Handler
}

// Open creates or opens a local SQLite-backed engine.
func Open(dbPath string) (*Engine, error) {
	st, err := store.Open(dbPath)
	if err != nil {
		return nil, err
	}
	svc := service.New(st)
	return &Engine{service: svc, store: st, handler: httpapi.New(svc)}, nil
}

func (e *Engine) Close() error { return e.store.Close() }

func (e *Engine) StorePath() string { return e.store.Path() }

func (e *Engine) Handler() http.Handler { return e.handler }

func (e *Engine) Recover(ctx context.Context) (RecoveryReport, error) {
	return e.service.Recover(ctx)
}

func (e *Engine) RecoveryReport(ctx context.Context) (RecoveryReport, error) {
	return e.service.RecoveryReport(ctx)
}

func (e *Engine) RestoreSnapshots(ctx context.Context) error {
	return e.service.RestoreSnapshots(ctx)
}

func (e *Engine) LoadSnapshots(ctx context.Context, req LoadSnapshotsRequest) (SnapshotPairMeta, error) {
	return e.service.LoadSnapshots(ctx, req)
}

func (e *Engine) SnapshotsStatus() SnapshotStatus { return e.service.SnapshotsStatus() }

func (e *Engine) Submit(ctx context.Context, req AggregateRequest) (*AggregateResponse, error) {
	return e.service.Submit(ctx, req)
}

func (e *Engine) GetJob(ctx context.Context, requestID string) (*AggregateResponse, error) {
	return e.service.GetJob(ctx, requestID)
}

func (e *Engine) GetPartitions(ctx context.Context, requestID string) ([]PartitionStat, error) {
	return e.service.GetPartitions(ctx, requestID)
}

func (e *Engine) GetResults(ctx context.Context, requestID string) (map[string]any, error) {
	return e.service.GetResults(ctx, requestID)
}

func (e *Engine) GetPartialSums(ctx context.Context, requestID string) ([]PartialSum, error) {
	return e.service.GetPartialSums(ctx, requestID)
}

func (e *Engine) GetErrorDeclaration(ctx context.Context, requestID string) (*ErrorDeclaration, error) {
	return e.service.GetErrorDeclaration(ctx, requestID)
}

func (e *Engine) GetRoute(ctx context.Context, requestID string) (*RouteDecision, error) {
	return e.service.GetRoute(ctx, requestID)
}

// GenerateSamplePair creates the deterministic sample pair used by the CLI and experiment.
func GenerateSamplePair(seed int64, leftRows, rightRows int, leftVersion, rightVersion uint64, generatedAt time.Time) (*Snapshot, *Snapshot, error) {
	return sample.GeneratePair(seed, leftRows, rightRows, leftVersion, rightVersion, generatedAt)
}

// WriteSnapshotFile writes a deterministic columnar snapshot and its full-file SHA-256 file.
func WriteSnapshotFile(path string, snap *Snapshot) (SnapshotMeta, error) {
	return snapshot.WriteFile(path, snap)
}

// ReadSnapshotFile validates embedded and external checksums before returning a snapshot.
func ReadSnapshotFile(path, checksumPath, expectedSHA string) (*Snapshot, error) {
	return snapshot.ReadFile(path, checksumPath, expectedSHA)
}
