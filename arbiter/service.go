// Package arbiter exposes the lease timing adjudicator as an embeddable
// service. Callers do not need to import the internal packages.
package arbiter

import (
	"context"
	"fmt"
	"time"

	"leasearbiter/internal/core"
	"leasearbiter/internal/engine"
	"leasearbiter/internal/loopback"
	"leasearbiter/internal/orchestrator"
	"leasearbiter/internal/store"
)

type Snapshot = core.Snapshot
type Dataset = core.Dataset
type Holder = core.Holder
type Observation = core.Observation
type ObservationRecord = core.ObservationRecord
type Policy = core.Policy
type BizError = core.BizError
type SnapshotSubmission = core.SnapshotSubmission
type ReadResult = core.ReadResult
type BatchReport = engine.BatchReport
type ProcessOptions = engine.ProcessOptions
type RecoveryReport = store.RecoveryReport

func ComputeSnapshotDigest(snapshot Snapshot) (string, error) {
	return core.ComputeSnapshotDigest(snapshot)
}

func DefaultPolicy() Policy {
	return core.DefaultPolicy()
}

func ValidateDataset(dataset Dataset) *BizError {
	return core.ValidateDataset(dataset)
}

func ValidateSnapshot(snapshot Snapshot) *BizError {
	return core.ValidateSnapshot(snapshot)
}

func ValidatePolicy(policy Policy) *BizError {
	return core.ValidatePolicy(policy)
}

const (
	CodeObservationInvalid = core.CodeObservationInvalid
	CodeReportMismatch     = core.CodeReportMismatch
	CodeHealTimeout        = core.CodeHealTimeout
	CodeHealRejected       = core.CodeHealRejected
	CodeHealFailed         = core.CodeHealFailed
	CodeQueueFull          = core.CodeQueueFull
	CodeStaleSnapshot      = core.CodeStaleSnapshot
	CodeSnapshotInvalid    = core.CodeSnapshotInvalid
	CodeStaleRead          = core.CodeStaleRead
	CodeStoreFailed        = core.CodeStoreFailed
)

type CoordinatorConfig struct {
	Concurrency   int
	QueueCapacity int
	Budget        time.Duration
}

type Config struct {
	// DBPath is the SQLite file used for intents and immutable state versions.
	DBPath string
	// EndpointURL points at a local loopback heal endpoint. When empty, Config
	// starts an embedded loopback endpoint owned by Service.
	EndpointURL string
	Coordinator CoordinatorConfig
	HealTimeout time.Duration
	Clock       func() time.Time
}

type Service struct {
	engine          *engine.Engine
	store           *store.Store
	ownedEndpoint   *loopback.Server
	startupRecovery RecoveryReport
}

func Open(config Config) (*Service, error) {
	if config.DBPath == "" {
		return nil, fmt.Errorf("DBPath is required")
	}
	coordinatorConfig := orchestrator.Config{
		Concurrency:   config.Coordinator.Concurrency,
		QueueCapacity: config.Coordinator.QueueCapacity,
		Budget:        config.Coordinator.Budget,
	}
	coordinator, err := orchestrator.New(coordinatorConfig)
	if err != nil {
		return nil, err
	}
	stateStore, err := store.Open(config.DBPath)
	if err != nil {
		return nil, err
	}
	// A restart must reconcile durable PREPARED intents before accepting new
	// work. The report is kept available to the embedding scheduler.
	startupRecovery, err := stateStore.Recover(context.Background())
	if err != nil {
		_ = stateStore.Close()
		return nil, err
	}
	endpointURL := config.EndpointURL
	var ownedEndpoint *loopback.Server
	if endpointURL == "" {
		ownedEndpoint, err = loopback.Start("127.0.0.1:0", loopback.HandlerConfig{})
		if err != nil {
			_ = stateStore.Close()
			return nil, err
		}
		endpointURL = ownedEndpoint.URL()
	}
	timeout := config.HealTimeout
	if timeout <= 0 {
		timeout = time.Second
	}
	dispatcher := loopback.NewDispatcher(endpointURL, timeout)
	app, err := engine.New(engine.Config{
		Store:       stateStore,
		Dispatcher:  dispatcher,
		Coordinator: coordinator,
		Clock:       config.Clock,
	})
	if err != nil {
		if ownedEndpoint != nil {
			_ = ownedEndpoint.Close()
		}
		_ = stateStore.Close()
		return nil, err
	}
	return &Service{engine: app, store: stateStore, ownedEndpoint: ownedEndpoint, startupRecovery: startupRecovery}, nil
}

func (s *Service) Close() error {
	var closeErr error
	if s.ownedEndpoint != nil {
		closeErr = s.ownedEndpoint.Close()
	}
	if s.store != nil {
		if err := s.store.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func (s *Service) SubmitSnapshot(ctx context.Context, snapshot Snapshot) (SnapshotSubmission, *BizError) {
	return s.engine.SubmitSnapshot(ctx, snapshot)
}

func (s *Service) Ingest(ctx context.Context, dataset Dataset, options ProcessOptions) (BatchReport, *BizError) {
	return s.engine.Ingest(ctx, dataset, options)
}

func (s *Service) Reprocess(ctx context.Context, options ProcessOptions) (BatchReport, *BizError) {
	return s.engine.Reprocess(ctx, options)
}

func (s *Service) Read(ctx context.Context, observedVersion int64) (ReadResult, *BizError) {
	return s.engine.Read(ctx, observedVersion)
}

func (s *Service) Recover(ctx context.Context) (RecoveryReport, *BizError) {
	return s.engine.Recover(ctx)
}

func (s *Service) StartupRecovery() RecoveryReport {
	return s.startupRecovery
}
