// Package flashattn 对外暴露确定性分块注意力引擎、服务编排与稳定错误码。
package flashattn

import (
	"context"

	"flashattention/internal/attn"
	"flashattention/internal/dataset"
	"flashattention/internal/errs"
	"flashattention/internal/service"
	"flashattention/internal/store"
	"flashattention/internal/tensor"
	"flashattention/internal/versioning"
)

type (
	Engine            = service.Service
	Store             = store.Store
	CheckpointRecord  = store.Record
	RunOptions        = service.RunOptions
	RunReport         = service.RunReport
	UpdateRequest     = service.UpdateRequest
	IncrementalReport = service.IncrementalReport
	DemoReport        = service.DemoReport
	Spec              = dataset.Spec
	Bundle            = dataset.Bundle
	Shape             = tensor.Shape
	Tensor            = tensor.Tensor
	Config            = attn.Config
	Result            = attn.Result
	RowBlockSummary   = attn.RowBlockSummary
	Fingerprint       = versioning.Fingerprint
	InvalidationRange = versioning.Range
	CodedError        = errs.Error
)

const (
	ScaledStrategy = attn.Scaled
	LogSumStrategy = attn.LogSum

	ENumeric             = errs.ENumeric
	EErrorBound          = errs.EErrorBound
	EIncrementalMismatch = errs.EIncrementalMismatch
	EStaleCheckpoint     = errs.EStaleCheckpoint
	EMemoryBudget        = errs.EMemoryBudget
	EConfig              = errs.EConfig
	EState               = errs.EState
)

var ErrGracefulStop = service.ErrGracefulStop

func NewEngine(dataDir string) (*Engine, error) {
	return service.New(dataDir)
}

func NewEngineWithReports(dataDir, reportDir string) (*Engine, error) {
	return service.NewWithReports(dataDir, reportDir)
}

func OpenStore(path string) (*Store, error) { return store.Open(path) }

func Catalog() []Spec { return dataset.Catalog() }

func Generate(spec Spec) *Bundle { return dataset.Generate(spec) }

func Attention(ctx context.Context, q, k, v *Tensor, cfg Config) (*Result, error) {
	kn, err := attn.NewKernel(cfg, q, k, v)
	if err != nil {
		return nil, err
	}
	return kn.Run(ctx, nil, nil)
}

func ErrorCode(err error) string { return errs.Code(err) }

func AsCodedError(err error) *CodedError { return errs.As(err) }
