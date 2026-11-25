package ropechunk

import (
	"net/http"

	"ropechunk/internal/engine"
	"ropechunk/internal/httpapi"
	"ropechunk/internal/model"
	"ropechunk/internal/rope"
	"ropechunk/internal/samples"
	"ropechunk/internal/store"
)

type Config = model.Config
type Shape = model.Shape
type QuantTensor = model.QuantTensor
type Shard = model.Shard
type ShapeDeclaration = model.ShapeDeclaration
type Bundle = model.Bundle
type ScalingMode = model.ScalingMode
type ConfigTransition = model.ConfigTransition
type InvalidationEvent = model.InvalidationEvent
type VersionEvent = model.VersionEvent
type BudgetEvent = model.BudgetEvent
type MemoryPlan = model.MemoryPlan
type MemoryStep = model.MemoryStep
type MemoryBreakdown = model.MemoryBreakdown
type Checkpoint = model.Checkpoint
type AppError = model.AppError
type Fingerprints = model.Fingerprints
type RotatedBlock = model.RotatedBlock
type AngleSummary = model.AngleSummary
type AttentionRow = model.AttentionRow
type ScoreBlock = model.ScoreBlock
type SoftmaxState = model.SoftmaxState
type PublishRequest = model.PublishRequest
type CheckpointRecord = store.CheckpointRecord
type CheckpointKey = store.CheckpointKey
type RotateOptions = rope.RotateOptions

type Engine = engine.Engine
type Store = store.Store
type ComputeOptions = engine.ComputeOptions
type ComputeReport = engine.ComputeReport
type ImportReport = engine.ImportReport
type StatusReport = engine.StatusReport
type VerificationReport = engine.VerificationReport
type ReferenceVerification = engine.ReferenceVerification
type TaskOutcome = engine.TaskOutcome
type RoundReport = engine.RoundReport
type ScorePayload = engine.ScorePayload
type RotationPayload = engine.RotationPayload
type ScoredRowState = engine.ScoredRowState

const (
	StageRotation = model.StageRotation
	StageScore    = model.StageScore

	CodeNumeric         = model.CodeNumeric
	CodeStaleCheckpoint = model.CodeStaleCheckpoint
	CodeMemoryBudget    = model.CodeMemoryBudget
	CodeInvalidConfig   = model.CodeInvalidConfig
	CodeNotFound        = model.CodeNotFound
	CodeState           = model.CodeState
	CodeIO              = model.CodeIO

	ScaleNone    = model.ScaleNone
	ScaleLinear  = model.ScaleLinear
	ScaleNTKPart = model.ScaleNTKPart
)

const DefaultSeed = samples.DefaultSeed

func OpenStore(path string) (*Store, error) { return store.Open(path) }

func CleanStore(path string) error { return store.Clean(path) }

func NewEngine(st *Store) *Engine { return engine.New(st) }

func GenerateSamples() (Bundle, error) { return samples.Generate() }

func DefaultConfig() Config { return samples.DefaultConfig() }

func NextConfigVersion(cfg Config, version int64, mutate func(*Config)) Config {
	return samples.NextVersion(cfg, version, mutate)
}

func RotateBlock(src QuantTensor, cfg Config, batch, head, rowStart, rowEnd int, opts RotateOptions) (RotatedBlock, error) {
	return rope.RotateBlock(src, cfg, batch, head, rowStart, rowEnd, opts)
}

func RotateTensor(src QuantTensor, cfg Config) (QuantTensor, []RotatedBlock, error) {
	return rope.RotateTensor(src, cfg)
}

func ReferenceRotate(src QuantTensor, cfg Config) (QuantTensor, error) {
	return rope.ReferenceRotate(src, cfg)
}

func InvFrequency(pairIndex, headDim int, cfg Config) float64 {
	return rope.InvFrequency(pairIndex, headDim, cfg)
}

func ComputeFingerprints(shard Shard, cfg Config) Fingerprints {
	return model.ComputeFingerprints(shard, cfg)
}

func InputDigest(shard Shard) string { return model.InputDigest(shard) }
func ConfigDigest(cfg Config) string { return model.ConfigDigest(cfg) }

func NewSoftmaxState(headDim int) SoftmaxState { return rope.NewSoftmaxState(headDim) }

func ConsumeKeyRange(q, k []float64, seqLen, headDim, queryIndex int, causal bool, keyStart, keyEnd int, state *SoftmaxState, rowBlock, colBlock int, shardID string) error {
	return rope.ConsumeKeyRange(q, k, seqLen, headDim, queryIndex, causal, keyStart, keyEnd, state, rowBlock, colBlock, shardID)
}

func FinalizeAttentionRow(q, k []float64, seqLen, headDim, queryIndex int, causal bool, state SoftmaxState, shardID string, rowBlock int) (AttentionRow, error) {
	return rope.FinalizeAttentionRow(q, k, seqLen, headDim, queryIndex, causal, state, shardID, rowBlock)
}

func PlanMemory(shard Shard, cfg Config, stage string) (MemoryPlan, error) {
	return engine.PlanMemory(shard, cfg, stage)
}

func EstimateMemory(shape Shape, cfg Config, singleBatch bool) MemoryBreakdown {
	return engine.EstimateBreakdown(shape, cfg, singleBatch)
}

func NewHTTPHandler(eng *Engine) http.Handler {
	return httpapi.New(eng).Handler()
}
