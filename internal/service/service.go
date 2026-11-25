// Package service 组合样本、内核、审计、版本与 SQLite 存储，提供 CLI/库接口。
package service

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"flashattention/internal/attn"
	"flashattention/internal/audit"
	"flashattention/internal/dataset"
	"flashattention/internal/errs"
	"flashattention/internal/scheduler"
	"flashattention/internal/store"
	"flashattention/internal/tensor"
	"flashattention/internal/versioning"
)

var ErrGracefulStop = errors.New("graceful stop after durable checkpoint")

type Service struct {
	DataDir   string
	ReportDir string
	Store     *store.Store
}

func New(dataDir string) (*Service, error) {
	return NewWithReports(dataDir, "reports")
}

func NewWithReports(dataDir, reportDir string) (*Service, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(reportDir, 0o755); err != nil {
		return nil, err
	}
	st, err := store.Open(filepath.Join(dataDir, "flashattn.db"))
	if err != nil {
		return nil, err
	}
	return &Service{DataDir: dataDir, ReportDir: reportDir, Store: st}, nil
}

func (s *Service) Close() error { return s.Store.Close() }

func (s *Service) ShardPath(id string) string { return dataset.Path(s.DataDir, id) }

func (s *Service) LoadShard(id string) (*dataset.Bundle, error) {
	b, err := dataset.Load(s.ShardPath(id))
	if err != nil {
		return nil, errs.New(errs.EIO, "load shard %s: %v", id, err).With("shard", id)
	}
	return b, nil
}

func (s *Service) SaveShard(b *dataset.Bundle) error {
	dir := dataset.Dir(s.DataDir, b.Spec.ID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b.InputFP = b.ComputeInputFP()
	return b.Save(dir)
}

func (s *Service) Generate(spec dataset.Spec, force bool) (*dataset.Bundle, error) {
	path := dataset.Path(s.DataDir, spec.ID)
	if !force {
		if b, err := dataset.Load(path); err == nil {
			return b, nil
		}
	}
	b := dataset.Generate(spec)
	if err := s.SaveShard(b); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *Service) GenerateAll(ctx context.Context, force bool) ([]*dataset.Bundle, error) {
	var bundles []*dataset.Bundle
	for _, spec := range dataset.Catalog() {
		if err := ctx.Err(); err != nil {
			return bundles, err
		}
		b, err := s.Generate(spec, force)
		if err != nil {
			return bundles, err
		}
		bundles = append(bundles, b)
	}
	if err := tensor.SaveJSON(filepath.Join(s.DataDir, "catalog.json"), dataset.Catalog()); err != nil {
		return nil, err
	}
	return bundles, nil
}

func (s *Service) ImportBundle(path, idOverride string) (*dataset.Bundle, error) {
	b, err := dataset.Load(path)
	if err != nil {
		return nil, errs.New(errs.EIO, "import %s: %v", path, err)
	}
	if idOverride != "" {
		b.Spec.ID = idOverride
		b.Config.ShardID = idOverride
	}
	if b.Spec.ID == "" {
		return nil, errs.New(errs.EConfig, "imported bundle has empty shard id")
	}
	if err := s.SaveShard(b); err != nil {
		return nil, err
	}
	return b, nil
}

func ReferenceFingerprint(b *dataset.Bundle) string {
	h := sha256.New()
	var buf [8]byte
	for _, c := range b.Reference.Codes {
		binary.LittleEndian.PutUint64(buf[:], uint64(c))
		h.Write(buf[:])
	}
	fmt.Fprintf(h, "|%d|%.17g", b.Reference.Bits, b.Reference.Step)
	return hex.EncodeToString(h.Sum(nil))[:32]
}

func Fingerprint(b *dataset.Bundle, cfg attn.Config, plan *scheduler.Plan) versioning.Fingerprint {
	planFP := ""
	if plan != nil {
		planFP = fmt.Sprintf("rb=%d,cb=%d,g=%d,budget=%d", plan.RowBlock, plan.ColBlock, plan.GroupBlocks, plan.BudgetBytes)
	}
	return versioning.Compute(versioning.Stages{
		QFP: b.Q.Fingerprint(), KFP: b.K.Fingerprint(), VFP: b.V.Fingerprint(),
		InputFP: b.InputFP, Config: cfg, QuantBits: b.Spec.Bits,
		RefFP: ReferenceFingerprint(b), MemoryPlan: planFP,
	})
}

type RunOptions struct {
	MemoryBudgetBytes   int64 `json:"memory_budget_bytes"`
	Resume              bool  `json:"resume"`
	StopAfterCheckpoint int   `json:"stop_after_checkpoints"`
	StrictNumeric       bool  `json:"strict_numeric"`
}

type RunReport struct {
	ShardID            string                 `json:"shard_id"`
	OK                 bool                   `json:"ok"`
	StoppedGracefully  bool                   `json:"stopped_gracefully,omitempty"`
	Config             attn.Config            `json:"config"`
	Fingerprint        string                 `json:"fingerprint"`
	Plan               *scheduler.Plan        `json:"plan"`
	OutputHash         string                 `json:"output_hash"`
	OutputPath         string                 `json:"output_path"`
	Audit              *audit.Set             `json:"audit,omitempty"`
	RecoveryIgnored    []store.IgnoredRecord  `json:"recovery_ignored,omitempty"`
	Summaries          []attn.RowBlockSummary `json:"summaries,omitempty"`
	NumericEventCount  int                    `json:"numeric_event_count"`
	NumericCodes       []string               `json:"numeric_codes,omitempty"`
	PersistedEvents    []attn.NumericEvent    `json:"persisted_numeric_events,omitempty"`
	CheckpointsWritten int                    `json:"checkpoints_written"`
}

func (s *Service) Run(ctx context.Context, shardID string, opts RunOptions) (*RunReport, error) {
	b, err := s.LoadShard(shardID)
	if err != nil {
		return nil, err
	}
	cfg := b.Config
	if opts.StrictNumeric {
		cfg.StrictNumeric = true
	}
	plan, err := scheduler.PlanWorkload(scheduler.Options{
		ShardID: shardID, Shape: b.Shape, RowBlock: cfg.RowBlock, ColBlock: cfg.ColBlock,
		GroupBlocks: cfg.GroupBlocks, BudgetBytes: opts.MemoryBudgetBytes,
	})
	if err != nil {
		return &RunReport{ShardID: shardID, OK: false, Config: cfg, Plan: plan}, err
	}
	cfg.RowBlock, cfg.ColBlock, cfg.GroupBlocks = plan.RowBlock, plan.ColBlock, plan.GroupBlocks
	cfg.Normalize(b.Shape)

	fp := Fingerprint(b, cfg, plan)
	if err := s.reconcileVersion(ctx, shardID, fp); err != nil {
		return nil, err
	}
	if err := s.Store.SaveVersion(ctx, fp); err != nil {
		return nil, err
	}

	resumeMap := map[string]attn.ResumeState{}
	var ignored []store.IgnoredRecord
	if opts.Resume {
		rr, err := s.Store.LoadRecovery(ctx, shardID)
		if err != nil {
			return nil, err
		}
		resumeMap, ignored = rr.States, rr.Ignored
	}

	kn, err := attn.NewKernel(cfg, b.Q, b.K, b.V)
	if err != nil {
		return nil, err
	}
	prevHash := map[string]string{}
	for k, rs := range resumeMap {
		prevHash[k] = rs.StateHash
	}
	written := 0
	cb := func(info attn.GroupInfo) error {
		key := attn.ResumeKey(info.BH, info.RowBlock)
		cp := store.Checkpoint{
			ShardID: shardID, BH: info.BH, RowBlock: info.RowBlock,
			ColStart: info.ColStart, ColEnd: info.ColEnd, Seq: info.Seq,
			Strategy: string(cfg.Strategy), Snapshots: info.Snapshots,
			InputFP: b.InputFP, VersionFP: fp.Hash, PrevHash: prevHash[key],
			Contributions: info.Contributions,
			Events:        info.Events,
		}
		rec, err := s.Store.Publish(ctx, cp)
		if err != nil {
			return err
		}
		prevHash[key] = rec.StateHash
		written++
		if opts.StopAfterCheckpoint > 0 && written >= opts.StopAfterCheckpoint {
			return ErrGracefulStop
		}
		return nil
	}

	result, runErr := kn.Run(ctx, cb, resumeMap)
	report := &RunReport{
		ShardID: shardID, Config: cfg, Fingerprint: fp.Hash, Plan: plan,
		RecoveryIgnored: ignored, CheckpointsWritten: written,
	}
	if result != nil {
		report.Summaries = result.Summaries
		report.NumericEventCount = len(result.Events)
		report.NumericCodes = codes(result.Events)
		if result.Output != nil {
			report.OutputHash = attn.HashFloat32(result.Output.Data)
		}
	}
	allNumericEvents := []attn.NumericEvent{}
	if result != nil {
		allNumericEvents = append(allNumericEvents, result.Events...)
	}
	if written == 0 {
		if persisted, err := s.Store.LoadNumericEvents(ctx, shardID); err == nil {
			report.PersistedEvents = persisted
			allNumericEvents = append(allNumericEvents, persisted...)
			report.NumericEventCount = len(allNumericEvents)
			report.NumericCodes = codes(allNumericEvents)
		}
	}
	if errors.Is(runErr, ErrGracefulStop) {
		report.OK = true
		report.StoppedGracefully = true
		_ = s.saveRunReport(report)
		return report, ErrGracefulStop
	}
	if runErr != nil {
		return report, runErr
	}

	outPath := filepath.Join(dataset.Dir(s.DataDir, shardID), "output.json")
	if err := tensor.SaveJSON(outPath, result.Output); err != nil {
		return report, err
	}
	report.OutputPath = outPath

	auditSet, auditErr := audit.Audit(shardID, cfg, b.Shape, result.Output, b.Reference, audit.Envelope{
		QMax: audit.TensorMaxAbs(b.Q), KMax: audit.TensorMaxAbs(b.K), VMax: audit.TensorMaxAbs(b.V),
	})
	report.Audit = auditSet
	report.OK = auditErr == nil
	if err := s.saveRunReport(report); err != nil {
		return report, err
	}
	if auditErr != nil {
		return report, auditErr
	}
	if opts.StrictNumeric && report.NumericEventCount > 0 {
		return report, errs.New(errs.ENumeric, "strict numeric mode: %d E_NUMERIC diagnostic events", report.NumericEventCount).
			With("shard", shardID).With("positions", firstEvents(allNumericEvents, 8))
	}
	return report, nil
}

func (s *Service) reconcileVersion(ctx context.Context, shardID string, next versioning.Fingerprint) error {
	old, err := s.Store.GetFingerprint(ctx, shardID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if old.Hash == next.Hash {
		return nil
	}
	ranges := versioning.Diff(shardID, old, next, next.Stages.Config.KeyLen, versioning.Change{})
	for _, r := range ranges {
		if err := s.Store.ApplyInvalidation(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) saveRunReport(r *RunReport) error {
	return tensor.SaveJSON(filepath.Join(s.ReportDir, r.ShardID+"-run.json"), r)
}

func codes(events []attn.NumericEvent) []string {
	seen := map[string]bool{}
	var out []string
	for _, e := range events {
		if e.Code != "" && !seen[e.Code] {
			seen[e.Code] = true
			out = append(out, e.Code)
		}
	}
	return out
}

func firstEvents(events []attn.NumericEvent, n int) []attn.NumericEvent {
	if len(events) <= n {
		return events
	}
	return events[:n]
}
