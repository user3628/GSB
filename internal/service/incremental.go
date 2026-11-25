package service

import (
	"context"
	"math"

	"flashattention/internal/attn"
	"flashattention/internal/audit"
	"flashattention/internal/dataset"
	"flashattention/internal/errs"
	"flashattention/internal/reference"
	"flashattention/internal/scheduler"
	"flashattention/internal/store"
	"flashattention/internal/tensor"
	"flashattention/internal/versioning"
)

type UpdateRequest struct {
	ShardID string    `json:"shard_id"`
	Mode    string    `json:"mode"` // correct or append
	Start   int       `json:"start"`
	End     int       `json:"end"`
	Seed    uint64    `json:"seed,omitempty"`
	K       []float32 `json:"k,omitempty"`
	V       []float32 `json:"v,omitempty"`
}

type RowBlockIncremental struct {
	BH                  int    `json:"bh"`
	RowBlock            int    `json:"row_block"`
	Mode                string `json:"mode"`
	RecomputedColBlocks []int  `json:"recomputed_col_blocks"`
	ReusedColBlocks     []int  `json:"reused_col_blocks"`
	FallbackColBlocks   []int  `json:"fallback_col_blocks,omitempty"`
	PublishedSeq        int64  `json:"published_seq,omitempty"`
}

type IncrementalReport struct {
	ShardID              string                `json:"shard_id"`
	Mode                 string                `json:"mode"`
	Start                int                   `json:"start"`
	End                  int                   `json:"end"`
	OldFingerprint       string                `json:"old_fingerprint"`
	NewFingerprint       string                `json:"new_fingerprint"`
	ByteIdentical        bool                  `json:"byte_identical"`
	OutputHash           string                `json:"output_hash"`
	ScratchOutputHash    string                `json:"scratch_output_hash"`
	FirstDifference      any                   `json:"first_difference,omitempty"`
	RowBlocks            []RowBlockIncremental `json:"row_blocks"`
	Invalidations        []versioning.Range    `json:"invalidations"`
	Audit                *audit.Set            `json:"audit,omitempty"`
	PublishedCheckpoints int                   `json:"published_checkpoints"`
}

func (s *Service) Incremental(ctx context.Context, req UpdateRequest, budget int64) (*IncrementalReport, error) {
	if req.ShardID == "" {
		return nil, errs.New(errs.EConfig, "shard_id is required")
	}
	b, err := s.LoadShard(req.ShardID)
	if err != nil {
		return nil, err
	}
	cfg := b.Config
	plan, err := scheduler.PlanWorkload(scheduler.Options{
		ShardID: req.ShardID, Shape: b.Shape, RowBlock: cfg.RowBlock, ColBlock: cfg.ColBlock,
		GroupBlocks: cfg.GroupBlocks, BudgetBytes: budget,
	})
	if err != nil {
		return nil, err
	}
	cfg.RowBlock, cfg.ColBlock, cfg.GroupBlocks = plan.RowBlock, plan.ColBlock, plan.GroupBlocks
	cfg.Normalize(b.Shape)

	oldFP, err := s.Store.GetFingerprint(ctx, req.ShardID)
	if err != nil {
		oldFP = Fingerprint(b, cfg, plan)
	}

	start, end, err := applyPatch(b, req)
	if err != nil {
		return nil, err
	}

	cfg.KeyLen = b.Config.KeyLen
	b.Reference = referenceCompute(b, cfg.KeyLen)
	newFP := Fingerprint(b, cfg, plan)
	invRanges := versioning.Diff(req.ShardID, oldFP, newFP, b.Shape.S, versioning.Change{
		Trigger: versioning.TriggerPrefix, KeyStart: start, KeyEnd: end,
	})
	for _, r := range invRanges {
		if err := s.Store.ApplyInvalidation(ctx, r); err != nil {
			return nil, err
		}
	}
	if err := s.Store.SaveVersion(ctx, newFP); err != nil {
		return nil, err
	}
	if err := s.SaveShard(b); err != nil {
		return nil, err
	}

	recovery, err := s.Store.LoadRecovery(ctx, req.ShardID)
	if err != nil {
		return nil, err
	}
	stored, _, err := s.Store.LoadContributions(ctx, req.ShardID)
	if err != nil {
		return nil, err
	}
	kn, err := attn.NewKernel(cfg, b.Q, b.K, b.V)
	if err != nil {
		return nil, err
	}

	incOut := tensor.New(b.Shape)
	incFinals := map[string]attn.RowFinal{}
	rep := &IncrementalReport{
		ShardID: req.ShardID, Mode: req.Mode, Start: start, End: end,
		OldFingerprint: oldFP.Hash, NewFingerprint: newFP.Hash, Invalidations: invRanges,
	}
	var pending []store.Checkpoint

	for bh := 0; bh < b.Shape.B*b.Shape.H; bh++ {
		batch := bh / b.Shape.H
		head := bh % b.Shape.H
		for rb := 0; rb*cfg.RowBlock < b.Shape.S; rb++ {
			r0 := rb * cfg.RowBlock
			r1 := min(r0+cfg.RowBlock, b.Shape.S)
			rows := r1 - r0
			key := attn.ResumeKey(bh, rb)
			rbRep := RowBlockIncremental{BH: bh, RowBlock: rb}
			inv := findRange(invRanges, rb)
			rbAffected := inv != nil

			if !rbAffected {
				rs, ok := recovery.States[key]
				if ok && len(rs.Snapshots) == rows {
					rbRep.Mode = "reused_row_block_checkpoint"
					for i, sn := range rs.Snapshots {
						st := &attn.RowState{Valid: sn.Valid, M: sn.M, Sum: sn.Sum, LSE: sn.LSE, Acc: append([]float64(nil), sn.Acc...)}
						final, lerr := attn.FinalizeOne(st, cfg)
						if lerr != nil {
							return rep, lerr
						}
						row := r0 + i
						copy(incOut.Row(batch, head, row), final.Output)
						final.BH, final.Batch, final.Head, final.Row = bh, batch, head, row
						incFinals[finalKey(bh, row)] = final
					}
					rep.RowBlocks = append(rep.RowBlocks, rbRep)
					continue
				}
				rbRep.Mode = "missing_checkpoint_fallback_recompute"
			} else {
				rbRep.Mode = "recompute_affected_col_blocks"
			}

			states := make([]*attn.RowState, rows)
			for i := range states {
				states[i] = attn.NewRowState(b.Shape.D)
			}
			byCB := map[int]attn.BlockContribution{}
			for _, c := range stored[key] {
				byCB[c.ColBlock] = c
			}
			nCol := (cfg.KeyLen + cfg.ColBlock - 1) / cfg.ColBlock
			var recomputedContribs []attn.BlockContribution
			for cb := 0; cb < nCol; cb++ {
				c0 := cb * cfg.ColBlock
				c1 := min(c0+cfg.ColBlock, cfg.KeyLen)
				shouldRecompute := inv != nil && cb >= inv.FirstColBlock && cb <= inv.LastColBlock
				var contrib attn.BlockContribution
				if shouldRecompute {
					contrib, _, err = kn.ComputeContribution(bh, rb, cb, r0, r1, c0, c1)
					if err != nil {
						return rep, err
					}
					rbRep.RecomputedColBlocks = append(rbRep.RecomputedColBlocks, cb)
					recomputedContribs = append(recomputedContribs, contrib)
				} else if old, ok := byCB[cb]; ok {
					contrib = old
					rbRep.ReusedColBlocks = append(rbRep.ReusedColBlocks, cb)
				} else {
					contrib, _, err = kn.ComputeContribution(bh, rb, cb, r0, r1, c0, c1)
					if err != nil {
						return rep, err
					}
					rbRep.FallbackColBlocks = append(rbRep.FallbackColBlocks, cb)
					recomputedContribs = append(recomputedContribs, contrib)
				}
				for i := 0; i < rows; i++ {
					if err := attn.MergeOne(states[i], contrib.M[i], contrib.Sum[i], contrib.PV[i*b.Shape.D:(i+1)*b.Shape.D], cfg); err != nil {
						return rep, err.With("bh", bh).With("row_block", rb)
					}
				}
			}

			snaps := make([]attn.Snapshot, rows)
			for i, st := range states {
				final, lerr := attn.FinalizeOne(st, cfg)
				if lerr != nil {
					return rep, lerr
				}
				row := r0 + i
				copy(incOut.Row(batch, head, row), final.Output)
				final.BH, final.Batch, final.Head, final.Row = bh, batch, head, row
				incFinals[finalKey(bh, row)] = final
				snaps[i] = st.ExportSnapshot(i)
			}
			headSeq := recovery.HeadSeq[key]
			pending = append(pending, store.Checkpoint{
				ShardID: req.ShardID, BH: bh, RowBlock: rb, ColStart: 0, ColEnd: cfg.KeyLen,
				Seq: headSeq + 1, Strategy: string(cfg.Strategy), Snapshots: snaps,
				InputFP: b.InputFP, VersionFP: newFP.Hash,
				PrevHash: recovery.States[key].StateHash, Contributions: recomputedContribs,
			})
			rbRep.PublishedSeq = headSeq + 1
			rep.RowBlocks = append(rep.RowBlocks, rbRep)
		}
	}

	scratch, err := kn.Run(ctx, nil, nil)
	if err != nil {
		return rep, err
	}
	rep.OutputHash = attn.HashFloat32(incOut.Data)
	rep.ScratchOutputHash = attn.HashFloat32(scratch.Output.Data)
	if mm := tensor.FirstBitMismatch(incOut, scratch.Output); mm != nil {
		rep.FirstDifference = map[string]any{"kind": "output", "mismatch": mm}
		return rep, incrementalMismatch(req.ShardID, rep.FirstDifference)
	}
	for _, sf := range scratch.Finals {
		inf, ok := incFinals[finalKey(sf.BH, sf.Row)]
		if !ok {
			rep.FirstDifference = map[string]any{"kind": "missing_row", "bh": sf.BH, "row": sf.Row}
			return rep, incrementalMismatch(req.ShardID, rep.FirstDifference)
		}
		if diff := finalMismatch(sf, inf); diff != nil {
			rep.FirstDifference = diff
			return rep, incrementalMismatch(req.ShardID, diff)
		}
	}
	rep.ByteIdentical = true

	for _, cp := range pending {
		rec, err := s.Store.Publish(ctx, cp)
		if err != nil {
			return rep, err
		}
		rep.PublishedCheckpoints++
		for i := range rep.RowBlocks {
			if rep.RowBlocks[i].BH == cp.BH && rep.RowBlocks[i].RowBlock == cp.RowBlock {
				rep.RowBlocks[i].PublishedSeq = rec.Seq
			}
		}
	}

	auditSet, auditErr := audit.Audit(req.ShardID, cfg, b.Shape, incOut, b.Reference, audit.Envelope{
		QMax: audit.TensorMaxAbs(b.Q), KMax: audit.TensorMaxAbs(b.K), VMax: audit.TensorMaxAbs(b.V),
	})
	rep.Audit = auditSet
	if err := tensor.SaveJSON(s.incrementalReportPath(req.ShardID), rep); err != nil {
		return rep, err
	}
	if auditErr != nil {
		return rep, auditErr
	}
	return rep, nil
}

func (s *Service) incrementalReportPath(id string) string {
	return s.ReportDir + "/" + id + "-incremental.json"
}

func applyPatch(b *dataset.Bundle, req UpdateRequest) (int, int, error) {
	curLen := b.Config.KeyLen
	start, end := req.Start, req.End
	switch req.Mode {
	case "append":
		if start == 0 && curLen > 0 {
			start = curLen
		}
		if start != curLen {
			return 0, 0, errs.New(errs.EConfig, "append start %d must equal current key_len %d", start, curLen)
		}
		if end <= start || end > b.Shape.S {
			return 0, 0, errs.New(errs.EConfig, "append end %d must be in (%d,%d]", end, start, b.Shape.S)
		}
		b.Config.KeyLen = end
		b.Spec.KeyLen = end
	case "correct", "prefix":
		if start < 0 || end <= start || end > curLen {
			return 0, 0, errs.New(errs.EConfig, "correct interval must be within [0,%d), got [%d,%d)", curLen, start, end)
		}
	default:
		return 0, 0, errs.New(errs.EConfig, "unknown update mode %q", req.Mode)
	}

	n := end - start
	patchShape := tensor.Shape{B: b.Shape.B, H: b.Shape.H, S: n, D: b.Shape.D}
	kPatch := req.K
	vPatch := req.V
	if kPatch == nil {
		seed := req.Seed
		if seed == 0 {
			seed = 9001 + uint64(start)*131 + uint64(end)*17
		}
		kPatch = tensor.Deterministic(patchShape, seed, -1, 1).Data
	}
	if vPatch == nil {
		seed := req.Seed
		if seed == 0 {
			seed = 9101 + uint64(start)*137 + uint64(end)*19
		} else {
			seed += 101
		}
		vPatch = tensor.Deterministic(patchShape, seed, -1, 1).Data
	}
	need := patchShape.Numel()
	if len(kPatch) != need || len(vPatch) != need {
		return 0, 0, errs.New(errs.EConfig, "patch length must be B*H*interval*D=%d", need)
	}
	for bi := 0; bi < b.Shape.B; bi++ {
		for hi := 0; hi < b.Shape.H; hi++ {
			off := ((bi*b.Shape.H + hi) * n) * b.Shape.D
			dstK := b.K.Data[b.K.Offset(bi, hi, start, 0) : b.K.Offset(bi, hi, end-1, b.Shape.D-1)+1]
			copy(dstK, kPatch[off:off+n*b.Shape.D])
			dstV := b.V.Data[b.V.Offset(bi, hi, start, 0) : b.V.Offset(bi, hi, end-1, b.Shape.D-1)+1]
			copy(dstV, vPatch[off:off+n*b.Shape.D])
		}
	}
	return start, end, nil
}

func referenceCompute(b *dataset.Bundle, keyLen int) *reference.Output {
	return reference.Compute(b.Q, b.K, b.V, b.Spec.Causal, b.Spec.Scale(), b.Spec.Bits, keyLen)
}

func finalKey(bh, row int) string { return attn.ResumeKey(bh, row) }

func findRange(ranges []versioning.Range, rb int) *versioning.Range {
	for i := range ranges {
		r := &ranges[i]
		if r.Trigger == versioning.TriggerPrefix && rb >= r.FirstRowBlock && rb <= r.LastRowBlock {
			return r
		}
	}
	return nil
}

func finalMismatch(a, b attn.RowFinal) map[string]any {
	if math.Float64bits(a.M) != math.Float64bits(b.M) {
		return map[string]any{"kind": "row_max", "bh": a.BH, "row": a.Row, "a_bits": math.Float64bits(a.M), "b_bits": math.Float64bits(b.M), "a": a.M, "b": b.M}
	}
	if math.Float64bits(a.Sum) != math.Float64bits(b.Sum) {
		return map[string]any{"kind": "normalizer", "bh": a.BH, "row": a.Row, "a_bits": math.Float64bits(a.Sum), "b_bits": math.Float64bits(b.Sum), "a": a.Sum, "b": b.Sum}
	}
	if math.Float64bits(a.LSE) != math.Float64bits(b.LSE) {
		return map[string]any{"kind": "lse", "bh": a.BH, "row": a.Row, "a_bits": math.Float64bits(a.LSE), "b_bits": math.Float64bits(b.LSE)}
	}
	if mm := tensor.FirstBitMismatch(&tensor.Tensor{Shape: tensor.Shape{D: len(a.Output)}, Data: a.Output}, &tensor.Tensor{Shape: tensor.Shape{D: len(b.Output)}, Data: b.Output}); mm != nil {
		return map[string]any{"kind": "output", "bh": a.BH, "row": a.Row, "dim": mm.D, "a_bits": mm.A, "b_bits": mm.BitsB}
	}
	return nil
}

func incrementalMismatch(shard string, first any) *errs.Error {
	return errs.New(errs.EIncrementalMismatch, "incremental result is not bit-identical to scratch computation").
		With("shard", shard).With("first_difference", first)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
