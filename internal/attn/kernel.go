package attn

import (
	"context"
	"fmt"
	"math"

	"flashattention/internal/errs"
	"flashattention/internal/tensor"
)

type ResumeState struct {
	BH        int        `json:"bh"`
	RowBlock  int        `json:"row_block"`
	ResumeCol int        `json:"resume_col"`
	Seq       int64      `json:"seq"`
	StateHash string     `json:"state_hash,omitempty"`
	Snapshots []Snapshot `json:"snapshots"`
}

func ResumeKey(bh, rowBlock int) string { return fmt.Sprintf("%d/%d", bh, rowBlock) }

type Kernel struct {
	cfg Config
	q   *tensor.Tensor
	k   *tensor.Tensor
	v   *tensor.Tensor
}

func NewKernel(cfg Config, q, k, v *tensor.Tensor) (*Kernel, error) {
	sh := q.Shape
	cfg.Normalize(sh)
	if err := cfg.Validate(sh); err != nil {
		return nil, err
	}
	if q.Shape != k.Shape || k.Shape != v.Shape {
		return nil, errs.New(errs.EConfig, "Q/K/V shape mismatch: %+v vs %+v vs %+v", q.Shape, k.Shape, v.Shape)
	}
	if err := q.ValidateData(); err != nil {
		return nil, errs.New(errs.ENumeric, "Q: %v", err)
	}
	if err := k.ValidateData(); err != nil {
		return nil, errs.New(errs.ENumeric, "K: %v", err)
	}
	if err := v.ValidateData(); err != nil {
		return nil, errs.New(errs.ENumeric, "V: %v", err)
	}
	return &Kernel{cfg: cfg, q: q, k: k, v: v}, nil
}

func (kn *Kernel) Config() Config { return kn.cfg }

func (kn *Kernel) Run(ctx context.Context, cb CheckpointCallback, resume map[string]ResumeState) (*Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sh := kn.q.Shape
	out := tensor.New(sh)
	result := &Result{Output: out}

	for bh := 0; bh < sh.B*sh.H; bh++ {
		b := bh / sh.H
		h := bh % sh.H
		for rb := 0; rb*kn.cfg.RowBlock < sh.S; rb++ {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			r0 := rb * kn.cfg.RowBlock
			r1 := minInt(r0+kn.cfg.RowBlock, sh.S)
			summary, err := kn.runRowBlock(ctx, b, h, bh, rb, r0, r1, out, cb, resume, result)
			if err != nil {
				return result, err
			}
			result.Summaries = append(result.Summaries, summary)
		}
	}
	return result, nil
}

func (kn *Kernel) runRowBlock(
	ctx context.Context,
	b, h, bh, rb, r0, r1 int,
	out *tensor.Tensor,
	cb CheckpointCallback,
	resume map[string]ResumeState,
	result *Result,
) (RowBlockSummary, error) {
	sh := kn.q.Shape
	rows := r1 - r0
	states := make([]*RowState, rows)
	for i := range states {
		states[i] = NewRowState(sh.D)
	}

	startCol := 0
	var seq int64
	if rs, ok := resume[ResumeKey(bh, rb)]; ok {
		if rs.BH != bh || rs.RowBlock != rb {
			return RowBlockSummary{}, errs.New(errs.EState, "resume key mismatch: got bh=%d rb=%d", rs.BH, rs.RowBlock)
		}
		startCol = rs.ResumeCol
		seq = rs.Seq
		if startCol == 0 && len(rs.Snapshots) == 0 {
			// 全部快照已失效，但调用方要求单调序号从 seq+1 继续。
		} else if len(rs.Snapshots) != rows {
			return RowBlockSummary{}, errs.New(errs.EState, "resume snapshot rows %d != %d at bh=%d rb=%d", len(rs.Snapshots), rows, bh, rb)
		} else {
			for i, sn := range rs.Snapshots {
				if err := states[i].restore(sn); err != nil {
					return RowBlockSummary{}, errs.New(errs.EState, "restore bh=%d rb=%d row=%d: %v", bh, rb, r0+i, err)
				}
			}
		}
	}

	blockEvents := make([]NumericEvent, 0)
	pending := make([]BlockContribution, 0, kn.cfg.GroupBlocks)
	groupsSince := 0
	lastSeq := seq

	for c0 := startCol; c0 < kn.cfg.KeyLen; c0 += kn.cfg.ColBlock {
		if err := ctx.Err(); err != nil {
			return RowBlockSummary{}, err
		}
		c1 := minInt(c0+kn.cfg.ColBlock, kn.cfg.KeyLen)
		cbIndex := c0 / kn.cfg.ColBlock
		contrib, events, err := kn.ComputeContribution(bh, rb, cbIndex, r0, r1, c0, c1)
		if err != nil {
			return RowBlockSummary{}, err
		}
		blockEvents = append(blockEvents, events...)
		result.Events = append(result.Events, events...)
		for i := 0; i < rows; i++ {
			if err := MergeOne(states[i], contrib.M[i], contrib.Sum[i], contrib.PV[i*sh.D:(i+1)*sh.D], kn.cfg); err != nil {
				return RowBlockSummary{}, err.With("bh", bh).With("row", r0+i).With("row_block", rb)
			}
		}
		pending = append(pending, contrib)
		groupsSince++

		if groupsSince >= kn.cfg.GroupBlocks || c1 == kn.cfg.KeyLen {
			seq++
			lastSeq = seq
			snaps := make([]Snapshot, rows)
			for i, st := range states {
				snaps[i] = st.snapshot(i)
			}
			info := GroupInfo{
				BH: bh, RowBlock: rb, ColStart: 0, ColEnd: c1, Seq: seq,
				Snapshots: snaps, Contributions: pending, Events: append([]NumericEvent(nil), blockEvents...),
			}
			if cb != nil {
				if err := cb(info); err != nil {
					return RowBlockSummary{}, err
				}
			}
			pending = make([]BlockContribution, 0, kn.cfg.GroupBlocks)
			groupsSince = 0
		}
	}

	rowMax := make([]float64, rows)
	rowSum := make([]float64, rows)
	rowLSE := make([]float64, rows)
	finalOutputs := make([][]float32, rows)
	stateHashes := make([]string, rows)
	clamps := 0
	for _, ev := range blockEvents {
		if ev.Kind == "exp_underflow_clamp" || ev.Kind == "width_would_clamp" {
			clamps += ev.Count
		}
	}

	for i, st := range states {
		row := r0 + i
		widthEvents := checkWidth(st, kn.cfg, bh, b, h, rb, i, row)
		blockEvents = append(blockEvents, widthEvents...)
		result.Events = append(result.Events, widthEvents...)
		final, err := FinalizeOne(st, kn.cfg)
		if err != nil {
			return RowBlockSummary{}, err.With("bh", bh).With("row_block", rb).With("row", row)
		}
		copy(out.Row(b, h, row), final.Output)
		rowMax[i], rowSum[i], rowLSE[i] = final.M, final.Sum, final.LSE
		final.BH, final.Batch, final.Head, final.Row = bh, b, h, row
		final.StateHash = HashState(final.M, final.Sum, final.LSE, st.Acc)
		final.OutputHash = HashFloat32(final.Output)
		stateHashes[i] = final.StateHash
		result.Finals = append(result.Finals, final)
		finalOutputs[i] = final.Output
	}

	outputHash := hashOutputSlices(finalOutputs)
	stateHash := tensor.HashFloat64s(append(append(append([]float64{}, rowMax...), rowSum...), rowLSE...))
	summary := RowBlockSummary{
		BH: bh, Batch: b, Head: h, RowBlock: rb, RowStart: r0, RowEnd: r1,
		ColEnd: kn.cfg.KeyLen, RowMax: rowMax, RowSum: rowSum, RowLSE: rowLSE,
		OutputHash: outputHash, StateHash: stateHash, ClampCount: clamps,
		EventCount: len(blockEvents), Events: compactEvents(blockEvents), CheckpointSeq: lastSeq,
	}
	return summary, nil
}

func (kn *Kernel) ComputeContribution(bh, rb, cb, r0, r1, c0, c1 int) (BlockContribution, []NumericEvent, error) {
	sh := kn.q.Shape
	b := bh / sh.H
	h := bh % sh.H
	rows := r1 - r0
	cols := c1 - c0
	logits := make([]float64, rows*cols)
	events := make([]NumericEvent, 0)

	for i := 0; i < rows; i++ {
		qi := r0 + i
		qrow := kn.q.Row(b, h, qi)
		for j := 0; j < cols; j++ {
			kj := c0 + j
			idx := i*cols + j
			if kn.cfg.Causal && kj > qi {
				logits[idx] = math.Inf(-1)
				continue
			}
			var dot float64
			krow := kn.k.Row(b, h, kj)
			for d := 0; d < sh.D; d++ {
				dot += float64(qrow[d]) * float64(krow[d])
			}
			v := dot * kn.cfg.Scale
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return BlockContribution{}, nil, errs.New(errs.ENumeric, "non-finite logit").
					With("shard", kn.cfg.ShardID).With("bh", bh).With("row", qi).With("col", kj).With("row_block", rb)
			}
			if math.Abs(v) > kn.cfg.ExtremeLogit {
				events = append(events, NumericEvent{
					Code: errs.ENumeric, Severity: "diagnostic", Kind: "extreme_value",
					BH: bh, Batch: b, Head: h, RowBlock: rb, LocalRow: i, Row: qi,
					ColBlock: cb, ColStart: c0, Col: kj, Count: 1, Value: v,
					Message: "absolute scaled logit exceeds extreme threshold; online max subtraction remains stable",
				})
			}
			logits[idx] = v
		}
	}

	m := make([]float64, rows)
	sum := make([]float64, rows)
	pv := make([]float64, rows*sh.D)
	for i := range m {
		m[i] = math.Inf(-1)
	}
	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			v := logits[i*cols+j]
			if !math.IsInf(v, -1) && v > m[i] {
				m[i] = v
			}
		}
		if math.IsInf(m[i], -1) {
			continue
		}
		for j := 0; j < cols; j++ {
			kj := c0 + j
			v := logits[i*cols+j]
			if math.IsInf(v, -1) {
				continue
			}
			x := v - m[i]
			e, ev := stableExp(x, NumericEvent{
				BH: bh, Batch: b, Head: h, RowBlock: rb, LocalRow: i, Row: r0 + i,
				ColBlock: cb, ColStart: c0, Col: kj,
			}, kn.cfg)
			if ev != nil {
				events = append(events, *ev)
			}
			sum[i] += e
			vrow := kn.v.Row(b, h, kj)
			base := i * sh.D
			for d := 0; d < sh.D; d++ {
				pv[base+d] += e * float64(vrow[d])
			}
		}
		if !isFinite(sum[i]) {
			return BlockContribution{}, nil, errs.New(errs.ENumeric, "non-finite block exponent sum").
				With("shard", kn.cfg.ShardID).With("bh", bh).With("row_block", rb).With("row", r0+i).With("col_start", c0)
		}
	}

	c := BlockContribution{
		BH: bh, Batch: b, Head: h, RowBlock: rb, ColBlock: cb,
		RowStart: r0, RowEnd: r1, ColStart: c0, ColEnd: c1, HeadDim: sh.D,
		M: m, Sum: sum, PV: pv,
	}
	c.Hash = c.ComputeHash()
	return c, events, nil
}

func stableExp(x float64, loc NumericEvent, cfg Config) (float64, *NumericEvent) {
	if math.IsNaN(x) || math.IsInf(x, 1) {
		ev := loc
		ev.Code, ev.Severity, ev.Kind, ev.Count = errs.ENumeric, "error", "non_finite_exp_argument", 1
		ev.Value = x
		ev.Message = "non-finite exponent argument"
		return 0, &ev
	}
	if x < cfg.ExpFloor {
		ev := loc
		ev.Code, ev.Severity, ev.Kind, ev.Count = errs.ENumeric, "diagnostic", "exp_underflow_clamp", 1
		ev.Value = x
		ev.Limit = cfg.ExpFloor
		ev.Message = "exp argument below declared float floor; flushed to zero"
		return 0, &ev
	}
	e := math.Exp(x)
	if e == 0 && x < 0 {
		ev := loc
		ev.Code, ev.Severity, ev.Kind, ev.Count = errs.ENumeric, "diagnostic", "exp_underflow_clamp", 1
		ev.Value = x
		ev.Message = "exp underflow; clamped to zero"
		return 0, &ev
	}
	return e, nil
}

func MergeOne(st *RowState, blockMax, blockSum float64, pv []float64, cfg Config) *errs.Error {
	if math.IsInf(blockMax, -1) || blockSum == 0 {
		return nil
	}
	if !st.Valid {
		st.Valid = true
		st.M = blockMax
		st.Sum = blockSum
		st.LSE = blockMax + math.Log(blockSum)
		copy(st.Acc, pv)
		return checkStateFinite(st)
	}

	newM := math.Max(st.M, blockMax)
	oldScale := math.Exp(st.M - newM)
	blockScale := math.Exp(blockMax - newM)

	switch cfg.Strategy {
	case Scaled:
		st.Sum = oldScale*st.Sum + blockScale*blockSum
	case LogSum:
		blockLSE := blockMax + math.Log(blockSum)
		st.LSE = logAddExp(st.LSE, blockLSE)
		st.Sum = math.Exp(st.LSE - newM)
	}
	for d := range st.Acc {
		st.Acc[d] = oldScale*st.Acc[d] + blockScale*pv[d]
	}
	if cfg.Strategy == Scaled {
		st.LSE = newM + math.Log(st.Sum)
	}
	st.M = newM
	return checkStateFinite(st)
}

func checkStateFinite(st *RowState) *errs.Error {
	if !isFinite(st.M) || !isFinite(st.Sum) || !isFinite(st.LSE) {
		return errs.New(errs.ENumeric, "non-finite running scalar")
	}
	for _, x := range st.Acc {
		if !isFinite(x) {
			return errs.New(errs.ENumeric, "non-finite accumulator")
		}
	}
	return nil
}

func checkWidth(st *RowState, cfg Config, bh, b, h, rb, local, row int) []NumericEvent {
	if !st.Valid || cfg.ExpSumBits >= 63 {
		return nil
	}
	limit := cfg.ExpSumLimit()
	if st.Sum > limit {
		return []NumericEvent{{
			Code: errs.ENumeric, Severity: "diagnostic", Kind: "width_would_clamp",
			BH: bh, Batch: b, Head: h, RowBlock: rb, LocalRow: local, Row: row,
			Count: 1, Value: st.Sum, Limit: limit, Bits: cfg.ExpSumBits,
			Message: "finite exponent sum exceeds declared signed fixed-point width; position recorded, arithmetic max-normalized sum retained",
		}}
	}
	return nil
}

func FinalizeOne(st *RowState, cfg Config) (RowFinal, *errs.Error) {
	d := len(st.Acc)
	out := make([]float32, d)
	final := RowFinal{M: st.M, Sum: st.Sum, LSE: st.LSE, Normalizer: st.Sum, Output: out}
	if !st.Valid {
		return final, nil
	}
	if !(st.Sum > 0) || !isFinite(st.Sum) {
		return final, errs.New(errs.ENumeric, "invalid exponent sum %v", st.Sum)
	}
	for i, x := range st.Acc {
		if !isFinite(x) {
			return final, errs.New(errs.ENumeric, "non-finite weighted accumulator at dim %d", i)
		}
		y := x / st.Sum
		if !isFinite(y) {
			return final, errs.New(errs.ENumeric, "non-finite normalized output at dim %d", i)
		}
		out[i] = float32(y)
		if math.IsNaN(float64(out[i])) || math.IsInf(float64(out[i]), 0) {
			return final, errs.New(errs.ENumeric, "output conversion is non-finite at dim %d", i)
		}
	}
	return final, nil
}

func logAddExp(a, b float64) float64 {
	if math.IsInf(a, -1) {
		return b
	}
	if math.IsInf(b, -1) {
		return a
	}
	if a > b {
		return a + math.Log1p(math.Exp(b-a))
	}
	return b + math.Log1p(math.Exp(a-b))
}

func isFinite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func compactEvents(events []NumericEvent) []NumericEvent {
	if len(events) <= 32 {
		return events
	}
	out := make([]NumericEvent, 0, 33)
	out = append(out, events[:16]...)
	counts := map[string]int{}
	for _, ev := range events[16 : len(events)-16] {
		counts[ev.Kind] += ev.Count
	}
	for kind, n := range counts {
		out = append(out, NumericEvent{Code: errs.ENumeric, Severity: "diagnostic", Kind: kind + "_aggregate", Count: n, Message: "additional events aggregated"})
	}
	out = append(out, events[len(events)-16:]...)
	return out
}

func hashOutputSlices(rows [][]float32) string {
	all := make([]float32, 0)
	for _, r := range rows {
		all = append(all, r...)
	}
	return HashFloat32(all)
}
