// Package audit 比较分块输出与定点参考真值，并推导逐行块误差界。
package audit

import (
	"math"

	"flashattention/internal/attn"
	"flashattention/internal/errs"
	"flashattention/internal/reference"
	"flashattention/internal/tensor"
)

const (
	Eps32        = 5.960464477539063e-8
	Eps64        = 2.220446049250313e-16
	SafetyFactor = 16.0
	RelFloor     = 1e-6
)

type Position struct {
	Batch int `json:"batch"`
	Head  int `json:"head"`
	Row   int `json:"row"`
	Dim   int `json:"dim"`
}

type BlockReport struct {
	ShardID           string             `json:"shard_id"`
	RowBlock          int                `json:"row_block"`
	RowStart          int                `json:"row_start"`
	RowEnd            int                `json:"row_end"`
	SamplePositions   []Position         `json:"sample_positions"`
	SampleMaxAbsErr   float64            `json:"sample_max_abs_err"`
	SampleMaxRelErr   float64            `json:"sample_max_rel_err"`
	ExhaustiveChecked bool               `json:"exhaustive_checked"`
	PointsChecked     int                `json:"points_checked"`
	MaxAbsErr         float64            `json:"max_abs_err"`
	MaxRelErr         float64            `json:"max_rel_err"`
	PeakRelErr        float64            `json:"peak_rel_err"`
	MaxAbsPoint       Position           `json:"max_abs_point"`
	MaxRelPoint       Position           `json:"max_rel_point"`
	BlockRefMaxAbs    float64            `json:"block_ref_max_abs"`
	Bound             float64            `json:"error_bound"`
	Tolerance         float64            `json:"tolerance"`
	EffectiveDigits   float64            `json:"effective_digits"`
	ObservedDigits    float64            `json:"observed_digits"`
	PeakDigits        float64            `json:"peak_digits"`
	GuaranteedDigits  float64            `json:"guaranteed_digits"`
	QuantDigits       float64            `json:"quant_digits"`
	QuantStep         float64            `json:"quant_step"`
	Scale             float64            `json:"scale"`
	ColBlocks         int                `json:"col_blocks"`
	HeadDim           int                `json:"head_dim"`
	OK                bool               `json:"ok"`
	BoundComponents   map[string]float64 `json:"bound_components"`
	Derivation        string             `json:"derivation"`
}

type Set struct {
	ShardID string        `json:"shard_id"`
	OK      bool          `json:"ok"`
	Blocks  []BlockReport `json:"blocks"`
}

type Envelope struct {
	QMax float64
	KMax float64
	VMax float64
}

func TensorMaxAbs(t *tensor.Tensor) float64 {
	m := 0.0
	for _, v := range t.Data {
		if a := math.Abs(float64(v)); a > m {
			m = a
		}
	}
	return m
}

func Audit(shardID string, cfg attn.Config, sh tensor.Shape, out *tensor.Tensor, ref *reference.Output, env Envelope) (*Set, error) {
	cfg.Normalize(sh)
	want := ref.Dequantized()
	nCol := (cfg.KeyLen + cfg.ColBlock - 1) / cfg.ColBlock
	set := &Set{ShardID: shardID, OK: true}
	var firstFailure error

	for rb := 0; rb*cfg.RowBlock < sh.S; rb++ {
		r0 := rb * cfg.RowBlock
		r1 := min(r0+cfg.RowBlock, sh.S)
		rep := BlockReport{
			ShardID: shardID, RowBlock: rb, RowStart: r0, RowEnd: r1,
			ExhaustiveChecked: true, QuantStep: ref.Step, Scale: cfg.Scale,
			ColBlocks: nCol, HeadDim: sh.D, OK: true,
			BoundComponents: map[string]float64{},
		}
		rep.SamplePositions = explicitSamples(sh, r0, r1, 32)

		for b := 0; b < sh.B; b++ {
			for h := 0; h < sh.H; h++ {
				for i := r0; i < r1; i++ {
					for d := 0; d < sh.D; d++ {
						off := out.Offset(b, h, i, d)
						got := float64(out.Data[off])
						w := want[off]
						absErr := math.Abs(got - w)
						relErr := absErr / math.Max(math.Abs(w), RelFloor)
						rep.PointsChecked++
						rep.BlockRefMaxAbs = math.Max(rep.BlockRefMaxAbs, math.Abs(w))
						if absErr > rep.MaxAbsErr {
							rep.MaxAbsErr = absErr
							rep.MaxAbsPoint = Position{b, h, i, d}
						}
						if relErr > rep.MaxRelErr {
							rep.MaxRelErr = relErr
							rep.MaxRelPoint = Position{b, h, i, d}
						}
					}
				}
			}
		}

		for _, p := range rep.SamplePositions {
			off := out.Offset(p.Batch, p.Head, p.Row, p.Dim)
			ae := math.Abs(float64(out.Data[off]) - want[off])
			re := ae / math.Max(math.Abs(want[off]), RelFloor)
			rep.SampleMaxAbsErr = math.Max(rep.SampleMaxAbsErr, ae)
			rep.SampleMaxRelErr = math.Max(rep.SampleMaxRelErr, re)
		}

		rep.Bound, rep.BoundComponents, rep.Derivation = deriveBound(cfg, sh.D, nCol, ref.Step, env, rep.BlockRefMaxAbs)
		rep.Tolerance = rep.Bound
		rep.ObservedDigits = digits(rep.MaxRelErr)
		rep.PeakRelErr = rep.MaxAbsErr / math.Max(rep.BlockRefMaxAbs, RelFloor)
		rep.PeakDigits = digits(rep.PeakRelErr)
		denom := math.Max(rep.BlockRefMaxAbs, RelFloor)
		rep.GuaranteedDigits = digits(rep.Bound / denom)
		rep.QuantDigits = math.Log10(math.Pow(2, float64(ref.Bits)))
		rep.EffectiveDigits = math.Min(math.Min(rep.PeakDigits, rep.GuaranteedDigits), rep.QuantDigits)
		if rep.MaxAbsErr > rep.Tolerance || !isFinite(rep.MaxAbsErr) {
			rep.OK = false
			set.OK = false
			if firstFailure == nil {
				firstFailure = errs.New(errs.EErrorBound,
					"shard %s row_block %d max_abs_err %.6g exceeds tolerance %.6g",
					shardID, rb, rep.MaxAbsErr, rep.Tolerance).
					With("shard", shardID).
					With("row_block", rb).
					With("max_abs_err", rep.MaxAbsErr).
					With("max_rel_err", rep.MaxRelErr).
					With("tolerance", rep.Tolerance).
					With("point", rep.MaxAbsPoint)
			}
		}
		set.Blocks = append(set.Blocks, rep)
	}
	return set, firstFailure
}

func explicitSamples(sh tensor.Shape, r0, r1, maxN int) []Position {
	total := sh.B * sh.H * (r1 - r0) * sh.D
	n := min(maxN, total)
	out := make([]Position, 0, n)
	seen := map[Position]bool{}
	for k := 0; k < n; k++ {
		flat := k * total / n
		d := flat % sh.D
		flat /= sh.D
		i := r0 + flat%(r1-r0)
		flat /= r1 - r0
		h := flat % sh.H
		b := flat / sh.H
		p := Position{b, h, i, d}
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

func deriveBound(cfg attn.Config, d, nCol int, step float64, env Envelope, outMag float64) (float64, map[string]float64, string) {
	gammaD := gamma(d)
	gammaCol := gamma(cfg.ColBlock)
	gammaMerge := gamma(nCol + cfg.GroupBlocks + 4)
	dotEnvelope := cfg.Scale * gammaD * float64(d) * max1(env.QMax) * max1(env.KMax)
	softmaxEnvelope := SafetyFactor * 8 * dotEnvelope * max1(env.VMax)
	pvEnvelope := SafetyFactor * gammaCol * max1(env.VMax)
	mergeEnvelope := SafetyFactor * gammaMerge * (max1(outMag) + max1(env.VMax))
	orderEnvelope := SafetyFactor * float64(nCol*d) * Eps64 * (max1(outMag) + max1(env.VMax))
	castEnvelope := 0.5*Eps32*math.Max(outMag, 1.0) + 16*Eps32
	platformMargin := 64 * Eps32
	quantEnvelope := step / 2
	bound := quantEnvelope + castEnvelope + platformMargin + SafetyFactor*(dotEnvelope+softmaxEnvelope+pvEnvelope+mergeEnvelope+orderEnvelope)
	components := map[string]float64{
		"quant_step_half":         quantEnvelope,
		"output_float32_rounding": castEnvelope,
		"platform_order_margin":   platformMargin,
		"qk_dot_order_envelope":   dotEnvelope,
		"softmax_sensitivity":     softmaxEnvelope,
		"pv_block_order_envelope": pvEnvelope,
		"online_merge_order":      mergeEnvelope,
		"ncol_by_dim_order":       orderEnvelope,
		"safety_factor":           SafetyFactor,
		"eps32":                   Eps32,
		"eps64":                   Eps64,
	}
	formula := "bound = step/2 + eps32/2*max(out_mag,1) + 80eps32 + safety*[scale*gamma(D)*D*|Q|max*|K|max*(1+8|V|max) + gamma(CB)|V|max + gamma(NCol+Group+4)(out_mag+|V|max) + NCol*D*eps64*(out_mag+|V|max)]; gamma(n)=n eps64/(1-n eps64); accumulation order follows row-block -> col-block -> dim"
	return bound, components, formula
}

func gamma(n int) float64 {
	nf := float64(max(n, 1))
	x := nf * Eps64
	if x >= 1 {
		return 1
	}
	return x / (1 - x)
}

func digits(rel float64) float64 {
	if rel <= 0 || math.IsNaN(rel) {
		return 15
	}
	d := -math.Log10(rel)
	if d > 15 {
		return 15
	}
	if d < 0 {
		return 0
	}
	return d
}

func max1(x float64) float64 { return math.Max(x, 1) }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func isFinite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }
