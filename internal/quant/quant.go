package quant

import (
	"math"
	"sort"

	"imretrace/internal/model"
)

type Matrix struct {
	Seed           int64       `json:"seed"`
	OutputDim      int         `json:"output_dim"`
	VisualInputDim int         `json:"visual_input_dim"`
	TextInputDim   int         `json:"text_input_dim"`
	Visual         [][]float64 `json:"visual"`
	Text           [][]float64 `json:"text"`
	Digest         string      `json:"digest"`
}

type rng struct{ state uint64 }

func newRng(seed int64) *rng { return &rng{state: uint64(seed)} }

func (r *rng) u64() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (r *rng) norm() float64 {
	u1 := (float64(r.u64()>>11) + 1) / float64(1<<53)
	u2 := float64(r.u64()>>11) / float64(1<<53)
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}

func deriveRows(seed int64, rows, cols int) [][]float64 {
	r := newRng(seed)
	out := make([][]float64, rows)
	scale := 1.0 / math.Sqrt(float64(cols))
	for i := 0; i < rows; i++ {
		out[i] = make([]float64, cols)
		for j := 0; j < cols; j++ {
			out[i][j] = round12(r.norm() * scale)
		}
	}
	return out
}

func round12(v float64) float64 { return math.Round(v*1e12) / 1e12 }

func DeriveMatrix(p model.Policy) *Matrix {
	visualDim := model.VisualDim(p.Grid)
	m := &Matrix{
		Seed:           p.ProjectionSeed,
		OutputDim:      p.ProjectionDim,
		VisualInputDim: visualDim,
		TextInputDim:   model.TextDim,
		Visual:         deriveRows(p.ProjectionSeed^0x566973, p.ProjectionDim, visualDim),
		Text:           deriveRows(p.ProjectionSeed^0x546578, p.ProjectionDim, model.TextDim),
	}
	m.Digest = model.HashBytes("projection-matrix-v1:", model.MustCanonical(struct {
		Seed int64       `json:"seed"`
		V    [][]float64 `json:"visual"`
		T    [][]float64 `json:"text"`
	}{m.Seed, m.Visual, m.Text}))
	return m
}

func Multiply(itemID, modality string, rows [][]float64, x []float64, outputDim int) ([]float64, error) {
	if len(rows) != outputDim {
		return nil, model.DimError(model.CodeQuantInvalid, model.StageProjectQuant, itemID, 0,
			"%s projection row count %d does not match declared dimension %d", modality, len(rows), outputDim)
	}
	if outputDim > 0 && len(rows[0]) != len(x) {
		dim := len(x)
		return nil, model.DimError(model.CodeQuantInvalid, model.StageProjectQuant, itemID, dim,
			"%s projection input dim %d does not match matrix column dim %d", modality, len(x), len(rows[0]))
	}
	out := make([]float64, outputDim)
	for d := 0; d < outputDim; d++ {
		if len(rows[d]) != len(x) {
			return nil, model.DimError(model.CodeQuantInvalid, model.StageProjectQuant, itemID, d,
				"%s projection row %d has %d columns, input has %d", modality, d, len(rows[d]), len(x))
		}
		sum := 0.0
		for j, v := range x {
			sum += rows[d][j] * v
		}
		out[d] = sum
	}
	return out, nil
}

func Normalize(itemID string, v []float64) ([]float64, float64, error) {
	norm := 0.0
	for _, x := range v {
		norm += x * x
	}
	if norm == 0 {
		return nil, 0, model.DimError(model.CodeQuantInvalid, model.StageProjectQuant, itemID, 0, "projected vector is zero")
	}
	norm = math.Sqrt(norm)
	out := make([]float64, len(v))
	for i, x := range v {
		out[i] = round12(x / norm)
	}
	return out, norm, nil
}

type Params struct {
	Modality    string    `json:"modality"`
	Bits        int       `json:"bits"`
	Levels      int       `json:"levels"`
	Dim         int       `json:"dim"`
	Granularity string    `json:"granularity"`
	RangeMin    []float64 `json:"range_min"`
	RangeMax    []float64 `json:"range_max"`
	Step        []float64 `json:"step"`
	Scale       []float64 `json:"scale"`
	QuantScale  []float64 `json:"quant_scale"`
	ZeroPoint   []int     `json:"zero_point"`
	ParamDigest string    `json:"param_digest"`
}

func Calibrate(modality string, vectors [][]float64, p model.Policy) (*Params, error) {
	d := p.ProjectionDim
	params := &Params{Modality: modality, Bits: p.QuantBits, Levels: (1 << p.QuantBits) - 1, Dim: d, Granularity: p.QuantGranularity}
	if len(vectors) == 0 {
		return nil, model.NewBiz(model.CodeQuantInvalid, model.StageProjectQuant, "corpus", "no vectors for %s calibration", modality)
	}
	for _, v := range vectors {
		if len(v) != d {
			return nil, model.DimError(model.CodeQuantInvalid, model.StageProjectQuant, "corpus", len(v), "%s calibration vector dim %d expected %d", modality, len(v), d)
		}
	}
	limit := make([]float64, d)
	globalMax := 0.0
	for _, v := range vectors {
		for j, x := range v {
			limit[j] = math.Max(limit[j], math.Abs(x))
			globalMax = math.Max(globalMax, math.Abs(x))
		}
	}
	const headroom = 1.35
	const minimumLimit = 0.20
	if p.QuantGranularity == "vector" {
		globalLimit := math.Max(minimumLimit, globalMax*headroom)
		for j := range limit {
			limit[j] = globalLimit
		}
	} else {
		for j := range limit {
			limit[j] = math.Max(minimumLimit, limit[j]*headroom)
		}
	}
	params.RangeMin = make([]float64, d)
	params.RangeMax = make([]float64, d)
	params.Step = make([]float64, d)
	params.Scale = make([]float64, d)
	params.QuantScale = make([]float64, d)
	params.ZeroPoint = make([]int, d)
	shiftBits := p.QuantBits - 1
	signedMaxInt := (1 << shiftBits) - 1
	signedMax := float64(signedMaxInt)
	zeroPoint := 1 << shiftBits
	for j := 0; j < d; j++ {
		params.RangeMin[j], params.RangeMax[j] = -limit[j], limit[j]
		step := limit[j] / signedMax
		zp := zeroPoint
		qs := 0.0
		if step > 0 {
			qs = 1 / step
		}
		params.Step[j] = step
		params.Scale[j] = step
		params.QuantScale[j] = qs
		params.ZeroPoint[j] = zp
	}
	params.ParamDigest = params.computeDigest()
	return params, nil
}

func (p Params) computeDigest() string {
	cp := p
	cp.ParamDigest = ""
	return model.HashBytes("quant-params-v1:", model.MustCanonical(cp))
}

func (p *Params) RefreshDigest() string {
	p.ParamDigest = p.computeDigest()
	return p.ParamDigest
}

type Vector struct {
	ItemID      string    `json:"item_id"`
	Modality    string    `json:"modality"`
	Codes       []int     `json:"codes"`
	Dequant     []float64 `json:"dequant"`
	ParamDigest string    `json:"param_digest"`
	Digest      string    `json:"digest"`
}

func QuantizeVector(itemID, modality string, x []float64, params *Params) (*Vector, error) {
	if params == nil {
		return nil, model.NewBiz(model.CodeQuantInvalid, model.StageProjectQuant, itemID, "missing quant params")
	}
	if len(x) != params.Dim {
		return nil, model.DimError(model.CodeQuantInvalid, model.StageProjectQuant, itemID, len(x), "%s vector dim %d expected %d", modality, len(x), params.Dim)
	}
	out := &Vector{ItemID: itemID, Modality: modality, Codes: make([]int, params.Dim), Dequant: make([]float64, params.Dim), ParamDigest: params.ParamDigest}
	allCodesZero := true
	allDequantZero := true
	for d := 0; d < params.Dim; d++ {
		step := params.Step[d]
		scale := params.Scale[d]
		if step == 0 || scale == 0 || math.IsNaN(step) || math.IsInf(step, 0) {
			return nil, model.DimError(model.CodeQuantInvalid, model.StageProjectQuant, itemID, d, "%s scale/step is zero or non-finite at dimension %d", modality, d)
		}
		code := int(math.Round(x[d]/step)) + params.ZeroPoint[d]
		if code < 0 {
			code = 0
		}
		if code > params.Levels {
			code = params.Levels
		}
		out.Codes[d] = code
		out.Dequant[d] = round12(step * float64(code-params.ZeroPoint[d]))
		if code != 0 {
			allCodesZero = false
		}
		if out.Dequant[d] != 0 {
			allDequantZero = false
		}
	}
	if allCodesZero || allDequantZero {
		d := 0
		return nil, model.DimError(model.CodeQuantInvalid, model.StageProjectQuant, itemID, d, "%s quantized integer vector is all zero; first dimension position %d", modality, d)
	}
	out.Digest = model.HashBytes("quant-vector-v1:", model.MustCanonical(out))
	return out, nil
}

type RouteDiagnostic struct {
	Route                       string    `json:"route"`
	Modality                    string    `json:"modality"`
	ItemID                      string    `json:"item_id"`
	ReferenceNorm               float64   `json:"reference_norm"`
	QuantNorm                   float64   `json:"quant_norm"`
	VectorL2Actual              float64   `json:"vector_l2_actual"`
	VectorL2Bound               float64   `json:"vector_l2_bound"`
	EffectiveBits               float64   `json:"effective_bits"`
	EffectiveDecimalDigits      float64   `json:"effective_decimal_digits"`
	LogContribution             float64   `json:"log_contribution"`
	LogNormContribution         float64   `json:"log_norm_contribution"`
	LogErrorContribution        float64   `json:"log_error_contribution"`
	Log10ErrorContribution      float64   `json:"log10_error_contribution"`
	ParamDigest                 string    `json:"param_digest"`
	Formula                     string    `json:"formula"`
	Scale                       []float64 `json:"scale"`
	Step                        []float64 `json:"step"`
	ZeroPoint                   []int     `json:"zero_point"`
	OutsideRange                []float64 `json:"outside_range"`
	CoordinateHalfStepBound     []float64 `json:"coordinate_half_step_bound"`
	CoordinateConservativeBound []float64 `json:"coordinate_conservative_bound"`
	ActualAbs                   []float64 `json:"actual_abs"`
}

func DiagnoseRoute(itemID, route, modality string, ref, dequant []float64, params *Params) (*RouteDiagnostic, error) {
	if len(ref) != len(dequant) || params == nil || len(params.Step) != len(ref) {
		return nil, model.NewBiz(model.CodeQuantInvalid, model.StageRecallVerify, itemID, "diagnostic dimension mismatch")
	}
	d := len(ref)
	diag := &RouteDiagnostic{
		Route: route, Modality: modality, ItemID: itemID, ParamDigest: params.ParamDigest,
		Formula: "bound_d=outside_calibration_range_d + step_d/2; ||e||_2 <= sqrt(sum bound_d^2)",
		Scale:   append([]float64(nil), params.Scale...), Step: append([]float64(nil), params.Step...), ZeroPoint: append([]int(nil), params.ZeroPoint...),
		OutsideRange: make([]float64, d), CoordinateHalfStepBound: make([]float64, d),
		CoordinateConservativeBound: make([]float64, d), ActualAbs: make([]float64, d),
	}
	var boundSq, actualSq float64
	for i := 0; i < d; i++ {
		outside := 0.0
		if ref[i] < params.RangeMin[i] {
			outside = params.RangeMin[i] - ref[i]
		} else if ref[i] > params.RangeMax[i] {
			outside = ref[i] - params.RangeMax[i]
		}
		half := params.Step[i] / 2
		conservative := outside + half
		actual := math.Abs(dequant[i] - ref[i])
		diag.OutsideRange[i] = round12(outside)
		diag.CoordinateHalfStepBound[i] = round12(half)
		diag.CoordinateConservativeBound[i] = round12(conservative)
		diag.ActualAbs[i] = round12(actual)
		boundSq += conservative * conservative
		actualSq += actual * actual
	}
	diag.VectorL2Bound = math.Sqrt(boundSq)
	diag.VectorL2Actual = math.Sqrt(actualSq)
	diag.ReferenceNorm = norm(ref)
	diag.QuantNorm = norm(dequant)
	diag.EffectiveBits = effectiveBits(diag.VectorL2Bound)
	diag.EffectiveDecimalDigits = decimalDigits(diag.VectorL2Bound)
	diag.LogContribution = math.Log(math.Max(diag.QuantNorm, 1e-300))
	diag.LogNormContribution = diag.LogContribution
	diag.LogErrorContribution = math.Log(math.Max(diag.VectorL2Bound, 1e-300))
	diag.Log10ErrorContribution = math.Log10(math.Max(diag.VectorL2Bound, 1e-300))
	return diag, nil
}

func norm(v []float64) float64 {
	s := 0.0
	for _, x := range v {
		s += x * x
	}
	return math.Sqrt(s)
}

func effectiveBits(bound float64) float64 {
	if bound <= 0 {
		return 53
	}
	bits := -math.Log2(bound)
	return math.Round(math.Max(0, bits)*1000) / 1000
}

func decimalDigits(bound float64) float64 {
	if bound <= 0 {
		return 15
	}
	digits := math.Floor(-math.Log10(bound))
	return math.Max(0, digits)
}

type ScoreBound struct {
	CosineBound                        float64 `json:"cosine_bound"`
	CosineActualAbs                    float64 `json:"cosine_actual_abs"`
	FinalScoreBound                    float64 `json:"final_score_bound"`
	TheoreticalScoreBound              float64 `json:"theoretical_score_bound"`
	ReferenceVerifiedBound             float64 `json:"reference_verified_bound"`
	FinalActualAbs                     float64 `json:"final_actual_abs"`
	ReferenceFusedScore                float64 `json:"reference_fused_score"`
	QuantFusedScore                    float64 `json:"quant_fused_score"`
	RelativeErrorBound                 float64 `json:"relative_error_bound"`
	RelativeActual                     float64 `json:"relative_actual"`
	EffectiveBits                      float64 `json:"effective_bits"`
	EffectiveSignificantDigits         float64 `json:"effective_significant_digits"`
	VerifiedEffectiveBits              float64 `json:"verified_effective_bits"`
	VerifiedEffectiveSignificantDigits float64 `json:"verified_effective_significant_digits"`
	EffectiveAbsDecimalDigits          float64 `json:"effective_abs_decimal_digits"`
	Formula                            string  `json:"formula"`
}

func Cosine(a, b []float64) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	c := dot / (math.Sqrt(na) * math.Sqrt(nb))
	return math.Max(-1, math.Min(1, c))
}

func BoundCosine(refCos float64, a, b *RouteDiagnostic) float64 {
	ea, eb := a.VectorL2Bound, b.VectorL2Bound
	amin := (1 - ea) * (1 - eb)
	if ea < 1 && eb < 1 && amin > 1e-12 {
		dotBound := ea + eb + ea*eb
		bound := dotBound * (1 + math.Abs(refCos)) / amin
		return math.Min(2, bound)
	}
	return 2
}

func ScoreDiagnostic(refFused, quantFused, refCos, quantCos, cosineBound, weightCos, weightTerm float64) ScoreBound {
	weight := weightCos + weightTerm
	finalBound := 0.0
	if weight > 0 {
		finalBound = math.Abs(weightCos) / weight * cosineBound
	}
	actualCos := math.Abs(refCos - quantCos)
	actualFinal := math.Abs(refFused - quantFused)
	relBound := finalBound / math.Max(math.Abs(refFused), 0.10)
	relActual := actualFinal / math.Max(math.Abs(refFused), 0.10)
	return ScoreBound{
		CosineBound: cosineBound, CosineActualAbs: actualCos, FinalScoreBound: finalBound, FinalActualAbs: actualFinal,
		ReferenceFusedScore: refFused, QuantFusedScore: quantFused, RelativeErrorBound: relBound, RelativeActual: relActual,
		EffectiveBits: effectiveBits(relBound), EffectiveSignificantDigits: math.Max(0, math.Floor(-math.Log10(math.Max(relBound, 1e-15)))),
		EffectiveAbsDecimalDigits: decimalDigits(finalBound),
		Formula:                   "B_cos <= (E_v+E_t+E_v E_t)(1+|cos_ref|)/((1-E_v)(1-E_t)); B_final=w_cos/(w_cos+w_term)*B_cos; rel uses policy relative_score_floor",
	}
}

// WithVerifiedReference adds the tight a-posteriori bound obtained by direct
// comparison with the local float64 reference. The small guard covers direct
// floating-point summation noise.
func WithVerifiedReference(sd ScoreBound, refFused, relativeFloor float64) ScoreBound {
	sd.TheoreticalScoreBound = sd.FinalScoreBound
	sd.ReferenceVerifiedBound = sd.FinalActualAbs + 1e-12*math.Max(1, math.Abs(refFused))
	relVerified := sd.ReferenceVerifiedBound / math.Max(math.Abs(refFused), relativeFloor)
	sd.VerifiedEffectiveBits = effectiveBits(relVerified)
	sd.VerifiedEffectiveSignificantDigits = math.Max(0, math.Floor(-math.Log10(math.Max(relVerified, 1e-15))))
	return sd
}

type CorpusEstimate struct {
	CorpusItems       int          `json:"corpus_items"`
	MaxVectorL2Bound  float64      `json:"max_vector_l2_bound"`
	MeanVectorL2Bound float64      `json:"mean_vector_l2_bound"`
	RMSVectorL2Bound  float64      `json:"rms_vector_l2_bound"`
	MaxScoreBound     float64      `json:"max_score_bound"`
	MeanScoreBound    float64      `json:"mean_score_bound"`
	ScaleEstimate     []ScalePoint `json:"scale_estimate"`
}

type ScalePoint struct {
	SampleSize               int     `json:"sample_size"`
	UnionWorstCaseScoreBound float64 `json:"union_worst_case_score_bound"`
	SumScoreBound            float64 `json:"sum_score_bound"`
	RMSScoreBound            float64 `json:"rms_score_bound"`
	AggregateL2VectorBound   float64 `json:"aggregate_l2_vector_bound"`
}

func BuildCorpusEstimate(itemCount int, vectorBounds, scoreBounds []float64) CorpusEstimate {
	est := CorpusEstimate{CorpusItems: itemCount}
	var sumV, sqV, maxV, sumS, sqS, maxS float64
	n := float64(max(1, len(vectorBounds)))
	for _, v := range vectorBounds {
		sumV += v
		sqV += v * v
		maxV = math.Max(maxV, v)
	}
	m := float64(max(1, len(scoreBounds)))
	for _, v := range scoreBounds {
		sumS += v
		sqS += v * v
		maxS = math.Max(maxS, v)
	}
	est.MaxVectorL2Bound = maxV
	est.MeanVectorL2Bound = sumV / n
	est.RMSVectorL2Bound = math.Sqrt(sqV / n)
	est.MaxScoreBound = maxS
	est.MeanScoreBound = sumS / m
	rmsScore := math.Sqrt(sqS / m)
	sizes := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}
	for _, sz := range sizes {
		est.ScaleEstimate = append(est.ScaleEstimate, ScalePoint{
			SampleSize: sz, UnionWorstCaseScoreBound: maxS,
			SumScoreBound: float64(sz) * est.MeanScoreBound, RMSScoreBound: rmsScore,
			AggregateL2VectorBound: math.Sqrt(float64(sz)) * est.RMSVectorL2Bound,
		})
	}
	return est
}

func SortedFloatKeys[V any](m map[int]V) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}
