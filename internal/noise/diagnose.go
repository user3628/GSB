package noise

import (
	"math"
	"math/big"
	"sort"

	"matbench/internal/recipe"
)

func bfNew(x float64) *big.Float { return new(big.Float).SetPrec(bigPrec).SetFloat64(x) }

// bfSampleLayer re-evaluates one octave at one pixel using explicit big.Float
// arithmetic so the reference accumulation is independent of float64 rounding.
func bfSampleLayer(l *octaveLayer, px, py int) *big.Float {
	size := bfNew(float64(FieldSize))
	fx := new(big.Float).SetPrec(bigPrec).Quo(bfNew(float64(px)*float64(l.girth)), size)
	fy := new(big.Float).SetPrec(bigPrec).Quo(bfNew(float64(py)*float64(l.girth)), size)
	x0f, _ := fx.Float64()
	y0f, _ := fy.Float64()
	x0 := int(math.Floor(x0f))
	y0 := int(math.Floor(y0f))
	tx := bfSmooth(new(big.Float).SetPrec(bigPrec).Sub(fx, bfNew(float64(x0))))
	ty := bfSmooth(new(big.Float).SetPrec(bigPrec).Sub(fy, bfNew(float64(y0))))
	at := func(gx, gy int) *big.Float {
		return bfNew(l.grid[(gy%l.girth)*l.girth+(gx%l.girth)])
	}
	one := bfNew(1)
	top := bfAdd(bfMul(at(x0, y0), bfSub(one, tx)), bfMul(at(x0+1, y0), tx))
	bot := bfAdd(bfMul(at(x0, y0+1), bfSub(one, tx)), bfMul(at(x0+1, y0+1), tx))
	return bfAdd(bfMul(top, bfSub(one, ty)), bfMul(bot, ty))
}

func bfSmooth(t *big.Float) *big.Float {
	// t*t*(3-2t)
	t2 := bfMul(t, t)
	three := bfNew(3)
	two := bfNew(2)
	return bfMul(t2, bfSub(three, bfMul(two, t)))
}

func bfAdd(a, b *big.Float) *big.Float { return new(big.Float).SetPrec(bigPrec).Add(a, b) }
func bfSub(a, b *big.Float) *big.Float { return new(big.Float).SetPrec(bigPrec).Sub(a, b) }
func bfMul(a, b *big.Float) *big.Float { return new(big.Float).SetPrec(bigPrec).Mul(a, b) }

// diagnose builds per-octave diagnostics. Error bounds are derived from an
// explicit 96-bit reference accumulation over a deterministic 16x16 grid,
// combined with the analytic float64 rounding margin; they are never guessed.
func diagnose(r recipe.Recipe, layers []*octaveLayer, field []float64) ([]OctaveDiag, error) {
	diags := make([]OctaveDiag, len(layers))

	// Explicit high-precision reference accumulation at the reference grid.
	type refPoint struct{ x, y int }
	points := make([]refPoint, 0, refGrid*refGrid)
	for ry := 0; ry < refGrid; ry++ {
		for rx := 0; rx < refGrid; rx++ {
			points = append(points, refPoint{
				x: rx * FieldSize / refGrid,
				y: ry * FieldSize / refGrid,
			})
		}
	}

	refPerOct := make([][]*big.Float, len(layers))
	refSum := make([]*big.Float, len(points))
	bfSum := make([]*big.Float, len(points))
	for i := range refSum {
		refSum[i] = bfNew(0)
		bfSum[i] = bfNew(0)
	}
	eps := math.Pow(2, -53)
	cumBound := 0.0
	for oi, l := range layers {
		refVals := make([]*big.Float, len(points))
		maxDelta := 0.0
		theory := 0.0
		l1 := 0.0
		logSum := 0.0
		for pi, pt := range points {
			refV := bfMul(bfNew(l.normAmp), bfSampleLayer(l, pt.x, pt.y))
			refVals[pi] = refV
			refSum[pi] = bfAdd(refSum[pi], refV)
			bfSum[pi] = bfAdd(bfSum[pi], refV)

			// Kahan float64 accumulation at the same pixel, rebuilt here from
			// the same sampled values to measure the actual rounding gap.
			loSum := kahanAt(layers[:oi+1], pt.x, pt.y)
			refF, _ := refSum[pi].Float64()
			if d := math.Abs(loSum - refF); d > maxDelta {
				maxDelta = d
			}
			rv, _ := refV.Float64()
			l1 += math.Abs(rv)
			logSum += math.Log2(1 + math.Abs(rv))

			// Analytic margin: each addition costs <= 0.5 ulp of the running sum.
			theory += math.Abs(refF)*eps + eps
		}
		refPerOct[oi] = refVals
		// Per-octave bound: observed reference gap plus the explicit rounding
		// margin accumulated for this layer's weighted values.
		bound := maxDelta + theory
		if bound < eps {
			bound = eps
		}
		cumBound += bound
		if cumBound > 1 {
			cumBound = 1
		}
		diags[oi] = OctaveDiag{
			Index:           oi,
			Girth:           l.girth,
			RawAmplitude:    l.rawAmp,
			NormAmplitude:   l.normAmp,
			LogContribution: logSum / float64(len(points)),
			ErrorBound:      bound,
			CumulativeBound: cumBound,
			RefMaxDelta:     maxDelta,
			TheoryBound:     theory,
			PerturbL1:       l1 / float64(len(points)),
		}
	}

	// Perturbation replay: re-synthesize with one octave perturbed and record
	// the L1 response of the final field over the reference grid.
	for oi := range layers {
		resp := 0.0
		for _, pt := range points {
			orig := kahanAt(layers, pt.x, pt.y)
			pert := kahanAtPerturbed(layers, pt.x, pt.y, oi)
			resp += math.Abs(pert-orig) / float64(len(points))
		}
		diags[oi].PerturbL1 = resp
	}

	rankOrders(diags)
	return diags, nil
}

// kahanAt recomputes the float64 Kahan accumulation through layer k at a pixel.
func kahanAt(layers []*octaveLayer, px, py int) float64 {
	sum, comp := 0.0, 0.0
	for _, l := range layers {
		if l.normAmp == 0 {
			continue
		}
		v := l.normAmp * sampleLayer(l, px, py)
		y := v - comp
		t := sum + y
		comp = (t - sum) - y
		sum = t
	}
	return sum
}

// kahanAtPerturbed is the replay path: the named octave receives a deterministic
// +1 ulp perturbation, everything else stays identical.
func kahanAtPerturbed(layers []*octaveLayer, px, py, target int) float64 {
	sum, comp := 0.0, 0.0
	for oi, l := range layers {
		if l.normAmp == 0 {
			continue
		}
		v := l.normAmp * sampleLayer(l, px, py)
		if oi == target {
			v = math.Nextafter(v, math.Inf(1)) - v + v
		}
		y := v - comp
		t := sum + y
		comp = (t - sum) - y
		sum = t
	}
	return sum
}

type rankItem struct {
	index int
	value float64
}

func rankOrders(diags []OctaveDiag) {
	contrib := make([]rankItem, len(diags))
	perturb := make([]rankItem, len(diags))
	for i, d := range diags {
		contrib[i] = rankItem{i, d.LogContribution}
		perturb[i] = rankItem{i, d.PerturbL1}
	}
	sort.SliceStable(contrib, func(a, b int) bool {
		if contrib[a].value != contrib[b].value {
			return contrib[a].value > contrib[b].value
		}
		return contrib[a].index < contrib[b].index
	})
	sort.SliceStable(perturb, func(a, b int) bool {
		if perturb[a].value != perturb[b].value {
			return perturb[a].value > perturb[b].value
		}
		return perturb[a].index < perturb[b].index
	})
	review := map[int]bool{}
	for rank := range contrib {
		diags[contrib[rank].index].ContribRank = rank
		diags[perturb[rank].index].PerturbRank = rank
	}
	for i := range diags {
		if diags[i].ContribRank != diags[i].PerturbRank {
			diags[i].ReviewFlag = true
			review[i] = true
		}
	}
}
