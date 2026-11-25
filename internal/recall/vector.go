package recall

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"math/rand"

	"multirecall/internal/model"
)

const CoordScale int64 = 100_000_000

func QueryVector(q model.Query, dim int) []float64 {
	sum := sha256.Sum256([]byte("multirecall-query-vector:v1:" + q.QueryID + ":" + textNormForVector(q.RawQuery)))
	seed := int64(binary.LittleEndian.Uint64(sum[:8]))
	r := rand.New(rand.NewSource(seed))
	raw := make([]float64, dim)
	for i := range raw {
		raw[i] = r.NormFloat64()
	}
	var norm float64
	for _, x := range raw {
		norm += x * x
	}
	norm = math.Sqrt(norm)
	out := make([]float64, dim)
	if norm > 0 {
		for i, x := range raw {
			out[i] = x / norm
		}
	}
	return out
}

func textNormForVector(s string) string {
	return string(model.CanonicalJSON(struct {
		Raw string `json:"raw"`
	}{Raw: s}))
}

func restoredVector(item model.Item) []float64 {
	out := make([]float64, len(item.VectorF32))
	for i, code := range item.VectorF32 {
		out[i] = float64(code) * item.VectorScale
	}
	return out
}

func validateVectorItem(item model.Item, expectedDim int) *model.APIError {
	dim := len(item.VectorF32)
	if item.VectorDim != expectedDim || dim != expectedDim {
		return &model.APIError{
			Code: model.CodeVectorInvalid, QueryID: "", ItemID: item.ItemID,
			Dimension: expectedDim, ExpectedDimension: expectedDim, ActualDimension: dim,
			Message: "vector dimension does not match catalog declaration",
		}
	}
	for i, v := range item.VectorF32 {
		if math.IsNaN(float64(v)) {
			return &model.APIError{
				Code: model.CodeVectorInvalid, ItemID: item.ItemID,
				Dimension: expectedDim, ExpectedDimension: expectedDim, ActualDimension: dim, NanIndex: i,
				Message: "encoded vector contains NaN",
			}
		}
	}
	restored := restoredVector(item)
	var norm2 float64
	for _, v := range restored {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return &model.APIError{Code: model.CodeVectorInvalid, ItemID: item.ItemID, Dimension: expectedDim, ActualDimension: dim, Message: "restored vector is non-finite"}
		}
		norm2 += v * v
	}
	if norm2 == 0 {
		return &model.APIError{
			Code: model.CodeVectorInvalid, ItemID: item.ItemID,
			Dimension: expectedDim, ExpectedDimension: expectedDim, ActualDimension: dim,
			Message: "restored vector has zero norm",
		}
	}
	return nil
}

func quantizeCoordinate(x float64) int64 {
	return int64(math.Round(x * float64(CoordScale)))
}

func cosineHighPrecision(a, b []float64) float64 {
	var dot, na, nb, c float64
	for i := range a {
		y := a[i]*b[i] - c
		t := dot + y
		c = (t - dot) - y
		dot = t
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

func SearchVector(q model.Query, items []model.Item, expectedDim, requestedDepth, effectiveDepth int) (RouteResult, *model.APIError) {
	requested := requestedDepth
	result := RouteResult{Route: "vector", Enabled: effectiveDepth > 0, RequestedDepth: requested, EffectiveDepth: effectiveDepth, Candidates: []Candidate{}}
	if effectiveDepth <= 0 {
		return result, nil
	}
	for _, item := range items {
		if apiErr := validateVectorItem(item, expectedDim); apiErr != nil {
			apiErr.QueryID = q.QueryID
			return RouteResult{}, apiErr
		}
	}

	queryVec := QueryVector(q, expectedDim)
	queryFP := make([]int64, expectedDim)
	var queryNormFP2 float64
	for i, x := range queryVec {
		queryFP[i] = quantizeCoordinate(x)
		v := float64(queryFP[i])
		queryNormFP2 += v * v
	}
	queryNormFP := math.Sqrt(queryNormFP2)
	queryDigest := model.DigestBytes("query-vector", model.CanonicalJSON(queryVec))

	var cands []Candidate
	for _, item := range items {
		itemVec := restoredVector(item)
		itemFP := make([]int64, expectedDim)
		var dotFP int64
		var itemNormFP2 float64
		for i, x := range itemVec {
			itemFP[i] = quantizeCoordinate(x)
			dotFP += itemFP[i] * queryFP[i]
			v := float64(itemFP[i])
			itemNormFP2 += v * v
		}
		itemNormFP := math.Sqrt(itemNormFP2)
		cosFixed := 0.0
		if itemNormFP > 0 && queryNormFP > 0 {
			cosFixed = float64(dotFP) / (itemNormFP * queryNormFP)
		}
		scoreFP := int64(math.Round(cosFixed * float64(ScaleVector)))
		cosRef := cosineHighPrecision(itemVec, queryVec)
		coordErrNorm := math.Sqrt(float64(expectedDim)) * 0.5 / float64(CoordScale)
		itemNorm := math.Sqrt(func() float64 {
			var s float64
			for _, x := range itemVec {
				s += x * x
			}
			return s
		}())
		queryNorm := math.Sqrt(func() float64 {
			var s float64
			for _, x := range queryVec {
				s += x * x
			}
			return s
		}())
		bound := 0.5 / float64(ScaleVector)
		if itemNorm > 0 && queryNorm > 0 {
			bound += 4 * (coordErrNorm/itemNorm + coordErrNorm/queryNorm)
		}
		ev := VectorEvidence{
			Dimension: expectedDim, ExpectedDimension: expectedDim, Scale: item.VectorScale,
			DotFP: dotFP, ItemNormFP: itemNormFP, QueryNormFP: queryNormFP,
			CosineReference: cosRef, CosineFixed: cosFixed, ScoreFP: scoreFP,
			ErrorBound: bound, QueryVectorDigest: queryDigest,
			Formula: "restore x=float32_code*scale; fixed dot=sum(round(x*1e5)*round(q*1e5)); score_fp=round(cosine_fixed*1e6)",
		}
		cands = append(cands, Candidate{
			ItemID: item.ItemID, DomainScore: float64(scoreFP) / float64(ScaleVector), DomainScoreFP: scoreFP,
			RawErrorBound: bound, Vector: &ev,
		})
	}
	return finalizeRoute("vector", requested, effectiveDepth, cands), nil
}
