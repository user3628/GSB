package httpapi

import (
	"math/big"
	"strconv"

	"blocksnap/internal/fixed"
)

type arithmeticDiagnosticsResponse struct {
	Representation    representationDiagnostic `json:"representation"`
	CapacitySample    capacitySumDiagnostic    `json:"capacity_sum_sample"`
	WeightedSample    weightedSampleDiagnostic `json:"weighted_sample"`
	ToleranceContract toleranceDiagnostic      `json:"tolerance_contract"`
}

type representationDiagnostic struct {
	RatioScale        int    `json:"ratio_scale"`
	IntegerDomain     string `json:"integer_domain"`
	Rounding          string `json:"rounding"`
	FloatingPointUsed bool   `json:"floating_point_used"`
	Accumulation      string `json:"accumulation"`
}

type capacitySumDiagnostic struct {
	ValuesBytes     []string `json:"values_bytes"`
	CompensatedSum  string   `json:"compensated_sum_bytes"`
	NaiveInt64Sum   string   `json:"naive_int64_sum_bytes"`
	Difference      string   `json:"difference_bytes"`
	ErrorUpperBytes string   `json:"error_upper_bytes"`
	Method          string   `json:"method"`
}

type weightedTermDiagnostic struct {
	Weight            string `json:"weight"`
	ValueUnits        string `json:"value_units"`
	NaiveRoundedUnits string `json:"naive_rounded_units"`
	Remainder         string `json:"division_remainder"`
}

type weightedSampleDiagnostic struct {
	Weights               []string                 `json:"weights"`
	ValueUnits            []string                 `json:"value_units"`
	TotalWeight           string                   `json:"total_weight"`
	ExactNumerator        string                   `json:"exact_weighted_numerator"`
	ExactRoundedUnits     string                   `json:"exact_rounded_units"`
	CompensatedUnits      string                   `json:"compensated_units"`
	NaiveUnits            string                   `json:"naive_per_term_units"`
	NaiveErrorNumerator   string                   `json:"naive_error_numerator"`
	NaiveErrorUpperUnits  string                   `json:"naive_error_upper_units"`
	CompensatedErrorUpper string                   `json:"compensated_error_upper_units"`
	Terms                 []weightedTermDiagnostic `json:"terms"`
}

type toleranceDiagnostic struct {
	Rule           string `json:"rule"`
	MarginExample  string `json:"margin_boundary_example"`
	CostExample    string `json:"cost_boundary_example"`
	StableTieBreak string `json:"stable_tie_break"`
}

func arithmeticDiagnostics() arithmeticDiagnosticsResponse {
	capacityValues := []*big.Int{
		mustBig("9007199254740993"), big.NewInt(1), big.NewInt(1), big.NewInt(1),
	}
	compensated := fixed.NewCompensatedSum()
	naive := new(big.Int)
	for _, v := range capacityValues {
		compensated.Add(v)
		naive.Add(naive, v)
	}
	capacityDiff := new(big.Int).Sub(compensated.Value(), naive)
	capacityDiff.Abs(capacityDiff)

	weights := []*big.Int{big.NewInt(1), big.NewInt(1), big.NewInt(1)}
	values := []*big.Int{big.NewInt(1), big.NewInt(1), big.NewInt(1000000)}
	weighted, _ := fixed.WeightedAverage(weights, values)
	terms := make([]weightedTermDiagnostic, 0, len(weighted.Terms))
	for _, term := range weighted.Terms {
		terms = append(terms, weightedTermDiagnostic{
			Weight: term.Weight.String(), ValueUnits: term.ValueUnits.String(),
			NaiveRoundedUnits: term.NaiveUnits.String(), Remainder: term.Remainder.String(),
		})
	}
	naiveUpper := new(big.Rat).SetFrac(big.NewInt(int64(len(weights))), big.NewInt(2)).FloatString(6)

	return arithmeticDiagnosticsResponse{
		Representation: representationDiagnostic{
			RatioScale: fixed.Scale, IntegerDomain: "math/big.Int fixed numerator and denominator",
			Rounding: "half-away-from-zero", FloatingPointUsed: false,
			Accumulation: "exact big.Int compensated accumulator (integer-exact Kahan equivalent) plus division-residual carry for weighted averages",
		},
		CapacitySample: capacitySumDiagnostic{
			ValuesBytes: bigStrings(capacityValues), CompensatedSum: compensated.Value().String(),
			NaiveInt64Sum: naive.String(), Difference: capacityDiff.String(), ErrorUpperBytes: "0",
			Method: "Integer fixed-domain addition has zero representation loss; compensation remains zero and the quantifiable summation error upper bound is 0 bytes.",
		},
		WeightedSample: weightedSampleDiagnostic{
			Weights: bigStrings(weights), ValueUnits: bigStrings(values), TotalWeight: weighted.TotalWeight.String(),
			ExactNumerator: weighted.ExactWeightedNumerator.String(), ExactRoundedUnits: weighted.Units.String(),
			CompensatedUnits: weighted.Units.String(), NaiveUnits: weighted.NaiveUnits.String(),
			NaiveErrorNumerator: weighted.NaiveErrorNumerator.String(), NaiveErrorUpperUnits: naiveUpper,
			CompensatedErrorUpper: "0.500000", Terms: terms,
		},
		ToleranceContract: toleranceDiagnostic{
			Rule:           "reject margin only when margin < floor - ceil(|floor|*relative_tolerance); reject cost only when cost > budget + ceil(budget*relative_tolerance); boundaries are accepted",
			MarginExample:  "floor=0.250000, tolerance=0.004000 => effective lower=0.249000",
			CostExample:    "budget=1000000, tolerance=0.004000 => effective upper=1004000",
			StableTieBreak: "candidate evaluations sort by snapshot_id lexicographically; the first accepting ID wins",
		},
	}
}

func mustBig(s string) *big.Int {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic("invalid diagnostic integer " + strconv.Quote(s))
	}
	return n
}

func bigStrings(values []*big.Int) []string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = v.String()
	}
	return out
}
