package scheduler

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"localinfer/workbench/internal/domain"
)

func applyPrecision(batch *domain.BatchPlan, catalog *domain.ModelCatalog, opts domain.OrganizationOptions, cumulative *int64) error {
	truth := new(big.Int)
	for _, m := range batch.Members {
		cost := catalog.ByID[m.ModelID].CostPerSample
		term := new(big.Int).Mul(big.NewInt(m.AllocatedSamples), big.NewInt(cost))
		truth.Add(truth, term)
	}
	if !truth.IsInt64() {
		return domain.NewBizError(domain.CodePrecisionBreach, "exact truth latency overflows int64", map[string]any{
			"batch_id": batch.BatchID, "truth": truth.String(),
		})
	}
	truthUS := truth.Int64()
	unitUS := roundingUnitUS(opts.LatencyDecimals)
	probe := opts.Faults.PrecisionOffsetUS
	probed, err := checkedAdd(truthUS, probe)
	if err != nil {
		return domain.NewBizError(domain.CodePrecisionBreach, "precision probe overflows int64", map[string]any{"batch_id": batch.BatchID})
	}
	reportedUS := roundHalfAwayFromZero(probed, unitUS)
	baseRoundedUS := roundHalfAwayFromZero(truthUS, unitUS)
	roundingErr := baseRoundedUS - truthUS
	*cumulative += roundingErr
	absDev := absInt64(reportedUS - truthUS)
	relPPM := relativePPM(absDev, truthUS)
	ok := absDev <= opts.MaxAbsDeviationUS && relPPM <= opts.MaxRelativeDeviationPPM
	explanation := fmt.Sprintf(
		"truth is exact integer sum of allocated_samples*cost_per_sample_us; internal fixed-point is whole microseconds; reported value is rounded half away from zero at %s; injected probe offset=%dus; tolerances are |dev|<=%dus and rel<=%dppm",
		roundingPlace(opts.LatencyDecimals), probe, opts.MaxAbsDeviationUS, opts.MaxRelativeDeviationPPM,
	)
	if !ok {
		explanation += "; precision breach: at least one declared bound was exceeded"
	}
	batch.Precision = domain.PrecisionResult{
		TruthUS: truthUS, ReportedUS: reportedUS, ReportedMS: formatMS(reportedUS, opts.LatencyDecimals),
		RoundingPlace: roundingPlace(opts.LatencyDecimals), InternalFixedUS: batch.LatencyUS,
		CompensationUS: batch.CompensationUS, RoundingErrorUS: roundingErr,
		CumulativeRoundingErrUS: *cumulative, AbsoluteDeviationUS: absDev, RelativeDeviationPPM: relPPM,
		MaxAbsDeviationUS: opts.MaxAbsDeviationUS, MaxRelativeDeviationPPM: opts.MaxRelativeDeviationPPM,
		PrecisionOK: ok, Explanation: explanation,
	}
	return nil
}

func roundingUnitUS(decimals int) int64 {
	switch decimals {
	case 0:
		return 1000
	case 1:
		return 100
	case 2:
		return 10
	default:
		return 1
	}
}

func roundingPlace(decimals int) string {
	switch decimals {
	case 0:
		return "1ms"
	case 1:
		return "0.1ms"
	case 2:
		return "0.01ms"
	default:
		return "0.001ms"
	}
}

func roundHalfAwayFromZero(value, unit int64) int64 {
	if unit == 1 {
		return value
	}
	if value >= 0 {
		return ((value + unit/2) / unit) * unit
	}
	return -(((-value + unit/2) / unit) * unit)
}

func relativePPM(deviation, truth int64) int64 {
	if truth == 0 {
		if deviation == 0 {
			return 0
		}
		return 1<<63 - 1
	}
	z := new(big.Int).Mul(big.NewInt(deviation), big.NewInt(1_000_000))
	q, r := new(big.Int), new(big.Int)
	z.QuoRem(z, big.NewInt(truth), r)
	if new(big.Int).Lsh(r, 1).Cmp(big.NewInt(truth)) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	if !q.IsInt64() {
		return 1<<63 - 1
	}
	return q.Int64()
}

func formatMS(us int64, decimals int) string {
	sign := ""
	if us < 0 {
		sign = "-"
		us = -us
	}
	unit := roundingUnitUS(decimals)
	units := (us + unit/2) / unit
	scale := int64(1)
	for i := 0; i < decimals; i++ {
		scale *= 10
	}
	whole := units / scale
	frac := units % scale
	if decimals == 0 {
		return sign + strconv.FormatInt(whole, 10) + ".0"
	}
	return sign + strconv.FormatInt(whole, 10) + "." + fmt.Sprintf("%0*d", decimals, frac)
}

func ParseDecimalMS(s string) (int64, error) {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid millisecond decimal %q", s)
	}
	whole, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || whole < 0 {
		return 0, fmt.Errorf("invalid millisecond decimal %q", s)
	}
	if len(parts[1]) > 3 {
		return 0, fmt.Errorf("too many fractional digits in %q", s)
	}
	frac := parts[1]
	for len(frac) < 3 {
		frac += "0"
	}
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid millisecond decimal %q", s)
	}
	return whole*1000 + f, nil
}
