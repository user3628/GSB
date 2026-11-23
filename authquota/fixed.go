package authquota

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// Scale is the fixed-point denominator used by every monetary value. Values
// are stored as signed int64 nano-units; no float type is used anywhere in
// the amount path.
const Scale int64 = 1_000_000_000

// Fixed is an amount represented as an integer number of 1e-9 units.
type Fixed int64

func (f Fixed) Int64() int64 { return int64(f) }

// String renders a Fixed value exactly with nine fractional digits.
func (f Fixed) String() string {
	n := int64(f)
	if n == 0 {
		return "0.000000000"
	}
	sign := ""
	var magnitude uint64
	if n < 0 {
		sign = "-"
		// Convert without overflowing math.MinInt64.
		magnitude = uint64(-(n + 1))
		magnitude++
	} else {
		magnitude = uint64(n)
	}
	whole := magnitude / uint64(Scale)
	frac := magnitude % uint64(Scale)
	return fmt.Sprintf("%s%d.%09d", sign, whole, frac)
}

func (f Fixed) MarshalJSON() ([]byte, error) { return json.Marshal(int64(f)) }

func (f *Fixed) UnmarshalJSON(data []byte) error {
	var n int64
	if err := json.Unmarshal(data, &n); err != nil {
		return fmt.Errorf("fixed amounts must be int64 nano-units: %w", err)
	}
	*f = Fixed(n)
	return nil
}

// ParseFixed parses an exact decimal amount without binary floating-point conversion.
func ParseFixed(text string) (Fixed, error) {
	s := strings.TrimSpace(text)
	if s == "" {
		return 0, bizError(CodeInvalidRequest, "empty fixed-point amount", nil)
	}
	sign := int64(1)
	if s[0] == '-' || s[0] == '+' {
		if s[0] == '-' {
			sign = -1
		}
		s = s[1:]
	}
	if s == "" {
		return 0, bizError(CodeInvalidRequest, "invalid fixed-point amount", nil)
	}
	parts := strings.Split(s, ".")
	if len(parts) > 2 || parts[0] == "" {
		return 0, bizError(CodeInvalidRequest, "invalid fixed-point amount", nil)
	}
	whole, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, bizError(CodeInvalidRequest, "fixed-point amount is out of range", err)
	}
	fracText := ""
	if len(parts) == 2 {
		fracText = parts[1]
	}
	if len(fracText) > 9 {
		return 0, bizError(CodePrecisionOverflow, "fixed-point amount has more than 9 decimal places", nil)
	}
	fracText += strings.Repeat("0", 9-len(fracText))
	var frac uint64
	if fracText != "" {
		frac, err = strconv.ParseUint(fracText, 10, 64)
		if err != nil {
			return 0, bizError(CodeInvalidRequest, "invalid fixed-point fraction", err)
		}
	}
	if whole > uint64(math.MaxInt64/Scale) {
		return 0, bizError(CodePrecisionOverflow, "fixed-point amount exceeds int64 nano-units", nil)
	}
	magnitude := whole*uint64(Scale) + frac
	if magnitude > uint64(math.MaxInt64) {
		return 0, bizError(CodePrecisionOverflow, "fixed-point amount exceeds int64 nano-units", nil)
	}
	if sign < 0 {
		return Fixed(-int64(magnitude)), nil
	}
	return Fixed(magnitude), nil
}

// ChargeQuote is the complete, auditable result of a billing calculation.
// TruncNum/TruncDen is the exact fractional nano-unit discarded by floor
// division. Every stored monetary field is an int64.
type ChargeQuote struct {
	Requests int64 `json:"requests"`
	UnitCost Fixed `json:"unit_cost_nano"`
	RateNum  int64 `json:"rate_numerator"`
	RateDen  int64 `json:"rate_denominator"`
	Charged  Fixed `json:"charged_nano"`
	TruncNum int64 `json:"truncation_numerator"`
	TruncDen int64 `json:"truncation_denominator"`
}

// ComputeCharge computes floor(requests * unitCost * rateNum / rateDen) using
// integer arithmetic only. Each intermediate int64 product is explicitly
// overflow checked before the final quotient is formed.
func ComputeCharge(requests int64, unitCost Fixed, rateNum, rateDen int64) (ChargeQuote, error) {
	if requests < 0 {
		return ChargeQuote{}, bizError(CodeInvalidRequest, "request count must be non-negative", nil)
	}
	if unitCost < 0 {
		return ChargeQuote{}, bizError(CodeInvalidRequest, "unit cost must be non-negative", nil)
	}
	if rateNum <= 0 || rateDen <= 0 {
		return ChargeQuote{}, bizError(CodeInvalidRequest, "rate numerator and denominator must be positive", nil)
	}

	// Use big.Int only as an exact guard around int64 multiplication. The
	// persisted and returned path remains integer fixed-point.
	product := new(big.Int).Mul(big.NewInt(requests), big.NewInt(int64(unitCost)))
	if product.Sign() < 0 || !product.IsInt64() {
		return ChargeQuote{}, bizError(CodePrecisionOverflow, "requests × unit cost overflows int64", nil)
	}
	product.Mul(product, big.NewInt(rateNum))
	if product.Sign() < 0 || !product.IsInt64() {
		return ChargeQuote{}, bizError(CodePrecisionOverflow, "rate multiplication overflows int64", nil)
	}
	divisor := big.NewInt(rateDen)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(product, divisor, remainder)
	if !quotient.IsInt64() {
		return ChargeQuote{}, bizError(CodePrecisionOverflow, "charged amount exceeds int64", nil)
	}
	return ChargeQuote{
		Requests: requests,
		UnitCost: unitCost,
		RateNum:  rateNum,
		RateDen:  rateDen,
		Charged:  Fixed(quotient.Int64()),
		TruncNum: remainder.Int64(),
		TruncDen: rateDen,
	}, nil
}

// TruncationReport aggregates all discarded fractional nano-units for a
// tenant. UpperBoundNano is a conservative integer bound (less than one nano
// per truncated entry). ExactResidual is an exact rational audit string.
type TruncationReport struct {
	Entries           int64  `json:"entries"`
	TruncatedEntries  int64  `json:"truncated_entries"`
	ChargedTotalNano  Fixed  `json:"charged_total_nano"`
	UpperBoundNano    int64  `json:"upper_bound_nano"`
	PerEntryBoundNano int64  `json:"per_entry_bound_nano"`
	ExactResidual     string `json:"exact_residual_nano_rational"`
}

type truncationRow struct {
	charged  int64
	truncNum int64
	truncDen int64
}

func aggregateTruncation(rows []truncationRow) (TruncationReport, error) {
	report := TruncationReport{Entries: int64(len(rows)), PerEntryBoundNano: 1, ExactResidual: "0"}
	residual := new(big.Rat)
	for _, row := range rows {
		if row.charged < 0 || report.ChargedTotalNano > Fixed(math.MaxInt64-row.charged) {
			return TruncationReport{}, bizError(CodePrecisionOverflow, "aggregate charged total overflows int64", nil)
		}
		report.ChargedTotalNano += Fixed(row.charged)
		if row.truncNum != 0 {
			if row.truncDen <= 0 || row.truncNum < 0 || row.truncNum >= row.truncDen {
				return TruncationReport{}, bizError(CodeInternal, "invalid persisted truncation residual", nil)
			}
			report.TruncatedEntries++
			residual.Add(residual, new(big.Rat).SetFrac64(row.truncNum, row.truncDen))
		}
	}
	if report.TruncatedEntries > 0 {
		report.UpperBoundNano = report.TruncatedEntries
	}
	if residual.Sign() != 0 {
		report.ExactResidual = residual.RatString()
	}
	return report, nil
}
