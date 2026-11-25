package units

import (
	"math/big"
	"sort"
	"strings"

	"configaudit/internal/fixedpoint"
)

// Unit declares how an enum/unit suffix maps to a base unit through an exact
// rational multiplier: value_base = value * Num / Den.
type Unit struct {
	Enum   string
	Suffix []string
	Num    int64
	Den    int64
}

// Table is the deterministic, built-in unit registry. Binary (IEC) and decimal
// (SI) byte units are distinct; "m" denotes milli (1/1000).
var Table = []Unit{
	{Enum: "m", Suffix: []string{"m"}, Num: 1, Den: 1000},
	{Enum: "B", Suffix: []string{"B", ""}, Num: 1, Den: 1},
	// Explicit IEC suffixes only; bare letters are reserved for SI so the two
	// generations cannot map the same text to different binary/decimal units.
	{Enum: "KiB", Suffix: []string{"KiB", "Ki"}, Num: 1024, Den: 1},
	{Enum: "MiB", Suffix: []string{"MiB", "Mi"}, Num: 1024 * 1024, Den: 1},
	{Enum: "GiB", Suffix: []string{"GiB", "Gi"}, Num: 1024 * 1024 * 1024, Den: 1},
	{Enum: "TiB", Suffix: []string{"TiB", "Ti"}, Num: 1024 * 1024 * 1024 * 1024, Den: 1},
	{Enum: "kB", Suffix: []string{"kB", "KB", "k"}, Num: 1000, Den: 1},
	{Enum: "MB", Suffix: []string{"MB", "M"}, Num: 1000 * 1000, Den: 1},
	{Enum: "GB", Suffix: []string{"GB", "G"}, Num: 1000 * 1000 * 1000, Den: 1},
}

var byEnum = func() map[string]Unit {
	m := make(map[string]Unit, len(Table))
	for _, u := range Table {
		m[u.Enum] = u
	}
	return m
}()

var bySuffix = func() map[string]Unit {
	m := map[string]Unit{}
	for _, u := range Table {
		for _, s := range u.Suffix {
			m[s] = u
		}
	}
	return m
}()

// Enums lists all known unit enums deterministically.
func Enums() []string {
	out := make([]string, 0, len(Table))
	for _, u := range Table {
		out = append(out, u.Enum)
	}
	sort.Strings(out)
	return out
}

// ByEnum resolves a structured unit enum.
func ByEnum(enum string) (Unit, bool) {
	u, ok := byEnum[enum]
	return u, ok
}

// Rat returns the exact multiplier to the base unit.
func (u Unit) Rat() *big.Rat { return big.NewRat(u.Num, u.Den) }

// Record is the normalized result shared by both description generations.
type Record struct {
	RawText        string           `json:"raw_text"`
	Generation     string           `json:"generation"` // v2 structured | v1 legacy
	UnitEnum       string           `json:"unit_enum"`
	Normalized     fixedpoint.Value `json:"normalized"`
	RoundingDigits int              `json:"rounding_digits"` // decimal positions dropped (<= scale)
	RoundingULP    int64            `json:"rounding_ulp"`    // 1 when rounding actually occurred
	CumErrorULP    int64            `json:"cumulative_error_ulp"`
	Priority       int              `json:"priority"`
	Channel        string           `json:"channel"`
	ChannelOrder   int              `json:"channel_order"`
}

// splitNumericSuffix separates the leading numeric token from the unit suffix,
// tolerating one optional whitespace separator (e.g. "1.5 GiB").
func splitNumericSuffix(text string) (num, suffix string) {
	t := strings.TrimSpace(text)
	i := 0
	for i < len(t) {
		c := t[i]
		if (c >= '0' && c <= '9') || c == '.' || c == '-' || c == '+' {
			i++
			continue
		}
		break
	}
	num = strings.TrimSpace(t[:i])
	suffix = strings.TrimSpace(t[i:])
	return num, suffix
}

// ParseLegacy parses old-generation text such as "512Mi", "1.5 GiB", "250m".
func ParseLegacy(raw string, scale int) (Record, error) {
	numText, suffix := splitNumericSuffix(raw)
	if numText == "" {
		return Record{}, fixedpoint.NewError(fixedpoint.EUnitInvalid, "legacy description %q has no numeric part", raw)
	}
	unit, ok := bySuffix[suffix]
	if !ok {
		return Record{}, fixedpoint.NewError(fixedpoint.EUnitInvalid, "unrecognized unit suffix %q in %q", suffix, raw)
	}
	return buildRecord(raw, "v1", unit, numText, scale, 0, "", 0)
}

// Structured is the new-generation structured description.
type Structured struct {
	Value    string `json:"value"`
	Unit     string `json:"unit"`
	Priority int    `json:"priority"`
	Channel  string `json:"channel"`
	Order    int    `json:"order"`
}

// ParseStructured parses new-generation fields.
func ParseStructured(raw string, s Structured, scale int) (Record, error) {
	unit, ok := byEnum[s.Unit]
	if !ok {
		return Record{}, fixedpoint.NewError(fixedpoint.EUnitInvalid, "unrecognized unit enum %q in %q", s.Unit, raw)
	}
	return buildRecord(raw, "v2", unit, s.Value, scale, s.Priority, s.Channel, s.Order)
}

// buildRecord normalizes value*unit into one internal fixed-point value at the
// target scale and reports the rounding position and accumulated error.
func buildRecord(raw, gen string, unit Unit, numText string, scale, priority int, channel string, order int) (Record, error) {
	// Exact rational value in base units.
	numRat, ok := new(big.Rat).SetString(numText)
	if !ok {
		return Record{}, fixedpoint.NewError(fixedpoint.EInvalidInput, "invalid numeric literal %q", numText)
	}
	exact := new(big.Rat).Mul(numRat, unit.Rat())

	// Integer path: express exact = Num/10^scale rounded into mantissa.
	scalePow, ok := fixedpoint.Pow10(scale)
	if !ok {
		return Record{}, fixedpoint.NewError(fixedpoint.ENumericOverflow, "scale power overflow")
	}
	targetNum := new(big.Rat).Mul(exact, big.NewRat(scalePow, 1))
	mant, overflow := ratRoundToInt64(targetNum)
	if overflow {
		return Record{}, fixedpoint.NewError(fixedpoint.ENumericOverflow, "normalized value of %q overflows int64 at scale %d", raw, scale)
	}
	rounded := fixedpoint.Value{Mantissa: mant, Scale: scale}

	// Rounding position: fractional decimal digits in the exact base value that
	// exceed the retained scale (bounded by scale).
	roundDigits := fractionalDigits(exact, scale)
	roundULP := int64(0)
	if exact.Cmp(rounded.Rat()) != 0 {
		roundULP = 1
	}
	return Record{
		RawText:        raw,
		Generation:     gen,
		UnitEnum:       unit.Enum,
		Normalized:     rounded,
		RoundingDigits: roundDigits,
		RoundingULP:    roundULP,
		CumErrorULP:    roundULP,
		Priority:       priority,
		Channel:        channel,
		ChannelOrder:   order,
	}, nil
}

// ratRoundToInt64 rounds half away from zero and reports int64 overflow.
func ratRoundToInt64(r *big.Rat) (int64, bool) {
	neg := r.Num().Sign() < 0
	abs := new(big.Rat).Abs(r)
	fl := new(big.Int).Quo(abs.Num(), abs.Denom())
	rem := new(big.Int).Mod(abs.Num(), abs.Denom())
	twice := new(big.Int).Lsh(rem, 1)
	if twice.Cmp(abs.Denom()) >= 0 {
		fl.Add(fl, big.NewInt(1))
	}
	max := big.NewInt(fixedpoint.MaxInt64)
	if fl.Cmp(max) > 0 {
		return 0, true
	}
	v := fl.Int64()
	if neg {
		if v == fixedpoint.MaxInt64 {
			return fixedpoint.MinInt64, false
		}
		v = -v
	}
	return v, false
}

// fractionalDigits reports how many decimal positions are dropped when exact
// is represented at the given scale.
func fractionalDigits(exact *big.Rat, scale int) int {
	scaled := new(big.Rat).Mul(exact, big.NewRat(1, 1))
	den := new(big.Int).Set(scaled.Denom())
	// Remove factors of 2 and 5 to count remaining decimal positions needed.
	digits := 0
	for digits < scale {
		two := new(big.Int).Quo(den, big.NewInt(2))
		if new(big.Int).Mul(two, big.NewInt(2)).Cmp(den) == 0 {
			den = two
			digits++
			continue
		}
		five := new(big.Int).Quo(den, big.NewInt(5))
		if new(big.Int).Mul(five, big.NewInt(5)).Cmp(den) == 0 {
			den = five
			digits++
			continue
		}
		break
	}
	if den.Cmp(big.NewInt(1)) != 0 && digits < scale {
		// Non-terminating fraction: all scale positions are potentially rounded.
		return scale
	}
	return digits
}
