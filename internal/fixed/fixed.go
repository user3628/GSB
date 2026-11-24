// Package fixed implements integer fixed-point arithmetic (scale 1e-6)
// and 128-bit address views. No floating point is used anywhere.
package fixed

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"net/netip"
	"strings"

	"fwadjudge/internal/codes"
)

// Scale is the fixed-point unit: 1 unit = 1e-6.
const Scale int64 = 1_000_000

// ErrScale is the finer unit (1e-12) used by the error ledger so that
// per-event truncation errors stay exactly representable as integers.
const ErrScale int64 = 1_000_000_000_000

// Fixed is a signed fixed-point number with scale 1e-6.
type Fixed int64

// ParseDecimal parses a decimal string into a Fixed value without any
// floating-point arithmetic. It returns the value and the truncation
// error expressed in ErrScale (1e-12) units; the error is 0 when the
// input is exactly representable. Digits beyond the 6th fractional
// digit are truncated toward zero and accounted in the returned error.
func ParseDecimal(s string) (Fixed, int64, error) {
	orig := s
	neg := false
	if s != "" && (s[0] == '-' || s[0] == '+') {
		neg = s[0] == '-'
		s = s[1:]
	}
	if s == "" {
		return 0, 0, codes.New(codes.ERuleInvalid, "empty decimal literal")
	}
	intStr := s
	fracStr := ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intStr = s[:i]
		fracStr = s[i+1:]
		if strings.IndexByte(fracStr, '.') >= 0 {
			return 0, 0, codes.New(codes.ERuleInvalid, "invalid decimal: "+orig)
		}
	}
	if intStr == "" {
		intStr = "0"
	}
	for _, c := range intStr + fracStr {
		if c < '0' || c > '9' {
			return 0, 0, codes.New(codes.ERuleInvalid, "invalid decimal: "+orig)
		}
	}
	var intPart int64
	for i := 0; i < len(intStr); i++ {
		d := int64(intStr[i] - '0')
		if intPart > (math.MaxInt64-d)/10 {
			return 0, 0, codes.New(codes.EFixedOverflow, "decimal integer part overflow: "+orig)
		}
		intPart = intPart*10 + d
	}
	var fracUnits int64
	main := len(fracStr)
	if main > 6 {
		main = 6
	}
	for i := 0; i < main; i++ {
		fracUnits = fracUnits*10 + int64(fracStr[i]-'0')
	}
	for i := main; i < 6; i++ {
		fracUnits *= 10
	}
	var errUnits int64
	if len(fracStr) > 6 {
		extra := fracStr[6:]
		allZero := true
		for i := 0; i < len(extra); i++ {
			if extra[i] != '0' {
				allZero = false
				break
			}
		}
		if !allZero {
			k := len(extra)
			if k <= 6 {
				var ev int64
				for i := 0; i < k; i++ {
					ev = ev*10 + int64(extra[i]-'0')
				}
				for i := 0; i < 6-k; i++ {
					ev *= 10
				}
				errUnits = ev
			} else {
				var ev int64
				for i := 0; i < 6; i++ {
					ev = ev*10 + int64(extra[i]-'0')
				}
				errUnits = ev + 1 // ceil: strictly positive tail
			}
		}
	}
	if intPart > (math.MaxInt64-fracUnits)/Scale {
		return 0, 0, codes.New(codes.EFixedOverflow, "decimal overflow: "+orig)
	}
	v := intPart*Scale + fracUnits
	if neg {
		v = -v
	}
	return Fixed(v), errUnits, nil
}

// Mul multiplies two fixed-point values, returning the truncated
// (toward zero) product and the truncation remainder in ErrScale units.
// It returns E_FIXED_OVERFLOW when the exact product does not fit.
func Mul(a, b Fixed) (Fixed, int64, error) {
	if a == math.MinInt64 || b == math.MinInt64 {
		return 0, 0, codes.New(codes.EFixedOverflow, "fixed mul overflow (MinInt64 operand)")
	}
	neg := (a < 0) != (b < 0)
	ua := absU(int64(a))
	ub := absU(int64(b))
	hi, lo := bits.Mul64(ua, ub)
	limHi, limLo := bits.Mul64(uint64(math.MaxInt64), uint64(Scale))
	if hi > limHi || (hi == limHi && lo > limLo) {
		return 0, 0, codes.New(codes.EFixedOverflow,
			fmt.Sprintf("fixed mul overflow: %s * %s", a.String(), b.String()))
	}
	q, r := bits.Div64(hi, lo, uint64(Scale))
	res := int64(q)
	if neg {
		res = -res
	}
	return Fixed(res), int64(r), nil
}

func absU(v int64) uint64 {
	if v < 0 {
		return uint64(-v)
	}
	return uint64(v)
}

// String renders the value with exactly 6 fractional digits.
func (f Fixed) String() string {
	v := int64(f)
	neg := v < 0
	if neg {
		v = -v
	}
	s := fmt.Sprintf("%d.%06d", v/Scale, v%Scale)
	if neg {
		return "-" + s
	}
	return s
}

// PortToFixed converts an integer port to its fixed-point view (exact).
func PortToFixed(p int) Fixed { return Fixed(int64(p) * Scale) }

// U128 is a 128-bit unsigned integer view of an IP address.
type U128 struct {
	Hi uint64
	Lo uint64
}

func (a U128) Cmp(b U128) int {
	if a.Hi != b.Hi {
		if a.Hi < b.Hi {
			return -1
		}
		return 1
	}
	if a.Lo != b.Lo {
		if a.Lo < b.Lo {
			return -1
		}
		return 1
	}
	return 0
}

func (a U128) String() string { return fmt.Sprintf("%016x:%016x", a.Hi, a.Lo) }

func addrToU128(a netip.Addr) U128 {
	b := a.As16()
	return U128{binary.BigEndian.Uint64(b[:8]), binary.BigEndian.Uint64(b[8:])}
}

// ParseIP parses a single IP address into its 128-bit view.
func ParseIP(s string) (U128, error) {
	a, err := netip.ParseAddr(s)
	if err != nil {
		return U128{}, codes.New(codes.ERuleInvalid, "invalid ip: "+s)
	}
	return addrToU128(a), nil
}

// ParseCIDR converts a CIDR into the [start, end] 128-bit unsigned
// integer view. IPv4 prefixes are viewed in the IPv4-mapped range.
func ParseCIDR(s string) (U128, U128, error) {
	p, err := netip.ParsePrefix(s)
	if err != nil {
		return U128{}, U128{}, codes.New(codes.ERuleInvalid, "invalid cidr: "+s)
	}
	p = p.Masked()
	bitsN := p.Bits()
	if p.Addr().Is4() {
		bitsN += 96
	}
	start := addrToU128(p.Addr())
	end := start
	host := 128 - bitsN
	switch {
	case host >= 64:
		end.Lo = ^uint64(0)
		end.Hi |= ^uint64(0) >> uint(128-host)
	case host > 0:
		end.Lo |= ^uint64(0) >> uint(64-host)
	}
	return start, end, nil
}
