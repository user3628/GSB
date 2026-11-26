package agg

import (
	"math"
	"strconv"
	"strings"
)

func addInt64(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, false
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}

func mulInt64(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	result := a * b
	if result/b != a {
		return 0, false
	}
	return result, true
}

func pow10Int64(scale int) int64 {
	if scale < 0 || scale > 18 {
		return 0
	}
	result := int64(1)
	for i := 0; i < scale; i++ {
		next, ok := mulInt64(result, 10)
		if !ok {
			return 0
		}
		result = next
	}
	return result
}

func Decimal(raw int64, scale int) string {
	if scale < 0 || scale > 18 {
		return ""
	}
	text := strconv.FormatInt(raw, 10)
	if scale == 0 {
		return text
	}
	negative := strings.HasPrefix(text, "-")
	digits := strings.TrimPrefix(text, "-")
	if len(digits) <= scale {
		fraction := strings.Repeat("0", scale-len(digits)) + digits
		if negative {
			return "-0." + fraction
		}
		return "0." + fraction
	}
	position := len(digits) - scale
	integer := digits[:position]
	fraction := digits[position:]
	if negative {
		return "-" + integer + "." + fraction
	}
	return integer + "." + fraction
}

func intToString(v int64) string {
	if v == 0 {
		return "0"
	}
	negative := v < 0
	if negative {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
