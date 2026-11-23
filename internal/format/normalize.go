package format

import (
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"strings"

	"gsb/internal/gwerr"
)

// 亚毫秒截断规则（由调用方声明）
const (
	TruncTruncate = "truncate"   // 直接截断（向零取整）
	TruncRound    = "round-half" // 四舍五入到最近毫秒
)

// NormalizeTimeUs 把微秒时间戳归一化为内部毫秒表示，
// 返回毫秒值与被丢弃的亚毫秒部分（微秒）。
func NormalizeTimeUs(us int64, rule string) (ms int64, dropped int64, err error) {
	switch rule {
	case TruncTruncate:
		ms = us / 1000
		dropped = us - ms*1000
	case TruncRound:
		ms = us / 1000
		rem := us - ms*1000
		if rem >= 500 {
			ms++
			dropped = rem - 1000 // 负值表示向上进位
		} else {
			dropped = rem
		}
	default:
		return 0, 0, gwerr.New(gwerr.EBadRequest, "未知的截断规则: "+rule, nil)
	}
	return ms, dropped, nil
}

// AbsoluteExpiry 依据记录自带基准时间把相对保留期换算为绝对过期时刻。
func AbsoluteExpiry(baseMs, retentionMs int64) int64 { return baseMs + retentionMs }

// ParseVRAM 把 "8192Mi" / "8Gi" / 纯字节数 归一化为字节整数，带溢出检查。
func ParseVRAM(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, gwerr.New(gwerr.EBadRequest, "显存预算为空", nil)
	}
	upper := strings.ToUpper(s)
	var mult uint64 = 1
	num := upper
	switch {
	case strings.HasSuffix(upper, "GI"):
		mult = 1 << 30
		num = strings.TrimSuffix(upper, "GI")
	case strings.HasSuffix(upper, "MI"):
		mult = 1 << 20
		num = strings.TrimSuffix(upper, "MI")
	case strings.HasSuffix(upper, "B"):
		num = strings.TrimSuffix(upper, "B")
	}
	if strings.Contains(num, ".") {
		f, err := strconv.ParseFloat(num, 64)
		if err != nil {
			return 0, gwerr.New(gwerr.EBadRequest, "显存预算解析失败: "+s, nil)
		}
		return VRAMFromFloatGiB(f / float64(mult) * float64(uint64(1)<<30))
	}
	v, err := strconv.ParseUint(num, 10, 64)
	if err != nil {
		return 0, gwerr.New(gwerr.EBadRequest, "显存预算解析失败: "+s, nil)
	}
	return CheckedMulBytes(v, mult, s)
}

// CheckedMulBytes 做量纲换算的溢出检查，越界返回 E_UNIT_OVERFLOW。
func CheckedMulBytes(v, mult uint64, orig string) (uint64, error) {
	hi, lo := bits.Mul64(v, mult)
	if hi != 0 {
		return 0, gwerr.New(gwerr.EUnitOverflow,
			fmt.Sprintf("显存预算 %s 换算为字节时溢出", orig),
			map[string]any{"input": orig})
	}
	return lo, nil
}

// VRAMFromFloatGiB 处理旧版低精度 float GiB 量纲，带溢出检查。
func VRAMFromFloatGiB(gib float64) (uint64, error) {
	if gib < 0 || math.IsNaN(gib) || math.IsInf(gib, 0) {
		return 0, gwerr.New(gwerr.EUnitOverflow, "非法的 GiB 显存值", map[string]any{"gib": gib})
	}
	bytes := gib * float64(uint64(1)<<30)
	if bytes >= float64(math.MaxUint64) {
		return 0, gwerr.New(gwerr.EUnitOverflow, "GiB 显存值换算为字节时溢出",
			map[string]any{"gib": gib})
	}
	return uint64(math.Round(bytes)), nil
}
