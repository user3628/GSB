package numeric

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"mpckpt/internal/model"
)

const (
	FixedScale              int64 = 1_000_000
	DefaultElementsPerShard       = 32
	RoundingMode                  = "ieee754_binary16_nearest_even"
)

// FixedParams derives an immutable local parameter vector from a digest. No
// external tensor file is read, which keeps every verification run reproducible.
func FixedParams(digest string, count int) []int64 {
	if count <= 0 {
		count = DefaultElementsPerShard
	}
	values := make([]int64, count)
	for i := 0; i < count; i++ {
		sum := sha256.Sum256([]byte(fmt.Sprintf("mpckpt-param-v1:%s:%d", digest, i)))
		raw := binary.LittleEndian.Uint64(sum[:8])
		values[i] = int64(raw%1_800_001) - 900_000
	}
	return values
}

func roundFixed(value float64) int64 {
	if value >= 0 {
		return int64(math.Floor(value + 0.5))
	}
	return int64(math.Ceil(value - 0.5))
}

func roundRightEven(value uint32, shift uint) uint32 {
	if shift == 0 {
		return value
	}
	if shift >= 32 {
		if shift == 32 && value == 0x80000000 {
			return 0
		}
		return 0
	}
	truncated := value >> shift
	remainder := value & ((uint32(1) << shift) - 1)
	half := uint32(1) << (shift - 1)
	if remainder > half || (remainder == half && truncated&1 == 1) {
		truncated++
	}
	return truncated
}

func float32ToHalfBits(value float32) uint16 {
	bits := math.Float32bits(value)
	sign := uint16(bits>>16) & 0x8000
	exponent := int32((bits >> 23) & 0xff)
	mantissa := bits & 0x7fffff
	if exponent == 0xff {
		if mantissa == 0 {
			return sign | 0x7c00
		}
		result := sign | 0x7e00
		if mantissa&0x400000 != 0 {
			result |= 0x0200
		}
		return result
	}
	unbiased := exponent - 127
	if unbiased > 15 {
		return sign | 0x7c00
	}
	if unbiased < -25 {
		return sign
	}
	if unbiased < -14 {
		significand := mantissa | 0x800000
		shift := uint(-unbiased - 1)
		rounded := roundRightEven(significand, shift)
		if rounded >= 0x400 {
			return sign | 0x0400
		}
		return sign | uint16(rounded)
	}
	rounded := roundRightEven(mantissa, 13)
	halfExponent := uint16(unbiased + 15)
	if rounded == 0x400 {
		rounded = 0
		halfExponent++
	}
	if halfExponent >= 31 {
		return sign | 0x7c00
	}
	return sign | halfExponent<<10 | uint16(rounded)
}

func halfBitsToFloat32(half uint16) float32 {
	sign := uint32(half&0x8000) << 16
	exponent := int32((half >> 10) & 0x1f)
	mantissa := uint32(half & 0x03ff)
	if exponent == 0 {
		if mantissa == 0 {
			return math.Float32frombits(sign)
		}
		normalizedExponent := int32(-14)
		for mantissa&0x0400 == 0 {
			mantissa <<= 1
			normalizedExponent--
		}
		mantissa &= 0x03ff
		bits := sign | uint32(normalizedExponent+127)<<23 | mantissa<<13
		return math.Float32frombits(bits)
	}
	if exponent == 31 {
		return math.Float32frombits(sign | 0x7f800000 | mantissa<<13)
	}
	bits := sign | uint32(exponent-15+127)<<23 | mantissa<<13
	return math.Float32frombits(bits)
}

func QuantizeHalf(value float32) float32 {
	return halfBitsToFloat32(float32ToHalfBits(value))
}

type ElementDeviation struct {
	Index         int   `json:"index"`
	MasterFixed   int64 `json:"master_fixed"`
	RestoredFixed int64 `json:"restored_fixed"`
	AbsErrorFixed int64 `json:"abs_error_fixed"`
	RelErrorPPM   int64 `json:"rel_error_ppm"`
}

type ShardPrecisionReport struct {
	ShardID                 string             `json:"shard_id"`
	SourceDigest            string             `json:"source_digest,omitempty"`
	Restored                bool               `json:"restored"`
	Elements                []ElementDeviation `json:"elements"`
	MaxAbsErrorFixed        int64              `json:"max_abs_error_fixed"`
	MaxRelErrorPPM          int64              `json:"max_rel_error_ppm"`
	CumulativeRoundingError int64              `json:"cumulative_rounding_error_fixed"`
}

type PrecisionReport struct {
	RoundingMode            string                 `json:"rounding_mode"`
	FixedScale              int64                  `json:"fixed_scale"`
	FixedRounding           string                 `json:"fixed_rounding"`
	LossScaleFixed          int64                  `json:"loss_scale_fixed"`
	ElementsPerShard        int                    `json:"elements_per_shard"`
	Shards                  []ShardPrecisionReport `json:"shards"`
	MaxAbsErrorFixed        int64                  `json:"max_abs_error_fixed"`
	MaxRelErrorPPM          int64                  `json:"max_rel_error_ppm"`
	CumulativeRoundingError int64                  `json:"cumulative_rounding_error_fixed"`
	AllowedAbsErrorFixed    int64                  `json:"allowed_abs_error_fixed"`
	AllowedRelErrorPPM      int64                  `json:"allowed_rel_error_ppm"`
	WithinPolicy            bool                   `json:"within_policy"`
	ReportChecksum          string                 `json:"report_checksum"`
}

func Evaluate(shards []model.Shard, strategy model.StrategySnapshot, elements int) (PrecisionReport, *model.BizError) {
	if elements <= 0 {
		elements = DefaultElementsPerShard
	}
	lossScale := float64(strategy.LossScaleInitialFixed) / float64(FixedScale)
	report := PrecisionReport{
		RoundingMode:         RoundingMode,
		FixedScale:           FixedScale,
		FixedRounding:        "half_away_from_zero",
		LossScaleFixed:       strategy.LossScaleInitialFixed,
		ElementsPerShard:     elements,
		AllowedAbsErrorFixed: strategy.MaxAbsErrorFixed,
		AllowedRelErrorPPM:   strategy.MaxRelErrorPPM,
	}
	ordered := model.CopyShards(shards)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	for _, shard := range ordered {
		shardReport := ShardPrecisionReport{
			ShardID:      shard.ID,
			SourceDigest: shard.SourceDigest,
			Elements:     []ElementDeviation{},
		}
		if shard.Precision == model.PrecisionFP16 {
			shardReport.Restored = true
			masterValues := FixedParams(shard.SourceDigest, elements)
			for index, masterFixed := range masterValues {
				masterValue := float32(float64(masterFixed) / float64(FixedScale))
				scaled := masterValue / float32(lossScale)
				restored := float64(QuantizeHalf(scaled)) * lossScale
				restoredFixed := roundFixed(restored * float64(FixedScale))
				absError := restoredFixed - masterFixed
				if absError < 0 {
					absError = -absError
				}
				relPPM := int64(0)
				if masterFixed != 0 {
					relPPM = roundFixed(float64(absError) * float64(FixedScale) / math.Abs(float64(masterFixed)))
				}
				element := ElementDeviation{
					Index:         index,
					MasterFixed:   masterFixed,
					RestoredFixed: restoredFixed,
					AbsErrorFixed: absError,
					RelErrorPPM:   relPPM,
				}
				shardReport.Elements = append(shardReport.Elements, element)
				shardReport.CumulativeRoundingError += absError
				if absError > shardReport.MaxAbsErrorFixed {
					shardReport.MaxAbsErrorFixed = absError
				}
				if relPPM > shardReport.MaxRelErrorPPM {
					shardReport.MaxRelErrorPPM = relPPM
				}
			}
		}
		report.Shards = append(report.Shards, shardReport)
		report.CumulativeRoundingError += shardReport.CumulativeRoundingError
		if shardReport.MaxAbsErrorFixed > report.MaxAbsErrorFixed {
			report.MaxAbsErrorFixed = shardReport.MaxAbsErrorFixed
		}
		if shardReport.MaxRelErrorPPM > report.MaxRelErrorPPM {
			report.MaxRelErrorPPM = shardReport.MaxRelErrorPPM
		}
	}
	report.WithinPolicy = report.MaxAbsErrorFixed <= strategy.MaxAbsErrorFixed && report.MaxRelErrorPPM <= strategy.MaxRelErrorPPM
	report.ReportChecksum = checksum(report)
	if !report.WithinPolicy {
		shardID := ""
		for _, shard := range report.Shards {
			if shard.MaxAbsErrorFixed > strategy.MaxAbsErrorFixed || shard.MaxRelErrorPPM > strategy.MaxRelErrorPPM {
				shardID = shard.ShardID
				break
			}
		}
		return report, model.NewError(model.CodePrecisionDrift, "restored FP16 parameters exceed the precision policy", map[string]any{
			"shard_id":                  shardID,
			"max_abs_error_fixed":       report.MaxAbsErrorFixed,
			"max_rel_error_ppm":         report.MaxRelErrorPPM,
			"allowed_abs_error_fixed":   strategy.MaxAbsErrorFixed,
			"allowed_rel_error_ppm":     strategy.MaxRelErrorPPM,
			"cumulative_rounding_error": report.CumulativeRoundingError,
		})
	}
	return report, nil
}

func checksum(report PrecisionReport) string {
	report.ReportChecksum = ""
	raw, _ := json.Marshal(report)
	return model.SHA256Hex(raw)
}
