package format

import (
	"fmt"

	"gsb/internal/gwerr"
)

// CompatRule 描述某个 (格式, 版本) 的兼容裁决规则。
type CompatRule struct {
	Verdict string
	Reason  string
}

// CompatMatrix 是显式声明的兼容矩阵。
var CompatMatrix = map[string]map[int]CompatRule{
	FormatBinary: {
		1: {VerdictDowngrade, "v1 缺工具链版本且显存为低精度 float32 GiB，按声明默认值补齐"},
		2: {VerdictAccept, "原生版本"},
	},
	FormatJSONL: {
		1: {VerdictDowngrade, "v1 缺工具链版本/并发上限，显存为低精度浮点 GiB，按声明默认值补齐"},
		2: {VerdictAccept, "原生版本"},
	},
}

// 降级默认值（显式声明）
const (
	DefaultToolchain   = "tc-0.0.0-legacy"
	DefaultConcurrency = 4
)

// Detect 检测输入格式与格式版本。
func Detect(data []byte) (formatID string, version int, err error) {
	if h, ok := DetectBinary(data); ok {
		return FormatBinary, h.Version, nil
	}
	if v, ok := DetectJSONL(data); ok {
		return FormatJSONL, v, nil
	}
	return "", 0, gwerr.New(gwerr.EFormatUnsupported, "无法识别的输入格式", nil)
}

// Adjudicate 按兼容矩阵裁决。
func Adjudicate(formatID string, version int) (CompatRule, error) {
	versions, ok := CompatMatrix[formatID]
	if !ok {
		return CompatRule{}, gwerr.New(gwerr.EFormatUnsupported, "未知格式: "+formatID, nil)
	}
	rule, ok := versions[version]
	if !ok {
		return CompatRule{}, gwerr.New(gwerr.EFormatUnsupported,
			fmt.Sprintf("格式 %s 版本 %d 不在兼容矩阵中（高版本含未知必填字段）", formatID, version),
			map[string]any{"format": formatID, "version": version})
	}
	if rule.Verdict == VerdictReject {
		return rule, gwerr.New(gwerr.EFormatUnsupported, rule.Reason,
			map[string]any{"format": formatID, "version": version})
	}
	return rule, nil
}

// Import 执行完整导入管线：检测 → 裁决 → 解码 → 归一化 → 报告。
// truncRule 由调用方声明（TruncTruncate / TruncRound）。
func Import(data []byte, truncRule string) (*ImportReport, error) {
	formatID, version, err := Detect(data)
	if err != nil {
		return nil, err
	}
	rule, err := Adjudicate(formatID, version)
	if err != nil {
		return nil, err
	}
	rep := &ImportReport{Format: formatID, FormatVersion: version, Verdict: rule.Verdict}
	switch formatID {
	case FormatBinary:
		if err := importBinary(data, truncRule, rep); err != nil {
			return nil, err
		}
	case FormatJSONL:
		if err := importJSONL(data, version, truncRule, rep); err != nil {
			return nil, err
		}
	}
	return rep, nil
}

func importBinary(data []byte, truncRule string, rep *ImportReport) error {
	h, _ := DetectBinary(data)
	raws, err := decodeBinaryRecords(data, h)
	if err != nil {
		return err
	}
	for _, raw := range raws {
		rec := RouteRecord{
			ModelID:     raw.modelID,
			Endpoint:    raw.endpoint,
			BaseTimeMs:  raw.baseTimeMs,
			RetentionMs: raw.retentionMs,
			LastWriteMs: raw.lastWriteMs,
		}
		// 容量归一化
		switch {
		case raw.hasVRAMBytes:
			rec.VRAMBytes = raw.vramBytes
		case raw.hasVRAMGiB:
			v, err := VRAMFromFloatGiB(raw.vramGiB)
			if err != nil {
				return err
			}
			rec.VRAMBytes = v
			rep.Downgrades = append(rep.Downgrades, DowngradeDetail{
				ModelID: rec.ModelID, Field: "vram_budget",
				Reason: "v1 低精度 float32 GiB 量纲", Default: fmt.Sprintf("%gGi", raw.vramGiB)})
		}
		// 缺省字段补齐
		if raw.hasConcurrency {
			rec.Concurrency = raw.concurrency
		} else {
			rec.Concurrency = DefaultConcurrency
			rep.Downgrades = append(rep.Downgrades, DowngradeDetail{
				ModelID: rec.ModelID, Field: "concurrency_cap",
				Reason: "旧版本缺省", Default: DefaultConcurrency})
		}
		if raw.hasToolchain {
			rec.Toolchain = raw.toolchain
		} else {
			rec.Toolchain = DefaultToolchain
			rep.Downgrades = append(rep.Downgrades, DowngradeDetail{
				ModelID: rec.ModelID, Field: "toolchain_version",
				Reason: "旧版本缺省", Default: DefaultToolchain})
		}
		rec.ExpiresAtMs = AbsoluteExpiry(rec.BaseTimeMs, rec.RetentionMs)
		rep.Records = append(rep.Records, rec)
	}
	return nil
}

func importJSONL(data []byte, version int, truncRule string, rep *ImportReport) error {
	raws, err := decodeJSONLRecords(data, version)
	if err != nil {
		return err
	}
	normTime := func(modelID, field string, us int64) (int64, error) {
		ms, dropped, err := NormalizeTimeUs(us, truncRule)
		if err != nil {
			return 0, err
		}
		rep.Rounding = append(rep.Rounding, RoundingDetail{
			ModelID: modelID, Field: field, DroppedBits: dropped, SignedError: dropped})
		rep.CumulativeRoundUs += dropped
		return ms, nil
	}
	for _, raw := range raws {
		rec := RouteRecord{ModelID: raw.ModelID, Endpoint: raw.Endpoint}
		// 容量归一化（Mi/Gi → 字节，溢出检查）
		switch {
		case raw.VRAMBudget != "":
			v, err := ParseVRAM(raw.VRAMBudget)
			if err != nil {
				return err
			}
			rec.VRAMBytes = v
		case raw.VRAMGiB != nil:
			v, err := VRAMFromFloatGiB(*raw.VRAMGiB)
			if err != nil {
				return err
			}
			rec.VRAMBytes = v
			rep.Downgrades = append(rep.Downgrades, DowngradeDetail{
				ModelID: rec.ModelID, Field: "vram_budget",
				Reason: "v1 低精度浮点 GiB 量纲", Default: fmt.Sprintf("%gGi", *raw.VRAMGiB)})
		default:
			return gwerr.New(gwerr.EBadRequest, "记录缺少显存预算: "+rec.ModelID, nil)
		}
		// 缺省字段补齐
		if raw.ConcurrencyCap != nil {
			rec.Concurrency = *raw.ConcurrencyCap
		} else {
			rec.Concurrency = DefaultConcurrency
			rep.Downgrades = append(rep.Downgrades, DowngradeDetail{
				ModelID: rec.ModelID, Field: "concurrency_cap",
				Reason: "旧版本缺省", Default: DefaultConcurrency})
		}
		if raw.ToolchainVersion != nil {
			rec.Toolchain = *raw.ToolchainVersion
		} else {
			rec.Toolchain = DefaultToolchain
			rep.Downgrades = append(rep.Downgrades, DowngradeDetail{
				ModelID: rec.ModelID, Field: "toolchain_version",
				Reason: "旧版本缺省", Default: DefaultToolchain})
		}
		// 时间归一化（微秒 → 毫秒）
		if raw.BaseTimeUs != nil {
			if rec.BaseTimeMs, err = normTime(rec.ModelID, "base_time", *raw.BaseTimeUs); err != nil {
				return err
			}
		}
		if raw.RetentionUs != nil {
			if rec.RetentionMs, err = normTime(rec.ModelID, "retention", *raw.RetentionUs); err != nil {
				return err
			}
		}
		if raw.LastWriteUs != nil {
			if rec.LastWriteMs, err = normTime(rec.ModelID, "last_write", *raw.LastWriteUs); err != nil {
				return err
			}
		}
		// 相对保留期 → 绝对过期时刻
		rec.ExpiresAtMs = AbsoluteExpiry(rec.BaseTimeMs, rec.RetentionMs)
		rep.Records = append(rep.Records, rec)
	}
	return nil
}

// RoundtripEqual 对两种格式互转后的记录做逐字段语义等价校验。
func RoundtripEqual(a, b []RouteRecord) error {
	if len(a) != len(b) {
		return fmt.Errorf("记录数不一致: %d vs %d", len(a), len(b))
	}
	am := make(map[string]RouteRecord, len(a))
	for _, r := range a {
		am[r.ModelID] = r
	}
	for _, rb := range b {
		ra, ok := am[rb.ModelID]
		if !ok {
			return fmt.Errorf("记录 %s 缺失", rb.ModelID)
		}
		if ra != rb {
			return fmt.Errorf("记录 %s 逐字段校验失败: %+v != %+v", rb.ModelID, ra, rb)
		}
	}
	return nil
}
