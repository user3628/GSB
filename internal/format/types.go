// Package format 实现路由表两种持久化格式的编解码、
// 版本兼容裁决与时间/容量归一化。
package format

// 格式标识
const (
	FormatBinary = "grt-bin"
	FormatJSONL  = "grt-jsonl"
)

// 当前系统原生支持的格式版本
const (
	NativeBinaryVersion = 2
	NativeJSONLVersion  = 2
)

// RouteRecord 是归一化后的内部路由记录。
// 时间统一为毫秒；显存统一为字节。
type RouteRecord struct {
	ModelID     string `json:"model_id"`
	Endpoint    string `json:"endpoint"`
	VRAMBytes   uint64 `json:"vram_bytes"`
	Concurrency uint32 `json:"concurrency_cap"`
	Toolchain   string `json:"toolchain_version"`
	BaseTimeMs  int64  `json:"base_time_ms"`  // 保留期基准时间（毫秒）
	RetentionMs int64  `json:"retention_ms"`  // 相对保留期（毫秒）
	ExpiresAtMs int64  `json:"expires_at_ms"` // 绝对过期时刻 = BaseTimeMs + RetentionMs
	LastWriteMs int64  `json:"last_write_ms"` // 最后写入时间（毫秒）
}

// 裁决结论
const (
	VerdictAccept    = "accept"
	VerdictDowngrade = "downgrade_accept"
	VerdictReject    = "reject"
)

// DowngradeDetail 记录一条降级明细。
type DowngradeDetail struct {
	ModelID string `json:"model_id"`
	Field   string `json:"field"`
	Reason  string `json:"reason"`
	Default any    `json:"default_applied"`
}

// RoundingDetail 记录一条记录的时间舍入明细。
type RoundingDetail struct {
	ModelID     string `json:"model_id"`
	Field       string `json:"field"`
	DroppedBits int64  `json:"dropped_sub_ms"` // 被丢弃的亚毫秒部分（微秒）
	SignedError int64  `json:"signed_error_us"`
}

// ImportReport 是导入/裁决的完整报告。
type ImportReport struct {
	Format            string            `json:"format"`
	FormatVersion     int               `json:"format_version"`
	Verdict           string            `json:"verdict"`
	Downgrades        []DowngradeDetail `json:"downgrades,omitempty"`
	Rounding          []RoundingDetail  `json:"rounding,omitempty"`
	CumulativeRoundUs int64             `json:"cumulative_rounding_error_us"`
	Records           []RouteRecord     `json:"records,omitempty"`
}
