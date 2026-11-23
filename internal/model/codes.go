package model

// 业务错误码：覆盖解析、闭包核验与快照全流程。
const (
	// EEOF 是内部哨兵：读到记录边界上的正常结束，不是业务错误。
	EEOF                = "EOF"
	ETruncated          = "E_TRUNCATED"
	EBadMagic           = "E_BAD_MAGIC"
	ELengthOverflow     = "E_LENGTH_OVERFLOW"
	EDupEdge            = "E_DUP_EDGE"
	EEdgeDangling       = "E_EDGE_DANGLING"
	EEdgeMalformed      = "E_EDGE_MALFORMED"
	ERootMissing        = "E_ROOT_MISSING"
	EVersionDowngrade   = "E_VERSION_DOWNGRADE"
	ECycle              = "E_CYCLE"
	EConstraintConflict = "E_CONSTRAINT_CONFLICT"
	ESnapshotVersion    = "E_SNAPSHOT_VERSION"
	EItemBudget         = "E_ITEM_BUDGET"
	EDigestMismatch     = "E_DIGEST_MISMATCH"
)

// 路径标识：规模自适应双路径核验。
const (
	PathPrecise = "PRECISE_EDGE"
	PathBatched = "SORTED_BATCH"
)

// 固定采样标记：报告中的时间字段不使用真实时钟，保证字节级可复现。
const FixedSampleMarker = "SAMPLE@2026-01-01T00:00:00Z"
