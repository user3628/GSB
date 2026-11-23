package freshness

// JudgeTimestamp applies the documented inclusive tolerance window:
//
//	lower = -tolerance_ms, upper = +tolerance_ms
//
// offset == lower or offset == upper is accepted.
func JudgeTimestamp(requestTS, gatewayRef, toleranceMS int64) (offset int64, triggered string, code string, lower int64, upper int64) {
	offset = requestTS - gatewayRef
	lower = -toleranceMS
	upper = toleranceMS
	switch {
	case offset < lower:
		return offset, BoundaryLower, CodeTimestampTooOld, lower, upper
	case offset > upper:
		return offset, BoundaryUpper, CodeTimestampAhead, lower, upper
	default:
		return offset, BoundaryNone, CodeOK, lower, upper
	}
}
