package domain

// BizCode is the stable business error code surface of the workbench.
type BizCode string

const (
	CodeOK                 BizCode = "E_OK"
	CodeSampleParse        BizCode = "E_SAMPLE_PARSE"
	CodeScoreOverflow      BizCode = "E_SCORE_OVERFLOW"
	CodePrivEscalation     BizCode = "E_PRIV_ESCALATION"
	CodeIndeterminateRange BizCode = "E_INDETERMINATE_RANGE"
	CodeOrphanRef          BizCode = "E_ORPHAN_REF"
	CodeSnapshotVersion    BizCode = "E_SNAPSHOT_VERSION"
	CodeSessionBudget      BizCode = "E_SESSION_BUDGET"
	CodeInjection          BizCode = "E_INJECTION"
	CodeInternal           BizCode = "E_INTERNAL"
)

// CodeError pairs a stable business code with a deterministic message and the
// source offset (byte position in the offending input) when applicable.
type CodeError struct {
	Code   BizCode
	Msg    string
	Offset int64
}

func (e *CodeError) Error() string {
	if e.Offset >= 0 {
		return string(e.Code) + ": " + e.Msg + " (source_offset=" + itoa(e.Offset) + ")"
	}
	return string(e.Code) + ": " + e.Msg
}

// NewError builds a business error without a source offset.
func NewError(code BizCode, msg string) *CodeError {
	return &CodeError{Code: code, Msg: msg, Offset: -1}
}

// NewOffsetError builds a business error carrying the offending source offset.
func NewOffsetError(code BizCode, msg string, offset int64) *CodeError {
	return &CodeError{Code: code, Msg: msg, Offset: offset}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
