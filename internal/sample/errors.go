package sample

// 业务/系统错误码。以 E_ 前缀命名，贯穿解析、校验、调度与恢复全流程。
const (
	E_OK                      = "E_OK"
	E_POLICY_MISSING          = "E_POLICY_MISSING"
	E_HASH_ALGO_UNSUPPORTED   = "E_HASH_ALGO_UNSUPPORTED"
	E_EVENT_SEQ_GAP           = "E_EVENT_SEQ_GAP"
	E_CHAIN_BROKEN            = "E_CHAIN_BROKEN"
	E_MITM_SUSPECTED          = "E_MITM_SUSPECTED"
	E_SNI_MISMATCH            = "E_SNI_MISMATCH"
	E_PROTOCOL_NOT_ALLOWED    = "E_PROTOCOL_NOT_ALLOWED"
	E_SNAPSHOT_VERSION        = "E_SNAPSHOT_VERSION"
	E_EVENT_BUDGET            = "E_EVENT_BUDGET"
	E_GLOBAL_BUDGET           = "E_GLOBAL_BUDGET"
	E_POLICY_UNCHANGED        = "E_POLICY_UNCHANGED"
	E_POLICY_VERSION_CONFLICT = "E_POLICY_VERSION_CONFLICT"
)

// BizError 携带稳定业务码与可读细节，便于报告逐条复核。
type BizError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *BizError) Error() string {
	if e == nil {
		return E_OK
	}
	return e.Code + ": " + e.Message
}

// NewBizError 构造业务错误。
func NewBizError(code, message string) *BizError {
	return &BizError{Code: code, Message: message}
}

// CodeOf 提取错误的业务码，nil 返回 E_OK。
func CodeOf(err error) string {
	if err == nil {
		return E_OK
	}
	if be, ok := err.(*BizError); ok {
		return be.Code
	}
	return err.Error()
}
