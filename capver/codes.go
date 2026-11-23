package capver

// Code 是组件对外暴露的稳定业务码。
type Code string

const (
	CodeOK              Code = "OK"
	CodeCapDenied       Code = "E_CAP_DENIED"       // 越权访问 / 白名单外子进程
	CodeCapLeak         Code = "E_CAP_LEAK"         // 能力泄漏：实际获得未声明令牌
	CodeHandleEscape    Code = "E_HANDLE_ESCAPE"    // 句柄继承逃逸
	CodeSnapshotVersion Code = "E_SNAPSHOT_VERSION" // 快照版本链跳号
	CodeProcessTimeout  Code = "E_PROCESS_TIMEOUT"  // 单进程运行超时
	CodeUnsupported     Code = "E_UNSUPPORTED"      // 当前平台不支持真实隔离
	CodePolicyInvalid   Code = "E_POLICY_INVALID"   // 策略声明非法
	CodeStore           Code = "E_STORE"            // 快照存储内部错误
	CodeSessionLimit    Code = "E_SESSION_LIMIT"    // 会话资源限制
)

// Error 携带稳定业务码的错误。
type Error struct {
	Code    Code
	Message string
	Cause   error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return string(e.Code) + ": " + e.Message + ": " + e.Cause.Error()
	}
	return string(e.Code) + ": " + e.Message
}

func (e *Error) Unwrap() error { return e.Cause }

func newError(code Code, msg string, cause error) *Error {
	return &Error{Code: code, Message: msg, Cause: cause}
}

// CodeOf 从 error 中提取业务码，非组件错误返回 E_STORE。
func CodeOf(err error) Code {
	if err == nil {
		return CodeOK
	}
	if ce, ok := err.(*Error); ok {
		return ce.Code
	}
	return CodeStore
}
