package domain

import "fmt"

const (
	CodeRecordCorrupt      = "E_RECORD_CORRUPT"
	CodeFormatUnsupported  = "E_FORMAT_UNSUPPORTED"
	CodeIdentityConflict   = "E_IDENTITY_CONFLICT"
	CodeChainInvalid       = "E_CHAIN_INVALID"
	CodeVersionRetired     = "E_VERSION_RETIRED"
	CodeInvalidTransition  = "E_INVALID_TRANSITION"
	CodeNotFound           = "E_NOT_FOUND"
	CodeConflict           = "E_CONFLICT"
	CodeInternal           = "E_INTERNAL"
	CodeWriterBusy         = "E_WRITER_BUSY"
	CodeRecoveryIncomplete = "E_RECOVERY_INCOMPLETE"
)

type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
	cause   error
}

func NewError(code, message string) *Error {
	return &Error{Code: code, Message: message}
}

func Errorf(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func WrapError(code string, err error, message string) *Error {
	return &Error{Code: code, Message: message, cause: err}
}

func (e *Error) WithContext(key string, value any) *Error {
	if e.Context == nil {
		e.Context = map[string]any{}
	}
	e.Context[key] = value
	return e
}

func (e *Error) WithContextMap(values map[string]any) *Error {
	if e.Context == nil {
		e.Context = map[string]any{}
	}
	for key, value := range values {
		e.Context[key] = value
	}
	return e
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.cause }

func AsError(err error) (*Error, bool) {
	for err != nil {
		if pe, ok := err.(*Error); ok {
			return pe, true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return nil, false
		}
		err = u.Unwrap()
	}
	return nil, false
}
