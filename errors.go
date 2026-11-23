package configimpact

import (
	"errors"
	"fmt"
)

const (
	ErrDocInvalid       = "E_DOC_INVALID"
	ErrStaleBase        = "E_STALE_BASE"
	ErrStaleRead        = "E_STALE_READ"
	ErrConflictingPatch = "E_CONFLICTING_PATCH"
	ErrPatchInvalid     = "E_PATCH_INVALID"
	ErrPatchInProgress  = "E_PATCH_IN_PROGRESS"
	ErrPatchAborted     = "E_PATCH_ABORTED"
	ErrNotFound         = "E_NOT_FOUND"
	ErrStorage          = "E_STORAGE"
	ErrInternal         = "E_INTERNAL"
)

// BusinessError is returned for all expected engine failures. Details are
// intentionally JSON-friendly so callers can expose them directly.
type BusinessError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *BusinessError) Error() string {
	if e == nil {
		return ""
	}
	if len(e.Details) == 0 {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Details)
}

func businessError(code, format string, args ...any) *BusinessError {
	return &BusinessError{Code: code, Message: fmt.Sprintf(format, args...)}
}

func withDetails(err *BusinessError, details map[string]any) *BusinessError {
	err.Details = details
	return err
}

// CodeOf extracts a business error code. Non-business errors return "".
func CodeOf(err error) string {
	var be *BusinessError
	if errors.As(err, &be) {
		return be.Code
	}
	return ""
}

// IsCode reports whether err is a BusinessError with code.
func IsCode(err error, code string) bool {
	return CodeOf(err) == code
}

func storageError(err error) error {
	if err == nil {
		return nil
	}
	return &BusinessError{Code: ErrStorage, Message: err.Error()}
}
