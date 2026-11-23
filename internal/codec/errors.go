package codec

import "fmt"

const (
	CodeFormatCorrupt     = "E_FORMAT_CORRUPT"
	CodeFormatUnsupported = "E_FORMAT_UNSUPPORTED"
	CodeSnapshotDigest    = "E_FORMAT_CORRUPT"
)

// Error is a deterministic, position-aware codec error.
type Error struct {
	Code          string   `json:"code"`
	Message       string   `json:"message"`
	Offset        int64    `json:"offset"`
	Line          int      `json:"line,omitempty"`
	RecordIndex   int      `json:"recordIndex,omitempty"`
	UnknownFields []string `json:"unknownFields,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s at byte %d: %s", e.Code, e.Offset, e.Message)
}

func corrupt(offset int64, format string, args ...any) *Error {
	return &Error{Code: CodeFormatCorrupt, Message: fmt.Sprintf(format, args...), Offset: offset}
}
