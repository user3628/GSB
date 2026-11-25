package speakerdiar

import "fmt"

// Stable business error codes used by the domain API and local HTTP/CLI entry points.
const (
	CodeInputLength           = "E_INPUT_LENGTH"
	CodeRateMismatch          = "E_RATE_MISMATCH"
	CodeInvalidParam          = "E_INVALID_PARAM"
	CodeNumeric               = "E_NUMERIC"
	CodeSeparation            = "E_SEPARATION"
	CodeEmbedding             = "E_EMBEDDING"
	CodeTimeline              = "E_TIMELINE"
	CodeIncrementalMismatch   = "E_INCREMENTAL_MISMATCH"
	CodeStaleCheckpoint       = "E_STALE_CHECKPOINT"
	CodeSnapshot              = "E_SNAPSHOT"
	CodeCheckpointUnavailable = "E_CHECKPOINT"
)

// Error carries a stable code and the diagnostic coordinates required by the task.
type Error struct {
	Code        string         `json:"code"`
	Message     string         `json:"message"`
	ChunkID     string         `json:"chunk_id,omitempty"`
	Stage       string         `json:"stage,omitempty"`
	FrameIndex  int            `json:"frame_index,omitempty"`
	SourceIndex int            `json:"source_index,omitempty"`
	Round       int            `json:"round,omitempty"`
	Details     map[string]any `json:"details,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	where := e.ChunkID
	if e.Stage != "" {
		where += "/" + e.Stage
	}
	if e.FrameIndex >= 0 {
		where += fmt.Sprintf("/frame=%d", e.FrameIndex)
	}
	if e.SourceIndex >= 0 {
		where += fmt.Sprintf("/source=%d", e.SourceIndex)
	}
	if e.Round > 0 {
		where += fmt.Sprintf("/round=%d", e.Round)
	}
	if where != "" {
		return fmt.Sprintf("%s %s: %s", e.Code, where, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// WithDetail returns e with an additional diagnostic context value.
func (e *Error) WithDetail(key string, value any) *Error {
	if e.Details == nil {
		e.Details = map[string]any{}
	}
	e.Details[key] = value
	return e
}

func newError(code, message string) *Error {
	return &Error{Code: code, Message: message, FrameIndex: -1, SourceIndex: -1}
}

// AsError returns the typed domain error and whether err has that type.
func AsError(err error) (*Error, bool) {
	if err == nil {
		return nil, false
	}
	if de, ok := err.(*Error); ok {
		return de, true
	}
	type unwrapper interface{ Unwrap() error }
	for err != nil {
		if de, ok := err.(*Error); ok {
			return de, true
		}
		u, ok := err.(unwrapper)
		if !ok {
			return nil, false
		}
		err = u.Unwrap()
	}
	return nil, false
}

// CodeOf extracts a stable business code or returns E_INTERNAL for ordinary errors.
func CodeOf(err error) string {
	if de, ok := AsError(err); ok {
		return de.Code
	}
	return "E_INTERNAL"
}
