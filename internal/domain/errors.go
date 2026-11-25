package domain

import "fmt"

const (
	CodeInvalidRequest        = "E_INVALID_REQUEST"
	CodeStaleCheckpoint       = "E_STALE_CHECKPOINT"
	CodeCheckpointRollback    = "E_CHECKPOINT_ROLLBACK"
	CodeCheckpointTampered    = "E_CHECKPOINT_TAMPERED"
	CodeStageTransition       = "E_STAGE_TRANSITION"
	CodeMissingPrerequisite   = "E_MISSING_PREREQUISITE"
	CodeMapVersionConflict    = "E_MAP_VERSION_CONFLICT"
	CodeUnknownRoleCode       = "E_UNKNOWN_ROLE_CODE"
	CodeUnknownRole           = "E_UNKNOWN_ROLE"
	CodeValueNormalize        = "E_VALUE_NORMALIZE"
	CodeRoleRangeIncompatible = "E_ROLE_RANGE_INCOMPATIBLE"
	CodeWorkspaceCorrupt      = "E_WORKSPACE_CORRUPT"
)

type BusinessError struct {
	Code       string   `json:"code"`
	Message    string   `json:"message"`
	SessionID  string   `json:"session_id,omitempty"`
	TurnIndex  *int     `json:"turn_index,omitempty"`
	Stage      string   `json:"stage,omitempty"`
	Field      string   `json:"field,omitempty"`
	Detail     string   `json:"detail,omitempty"`
	Candidates []string `json:"candidates,omitempty"`
}

func (e *BusinessError) Error() string {
	if e.SessionID != "" || e.TurnIndex != nil || e.Stage != "" {
		turn := -1
		if e.TurnIndex != nil {
			turn = *e.TurnIndex
		}
		return fmt.Sprintf("%s: %s (session=%s turn=%d stage=%s field=%s)", e.Code, e.Message, e.SessionID, turn, e.Stage, e.Field)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func BizError(code, message string) *BusinessError {
	return &BusinessError{Code: code, Message: message}
}

func ContextError(code, message, sessionID string, turn int, field string) *BusinessError {
	t := turn
	return &BusinessError{
		Code:      code,
		Message:   message,
		SessionID: sessionID,
		TurnIndex: &t,
		Field:     field,
	}
}

func AsBusinessError(err error) *BusinessError {
	if err == nil {
		return nil
	}
	if be, ok := err.(*BusinessError); ok {
		return be
	}
	return &BusinessError{Code: CodeWorkspaceCorrupt, Message: err.Error()}
}
