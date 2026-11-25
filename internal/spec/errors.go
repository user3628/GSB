package spec

import "fmt"

// BizCode is a stable business error code. Codes never change once released.
type BizCode string

const (
	CodeEmptyQuery      BizCode = "E_EMPTY_QUERY"
	CodeNoCandidates    BizCode = "E_NO_CANDIDATES"
	CodeEmptyRewrite    BizCode = "E_EMPTY_REWRITE"
	CodeNumeric         BizCode = "E_NUMERIC"
	CodeStaleVersion    BizCode = "E_STALE_VERSION"
	CodeStaleCheckpoint BizCode = "E_STALE_CHECKPOINT"
	CodeBudgetMille     BizCode = "E_BUDGET_MILLE"
	CodeBudgetExceeded  BizCode = "E_BUDGET_EXCEEDED"
	CodeCacheFull       BizCode = "E_CACHE_FULL"
	CodeStaleRead       BizCode = "E_STALE_READ"
	CodeInvalidPolicy   BizCode = "E_INVALID_POLICY"
)

// Stage names are fixed strings used in checkpoints and error contexts.
const (
	StageGenerate = "stage1_generate"
	StagePrefix   = "stage2_prefix"
	StageAdjudge  = "stage3_adjudge"
	StageSchedule = "stage4_schedule"
)

// AllStages returns the ordered pipeline stage names.
func AllStages() []string {
	return []string{StageGenerate, StagePrefix, StageAdjudge, StageSchedule}
}

// BizError is the structured, stable error carried across stages and APIs.
type BizError struct {
	Code      BizCode           `json:"code"`
	Message   string            `json:"message"`
	SessionID string            `json:"session_id,omitempty"`
	Turn      int               `json:"turn,omitempty"`
	Stage     string            `json:"stage,omitempty"`
	Candidate string            `json:"candidate_id,omitempty"`
	Details   map[string]string `json:"details,omitempty"`
}

func (e *BizError) Error() string {
	return fmt.Sprintf("%s: %s (session=%s turn=%d stage=%s candidate=%s)",
		e.Code, e.Message, e.SessionID, e.Turn, e.Stage, e.Candidate)
}

// NewBizError builds a structured business error.
func NewBizError(code BizCode, msg, sessionID string, turn int, stage string) *BizError {
	return &BizError{
		Code:      code,
		Message:   msg,
		SessionID: sessionID,
		Turn:      turn,
		Stage:     stage,
		Details:   map[string]string{},
	}
}

// WithCandidate attaches the candidate identifier (E_NUMERIC requirement).
func (e *BizError) WithCandidate(cid string) *BizError {
	e.Candidate = cid
	return e
}

// WithDetail attaches a deterministic derivation detail.
func (e *BizError) WithDetail(k, v string) *BizError {
	if e.Details == nil {
		e.Details = map[string]string{}
	}
	e.Details[k] = v
	return e
}

// String returns the raw stable code text.
func (c BizCode) String() string { return string(c) }
