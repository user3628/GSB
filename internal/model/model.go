package model

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
)

type Code string

const (
	CodeInvalidArgument    Code = "E_INVALID_ARGUMENT"
	CodeSessionConflict    Code = "E_SESSION_CONFLICT"
	CodeSessionNotFound    Code = "E_SESSION_NOT_FOUND"
	CodeSessionState       Code = "E_SESSION_STATE"
	CodePartMismatch       Code = "E_PART_MISMATCH"
	CodePartMissing        Code = "E_PART_MISSING"
	CodeSizeMismatch       Code = "E_SIZE_MISMATCH"
	CodeVerifyQueueFull    Code = "E_VERIFY_QUEUE_FULL"
	CodeConflictingRequest Code = "E_CONFLICTING_REQUEST"
	CodeObjectNotFound     Code = "E_OBJECT_NOT_FOUND"
	CodeRequestInProgress  Code = "E_REQUEST_IN_PROGRESS"
	CodeInternal           Code = "E_INTERNAL"
)

type Error struct {
	Code    Code           `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
	Cause   error          `json:"-"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Cause }

func NewError(code Code, message string) *Error {
	return &Error{Code: code, Message: message, Context: map[string]any{}}
}

func WrapError(code Code, message string, cause error) *Error {
	return &Error{Code: code, Message: message, Context: map[string]any{}, Cause: cause}
}

func (e *Error) WithContext(key string, value any) *Error {
	if e.Context == nil {
		e.Context = make(map[string]any)
	}
	e.Context[key] = value
	return e
}

func AsError(err error) *Error {
	if err == nil {
		return nil
	}
	if business, ok := err.(*Error); ok {
		return business
	}
	return WrapError(CodeInternal, "internal error", err)
}

func HTTPStatus(err *Error) int {
	if err == nil {
		return http.StatusOK
	}
	switch err.Code {
	case CodeInvalidArgument:
		return http.StatusBadRequest
	case CodeSessionConflict, CodeConflictingRequest, CodeSessionState:
		return http.StatusConflict
	case CodeSessionNotFound, CodeObjectNotFound:
		return http.StatusNotFound
	case CodePartMismatch, CodePartMissing, CodeSizeMismatch:
		return http.StatusUnprocessableEntity
	case CodeVerifyQueueFull, CodeRequestInProgress:
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}

const (
	SessionActive    = "active"
	SessionCompleted = "completed"
	SessionCancelled = "cancelled"
	SessionExpired   = "expired"
)

const (
	PartActive = "active"
	PartVoid   = "void"
)

const (
	IntentPending   = "pending"
	IntentCommitted = "committed"
	IntentRejected  = "rejected"
	IntentAbandoned = "abandoned"
)

const (
	ActionCreateSession = "create_session"
	ActionSubmitPart    = "submit_part"
	ActionCancelSession = "cancel_session"
	ActionExpireSession = "expire_session"
	ActionMerge         = "merge"
)

type Session struct {
	SessionID         string `json:"session_id"`
	ObjectKey         string `json:"object_key"`
	Generation        int64  `json:"generation"`
	LowerBound        int64  `json:"lower_bound"`
	TTLMS             int64  `json:"ttl_ms"`
	Status            string `json:"status"`
	CreatedAtMS       int64  `json:"created_at_ms"`
	ExpiresAtMS       int64  `json:"expires_at_ms"`
	CompletedAtMS     *int64 `json:"completed_at_ms,omitempty"`
	CancelledAtMS     *int64 `json:"cancelled_at_ms,omitempty"`
	CancelReason      string `json:"cancel_reason,omitempty"`
	ObjectSHA256      string `json:"object_sha256,omitempty"`
	ObjectTotalSize   int64  `json:"object_total_size,omitempty"`
	ObjectPartCount   int64  `json:"object_part_count,omitempty"`
	CreationRequestID string `json:"creation_request_id,omitempty"`
}

type Part struct {
	SessionID      string `json:"session_id"`
	PartNumber     int64  `json:"part_number"`
	Size           int64  `json:"size"`
	SHA256         string `json:"sha256"`
	ArrivedAtMS    int64  `json:"arrived_at_ms"`
	DuplicateCount int64  `json:"duplicate_count"`
	Status         string `json:"status"`
	CreatedAtMS    int64  `json:"created_at_ms"`
	Content        []byte `json:"-"`
}

type ObjectMetadata struct {
	ObjectKey   string `json:"object_key"`
	SessionID   string `json:"session_id"`
	Generation  int64  `json:"generation"`
	TotalSize   int64  `json:"total_size"`
	PartCount   int64  `json:"part_count"`
	SHA256      string `json:"sha256"`
	UpdatedAtMS int64  `json:"updated_at_ms"`
}

type Intent struct {
	Seq           int64  `json:"seq"`
	RequestID     string `json:"request_id,omitempty"`
	Action        string `json:"action"`
	SessionID     string `json:"session_id,omitempty"`
	ObjectKey     string `json:"object_key,omitempty"`
	PartNumber    *int64 `json:"part_number,omitempty"`
	SHA256        string `json:"sha256,omitempty"`
	Size          *int64 `json:"size,omitempty"`
	PayloadJSON   string `json:"payload_json,omitempty"`
	Status        string `json:"status"`
	CreatedAtMS   int64  `json:"created_at_ms"`
	CommittedAtMS *int64 `json:"committed_at_ms,omitempty"`
}

type VerificationReport struct {
	Concurrency         int     `json:"concurrency"`
	QueueCapacity       int     `json:"queue_capacity"`
	BudgetMS            int64   `json:"budget_ms"`
	DurationMS          int64   `json:"duration_ms"`
	VerifiedPartNumbers []int64 `json:"verified_part_numbers"`
	SkippedPartNumbers  []int64 `json:"skipped_part_numbers"`
	Degraded            bool    `json:"degraded"`
}

func (r *VerificationReport) Normalize() {
	if r.VerifiedPartNumbers == nil {
		r.VerifiedPartNumbers = []int64{}
	}
	if r.SkippedPartNumbers == nil {
		r.SkippedPartNumbers = []int64{}
	}
	sort.Slice(r.VerifiedPartNumbers, func(i, j int) bool {
		return r.VerifiedPartNumbers[i] < r.VerifiedPartNumbers[j]
	})
	sort.Slice(r.SkippedPartNumbers, func(i, j int) bool {
		return r.SkippedPartNumbers[i] < r.SkippedPartNumbers[j]
	})
}

type RequestRecord struct {
	RequestID     string `json:"request_id"`
	Operation     string `json:"operation"`
	Fingerprint   string `json:"fingerprint"`
	Status        string `json:"status"`
	HTTPStatus    int    `json:"http_status,omitempty"`
	ResponseBody  []byte `json:"-"`
	Code          string `json:"code,omitempty"`
	CreatedAtMS   int64  `json:"created_at_ms"`
	CompletedAtMS *int64 `json:"completed_at_ms,omitempty"`
}

func NormalizeSHA256(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
