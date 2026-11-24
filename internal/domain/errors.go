package domain

import "fmt"

const (
	ErrPartitionInvalid  = "E_PARTITION_INVALID"
	ErrVRAMPoolFull      = "E_VRAM_POOL_FULL"
	ErrLossScaleOverflow = "E_LOSS_SCALE_OVERFLOW"
	ErrStaleSnapshot     = "E_STALE_SNAPSHOT"
	ErrStaleBase         = "E_STALE_BASE"
	ErrWriteQueueFull    = "E_WRITE_QUEUE_FULL"
	ErrInvalidRequest    = "E_INVALID_REQUEST"
	ErrNotFound          = "E_NOT_FOUND"
	ErrChecksumMismatch  = "E_CHECKSUM_MISMATCH"
	ErrInternal          = "E_INTERNAL"
)

type BusinessError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context"`
}

func (e *BusinessError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func BizError(code, message string, context map[string]any) *BusinessError {
	return &BusinessError{Code: code, Message: message, Context: context}
}

func ErrorCode(err error) string {
	if be, ok := err.(*BusinessError); ok {
		return be.Code
	}
	return ErrInternal
}
