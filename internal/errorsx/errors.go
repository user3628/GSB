// Package errorsx defines the pipeline business error codes.
package errorsx

import "fmt"

// Business error codes.
const (
	EEmptyPrompt         = "E_EMPTY_PROMPT"
	EHashDegenerate      = "E_HASH_DEGENERATE"
	ENoiseDegenerate     = "E_NOISE_DEGENERATE"
	EPaletteEmpty        = "E_PALETTE_EMPTY"
	ECardDimMismatch     = "E_CARD_DIM_MISMATCH"
	EQuantAllZero        = "E_QUANT_ALL_ZERO"
	EIncrementalMismatch = "E_INCREMENTAL_MISMATCH"
	EStaleCheckpoint     = "E_STALE_CHECKPOINT"
	EInvalidInput        = "E_INVALID_INPUT"
)

// Error carries a stable business code, the material id and diagnostic detail.
type Error struct {
	Code       string `json:"code"`
	MaterialID string `json:"material_id,omitempty"`
	Stage      string `json:"stage,omitempty"`
	Detail     string `json:"detail"`
}

func (e *Error) Error() string {
	if e.MaterialID != "" {
		return fmt.Sprintf("%s [%s] %s", e.Code, e.MaterialID, e.Detail)
	}
	return e.Code + ": " + e.Detail
}

// New constructs a business error.
func New(code, materialID, detail string) *Error {
	return &Error{Code: code, MaterialID: materialID, Detail: detail}
}

// WithStage annotates the stage at which the error occurred.
func (e *Error) WithStage(stage string) *Error {
	cp := *e
	cp.Stage = stage
	return &cp
}

// CodeOf extracts a business code from any error, returning "" for plain errors.
func CodeOf(err error) string {
	if be, ok := err.(*Error); ok {
		return be.Code
	}
	return ""
}
