package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

const (
	CodeEmptyQuery      = "E_EMPTY_QUERY"
	CodeVectorInvalid   = "E_VECTOR_INVALID"
	CodeNoWeight        = "E_NO_WEIGHT"
	CodeNumericMismatch = "E_NUMERIC_MISMATCH"
	CodeStaleBase       = "E_STALE_BASE"
	CodeStaleVersion    = "E_STALE_VERSION"
	CodeBudgetExhausted = "E_BUDGET_EXHAUSTED"
	CodeCheckpoint      = "E_CHECKPOINT"
	CodeInvalidRequest  = "E_INVALID_REQUEST"
)

type APIError struct {
	Code              string  `json:"code"`
	Message           string  `json:"message"`
	QueryID           string  `json:"query_id,omitempty"`
	ItemID            string  `json:"item_id,omitempty"`
	Dimension         int     `json:"dimension,omitempty"`
	ActualDimension   int     `json:"actual_dimension,omitempty"`
	ExpectedDimension int     `json:"expected_dimension,omitempty"`
	NanIndex          int     `json:"nan_index,omitempty"`
	MaxDeviation      float64 `json:"max_deviation,omitempty"`
	SourceRoute       string  `json:"source_route,omitempty"`
	BaseVersion       int64   `json:"base_version,omitempty"`
	CurrentVersion    int64   `json:"current_version,omitempty"`
	RequestedVersion  int64   `json:"requested_version,omitempty"`
	Digest            string  `json:"digest,omitempty"`
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewError(code, message string) *APIError {
	return &APIError{Code: code, Message: message}
}

type Item struct {
	ItemID       string    `json:"item_id"`
	Category     string    `json:"category"`
	CategoryPath []string  `json:"category_path"`
	VectorDim    int       `json:"vector_dim"`
	VectorScale  float64   `json:"vector_scale"`
	VectorF32    []float32 `json:"vector_f32"`
	Title        string    `json:"title"`
	Lang         string    `json:"lang"`
}

type Query struct {
	QueryID                string   `json:"query_id"`
	RawQuery               string   `json:"raw_query"`
	Locale                 string   `json:"locale"`
	HistoryClickCategories []string `json:"history_click_categories"`
}

type Dataset struct {
	Seed      int64   `json:"seed"`
	VectorDim int     `json:"vector_dim"`
	Items     []Item  `json:"items"`
	Queries   []Query `json:"queries"`
}

func CanonicalJSON(v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return []byte("canonical-fallback:" + fmt.Sprintf("%#v", v))
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

func DigestBytes(prefix string, b []byte) string {
	sum := sha256.Sum256(append([]byte(prefix), b...))
	return hex.EncodeToString(sum[:])
}

func Digest(v any) string {
	return DigestBytes("multirecall:v1:", CanonicalJSON(v))
}
