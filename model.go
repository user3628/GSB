package grayrouter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	CodeGraphInvalid      = "E_GRAPH_INVALID"
	CodeStaleBase         = "E_STALE_BASE"
	CodeConflictingUpdate = "E_CONFLICTING_UPDATE"
	CodeNotFound          = "E_NOT_FOUND"
	CodeVersionConflict   = "E_VERSION_CONFLICT"
	CodeIdempotency       = "E_IDEMPOTENCY_CONFLICT"
	CodeRecoveryAborted   = "E_RECOVERY_ABORTED"
	CodeInvalidRequest    = "E_INVALID_REQUEST"
	CodeInternal          = "E_INTERNAL"
)

type APIError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewAPIError(code, message string, context map[string]any) *APIError {
	return &APIError{Code: code, Message: message, Context: context}
}

func WrapInternal(err error, context map[string]any) error {
	if err == nil {
		return nil
	}
	return &APIError{Code: CodeInternal, Message: err.Error(), Context: context}
}

var AllowedDTypes = map[string]bool{
	"float32": true,
	"float64": true,
	"int8":    true,
	"int32":   true,
	"uint8":   true,
}

var AllowedQuantBits = map[int]bool{2: true, 4: true, 8: true, 16: true, 32: true}

var AllowedOps = map[string]bool{
	"input":   true,
	"linear":  true,
	"relu":    true,
	"add":     true,
	"scale":   true,
	"sigmoid": true,
	"output":  true,
}

type TensorDesc struct {
	Name  string `json:"name"`
	Shape []int  `json:"shape"`
	DType string `json:"dtype"`
}

type Node struct {
	ID        string         `json:"id"`
	Op        string         `json:"op"`
	Inputs    []TensorDesc   `json:"inputs,omitempty"`
	Outputs   []TensorDesc   `json:"outputs"`
	QuantBits int            `json:"quant_bits"`
	PreSteps  []string       `json:"pre_steps,omitempty"`
	PostSteps []string       `json:"post_steps,omitempty"`
	Attrs     map[string]any `json:"attrs,omitempty"`
}

type Graph struct {
	Nodes   []Node   `json:"nodes"`
	Outputs []string `json:"outputs,omitempty"`
}

type RouteConfig struct {
	Path    string `json:"path,omitempty"`
	Weight  int    `json:"weight,omitempty"`
	Enabled bool   `json:"enabled"`
}

type GrayConfig struct {
	RouteKey   string             `json:"route_key,omitempty"`
	Baseline   string             `json:"baseline,omitempty"`
	Canary     string             `json:"canary,omitempty"`
	CanaryRate float64            `json:"canary_rate,omitempty"`
	Weights    map[string]float64 `json:"weights,omitempty"`
	Salt       string             `json:"salt,omitempty"`
}

type ModelVersion struct {
	Name    string      `json:"name"`
	Version string      `json:"version"`
	Graph   Graph       `json:"graph"`
	Route   RouteConfig `json:"route,omitempty"`
	Gray    GrayConfig  `json:"gray,omitempty"`
}

func (m ModelVersion) Key() string { return m.Name + "@" + m.Version }

type Snapshot struct {
	Version int64          `json:"version"`
	Models  []ModelVersion `json:"models"`
}

type Delta struct {
	Version     int64          `json:"version"`
	BaseVersion int64          `json:"base_version"`
	Models      []ModelVersion `json:"models"`
}

type FieldError struct {
	NodeID string `json:"node_id,omitempty"`
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

type ValidationReport struct {
	Valid  bool         `json:"valid"`
	Errors []FieldError `json:"errors,omitempty"`
}

func canonicalJSON(value any) []byte {
	b, err := json.Marshal(value)
	if err != nil {
		return []byte(fmt.Sprintf("%v", value))
	}
	return b
}

func sha256Hex(parts ...[]byte) string {
	h := sha256.New()
	for _, part := range parts {
		h.Write(part)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func sortedStrings(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func stableMapDigest(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.Write(canonicalJSON(m[k]))
		b.WriteByte(';')
	}
	return b.String()
}
