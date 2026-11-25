package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

const (
	CodeHashDimZero            = "E_HASH_DIM_ZERO"
	CodeTaskMappingMissing     = "E_TASK_MAPPING_MISSING"
	CodeEmptyText              = "E_EMPTY_TEXT"
	CodeLabelUnmapped          = "E_LABEL_UNMAPPED"
	CodeCoverageLow            = "E_COVERAGE_LOW"
	CodeCoverageDeclInvalid    = "E_COVERAGE_DECL_INVALID"
	CodeTaskVersionUnsupported = "E_TASK_VERSION_UNSUPPORTED"
	CodeStaleRead              = "E_STALE_READ"
	CodeStaleCheckpoint        = "E_STALE_CHECKPOINT"
	CodeStaleVersion           = "E_STALE_VERSION"
	CodeNotFound               = "E_NOT_FOUND"
	CodeConflict               = "E_CONFLICT"
)

type CodedError struct {
	Code                     string `json:"code"`
	Message                  string `json:"message"`
	SampleID                 string `json:"sample_id,omitempty"`
	Task                     string `json:"task,omitempty"`
	CurrentVersion           int64  `json:"current_version,omitempty"`
	MinimumAcceptableVersion int64  `json:"minimum_acceptable_version,omitempty"`
	MissingStage             string `json:"missing_stage,omitempty"`
	ExpectedDigest           string `json:"expected_digest,omitempty"`
	ActualDigest             string `json:"actual_digest,omitempty"`
	CurrentTaskSequence      int64  `json:"current_task_sequence,omitempty"`
}

func (e *CodedError) Error() string {
	if e.SampleID != "" || e.Task != "" {
		return fmt.Sprintf("%s: %s (sample_id=%s task=%s)", e.Code, e.Message, e.SampleID, e.Task)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *CodedError) Is(target error) bool {
	other, ok := target.(*CodedError)
	return ok && other.Code == e.Code
}

func coded(code, msg, sampleID, task string) *CodedError {
	return &CodedError{Code: code, Message: msg, SampleID: sampleID, Task: task}
}

func HashDimZero(sampleID, task string) *CodedError {
	return coded(CodeHashDimZero, "hash_dim must be greater than zero", sampleID, task)
}

func TaskMappingMissing(sampleID, task string) *CodedError {
	return coded(CodeTaskMappingMissing, "task mapping rule is missing", sampleID, task)
}

func EmptyText(sampleID, task string) *CodedError {
	return coded(CodeEmptyText, "raw text is empty after normalization", sampleID, task)
}

func LabelUnmapped(sampleID, task, rawLabel string) *CodedError {
	err := coded(CodeLabelUnmapped, "label has no recoding entry: "+rawLabel, sampleID, task)
	return err
}

func CoverageLow(sampleID, task string, ratio, threshold float64) *CodedError {
	return coded(CodeCoverageLow, fmt.Sprintf("coverage %.6f is below threshold %.6f", ratio, threshold), sampleID, task)
}

func CanonicalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func MustCanonicalJSON(v any) []byte {
	b, err := CanonicalJSON(v)
	if err != nil {
		panic(err)
	}
	return b
}

func DigestBytes(prefix string, b []byte) string {
	sum := sha256.Sum256(append([]byte(prefix), b...))
	return hex.EncodeToString(sum[:])
}

func DigestValue(prefix string, v any) (string, []byte, error) {
	b, err := CanonicalJSON(v)
	if err != nil {
		return "", nil, err
	}
	return DigestBytes(prefix, b), b, nil
}

type CoverageDeclaration struct {
	Denominator     int      `json:"denominator,omitempty"`
	DeclaredHitBits *int     `json:"declared_hit_bits,omitempty"`
	DeclaredRatio   *float64 `json:"declared_ratio,omitempty"`
	Note            string   `json:"note,omitempty"`
}

type Sample struct {
	SampleID    string               `json:"sample_id"`
	Task        string               `json:"task"`
	RawText     string               `json:"raw_text"`
	Label       string               `json:"label"`
	TaskVersion int                  `json:"task_version"`
	Coverage    *CoverageDeclaration `json:"coverage_declaration,omitempty"`
}

type Shard struct {
	Task    string   `json:"task"`
	Samples []Sample `json:"samples"`
}

func (s Sample) ContentFingerprint() (string, error) {
	fingerprintOnly := struct {
		SampleID    string               `json:"sample_id"`
		Task        string               `json:"task"`
		RawText     string               `json:"raw_text"`
		Label       string               `json:"label"`
		TaskVersion int                  `json:"task_version"`
		Coverage    *CoverageDeclaration `json:"coverage_declaration,omitempty"`
	}{s.SampleID, s.Task, s.RawText, s.Label, s.TaskVersion, s.Coverage}
	digest, _, err := DigestValue("sample/v1:", fingerprintOnly)
	return digest, err
}

type Token struct {
	Position int    `json:"position"`
	Text     string `json:"text"`
	Kind     string `json:"kind"`
}

type NormalizedSample struct {
	RecordSchema   string  `json:"record_schema"`
	SampleID       string  `json:"sample_id"`
	Task           string  `json:"task"`
	NormalizedText string  `json:"normalized_text"`
	Tokens         []Token `json:"tokens"`
}

type FeatureBit struct {
	FeatureIndex   int    `json:"feature_index"`
	SourceTask     string `json:"source_task"`
	SourceToken    string `json:"source_token"`
	MappedTerm     string `json:"mapped_term"`
	MappingBasis   string `json:"mapping_basis"`
	MappingVersion string `json:"mapping_version"`
}

type CoverageResult struct {
	Applicable       bool     `json:"applicable"`
	Denominator      int      `json:"denominator"`
	HitBits          int      `json:"hit_bits"`
	CoverageRatio    *float64 `json:"coverage_ratio,omitempty"`
	Threshold        float64  `json:"threshold"`
	ThresholdMargin  *float64 `json:"threshold_margin,omitempty"`
	DeclarationValid bool     `json:"declaration_valid"`
	Messages         []string `json:"messages"`
}

type Record struct {
	RecordSchema      string          `json:"record_schema"`
	SampleID          string          `json:"sample_id"`
	Task              string          `json:"task"`
	TaskSequence      int64           `json:"task_sequence,omitempty"`
	TaskVersion       int             `json:"task_version"`
	ConfigVersion     int64           `json:"config_version"`
	MappingVersion    string          `json:"mapping_version,omitempty"`
	LabelTableVersion string          `json:"label_table_version,omitempty"`
	RawLabel          string          `json:"raw_label"`
	UnifiedLabel      string          `json:"unified_label,omitempty"`
	VersionStatus     string          `json:"version_status"`
	Warnings          []string        `json:"warnings"`
	NormalizedText    string          `json:"normalized_text,omitempty"`
	Tokens            []Token         `json:"tokens"`
	Features          []FeatureBit    `json:"features"`
	ActiveBitCount    int             `json:"active_bit_count"`
	Coverage          *CoverageResult `json:"coverage,omitempty"`
	Status            string          `json:"status"`
	Rejections        []*CodedError   `json:"rejections"`
	RecordDigest      string          `json:"record_digest"`
}

func (r Record) Digest() (string, error) {
	r.RecordDigest = ""
	digest, _, err := DigestValue("feature-record/v1:", r)
	return digest, err
}

func (r *Record) FillDigest() error {
	d, err := r.Digest()
	if err != nil {
		return err
	}
	r.RecordDigest = d
	return nil
}
