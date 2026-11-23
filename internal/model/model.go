// Package model defines the shared domain model and business codes for the
// injection detection and context-encoding adjudication engine.
package model

// Landing positions supported by the engine.
const (
	LandingStringLiteral   = "string_literal"    // SQL 字符串字面量
	LandingIdentifier      = "identifier"         // SQL 标识符位置
	LandingCommandArgument = "command_argument"   // 命令参数位
	LandingPathSegment     = "path_segment"       // 文件路径段
	LandingTemplateInterp  = "template_interp"    // 模板插值位
)

// Encoding requirements supported by the engine.
const (
	EncodingSingleQuoteEscape = "single_quote_escape" // 单引号转义
	EncodingIdentifierQuote   = "identifier_quote"     // 标识符引用
	EncodingParamPlaceholder  = "param_placeholder"    // 参数化占位
	EncodingPathNormalize     = "path_normalize"       // 路径分隔符归一
	EncodingHTMLEntity        = "html_entity"          // HTML 实体
)

// Business adjudication codes.
const (
	CodeOK                 = "OK"
	CodeContextInvalid     = "E_CONTEXT_INVALID"
	CodeCtxEscape          = "E_CTX_ESCAPE"
	CodeSnapshotVersion    = "E_SNAPSHOT_VERSION"
	CodeSampleBudget       = "E_SAMPLE_BUDGET"
	CodeEncodeLimit        = "E_ENCODE_LIMIT"
	CodeEmptyPayload       = "E_EMPTY_PAYLOAD"
	CodeContextOutOfBounds = "E_CONTEXT_OOB"
)

// RuleSetVersion is the version of the boundary-marker rule collection.
const RuleSetVersion = "ruleset-1.0.0"

// SupportedLandings maps each landing to its surface label.
var SupportedLandings = map[string]string{
	LandingStringLiteral:   "sql",
	LandingIdentifier:      "sql",
	LandingCommandArgument: "command",
	LandingPathSegment:     "path",
	LandingTemplateInterp:  "template",
}

// SupportedEncodings is the set of valid encoding requirement identifiers.
var SupportedEncodings = map[string]bool{
	EncodingSingleQuoteEscape: true,
	EncodingIdentifierQuote:   true,
	EncodingParamPlaceholder:  true,
	EncodingPathNormalize:     true,
	EncodingHTMLEntity:        true,
}

// ContextDecl is a single host-context declaration.
type ContextDecl struct {
	ID        string   `json:"id"`
	Surface   string   `json:"surface"`
	Landing   string   `json:"landing"`
	Encodings []string `json:"encodings"`
	// SourceOffset is the byte offset inside the JSON declaration stream
	// where this declaration starts; filled in by the parser.
	SourceOffset int `json:"-"`
}

// ContextFile is the top-level JSON context declaration document.
type ContextFile struct {
	ContextVersion string        `json:"context_version"`
	Declarations   []ContextDecl `json:"declarations"`
}

// PayloadBlock is one length-prefixed text block read from the payload set.
type PayloadBlock struct {
	ID           string
	Surface      string
	Raw          string
	SourceOffset int // byte offset of the block header inside the payload stream
}

// Sample is the normalized internal model unifying a payload and its context.
type Sample struct {
	ID           string
	Seq          int      // 1-based sequence number within its batch
	Surface      string   // injection surface: sql / command / path / template
	Raw          string   // original payload bytes (valid UTF-8)
	Landing      string   // landing position
	Encodings    []string // required encoding chain, declaration order
	SourceOffset int      // source byte offset (payload block header)
}

// Verdict is the per-sample adjudication result.
type Verdict struct {
	SampleID      string `json:"sample_id"`
	Seq           int    `json:"seq"`
	Surface       string `json:"surface"`
	Landing       string `json:"landing"`
	Code          string `json:"code"`
	RuleID        string `json:"rule_id,omitempty"`
	RawOffset     int    `json:"raw_offset"`            // 归一化前字节偏移
	LandedOffset  int    `json:"landed_offset"`         // 归一化后字节偏移
	RawLength     int    `json:"raw_length"`
	LandedLength  int    `json:"landed_length"`
	Normalized    string `json:"normalized,omitempty"`
	ReasonVersion int    `json:"reason_version"`
	Marker        string `json:"marker,omitempty"`
}

// LatencyPoint is one interactive-side latency sample using monotonic time.
type LatencyPoint struct {
	Seq     int
	Landing string
	Code    string
	Nanos   int64 // monotonic-clock difference in nanoseconds
}
