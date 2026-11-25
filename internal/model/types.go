package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

const (
	StageNormalize = "box_normalize_merge"
	StageColumns   = "column_inference"
	StageOrder     = "reading_order"
	StageSummary   = "result_summary"

	KindHeader = "header"
	KindBody   = "body"
	KindFooter = "footer"

	PrioritySpanFirst    = "span_first"
	PriorityContentFirst = "content_first"

	CodeBoxInvalid         = "E_BOX_INVALID"
	CodeColumnUnassigned   = "E_COLUMN_UNASSIGNED"
	CodeOrderConflict      = "E_ORDER_CONFLICT"
	CodeLatencyBudget      = "E_LATENCY_BUDGET"
	CodeStaleCheckpoint    = "E_STALE_CHECKPOINT"
	CodeSnapshotReplaced   = "E_SNAPSHOT_REPLACED"
	CodeInvalidRequest     = "E_INVALID_REQUEST"
	CodeNotFound           = "E_NOT_FOUND"
	CodeInternal           = "E_INTERNAL"
	CodeAlreadyRunning     = "E_ALREADY_RUNNING"
	CodeNotRunning         = "E_NOT_RUNNING"
	CodeStrategyInvalid    = "E_STRATEGY_INVALID"
	CodeDocumentInvalid    = "E_DOCUMENT_INVALID"
	CodeCheckpointCorrupt  = "E_CHECKPOINT_CORRUPT"
	CodeCheckpointRollback = "E_CHECKPOINT_ROLLBACK"
)

var StageOrderList = []string{StageNormalize, StageColumns, StageOrder, StageSummary}

func StageNumber(stage string) int {
	for i, candidate := range StageOrderList {
		if stage == candidate {
			return i + 1
		}
	}
	return 0
}

// Page uses pixel dimensions. Input coordinates remain normalized in Document.
type Page struct {
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
}

type TextBox struct {
	Index      int     `json:"index"`
	X          float64 `json:"x"`
	Y          float64 `json:"y"`
	W          float64 `json:"w"`
	H          float64 `json:"h"`
	FontTier   int     `json:"font_tier"`
	Text       string  `json:"text"`
	Lang       string  `json:"lang"`
	ColumnHint int     `json:"column_hint"`
}

// UnmarshalJSON accepts both w/h and width/height, and common font tier aliases.
// It also fills an omitted box index from the declaration order.
func (b *TextBox) UnmarshalJSON(data []byte) error {
	type rawBox struct {
		Index      *int            `json:"index"`
		X          *float64        `json:"x"`
		Y          *float64        `json:"y"`
		W          *float64        `json:"w"`
		Width      *float64        `json:"width"`
		H          *float64        `json:"h"`
		Height     *float64        `json:"height"`
		FontTier   *int            `json:"font_tier"`
		FontSize   json.RawMessage `json:"font_size"`
		Text       string          `json:"text"`
		Lang       string          `json:"lang"`
		ColumnHint *int            `json:"column_hint"`
	}
	var r rawBox
	if err := json.Unmarshal(data, &r); err != nil {
		return err
	}
	if r.Index != nil {
		b.Index = *r.Index
	} else {
		b.Index = -1
	}
	if r.X != nil {
		b.X = *r.X
	}
	if r.Y != nil {
		b.Y = *r.Y
	}
	if r.W != nil {
		b.W = *r.W
	} else if r.Width != nil {
		b.W = *r.Width
	}
	if r.H != nil {
		b.H = *r.H
	} else if r.Height != nil {
		b.H = *r.Height
	}
	if r.FontTier != nil {
		b.FontTier = *r.FontTier
	}
	if len(r.FontSize) != 0 {
		if tier, ok := parseFontTier(r.FontSize); ok {
			b.FontTier = tier
		}
	}
	b.Text = r.Text
	b.Lang = strings.TrimSpace(r.Lang)
	if b.Lang == "" {
		b.Lang = "unknown"
	}
	if r.ColumnHint != nil {
		b.ColumnHint = *r.ColumnHint
	} else {
		b.ColumnHint = -2
	}
	return nil
}

func parseFontTier(raw json.RawMessage) (int, bool) {
	var n int
	if err := json.Unmarshal(raw, &n); err == nil {
		return n, true
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, false
	}
	s = strings.ToLower(strings.TrimSpace(s))
	if n, err := strconv.Atoi(s); err == nil {
		return n, true
	}
	switch s {
	case "caption", "small", "小", "注释":
		return 1, true
	case "body", "normal", "regular", "正文", "常规":
		return 2, true
	case "title", "heading", "large", "标题", "大":
		return 3, true
	default:
		return 0, false
	}
}

type Document struct {
	DocID string    `json:"doc_id"`
	Page  Page      `json:"page"`
	Boxes []TextBox `json:"boxes"`
}

func (d *Document) UnmarshalJSON(data []byte) error {
	type rawDoc struct {
		DocID      string    `json:"doc_id"`
		Page       *Page     `json:"page"`
		PageWidth  *float64  `json:"page_width"`
		PageHeight *float64  `json:"page_height"`
		Width      *float64  `json:"width"`
		Height     *float64  `json:"height"`
		Boxes      []TextBox `json:"boxes"`
	}
	var r rawDoc
	if err := json.Unmarshal(data, &r); err != nil {
		return err
	}
	d.DocID = strings.TrimSpace(r.DocID)
	if r.Page != nil {
		d.Page = *r.Page
	} else {
		if r.PageWidth != nil {
			d.Page.Width = *r.PageWidth
		} else if r.Width != nil {
			d.Page.Width = *r.Width
		}
		if r.PageHeight != nil {
			d.Page.Height = *r.PageHeight
		} else if r.Height != nil {
			d.Page.Height = *r.Height
		}
	}
	d.Boxes = r.Boxes
	for i := range d.Boxes {
		if d.Boxes[i].Index < 0 {
			d.Boxes[i].Index = i
		}
	}
	return nil
}

func (d Document) Validate() error {
	if d.DocID == "" {
		return NewError(CodeDocumentInvalid, "doc_id is required", "")
	}
	if d.Page.Width <= 0 || d.Page.Height <= 0 || !finite(d.Page.Width) || !finite(d.Page.Height) {
		return NewError(CodeDocumentInvalid, "page width and height must be positive finite values", d.DocID)
	}
	seen := map[int]struct{}{}
	for _, box := range d.Boxes {
		if _, exists := seen[box.Index]; exists {
			return NewError(CodeDocumentInvalid, "box indexes must be unique", d.DocID)
		}
		seen[box.Index] = struct{}{}
	}
	return nil
}

type Strategy struct {
	Version                  int64   `json:"version"`
	MergeGapPX               float64 `json:"merge_gap_px"`
	MarginXPX                float64 `json:"margin_x_px"`
	MarginYPX                float64 `json:"margin_y_px"`
	HeaderFooterMaxFontTier  int     `json:"header_footer_max_font_tier"`
	ColumnHistogramBinPX     float64 `json:"column_histogram_bin_px"`
	ColumnGapPX              float64 `json:"column_gap_px"`
	ColumnMinSupport         int     `json:"column_min_support"`
	SpanWidthRatio           float64 `json:"span_width_ratio"`
	CrossColumnPriority      string  `json:"cross_column_priority"`
	ConfidenceVerticalWeight float64 `json:"confidence_vertical_weight"`
	ConfidenceColumnWeight   float64 `json:"confidence_column_weight"`
	LatencyBudgetMS          int     `json:"latency_budget_ms"`
}

func DefaultStrategy() Strategy {
	return Strategy{
		Version:                  1,
		MergeGapPX:               8,
		MarginXPX:                54,
		MarginYPX:                96,
		HeaderFooterMaxFontTier:  2,
		ColumnHistogramBinPX:     16,
		ColumnGapPX:              40,
		ColumnMinSupport:         1,
		SpanWidthRatio:           0.72,
		CrossColumnPriority:      PrioritySpanFirst,
		ConfidenceVerticalWeight: 0.6,
		ConfidenceColumnWeight:   0.4,
		LatencyBudgetMS:          50,
	}
}

func (s Strategy) Validate() error {
	problems := make([]string, 0)
	if s.Version <= 0 {
		problems = append(problems, "version must be positive")
	}
	if s.MergeGapPX < 0 || !finite(s.MergeGapPX) {
		problems = append(problems, "merge_gap_px must be finite and non-negative")
	}
	if s.MarginXPX < 0 || !finite(s.MarginXPX) {
		problems = append(problems, "margin_x_px must be finite and non-negative")
	}
	if s.MarginYPX < 0 || !finite(s.MarginYPX) {
		problems = append(problems, "margin_y_px must be finite and non-negative")
	}
	if s.HeaderFooterMaxFontTier < 0 {
		problems = append(problems, "header_footer_max_font_tier must be non-negative")
	}
	if s.ColumnHistogramBinPX <= 0 || !finite(s.ColumnHistogramBinPX) {
		problems = append(problems, "column_histogram_bin_px must be positive")
	}
	if s.ColumnGapPX < 0 || !finite(s.ColumnGapPX) {
		problems = append(problems, "column_gap_px must be non-negative")
	}
	if s.ColumnMinSupport < 1 {
		problems = append(problems, "column_min_support must be at least 1")
	}
	if !finite(s.SpanWidthRatio) || s.SpanWidthRatio <= 0 || s.SpanWidthRatio > 1 {
		problems = append(problems, "span_width_ratio must be in (0,1]")
	}
	if s.CrossColumnPriority != PrioritySpanFirst && s.CrossColumnPriority != PriorityContentFirst {
		problems = append(problems, "cross_column_priority must be span_first or content_first")
	}
	if s.ConfidenceVerticalWeight < 0 || !finite(s.ConfidenceVerticalWeight) ||
		s.ConfidenceColumnWeight < 0 || !finite(s.ConfidenceColumnWeight) {
		problems = append(problems, "confidence weights must be finite and non-negative")
	}
	if s.LatencyBudgetMS <= 0 {
		problems = append(problems, "latency_budget_ms must be positive")
	}
	if len(problems) > 0 {
		return NewError(CodeStrategyInvalid, strings.Join(problems, "; "), "")
	}
	return nil
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

type Diagnostic struct {
	Code        string `json:"code"`
	Stage       string `json:"stage"`
	DocID       string `json:"doc_id,omitempty"`
	BoxIndex    *int   `json:"box_index,omitempty"`
	BlockID     string `json:"block_id,omitempty"`
	Message     string `json:"message"`
	Recoverable bool   `json:"recoverable"`
}

type BoxPair struct {
	DocID string `json:"doc_id"`
	BoxA  int    `json:"box_a"`
	BoxB  int    `json:"box_b"`
}

type Error struct {
	Code        string       `json:"code"`
	Message     string       `json:"message"`
	DocID       string       `json:"doc_id,omitempty"`
	BoxIndex    *int         `json:"box_index,omitempty"`
	QueryID     string       `json:"query_id,omitempty"`
	ElapsedMS   float64      `json:"elapsed_ms"`
	Conflict    []BoxPair    `json:"conflict_boxes,omitempty"`
	Stage       string       `json:"stage,omitempty"`
	Diagnostics []Diagnostic `json:"diagnostics,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	parts := []string{e.Code + ": " + e.Message}
	if e.DocID != "" {
		parts = append(parts, "doc_id="+e.DocID)
	}
	if e.BoxIndex != nil {
		parts = append(parts, fmt.Sprintf("box_index=%d", *e.BoxIndex))
	}
	if e.QueryID != "" {
		parts = append(parts, "query_id="+e.QueryID)
	}
	return strings.Join(parts, " ")
}

func NewError(code, message, docID string) *Error {
	return &Error{Code: code, Message: message, DocID: docID}
}

func AsError(err error) *Error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*Error); ok {
		return e
	}
	return &Error{Code: CodeInternal, Message: err.Error()}
}

type StageFingerprint struct {
	Stage               string `json:"stage"`
	InputFingerprint    string `json:"input_fingerprint"`
	ArtifactFingerprint string `json:"artifact_fingerprint"`
	Sequence            int64  `json:"-"`
	Reused              bool   `json:"-"`
}

type MergedBox struct {
	BlockID       string `json:"block_id"`
	SourceIndices []int  `json:"source_indices"`
	X             int    `json:"x"`
	Y             int    `json:"y"`
	W             int    `json:"w"`
	H             int    `json:"h"`
	FontTier      int    `json:"font_tier"`
	Text          string `json:"text"`
	Lang          string `json:"lang"`
	Kind          string `json:"kind"`
	ColumnHint    int    `json:"column_hint"`
	FullSpan      bool   `json:"full_span"`
	Column        int    `json:"column"`
}

func (b MergedBox) Right() int  { return b.X + b.W }
func (b MergedBox) Bottom() int { return b.Y + b.H }

type NormalizedArtifact struct {
	DocID        string       `json:"doc_id"`
	Page         Page         `json:"page"`
	ContentLeft  int          `json:"content_left"`
	ContentRight int          `json:"content_right"`
	Boxes        []MergedBox  `json:"boxes"`
	Diagnostics  []Diagnostic `json:"diagnostics"`
}

type Column struct {
	Index    int `json:"index"`
	X0       int `json:"x0"`
	X1       int `json:"x1"`
	BoxCount int `json:"box_count"`
}

type ColumnArtifact struct {
	DocID            string       `json:"doc_id"`
	Page             Page         `json:"page"`
	ContentLeft      int          `json:"content_left"`
	ContentRight     int          `json:"content_right"`
	Columns          []Column     `json:"columns"`
	Boxes            []MergedBox  `json:"boxes"`
	FullSpanBlockIDs []string     `json:"full_span_block_ids"`
	Diagnostics      []Diagnostic `json:"diagnostics"`
}

type OrderedBlock struct {
	MergedBox
	Order int `json:"order"`
}

type OrderConfidence struct {
	FromOrder      int     `json:"from_order"`
	ToOrder        int     `json:"to_order"`
	FromBlockID    string  `json:"from_block_id"`
	ToBlockID      string  `json:"to_block_id"`
	VerticalGapPX  int     `json:"vertical_gap_px"`
	SameColumn     bool    `json:"same_column"`
	ColumnAffinity float64 `json:"column_affinity"`
	VerticalScore  float64 `json:"vertical_score"`
	ColumnScore    float64 `json:"column_score"`
	Confidence     float64 `json:"confidence"`
}

type OrderArtifact struct {
	DocID       string            `json:"doc_id"`
	Blocks      []OrderedBlock    `json:"blocks"`
	Confidences []OrderConfidence `json:"confidences"`
	Diagnostics []Diagnostic      `json:"diagnostics"`
}

type BlockSequenceItem struct {
	Order         int    `json:"order"`
	BlockID       string `json:"block_id"`
	Column        int    `json:"column"`
	Kind          string `json:"kind"`
	FullSpan      bool   `json:"full_span"`
	X             int    `json:"x"`
	Y             int    `json:"y"`
	W             int    `json:"w"`
	H             int    `json:"h"`
	Text          string `json:"text"`
	Lang          string `json:"lang"`
	FontTier      int    `json:"font_tier"`
	SourceIndices []int  `json:"source_indices"`
}

type ColumnStat struct {
	ColumnIndex int     `json:"column_index"`
	X0          int     `json:"x0"`
	X1          int     `json:"x1"`
	Width       int     `json:"width"`
	BoxCount    int     `json:"box_count"`
	CharCount   int     `json:"char_count"`
	AreaRatio   float64 `json:"area_ratio"`
}

type SummaryArtifact struct {
	DocID            string              `json:"doc_id"`
	Page             Page                `json:"page"`
	BlockSequence    []BlockSequenceItem `json:"block_sequence"`
	Columns          []ColumnStat        `json:"columns"`
	Diagnostics      []Diagnostic        `json:"diagnostics"`
	OrderFingerprint string              `json:"order_fingerprint"`
}

type DocumentResult struct {
	DocID               string             `json:"doc_id"`
	ContentFingerprint  string             `json:"content_fingerprint"`
	StrategyVersion     int64              `json:"strategy_version"`
	StrategyFingerprint string             `json:"strategy_fingerprint"`
	StageFingerprints   []StageFingerprint `json:"stage_fingerprints"`
	Blocks              []OrderedBlock     `json:"blocks"`
	Columns             []Column           `json:"columns"`
	ColumnStats         []ColumnStat       `json:"column_stats"`
	Confidences         []OrderConfidence  `json:"confidences"`
	Diagnostics         []Diagnostic       `json:"diagnostics"`
	ResultFingerprint   string             `json:"result_fingerprint"`
}

type Bundle struct {
	Documents []Document `json:"documents"`
	Strategy  Strategy   `json:"strategy"`
}

type DemoReport struct {
	StrategyVersion      int64            `json:"strategy_version"`
	StrategyFingerprint  string           `json:"strategy_fingerprint"`
	DocumentFingerprints []string         `json:"document_fingerprints"`
	DocumentResults      []DocumentResult `json:"document_results"`
	ResultFingerprint    string           `json:"result_fingerprint"`
}

func CanonicalJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}

func Digest(v any) string {
	data, err := CanonicalJSON(v)
	if err != nil {
		panic("model.Digest: " + err.Error())
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func DigestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (d Document) ContentFingerprint() string {
	copyDoc := d
	copyDoc.Boxes = append([]TextBox(nil), d.Boxes...)
	sort.Slice(copyDoc.Boxes, func(i, j int) bool { return copyDoc.Boxes[i].Index < copyDoc.Boxes[j].Index })
	return Digest(copyDoc)
}

func (s Strategy) Fingerprint() string { return Digest(s) }

func (a NormalizedArtifact) Fingerprint() string { return Digest(a) }
func (a ColumnArtifact) Fingerprint() string     { return Digest(a) }
func (a OrderArtifact) Fingerprint() string      { return Digest(a) }
func (a SummaryArtifact) Fingerprint() string    { return Digest(a) }

func ResultFingerprint(result DocumentResult) string {
	copyResult := result
	copyResult.ResultFingerprint = ""
	return Digest(copyResult)
}

func Round6(v float64) float64 {
	if v == 0 {
		return 0
	}
	return math.Round(v*1_000_000) / 1_000_000
}

func Clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func IndexPtr(i int) *int { return &i }
