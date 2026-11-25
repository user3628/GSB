package model

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"math"
	"strings"
)

const (
	StageRotation = "rotation"
	StageScore    = "score"

	CodeNumeric         = "E_NUMERIC"
	CodeStaleCheckpoint = "E_STALE_CHECKPOINT"
	CodeMemoryBudget    = "E_MEMORY_BUDGET"
	CodeInvalidConfig   = "E_INVALID_CONFIG"
	CodeNotFound        = "E_NOT_FOUND"
	CodeState           = "E_STATE"
	CodeIO              = "E_IO"
)

type ScalingMode string

const (
	ScaleNone    ScalingMode = "none"
	ScaleLinear  ScalingMode = "linear"
	ScaleNTKPart ScalingMode = "ntk_piecewise"
)

// Shape is [batch, heads, sequence, head dimension].
type Shape [4]int

func (s Shape) Batch() int   { return s[0] }
func (s Shape) Heads() int   { return s[1] }
func (s Shape) Seq() int     { return s[2] }
func (s Shape) HeadDim() int { return s[3] }

func (s Shape) Len() int {
	return s[0] * s[1] * s[2] * s[3]
}

func (s Shape) Validate() error {
	for i, v := range s {
		if v <= 0 {
			return fmt.Errorf("shape dimension %d must be positive, got %d", i, v)
		}
	}
	return nil
}

func (s Shape) ProductExceptBatch() int {
	return s[1] * s[2] * s[3]
}

type Config struct {
	SnapshotVersion    int64       `json:"snapshot_version"`
	BaseFrequency      float64     `json:"base_frequency"`
	ScalingMode        ScalingMode `json:"scaling_mode"`
	ScaleFactor        float64     `json:"scale_factor"`
	AngleBlockSize     int         `json:"angle_block_size"`
	CausalMask         bool        `json:"causal_mask"`
	RowBlockSize       int         `json:"row_block_size"`
	KeyBlockSize       int         `json:"key_block_size"`
	MaxConcurrentBlock int         `json:"max_concurrent_blocks"`
	WorkerBlockQuota   int         `json:"worker_block_quota"`
	RoundTimeBudgetMS  int64       `json:"round_time_budget_ms"`
	PeakMemoryBytes    int64       `json:"peak_memory_bytes"`
}

func (c Config) Validate() error {
	if c.SnapshotVersion < 1 {
		return fmt.Errorf("snapshot_version must be >= 1")
	}
	if math.IsNaN(c.BaseFrequency) || math.IsInf(c.BaseFrequency, 0) || c.BaseFrequency <= 1 {
		return fmt.Errorf("base_frequency must be finite and > 1")
	}
	if c.ScalingMode != ScaleNone && c.ScalingMode != ScaleLinear && c.ScalingMode != ScaleNTKPart {
		return fmt.Errorf("unsupported scaling_mode %q", c.ScalingMode)
	}
	if math.IsNaN(c.ScaleFactor) || math.IsInf(c.ScaleFactor, 0) || c.ScaleFactor <= 0 {
		return fmt.Errorf("scale_factor must be finite and > 0")
	}
	if c.AngleBlockSize < 2 || c.AngleBlockSize%2 != 0 {
		return fmt.Errorf("angle_block_size must be an even integer >= 2")
	}
	if c.RowBlockSize < 1 || c.KeyBlockSize < 1 {
		return fmt.Errorf("row_block_size and key_block_size must be >= 1")
	}
	if c.MaxConcurrentBlock < 1 || c.WorkerBlockQuota < 1 {
		return fmt.Errorf("concurrency limits must be >= 1")
	}
	if c.RoundTimeBudgetMS < 1 {
		return fmt.Errorf("round_time_budget_ms must be >= 1")
	}
	if c.PeakMemoryBytes < 1 {
		return fmt.Errorf("peak_memory_bytes must be >= 1")
	}
	return nil
}

type QuantTensor struct {
	Shape Shape   `json:"shape"`
	Scale int64   `json:"scale"`
	Data  []int64 `json:"data"`
}

func (t QuantTensor) Validate(name string) error {
	if err := t.Shape.Validate(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if t.Scale <= 0 {
		return fmt.Errorf("%s: scale must be positive", name)
	}
	if len(t.Data) != t.Shape.Len() {
		return fmt.Errorf("%s: data length %d != shape product %d", name, len(t.Data), t.Shape.Len())
	}
	return nil
}

func (t QuantTensor) Index(batch, head, seq, dim int) int {
	s := t.Shape
	return ((batch*s.Heads()+head)*s.Seq()+seq)*s.HeadDim() + dim
}

func (t QuantTensor) FloatAt(idx int) float64 {
	return float64(t.Data[idx]) / float64(t.Scale)
}

type ShapeDeclaration struct {
	ShardID     string `json:"shard_id"`
	Shape       Shape  `json:"shape"`
	Description string `json:"description,omitempty"`
	HasExtremes bool   `json:"has_extremes"`
}

type Shard struct {
	ID             string       `json:"id"`
	Description    string       `json:"description,omitempty"`
	Shape          Shape        `json:"shape"`
	Q              QuantTensor  `json:"q"`
	K              QuantTensor  `json:"k"`
	ReferenceQ     *QuantTensor `json:"reference_rotated_q,omitempty"`
	ReferenceK     *QuantTensor `json:"reference_rotated_k,omitempty"`
	InputDigest    string       `json:"input_digest,omitempty"`
	SampleMetadata any          `json:"sample_metadata,omitempty"`
}

func (s Shard) Validate() error {
	if strings.TrimSpace(s.ID) == "" {
		return fmt.Errorf("shard id is empty")
	}
	if err := s.Shape.Validate(); err != nil {
		return fmt.Errorf("shard %s: %w", s.ID, err)
	}
	if s.Q.Shape != s.Shape || s.K.Shape != s.Shape {
		return fmt.Errorf("shard %s: q/k shape does not match declared shape", s.ID)
	}
	if err := s.Q.Validate("q"); err != nil {
		return fmt.Errorf("shard %s: %w", s.ID, err)
	}
	if err := s.K.Validate("k"); err != nil {
		return fmt.Errorf("shard %s: %w", s.ID, err)
	}
	if s.ReferenceQ != nil {
		if err := s.ReferenceQ.Validate("reference_rotated_q"); err != nil {
			return fmt.Errorf("shard %s: %w", s.ID, err)
		}
	}
	if s.ReferenceK != nil {
		if err := s.ReferenceK.Validate("reference_rotated_k"); err != nil {
			return fmt.Errorf("shard %s: %w", s.ID, err)
		}
	}
	return nil
}

type Bundle struct {
	FormatVersion int                `json:"format_version"`
	Config        Config             `json:"config"`
	Declarations  []ShapeDeclaration `json:"shape_declarations"`
	Shards        []Shard            `json:"shards"`
}

func (b Bundle) Validate() error {
	if err := b.Config.Validate(); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if len(b.Shards) == 0 {
		return fmt.Errorf("bundle contains no shards")
	}
	ids := make(map[string]struct{}, len(b.Shards))
	for _, s := range b.Shards {
		if err := s.Validate(); err != nil {
			return err
		}
		if _, exists := ids[s.ID]; exists {
			return fmt.Errorf("duplicate shard id %q", s.ID)
		}
		ids[s.ID] = struct{}{}
	}
	declared := make(map[string]Shape, len(b.Declarations))
	for _, d := range b.Declarations {
		if err := d.Shape.Validate(); err != nil {
			return fmt.Errorf("declaration %s: %w", d.ShardID, err)
		}
		if _, exists := declared[d.ShardID]; exists {
			return fmt.Errorf("duplicate shape declaration %q", d.ShardID)
		}
		declared[d.ShardID] = d.Shape
	}
	for _, s := range b.Shards {
		shape, ok := declared[s.ID]
		if !ok {
			return fmt.Errorf("shard %s has no shape declaration", s.ID)
		}
		if shape != s.Shape {
			return fmt.Errorf("shard %s shape %v differs from declaration %v", s.ID, s.Shape, shape)
		}
	}
	return nil
}

type Fingerprints struct {
	Input     string `json:"input_digest"`
	Frequency string `json:"frequency_digest"`
	Rotation  string `json:"rotation_fingerprint"`
	Score     string `json:"score_fingerprint"`
}

func (f Fingerprints) ForStage(stage string) string {
	if stage == StageScore {
		return f.Score
	}
	return f.Rotation
}

func ComputeFingerprints(shard Shard, cfg Config) Fingerprints {
	input := InputDigest(shard)
	freq := FrequencyDigest(cfg, shard.Shape.HeadDim())
	rot := digestStrings("rope-rotation-v1", input, freq, fmt.Sprint(cfg.AngleBlockSize))
	score := digestStrings("rope-score-v1", rot, fmt.Sprint(cfg.CausalMask))
	return Fingerprints{Input: input, Frequency: freq, Rotation: rot, Score: score}
}

func InputDigest(shard Shard) string {
	h := sha256.New()
	writeString(h, "rope-input-v1")
	writeShape(h, shard.Shape)
	writeTensor(h, shard.Q)
	writeTensor(h, shard.K)
	return hex.EncodeToString(h.Sum(nil))
}

func TensorDigest(t QuantTensor) string {
	h := sha256.New()
	writeTensor(h, t)
	return hex.EncodeToString(h.Sum(nil))
}

func ConfigDigest(cfg Config) string {
	h := sha256.New()
	writeString(h, "rope-config-v1")
	writeInt64(h, cfg.SnapshotVersion)
	writeFloat64(h, cfg.BaseFrequency)
	writeString(h, string(cfg.ScalingMode))
	writeFloat64(h, cfg.ScaleFactor)
	writeInt64(h, int64(cfg.AngleBlockSize))
	writeBool(h, cfg.CausalMask)
	writeInt64(h, int64(cfg.RowBlockSize))
	writeInt64(h, int64(cfg.KeyBlockSize))
	writeInt64(h, int64(cfg.MaxConcurrentBlock))
	writeInt64(h, int64(cfg.WorkerBlockQuota))
	writeInt64(h, cfg.RoundTimeBudgetMS)
	writeInt64(h, cfg.PeakMemoryBytes)
	return hex.EncodeToString(h.Sum(nil))
}

func FrequencyDigest(cfg Config, headDim int) string {
	h := sha256.New()
	writeString(h, "rope-frequency-v1")
	writeFloat64(h, cfg.BaseFrequency)
	writeString(h, string(cfg.ScalingMode))
	writeFloat64(h, cfg.ScaleFactor)
	writeInt64(h, int64(headDim))
	return hex.EncodeToString(h.Sum(nil))
}

func digestStrings(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		writeString(h, p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeShape(h hash.Hash, s Shape) {
	for _, v := range s {
		writeInt64(h, int64(v))
	}
}

func writeTensor(h hash.Hash, t QuantTensor) {
	writeShape(h, t.Shape)
	writeInt64(h, t.Scale)
	var buf [8]byte
	for _, v := range t.Data {
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		_, _ = h.Write(buf[:])
	}
}

func writeString(h hash.Hash, s string) {
	writeInt64(h, int64(len(s)))
	_, _ = h.Write([]byte(s))
}

func writeFloat64(h hash.Hash, v float64) {
	writeInt64(h, int64(math.Float64bits(v)))
}

func writeBool(h hash.Hash, v bool) {
	if v {
		writeInt64(h, 1)
	} else {
		writeInt64(h, 0)
	}
}

func writeInt64(h hash.Hash, v int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	_, _ = h.Write(buf[:])
}

type AngleSummary struct {
	DimStart    int     `json:"dim_start"`
	DimEnd      int     `json:"dim_end"`
	PairCount   int     `json:"pair_count"`
	FirstAngle  float64 `json:"first_angle"`
	LastAngle   float64 `json:"last_angle"`
	AngleDigest string  `json:"angle_digest"`
}

type RotatedBlock struct {
	ShardID      string         `json:"shard_id"`
	Batch        int            `json:"batch"`
	Head         int            `json:"head"`
	RowStart     int            `json:"row_start"`
	RowEnd       int            `json:"row_end"`
	Shape        Shape          `json:"shape"`
	Scale        int64          `json:"scale"`
	Data         []int64        `json:"data"`
	Digest       string         `json:"digest"`
	Angles       []AngleSummary `json:"angle_summaries"`
	UnrotatedDim []int          `json:"unrotated_dims,omitempty"`
}

type TensorRole string

const (
	RoleQ TensorRole = "q"
	RoleK TensorRole = "k"
)

type ScoreBlock struct {
	ShardID  string         `json:"shard_id"`
	Batch    int            `json:"batch"`
	Head     int            `json:"head"`
	RowStart int            `json:"row_start"`
	RowEnd   int            `json:"row_end"`
	Causal   bool           `json:"causal"`
	Rows     []AttentionRow `json:"rows"`
	Digest   string         `json:"weight_digest"`
}

type AttentionRow struct {
	QueryIndex      int       `json:"query_index"`
	AllowedKeyStart int       `json:"allowed_key_start"`
	AllowedKeyEnd   int       `json:"allowed_key_end"`
	MaxScore        float64   `json:"max_score"`
	ExpSum          float64   `json:"exp_sum"`
	LogSumExp       float64   `json:"log_sum_exp"`
	Normalization   float64   `json:"normalization_factor"`
	WeightDigest    string    `json:"weight_digest"`
	OutputDigest    string    `json:"output_digest"`
	Output          []float64 `json:"output"`
}

type SoftmaxState struct {
	Max      float64   `json:"max"`
	SumExp   float64   `json:"sum_exp"`
	Weighted []float64 `json:"weighted"`
}

type Checkpoint struct {
	ShardID            string    `json:"shard_id"`
	Stage              string    `json:"stage"`
	Batch              int       `json:"batch"`
	Head               int       `json:"head"`
	RowBlock           int       `json:"row_block"`
	RowStart           int       `json:"row_start"`
	RowEnd             int       `json:"row_end"`
	CoveredKeyStart    int       `json:"covered_key_start"`
	CoveredKeyEnd      int       `json:"covered_key_end"`
	RunningMax         float64   `json:"running_max"`
	ExpSum             float64   `json:"exp_sum"`
	LSE                float64   `json:"log_sum_exp"`
	PartialOutput      []float64 `json:"partial_output,omitempty"`
	PartialDigest      string    `json:"partial_output_digest"`
	InputFingerprint   string    `json:"input_fingerprint"`
	VersionFingerprint string    `json:"version_fingerprint"`
	Sequence           int64     `json:"sequence"`
	Complete           bool      `json:"complete"`
	ConfigVersion      int64     `json:"config_version"`
	RunID              string    `json:"run_id,omitempty"`
	UpdatedAt          string    `json:"updated_at"`
}

type PublishRequest struct {
	Checkpoint
	AssignedShardID  string          `json:"assigned_shard_id"`
	AssignedRowBlock int             `json:"assigned_row_block"`
	ExpectedSequence int64           `json:"expected_sequence"`
	ResultPayload    json.RawMessage `json:"result_payload,omitempty"`
}

type BudgetEvent struct {
	RunID       string `json:"run_id"`
	ShardID     string `json:"shard_id"`
	Stage       string `json:"stage"`
	Batch       int    `json:"batch"`
	Head        int    `json:"head"`
	RowBlock    int    `json:"row_block"`
	Status      string `json:"status"`
	Reason      string `json:"reason,omitempty"`
	EstimatedUS int64  `json:"estimated_us"`
	RemainingUS int64  `json:"remaining_us"`
	CreatedAt   string `json:"created_at"`
}

type MemoryStep struct {
	Index          int    `json:"index"`
	Mode           string `json:"mode"`
	Batch          int    `json:"batch"`
	RowStart       int    `json:"row_start"`
	RowEnd         int    `json:"row_end"`
	EstimatedBytes int64  `json:"estimated_bytes"`
	Note           string `json:"note"`
}

type MemoryBreakdown struct {
	RotatedBytes       int64 `json:"rotated_tensor_bytes"`
	RowWorkspace       int64 `json:"row_workspace_bytes"`
	AngleTable         int64 `json:"angle_table_bytes"`
	AttentionWorkspace int64 `json:"attention_workspace_bytes"`
	Overhead           int64 `json:"overhead_bytes"`
	Total              int64 `json:"total_bytes"`
}

type MemoryPlan struct {
	ShardID   string          `json:"shard_id"`
	Stage     string          `json:"stage"`
	Budget    int64           `json:"budget_bytes"`
	Full      MemoryBreakdown `json:"full_estimate"`
	Mode      string          `json:"mode"`
	Steps     []MemoryStep    `json:"steps"`
	CreatedAt string          `json:"created_at"`
}

type InvalidationEvent struct {
	ID               int64  `json:"id"`
	ConfigVersion    int64  `json:"config_version"`
	Stage            string `json:"stage"`
	ShardID          string `json:"shard_id"`
	EarliestRowBlock int    `json:"earliest_row_block"`
	ColBlockStart    int    `json:"col_block_start"`
	ColBlockEnd      int    `json:"col_block_end"`
	Reason           string `json:"reason"`
	AffectedRows     int    `json:"affected_rows"`
	ReusedRows       int    `json:"reused_rows"`
	CreatedAt        string `json:"created_at"`
}

type VersionEvent struct {
	FromVersion int64  `json:"from_version"`
	ToVersion   int64  `json:"to_version"`
	Kind        string `json:"kind"`
	FromDigest  string `json:"from_digest,omitempty"`
	ToDigest    string `json:"to_digest,omitempty"`
	CreatedAt   string `json:"created_at"`
}

type ConfigTransition struct {
	Kind  string       `json:"kind"`
	From  *Config      `json:"from,omitempty"`
	To    Config       `json:"to"`
	Event VersionEvent `json:"event"`
}

type RunRecord struct {
	ID            string `json:"id"`
	ConfigVersion int64  `json:"config_version"`
	Stage         string `json:"stage"`
	Status        string `json:"status"`
	StartedAt     string `json:"started_at"`
	FinishedAt    string `json:"finished_at,omitempty"`
	Note          string `json:"note,omitempty"`
}

type AppError struct {
	Code       string         `json:"code"`
	ShardID    string         `json:"shard_id,omitempty"`
	Stage      string         `json:"stage,omitempty"`
	Batch      int            `json:"batch,omitempty"`
	Head       int            `json:"head,omitempty"`
	RowBlock   int            `json:"row_block,omitempty"`
	ColBlock   int            `json:"col_block,omitempty"`
	Message    string         `json:"message"`
	Details    map[string]any `json:"details,omitempty"`
	CauseError string         `json:"cause,omitempty"`
}

func (e *AppError) Error() string {
	location := ""
	if e.ShardID != "" {
		location = fmt.Sprintf(" shard=%s", e.ShardID)
	}
	if e.Stage != "" {
		location += fmt.Sprintf(" stage=%s", e.Stage)
	}
	if e.Batch > 0 || e.Head > 0 || e.RowBlock > 0 {
		location += fmt.Sprintf(" b=%d h=%d rb=%d", e.Batch, e.Head, e.RowBlock)
	}
	return fmt.Sprintf("%s%s: %s", e.Code, location, e.Message)
}

func NewAppError(code, message string) *AppError {
	return &AppError{Code: code, Message: message}
}

func AsAppError(err error) (*AppError, bool) {
	if err == nil {
		return nil, false
	}
	if e, ok := err.(*AppError); ok {
		return e, true
	}
	return nil, false
}

func MarshalCanonical(v any) ([]byte, error) {
	return json.Marshal(v)
}
