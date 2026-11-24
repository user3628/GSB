package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
)

const (
	ManifestVersion = "motion-manifest/v1"
	PolicyVersion   = 1
	VirtualStepCost = 1 // deterministic work-milliseconds charged per prediction step
)

// MotionState is the complete, serializable state of one rolling motion
// hypothesis. All prediction branches start from a copy of the same state.
type MotionState struct {
	Step         int     `json:"step"`
	X            float64 `json:"x"`
	Y            float64 `json:"y"`
	VX           float64 `json:"vx"`
	VY           float64 `json:"vy"`
	ScaleX       float64 `json:"scale_x"`
	ScaleY       float64 `json:"scale_y"`
	Rotation     float64 `json:"rotation"`
	EventApplied bool    `json:"event_applied"`
}

type FloatRange struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

type StateRange struct {
	X      FloatRange `json:"x"`
	Y      FloatRange `json:"y"`
	VX     FloatRange `json:"vx"`
	VY     FloatRange `json:"vy"`
	ScaleX FloatRange `json:"scale_x"`
	ScaleY FloatRange `json:"scale_y"`
}

type Rect struct {
	X int `json:"x"`
	Y int `json:"y"`
	W int `json:"w"`
	H int `json:"h"`
}

type SceneSpec struct {
	BackgroundSeed int64 `json:"background_seed"`
	SquareRect     Rect  `json:"square_rect"`
	BlockRect      Rect  `json:"block_rect"`
}

// SequenceSpec is the JSON motion inventory entry. InitialState is the shared
// immutable origin for every branch. TotalFrames includes frame zero (truth).
type SequenceSpec struct {
	SequenceID          string      `json:"sequence_id"`
	Name                string      `json:"name"`
	Seed                int64       `json:"seed"`
	Width               int         `json:"width"`
	Height              int         `json:"height"`
	TotalFrames         int         `json:"total_frames"`
	PredictSteps        int         `json:"predict_steps"`
	Branches            int         `json:"branches"`
	InitialState        MotionState `json:"initial_state"`
	VelocityDecay       float64     `json:"velocity_decay"`
	DirectionChangeStep int         `json:"direction_change_step"`
	ExpectedStateRange  StateRange  `json:"expected_state_range"`
	Scene               SceneSpec   `json:"scene"`
	Tags                []string    `json:"tags,omitempty"`
}

type Manifest struct {
	Version   string         `json:"version"`
	Seed      int64          `json:"seed"`
	Sequences []SequenceSpec `json:"sequences"`
	Policy    PolicySnapshot `json:"policy"`
}

// PolicySnapshot is an immutable execution-policy snapshot. ParentVersion and
// ParentHash form the version chain used to distinguish advance from replace
// and rollback.
type PolicySnapshot struct {
	Version        int     `json:"version"`
	ParentVersion  int     `json:"parent_version"`
	ParentHash     string  `json:"parent_hash,omitempty"`
	PolicyID       string  `json:"policy_id"`
	Concurrency    int     `json:"global_concurrency"`
	BranchQuota    int     `json:"branch_quota"`
	RoundBudgetMS  int     `json:"round_time_budget_ms"`
	PruneThreshold float64 `json:"prune_threshold"`
}

func (p PolicySnapshot) Hash() string {
	return Digest(p)
}

func (p PolicySnapshot) Validate() error {
	if p.Version < 1 || p.PolicyID == "" {
		return NewError(CodeValidation, "policy version and policy_id are required")
	}
	if p.Concurrency < 1 {
		return NewError(CodeValidation, "global_concurrency must be >= 1")
	}
	if p.BranchQuota < 1 {
		return NewError(CodeValidation, "branch_quota must be >= 1")
	}
	if p.RoundBudgetMS < 1 {
		return NewError(CodeValidation, "round_time_budget_ms must be >= 1")
	}
	if p.PruneThreshold < 0 {
		return NewError(CodeValidation, "prune_threshold must be >= 0 (0 disables pruning)")
	}
	return nil
}

type RunConfig struct {
	SequenceID      string  `json:"sequence_id"`
	RequestedSteps  int     `json:"requested_steps"`
	Concurrency     int     `json:"concurrency"`
	BranchQuota     int     `json:"branch_quota"`
	RoundBudgetMS   int     `json:"round_time_budget_ms"`
	PruneThreshold  float64 `json:"prune_threshold"`
	DisablePrune    bool    `json:"disable_prune,omitempty"`
	StageMode       bool    `json:"stage_mode,omitempty"`
	BranchFilter    []int   `json:"branch_filter,omitempty"`
	VirtualTimeMode bool    `json:"virtual_time_mode"`
}

func (c RunConfig) Normalize(spec SequenceSpec, policy PolicySnapshot) (RunConfig, error) {
	if c.SequenceID == "" {
		c.SequenceID = spec.SequenceID
	}
	if c.RequestedSteps == 0 {
		c.RequestedSteps = spec.PredictSteps
	}
	if c.Concurrency == 0 {
		c.Concurrency = policy.Concurrency
	}
	if c.BranchQuota == 0 {
		c.BranchQuota = policy.BranchQuota
	}
	if c.RoundBudgetMS == 0 {
		c.RoundBudgetMS = policy.RoundBudgetMS
	}
	if c.PruneThreshold == 0 {
		c.PruneThreshold = policy.PruneThreshold
	}
	c.VirtualTimeMode = true
	if c.RequestedSteps < 1 || c.Concurrency < 1 || c.BranchQuota < 1 || c.RoundBudgetMS < 1 {
		return c, NewError(CodeValidation, "requested_steps, concurrency, branch_quota and round budget must be >= 1")
	}
	if c.PruneThreshold < 0 {
		return c, NewError(CodeValidation, "prune_threshold must be >= 0")
	}
	if c.RequestedSteps > spec.TotalFrames-1 {
		err := NewError(CodeTimelineOverflow,
			fmt.Sprintf("requested %d prediction steps but truth timeline has %d steps", c.RequestedSteps, spec.TotalFrames-1))
		err.SequenceID = spec.SequenceID
		err.Details = map[string]any{
			"requested_steps": c.RequestedSteps,
			"truth_steps":     spec.TotalFrames - 1,
			"overflow_steps":  c.RequestedSteps - (spec.TotalFrames - 1),
		}
		return c, err
	}
	return c, nil
}

func (c RunConfig) BranchRequestedSteps() int {
	if c.BranchQuota < c.RequestedSteps {
		return c.BranchQuota
	}
	return c.RequestedSteps
}

func (c RunConfig) EffectiveStepsForBranch(policyQuota int) int {
	quota := c.BranchQuota
	if policyQuota > 0 && policyQuota < quota {
		quota = policyQuota
	}
	if quota < c.RequestedSteps {
		return quota
	}
	return c.RequestedSteps
}

type BranchProfile struct {
	BranchID      int     `json:"branch_id"`
	VelocityScale float64 `json:"velocity_scale"`
	TurnX         float64 `json:"turn_x"`
	TurnY         float64 `json:"turn_y"`
	DecayScale    float64 `json:"decay_scale"`
	DeformDrift   float64 `json:"deform_drift"`
	Description   string  `json:"description"`
}

type BranchPlan struct {
	BranchID        int    `json:"branch_id"`
	RequestedSteps  int    `json:"requested_steps"`
	TargetSteps     int    `json:"target_steps"`
	Wave            int    `json:"wave"`
	Admitted        bool   `json:"admitted"`
	Reason          string `json:"reason,omitempty"`
	EstimatedWorkMS int    `json:"estimated_work_ms"`
}

type BranchStatus string

const (
	BranchPending      BranchStatus = "pending"
	BranchRunning      BranchStatus = "running"
	BranchCompleted    BranchStatus = "completed"
	BranchQuotaLimited BranchStatus = "quota_limited"
	BranchPruned       BranchStatus = "pruned"
	BranchNotStarted   BranchStatus = "not_started"
	BranchStopped      BranchStatus = "stopped"
	BranchFailed       BranchStatus = "failed"
)

func (s BranchStatus) Terminal() bool {
	switch s {
	case BranchCompleted, BranchQuotaLimited, BranchPruned, BranchNotStarted, BranchFailed:
		return true
	default:
		return false
	}
}

type StepRecord struct {
	Step         int         `json:"step"`
	State        MotionState `json:"state"`
	StateSHA256  string      `json:"state_sha256"`
	FrameSHA256  string      `json:"frame_sha256"`
	TruthSHA256  string      `json:"truth_sha256"`
	MAE          float64     `json:"mae"`
	RangeOK      bool        `json:"range_ok"`
	Decision     string      `json:"decision"`
	CommittedSeq int64       `json:"committed_seq,omitempty"`
}

type BranchReport struct {
	BranchID        int          `json:"branch_id"`
	Status          BranchStatus `json:"status"`
	Reason          string       `json:"reason,omitempty"`
	Wave            int          `json:"wave"`
	RequestedSteps  int          `json:"requested_steps"`
	TargetSteps     int          `json:"target_steps"`
	StepsCommitted  int          `json:"steps_committed"`
	QuotaUsed       int          `json:"quota_used"`
	ReclaimedQuota  int          `json:"reclaimed_quota"`
	FinalMAE        float64      `json:"final_mae"`
	MeanMAE         float64      `json:"mean_mae"`
	BestStepMAE     float64      `json:"best_step_mae"`
	WorstStepMAE    float64      `json:"worst_step_mae"`
	FinalState      *MotionState `json:"final_state,omitempty"`
	FinalStateSHA   string       `json:"final_state_sha256,omitempty"`
	FinalFrameSHA   string       `json:"final_frame_sha256,omitempty"`
	ErrorCurve      []float64    `json:"error_curve"`
	RangeViolations []int        `json:"range_violations,omitempty"`
	Steps           []StepRecord `json:"steps,omitempty"`
}

type RunSummary struct {
	Completed      int     `json:"completed"`
	Pruned         int     `json:"pruned"`
	NotStarted     int     `json:"not_started"`
	Stopped        int     `json:"stopped"`
	BestBranch     int     `json:"best_branch"`
	BestMeanMAE    float64 `json:"best_mean_mae"`
	CompletedSteps int     `json:"completed_steps"`
	ReclaimedQuota int     `json:"reclaimed_quota"`
	CheckpointSeq  int64   `json:"checkpoint_seq"`
}

type RunReport struct {
	RunID             string         `json:"run_id"`
	SequenceID        string         `json:"sequence_id"`
	Status            string         `json:"status"`
	PolicyVersion     int            `json:"policy_version"`
	PolicyHash        string         `json:"policy_hash"`
	PolicyFingerprint string         `json:"policy_fingerprint"`
	ConfigSHA256      string         `json:"config_sha256"`
	Config            RunConfig      `json:"config"`
	InitialStateSHA   string         `json:"initial_state_sha256"`
	InitialFrameSHA   string         `json:"initial_frame_sha256"`
	Branches          []BranchReport `json:"branches"`
	Summary           RunSummary     `json:"summary"`
	PruneAudit        []string       `json:"prune_audit,omitempty"`
	DeterministicSHA  string         `json:"deterministic_sha256"`
	Recovered         bool           `json:"recovered,omitempty"`
	RecoveryCount     int            `json:"recovery_count,omitempty"`
	CreatedAt         string         `json:"created_at,omitempty"`
	UpdatedAt         string         `json:"updated_at,omitempty"`
}

// CanonicalReport removes presentation/recovery metadata before hashing or
// byte-level comparison.
func CanonicalReport(r RunReport) RunReport {
	r.DeterministicSHA = ""
	r.Recovered = false
	r.RecoveryCount = 0
	r.CreatedAt = ""
	r.UpdatedAt = ""
	for i := range r.Branches {
		for j := range r.Branches[i].Steps {
			// The global checkpoint sequence reflects physical transaction
			// interleaving under concurrency. It is audit data, not part of the
			// logical deterministic result.
			r.Branches[i].Steps[j].CommittedSeq = 0
		}
	}
	return r
}

func (r RunReport) ComputeFingerprint() string {
	return Digest(CanonicalReport(r))
}

type ErrorCode string

const (
	CodeValidation       ErrorCode = "E_VALIDATION"
	CodeNotFound         ErrorCode = "E_NOT_FOUND"
	CodeTimelineOverflow ErrorCode = "E_TIMELINE_OVERFLOW"
	CodeSharedFrameWrite ErrorCode = "E_SHARED_FRAME_WRITE"
	CodeStaleCheckpoint  ErrorCode = "E_STALE_CHECKPOINT"
	CodePolicyRollback   ErrorCode = "E_POLICY_ROLLBACK"
	CodePolicyConflict   ErrorCode = "E_POLICY_CONFLICT"
	CodeRunBusy          ErrorCode = "E_RUN_BUSY"
	CodeRunStopped       ErrorCode = "E_RUN_STOPPED"
	CodeBranchFailed     ErrorCode = "E_BRANCH_FAILED"
	CodeInternal         ErrorCode = "E_INTERNAL"
)

type AppError struct {
	Code       ErrorCode      `json:"code"`
	Message    string         `json:"message"`
	SequenceID string         `json:"sequence_id,omitempty"`
	BranchID   *int           `json:"branch_id,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
	Status     int            `json:"-"`
}

func (e *AppError) Error() string {
	if e.SequenceID != "" {
		return fmt.Sprintf("%s: %s (sequence=%s)", e.Code, e.Message, e.SequenceID)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *AppError) HTTPStatus() int {
	if e.Status != 0 {
		return e.Status
	}
	switch e.Code {
	case CodeNotFound:
		return http.StatusNotFound
	case CodeStaleCheckpoint, CodePolicyConflict, CodePolicyRollback, CodeRunBusy:
		return http.StatusConflict
	case CodeTimelineOverflow, CodeSharedFrameWrite:
		return http.StatusUnprocessableEntity
	case CodeValidation:
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

func NewError(code ErrorCode, message string) *AppError {
	return &AppError{Code: code, Message: message}
}

func WithBranch(err *AppError, sequenceID string, branchID int) *AppError {
	err.SequenceID = sequenceID
	b := branchID
	err.BranchID = &b
	return err
}

func AsAppError(err error) *AppError {
	if err == nil {
		return nil
	}
	if e, ok := err.(*AppError); ok {
		return e
	}
	return NewError(CodeInternal, err.Error())
}

func Digest(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func DigestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func CanonicalJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func Quantize8(v float64) uint8 {
	if math.IsNaN(v) || v <= 0 {
		return 0
	}
	if v >= 1 {
		return 255
	}
	return uint8(int(math.Floor(v*255 + 0.5)))
}

func SortedUniqueInts(v []int) []int {
	if len(v) == 0 {
		return nil
	}
	seen := make(map[int]struct{}, len(v))
	out := make([]int, 0, len(v))
	for _, n := range v {
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	sort.Ints(out)
	return out
}
