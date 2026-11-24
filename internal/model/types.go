package model

import (
	"errors"
	"fmt"
)

const (
	SchemaV2 = 2
	SchemaV1 = 1

	ActionRepaint  = "repaint_category"
	ActionReassign = "reassign_instance"
	ActionIgnore   = "ignore"
	ActionUndo     = "undo"
	ActionNoop     = "noop"

	StatusActive   = "active"
	StatusIgnored  = "ignored"
	StatusRecycled = "recycled"
	StatusVoided   = "voided"

	OverlapLastWrite  = "last_write_wins"
	OverlapFirstWrite = "first_write_wins"

	UndoSegmentRecycle = "recycle"
	UndoSegmentVoid    = "void"
)

// Region is a half-open rectangle in image coordinates.
type Region struct {
	X int `json:"x"`
	Y int `json:"y"`
	W int `json:"w"`
	H int `json:"h"`
}

func (r Region) Empty() bool { return r.W <= 0 || r.H <= 0 }

func (r Region) Contains(x, y int) bool {
	return x >= r.X && y >= r.Y && x < r.X+r.W && y < r.Y+r.H
}

func (r Region) Pixels(width, height int) ([]int, *BusinessError) {
	if r.W <= 0 || r.H <= 0 {
		return nil, NewBusinessError("E_REGION_OUT_OF_BOUNDS", "region has non-positive size", map[string]any{
			"x": r.X, "y": r.Y, "first_x": r.X, "first_y": r.Y,
		})
	}
	if r.X < 0 || r.Y < 0 || r.X+r.W > width || r.Y+r.H > height {
		for y := r.Y; y < r.Y+r.H; y++ {
			for x := r.X; x < r.X+r.W; x++ {
				if x < 0 || y < 0 || x >= width || y >= height {
					return nil, NewBusinessError("E_REGION_OUT_OF_BOUNDS", "region exceeds shard bounds", map[string]any{
						"x": x, "y": y, "first_x": x, "first_y": y,
						"width": width, "height": height,
					})
				}
			}
		}
		return nil, NewBusinessError("E_REGION_OUT_OF_BOUNDS", "region exceeds shard bounds", map[string]any{
			"first_x": r.X, "first_y": r.Y, "width": width, "height": height,
		})
	}
	out := make([]int, 0, r.W*r.H)
	for y := r.Y; y < r.Y+r.H; y++ {
		for x := r.X; x < r.X+r.W; x++ {
			out = append(out, y*width+x)
		}
	}
	return out, nil
}

// Revision is the normalized, replayable v2 form. Optional fields use zero as
// unspecified; category and segment identifiers are always positive.
type Revision struct {
	Revision            int      `json:"revision"`
	ParentRevision      int      `json:"parent_revision,omitempty"`
	Action              string   `json:"action"`
	Region              Region   `json:"region"`
	CategoryID          int      `json:"category_id,omitempty"`
	TargetSegmentID     int      `json:"target_segment_id,omitempty"`
	InstanceNo          int      `json:"instance_no,omitempty"`
	Confidence          float64  `json:"confidence,omitempty"`
	Actor               string   `json:"actor,omitempty"`
	UndoOf              int      `json:"undo_of,omitempty"`
	Phrase              string   `json:"phrase,omitempty"`
	SourceVersion       int      `json:"source_version,omitempty"`
	InferenceBasis      string   `json:"inference_basis,omitempty"`
	UnrecoverableReason string   `json:"unrecoverable_reason,omitempty"`
	SubRegions          []Region `json:"sub_regions,omitempty"`
	Note                string   `json:"note,omitempty"`
}

type Log struct {
	SchemaVersion int        `json:"schema_version"`
	LogID         string     `json:"log_id"`
	ShardID       string     `json:"shard_id"`
	Width         int        `json:"width"`
	Height        int        `json:"height"`
	ForkOf        string     `json:"fork_of,omitempty"`
	Branch        string     `json:"branch,omitempty"`
	Revisions     []Revision `json:"revisions"`
}

type SegmentEvent struct {
	Revision int      `json:"revision"`
	Action   string   `json:"action"`
	Event    string   `json:"event"`
	Before   *Segment `json:"before,omitempty"`
	After    *Segment `json:"after,omitempty"`
	Basis    string   `json:"basis,omitempty"`
}

type Segment struct {
	ID              int            `json:"id"`
	CategoryID      int            `json:"category_id"`
	InstanceNo      int            `json:"instance_no"`
	Status          string         `json:"status"`
	PixelCount      int            `json:"pixel_count"`
	Source          string         `json:"source"`
	CreatedRevision int            `json:"created_revision"`
	UpdatedRevision int            `json:"updated_revision"`
	History         []SegmentEvent `json:"history,omitempty"`
}

type PixelChange struct {
	Index       int    `json:"index"`
	X           int    `json:"x"`
	Y           int    `json:"y"`
	BeforeID    int    `json:"before_segment_id"`
	AfterID     int    `json:"after_segment_id"`
	BeforeClaim int    `json:"before_claim_revision,omitempty"`
	AfterClaim  int    `json:"after_claim_revision,omitempty"`
	Revision    int    `json:"revision"`
	Action      string `json:"action"`
}

type SegmentAttributeChange struct {
	SegmentID        int    `json:"segment_id"`
	BeforeCategoryID int    `json:"before_category_id,omitempty"`
	AfterCategoryID  int    `json:"after_category_id,omitempty"`
	BeforeInstanceNo int    `json:"before_instance_no,omitempty"`
	AfterInstanceNo  int    `json:"after_instance_no,omitempty"`
	BeforeStatus     string `json:"before_status,omitempty"`
	AfterStatus      string `json:"after_status,omitempty"`
	BeforePixelCount int    `json:"before_pixel_count"`
	AfterPixelCount  int    `json:"after_pixel_count"`
	Revision         int    `json:"revision"`
	Action           string `json:"action"`
}

type RevisionRecord struct {
	Revision        int                      `json:"revision"`
	ParentRevision  int                      `json:"parent_revision"`
	Action          string                   `json:"action"`
	Status          string                   `json:"status"` // active or undone; undo markers are active
	UndoOf          int                      `json:"undo_of,omitempty"`
	DerivedSegments []int                    `json:"derived_segments,omitempty"`
	PixelChanges    []PixelChange            `json:"pixel_changes,omitempty"`
	SegmentChanges  []SegmentAttributeChange `json:"segment_changes,omitempty"`
	PixelDigest     string                   `json:"pixel_digest"`
	SegmentDigest   string                   `json:"segment_digest"`
	SnapshotDigest  string                   `json:"snapshot_digest"`
	RevisionFP      string                   `json:"revision_fp"`
	LogPrefixFP     string                   `json:"log_prefix_fp"`
	InferenceBasis  string                   `json:"inference_basis,omitempty"`
	Unrecoverable   string                   `json:"unrecoverable,omitempty"`
	ForkDecision    string                   `json:"fork_decision,omitempty"`
}

type ForkDecision struct {
	Revision       int    `json:"revision"`
	ParentRevision int    `json:"parent_revision"`
	HeadRevision   int    `json:"head_revision"`
	Decision       string `json:"decision"`
	Reason         string `json:"reason"`
}

type ReplayState struct {
	ShardID          string           `json:"shard_id"`
	InputFingerprint string           `json:"input_fingerprint"`
	Width            int              `json:"width"`
	Height           int              `json:"height"`
	Pixels           []int            `json:"pixels"`
	Segments         map[int]*Segment `json:"segments"`
	NextSegmentID    int              `json:"next_segment_id"`
	FreeSegmentIDs   []int            `json:"free_segment_ids,omitempty"`
	NextInstanceNo   map[int]int      `json:"next_instance_no,omitempty"`
	RevisionRecords  []RevisionRecord `json:"revision_records"`
	Undone           map[int]bool     `json:"undone"`
	WriteClaim       map[int]int      `json:"write_claim,omitempty"`
	LastRevision     int              `json:"last_revision"`
	ForkDecisions    []ForkDecision   `json:"fork_decisions,omitempty"`
}

type Snapshot struct {
	ShardID          string    `json:"shard_id"`
	Revision         int       `json:"revision"`
	Width            int       `json:"width"`
	Height           int       `json:"height"`
	InputFingerprint string    `json:"input_fingerprint"`
	LogPrefixFP      string    `json:"log_prefix_fp"`
	RevisionFP       string    `json:"revision_fp"`
	PixelDigest      string    `json:"pixel_digest"`
	SegmentDigest    string    `json:"segment_table_digest"`
	SnapshotDigest   string    `json:"snapshot_digest"`
	Pixels           []int     `json:"pixels"`
	Segments         []Segment `json:"segments"`
}

type ManifestShard struct {
	ShardID        string    `json:"shard_id"`
	Image          string    `json:"image"`
	ImageSHA256    string    `json:"image_sha256"`
	Width          int       `json:"width"`
	Height         int       `json:"height"`
	Log            string    `json:"log"`
	ForkLog        string    `json:"fork_log,omitempty"`
	Segments       []Segment `json:"segments"`
	InitialPixelFP string    `json:"initial_pixel_fp"`
}

type Manifest struct {
	SchemaVersion int             `json:"schema_version"`
	Seed          int64           `json:"seed"`
	Generator     string          `json:"generator"`
	Shards        []ManifestShard `json:"shards"`
}

// BusinessError is serialized at API/CLI boundaries. It intentionally carries
// enough context to locate the first failing pixel and revision.
type BusinessError struct {
	Code     string         `json:"code"`
	Message  string         `json:"message"`
	ShardID  string         `json:"shard_id,omitempty"`
	Revision int            `json:"revision,omitempty"`
	Reason   string         `json:"reason,omitempty"`
	Details  map[string]any `json:"details,omitempty"`
}

func (e *BusinessError) Error() string {
	if e == nil {
		return "<nil>"
	}
	loc := ""
	if e.ShardID != "" {
		loc += " shard=" + e.ShardID
	}
	if e.Revision != 0 {
		loc += fmt.Sprintf(" revision=%d", e.Revision)
	}
	return e.Code + loc + ": " + e.Message
}

func NewBusinessError(code, message string, details map[string]any) *BusinessError {
	return &BusinessError{Code: code, Message: message, Reason: message, Details: details}
}

func WithContext(err *BusinessError, shardID string, revision int) *BusinessError {
	if err == nil {
		return nil
	}
	out := *err
	out.ShardID = shardID
	if revision != 0 {
		out.Revision = revision
	}
	if out.Details == nil {
		out.Details = map[string]any{}
	}
	if _, ok := out.Details["shard_id"]; !ok && shardID != "" {
		out.Details["shard_id"] = shardID
	}
	if revision != 0 {
		if _, ok := out.Details["revision"]; !ok {
			out.Details["revision"] = revision
		}
	}
	return &out
}

func AsBusinessError(err error) *BusinessError {
	if err == nil {
		return nil
	}
	var be *BusinessError
	if errors.As(err, &be) {
		return be
	}
	return NewBusinessError("E_INTERNAL", err.Error(), nil)
}
