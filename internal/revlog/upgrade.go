package revlog

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"maskrevision/internal/model"
)

type PhraseRule struct {
	Contains string `json:"contains"`
	Action   string `json:"action"`
}

type UpgradeOptions struct {
	PhraseRules         []PhraseRule `json:"phrase_rules"`
	NeighborRadius      int          `json:"neighbor_radius"`
	Width               int          `json:"-"`
	Height              int          `json:"-"`
	Pixels              []int        `json:"-"`
	TopConfidencePct    int          `json:"-"`
	StrictUnrecoverable bool         `json:"strict_unrecoverable"`
}

type UpgradeInference struct {
	Revision            int     `json:"revision"`
	OriginalPhrase      string  `json:"original_phrase"`
	InferredAction      string  `json:"inferred_action"`
	TargetSegmentID     int     `json:"target_segment_id,omitempty"`
	ConfidencePercent   int     `json:"confidence_percent"`
	Confidence          float64 `json:"confidence"`
	Basis               string  `json:"basis"`
	UnrecoverableReason string  `json:"unrecoverable_reason,omitempty"`
}

type UpgradeReport struct {
	SourceVersion      int                `json:"source_version"`
	TargetVersion      int                `json:"target_version"`
	ShardID            string             `json:"shard_id"`
	LogID              string             `json:"log_id"`
	PhraseRules        []PhraseRule       `json:"phrase_rules"`
	Inferences         []UpgradeInference `json:"inferences"`
	UnrecoverableCount int                `json:"unrecoverable_count"`
	ConfidenceRule     string             `json:"confidence_rule"`
	NeighborRule       string             `json:"neighbor_rule"`
}

type UpgradeResult struct {
	Log    model.Log     `json:"log"`
	Report UpgradeReport `json:"report"`
}

type rawV1Log struct {
	SchemaVersion     int             `json:"schema_version"`
	Version           int             `json:"version"`
	LogID             string          `json:"log_id"`
	ShardID           string          `json:"shard_id"`
	Width             int             `json:"width"`
	Height            int             `json:"height"`
	ConfidencePercent int             `json:"confidence_percent"`
	Revisions         []rawV1Revision `json:"revisions"`
}

type rawV1Revision struct {
	Revision       int          `json:"revision"`
	ParentRevision int          `json:"parent_revision,omitempty"`
	Action         string       `json:"action,omitempty"`
	ActionPhrase   string       `json:"action_phrase,omitempty"`
	Region         model.Region `json:"region"`
	CategoryID     int          `json:"category_id,omitempty"`
	Confidence     int          `json:"confidence,omitempty"`
	Actor          string       `json:"actor,omitempty"`
	Note           string       `json:"note,omitempty"`
}

func DefaultPhraseRules() []PhraseRule {
	return []PhraseRule{
		{Contains: "撤销", Action: model.ActionUndo},
		{Contains: "置为忽略", Action: model.ActionIgnore},
		{Contains: "忽略", Action: model.ActionIgnore},
		{Contains: "改判", Action: model.ActionReassign},
		{Contains: "实例", Action: model.ActionReassign},
		{Contains: "重涂", Action: model.ActionRepaint},
		{Contains: "类别", Action: model.ActionRepaint},
	}
}

func UpgradeV1(data []byte, opts UpgradeOptions) (UpgradeResult, error) {
	var raw rawV1Log
	if err := json.Unmarshal(data, &raw); err != nil {
		return UpgradeResult{}, fmt.Errorf("decode v1 log: %w", err)
	}
	version := raw.SchemaVersion
	if version == 0 {
		version = raw.Version
	}
	if version != model.SchemaV1 {
		return UpgradeResult{}, fmt.Errorf("expected v1 log, got version %d", version)
	}
	if raw.ShardID == "" {
		return UpgradeResult{}, fmt.Errorf("v1 shard_id is required")
	}
	if opts.Width == 0 {
		opts.Width = raw.Width
	}
	if opts.Height == 0 {
		opts.Height = raw.Height
	}
	if opts.Width != raw.Width || opts.Height != raw.Height || len(opts.Pixels) != opts.Width*opts.Height {
		return UpgradeResult{}, fmt.Errorf("v1 initial pixel map does not match %dx%d", raw.Width, raw.Height)
	}
	if len(opts.PhraseRules) == 0 {
		opts.PhraseRules = DefaultPhraseRules()
	}
	if opts.NeighborRadius <= 0 {
		opts.NeighborRadius = 1
	}
	if opts.TopConfidencePct == 0 {
		opts.TopConfidencePct = raw.ConfidencePercent
	}

	out := model.Log{
		SchemaVersion: model.SchemaV2,
		LogID:         raw.LogID + ".upgraded.v2",
		ShardID:       raw.ShardID,
		Width:         raw.Width,
		Height:        raw.Height,
		Revisions:     make([]model.Revision, 0, len(raw.Revisions)),
	}
	report := UpgradeReport{
		SourceVersion:  model.SchemaV1,
		TargetVersion:  model.SchemaV2,
		ShardID:        raw.ShardID,
		LogID:          out.LogID,
		PhraseRules:    append([]PhraseRule(nil), opts.PhraseRules...),
		ConfidenceRule: "integer_percent/100 -> fraction; 0 means unspecified",
		NeighborRule:   fmt.Sprintf("radius=%d; exclude segment ids inside region; choose max neighbor pixel count, tie lower id", opts.NeighborRadius),
	}
	previous := 0
	for _, rv := range raw.Revisions {
		phrase := trimPhrase(rv.ActionPhrase)
		if phrase == "" {
			phrase = trimPhrase(rv.Action)
		}
		action, ok := classifyPhrase(phrase, opts.PhraseRules)
		if !ok {
			return UpgradeResult{}, fmt.Errorf("revision %d phrase %q is not mapped", rv.Revision, phrase)
		}
		confidencePct := rv.Confidence
		if confidencePct == 0 {
			confidencePct = opts.TopConfidencePct
		}
		if confidencePct < 0 || confidencePct > 100 {
			return UpgradeResult{}, fmt.Errorf("revision %d confidence_percent %d is outside [0,100]", rv.Revision, confidencePct)
		}
		parent := rv.ParentRevision
		if parent == 0 && previous > 0 {
			parent = previous
		}
		norm := model.Revision{
			Revision:       rv.Revision,
			ParentRevision: parent,
			Action:         action,
			Region:         rv.Region,
			CategoryID:     rv.CategoryID,
			Confidence:     float64(confidencePct) / 100.0,
			Actor:          rv.Actor,
			Phrase:         phrase,
			SourceVersion:  model.SchemaV1,
			Note:           rv.Note,
		}
		inference := UpgradeInference{
			Revision:          rv.Revision,
			OriginalPhrase:    phrase,
			InferredAction:    action,
			ConfidencePercent: confidencePct,
			Confidence:        norm.Confidence,
			Basis:             fmt.Sprintf("phrase rule %q -> %s; confidence %d%% -> %.2f", matchingNeedle(phrase, opts.PhraseRules, action), action, confidencePct, norm.Confidence),
		}
		if action == model.ActionReassign {
			target, basis, reason := inferNeighborSegment(opts.Pixels, opts.Width, opts.Height, rv.Region, opts.NeighborRadius)
			norm.TargetSegmentID = target
			norm.InferenceBasis = basis
			inference.TargetSegmentID = target
			inference.Basis += "; " + basis
			if target == 0 {
				norm.UnrecoverableReason = reason
				norm.Action = model.ActionNoop
				inference.UnrecoverableReason = reason
				inference.Basis += "; unrecoverable: " + reason
				report.UnrecoverableCount++
				if opts.StrictUnrecoverable {
					return UpgradeResult{}, fmt.Errorf("revision %d: %s", rv.Revision, reason)
				}
			}
		}
		if action == model.ActionUndo {
			norm.ParentRevision = rv.ParentRevision
			if norm.ParentRevision == 0 && previous > 0 {
				norm.ParentRevision = previous
			}
		}
		out.Revisions = append(out.Revisions, norm)
		report.Inferences = append(report.Inferences, inference)
		previous = rv.Revision
	}
	if err := ValidateV2(out); err != nil {
		return UpgradeResult{}, err
	}
	return UpgradeResult{Log: out, Report: report}, nil
}

func classifyPhrase(phrase string, rules []PhraseRule) (string, bool) {
	for _, rule := range rules {
		if rule.Contains != "" && strings.Contains(phrase, rule.Contains) {
			return rule.Action, true
		}
	}
	return "", false
}

func matchingNeedle(phrase string, rules []PhraseRule, action string) string {
	for _, rule := range rules {
		if rule.Action == action && strings.Contains(phrase, rule.Contains) {
			return rule.Contains
		}
	}
	return ""
}

func inferNeighborSegment(pixels []int, width, height int, region model.Region, radius int) (int, string, string) {
	if region.W <= 0 || region.H <= 0 || region.X < 0 || region.Y < 0 || region.X+region.W > width || region.Y+region.H > height {
		return 0, "invalid region", "region outside shard bounds"
	}
	inside := map[int]bool{}
	for y := region.Y; y < region.Y+region.H; y++ {
		for x := region.X; x < region.X+region.W; x++ {
			inside[pixels[y*width+x]] = true
		}
	}
	counts := map[int]int{}
	excluded := map[int]bool{0: true}
	for id := range inside {
		excluded[id] = true
	}
	minX, minY := region.X-radius, region.Y-radius
	maxX, maxY := region.X+region.W+radius-1, region.Y+region.H+radius-1
	for y := minY; y <= maxY; y++ {
		for x := minX; x <= maxX; x++ {
			if x < 0 || y < 0 || x >= width || y >= height {
				continue
			}
			if region.Contains(x, y) {
				continue
			}
			id := pixels[y*width+x]
			if !excluded[id] {
				counts[id]++
			}
		}
	}
	if len(counts) == 0 {
		return 0, fmt.Sprintf("inside=%s; no differing segment in radius %d", idList(inside), radius), "no neighboring segment different from the source region"
	}
	ids := make([]int, 0, len(counts))
	for id := range counts {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if counts[ids[i]] != counts[ids[j]] {
			return counts[ids[i]] > counts[ids[j]]
		}
		return ids[i] < ids[j]
	})
	var cand strings.Builder
	for i, id := range ids {
		if i > 0 {
			cand.WriteByte(',')
		}
		fmt.Fprintf(&cand, "%d:%d", id, counts[id])
	}
	basis := fmt.Sprintf("inside=%s; neighbor radius=%d counts=%s; chosen=%d", idList(inside), radius, cand.String(), ids[0])
	return ids[0], basis, ""
}

func idList(m map[int]bool) string {
	ids := make([]int, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(parts, ",")
}
