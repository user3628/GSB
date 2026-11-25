package tm

import (
	"encoding/json"
	"fmt"
	"strings"
)

type QualityMark struct {
	Raw        string `json:"raw,omitempty"`
	Label      string `json:"label"`
	Rank       int    `json:"rank"`
	Valid      bool   `json:"valid"`
	OutOfRange bool   `json:"out_of_range,omitempty"`
}

func (q *QualityMark) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		q.Raw = s
		q.normalizeString(s)
		return nil
	}
	var n int
	if err := json.Unmarshal(data, &n); err == nil {
		q.Raw = fmt.Sprintf("%d", n)
		if n < 0 || n > 100 {
			q.Label = "out_of_range"
			q.Rank = 1
			q.Valid = false
			q.OutOfRange = true
			return nil
		}
		switch {
		case n >= 80:
			q.Label = "preferred"
			q.Rank = 3
		case n >= 50:
			q.Label = "reviewed"
			q.Rank = 2
		default:
			q.Label = "draft"
			q.Rank = 1
		}
		q.Valid = true
		return nil
	}
	var obj struct {
		Label string `json:"label"`
		Score *int   `json:"score"`
	}
	if err := json.Unmarshal(data, &obj); err == nil {
		q.Raw = obj.Label
		if obj.Score != nil {
			n = *obj.Score
			if n < 0 || n > 100 {
				q.OutOfRange = true
				q.Valid = false
				q.Label = "out_of_range"
				q.Rank = 1
				return nil
			}
		}
		q.normalizeString(obj.Label)
		return nil
	}
	q.Raw = strings.Trim(string(data), `"`)
	q.normalizeString(q.Raw)
	return nil
}

func (q *QualityMark) normalizeString(s string) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "preferred", "good", "approved", "3":
		q.Label = "preferred"
		q.Rank = 3
		q.Valid = true
	case "reviewed", "ok", "needs_review", "2":
		q.Label = "reviewed"
		q.Rank = 2
		q.Valid = true
	case "draft", "bad", "rejected", "1", "0":
		q.Label = "draft"
		q.Rank = 1
		q.Valid = true
	default:
		q.Label = "unknown"
		q.Rank = 0
		q.Valid = false
		q.OutOfRange = true
	}
}

type SegmentRecord struct {
	SegmentID  string      `json:"segment_id"`
	SourceLang string      `json:"source_lang"`
	TargetLang string      `json:"target_lang"`
	SourceText string      `json:"source_text"`
	TargetText string      `json:"target_text"`
	UpdateSeq  int64       `json:"update_seq"`
	Quality    QualityMark `json:"quality"`
	SourceFile string      `json:"source_file,omitempty"`
}

type Change struct {
	SegmentID     string       `json:"segment_id"`
	NewTargetText string       `json:"new_target_text"`
	ChangeType    string       `json:"change_type"`
	BaseVersion   int64        `json:"base_version"`
	UpdateSeq     int64        `json:"update_seq,omitempty"`
	Quality       *QualityMark `json:"quality,omitempty"`
}

type Increment struct {
	IncrementID string   `json:"increment_id"`
	BaseVersion int64    `json:"base_version"`
	Changes     []Change `json:"changes"`
}

type ReasonChainEntry struct {
	Code       string         `json:"code"`
	Message    string         `json:"message"`
	EntityType string         `json:"entity_type"`
	EntityID   string         `json:"entity_id"`
	Version    int64          `json:"version"`
	Details    map[string]any `json:"details,omitempty"`
}

type StageStatus struct {
	Stage          string `json:"stage"`
	SegmentID      string `json:"segment_id,omitempty"`
	Status         string `json:"status"`
	Version        int64  `json:"version"`
	ContentFP      string `json:"content_fp,omitempty"`
	ArtifactDigest string `json:"artifact_digest,omitempty"`
	SeqNo          int64  `json:"seq_no"`
}
