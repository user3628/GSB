package revlog

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"maskrevision/internal/model"
)

type ParseOptions struct {
	Upgrade UpgradeOptions
}

type Parsed struct {
	Log      model.Log
	Upgrade  *UpgradeReport
	RawV1    []byte
	SourceFP string
}

func ParseFile(path string) (model.Log, *UpgradeReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return model.Log{}, nil, err
	}
	return Parse(data)
}

func Parse(data []byte) (model.Log, *UpgradeReport, error) {
	var header struct {
		SchemaVersion int `json:"schema_version"`
		Version       int `json:"version"`
		LegacyVersion int `json:"legacy_version"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return model.Log{}, nil, fmt.Errorf("decode log header: %w", err)
	}
	version := header.SchemaVersion
	if version == 0 {
		version = header.Version
	}
	if version == 0 {
		version = header.LegacyVersion
	}
	switch version {
	case model.SchemaV2:
		var log model.Log
		if err := json.Unmarshal(data, &log); err != nil {
			return model.Log{}, nil, fmt.Errorf("decode v2 log: %w", err)
		}
		if err := ValidateV2(log); err != nil {
			return model.Log{}, nil, err
		}
		return log, nil, nil
	case model.SchemaV1:
		return model.Log{}, nil, fmt.Errorf("v1 log requires initial pixel map; use UpgradeV1")
	default:
		return model.Log{}, nil, fmt.Errorf("unsupported schema_version %d", version)
	}
}

func ValidateV2(log model.Log) error {
	if log.SchemaVersion != model.SchemaV2 {
		return fmt.Errorf("expected schema_version 2, got %d", log.SchemaVersion)
	}
	if log.ShardID == "" || log.LogID == "" {
		return fmt.Errorf("shard_id and log_id are required")
	}
	if log.Width <= 0 || log.Height <= 0 {
		return fmt.Errorf("width and height must be positive")
	}
	seen := map[int]bool{}
	prev := 0
	for i := range log.Revisions {
		r := &log.Revisions[i]
		if r.Revision <= 0 || seen[r.Revision] {
			return fmt.Errorf("revision %d is invalid or duplicated", r.Revision)
		}
		if r.Revision <= prev {
			return fmt.Errorf("revisions must be strictly increasing: %d after %d", r.Revision, prev)
		}
		seen[r.Revision] = true
		prev = r.Revision
		if r.SourceVersion == 0 {
			r.SourceVersion = model.SchemaV2
		}
		switch r.Action {
		case model.ActionRepaint:
			if r.CategoryID <= 0 {
				return fmt.Errorf("revision %d repaint requires category_id", r.Revision)
			}
		case model.ActionReassign:
			if r.TargetSegmentID <= 0 && r.UnrecoverableReason == "" {
				return fmt.Errorf("revision %d reassign requires target_segment_id", r.Revision)
			}
		case model.ActionIgnore, model.ActionUndo, model.ActionNoop:
		default:
			return fmt.Errorf("revision %d has unsupported action %q", r.Revision, r.Action)
		}
		if r.Action != model.ActionUndo && r.Action != model.ActionNoop {
			if r.Region.W <= 0 || r.Region.H <= 0 {
				return fmt.Errorf("revision %d requires a non-empty region", r.Revision)
			}
			if r.Region.X < 0 || r.Region.Y < 0 || r.Region.X+r.Region.W > log.Width || r.Region.Y+r.Region.H > log.Height {
				return fmt.Errorf("revision %d region is outside the shard", r.Revision)
			}
		}
		if r.Confidence < 0 || r.Confidence > 1 {
			return fmt.Errorf("revision %d confidence must be a fraction in [0,1]", r.Revision)
		}
	}
	return nil
}

func EncodeV2(log model.Log) ([]byte, error) {
	log.SchemaVersion = model.SchemaV2
	data, err := json.MarshalIndent(log, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func revisionFingerprints(log model.Log) ([]string, string) {
	fps := make([]string, 0, len(log.Revisions))
	prefixRevs := make([]model.Revision, 0, len(log.Revisions))
	for _, r := range log.Revisions {
		prefixRevs = append(prefixRevs, r)
		fps = append(fps, model.RevisionDigest(r))
	}
	return fps, model.LogPrefixDigest(prefixRevs)
}

func sortedUnique(xs []int) []int {
	sort.Ints(xs)
	out := xs[:0]
	for _, x := range xs {
		if len(out) == 0 || out[len(out)-1] != x {
			out = append(out, x)
		}
	}
	return out
}

func trimPhrase(s string) string { return strings.TrimSpace(s) }
