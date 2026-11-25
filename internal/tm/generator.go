package tm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
)

const GeneratorSeed int64 = 2026112601

type GeneratedDataset struct {
	Seed           int64           `json:"seed"`
	Dir            string          `json:"dir"`
	SourceFiles    []string        `json:"source_files"`
	IncrementFiles []string        `json:"increment_files"`
	Records        []SegmentRecord `json:"records"`
}

type incrementLine struct {
	IncrementID   string `json:"increment_id"`
	BaseVersion   int64  `json:"base_version"`
	SegmentID     string `json:"segment_id"`
	ChangeType    string `json:"change_type"`
	NewTargetText string `json:"new_target_text"`
}

func seededRecords(r *rand.Rand) []SegmentRecord {
	longClauses := 36 + r.Intn(12)
	longSource := ""
	longTarget := ""
	for i := 1; i <= longClauses; i++ {
		longSource += fmt.Sprintf("第%02d项任务TASK-%04d已经通过校验。", i, 1000+i)
		longTarget += fmt.Sprintf("Item %02d TASK-%04d has passed validation. ", i, 1000+i)
	}

	return []SegmentRecord{
		{
			SegmentID: "seg-001", SourceLang: "zh-CN", TargetLang: "en-US",
			SourceText: "请立即提交。系统已保存您的偏好设置。",
			TargetText: "Submit. The system has saved your preferences.",
			UpdateSeq:  1, Quality: QualityMark{Label: "preferred", Rank: 3, Valid: true},
		},
		{
			SegmentID: "seg-002", SourceLang: "zh-CN", TargetLang: "en-US",
			SourceText: "点击 OK，然后提交表单。",
			TargetText: "Click OK, then submit the form.",
			UpdateSeq:  2, Quality: QualityMark{Label: "reviewed", Rank: 2, Valid: true},
		},
		{
			SegmentID: "seg-003-empty-target", SourceLang: "zh-CN", TargetLang: "en-US",
			SourceText: "这条记录暂时没有译文。",
			TargetText: "",
			UpdateSeq:  3, Quality: QualityMark{Label: "draft", Rank: 1, Valid: true},
		},
		{
			SegmentID: "seg-004", SourceLang: "zh-CN", TargetLang: "en-US",
			SourceText: "请立即提交。配置稍后自动更新。",
			TargetText: "Submit. Configuration will update shortly.",
			UpdateSeq:  4, Quality: QualityMark{Label: "reviewed", Rank: 2, Valid: true},
		},
		{
			SegmentID: "seg-005", SourceLang: "zh-CN", TargetLang: "en-US",
			SourceText: "请立即提交。其他更改将在重启后生效。",
			TargetText: "Submit. Other changes take effect after restart.",
			UpdateSeq:  5, Quality: QualityMark{Label: "preferred", Rank: 3, Valid: true},
		},
		{
			SegmentID: "seg-006", SourceLang: "zh-CN", TargetLang: "en-US",
			SourceText: "请及时保存。当前草稿已经加密。",
			TargetText: "Save. The current draft is encrypted.",
			UpdateSeq:  6, Quality: QualityMark{Label: "reviewed", Rank: 2, Valid: true},
		},
		{
			SegmentID: "seg-007-long", SourceLang: "zh-CN", TargetLang: "en-US",
			SourceText: longSource,
			TargetText: longTarget,
			UpdateSeq:  7, Quality: QualityMark{Label: "preferred", Rank: 3, Valid: true},
		},
		{
			SegmentID: "seg-008-punct-only", SourceLang: "zh-CN", TargetLang: "en-US",
			SourceText: "有效前缀。",
			TargetText: "。。。！！",
			UpdateSeq:  8, Quality: QualityMark{Label: "unknown", Rank: 0, Valid: false, OutOfRange: true},
		},
		{
			SegmentID: "seg-009", SourceLang: "zh-CN", TargetLang: "en-US",
			SourceText: "请及时保存。可以安全关闭页面。",
			TargetText: "Save. It is safe to close the page.",
			UpdateSeq:  9, Quality: QualityMark{Label: "preferred", Rank: 3, Valid: true},
		},
		{
			SegmentID: "seg-010-mixed-marker", SourceLang: "zh-CN", TargetLang: "en-US",
			SourceText: "构建编号 BUILD-7788 已完成，测试覆盖率为 96%。",
			TargetText: "Build BUILD-7788 is complete; test coverage is 96%.",
			UpdateSeq:  10, Quality: QualityMark{Raw: "4096", Label: "out_of_range", Rank: 1, Valid: false, OutOfRange: true},
		},
		{
			SegmentID: "seg-011", SourceLang: "zh-CN", TargetLang: "en-US",
			SourceText: "同步任务在后台运行，完成后会通知您。",
			TargetText: "The sync job runs in the background and will notify you when complete.",
			UpdateSeq:  11, Quality: QualityMark{Label: "reviewed", Rank: 2, Valid: true},
		},
		{
			SegmentID: "seg-012", SourceLang: "zh-CN", TargetLang: "en-US",
			SourceText: "恢复过程只读取已经提交的检查点。",
			TargetText: "Recovery only reads committed checkpoints.",
			UpdateSeq:  12, Quality: QualityMark{Label: "preferred", Rank: 3, Valid: true},
		},
	}
}

func GenerateDataset(dir string) (*GeneratedDataset, error) {
	if err := os.MkdirAll(filepath.Join(dir, "sources"), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "increments"), 0o755); err != nil {
		return nil, err
	}
	r := rand.New(rand.NewSource(GeneratorSeed))
	records := seededRecords(r)

	files := [][]SegmentRecord{
		{records[0], records[1], records[2], records[3]},
		{records[4], records[5], records[6], records[7]},
		{records[8], records[9], records[10], records[11]},
	}
	dataset := &GeneratedDataset{Seed: GeneratorSeed, Dir: dir, Records: records}
	for i, group := range files {
		name := filepath.Join(dir, "sources", fmt.Sprintf("source-%03d.jsonl", i+1))
		if err := writeJSONL(name, group); err != nil {
			return nil, err
		}
		dataset.SourceFiles = append(dataset.SourceFiles, name)
		for j := range group {
			group[j].SourceFile = filepath.Base(name)
		}
	}

	inc1 := []incrementLine{{
		IncrementID: "inc-001", BaseVersion: 1, SegmentID: "seg-004",
		ChangeType: "target_corrected", NewTargetText: "Commit. Configuration will update shortly.",
	}}
	inc2 := []incrementLine{{
		IncrementID: "inc-002", BaseVersion: 2, SegmentID: "seg-009",
		ChangeType: "target_corrected", NewTargetText: "Store. It is safe to close the page.",
	}}
	for i, lines := range [][]incrementLine{inc1, inc2} {
		name := filepath.Join(dir, "increments", fmt.Sprintf("increment-%03d.jsonl", i+1))
		if err := writeJSONL(name, lines); err != nil {
			return nil, err
		}
		dataset.IncrementFiles = append(dataset.IncrementFiles, name)
	}

	manifest, err := CanonicalJSON(map[string]any{
		"seed":            GeneratorSeed,
		"source_files":    []string{"source-001.jsonl", "source-002.jsonl", "source-003.jsonl"},
		"increment_files": []string{"increment-001.jsonl", "increment-002.jsonl"},
		"segment_count":   len(records),
		"scenarios":       []string{"zh-en-mixed", "empty-target", "repeated-source", "long-paragraph", "quality-marker-out-of-range"},
	})
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), manifest, 0o644); err != nil {
		return nil, err
	}
	return dataset, nil
}

func writeJSONL(path string, rows any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	switch v := rows.(type) {
	case []SegmentRecord:
		for _, row := range v {
			if err := enc.Encode(row); err != nil {
				return err
			}
		}
	case []incrementLine:
		for _, row := range v {
			if err := enc.Encode(row); err != nil {
				return err
			}
		}
	}
	return nil
}

func ParseSegmentJSONL(data []byte) ([]SegmentRecord, error) {
	var out []SegmentRecord
	if err := decodeJSONL(data, func(raw json.RawMessage) error {
		var row SegmentRecord
		if err := json.Unmarshal(raw, &row); err != nil {
			return err
		}
		out = append(out, row)
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func ParseIncrementJSONL(data []byte) (Increment, error) {
	var inc Increment
	seenID := ""
	err := decodeJSONL(data, func(raw json.RawMessage) error {
		var line struct {
			IncrementID   string       `json:"increment_id"`
			BaseVersion   int64        `json:"base_version"`
			SegmentID     string       `json:"segment_id"`
			ChangeType    string       `json:"change_type"`
			NewTargetText string       `json:"new_target_text"`
			UpdateSeq     int64        `json:"update_seq"`
			Quality       *QualityMark `json:"quality"`
		}
		if err := json.Unmarshal(raw, &line); err != nil {
			return err
		}
		if seenID == "" {
			seenID = line.IncrementID
			inc.IncrementID = line.IncrementID
			inc.BaseVersion = line.BaseVersion
		}
		inc.Changes = append(inc.Changes, Change{
			SegmentID: line.SegmentID, NewTargetText: line.NewTargetText,
			ChangeType: line.ChangeType, BaseVersion: line.BaseVersion,
			UpdateSeq: line.UpdateSeq, Quality: line.Quality,
		})
		return nil
	})
	if err != nil {
		return Increment{}, err
	}
	return inc, nil
}

func decodeJSONL(data []byte, fn func(json.RawMessage) error) error {
	var rows []json.RawMessage
	if err := json.Unmarshal(data, &rows); err == nil {
		for _, row := range rows {
			if err := fn(row); err != nil {
				return err
			}
		}
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	for {
		var row json.RawMessage
		if err := dec.Decode(&row); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if len(row) == 0 || string(row) == "null" {
			continue
		}
		if err := fn(row); err != nil {
			return err
		}
	}
	return nil
}
