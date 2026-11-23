package report

import (
	"bytes"
	"encoding/json"
	"sort"

	"depverify/internal/model"
	"depverify/internal/session"
	"depverify/internal/snapshot"
)

// SessionEntry 是报告中的单次会话条目。
type SessionEntry struct {
	TaskID     string               `json:"task_id"`
	State      string               `json:"state"`
	Result     *model.ClosureResult `json:"result,omitempty"`
	Snapshot   *snapshot.Record     `json:"snapshot,omitempty"`
	ParseError *ParseErrorView      `json:"parse_error,omitempty"`
	SnapReport *snapshot.LoadReport `json:"snapshot_report,omitempty"`
}

// ParseErrorView 是清单解析错误的稳定视图。
type ParseErrorView struct {
	Code     string `json:"code"`
	Offset   int    `json:"offset"`
	Resolved int    `json:"resolved"`
	Message  string `json:"message"`
}

// Report 是确定性导出根对象；时间字段只保留固定采样标记。
type Report struct {
	SampleMarker string         `json:"sample_marker"`
	Sessions     []SessionEntry `json:"sessions"`
}

// FromResults 将会话结论转换为稳定排序的报告。
func FromResults(results []session.RunResult, loadReports map[string]snapshot.LoadReport) Report {
	entries := make([]SessionEntry, 0, len(results))
	for _, r := range results {
		entry := SessionEntry{TaskID: r.TaskID, State: r.State, Result: r.Result, Snapshot: r.Snapshot}
		if r.ParseError != nil {
			entry.ParseError = &ParseErrorView{
				Code: r.ParseError.Code, Offset: r.ParseError.Offset,
				Resolved: r.ParseError.Resolved, Message: r.ParseError.Message,
			}
		}
		if lr, ok := loadReports[r.TaskID]; ok {
			cp := lr
			entry.SnapReport = &cp
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].TaskID < entries[j].TaskID })
	return Report{SampleMarker: model.FixedSampleMarker, Sessions: entries}
}

// Marshal 输出字节级稳定的 JSON：缩进、键序由 encoding/json 按结构体定义固定，
// 无 map 随机序、无时间戳字段。
func (r Report) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
