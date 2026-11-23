package smuggle

import (
	"encoding/json"
	"sort"
)

// ReportMarker 是报告中的固定采样标记，替代墙钟时间以保证可重复性。
const ReportMarker = "SAMPLE-0001"

// Report 是确定性排序的裁决报告。
type Report struct {
	Marker          string       `json:"marker"`
	StreamThreshold int          `json:"stream_threshold"`
	Connections     []ConnReport `json:"connections"`
}

// BuildReport 按连接相对路径稳定排序并组装报告。
func BuildReport(cfg Config, reps []ConnReport) *Report {
	sorted := append([]ConnReport(nil), reps...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Conn < sorted[j].Conn })
	for i := range sorted {
		if sorted[i].ProxyRequests == nil {
			sorted[i].ProxyRequests = []Boundary{}
		}
		if sorted[i].BackendRequests == nil {
			sorted[i].BackendRequests = []Boundary{}
		}
	}
	return &Report{
		Marker:          ReportMarker,
		StreamThreshold: cfg.StreamThreshold,
		Connections:     sorted,
	}
}

// MarshalReport 序列化报告为字节级可重复的 JSON。
func MarshalReport(r *Report) ([]byte, error) {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
