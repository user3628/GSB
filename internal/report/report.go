package report

import (
	"bytes"
	"encoding/json"
	"sort"

	"mitmpin/internal/engine"
	"mitmpin/internal/replay"
	"mitmpin/internal/sample"
)

// SnapshotInfo 是报告中的策略快照链节点。
type SnapshotInfo struct {
	Version     int    `json:"version"`
	Marker      string `json:"marker"`
	ContentHash string `json:"content_hash"`
}

// ConnReport 是单条连接的可复核报告条目。
type ConnReport struct {
	ConnID          string              `json:"conn_id"`
	PolicyVersion   int                 `json:"policy_version"`
	FinalCode       string              `json:"final_code"`
	ProcessedEvents int                 `json:"processed_events"`
	BudgetCode      string              `json:"budget_code"`
	Trace           []engine.TraceEntry `json:"trace"`
	Replay          replay.ConnReplay   `json:"replay"`
}

// Report 是整批确定性一致性报告。时间字段只使用固定采样 marker。
type Report struct {
	BatchID     string         `json:"batch_id"`
	Marker      string         `json:"marker"`
	Budget      sample.Budget  `json:"budget"`
	Snapshots   []SnapshotInfo `json:"snapshot_chain"`
	Connections []ConnReport   `json:"connections"`
}

// Options 控制报告装配输入。
type Options struct {
	Batch       sample.Batch
	Budget      sample.Budget
	Snapshots   []SnapshotInfo
	Results     []engine.ConnResult
	BudgetCodes map[string]string
	Replays     []replay.ConnReplay
}

// Build 装配报告，所有集合按连接标识/事件序号稳定排序。
func Build(opts Options) Report {
	replayByConn := make(map[string]replay.ConnReplay, len(opts.Replays))
	for _, r := range opts.Replays {
		replayByConn[r.ConnID] = r
	}
	sort.Slice(opts.Results, func(i, j int) bool {
		return opts.Results[i].ConnID < opts.Results[j].ConnID
	})

	conns := make([]ConnReport, 0, len(opts.Results))
	for _, result := range opts.Results {
		trace := append([]engine.TraceEntry(nil), result.Trace...)
		sort.Slice(trace, func(i, j int) bool { return trace[i].Seq < trace[j].Seq })
		budgetCode := sample.E_OK
		if code, ok := opts.BudgetCodes[result.ConnID]; ok {
			budgetCode = code
		}
		conns = append(conns, ConnReport{
			ConnID:          result.ConnID,
			PolicyVersion:   result.PolicyVersion,
			FinalCode:       result.FinalCode,
			ProcessedEvents: result.ProcessedEvents,
			BudgetCode:      budgetCode,
			Trace:           trace,
			Replay:          replayByConn[result.ConnID],
		})
	}

	snaps := append([]SnapshotInfo(nil), opts.Snapshots...)
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].Version < snaps[j].Version })

	return Report{
		BatchID:     opts.Batch.BatchID,
		Marker:      opts.Batch.Marker,
		Budget:      opts.Budget,
		Snapshots:   snaps,
		Connections: conns,
	}
}

// Marshal 输出字节级确定的 JSON：固定缩进、键序由结构体定义、无任何墙上时间。
func (r Report) Marshal() ([]byte, error) {
	// 先序列化再解码归一化，避免 map 迭代顺序差异。
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	var generic any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(generic); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
