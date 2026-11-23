package session

import (
	"context"
	"encoding/json"
	"sort"
	"sync"

	"depverify/internal/closure"
	"depverify/internal/edges"
	"depverify/internal/engine"
	"depverify/internal/manifest"
	"depverify/internal/model"
	"depverify/internal/snapshot"
)

// 任务运行状态。
const (
	StateQueued    = "QUEUED"
	StateRunning   = "RUNNING"
	StateCompleted = "COMPLETED"
	StateFailed    = "FAILED"
)

// Run 是一次核验任务的可恢复进度记录。
type Run struct {
	TaskID string   `json:"task_id"`
	State  string   `json:"state"`
	Roots  []string `json:"roots"`
	Detail string   `json:"detail"`
	Marker string   `json:"marker"`
}

// Config 控制并发度与单会话组件预算。
type Config struct {
	MaxConcurrent int
	ItemBudget    int
	Threshold     int
}

// Manager 调度并发核验会话；每个会话使用独立的图与闭包状态。
type Manager struct {
	store *snapshot.Store
	cfg   Config
	mu    sync.Mutex
	sem   chan struct{}
}

// NewManager 创建管理器并从 SQLite 恢复历史任务进度。
func NewManager(store *snapshot.Store, cfg Config) *Manager {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 1
	}
	return &Manager{
		store: store,
		cfg:   cfg,
		sem:   make(chan struct{}, cfg.MaxConcurrent),
	}
}

// Recover 重启时把上次未完成（RUNNING）的任务标记为中断并重新入队。
// 已提交快照不回退，未提交事务已由 SQLite 自动丢弃。
func (m *Manager) Recover(ctx context.Context) ([]Run, error) {
	runs, err := m.loadRuns(ctx)
	if err != nil {
		return nil, err
	}
	var recovered []Run
	for _, run := range runs {
		if run.State == StateRunning || run.State == StateQueued {
			run.State = StateQueued
			run.Detail = "interrupted by process termination; requeued"
			run.Marker = model.FixedSampleMarker
			if err := m.upsertRun(ctx, run); err != nil {
				return nil, err
			}
			recovered = append(recovered, run)
		}
	}
	return recovered, nil
}

// RunRequest 是一次独立核验会话的输入。
type RunRequest struct {
	TaskID      string
	ManifestBuf []byte
	EdgesBuf    []byte
	Roots       []string
	// PersistSnapshot 为 false 时（异常注入样本）不推进快照版本。
	PersistSnapshot bool
}

// RunResult 是会话结论，会话间不共享闭包状态。
type RunResult struct {
	TaskID     string               `json:"task_id"`
	State      string               `json:"state"`
	Result     *model.ClosureResult `json:"result,omitempty"`
	Snapshot   *snapshot.Record     `json:"snapshot,omitempty"`
	ParseError *manifest.Error      `json:"parse_error,omitempty"`
	LoadReport *snapshot.LoadReport `json:"load_report,omitempty"`
}

// SubmitAndWait 提交任务：超并发上限时在等待队列阻塞，而非拒绝。
func (m *Manager) SubmitAndWait(ctx context.Context, req RunRequest) RunResult {
	m.sem <- struct{}{}
	defer func() { <-m.sem }()
	return m.execute(ctx, req)
}

func (m *Manager) execute(ctx context.Context, req RunRequest) RunResult {
	_ = m.upsertRun(ctx, Run{TaskID: req.TaskID, State: StateRunning, Roots: req.Roots,
		Detail: "session started", Marker: model.FixedSampleMarker})

	components, mErr := manifest.Parse(bytesReader(req.ManifestBuf))
	if mErr != nil {
		_ = m.upsertRun(ctx, Run{TaskID: req.TaskID, State: StateFailed, Roots: req.Roots,
			Detail: mErr.Code, Marker: model.FixedSampleMarker})
		return RunResult{TaskID: req.TaskID, State: StateFailed, ParseError: mErr}
	}
	edgeList, edgeIssues := edges.Parse(bytesReader(req.EdgesBuf))
	known := make(map[string]struct{}, len(components))
	compMap := make(map[string]model.Component, len(components))
	for _, c := range components {
		known[c.ID] = struct{}{}
		compMap[c.ID] = c
	}
	graphIssues, unique := edges.CheckGraph(edgeList, known)
	allIssues := append(append([]model.ParseIssue{}, edgeIssues...), graphIssues...)
	uniqueEdges := make([]model.Edge, 0, len(unique))
	for _, e := range unique {
		uniqueEdges = append(uniqueEdges, e)
	}
	sort.Slice(uniqueEdges, func(i, j int) bool {
		if uniqueEdges[i].Parent != uniqueEdges[j].Parent {
			return uniqueEdges[i].Parent < uniqueEdges[j].Parent
		}
		return uniqueEdges[i].Child < uniqueEdges[j].Child
	})

	g := closure.NewGraph(uniqueEdges, compMap)
	eng := engine.New(g, m.cfg.Threshold)
	result := eng.Verify(req.Roots, m.cfg.ItemBudget, allIssues)

	// 中断检查：若 ctx 已取消，保留已解析状态但不提交快照。
	if err := ctx.Err(); err != nil {
		_ = m.upsertRun(ctx, Run{TaskID: req.TaskID, State: StateQueued, Roots: req.Roots,
			Detail: "interrupted: " + err.Error(), Marker: model.FixedSampleMarker})
		return RunResult{TaskID: req.TaskID, State: StateQueued, Result: &result}
	}

	out := RunResult{TaskID: req.TaskID, State: StateCompleted, Result: &result}
	if req.PersistSnapshot {
		rec, err := m.store.SaveOrReuse(ctx, result, result.Roots)
		if err != nil {
			_ = m.upsertRun(ctx, Run{TaskID: req.TaskID, State: StateFailed, Roots: req.Roots,
				Detail: err.Error(), Marker: model.FixedSampleMarker})
			out.State = StateFailed
			return out
		}
		out.Snapshot = &rec
	}
	_ = m.upsertRun(ctx, Run{TaskID: req.TaskID, State: out.State, Roots: req.Roots,
		Detail: "closure size " + itoa(result.ClosureSize), Marker: model.FixedSampleMarker})
	return out
}

// LoadSnapshot 加载快照并在跳号时回退，回退目标写入报告。
func (m *Manager) LoadSnapshot(ctx context.Context, version int) (snapshot.Record, snapshot.LoadReport, error) {
	return m.store.Load(ctx, version)
}

func (m *Manager) loadRuns(ctx context.Context) ([]Run, error) {
	rows, err := m.store.DB().QueryContext(ctx, `SELECT task_id, state, detail FROM task_runs ORDER BY task_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []Run
	for rows.Next() {
		var r Run
		var detail string
		if err := rows.Scan(&r.TaskID, &r.State, &detail); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(detail), &r)
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

func (m *Manager) upsertRun(ctx context.Context, run Run) error {
	run.Marker = model.FixedSampleMarker
	detail, err := json.Marshal(run)
	if err != nil {
		return err
	}
	_, err = m.store.DB().ExecContext(ctx,
		`INSERT INTO task_runs(task_id, state, updated_marker, detail) VALUES (?, ?, ?, ?)
		 ON CONFLICT(task_id) DO UPDATE SET state=excluded.state, updated_marker=excluded.updated_marker, detail=excluded.detail`,
		run.TaskID, run.State, model.FixedSampleMarker, string(detail))
	return err
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
