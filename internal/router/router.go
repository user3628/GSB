// Package router 实现路由核心：单写者提交、多读者查询、
// 冲突解决、快照切换、请求转发与确定性回放。
package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gsb/internal/format"
	"gsb/internal/gwerr"
	"gsb/internal/store"
)

// 冲突解决结局
const (
	OutcomeOverwrite = "overwrite"
	OutcomeSkip      = "skip"
	OutcomeReject    = "reject"
	OutcomeInsert    = "insert"
)

// ConflictEntry 是导入报告中一条冲突解决记录。
type ConflictEntry struct {
	ModelID string `json:"model_id"`
	Outcome string `json:"outcome"`
	Reason  string `json:"reason"`
}

// Router 是路由核心。写操作经 writeMu 单写者串行；
// 读操作经 mu 并发读取某一完整提交版本。
type Router struct {
	writeMu     sync.Mutex
	mu          sync.RWMutex
	routes      map[string]format.RouteRecord
	version     int64
	snapshotSeq int64
	store       *store.Store
	httpClient  *http.Client
	nowMs       func() int64
}

// New 创建路由核心并从 SQLite 加载最后一个完整提交版本。
func New(st *store.Store) *Router {
	r := &Router{
		routes: make(map[string]format.RouteRecord),
		store:  st,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
		nowMs: func() int64 { return time.Now().UnixMilli() },
	}
	recs, version, err := st.LoadRoutes()
	if err == nil {
		for _, rec := range recs {
			r.routes[rec.ModelID] = rec
		}
		r.version = version
	}
	r.snapshotSeq = st.MetaInt("snapshot_seq")
	return r
}

// Version 返回当前完整提交版本。
func (r *Router) Version() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.version
}

// SnapshotSeq 返回当前快照序号。
func (r *Router) SnapshotSeq() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snapshotSeq
}

// Routes 返回当前版本的全部路由（只读快照）。
func (r *Router) Routes() []format.RouteRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]format.RouteRecord, 0, len(r.routes))
	for _, rec := range r.routes {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ModelID < out[j].ModelID })
	return out
}

// Decision 是一次路由决策。
type Decision struct {
	ModelID   string `json:"model_id"`
	Endpoint  string `json:"endpoint"`
	Version   int64  `json:"table_version"`
	Toolchain string `json:"toolchain_version"`
}

// Decide 按模型标识查询路由决策。
func (r *Router) Decide(modelID string) (*Decision, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.routes[modelID]
	if !ok {
		return nil, gwerr.New(gwerr.EModelNotFound, "模型无路由: "+modelID,
			map[string]any{"model_id": modelID})
	}
	return &Decision{ModelID: modelID, Endpoint: rec.Endpoint,
		Version: r.version, Toolchain: rec.Toolchain}, nil
}

// toolchainPriority 解析 "tc-X.Y.Z" 形式的工具链版本优先级。
func toolchainPriority(tc string) ([3]int, error) {
	var pri [3]int
	s := strings.TrimPrefix(tc, "tc-")
	parts := strings.Split(s, ".")
	if len(parts) < 3 {
		return pri, fmt.Errorf("非法工具链版本: %s", tc)
	}
	for i := 0; i < 3; i++ {
		v, err := strconv.Atoi(strings.TrimRight(parts[i], "-legacy"))
		if err != nil {
			// 形如 "0-legacy" 的段
			v, err = strconv.Atoi(strings.Split(parts[i], "-")[0])
			if err != nil {
				return pri, fmt.Errorf("非法工具链版本: %s", tc)
			}
		}
		pri[i] = v
	}
	return pri, nil
}

// probeEndpoint 探测后端端点可用性。
func (r *Router) probeEndpoint(endpoint string) bool {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get(endpoint + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}

// resolveConflict 按确定规则解决冲突：工具链版本优先级 →
// 后续端点可用性 → 最后写入时间打破平局。
func (r *Router) resolveConflict(existing, incoming format.RouteRecord) (string, string) {
	ep, err1 := toolchainPriority(existing.Toolchain)
	ip, err2 := toolchainPriority(incoming.Toolchain)
	if err1 == nil && err2 == nil && ep != ip {
		if lessPri(ep, ip) {
			return OutcomeOverwrite, fmt.Sprintf("工具链版本优先级: %s > %s", incoming.Toolchain, existing.Toolchain)
		}
		return OutcomeSkip, fmt.Sprintf("工具链版本优先级: %s < %s", incoming.Toolchain, existing.Toolchain)
	}
	// 端点可用性
	inUp := r.probeEndpoint(incoming.Endpoint)
	exUp := r.probeEndpoint(existing.Endpoint)
	if inUp != exUp {
		if inUp {
			return OutcomeOverwrite, "后续端点可用性: 新端点可达而现有端点不可达"
		}
		return OutcomeSkip, "后续端点可用性: 现有端点可达而新端点不可达"
	}
	// 最后写入时间
	if incoming.LastWriteMs != existing.LastWriteMs {
		if incoming.LastWriteMs > existing.LastWriteMs {
			return OutcomeOverwrite, fmt.Sprintf("最后写入时间: %d > %d", incoming.LastWriteMs, existing.LastWriteMs)
		}
		return OutcomeSkip, fmt.Sprintf("最后写入时间: %d < %d", incoming.LastWriteMs, existing.LastWriteMs)
	}
	if incoming == existing {
		return OutcomeSkip, "记录完全一致，无需变更"
	}
	return OutcomeReject, "全部裁决维度平局但记录内容不同，无法确定性解决"
}

func lessPri(a, b [3]int) bool {
	for i := 0; i < 3; i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// ImportBatch 原子导入一批记录。任一条被拒绝则整批回滚。
func (r *Router) ImportBatch(recs []format.RouteRecord) ([]ConflictEntry, int64, error) {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	r.mu.RLock()
	current := make(map[string]format.RouteRecord, len(r.routes))
	for k, v := range r.routes {
		current[k] = v
	}
	baseVersion := r.version
	r.mu.RUnlock()

	var entries []ConflictEntry
	var upserts []format.RouteRecord
	for _, rec := range recs {
		existing, ok := current[rec.ModelID]
		if !ok {
			entries = append(entries, ConflictEntry{ModelID: rec.ModelID, Outcome: OutcomeInsert, Reason: "新模型路由"})
			upserts = append(upserts, rec)
			continue
		}
		outcome, reason := r.resolveConflict(existing, rec)
		entries = append(entries, ConflictEntry{ModelID: rec.ModelID, Outcome: outcome, Reason: reason})
		switch outcome {
		case OutcomeOverwrite:
			upserts = append(upserts, rec)
		case OutcomeReject:
			return entries, baseVersion, gwerr.New(gwerr.EConflictingImport,
				"模型 "+rec.ModelID+" 冲突无法解决，整批回滚",
				map[string]any{"model_id": rec.ModelID, "reason": reason})
		}
	}
	if len(upserts) == 0 {
		return entries, baseVersion, nil
	}
	newVersion := baseVersion + 1
	intentID, err := r.store.BeginIntent("import",
		fmt.Sprintf("target_version=%d records=%d", newVersion, len(upserts)))
	if err != nil {
		return entries, baseVersion, err
	}
	if err := r.store.CommitBatch(intentID, newVersion, upserts, false); err != nil {
		return entries, baseVersion, err
	}
	r.mu.Lock()
	for _, rec := range upserts {
		r.routes[rec.ModelID] = rec
	}
	r.version = newVersion
	r.mu.Unlock()
	return entries, newVersion, nil
}

// SnapshotFile 是快照文件结构。
type SnapshotFile struct {
	Format        string               `json:"format"`
	FormatVersion int                  `json:"format_version"`
	SnapshotSeq   int64                `json:"snapshot_seq"`
	GeneratedMs   int64                `json:"generated_ms"`
	Records       []format.RouteRecord `json:"records"`
}

// LoadSnapshot 校验并原子切换快照。
func (r *Router) LoadSnapshot(snap *SnapshotFile) (int64, error) {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	r.mu.RLock()
	curSeq := r.snapshotSeq
	baseVersion := r.version
	r.mu.RUnlock()

	if snap.Format != "grt-snapshot" {
		return baseVersion, gwerr.New(gwerr.ESnapshotInvalid, "快照格式标识非法: "+snap.Format, nil)
	}
	if _, err := format.Adjudicate(format.FormatBinary, snap.FormatVersion); err != nil {
		return baseVersion, gwerr.New(gwerr.ESnapshotInvalid,
			fmt.Sprintf("快照格式版本 %d 不兼容", snap.FormatVersion),
			map[string]any{"format_version": snap.FormatVersion})
	}
	if snap.SnapshotSeq <= curSeq {
		return baseVersion, gwerr.New(gwerr.ESnapshotInvalid,
			fmt.Sprintf("快照序号 %d 不满足单调递增（当前 %d）", snap.SnapshotSeq, curSeq),
			map[string]any{"snapshot_seq": snap.SnapshotSeq, "current_seq": curSeq})
	}
	newVersion := baseVersion + 1
	intentID, err := r.store.BeginIntent("snapshot",
		fmt.Sprintf("target_version=%d snapshot_seq=%d", newVersion, snap.SnapshotSeq))
	if err != nil {
		return baseVersion, err
	}
	if err := r.store.CommitBatch(intentID, newVersion, snap.Records, true); err != nil {
		return baseVersion, err
	}
	if err := r.store.SetMeta("snapshot_seq", snap.SnapshotSeq); err != nil {
		return baseVersion, err
	}
	// 原子切换内存索引
	newRoutes := make(map[string]format.RouteRecord, len(snap.Records))
	for _, rec := range snap.Records {
		newRoutes[rec.ModelID] = rec
	}
	r.mu.Lock()
	r.routes = newRoutes
	r.version = newVersion
	r.snapshotSeq = snap.SnapshotSeq
	r.mu.Unlock()
	// 切换成功后释放旧快照派生统计
	_ = r.store.ResetHits()
	return newVersion, nil
}

// Recover 扫描未完成意图并回滚到最后一个完整版本。只基于已落盘事实。
func (r *Router) Recover() (*RecoveryReport, error) {
	pending, err := r.store.PendingIntents()
	if err != nil {
		return nil, err
	}
	rep := &RecoveryReport{CompleteVersion: r.Version()}
	for _, p := range pending {
		if err := r.store.AbortIntent(p.ID); err != nil {
			return nil, err
		}
		rep.RolledBack = append(rep.RolledBack, p)
	}
	if len(rep.RolledBack) == 0 {
		rep.Message = "无未完成意图，路由表处于最后完整版本"
	} else {
		rep.Message = fmt.Sprintf("回滚 %d 条未完成意图，路由表保持最后完整版本 %d",
			len(rep.RolledBack), rep.CompleteVersion)
	}
	return rep, nil
}

// RecoveryReport 是恢复报告。
type RecoveryReport struct {
	CompleteVersion int64                 `json:"complete_version"`
	RolledBack      []store.PendingIntent `json:"rolled_back_intents,omitempty"`
	Message         string                `json:"message"`
}

// BackendSummary 是后端测试服务返回的结果摘要。
type BackendSummary struct {
	Digest    string `json:"digest"`
	LatencyMs int64  `json:"latency_ms"`
	Backend   string `json:"backend"`
}

// ForwardResult 是一次转发的完整结果。
type ForwardResult struct {
	Decision Decision       `json:"decision"`
	Summary  BackendSummary `json:"summary"`
}

// Forward 按模型标识把推理请求路由到对应后端并回传结果摘要。
func (r *Router) Forward(modelID string, payload []byte) (*ForwardResult, error) {
	dec, err := r.Decide(modelID)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, dec.Endpoint+"/infer",
		bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, gwerr.New(gwerr.EBackendUnavailable,
			"后端不可达: "+err.Error(),
			map[string]any{"endpoint": dec.Endpoint, "model_id": modelID})
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, gwerr.New(gwerr.EBackendUnavailable,
			"后端返回非 200: "+resp.Status,
			map[string]any{"endpoint": dec.Endpoint, "model_id": modelID})
	}
	var summary BackendSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		return nil, gwerr.New(gwerr.EBackendUnavailable, "后端响应解析失败",
			map[string]any{"endpoint": dec.Endpoint})
	}
	_ = r.store.RecordHit(modelID, r.nowMs())
	return &ForwardResult{Decision: *dec, Summary: summary}, nil
}

// ReplayEntry 是回放序列中一条确定性请求的结果。
type ReplayEntry struct {
	Seq       int    `json:"seq"`
	ModelID   string `json:"model_id"`
	Endpoint  string `json:"endpoint"`
	Version   int64  `json:"table_version"`
	Payload   string `json:"payload"`
	Digest    string `json:"digest"`
	LatencyMs int64  `json:"latency_ms"`
}

// Replay 用固定种子生成确定性请求序列并逐条转发。
func (r *Router) Replay(seed int64, count int) ([]ReplayEntry, error) {
	routes := r.Routes()
	if len(routes) == 0 {
		return nil, gwerr.New(gwerr.EBadRequest, "路由表为空，无法回放", nil)
	}
	rng := rand.New(rand.NewSource(seed))
	entries := make([]ReplayEntry, 0, count)
	for i := 0; i < count; i++ {
		rec := routes[rng.Intn(len(routes))]
		payload := fmt.Sprintf("replay-seed=%d-seq=%d-len=%d", seed, i, 16+rng.Intn(240))
		res, err := r.Forward(rec.ModelID, []byte(payload))
		if err != nil {
			return entries, err
		}
		entries = append(entries, ReplayEntry{
			Seq: i, ModelID: rec.ModelID, Endpoint: res.Decision.Endpoint,
			Version: res.Decision.Version, Payload: payload,
			Digest: res.Summary.Digest, LatencyMs: res.Summary.LatencyMs,
		})
	}
	return entries, nil
}
