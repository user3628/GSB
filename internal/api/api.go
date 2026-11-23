// Package api 提供本地 HTTP 接口与诊断视图。
package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"gsb/internal/format"
	"gsb/internal/gwerr"
	"gsb/internal/router"
	"gsb/internal/store"
)

// Server 是网关 HTTP 服务。
type Server struct {
	Rtr   *router.Router
	Store *store.Store
	Mux   *http.ServeMux
}

// NewServer 注册全部接口。
func NewServer(rtr *router.Router, st *store.Store) *Server {
	s := &Server{Rtr: rtr, Store: st, Mux: http.NewServeMux()}
	s.Mux.HandleFunc("POST /api/import", s.handleImport)
	s.Mux.HandleFunc("GET /api/export", s.handleExport)
	s.Mux.HandleFunc("POST /api/replay", s.handleReplay)
	s.Mux.HandleFunc("POST /api/forward", s.handleForward)
	s.Mux.HandleFunc("GET /api/route/", s.handleRoute)
	s.Mux.HandleFunc("GET /api/stats", s.handleStats)
	s.Mux.HandleFunc("POST /api/snapshot", s.handleSnapshot)
	s.Mux.HandleFunc("GET /api/recovery", s.handleRecovery)
	s.Mux.HandleFunc("GET /api/diagnostics", s.handleDiagnostics)
	s.Mux.HandleFunc("POST /api/debug/crash", s.handleDebugCrash)
	return s
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// fail 统一失败响应：含业务码与上下文的 JSON。
func fail(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if ge, ok := err.(*gwerr.Error); ok {
		switch ge.Code {
		case gwerr.ERecordCorrupt, gwerr.EBadRequest:
			status = http.StatusBadRequest
		case gwerr.EFormatUnsupported, gwerr.ESnapshotInvalid, gwerr.EUnitOverflow:
			status = http.StatusUnprocessableEntity
		case gwerr.EConflictingImport:
			status = http.StatusConflict
		case gwerr.EBackendUnavailable:
			status = http.StatusBadGateway
		case gwerr.EModelNotFound:
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]any{"error": ge})
		return
	}
	writeJSON(w, status, map[string]any{"error": &gwerr.Error{
		Code: gwerr.EInternal, Message: err.Error()}})
}

// handleImport 导入路由表（自动检测格式，trunc 参数声明截断规则）。
func (s *Server) handleImport(w http.ResponseWriter, r *http.Request) {
	truncRule := r.URL.Query().Get("trunc")
	if truncRule == "" {
		truncRule = format.TruncTruncate
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		fail(w, err)
		return
	}
	rep, err := format.Import(body, truncRule)
	if err != nil {
		fail(w, err)
		return
	}
	entries, version, err := s.Rtr.ImportBatch(rep.Records)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": err, "import_report": rep, "conflicts": entries,
			"table_version": version, "rolled_back": true,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"import_report": rep, "conflicts": entries, "table_version": version,
	})
}

// handleExport 导出路由表（format=bin|jsonl）。
func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	recs := s.Rtr.Routes()
	switch r.URL.Query().Get("format") {
	case "bin":
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="routes.bin"`)
		_, _ = w.Write(format.EncodeBinary(recs, true))
	case "jsonl":
		w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
		_, _ = w.Write(format.EncodeJSONL(recs))
	default:
		fail(w, gwerr.New(gwerr.EBadRequest, "format 参数须为 bin 或 jsonl", nil))
	}
}

// handleReplay 触发确定性请求回放。
func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Seed  int64 `json:"seed"`
		Count int   `json:"count"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, gwerr.New(gwerr.EBadRequest, "回放请求体非法: "+err.Error(), nil))
		return
	}
	if req.Count <= 0 {
		req.Count = 8
	}
	entries, err := s.Rtr.Replay(req.Seed, req.Count)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": err, "partial_entries": entries})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"seed": req.Seed, "count": req.Count, "entries": entries,
	})
}

// handleForward 转发单条推理请求。
func (s *Server) handleForward(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ModelID string `json:"model_id"`
		Payload string `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, gwerr.New(gwerr.EBadRequest, "请求体非法: "+err.Error(), nil))
		return
	}
	res, err := s.Rtr.Forward(req.ModelID, []byte(req.Payload))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleRoute 按模型查询路由决策。
func (s *Server) handleRoute(w http.ResponseWriter, r *http.Request) {
	modelID := strings.TrimPrefix(r.URL.Path, "/api/route/")
	dec, err := s.Rtr.Decide(modelID)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, dec)
}

// handleStats 查询命中统计。
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	hits, err := s.Store.LoadHits()
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"table_version": s.Rtr.Version(), "snapshot_seq": s.Rtr.SnapshotSeq(),
		"hits": hits,
	})
}

// handleSnapshot 加载快照文件并原子切换。
func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path     string               `json:"path"`
		Snapshot *router.SnapshotFile `json:"snapshot"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, gwerr.New(gwerr.EBadRequest, "请求体非法: "+err.Error(), nil))
		return
	}
	snap := req.Snapshot
	if snap == nil && req.Path != "" {
		data, err := os.ReadFile(req.Path)
		if err != nil {
			fail(w, gwerr.New(gwerr.ESnapshotInvalid, "快照文件读取失败: "+err.Error(),
				map[string]any{"path": req.Path}))
			return
		}
		var sf router.SnapshotFile
		if err := json.Unmarshal(data, &sf); err != nil {
			fail(w, gwerr.New(gwerr.ESnapshotInvalid, "快照文件解析失败: "+err.Error(),
				map[string]any{"path": req.Path}))
			return
		}
		snap = &sf
	}
	if snap == nil {
		fail(w, gwerr.New(gwerr.EBadRequest, "缺少 path 或 snapshot", nil))
		return
	}
	version, err := s.Rtr.LoadSnapshot(snap)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"table_version": version, "snapshot_seq": snap.SnapshotSeq,
		"routes": s.Rtr.Routes(),
	})
}

// handleRecovery 查询恢复报告（重启后扫描未完成意图）。
func (s *Server) handleRecovery(w http.ResponseWriter, r *http.Request) {
	rep, err := s.Rtr.Recover()
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// handleDiagnostics 诊断视图（JSON 汇总）。
func (s *Server) handleDiagnostics(w http.ResponseWriter, r *http.Request) {
	hits, _ := s.Store.LoadHits()
	writeJSON(w, http.StatusOK, map[string]any{
		"table_version": s.Rtr.Version(),
		"snapshot_seq":  s.Rtr.SnapshotSeq(),
		"routes":        s.Rtr.Routes(),
		"hits":          hits,
	})
}

// handleDebugCrash 写入一条挂起意图后强制退出，用于恢复演练。
func (s *Server) handleDebugCrash(w http.ResponseWriter, r *http.Request) {
	if os.Getenv("GATEWAY_ENABLE_DEBUG_CRASH") != "1" {
		fail(w, gwerr.New(gwerr.EBadRequest, "调试崩溃未启用", nil))
		return
	}
	id, err := s.Store.BeginIntent("import", "simulated-crash target_version=debug")
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message": fmt.Sprintf("意图 %d 已落盘，进程即将强制退出", id), "intent_id": id})
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	go func() { os.Exit(1) }()
}

// ParsePort 从环境变量读取端口。
func ParsePort(env string, def int) int {
	if v := os.Getenv(env); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			return p
		}
	}
	return def
}
