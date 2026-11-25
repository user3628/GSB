package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"layoutread/internal/demo"
	"layoutread/internal/model"
	"layoutread/internal/service"
)

type Server struct {
	svc *service.Service
	mux *http.ServeMux
}

func New(svc *service.Service) *Server {
	server := &Server{svc: svc, mux: http.NewServeMux()}
	server.routes()
	return server
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /{$}", s.handleIndex)
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /api/demo", s.handleDemoBundle)
	s.mux.HandleFunc("POST /api/import", s.handleImport)
	s.mux.HandleFunc("POST /api/analyze", s.handleAnalyze)
	s.mux.HandleFunc("POST /api/resume", s.handleAnalyze)
	s.mux.HandleFunc("POST /api/stop", s.handleStop)
	s.mux.HandleFunc("GET /api/status", s.handleStatus)
	s.mux.HandleFunc("GET /api/fingerprint", s.handleFingerprint)
	s.mux.HandleFunc("GET /api/query", s.handleQuery)
	s.mux.HandleFunc("GET /api/export", s.handleExport)
	s.mux.HandleFunc("POST /api/demo/run", s.handleDemoRun)
}

func (s *Server) handleIndex(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(writer, indexHTML)
}

func (s *Server) handleHealth(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleDemoBundle(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, demo.Generate())
}

func (s *Server) handleImport(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(io.LimitReader(request.Body, 8<<20))
	if err != nil {
		writeError(writer, model.NewError(model.CodeInvalidRequest, "cannot read request body", ""))
		return
	}
	var bundle model.Bundle
	if err := json.Unmarshal(body, &bundle); err != nil {
		writeError(writer, model.NewError(model.CodeInvalidRequest, "invalid bundle JSON: "+err.Error(), ""))
		return
	}
	if err := s.svc.Import(request.Context(), bundle); err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"imported_documents":   len(bundle.Documents),
		"strategy_version":     bundle.Strategy.Version,
		"strategy_fingerprint": bundle.Strategy.Fingerprint(),
	})
}

func (s *Server) handleAnalyze(writer http.ResponseWriter, request *http.Request) {
	options := service.RunOptions{}
	if delayText := request.URL.Query().Get("stage_delay_ms"); delayText != "" {
		delay, err := strconv.Atoi(delayText)
		if err != nil || delay < 0 {
			writeError(writer, model.NewError(model.CodeInvalidRequest, "stage_delay_ms must be a non-negative integer", ""))
			return
		}
		options.StageDelay = time.Duration(delay) * time.Millisecond
	}
	report, err := s.svc.Run(request.Context(), options)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, report)
}

func (s *Server) handleStop(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, s.svc.RequestStop())
}

func (s *Server) handleStatus(writer http.ResponseWriter, request *http.Request) {
	status, err := s.svc.Status(request.Context())
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, status)
}

func (s *Server) handleFingerprint(writer http.ResponseWriter, request *http.Request) {
	status, err := s.svc.Status(request.Context())
	if err != nil {
		writeError(writer, err)
		return
	}
	documents := make([]map[string]any, 0, len(status.Documents))
	for _, doc := range status.Documents {
		stages := make([]map[string]any, 0, len(doc.Stages))
		for _, stage := range doc.Stages {
			stages = append(stages, map[string]any{
				"stage": stage.Stage, "input_fingerprint": stage.InputFingerprint,
				"artifact_fingerprint": stage.ArtifactFingerprint,
			})
		}
		documents = append(documents, map[string]any{
			"doc_id": doc.DocID, "content_fingerprint": doc.ContentFingerprint,
			"head_sequence": doc.HeadSequence, "stages": stages,
		})
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"strategy_version":     status.StrategyVersion,
		"strategy_fingerprint": status.StrategyFingerprint,
		"snapshot_digest":      status.SnapshotDigest,
		"documents":            documents,
	})
}

func (s *Server) handleQuery(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	req := service.QueryRequest{
		DocID:   query.Get("doc_id"),
		Scope:   query.Get("scope"),
		QueryID: query.Get("query_id"),
	}
	if req.Scope == "" {
		req.Scope = "blocks"
	}
	if budgetText := query.Get("budget_ms"); budgetText != "" {
		budget, err := strconv.Atoi(budgetText)
		if err != nil || budget < 0 {
			writeError(writer, model.NewError(model.CodeInvalidRequest, "budget_ms must be a non-negative integer", req.DocID))
			return
		}
		req.BudgetMS = budget
		req.HasBudget = true
	}
	if delayText := query.Get("simulated_delay_ms"); delayText != "" {
		delay, err := strconv.Atoi(delayText)
		if err != nil || delay < 0 {
			writeError(writer, model.NewError(model.CodeInvalidRequest, "simulated_delay_ms must be a non-negative integer", req.DocID))
			return
		}
		req.SimulatedDelayMS = delay
	}
	response, err := s.svc.Query(request.Context(), req)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, response)
}

func (s *Server) handleExport(writer http.ResponseWriter, request *http.Request) {
	report, err := s.svc.Export(request.Context())
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, report)
}

func (s *Server) handleDemoRun(writer http.ResponseWriter, request *http.Request) {
	bundle := demo.Generate()
	if err := s.svc.Import(request.Context(), bundle); err != nil {
		writeError(writer, err)
		return
	}
	report, err := s.svc.Run(request.Context(), service.RunOptions{})
	if err != nil {
		writeError(writer, err)
		return
	}
	export, err := s.svc.Export(request.Context())
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"run": report, "export": export})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

func writeError(writer http.ResponseWriter, err error) {
	var modelErr *model.Error
	if !errors.As(err, &modelErr) {
		modelErr = &model.Error{Code: model.CodeInternal, Message: err.Error()}
	}
	status := http.StatusInternalServerError
	switch modelErr.Code {
	case model.CodeInvalidRequest, model.CodeStrategyInvalid, model.CodeDocumentInvalid, model.CodeBoxInvalid:
		status = http.StatusBadRequest
	case model.CodeNotFound:
		status = http.StatusNotFound
	case model.CodeLatencyBudget:
		status = http.StatusRequestTimeout
	case model.CodeStaleCheckpoint, model.CodeSnapshotReplaced, model.CodeCheckpointRollback,
		model.CodeOrderConflict, model.CodeColumnUnassigned, model.CodeAlreadyRunning:
		status = http.StatusConflict
	}
	writeJSON(writer, status, modelErr)
}

const indexHTML = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Layout Reading Order Console</title>
<style>
:root{color-scheme:light dark;--bg:#f5f2ea;--card:#fffdf8;--ink:#25231f;--muted:#6d665c;--line:#d9d1c3;--accent:#0a6b63;--accent2:#b44b2a}
@media(prefers-color-scheme:dark){:root{--bg:#171815;--card:#21231f;--ink:#eee9df;--muted:#aaa293;--line:#3d4038;--accent:#63d0c4;--accent2:#ee8a63}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.55 ui-sans-serif,system-ui,sans-serif}.wrap{max-width:1120px;margin:0 auto;padding:32px 20px 64px}h1{font:700 28px/1.2 ui-serif,Georgia,serif;margin:0 0 6px}.sub{color:var(--muted);margin-bottom:24px}.toolbar{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:20px}button{background:var(--accent);color:var(--bg);border:0;border-radius:8px;padding:9px 14px;font-weight:650;cursor:pointer}button.secondary{background:transparent;color:var(--ink);border:1px solid var(--line)}button.danger{background:var(--accent2)}.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}@media(max-width:800px){.grid{grid-template-columns:1fr}}.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:18px;min-width:0;box-shadow:0 8px 24px #0001}h2{font-size:15px;letter-spacing:.06em;text-transform:uppercase;margin:0 0 12px;color:var(--muted)}pre{white-space:pre-wrap;word-break:break-word;background:color-mix(in srgb,var(--bg) 72%,transparent);border:1px solid var(--line);border-radius:9px;padding:13px;max-height:460px;overflow:auto;font:12px/1.55 ui-monospace,SFMono-Regular,monospace}.field{display:flex;gap:8px;margin-bottom:10px}.field input,.field select{min-width:0;flex:1;background:var(--card);color:var(--ink);border:1px solid var(--line);border-radius:7px;padding:8px}.hint{color:var(--muted);font-size:12px;margin-top:8px}.badge{display:inline-block;border:1px solid var(--line);border-radius:999px;padding:2px 8px;margin:0 4px 4px 0;font-size:12px;color:var(--muted)}
</style>
</head>
<body><main class="wrap">
<h1>Document Layout Reading Order</h1>
<div class="sub">确定性四阶段引擎 · SQLite 检查点 · 延迟预算查询 · 增量重排一致性</div>
<div class="toolbar">
  <button onclick="demoRun()">导入并运行固定演示</button>
  <button class="secondary" onclick="loadDemo()">仅导入样本</button>
  <button class="secondary" onclick="analyze()">分阶段分析 / 恢复</button>
  <button class="secondary" onclick="status()">刷新状态</button>
  <button class="danger" onclick="stopRun()">请求停止</button>
</div>
<div class="grid">
  <section class="card"><h2>运行报告 / Status</h2><pre id="status">尚未运行。</pre></section>
  <section class="card"><h2>交互查询</h2>
    <div class="field"><select id="doc"><option value="doc-single-zh">doc-single-zh</option><option value="doc-double-en">doc-double-en</option><option value="doc-span-title-zh-en">doc-span-title-zh-en</option><option value="doc-dual-band-mixed">doc-dual-band-mixed</option></select><select id="scope"><option>blocks</option><option>confidence</option><option>columns</option></select></div>
    <div class="field"><input id="budget" type="number" min="0" value="25" aria-label="budget ms"><button onclick="query()">查询</button></div>
    <pre id="result">查询结果会显示在这里。</pre>
    <div class="hint">将预算设为 0，或设置 simulated_delay_ms 超出预算，可直接验证 E_LATENCY_BUDGET。</div>
  </section>
</div>
</main>
<script>
const $=id=>document.getElementById(id);
const show=(id,v)=>{$(id).textContent=typeof v==='string'?v:JSON.stringify(v,null,2)};
async function req(url,opt={}){const r=await fetch(url,opt);const t=await r.text();let v;try{v=JSON.parse(t)}catch{v=t}if(!r.ok){throw v}return v}
async function demoRun(){try{show('status','运行中…');const v=await req('/api/demo/run',{method:'POST'});show('status',v.run)}catch(e){show('status',e)}status()}
async function loadDemo(){try{const b=await req('/api/demo');show('status',await req('/api/import',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b)}))}catch(e){show('status',e)}}
async function analyze(){try{show('status','分析中…');show('status',await req('/api/analyze',{method:'POST'}))}catch(e){show('status',e)}}
async function status(){try{show('status',await req('/api/status'))}catch(e){show('status',e)}}
async function stopRun(){try{show('status',await req('/api/stop',{method:'POST'}))}catch(e){show('status',e)}}
async function query(){try{const u='/api/query?doc_id='+encodeURIComponent($('doc').value)+'&scope='+$('scope').value+'&budget_ms='+$('budget').value;show('result',await req(u))}catch(e){show('result',e)}}
</script></body></html>`
