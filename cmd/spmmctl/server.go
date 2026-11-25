package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"spmmopt/engine"
	"spmmopt/samples"
	"spmmopt/spmm"
	"spmmopt/store"
)

type apiServer struct {
	opts     planOptions
	manifest *samples.Manifest
	mu       sync.Mutex
	cache    map[string]*samples.Sample
}

type planAPIRequest struct {
	Sample        string  `json:"sample"`
	Sum           string  `json:"sum"`
	Order         string  `json:"order"`
	SkipZeros     *bool   `json:"skip_zeros"`
	CacheBytes    int64   `json:"cache_bytes"`
	PeakBytes     int64   `json:"peak_bytes"`
	RowBlock      int     `json:"row_block"`
	ColBlock      int     `json:"col_block"`
	ErrorSamples  int     `json:"error_samples"`
	AbsTolerance  float64 `json:"abs_tolerance"`
	RelTolerance  float64 `json:"rel_tolerance"`
	InteractiveMS float64 `json:"interactive_budget_ms"`
}

type executionAPIRequest struct {
	PlanKey   string  `json:"plan_key"`
	Sample    string  `json:"sample"`
	Start     int     `json:"start"`
	End       int     `json:"end"`
	LatencyMS float64 `json:"latency_ms"`
	QueryID   string  `json:"query_id"`
	Persist   *bool   `json:"persist"`
}

type reconcileAPIRequest struct {
	PlanKey         string `json:"plan_key"`
	ExpectedVersion int64  `json:"expected_version"`
	ExpectedDigest  string `json:"expected_digest"`
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	o := addPlanFlags(fs, false)
	addr := fs.String("addr", "127.0.0.1:8787", "local HTTP listen address")
	genSeed := fs.Int64("gen-seed", samples.DefaultSeed, "sample seed used if the corpus must be generated")
	if err := fs.Parse(args); err != nil {
		return err
	}
	m, err := ensureManifest(o.samplesDir, *genSeed)
	if err != nil {
		return err
	}
	app := &apiServer{opts: *o, manifest: m, cache: make(map[string]*samples.Sample)}
	mux := http.NewServeMux()
	mux.HandleFunc("/", app.handleIndex)
	mux.HandleFunc("/api/health", app.handleHealth)
	mux.HandleFunc("/api/samples", app.handleSamples)
	mux.HandleFunc("/api/status", app.handleStatus)
	mux.HandleFunc("/api/plan", app.handlePlan)
	mux.HandleFunc("/api/history", app.handleHistory)
	mux.HandleFunc("/api/state", app.handleState)
	mux.HandleFunc("/api/query", app.handleQuery)
	mux.HandleFunc("/api/settle", app.handleSettle)
	mux.HandleFunc("/api/compare", app.handleCompare)
	mux.HandleFunc("/api/reconcile", app.handleReconcile)
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", *addr, err)
	}
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt)
	defer signal.Stop(shutdown)
	go func() {
		<-shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()
	fmt.Printf("SpMM diagnostic app listening on http://%s\n", listener.Addr().String())
	if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (a *apiServer) sample(name string) (*samples.Sample, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if s := a.cache[name]; s != nil {
		return s, nil
	}
	s, err := a.manifest.Load(a.opts.samplesDir, name)
	if err != nil {
		return nil, err
	}
	a.cache[name] = s
	return s, nil
}

func (a *apiServer) planOptionsFor(req planAPIRequest) planOptions {
	o := a.opts
	if req.Sample != "" {
		o.sample = req.Sample
	}
	if req.Sum != "" {
		o.sum = req.Sum
	}
	if req.Order != "" {
		o.order = req.Order
	}
	if req.SkipZeros != nil {
		o.skipZeros = *req.SkipZeros
	}
	if req.CacheBytes > 0 {
		o.cacheBytes = req.CacheBytes
	}
	if req.PeakBytes > 0 {
		o.peakBytes = req.PeakBytes
	}
	o.rowBlock = req.RowBlock
	o.colBlock = req.ColBlock
	if req.ErrorSamples > 0 {
		o.errorSamples = req.ErrorSamples
	}
	if req.AbsTolerance > 0 {
		o.absTol = req.AbsTolerance
	}
	if req.RelTolerance > 0 {
		o.relTol = req.RelTolerance
	}
	if req.InteractiveMS > 0 {
		o.interactiveMS = req.InteractiveMS
	}
	return o
}

func (a *apiServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, diagnosticHTML)
}

func (a *apiServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeAPIOK(w, map[string]any{"ok": true, "service": "spmm-diagnostic"})
}

func (a *apiServer) handleSamples(w http.ResponseWriter, r *http.Request) {
	type sampleInfo struct {
		Name             string  `json:"name"`
		Shape            string  `json:"shape"`
		DeclaredSparsity float64 `json:"declared_sparsity"`
		Rows             int     `json:"rows"`
		Cols             int     `json:"cols"`
		DenseCols        int     `json:"dense_cols"`
		NNZ              int     `json:"nnz"`
		ObservedSparsity float64 `json:"observed_sparsity"`
		EmptyRows        int     `json:"empty_rows"`
		DuplicateEntries int     `json:"duplicate_entries"`
		ExplicitZeros    int     `json:"explicit_zeros"`
		Degenerate       bool    `json:"degenerate"`
	}
	out := make([]sampleInfo, 0, len(a.manifest.Samples))
	for _, s := range a.manifest.Samples {
		out = append(out, sampleInfo{
			Name: s.Name, Shape: s.Shape, DeclaredSparsity: s.DeclaredSparsity,
			Rows: s.Rows, Cols: s.Cols, DenseCols: s.DenseCols, NNZ: s.Stats.NNZ,
			ObservedSparsity: s.Stats.ObservedSparsity, EmptyRows: s.Stats.EmptyRows,
			DuplicateEntries: s.Stats.DuplicateEntries, ExplicitZeros: s.Stats.ExplicitZeros,
			Degenerate: s.Degenerate,
		})
	}
	writeAPIOK(w, map[string]any{"samples": out, "database": a.opts.db})
}

func (a *apiServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	st, err := store.Open(ctx, a.opts.db)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer st.Close()
	keys, err := st.SortedKeys(ctx)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPIOK(w, map[string]any{
		"database": a.opts.db, "plan_count": len(keys), "plan_keys": keys,
		"samples": len(a.manifest.Samples),
	})
}

func (a *apiServer) handlePlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req planAPIRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	o := a.planOptionsFor(req)
	s, err := a.sample(o.sample)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	p, err := o.selectPlan(s)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	ctx := context.Background()
	st, err := store.Open(ctx, o.db)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer st.Close()
	rec, err := publishChosen(ctx, st, p)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPIOK(w, map[string]any{"plan_key": rec.PlanKey, "version": rec.Version, "digest": rec.Digest, "plan": rec.Plan})
}

func (a *apiServer) handleHistory(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("plan_key")
	if key == "" {
		writeAPIError(w, spmm.NewError(spmm.CodeInput, "plan_key is required"))
		return
	}
	ctx := context.Background()
	st, err := store.Open(ctx, a.opts.db)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer st.Close()
	history, err := st.History(ctx, key)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPIOK(w, map[string]any{"history": history})
}

func (a *apiServer) handleState(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("plan_key")
	if key == "" {
		writeAPIError(w, spmm.NewError(spmm.CodeInput, "plan_key is required"))
		return
	}
	ctx := context.Background()
	st, err := store.Open(ctx, a.opts.db)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer st.Close()
	state, err := st.State(ctx, key)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	invalidations, _ := st.Invalidations(ctx, key)
	writeAPIOK(w, map[string]any{"state": state, "invalidations": invalidations})
}

func (a *apiServer) openEngine(ctx context.Context, req executionAPIRequest) (*store.Store, *engine.Engine, *samples.Sample, error) {
	s, err := a.sample(req.Sample)
	if err != nil {
		return nil, nil, nil, err
	}
	if req.PlanKey == "" {
		return nil, nil, nil, spmm.NewError(spmm.CodeInput, "plan_key is required")
	}
	st, err := store.Open(ctx, a.opts.db)
	if err != nil {
		return nil, nil, nil, err
	}
	en, err := engine.Open(ctx, st, s.CSR, s.Dense, req.PlanKey)
	if err != nil {
		st.Close()
		return nil, nil, nil, err
	}
	return st, en, s, nil
}

func (a *apiServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req executionAPIRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	ctx := context.Background()
	st, en, s, err := a.openEngine(ctx, req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer st.Close()
	if req.End == 0 {
		req.End = s.CSR.Rows
	}
	if req.LatencyMS <= 0 {
		req.LatencyMS = en.Plan.InteractiveBudgetMS
	}
	result, err := en.Query(ctx, engine.QueryOptions{
		RowStart: req.Start, RowEnd: req.End, QueryID: req.QueryID,
		LatencyBudget: time.Duration(req.LatencyMS * float64(time.Millisecond)),
	})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPIOK(w, result)
}

func (a *apiServer) handleSettle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req executionAPIRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	ctx := context.Background()
	st, en, s, err := a.openEngine(ctx, req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer st.Close()
	if req.End == 0 {
		req.End = s.CSR.Rows
	}
	persist := req.Start == 0 && req.End == s.CSR.Rows
	if req.Persist != nil {
		persist = *req.Persist
	}
	var result *engine.Result
	if req.Start == 0 && req.End == s.CSR.Rows {
		result, err = en.Settle(ctx, engine.RunOptions{Persist: persist})
	} else {
		result, err = en.SettleRange(ctx, req.Start, req.End, engine.RunOptions{Persist: persist})
	}
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPIOK(w, result)
}

func (a *apiServer) handleCompare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req executionAPIRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	ctx := context.Background()
	st, en, s, err := a.openEngine(ctx, req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer st.Close()
	if req.End == 0 {
		req.End = s.CSR.Rows
	}
	if req.LatencyMS <= 0 {
		req.LatencyMS = en.Plan.InteractiveBudgetMS
	}
	query, err := en.Query(ctx, engine.QueryOptions{
		RowStart: req.Start, RowEnd: req.End, QueryID: req.QueryID,
		LatencyBudget: time.Duration(req.LatencyMS * float64(time.Millisecond)),
	})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	persist := false
	if req.Persist != nil {
		persist = *req.Persist
	}
	settlement, err := en.Settle(ctx, engine.RunOptions{Persist: persist})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	matchErr := engine.ComparePaths(query, settlement)
	payload := map[string]any{
		"match": matchErr == nil, "query": query, "settlement": settlement,
	}
	if matchErr != nil {
		if e, ok := spmm.AsError(matchErr); ok {
			payload["error"] = e
		} else {
			payload["error"] = matchErr.Error()
		}
	}
	writeJSONResponse(w, http.StatusOK, payload)
}

func (a *apiServer) handleReconcile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req reconcileAPIRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	ctx := context.Background()
	st, err := store.Open(ctx, a.opts.db)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer st.Close()
	var expected *store.PlanRef
	if req.ExpectedVersion != 0 || req.ExpectedDigest != "" {
		expected = &store.PlanRef{PlanKey: req.PlanKey, Version: req.ExpectedVersion, Digest: req.ExpectedDigest}
	}
	report, err := st.ReconcilePlan(ctx, req.PlanKey, expected)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPIOK(w, report)
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return spmm.NewError(spmm.CodeInput, "decode request: %v", err)
	}
	return nil
}

func writeAPIOK(w http.ResponseWriter, payload any) {
	if m, ok := payload.(map[string]any); ok {
		m["ok"] = true
	} else {
		payload = map[string]any{"ok": true, "result": payload}
	}
	writeJSONResponse(w, http.StatusOK, payload)
}

func writeAPIError(w http.ResponseWriter, err error) {
	status := http.StatusOK
	if spmm.IsCode(err, spmm.CodeInput) {
		status = http.StatusBadRequest
	}
	var payload any = map[string]any{"ok": false, "error": err.Error()}
	if e, ok := spmm.AsError(err); ok {
		payload = map[string]any{"ok": false, "error": e}
	}
	writeJSONResponse(w, status, payload)
}

func writeJSONResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

const diagnosticHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>SpMM Diagnostic Lab</title>
<style>
:root{color-scheme:light dark;--bg:#f5f3ef;--card:#fff;--ink:#1c1b19;--muted:#6d6a64;--line:#d9d3c7;--accent:#086f69}
@media(prefers-color-scheme:dark){:root{--bg:#171816;--card:#22231f;--ink:#efeee8;--muted:#aaa69c;--line:#3c3d37;--accent:#62d6ca}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
main{max-width:1180px;margin:0 auto;padding:28px 18px 60px}h1{font:700 27px/1.2 ui-sans-serif,system-ui;margin:0 0 6px}h2{font:650 16px/1.3 ui-sans-serif,system-ui;margin:0 0 14px}.sub{color:var(--muted);margin-bottom:22px}
.grid{display:grid;grid-template-columns:360px 1fr;gap:18px}@media(max-width:850px){.grid{grid-template-columns:1fr}}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px;box-shadow:0 8px 24px rgba(0,0,0,.04)}
label{display:block;color:var(--muted);font-size:12px;margin:9px 0 4px}input,select{width:100%;padding:8px 9px;border:1px solid var(--line);border-radius:7px;background:var(--bg);color:var(--ink)}
.row{display:grid;grid-template-columns:1fr 1fr;gap:8px}.checks{display:flex;gap:14px;align-items:center;margin-top:12px;color:var(--muted)}.checks input{width:auto}
button{margin:8px 6px 0 0;padding:9px 13px;border:1px solid var(--accent);border-radius:7px;background:var(--accent);color:#fff;font-weight:650;cursor:pointer}button.alt{background:transparent;color:var(--accent)}
pre{white-space:pre-wrap;overflow-wrap:anywhere;margin:0;max-height:72vh;overflow:auto;background:#11140f;color:#e7f1dd;border-radius:10px;padding:16px;min-height:420px}
.pill{display:inline-block;padding:2px 8px;border-radius:99px;background:var(--accent);color:#fff;font-size:11px;margin-left:7px}.hint{color:var(--muted);font-size:12px;margin-top:10px}
</style></head>
<body><main>
<h1>SpMM Diagnostic Lab <span class="pill">local</span></h1>
<div class="sub">Deterministic CSR × dense blocking, dual-path adjudication, sampled error bounds and persisted recovery facts.</div>
<div class="grid">
<section class="card">
<h2>Run controls</h2>
<label>Sample</label><select id="sample"></select>
<div class="row"><div><label>Start row</label><input id="start" type="number" value="0"></div><div><label>End row (-1=all)</label><input id="end" type="number" value="-1"></div></div>
<div class="row"><div><label>Latency budget ms</label><input id="latency" type="number" value="50"></div><div><label>Peak bytes</label><input id="peak" type="number" value="16777216"></div></div>
<div class="row"><div><label>Sum strategy</label><select id="sum"><option>compensated</option><option>sequential</option></select></div><div><label>Order</label><select id="order"><option>block_order</option><option>column_ascending</option></select></div></div>
<div class="checks"><label style="margin:0"><input id="skip" type="checkbox" checked> skip explicit zeros</label><span id="planstate" style="color:var(--muted)">no plan yet</span></div>
<button onclick="plan()">Select + publish plan</button>
<div><button class="alt" onclick="query()">Query</button><button class="alt" onclick="settle()">Settle + persist</button><button class="alt" onclick="compare()">Compare paths</button></div>
<div class="hint">Plan, query, settlement, candidate scores, memory splits and per-block summaries are returned as JSON. Re-running with the same sample/config advances the persisted plan version.</div>
</section>
<section class="card"><h2>Diagnostic output</h2><pre id="out">Select a sample and publish a plan.</pre></section>
</div></main>
<script>
let planKey="";
async function api(path, body){
  const r=await fetch(path,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(body)});
  const j=await r.json(); document.getElementById("out").textContent=JSON.stringify(j,null,2); if(!j.ok) throw new Error((j.error&&j.error.code)||j.error||"request failed"); return j;
}
function bounds(){const start=+document.getElementById("start").value;let end=+document.getElementById("end").value;if(end<0)end=+document.getElementById("sample").dataset.rows;return {start,end};}
function base(){return {sample:document.getElementById("sample").value,...bounds(),latency_ms:+document.getElementById("latency").value,plan_key:planKey,persist:true};}
async function plan(){
  try{const j=await api("/api/plan",{sample:document.getElementById("sample").value,sum:document.getElementById("sum").value,order:document.getElementById("order").value,skip_zeros:document.getElementById("skip").checked,peak_bytes:+document.getElementById("peak").value});planKey=j.plan_key;document.getElementById("planstate").textContent="v"+j.version+" · "+j.plan_key.slice(0,12)+"…";}catch(e){}
}
async function query(){try{await api("/api/query",base());}catch(e){}}
async function settle(){try{await api("/api/settle",base());}catch(e){}}
async function compare(){try{await api("/api/compare",base());}catch(e){}}
fetch("/api/samples").then(r=>r.json()).then(j=>{const s=document.getElementById("sample");for(const x of j.samples){const o=document.createElement("option");o.value=x.name;o.textContent=x.name+" · "+x.shape+" · "+(x.declared_sparsity*100).toFixed(0)+"% · nnz="+x.nnz;o.dataset.rows=x.rows;s.appendChild(o);}document.getElementById("end").value=j.samples[0].rows;document.getElementById("sample").onchange=()=>{document.getElementById("end").value=document.getElementById("sample").selectedOptions[0].dataset.rows;planKey="";document.getElementById("planstate").textContent="no plan yet";};});
</script></body></html>`
