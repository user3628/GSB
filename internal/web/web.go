package web

import (
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"path/filepath"
	"time"

	"imretrace/internal/model"
	"imretrace/internal/pipeline"
)

//go:embed index.html
var content embed.FS

type Handler struct {
	Svc    *pipeline.Service
	OnStop func()
}

func NewHandler(svc *pipeline.Service, onStop func()) http.Handler {
	h := &Handler{Svc: svc, OnStop: onStop}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", h.index)
	mux.HandleFunc("GET /api/status", h.status)
	mux.HandleFunc("GET /api/items", h.items)
	mux.HandleFunc("POST /api/prepare", h.prepare)
	mux.HandleFunc("POST /api/generate", h.generate)
	mux.HandleFunc("POST /api/build", h.build)
	mux.HandleFunc("POST /api/recover", h.build)
	mux.HandleFunc("GET /api/search/text", h.searchText)
	mux.HandleFunc("POST /api/search/text", h.searchText)
	mux.HandleFunc("GET /api/search/image", h.searchImage)
	mux.HandleFunc("POST /api/search/image", h.searchImage)
	mux.HandleFunc("GET /api/policy", h.status)
	mux.HandleFunc("POST /api/policy/patch", h.patchPolicy)
	mux.HandleFunc("POST /api/policy/replace", h.replacePolicy)
	mux.HandleFunc("GET /api/report", h.report)
	mux.HandleFunc("POST /api/export", h.export)
	mux.HandleFunc("POST /api/stop", h.stop)
	return logging(mux)
}

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
}

func (h *Handler) index(w http.ResponseWriter, r *http.Request) {
	b, err := fs.ReadFile(content, "index.html")
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	if biz, ok := model.AsBiz(err); ok {
		code := http.StatusUnprocessableEntity
		switch biz.Code {
		case model.CodeStaleVersion, model.CodeStaleCheckpoint:
			code = http.StatusConflict
		case model.CodePolicyInvalid:
			code = http.StatusBadRequest
		}
		writeJSON(w, code, map[string]any{"ok": false, "error": biz})
		return
	}
	writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": map[string]string{"code": model.CodeInternal, "message": err.Error()}})
}

func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	st, err := h.Svc.Status(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, st)
}

func (h *Handler) items(w http.ResponseWriter, r *http.Request) {
	items, _, err := h.Svc.Store.ListItems(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "items": items})
}

func (h *Handler) prepare(w http.ResponseWriter, r *http.Request) {
	items, err := h.Svc.EnsureDemo(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "items": items})
}

func (h *Handler) generate(w http.ResponseWriter, r *http.Request) {
	items, err := h.Svc.RegenerateDemo(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "items": items})
}

func (h *Handler) build(w http.ResponseWriter, r *http.Request) {
	rep, err := h.Svc.Build(r.Context(), pipeline.BuildOptions{})
	if err != nil {
		if biz, ok := model.AsBiz(err); ok && biz.Partial == nil && rep != nil {
			biz.Partial = rep
		}
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "report": rep})
}

type textQueryReq struct {
	Query string `json:"query"`
	K     int    `json:"k"`
}

func (h *Handler) searchText(w http.ResponseWriter, r *http.Request) {
	var q string
	var k int
	if r.Method == http.MethodPost {
		var req textQueryReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, model.NewBiz(model.CodeQueryInvalid, "http", "", "invalid JSON: %v", err))
			return
		}
		q, k = req.Query, req.K
	} else {
		q, k = r.URL.Query().Get("q"), atoiDefault(r.URL.Query().Get("k"), 0)
	}
	res, err := h.Svc.SearchText(r.Context(), q, k)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

type imageQueryReq struct {
	ItemID string `json:"item_id"`
	K      int    `json:"k"`
}

func (h *Handler) searchImage(w http.ResponseWriter, r *http.Request) {
	var itemID string
	var k int
	if r.Method == http.MethodPost {
		var req imageQueryReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, model.NewBiz(model.CodeQueryInvalid, "http", "", "invalid JSON: %v", err))
			return
		}
		itemID, k = req.ItemID, req.K
	} else {
		itemID, k = r.URL.Query().Get("item_id"), atoiDefault(r.URL.Query().Get("k"), 0)
	}
	res, err := h.Svc.SearchImage(r.Context(), itemID, k)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

func (h *Handler) patchPolicy(w http.ResponseWriter, r *http.Request) {
	var patch model.Patch
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeError(w, model.NewBiz(model.CodePolicyInvalid, "policy_patch", "", "invalid JSON: %v", err))
		return
	}
	p, changed, err := h.Svc.ApplyPatch(r.Context(), patch)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "changed": changed, "policy": p})
}

func (h *Handler) replacePolicy(w http.ResponseWriter, r *http.Request) {
	var p model.Policy
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeError(w, model.NewBiz(model.CodePolicyInvalid, "policy_replace", "", "invalid JSON: %v", err))
		return
	}
	saved, changed, err := h.Svc.ReplacePolicy(r.Context(), p)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "changed": changed, "policy": saved})
}

func (h *Handler) report(w http.ResponseWriter, r *http.Request) {
	rep, err := h.Svc.ExportReport(r.Context(), "")
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, rep)
}

type exportReq struct {
	Path string `json:"path"`
}

func (h *Handler) export(w http.ResponseWriter, r *http.Request) {
	var req exportReq
	_ = json.NewDecoder(r.Body).Decode(&req)
	out := req.Path
	if out != "" && !filepath.IsAbs(out) {
		out = filepath.Join(h.Svc.Paths.ExportDir, filepath.Base(out))
	}
	rep, err := h.Svc.ExportReport(r.Context(), out)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "path": outOrDefault(out, h.Svc.Paths.ReportPath), "state_hash": rep.StateHash})
}

func outOrDefault(out, def string) string {
	if out == "" {
		return def
	}
	return out
}

func (h *Handler) stop(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"ok": true, "stopping": true})
	if h.OnStop != nil {
		go func() { time.Sleep(120 * time.Millisecond); h.OnStop() }()
	}
}

func atoiDefault(s string, def int) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return def
		}
		n = n*10 + int(r-'0')
	}
	if s == "" {
		return def
	}
	return n
}
