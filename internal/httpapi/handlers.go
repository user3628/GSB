package httpapi

import (
	"net/http"

	"leasearbiter/internal/arbiter"
)

func (s *Server) handleRows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "POST required"))
		return
	}
	var req arbiter.ReadRequest
	if err := decode(r, &req); err != nil {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "invalid JSON: "+err.Error()))
		return
	}
	view, err := s.svc.Engine().Read(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeResult(w, view)
}

func (s *Server) handlePanelOpen(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "POST required"))
		return
	}
	var req arbiter.PanelOpenRequest
	if err := decode(r, &req); err != nil {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "invalid JSON: "+err.Error()))
		return
	}
	resp, err := s.svc.Engine().PanelOpen(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeResult(w, resp)
}

func (s *Server) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		list, err := s.svc.Engine().ListSnapshots(r.Context())
		if err != nil {
			writeAPIError(w, err)
			return
		}
		writeResult(w, list)
	case http.MethodPost:
		var req struct {
			SnapshotVer int64  `json:"snapshot_ver"`
			Holder      string `json:"holder"`
			Action      string `json:"action"`
		}
		if err := decode(r, &req); err != nil {
			writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "invalid JSON: "+err.Error()))
			return
		}
		switch req.Action {
		case "release":
			if err := s.svc.Engine().ReleaseSnapshot(req.SnapshotVer, req.Holder); err != nil {
				writeAPIError(w, err)
				return
			}
		case "prune_orphans":
			n, err := s.svc.Engine().PruneOrphanedSnapshots()
			if err != nil {
				writeAPIError(w, err)
				return
			}
			writeResult(w, map[string]any{"pruned": n})
			return
		default:
			if err := s.svc.Engine().RegisterSnapshot(req.SnapshotVer, req.Holder); err != nil {
				writeAPIError(w, err)
				return
			}
		}
		writeResult(w, map[string]any{"status": "ok"})
	default:
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "GET or POST required"))
	}
}

func (s *Server) handleAcquire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "POST required"))
		return
	}
	var req arbiter.AcquireRequest
	if err := decode(r, &req); err != nil {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "invalid JSON: "+err.Error()))
		return
	}
	resp, err := s.svc.Acquire(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeResult(w, resp)
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "POST required"))
		return
	}
	var req arbiter.RenewRequest
	if err := decode(r, &req); err != nil {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "invalid JSON: "+err.Error()))
		return
	}
	resp, err := s.svc.Renew(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeResult(w, resp)
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "POST required"))
		return
	}
	var req arbiter.ReleaseRequest
	if err := decode(r, &req); err != nil {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "invalid JSON: "+err.Error()))
		return
	}
	if err := s.svc.Release(r.Context(), req); err != nil {
		writeAPIError(w, err)
		return
	}
	writeResult(w, map[string]any{"status": "released"})
}

func (s *Server) handlePrecheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "POST required"))
		return
	}
	var req arbiter.PrecheckRequest
	if err := decode(r, &req); err != nil {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "invalid JSON: "+err.Error()))
		return
	}
	resp, err := s.svc.Engine().Precheck(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if !resp.Completed {
		writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "data": resp,
			"error": map[string]any{"code": string(arbiter.CodePrecheckIncomplete),
				"message": "precheck did not finish within budget"}})
		return
	}
	writeResult(w, resp)
}

func (s *Server) handleReconcile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "POST required"))
		return
	}
	var req arbiter.ReconcileRequest
	if err := decode(r, &req); err != nil {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "invalid JSON: "+err.Error()))
		return
	}
	resp, err := s.svc.Engine().Reconcile(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeResult(w, resp)
}

func (s *Server) handleCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "POST required"))
		return
	}
	var req arbiter.CommitRequest
	if err := decode(r, &req); err != nil {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "invalid JSON: "+err.Error()))
		return
	}
	resp, err := s.svc.Commit(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeResult(w, resp)
}

func (s *Server) handleRecycle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "POST required"))
		return
	}
	var req arbiter.RecycleRequest
	if err := decode(r, &req); err != nil {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "invalid JSON: "+err.Error()))
		return
	}
	resp, err := s.svc.Recycle(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeResult(w, resp)
}

func (s *Server) handleRecoveryReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, arbiterError(arbiter.CodeBadRequest, "GET required"))
		return
	}
	events, err := s.svc.Engine().RecoveryEvents(r.Context())
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeResult(w, map[string]any{"events": events})
}

func arbiterError(code arbiter.ErrorCode, msg string) *arbiter.APIError {
	return &arbiter.APIError{Code: code, Message: msg}
}
