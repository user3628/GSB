package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"mitmpipeline/internal/codec"
	"mitmpipeline/internal/domain"
	"mitmpipeline/internal/normalize"
	"mitmpipeline/internal/service"
)

type Server struct {
	Service *service.Service
	Mux     *http.ServeMux
}

func New(svc *service.Service) *Server {
	s := &Server{Service: svc, Mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.Mux.HandleFunc("GET /healthz", s.health)
	s.Mux.HandleFunc("GET /api/v1/compatibility", s.compatibility)
	s.Mux.HandleFunc("POST /api/v1/import", s.importData)
	s.Mux.HandleFunc("GET /api/v1/conclusions", s.listConclusions)
	s.Mux.HandleFunc("GET /api/v1/connections/{id}", s.getConnection)
	s.Mux.HandleFunc("POST /api/v1/connections/{id}/replay", s.replayConnection)
	s.Mux.HandleFunc("GET /api/v1/policy-versions", s.listPolicyVersions)
	s.Mux.HandleFunc("POST /api/v1/policy-versions/switch", s.switchPolicy)
	s.Mux.HandleFunc("GET /api/v1/policy-versions/{version}", s.getPolicyVersion)
	s.Mux.HandleFunc("GET /api/v1/policy-versions/{version}/conclusions", s.versionConclusions)
	s.Mux.HandleFunc("GET /api/v1/recovery", s.recoveryReport)
	s.Mux.HandleFunc("POST /api/v1/recovery", s.runRecovery)
}

func (s *Server) Handler() http.Handler { return s.Mux }

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "mtls-mitm-pipeline"})
}

func (s *Server) compatibility(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "matrix": codec.CompatibilityMatrix()})
}

func (s *Server) importData(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
	if err != nil {
		writeError(w, domain.NewError(domain.CodeRecordCorrupt, "read request body failed").WithContext("error", err.Error()))
		return
	}
	result, err := s.Service.ImportData(r.Context(), body, service.ImportOptions{
		BatchID:      r.URL.Query().Get("batch_id"),
		RoundingRule: queryRounding(r),
	})
	if err != nil {
		extra := map[string]any{}
		if result != nil {
			extra["negotiation"] = result.Negotiation
		}
		writeErrorWithResult(w, err, extra)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": result})
}

func (s *Server) listConclusions(w http.ResponseWriter, r *http.Request) {
	commit := queryInt64(r, "commit")
	conclusions, visible, err := s.Service.Store.ListConclusions(r.Context(), commit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "visible_commit": visible, "conclusions": conclusions})
}

func (s *Server) getConnection(w http.ResponseWriter, r *http.Request) {
	id := normalize.ConnectionID(r.PathValue("id"))
	commit := queryInt64(r, "commit")
	conclusion, err := s.Service.Store.GetConclusion(r.Context(), id, commit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "conclusion": conclusion})
}

func (s *Server) replayConnection(w http.ResponseWriter, r *http.Request) {
	var request struct {
		RoundingRule string `json:"rounding_rule"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&request)
	}
	if request.RoundingRule == "" {
		request.RoundingRule = queryRounding(r)
	}
	conclusion, err := s.Service.ReplayConnection(r.Context(), r.PathValue("id"), request.RoundingRule)
	if err != nil {
		writeErrorWithResult(w, err, map[string]any{"conclusion": conclusion})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "conclusion": conclusion})
}

func (s *Server) listPolicyVersions(w http.ResponseWriter, r *http.Request) {
	versions, err := s.Service.Store.ListPolicyVersions(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "policy_versions": versions})
}

func (s *Server) switchPolicy(w http.ResponseWriter, r *http.Request) {
	var input service.SwitchPolicyInput
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&input); err != nil {
		writeError(w, domain.WrapError(domain.CodeRecordCorrupt, err, "invalid policy switch JSON"))
		return
	}
	result, err := s.Service.SwitchPolicy(r.Context(), input)
	if err != nil {
		writeErrorWithResult(w, err, map[string]any{"result": result})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": result})
}

func (s *Server) getPolicyVersion(w http.ResponseWriter, r *http.Request) {
	stored, perr := s.Service.Store.GetPolicyVersion(r.Context(), r.PathValue("version"))
	if perr != nil {
		status := http.StatusConflict
		if perr.Code == domain.CodeNotFound {
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]any{"ok": false, "error": perr, "policy": stored})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "policy": stored})
}

func (s *Server) versionConclusions(w http.ResponseWriter, r *http.Request) {
	conclusions, perr := s.Service.Store.QueryVersionConclusions(r.Context(), r.PathValue("version"))
	if perr != nil {
		writeError(w, perr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "version": r.PathValue("version"), "conclusions": conclusions})
}

func (s *Server) recoveryReport(w http.ResponseWriter, r *http.Request) {
	report, err := s.Service.Store.LatestRecoveryReport(r.Context())
	if err != nil {
		if de, ok := domain.AsError(err); ok && de.Code == domain.CodeNotFound {
			report, err = s.Service.Store.Recover(r.Context())
		}
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "recovery": report})
}

func (s *Server) runRecovery(w http.ResponseWriter, r *http.Request) {
	report, err := s.Service.Store.Recover(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "recovery": report})
}

func queryRounding(r *http.Request) string {
	rule := r.URL.Query().Get("rounding")
	if rule == "" {
		rule = r.URL.Query().Get("rounding_rule")
	}
	return rule
}

func queryInt64(r *http.Request, key string) int64 {
	value := r.URL.Query().Get(key)
	if value == "" {
		return 0
	}
	parsed, _ := strconv.ParseInt(value, 10, 64)
	return parsed
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	writeErrorWithResult(w, err, nil)
}

func writeErrorWithResult(w http.ResponseWriter, err error, extra map[string]any) {
	de, ok := domain.AsError(err)
	if !ok {
		de = domain.WrapError(domain.CodeInternal, err, "internal error")
	}
	status := http.StatusInternalServerError
	switch de.Code {
	case domain.CodeRecordCorrupt, domain.CodeFormatUnsupported:
		status = http.StatusBadRequest
	case domain.CodeNotFound:
		status = http.StatusNotFound
	case domain.CodeIdentityConflict, domain.CodeVersionRetired, domain.CodeConflict, domain.CodeInvalidTransition:
		status = http.StatusConflict
	case domain.CodeChainInvalid:
		status = http.StatusUnprocessableEntity
	}
	body := map[string]any{"ok": false, "error": de}
	for key, value := range extra {
		if value != nil {
			body[key] = value
		}
	}
	writeJSON(w, status, body)
}

var _ = errors.Is
var _ = io.EOF
