package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"sort"
	"time"

	"example.com/logicplan/editor/internal/bizerr"
	"example.com/logicplan/editor/internal/codec"
	"example.com/logicplan/editor/internal/engine"
	"example.com/logicplan/editor/internal/fault"
	"example.com/logicplan/editor/internal/model"
	"example.com/logicplan/editor/internal/planstate"
)

type Server struct {
	engine  *engine.Engine
	handler http.Handler
	started time.Time
}

func New(editor *engine.Engine) *Server {
	s := &Server{engine: editor, started: time.Now().UTC()}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", allow(http.MethodGet, s.health))
	mux.HandleFunc("/v1/compatibility", allow(http.MethodGet, s.compatibility))
	mux.HandleFunc("/v1/snapshot", allow(http.MethodGet, s.snapshot))
	mux.HandleFunc("/v1/tree", allow(http.MethodGet, s.tree))
	mux.HandleFunc("/v1/indexes/columns", allow(http.MethodGet, s.columnIndex))
	mux.HandleFunc("/v1/indexes/predicates", allow(http.MethodGet, s.predicateIndex))
	mux.HandleFunc("/v1/estimates", allow(http.MethodGet, s.estimates))
	mux.HandleFunc("/v1/degradations", allow(http.MethodGet, s.degradations))
	mux.HandleFunc("/v1/recovery", allow(http.MethodGet, s.recovery))
	mux.HandleFunc("/v1/state", allow(http.MethodGet, s.fullState))
	mux.HandleFunc("/v1/snapshots/import", allow(http.MethodPost, s.importSnapshot))
	mux.HandleFunc("/v1/patches", allow(http.MethodPost, s.applyPatch))
	mux.HandleFunc("/v1/admin/crash-point", allowMultiple(map[string]http.HandlerFunc{
		http.MethodPost: s.armFault, http.MethodDelete: s.disarmFault, http.MethodGet: s.listFaults,
	}))
	mux.HandleFunc("/", s.notFound)
	s.handler = s.withRecovery(mux)
	return s
}

func (s *Server) Handler() http.Handler { return s.handler }

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	state := s.engine.Current()
	response := map[string]any{
		"status":       "ok",
		"started_at":   s.started,
		"has_snapshot": state != nil,
	}
	if state != nil {
		response["version"] = state.Version
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) compatibility(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"matrix": codec.Matrix()})
}

func (s *Server) snapshot(w http.ResponseWriter, r *http.Request) {
	state, err := s.current()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, state.Summary())
}

type treeNode struct {
	ID        string              `json:"id"`
	Type      model.OperatorType  `json:"type"`
	Children  []string            `json:"children"`
	Columns   []model.ColumnRef   `json:"columns"`
	Predicate *model.Predicate    `json:"predicate,omitempty"`
	Stats     *model.StatsBinding `json:"stats,omitempty"`
	Outputs   []model.ColumnRef   `json:"outputs"`
}

func (s *Server) tree(w http.ResponseWriter, r *http.Request) {
	state, err := s.current()
	if err != nil {
		writeError(w, err)
		return
	}
	nodes := make([]treeNode, 0, len(state.Nodes))
	for _, node := range state.NodeList() {
		nodes = append(nodes, treeNode{
			ID: node.ID, Type: node.Type, Children: node.Children, Columns: node.Columns,
			Predicate: node.Predicate, Stats: node.Stats, Outputs: state.Outputs[node.ID],
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version": state.Version, "root": state.Root, "tree_hash": state.Derived.RootHash, "nodes": nodes,
	})
}

func (s *Server) columnIndex(w http.ResponseWriter, r *http.Request) {
	state, err := s.current()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": state.Version, "index": state.Derived.ColumnIndex})
}

func (s *Server) predicateIndex(w http.ResponseWriter, r *http.Request) {
	state, err := s.current()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": state.Version, "index": state.Derived.PredicateIndex})
}

func (s *Server) estimates(w http.ResponseWriter, r *http.Request) {
	state, err := s.current()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": state.Version, "estimate": state.Derived.CostEstimate})
}

func (s *Server) degradations(w http.ResponseWriter, r *http.Request) {
	state, err := s.current()
	if err != nil {
		writeError(w, err)
		return
	}
	items := state.Degradations
	if items == nil {
		items = []model.Degradation{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version": state.Version, "decision": state.Decision, "degradations": items,
	})
}

func (s *Server) recovery(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.engine.Recovery())
}

func (s *Server) fullState(w http.ResponseWriter, r *http.Request) {
	state, err := s.current()
	if err != nil {
		writeError(w, err)
		return
	}
	nodes := make([]treeNode, 0, len(state.Nodes))
	for _, node := range state.NodeList() {
		nodes = append(nodes, treeNode{
			ID: node.ID, Type: node.Type, Children: node.Children, Columns: node.Columns,
			Predicate: node.Predicate, Stats: node.Stats, Outputs: state.Outputs[node.ID],
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot": state.Summary(), "tree": nodes, "derived": state.Derived,
		"degradations": state.Degradations,
	})
}

type importEnvelope struct {
	RequestID  string `json:"request_id"`
	CommitID   string `json:"commit_id"`
	Encoding   string `json:"encoding"`
	DataBase64 string `json:"data_base64"`
	Data       string `json:"data"`
}

func (s *Server) importSnapshot(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024*1024+1))
	if err != nil {
		writeError(w, bizerr.New(bizerr.CodeBadRequest, "read import body", map[string]any{"cause": err.Error()}))
		return
	}
	if len(body) > 64*1024*1024 {
		writeError(w, bizerr.New(bizerr.CodeBadRequest, "import body exceeds 64 MiB", nil))
		return
	}
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = r.Header.Get("X-Commit-ID")
	}
	if requestID == "" {
		requestID = r.URL.Query().Get("request_id")
	}
	if requestID == "" {
		requestID = r.URL.Query().Get("commit_id")
	}
	data := body
	var envelope importEnvelope
	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaType == "application/json" {
		if json.Unmarshal(body, &envelope) == nil && (envelope.DataBase64 != "" || envelope.Data != "") {
			if requestID == "" {
				requestID = envelope.RequestID
			}
			if requestID == "" {
				requestID = envelope.CommitID
			}
			if envelope.DataBase64 != "" {
				decoded, err := base64.StdEncoding.DecodeString(envelope.DataBase64)
				if err != nil {
					writeError(w, bizerr.New(bizerr.CodeBadRequest, "data_base64 is not valid base64", map[string]any{"cause": err.Error()}))
					return
				}
				data = decoded
			} else {
				data = []byte(envelope.Data)
			}
		}
	}
	result, err := s.engine.Import(r.Context(), data, requestID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) applyPatch(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 16*1024*1024))
	dec.DisallowUnknownFields()
	var patch engine.PatchRequest
	if err := dec.Decode(&patch); err != nil {
		writeError(w, bizerr.New(bizerr.CodeBadRequest, "invalid incremental fragment JSON", map[string]any{"cause": err.Error()}))
		return
	}
	result, err := s.engine.ApplyPatch(r.Context(), patch)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type armRequest struct {
	Stage   string `json:"stage"`
	Mode    string `json:"mode"`
	Skip    int    `json:"skip"`
	DelayMS int64  `json:"delay_ms"`
}

func (s *Server) armFault(w http.ResponseWriter, r *http.Request) {
	var request armRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&request); err != nil {
		writeError(w, bizerr.New(bizerr.CodeBadRequest, "invalid fault configuration", map[string]any{"cause": err.Error()}))
		return
	}
	if request.Mode == "" {
		request.Mode = "crash"
	}
	if err := s.engine.Faults().Arm(request.Stage, request.Mode, request.Skip, time.Duration(request.DelayMS)*time.Millisecond); err != nil {
		writeError(w, bizerr.New(bizerr.CodeBadRequest, err.Error(), map[string]any{"stage": request.Stage, "mode": request.Mode}))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"armed": true, "rules": s.engine.Faults().Rules(), "stages": fault.Stages})
}

func (s *Server) disarmFault(w http.ResponseWriter, r *http.Request) {
	stage := r.URL.Query().Get("stage")
	if stage == "" {
		writeError(w, bizerr.New(bizerr.CodeBadRequest, "stage query parameter is required", nil))
		return
	}
	s.engine.Faults().Disarm(stage)
	writeJSON(w, http.StatusOK, map[string]any{"armed": false, "stage": stage, "rules": s.engine.Faults().Rules()})
}

func (s *Server) listFaults(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"rules": s.engine.Faults().Rules(), "stages": fault.Stages})
}

func (s *Server) notFound(w http.ResponseWriter, r *http.Request) {
	writeError(w, bizerr.New(bizerr.CodeBadRequest, "route not found", map[string]any{"method": r.Method, "path": r.URL.Path}))
}

func allow(method string, handler http.HandlerFunc) http.HandlerFunc {
	return allowMultiple(map[string]http.HandlerFunc{method: handler})
}

func allowMultiple(handlers map[string]http.HandlerFunc) http.HandlerFunc {
	allowed := make([]string, 0, len(handlers))
	for method := range handlers {
		allowed = append(allowed, method)
	}
	sort.Strings(allowed)
	return func(w http.ResponseWriter, r *http.Request) {
		if handler, ok := handlers[r.Method]; ok {
			handler(w, r)
			return
		}
		writeError(w, bizerr.New(bizerr.CodeBadRequest, "HTTP method not allowed", map[string]any{
			"method": r.Method, "path": r.URL.Path, "allowed_methods": allowed,
		}))
	}
}

func (s *Server) current() (*planstate.State, error) {
	state := s.engine.Current()
	if state == nil {
		return nil, bizerr.New(bizerr.CodeSnapshotNotFound, "no committed snapshot is loaded", nil)
	}
	return state, nil
}

func (s *Server) withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				writeError(w, bizerr.New(bizerr.CodeInternal, "request handler panicked", map[string]any{"panic": fmt.Sprint(recovered)}))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	be, ok := bizerr.As(err)
	if !ok {
		be = bizerr.New(bizerr.CodeInternal, "unexpected internal error", map[string]any{"cause": err.Error()})
	}
	status := statusFor(be.Code)
	writeJSON(w, status, be)
}

func statusFor(code string) int {
	switch code {
	case bizerr.CodeBadRequest, bizerr.CodeFormatCorrupt:
		return http.StatusBadRequest
	case bizerr.CodePlanInvalid, bizerr.CodeFormatUnsupported:
		return http.StatusUnprocessableEntity
	case bizerr.CodeSnapshotStale, bizerr.CodeStaleBase, bizerr.CodeConflictingRequest:
		return http.StatusConflict
	case bizerr.CodeSnapshotNotFound:
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}
