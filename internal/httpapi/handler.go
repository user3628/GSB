package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"ocrlab/internal/apperr"
	"ocrlab/internal/canonical"
	"ocrlab/internal/pipeline"
	"ocrlab/internal/querysvc"
)

type Handler struct {
	pipeline *pipeline.Pipeline
	queries  *querysvc.Service
	mux      *http.ServeMux
}

type queryErrorEnvelope struct {
	QueryID   string        `json:"query_id"`
	ElapsedMS float64       `json:"elapsed_ms"`
	BudgetMS  int           `json:"budget_ms"`
	Error     *apperr.Error `json:"error"`
}

type runRequest struct {
	Groups         []string `json:"groups"`
	Rebuild        bool     `json:"rebuild"`
	StopAfterUnits int      `json:"stop_after_units"`
}

type exportRequest struct {
	Path string `json:"path"`
}

func New(p *pipeline.Pipeline) *Handler {
	handler := &Handler{pipeline: p, queries: querysvc.New(p), mux: http.NewServeMux()}
	handler.mux.HandleFunc("GET /healthz", handler.health)
	handler.mux.HandleFunc("GET /v1/queries/boxes", handler.query(querysvc.KindBoxes))
	handler.mux.HandleFunc("GET /v1/queries/recognition", handler.query(querysvc.KindRecognition))
	handler.mux.HandleFunc("GET /v1/queries/image", handler.query(querysvc.KindImage))
	handler.mux.HandleFunc("GET /v1/queries/diagnostics", handler.query(querysvc.KindDiagnostics))
	handler.mux.HandleFunc("POST /v1/import", handler.importManifest)
	handler.mux.HandleFunc("POST /v1/run", handler.run)
	handler.mux.HandleFunc("POST /v1/export", handler.exportWrite)
	handler.mux.HandleFunc("GET /v1/export", handler.exportRead)
	handler.mux.HandleFunc("POST /v1/verify", handler.verify)
	return handler
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	h.mux.ServeHTTP(writer, request)
}

func (h *Handler) health(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, struct {
		Status         string `json:"status"`
		ManifestDigest string `json:"manifest_digest"`
		ConfigVersion  string `json:"config_version"`
	}{Status: "ok", ManifestDigest: h.pipeline.ManifestDigest(), ConfigVersion: h.pipeline.Config().Version()})
}

func (h *Handler) query(kind string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		var budget *int
		if raw := query.Get("budget_ms"); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil {
				writeQueryError(writer, http.StatusBadRequest, querysvc.Response{}, apperr.New(apperr.CodeInvalidArgument, "budget_ms must be an integer"))
				return
			}
			budget = &value
		}
		force := false
		if raw := query.Get("force_recompute"); raw != "" {
			force, _ = strconv.ParseBool(raw)
		}
		response, err := h.queries.Execute(request.Context(), querysvc.Request{
			Kind: kind, ImageID: query.Get("image_id"), Group: query.Get("group"),
			BudgetMS: budget, ForceRecompute: force,
		})
		if err != nil {
			writeQueryError(writer, statusForError(err), response, err)
			return
		}
		writeJSON(writer, http.StatusOK, response)
	}
}

func (h *Handler) importManifest(writer http.ResponseWriter, request *http.Request) {
	summary, err := h.pipeline.Reimport(request.Context())
	if err != nil {
		writeError(writer, statusForError(err), err)
		return
	}
	writeJSON(writer, http.StatusOK, summary)
}

func (h *Handler) run(writer http.ResponseWriter, request *http.Request) {
	var body runRequest
	if err := decodeOptionalJSON(request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, apperr.Wrap(apperr.CodeInvalidArgument, "decode run request", err))
		return
	}
	summary, err := h.pipeline.Run(request.Context(), pipeline.RunOptions{
		Groups: body.Groups, Rebuild: body.Rebuild, StopAfterUnits: body.StopAfterUnits,
	})
	if err != nil {
		writeError(writer, statusForError(err), err)
		return
	}
	writeJSON(writer, http.StatusOK, summary)
}

func (h *Handler) exportWrite(writer http.ResponseWriter, request *http.Request) {
	var body exportRequest
	if err := decodeOptionalJSON(request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, apperr.Wrap(apperr.CodeInvalidArgument, "decode export request", err))
		return
	}
	report, digest, err := h.pipeline.Export(request.Context(), body.Path)
	if err != nil {
		writeError(writer, statusForError(err), err)
		return
	}
	writeJSON(writer, http.StatusOK, struct {
		ReportDigest string `json:"report_sha256"`
		Groups       int    `json:"groups"`
		Images       int    `json:"images"`
	}{ReportDigest: digest, Groups: len(report.Groups), Images: len(report.Images)})
}

func (h *Handler) exportRead(writer http.ResponseWriter, request *http.Request) {
	_, reportBytes, err := h.pipeline.Report(request.Context())
	if err != nil {
		writeError(writer, statusForError(err), err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Content-Disposition", `attachment; filename="report.json"`)
	writer.Header().Set("X-Report-SHA256", canonical.DigestBytes(reportBytes))
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(reportBytes)
}

func (h *Handler) verify(writer http.ResponseWriter, request *http.Request) {
	summary, err := h.pipeline.VerifyIncremental(request.Context())
	if err != nil {
		writeError(writer, statusForError(err), err)
		return
	}
	writeJSON(writer, http.StatusOK, summary)
}

func decodeOptionalJSON(request *http.Request, target any) error {
	if request.Body == nil {
		return nil
	}
	limited := io.LimitReader(request.Body, 1<<20)
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(target); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	return nil
}

func statusForError(err error) int {
	appErr := apperr.From(err)
	switch appErr.Code {
	case apperr.CodeInvalidArgument:
		return http.StatusBadRequest
	case apperr.CodeNotFound:
		return http.StatusNotFound
	case apperr.CodeLatencyBudget:
		return http.StatusGatewayTimeout
	case apperr.CodeStaleCheckpoint, apperr.CodeIncrementalMismatch:
		return http.StatusConflict
	case apperr.CodeBoxInvalid, apperr.CodeTemplateMiss:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}

func writeQueryError(writer http.ResponseWriter, status int, response querysvc.Response, err error) {
	appErr := apperr.From(err)
	if appErr.QueryID == "" {
		appErr.QueryID = response.QueryID
	}
	envelope := queryErrorEnvelope{
		QueryID: response.QueryID, ElapsedMS: response.ElapsedMS,
		BudgetMS: response.BudgetMS, Error: appErr,
	}
	writeJSON(writer, status, envelope)
}

func writeError(writer http.ResponseWriter, status int, err error) {
	writeJSON(writer, status, struct {
		Error *apperr.Error `json:"error"`
	}{Error: apperr.From(err)})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	body, err := canonical.Marshal(value)
	if err != nil {
		http.Error(writer, `{"error":{"code":"E_INTERNAL","message":"encode response"}}`, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_, _ = writer.Write(body)
}
