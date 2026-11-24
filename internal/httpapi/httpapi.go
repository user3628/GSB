// Package httpapi exposes the local single-writer replication lag engine.
package httpapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"replicationlag/internal/apperr"
	"replicationlag/internal/domain"
	"replicationlag/internal/engine"
	"replicationlag/internal/store"
)

const maxRequestBody = 32 << 20

type Server struct {
	engine *engine.Engine
	mux    *http.ServeMux
}

func New(engineInstance *engine.Engine) *Server {
	server := &Server{engine: engineInstance, mux: http.NewServeMux()}
	server.routes()
	return server
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("GET /v1/version", s.version)
	s.mux.HandleFunc("GET /v1/observations", s.listObservations)
	s.mux.HandleFunc("POST /v1/observations", s.upsertObservation)
	s.mux.HandleFunc("PUT /v1/observations/{replicaID}/{sampledAt}", s.putObservation)
	s.mux.HandleFunc("DELETE /v1/observations/{replicaID}/{sampledAt}", s.deleteObservation)
	s.mux.HandleFunc("POST /v1/import", s.importObservations)
	s.mux.HandleFunc("POST /v1/merge", s.merge)
	s.mux.HandleFunc("GET /v1/sequences", s.listSequences)
	s.mux.HandleFunc("GET /v1/replicas/{replicaID}/sequence", s.getSequence)
	s.mux.HandleFunc("GET /v1/replicas/{replicaID}/state", s.getState)
	s.mux.HandleFunc("GET /v1/export", s.export)
	s.mux.HandleFunc("GET /v1/recovery/report", s.recoveryReport)
	s.mux.HandleFunc("GET /v1/policy", s.getPolicy)
	s.mux.HandleFunc("PUT /v1/policy", s.putPolicy)
	s.mux.HandleFunc("POST /v1/state/validate", s.validateState)
	s.mux.HandleFunc("/", s.notFound)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) version(w http.ResponseWriter, r *http.Request) {
	version, err := s.engine.Store().Version(r.Context())
	if err != nil {
		writeError(w, apperr.As(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": version, "policy": s.engine.Policy()})
}

func (s *Server) listObservations(w http.ResponseWriter, r *http.Request) {
	observations, version, err := s.engine.ListObservations(r.Context())
	if err != nil {
		writeError(w, apperr.As(err))
		return
	}
	views := make([]domain.ObservationView, len(observations))
	for i, observation := range observations {
		views[i] = observation.View()
	}
	setVersion(w, version)
	writeJSON(w, http.StatusOK, map[string]any{"version": version, "observations": views, "count": len(views)})
}

type observationEnvelope struct {
	BatchID     string                  `json:"batch_id"`
	BaseVersion *int64                  `json:"base_version"`
	Observation engine.ObservationInput `json:"observation"`
}

func (s *Server) upsertObservation(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		writeError(w, err)
		return
	}
	envelope, parseErr := decodeObservationEnvelope(body)
	if parseErr != nil {
		writeError(w, parseErr)
		return
	}
	result, submitErr := s.engine.UpsertObservation(r.Context(), engine.UpsertRequest{
		BatchID: envelope.BatchID, BaseVersion: envelope.BaseVersion, Observation: envelope.Observation,
	})
	s.writeWriteResult(w, result, submitErr)
}

func (s *Server) putObservation(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		writeError(w, err)
		return
	}
	envelope, parseErr := decodeObservationEnvelope(body)
	if parseErr != nil {
		writeError(w, parseErr)
		return
	}
	envelope.Observation.ReplicaID = r.PathValue("replicaID")
	sampledAt, parseTimeErr := strconv.ParseInt(r.PathValue("sampledAt"), 10, 64)
	if parseTimeErr != nil {
		writeError(w, apperr.New(apperr.CodeBadRequest, "sample time path value must be an integer", map[string]any{"value": r.PathValue("sampledAt")}))
		return
	}
	envelope.Observation.SampledAtMS = sampledAt
	result, submitErr := s.engine.UpsertObservation(r.Context(), engine.UpsertRequest{
		BatchID: envelope.BatchID, BaseVersion: envelope.BaseVersion, Observation: envelope.Observation,
	})
	s.writeWriteResult(w, result, submitErr)
}

func decodeObservationEnvelope(body []byte) (observationEnvelope, *apperr.Error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(body, &object); err != nil {
		return observationEnvelope{}, apperr.New(apperr.CodeBadRequest, "request body must be a JSON object", map[string]any{"detail": err.Error()})
	}
	var envelope observationEnvelope
	if raw, ok := object["observation"]; ok {
		if err := json.Unmarshal(raw, &envelope.Observation); err != nil {
			return observationEnvelope{}, observationDecodeError(raw, err)
		}
		if value, ok := object["batch_id"]; ok {
			_ = json.Unmarshal(value, &envelope.BatchID)
		}
		if value, ok := object["base_version"]; ok && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			var base int64
			if err := json.Unmarshal(value, &base); err != nil {
				return observationEnvelope{}, apperr.New(apperr.CodeBadRequest, "base_version must be an integer or null", nil)
			}
			envelope.BaseVersion = &base
		}
		return envelope, nil
	}
	if err := json.Unmarshal(body, &envelope.Observation); err != nil {
		return observationEnvelope{}, observationDecodeError(body, err)
	}
	return envelope, nil
}

func observationDecodeError(raw []byte, err error) *apperr.Error {
	context := map[string]any{"detail": err.Error()}
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		context["field"] = typeErr.Field
	}
	var probe struct {
		ReplicaID   string `json:"replica_id"`
		SampledAtMS int64  `json:"sampled_at_ms"`
	}
	_ = json.Unmarshal(raw, &probe)
	context["replica_id"] = probe.ReplicaID
	context["sampled_at_ms"] = probe.SampledAtMS
	return apperr.New(apperr.CodeObservationInvalid, "observation JSON is invalid", context)
}

func (s *Server) deleteObservation(w http.ResponseWriter, r *http.Request) {
	sampledAt, err := strconv.ParseInt(r.PathValue("sampledAt"), 10, 64)
	if err != nil {
		writeError(w, apperr.New(apperr.CodeBadRequest, "sample time path value must be an integer", map[string]any{"value": r.PathValue("sampledAt")}))
		return
	}
	var baseVersion *int64
	if raw := r.URL.Query().Get("base_version"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			writeError(w, apperr.New(apperr.CodeBadRequest, "base_version query parameter must be an integer", map[string]any{"value": raw}))
			return
		}
		baseVersion = &value
	}
	result, submitErr := s.engine.DeleteObservation(r.Context(), engine.DeleteRequest{
		BatchID: r.URL.Query().Get("batch_id"), BaseVersion: baseVersion,
		ReplicaID: r.PathValue("replicaID"), SampledAtMS: sampledAt,
	})
	s.writeWriteResult(w, result, submitErr)
}

type importEnvelope struct {
	BatchID     string `json:"batch_id"`
	BaseVersion *int64 `json:"base_version"`
	Format      string `json:"format"`
	Data        string `json:"data"`
	DataBase64  string `json:"data_base64"`
}

func (s *Server) importObservations(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		writeError(w, err)
		return
	}
	batchID := r.URL.Query().Get("batch_id")
	formatName := r.URL.Query().Get("format")
	var baseVersion *int64
	if raw := r.URL.Query().Get("base_version"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			writeError(w, apperr.New(apperr.CodeBadRequest, "base_version query parameter must be an integer", map[string]any{"value": raw}))
			return
		}
		baseVersion = &value
	}
	data := body
	if r.URL.Query().Get("envelope") == "true" || looksLikeImportEnvelope(body) {
		var envelope importEnvelope
		if err := json.Unmarshal(body, &envelope); err != nil {
			writeError(w, apperr.New(apperr.CodeFormatCorrupt, "import envelope is malformed JSON", map[string]any{"line": 1, "detail": err.Error()}))
			return
		}
		if batchID == "" {
			batchID = envelope.BatchID
		}
		if formatName == "" {
			formatName = envelope.Format
		}
		if baseVersion == nil {
			baseVersion = envelope.BaseVersion
		}
		if envelope.DataBase64 != "" {
			decoded, decodeErr := base64.StdEncoding.DecodeString(envelope.DataBase64)
			if decodeErr != nil {
				writeError(w, apperr.New(apperr.CodeFormatCorrupt, "data_base64 is invalid", map[string]any{"detail": decodeErr.Error()}))
				return
			}
			data = decoded
		} else {
			data = []byte(envelope.Data)
		}
	}
	result, submitErr := s.engine.Import(r.Context(), engine.ImportRequest{
		BatchID: batchID, BaseVersion: baseVersion, Format: formatName, Data: data,
	})
	s.writeWriteResult(w, result, submitErr)
}

func looksLikeImportEnvelope(body []byte) bool {
	var probe map[string]json.RawMessage
	if json.Unmarshal(body, &probe) != nil {
		return false
	}
	_, hasData := probe["data"]
	_, hasBase64 := probe["data_base64"]
	return hasData || hasBase64
}

func (s *Server) merge(w http.ResponseWriter, r *http.Request) {
	var request struct {
		BatchID     string `json:"batch_id"`
		BaseVersion *int64 `json:"base_version"`
	}
	body, err := readBody(r)
	if err != nil {
		writeError(w, err)
		return
	}
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &request); err != nil {
			writeError(w, apperr.New(apperr.CodeBadRequest, "merge request must be a JSON object", map[string]any{"detail": err.Error()}))
			return
		}
	}
	if request.BatchID == "" {
		request.BatchID = r.URL.Query().Get("batch_id")
	}
	if request.BaseVersion == nil {
		if raw := r.URL.Query().Get("base_version"); raw != "" {
			value, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				writeError(w, apperr.New(apperr.CodeBadRequest, "base_version must be an integer", map[string]any{"value": raw}))
				return
			}
			request.BaseVersion = &value
		}
	}
	result, submitErr := s.engine.Merge(r.Context(), engine.MergeRequest{BatchID: request.BatchID, BaseVersion: request.BaseVersion})
	s.writeWriteResult(w, result, submitErr)
}

func (s *Server) listSequences(w http.ResponseWriter, r *http.Request) {
	sequences, version, err := s.engine.ListSequences(r.Context())
	if err != nil {
		writeError(w, apperr.As(err))
		return
	}
	setVersion(w, version)
	writeJSON(w, http.StatusOK, map[string]any{"version": version, "sequences": sequences, "count": len(sequences)})
}

func (s *Server) getSequence(w http.ResponseWriter, r *http.Request) {
	sequence, version, err := s.engine.GetSequence(r.Context(), r.PathValue("replicaID"))
	if err != nil {
		writeError(w, apperr.As(err))
		return
	}
	setVersion(w, version)
	writeJSON(w, http.StatusOK, map[string]any{"version": version, "sequence": sequence})
}

func (s *Server) getState(w http.ResponseWriter, r *http.Request) {
	state, version, err := s.engine.GetState(r.Context(), r.PathValue("replicaID"))
	if err != nil {
		writeError(w, apperr.As(err))
		return
	}
	setVersion(w, version)
	writeJSON(w, http.StatusOK, map[string]any{"version": version, "replica": state})
}

func (s *Server) export(w http.ResponseWriter, r *http.Request) {
	formatName := r.URL.Query().Get("format")
	if formatName == "" {
		formatName = "jsonl"
	}
	data, version, resolvedFormat, err := s.engine.Export(r.Context(), formatName)
	if err != nil {
		writeError(w, apperr.As(err))
		return
	}
	setVersion(w, version)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"observations.%s\"", resolvedFormat))
	if resolvedFormat == "binary" {
		w.Header().Set("Content-Type", "application/octet-stream")
	} else {
		w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *Server) recoveryReport(w http.ResponseWriter, r *http.Request) {
	report, version, err := s.engine.RecoveryReport(r.Context())
	if err != nil {
		writeError(w, apperr.As(err))
		return
	}
	setVersion(w, version)
	writeJSON(w, http.StatusOK, map[string]any{"version": version, "recovery": report})
}

func (s *Server) getPolicy(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"policy": s.engine.Policy()})
}

func (s *Server) putPolicy(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		writeError(w, err)
		return
	}
	var policy domain.Policy
	if err := json.Unmarshal(body, &policy); err != nil {
		writeError(w, apperr.New(apperr.CodeBadRequest, "policy must be a JSON object", map[string]any{"detail": err.Error()}))
		return
	}
	if err := s.engine.SetPolicy(policy); err != nil {
		writeError(w, apperr.As(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"policy": policy, "note": "run merge to persist derived state with this policy"})
}

func (s *Server) validateState(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		writeError(w, err)
		return
	}
	var request struct {
		From domain.ReplicaState `json:"from"`
		To   domain.ReplicaState `json:"to"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		writeError(w, apperr.New(apperr.CodeBadRequest, "state validation request must contain from and to", map[string]any{"detail": err.Error()}))
		return
	}
	if err := domain.ValidateTransition(request.From, request.To); err != nil {
		writeError(w, apperr.As(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"valid": true, "from": request.From, "to": request.To})
}

func (s *Server) notFound(w http.ResponseWriter, r *http.Request) {
	writeError(w, apperr.New(apperr.CodeNotFound, "route not found", map[string]any{
		"method": r.Method, "path": r.URL.Path,
	}))
}

func (s *Server) writeWriteResult(w http.ResponseWriter, result store.Result, err error) {
	if err != nil {
		writeError(w, apperr.As(err))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Engine-Version", strconv.FormatInt(result.Version, 10))
	if result.Cached {
		w.Header().Set("X-Idempotent-Replay", "true")
	}
	status := result.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(result.Body)
}

func readBody(r *http.Request) ([]byte, *apperr.Error) {
	defer r.Body.Close()
	data, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil {
		return nil, apperr.New(apperr.CodeBadRequest, "cannot read request body", map[string]any{"detail": err.Error()})
	}
	if len(data) > maxRequestBody {
		return nil, apperr.New(apperr.CodeBadRequest, "request body is too large", map[string]any{"limit_bytes": maxRequestBody})
	}
	return data, nil
}

func setVersion(w http.ResponseWriter, version int64) {
	w.Header().Set("X-Engine-Version", strconv.FormatInt(version, 10))
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		writeError(w, apperr.Wrap(apperr.CodeInternal, "cannot encode response", nil, err))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}

func writeError(w http.ResponseWriter, appErr *apperr.Error) {
	if appErr == nil {
		appErr = apperr.New(apperr.CodeInternal, "internal error", nil)
	}
	body, _ := json.Marshal(appErr.Public())
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(apperr.HTTPStatus(appErr.Code))
	_, _ = w.Write(append(body, '\n'))
}
