package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"netpolcompiler/internal/compiler"
	"netpolcompiler/internal/fixedpoint"
	"netpolcompiler/internal/model"
	"netpolcompiler/internal/sample"
	"netpolcompiler/internal/service"
)

type Server struct {
	service *service.Service
}

func NewHandler(svc *service.Service) http.Handler {
	server := &Server{service: svc}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.health)
	mux.HandleFunc("GET /v1/sample/snapshot", server.sampleSnapshot)
	mux.HandleFunc("GET /v1/sample/increment", server.sampleIncrement)
	mux.HandleFunc("POST /v1/snapshots", server.submitSnapshot)
	mux.HandleFunc("POST /v1/increments", server.submitIncrement)
	mux.HandleFunc("GET /v1/views/{namespace}", server.getView)
	mux.HandleFunc("POST /v1/adjudications", server.adjudicate)
	mux.HandleFunc("GET /v1/consistency", server.consistency)
	mux.HandleFunc("GET /v1/recovery", server.recoveryReport)
	mux.HandleFunc("POST /v1/admin/recover", server.recoverNow)
	mux.HandleFunc("GET /v1/requests/{requestId}", server.getRequest)
	return mux
}

func (s *Server) health(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"ok": true, "service": "netpol-compiler", "time": request.Header.Get("Date")})
}

func (s *Server) sampleSnapshot(writer http.ResponseWriter, request *http.Request) {
	seed := sample.DefaultSeed
	if raw := request.URL.Query().Get("seed"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			writeResult(writer, service.FailureResult(queryError("seed", "must be an int64 seed")))
			return
		}
		seed = parsed
	}
	snapshot := sample.GenerateSnapshot(seed)
	if rawVersion := request.URL.Query().Get("version"); rawVersion != "" {
		version, err := strconv.ParseInt(rawVersion, 10, 64)
		if err != nil || version <= 0 {
			writeResult(writer, service.FailureResult(queryError("version", "must be a positive int64")))
			return
		}
		snapshot.Version = version
		snapshot.Digest = model.DigestSnapshot(snapshot)
	}
	writeJSON(writer, http.StatusOK, service.Envelope{OK: true, Data: snapshot})
}

func (s *Server) sampleIncrement(writer http.ResponseWriter, request *http.Request) {
	baseVersion := sample.SampleVersion
	if raw := request.URL.Query().Get("baseVersion"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			writeResult(writer, service.FailureResult(queryError("baseVersion", "must be a non-negative int64")))
			return
		}
		baseVersion = parsed
	}
	increment := sample.GenerateIncrement(baseVersion, request.URL.Query().Get("requestId"))
	writeJSON(writer, http.StatusOK, service.Envelope{OK: true, Data: increment})
}

func (s *Server) submitSnapshot(writer http.ResponseWriter, request *http.Request) {
	var snapshot model.Snapshot
	if !decodeJSON(writer, request, &snapshot) {
		return
	}
	if snapshot.RequestID == "" {
		snapshot.RequestID = request.Header.Get("X-Request-ID")
	}
	writeResult(writer, s.service.SubmitSnapshot(request.Context(), snapshot))
}

func (s *Server) submitIncrement(writer http.ResponseWriter, request *http.Request) {
	var increment model.Increment
	if !decodeJSON(writer, request, &increment) {
		return
	}
	if increment.RequestID == "" {
		increment.RequestID = request.Header.Get("X-Request-ID")
	}
	writeResult(writer, s.service.SubmitIncrement(request.Context(), increment))
}

func (s *Server) getView(writer http.ResponseWriter, request *http.Request) {
	observed, ok := parseObservationVersion(writer, request)
	if !ok {
		return
	}
	view, bizError := s.service.GetView(request.PathValue("namespace"), observed)
	if bizError != nil {
		writeResult(writer, service.FailureResult(bizError))
		return
	}
	writeJSON(writer, http.StatusOK, service.Envelope{OK: true, Data: view})
}

func (s *Server) adjudicate(writer http.ResponseWriter, request *http.Request) {
	var evaluation compiler.EvaluationRequest
	if !decodeJSON(writer, request, &evaluation) {
		return
	}
	if raw := request.URL.Query().Get("observationVersion"); raw != "" {
		observed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || observed < 0 {
			writeResult(writer, service.FailureResult(queryError("observationVersion", "must be a non-negative int64")))
			return
		}
		evaluation.ObservationVersion = observed
	}
	writeResult(writer, s.service.Adjudicate(evaluation))
}

func (s *Server) consistency(writer http.ResponseWriter, request *http.Request) {
	query := service.ConsistencyQuery{}
	observed, ok := parseObservationVersion(writer, request)
	if !ok {
		return
	}
	query.ObservationVersion = observed
	values := request.URL.Query()
	if raw := values.Get("thresholdQ"); raw != "" {
		threshold, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || threshold < 0 {
			writeResult(writer, service.FailureResult(queryError("thresholdQ", "must be a non-negative Q6 integer")))
			return
		}
		query.ThresholdQ = &threshold
	} else if raw := values.Get("threshold"); raw != "" {
		threshold, err := fixedpoint.ParseDecimalToQ(raw)
		if err != nil || threshold < 0 {
			writeResult(writer, service.FailureResult(queryError("threshold", "must be a non-negative decimal fixed-point value")))
			return
		}
		query.ThresholdQ = &threshold
	}
	if raw := values.Get("relativeTolerancePpm"); raw != "" {
		tolerance, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || tolerance < 0 || tolerance > fixedpoint.PPM {
			writeResult(writer, service.FailureResult(queryError("relativeTolerancePpm", "must be within [0,1000000]")))
			return
		}
		query.TolerancePpm = &tolerance
	}
	report, bizError := s.service.GetConsistency(query)
	if bizError != nil {
		writeResult(writer, service.FailureResult(bizError))
		return
	}
	writeJSON(writer, http.StatusOK, service.Envelope{OK: true, Data: report})
}

func (s *Server) recoveryReport(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, service.Envelope{OK: true, Data: s.service.LastRecovery()})
}

func (s *Server) recoverNow(writer http.ResponseWriter, request *http.Request) {
	report, err := s.service.Recover(request.Context())
	if err != nil {
		writeResult(writer, service.InternalError("recovery failed", err))
		return
	}
	writeJSON(writer, http.StatusOK, service.Envelope{OK: true, Data: report})
}

func (s *Server) getRequest(writer http.ResponseWriter, request *http.Request) {
	record, err := s.service.Store().GetRequest(request.Context(), request.PathValue("requestId"))
	if err != nil {
		writeResult(writer, service.InternalError("failed to load request", err))
		return
	}
	if record == nil {
		biz := service.NewBizError("E_REQUEST_NOT_FOUND", http.StatusNotFound, "request id was not found")
		writeResult(writer, service.FailureResult(biz))
		return
	}
	writeJSON(writer, http.StatusOK, service.Envelope{OK: true, Data: record})
}

func parseObservationVersion(writer http.ResponseWriter, request *http.Request) (int64, bool) {
	raw := request.URL.Query().Get("observationVersion")
	if raw == "" {
		return 0, true
	}
	observed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || observed < 0 {
		writeResult(writer, service.FailureResult(queryError("observationVersion", "must be a non-negative int64")))
		return 0, false
	}
	return observed, true
}

func queryError(field, message string) *service.BizError {
	biz := service.NewBizError("E_INVALID_QUERY", http.StatusBadRequest, "invalid query parameter")
	biz.Field = field
	biz.Context["reason"] = message
	return biz
}

func decodeJSON(writer http.ResponseWriter, request *http.Request, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 16<<20))
	if err := decoder.Decode(target); err != nil {
		biz := service.NewBizError("E_INVALID_JSON", http.StatusBadRequest, "request body is not valid expected JSON")
		biz.Context["cause"] = err.Error()
		writeResult(writer, service.FailureResult(biz))
		return false
	}
	return true
}

func writeResult(writer http.ResponseWriter, result service.RawResult) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(result.Status)
	_, _ = writer.Write(result.Body)
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		body, _ = json.Marshal(service.Envelope{OK: false, Error: service.NewBizError("E_INTERNAL", 500, err.Error())})
		status = 500
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_, _ = writer.Write(body)
}
