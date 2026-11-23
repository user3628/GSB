package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"local/objectstore/internal/engine"
	"local/objectstore/internal/model"
)

const maxBodyBytes = 128 << 20

type Config struct {
	Logger *log.Logger
	Now    func() time.Time
}

type Server struct {
	engine  *engine.Engine
	ledger  *RequestCoordinator
	logger  *log.Logger
	now     func() time.Time
	handler http.Handler
}

func New(service *engine.Engine, config Config) *Server {
	if config.Logger == nil {
		config.Logger = log.Default()
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	server := &Server{
		engine: service,
		ledger: NewRequestCoordinator(service.Store(), config.Now),
		logger: config.Logger,
		now:    config.Now,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/uploads", server.handleCreateSession)
	mux.HandleFunc("GET /v1/uploads/{session_id}", server.handleGetSession)
	mux.HandleFunc("GET /v1/uploads/{session_id}/parts", server.handleListParts)
	mux.HandleFunc("PUT /v1/uploads/{session_id}/parts/{part_number}", server.handleSubmitPart)
	mux.HandleFunc("POST /v1/uploads/{session_id}/merge", server.handleMerge)
	mux.HandleFunc("POST /v1/uploads/{session_id}/cancel", server.handleCancel)
	mux.HandleFunc("GET /v1/objects", server.handleGetObject)
	mux.HandleFunc("GET /v1/objects/{object_key...}", server.handleGetObjectPath)
	mux.HandleFunc("POST /v1/admin/recover", server.handleRecover)
	mux.HandleFunc("GET /v1/admin/recovery-report", server.handleRecoveryReport)
	mux.HandleFunc("POST /v1/admin/reclaim-expired", server.handleReclaimExpired)
	mux.HandleFunc("POST /v1/recover", server.handleRecover)
	mux.HandleFunc("GET /v1/recovery-report", server.handleRecoveryReport)
	mux.HandleFunc("GET /healthz", server.handleHealth)
	mux.HandleFunc("/", server.handleNotFound)
	server.handler = server.middleware(mux)
	return server
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.logger.Printf("panic serving %s %s: %v", r.Method, r.URL.Path, recovered)
				writeError(w, "", model.NewError(model.CodeInternal, "internal server error"))
			}
		}()
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		s.logger.Printf("%s %s -> %d (%s)", r.Method, r.URL.Path, recorder.status, time.Since(started).Round(time.Millisecond))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

type apiError struct {
	Code      model.Code     `json:"code"`
	Message   string         `json:"message"`
	Context   map[string]any `json:"context"`
	RequestID string         `json:"request_id,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		http.Error(w, `{"code":"E_INTERNAL","message":"encode response","context":{}}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(body)
	_, _ = w.Write([]byte("\n"))
}

func renderError(requestID string, business *model.Error) Outcome {
	if business == nil {
		business = model.NewError(model.CodeInternal, "internal server error")
	}
	context := business.Context
	if context == nil {
		context = map[string]any{}
	}
	body, _ := json.Marshal(apiError{
		Code: business.Code, Message: business.Message, Context: context, RequestID: requestID,
	})
	return Outcome{Status: model.HTTPStatus(business), Body: append(body, '\n'), Code: string(business.Code)}
}

func writeOutcome(w http.ResponseWriter, outcome Outcome, replayed bool) {
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(outcome.Status)
	_, _ = w.Write(outcome.Body)
}

func writeError(w http.ResponseWriter, requestID string, business *model.Error) {
	writeOutcome(w, renderError(requestID, business), false)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) *model.Error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(destination); err != nil {
		if errors.Is(err, io.EOF) {
			return model.NewError(model.CodeInvalidArgument, "request body is required")
		}
		return model.WrapError(model.CodeInvalidArgument, "invalid JSON request body", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return model.NewError(model.CodeInvalidArgument, "request body must contain exactly one JSON value")
	}
	return nil
}

func decodeOptionalJSON(w http.ResponseWriter, r *http.Request, destination any) *model.Error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(destination); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return model.WrapError(model.CodeInvalidArgument, "invalid JSON request body", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return model.NewError(model.CodeInvalidArgument, "request body must contain exactly one JSON value")
	}
	return nil
}

func resolveRequestID(headerValue, bodyValue string) (string, *model.Error) {
	headerValue = strings.TrimSpace(headerValue)
	bodyValue = strings.TrimSpace(bodyValue)
	if headerValue != "" && bodyValue != "" && headerValue != bodyValue {
		return "", model.NewError(model.CodeConflictingRequest, "Idempotency-Key header and request_id body field differ")
	}
	if headerValue != "" {
		return headerValue, nil
	}
	if bodyValue != "" {
		return bodyValue, nil
	}
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", model.WrapError(model.CodeInternal, "generate request id", err)
	}
	return "req_" + hex.EncodeToString(buffer), nil
}

func resolveRequestIDFromHeaders(r *http.Request, bodyValue string) (string, *model.Error) {
	headerValue := ""
	for _, name := range []string{"Idempotency-Key", "X-Request-ID", "X-Idempotency-Key"} {
		value := strings.TrimSpace(r.Header.Get(name))
		if value == "" {
			continue
		}
		if headerValue != "" && headerValue != value {
			return "", model.NewError(model.CodeConflictingRequest, "request identity headers differ").
				WithContext("header", name)
		}
		headerValue = value
	}
	return resolveRequestID(headerValue, bodyValue)
}

func encodeOutcome(status int, requestID string, value any) Outcome {
	body, err := json.Marshal(value)
	if err != nil {
		return renderError(requestID, model.WrapError(model.CodeInternal, "encode response", err))
	}
	return Outcome{Status: status, Body: append(body, '\n')}
}

type createSessionRequest struct {
	RequestID  string `json:"request_id"`
	ObjectKey  string `json:"object_key"`
	LowerBound *int64 `json:"lower_bound"`
	TTLMS      int64  `json:"ttl_ms"`
}

type createSessionResponse struct {
	RequestID string `json:"request_id"`
	engine.CreateSessionOutput
}

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	var request createSessionRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, "", err)
		return
	}
	requestID, err := resolveRequestIDFromHeaders(r, request.RequestID)
	if err != nil {
		writeError(w, "", err)
		return
	}
	request.RequestID = requestID
	lowerBound := int64(1)
	if request.LowerBound != nil {
		lowerBound = *request.LowerBound
	}
	fingerprint := RequestFingerprint("create_session", struct {
		ObjectKey  string `json:"object_key"`
		LowerBound int64  `json:"lower_bound"`
		TTLMS      int64  `json:"ttl_ms"`
	}{request.ObjectKey, lowerBound, request.TTLMS})
	outcome, replayed, business := s.ledger.Run(r.Context(), requestID, "create_session", fingerprint, func() Outcome {
		result, callErr := s.engine.CreateSession(r.Context(), engine.CreateSessionInput{
			RequestID: requestID, ObjectKey: request.ObjectKey, LowerBound: lowerBound, TTLMS: request.TTLMS,
		})
		if callErr != nil {
			return renderError(requestID, callErr)
		}
		status := http.StatusCreated
		if result.Reused {
			status = http.StatusOK
		}
		return encodeOutcome(status, requestID, createSessionResponse{RequestID: requestID, CreateSessionOutput: result})
	})
	if business != nil {
		writeError(w, requestID, business)
		return
	}
	writeOutcome(w, outcome, replayed)
}

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	session, err := s.engine.GetSession(r.Context(), r.PathValue("session_id"))
	if err != nil {
		writeError(w, "", err)
		return
	}
	writeJSON(w, http.StatusOK, session)
}

func (s *Server) handleListParts(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("session_id")
	parts, err := s.engine.ListParts(r.Context(), sessionID)
	if err != nil {
		writeError(w, "", err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		SessionID string       `json:"session_id"`
		Parts     []model.Part `json:"parts"`
	}{SessionID: sessionID, Parts: parts})
}

type submitPartRequest struct {
	RequestID     string `json:"request_id"`
	Size          *int64 `json:"size"`
	SHA256        string `json:"sha256"`
	ArrivedAtMS   *int64 `json:"arrived_at_ms"`
	ContentBase64 string `json:"content_base64"`
}

type submitPartResponse struct {
	RequestID string `json:"request_id"`
	engine.SubmitPartOutput
}

func (s *Server) handleSubmitPart(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("session_id")
	partNumber, parseErr := strconv.ParseInt(r.PathValue("part_number"), 10, 64)
	if parseErr != nil || partNumber < 0 {
		writeError(w, "", model.NewError(model.CodeInvalidArgument, "part_number must be a non-negative integer").
			WithContext("part_number", r.PathValue("part_number")))
		return
	}
	var request submitPartRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, "", err)
		return
	}
	requestID, err := resolveRequestIDFromHeaders(r, request.RequestID)
	if err != nil {
		writeError(w, "", err)
		return
	}
	content, decodeErr := base64.StdEncoding.DecodeString(request.ContentBase64)
	if decodeErr != nil {
		writeError(w, requestID, model.WrapError(model.CodeInvalidArgument, "content_base64 is not valid standard base64", decodeErr))
		return
	}
	size := int64(len(content))
	if request.Size != nil {
		size = *request.Size
	}
	fingerprint := RequestFingerprint("submit_part", struct {
		SessionID     string `json:"session_id"`
		PartNumber    int64  `json:"part_number"`
		Size          int64  `json:"size"`
		SHA256        string `json:"sha256"`
		ArrivedAtMS   *int64 `json:"arrived_at_ms,omitempty"`
		ContentBase64 string `json:"content_base64"`
	}{sessionID, partNumber, size, request.SHA256, request.ArrivedAtMS, request.ContentBase64})
	outcome, replayed, business := s.ledger.Run(r.Context(), requestID, "submit_part", fingerprint, func() Outcome {
		result, callErr := s.engine.SubmitPart(r.Context(), engine.SubmitPartInput{
			RequestID: requestID, SessionID: sessionID, PartNumber: partNumber,
			Size: size, SHA256: request.SHA256, ArrivedAtMS: request.ArrivedAtMS, Content: content,
		})
		if callErr != nil {
			return renderError(requestID, callErr)
		}
		status := http.StatusCreated
		if result.Duplicate {
			status = http.StatusOK
		}
		return encodeOutcome(status, requestID, submitPartResponse{RequestID: requestID, SubmitPartOutput: result})
	})
	if business != nil {
		writeError(w, requestID, business)
		return
	}
	writeOutcome(w, outcome, replayed)
}

type mergeRequest struct {
	RequestID         string `json:"request_id"`
	ExpectedTotalSize *int64 `json:"expected_total_size"`
	ExpectedPartCount *int64 `json:"expected_part_count"`
}

type mergeResponse struct {
	RequestID string `json:"request_id"`
	engine.MergeSessionOutput
}

func (s *Server) handleMerge(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("session_id")
	var request mergeRequest
	if err := decodeOptionalJSON(w, r, &request); err != nil {
		writeError(w, "", err)
		return
	}
	requestID, err := resolveRequestIDFromHeaders(r, request.RequestID)
	if err != nil {
		writeError(w, "", err)
		return
	}
	fingerprint := RequestFingerprint("merge", struct {
		SessionID         string `json:"session_id"`
		ExpectedTotalSize *int64 `json:"expected_total_size,omitempty"`
		ExpectedPartCount *int64 `json:"expected_part_count,omitempty"`
	}{sessionID, request.ExpectedTotalSize, request.ExpectedPartCount})
	outcome, replayed, business := s.ledger.Run(r.Context(), requestID, "merge", fingerprint, func() Outcome {
		result, callErr := s.engine.MergeSession(r.Context(), engine.MergeSessionInput{
			RequestID: requestID, SessionID: sessionID,
			ExpectedTotalSize: request.ExpectedTotalSize, ExpectedPartCount: request.ExpectedPartCount,
		})
		if callErr != nil {
			return renderError(requestID, callErr)
		}
		return encodeOutcome(http.StatusOK, requestID, mergeResponse{RequestID: requestID, MergeSessionOutput: result})
	})
	if business != nil {
		writeError(w, requestID, business)
		return
	}
	writeOutcome(w, outcome, replayed)
}

type cancelRequest struct {
	RequestID string `json:"request_id"`
	Reason    string `json:"reason"`
}

type cancelResponse struct {
	RequestID string `json:"request_id"`
	engine.CancelSessionOutput
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("session_id")
	var request cancelRequest
	if err := decodeOptionalJSON(w, r, &request); err != nil {
		writeError(w, "", err)
		return
	}
	requestID, err := resolveRequestIDFromHeaders(r, request.RequestID)
	if err != nil {
		writeError(w, "", err)
		return
	}
	fingerprint := RequestFingerprint("cancel_session", struct {
		SessionID string `json:"session_id"`
		Reason    string `json:"reason"`
	}{sessionID, request.Reason})
	outcome, replayed, business := s.ledger.Run(r.Context(), requestID, "cancel_session", fingerprint, func() Outcome {
		result, callErr := s.engine.CancelSession(r.Context(), engine.CancelSessionInput{
			RequestID: requestID, SessionID: sessionID, Reason: request.Reason,
		})
		if callErr != nil {
			return renderError(requestID, callErr)
		}
		return encodeOutcome(http.StatusOK, requestID, cancelResponse{RequestID: requestID, CancelSessionOutput: result})
	})
	if business != nil {
		writeError(w, requestID, business)
		return
	}
	writeOutcome(w, outcome, replayed)
}

func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeError(w, "", model.NewError(model.CodeInvalidArgument, "key query parameter is required"))
		return
	}
	object, err := s.engine.GetObject(r.Context(), key)
	if err != nil {
		writeError(w, "", err)
		return
	}
	writeJSON(w, http.StatusOK, object)
}

func (s *Server) handleGetObjectPath(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("object_key")
	if key == "" {
		writeError(w, "", model.NewError(model.CodeInvalidArgument, "object key is required"))
		return
	}
	object, err := s.engine.GetObject(r.Context(), key)
	if err != nil {
		writeError(w, "", err)
		return
	}
	writeJSON(w, http.StatusOK, object)
}

func (s *Server) handleRecover(w http.ResponseWriter, r *http.Request) {
	report, err := s.engine.Recover(r.Context())
	if err != nil {
		writeError(w, "", err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleRecoveryReport(w http.ResponseWriter, r *http.Request) {
	reportID := int64(0)
	if raw := r.URL.Query().Get("report_id"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 {
			writeError(w, "", model.NewError(model.CodeInvalidArgument, "report_id must be a positive integer"))
			return
		}
		reportID = parsed
	}
	report, err := s.engine.GetRecoveryReport(r.Context(), reportID)
	if err != nil {
		writeError(w, "", err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleReclaimExpired(w http.ResponseWriter, r *http.Request) {
	reclaimed, err := s.engine.ReclaimExpired(r.Context())
	if err != nil {
		writeError(w, "", err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Reclaimed []any `json:"reclaimed"`
	}{Reclaimed: interfaceSlice(reclaimed)})
}

func interfaceSlice[T any](values []T) []any {
	result := make([]any, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	return result
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.engine.Store().Ping(ctx); err != nil {
		writeError(w, "", model.WrapError(model.CodeInternal, "database is not healthy", err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	writeError(w, "", model.NewError(model.CodeInvalidArgument, fmt.Sprintf("no route for %s %s", r.Method, r.URL.Path)))
}
