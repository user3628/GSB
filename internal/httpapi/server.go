package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"local/eventbus/internal/bus"
)

const maxBodyBytes = 8 << 20

type Server struct {
	service *bus.Service
	logger  *log.Logger
}

func New(service *bus.Service) *Server {
	return &Server{service: service}
}

func NewWithLogger(service *bus.Service, logger *log.Logger) *Server {
	return &Server{service: service, logger: logger}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if s.logger != nil {
				s.logger.Printf("panic while serving %s: %v", r.URL.Path, recovered)
			}
			writeError(w, bus.NewError(bus.CodeInternal, fmt.Sprintf("internal server error: %v", recovered)))
		}
	}()

	switch r.URL.Path {
	case "/healthz":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, r)
			return
		}
		writeSuccess(w, http.StatusOK, map[string]string{"status": "ok"})
	case "/v1/topics":
		if r.Method == http.MethodPost {
			s.handleCreateTopic(w, r)
		} else if r.Method == http.MethodGet {
			s.handleGetTopic(w, r)
		} else {
			methodNotAllowed(w, r)
		}
	case "/v1/events/publish":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, r)
			return
		}
		s.handlePublish(w, r)
	case "/v1/pull":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, r)
			return
		}
		s.handlePull(w, r)
	case "/v1/ack":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, r)
			return
		}
		s.handleAck(w, r)
	case "/v1/dead-letter":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, r)
			return
		}
		s.handleDeadLetter(w, r)
	case "/v1/rewind":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, r)
			return
		}
		s.handleRewind(w, r)
	case "/v1/algorithm":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, r)
			return
		}
		s.handleSetAlgorithm(w, r)
	case "/v1/retention":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, r)
			return
		}
		s.handleSetRetention(w, r)
	case "/v1/offsets":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, r)
			return
		}
		s.handleGetOffset(w, r)
	case "/v1/events/state":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, r)
			return
		}
		s.handleGetEventState(w, r)
	case "/v1/aggregate":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, r)
			return
		}
		s.handleGetAggregate(w, r)
	case "/v1/errors":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, r)
			return
		}
		s.handleGetErrors(w, r)
	case "/v1/recovery":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, r)
			return
		}
		s.handleGetRecovery(w, r)
	default:
		writeErrorStatus(w, http.StatusNotFound, bus.NewError(bus.CodeNotFound, "route not found: "+r.URL.Path))
	}
}

func (s *Server) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var request bus.CreateTopicRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.RequestID = requestID(r, request.RequestID)
	result, err := s.service.CreateTopic(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, http.StatusCreated, result)
}

func (s *Server) handleGetTopic(w http.ResponseWriter, r *http.Request) {
	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	if topic == "" {
		writeError(w, bus.NewError(bus.CodeInvalidArgument, "topic query parameter is required"))
		return
	}
	result, err := s.service.GetTopic(r.Context(), topic)
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, http.StatusOK, result)
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	var request bus.PublishRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.RequestID = requestID(r, request.RequestID)
	result, err := s.service.Publish(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, http.StatusCreated, result)
}

func (s *Server) handlePull(w http.ResponseWriter, r *http.Request) {
	var request bus.PullRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.RequestID = requestID(r, request.RequestID)
	result, err := s.service.Pull(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, http.StatusOK, result)
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	var request bus.AckRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.RequestID = requestID(r, request.RequestID)
	result, err := s.service.Ack(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, http.StatusOK, result)
}

func (s *Server) handleDeadLetter(w http.ResponseWriter, r *http.Request) {
	var request bus.DeadLetterRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.RequestID = requestID(r, request.RequestID)
	result, err := s.service.DeadLetter(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, http.StatusOK, result)
}

func (s *Server) handleRewind(w http.ResponseWriter, r *http.Request) {
	var request bus.RewindRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.RequestID = requestID(r, request.RequestID)
	result, err := s.service.Rewind(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, http.StatusOK, result)
}

func (s *Server) handleSetAlgorithm(w http.ResponseWriter, r *http.Request) {
	var request bus.SetAlgorithmRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.RequestID = requestID(r, request.RequestID)
	result, err := s.service.SetAlgorithm(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, http.StatusOK, result)
}

func (s *Server) handleSetRetention(w http.ResponseWriter, r *http.Request) {
	var request bus.SetRetentionRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.RequestID = requestID(r, request.RequestID)
	result, err := s.service.SetRetention(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, http.StatusOK, result)
}

func (s *Server) handleGetOffset(w http.ResponseWriter, r *http.Request) {
	topic, group, partition, err := queryScope(r)
	if err != nil {
		writeError(w, err)
		return
	}
	result, err := s.service.GetOffset(r.Context(), topic, group, partition)
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, http.StatusOK, result)
}

func (s *Server) handleGetEventState(w http.ResponseWriter, r *http.Request) {
	topic, group, partition, err := queryScope(r)
	if err != nil {
		writeError(w, err)
		return
	}
	seq, err := strconv.ParseInt(r.URL.Query().Get("seq"), 10, 64)
	if err != nil || seq <= 0 {
		writeError(w, bus.NewError(bus.CodeInvalidArgument, "seq must be a positive integer"))
		return
	}
	result, err := s.service.GetEventState(r.Context(), topic, group, partition, seq)
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, http.StatusOK, result)
}

func (s *Server) handleGetAggregate(w http.ResponseWriter, r *http.Request) {
	topic, group, partition, err := queryScope(r)
	if err != nil {
		writeError(w, err)
		return
	}
	result, err := s.service.GetAggregate(r.Context(), topic, group, partition)
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, http.StatusOK, result)
}

func (s *Server) handleGetErrors(w http.ResponseWriter, r *http.Request) {
	topic, group, partition, err := queryScope(r)
	if err != nil {
		writeError(w, err)
		return
	}
	result, err := s.service.GetErrorLedger(r.Context(), topic, group, partition)
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, http.StatusOK, result)
}

func (s *Server) handleGetRecovery(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.LatestRecoveryReport(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, http.StatusOK, result)
}

func requestID(r *http.Request, bodyValue string) string {
	if bodyValue != "" {
		return bodyValue
	}
	return strings.TrimSpace(r.Header.Get("Idempotency-Key"))
}

func queryScope(r *http.Request) (string, string, int, error) {
	query := r.URL.Query()
	topic := strings.TrimSpace(query.Get("topic"))
	group := strings.TrimSpace(query.Get("group"))
	if topic == "" || group == "" {
		return "", "", 0, bus.NewError(bus.CodeInvalidArgument, "topic and group query parameters are required")
	}
	partition, err := strconv.Atoi(query.Get("partition"))
	if err != nil || partition < 0 {
		return "", "", 0, bus.NewError(bus.CodeInvalidArgument, "partition must be a non-negative integer")
	}
	return topic, group, partition, nil
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(target); err != nil {
		if errors.Is(err, io.EOF) {
			return bus.NewError(bus.CodeInvalidArgument, "request body is required")
		}
		return bus.NewError(bus.CodeInvalidArgument, "invalid JSON body: "+err.Error())
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return bus.NewError(bus.CodeInvalidArgument, "request body must contain exactly one JSON value")
	}
	return nil
}

func methodNotAllowed(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", allowedMethods(r.URL.Path))
	writeErrorStatus(w, http.StatusMethodNotAllowed,
		bus.NewError(bus.CodeInvalidArgument, "method "+r.Method+" is not allowed"))
}

func allowedMethods(path string) string {
	switch path {
	case "/v1/topics":
		return "GET, POST"
	case "/v1/offsets", "/v1/events/state", "/v1/aggregate", "/v1/errors", "/v1/recovery", "/healthz":
		return "GET"
	default:
		return "POST"
	}
}

type successResponse struct {
	OK     bool `json:"ok"`
	Result any  `json:"result"`
}

type errorResponse struct {
	OK    bool          `json:"ok"`
	Error errorEnvelope `json:"error"`
}

type errorEnvelope struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeSuccess(w http.ResponseWriter, status int, result any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(successResponse{OK: true, Result: result})
}

func writeError(w http.ResponseWriter, err error) {
	writeErrorStatus(w, statusForError(err), err)
}

func writeErrorStatus(w http.ResponseWriter, status int, err error) {
	code := bus.ErrorCode(err)
	message := err.Error()
	var busErr *bus.BusError
	if errors.As(err, &busErr) {
		message = busErr.Message
	}
	if code == "" {
		code = bus.CodeInternal
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{
		OK: false,
		Error: errorEnvelope{
			Code:    code,
			Message: message,
		},
	})
}

func statusForError(err error) int {
	switch bus.ErrorCode(err) {
	case bus.CodeInvalidArgument, bus.CodePartitionIDsInvalid:
		return http.StatusBadRequest
	case bus.CodeUnknownTopic, bus.CodeUnknownGroup, bus.CodeNotFound, bus.CodeNoEvent:
		return http.StatusNotFound
	case bus.CodeConflictingEvent, bus.CodeConflictingRequest, bus.CodeInvalidTransition,
		bus.CodeLeaseHeld, bus.CodeLeaseExpired, bus.CodeOutOfOrder,
		bus.CodeOffsetExpired, bus.CodeOffsetInvalid, bus.CodeReplayActive, bus.CodeAlgorithmLocked:
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
