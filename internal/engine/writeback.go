package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"driftconverge/internal/apperr"
	"driftconverge/internal/detect"
	"driftconverge/internal/model"
	"driftconverge/internal/plan"
	"driftconverge/internal/simulator"
	"driftconverge/internal/store"
)

type httpAttemptResult struct {
	state      string
	errorCode  string
	errorText  string
	writeResp  *simulator.WriteResponse
	readback   *simulator.ObjectState
	categories []detect.Category
	fields     []string
}

func (s *Session) executeBatch(ctx context.Context, roundID, endpoint string, bp plan.BatchPlan) BatchExecutionResult {
	start := time.Now()
	result := BatchExecutionResult{
		BatchID: bp.BatchID, Index: bp.Index, ObjectIDs: append([]string(nil), bp.ObjectIDs...),
		MaxSeverityRank: bp.MaxSeverityRank, StartedAt: start.UTC().Format(time.RFC3339Nano),
		Objects: []ObjectWriteResult{},
	}
	for _, item := range bp.Objects {
		if ctx.Err() != nil {
			result.Objects = append(result.Objects, ObjectWriteResult{
				ObjectID: item.ObjectID, State: "failed", ErrorCode: string(apperr.RoundBudgetExhausted),
				ErrorMessage: "round budget expired before object writeback",
			})
			continue
		}
		result.Objects = append(result.Objects, s.executeObject(ctx, roundID, bp.BatchID, item))
	}
	result.DurationMicros = time.Since(start).Microseconds()
	result.State = "converged"
	codeRank := map[apperr.Code]int{apperr.EndpointRefused: 5, apperr.WriteTimeout: 4, apperr.ReadbackFailed: 3, apperr.PersistenceFailed: 2, apperr.WriteFailed: 1}
	best := 0
	for _, object := range result.Objects {
		if object.State == "unresolved" {
			result.State = "partial"
		}
		if object.State == "failed" {
			result.State = "failed"
		}
		if object.ErrorCode != "" {
			code := apperr.Code(object.ErrorCode)
			if codeRank[code] > best {
				best = codeRank[code]
				result.ErrorCode = object.ErrorCode
			}
		}
	}
	return result
}

func (s *Session) executeObject(ctx context.Context, roundID, batchID string, item plan.WriteItem) ObjectWriteResult {
	start := time.Now()
	policy := s.Samples.Policy
	obj := s.objectByID(item.ObjectID)
	result := ObjectWriteResult{ObjectID: item.ObjectID, State: "failed"}
	if obj == nil {
		result.ErrorCode = string(apperr.SpecInvalid)
		result.ErrorMessage = "object disappeared from analyzed snapshot"
		result.DurationMicros = time.Since(start).Microseconds()
		return result
	}
	request := simulator.WriteRequest{
		RoundID: roundID, BatchID: batchID, ObjectID: item.ObjectID,
		DesiredFields: cloneFieldValues(item.DesiredFields), ExpectedSources: cloneFieldSources(item.ExpectedSources),
		RemoveFields: append([]string(nil), item.RemoveFields...),
	}
	var last httpAttemptResult
	maxAttempts := policy.RetryLimit + 1
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		result.Attempts = attempt
		intentID, err := s.Store.BeginWriteAttempt(ctx, roundID, batchID, item.ObjectID, attempt, request)
		if err != nil {
			result.State = "failed"
			result.ErrorCode = string(apperr.CodeOf(err))
			result.ErrorMessage = err.Error()
			break
		}
		result.WriteRecordIDs = append(result.WriteRecordIDs, intentID)
		last = s.performWriteAttempt(ctx, request, item, obj)
		outcome := store.WriteOutcome{
			State: last.state, ErrorCode: last.errorCode, Attempt: attempt, Detail: last.errorText,
			Request: request, Readback: last.readback,
		}
		if last.readback != nil {
			outcome.Actual = last.readback.Actual
			outcome.Sources = last.readback.Sources
			s.setObjectActual(item.ObjectID, last.readback.Actual, last.readback.Sources)
		}
		completeErr := s.Store.CompleteWriteAttempt(context.Background(), intentID, outcome)
		if completeErr != nil {
			result.State = "failed"
			result.ErrorCode = string(apperr.CodeOf(completeErr))
			result.ErrorMessage = completeErr.Error()
			break
		}
		switch last.state {
		case "converged":
			result.State = "converged"
			result.ErrorCode = ""
			result.ErrorMessage = ""
			result.ResidualCategories = nil
			result.ResidualFields = nil
			result.DurationMicros = time.Since(start).Microseconds()
			return result
		case "unresolved":
			result.State = "unresolved"
			result.ErrorCode = ""
			result.ErrorMessage = "write request succeeded but readback still reports drift"
			result.ResidualCategories = append([]detect.Category(nil), last.categories...)
			result.ResidualFields = append([]string(nil), last.fields...)
		default:
			result.State = "failed"
			result.ErrorCode = last.errorCode
			result.ErrorMessage = last.errorText
		}
	}
	result.DurationMicros = time.Since(start).Microseconds()
	return result
}

func (s *Session) performWriteAttempt(ctx context.Context, request simulator.WriteRequest, item plan.WriteItem, obj *model.Object) httpAttemptResult {
	writeResp, err := s.postWrite(ctx, request)
	if err != nil {
		code, text := classifyHTTPError(err)
		return httpAttemptResult{state: "failed", errorCode: string(code), errorText: text}
	}
	readCtx, cancel := context.WithTimeout(ctx, time.Duration(s.Samples.Policy.WriteTimeoutMs)*time.Millisecond)
	defer cancel()
	readback, err := s.readObject(readCtx, request.ObjectID)
	if err != nil {
		code, text := classifyHTTPError(err)
		if code == apperr.WriteFailed {
			code = apperr.ReadbackFailed
		}
		return httpAttemptResult{state: "failed", errorCode: string(code), errorText: "readback failed: " + text, writeResp: writeResp}
	}
	evaluationObject := *obj
	evaluationObject.Actual = cloneFieldValues(readback.Actual)
	evaluationObject.Sources = cloneFieldSources(readback.Sources)
	residual := detect.EvaluateObject(&evaluationObject, s.Samples.Policy)
	if residual.HasDrift {
		fields := make([]string, 0, len(residual.FieldDrifts))
		for _, drift := range residual.FieldDrifts {
			fields = append(fields, drift.Field)
		}
		return httpAttemptResult{
			state: "unresolved", writeResp: writeResp, readback: readback,
			categories: append([]detect.Category(nil), residual.Categories...), fields: sortedUnique(fields),
		}
	}
	return httpAttemptResult{state: "converged", writeResp: writeResp, readback: readback}
}

func (s *Session) postWrite(ctx context.Context, request simulator.WriteRequest) (*simulator.WriteResponse, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode write request: %w", err)
	}
	body, status, err := s.doJSON(ctx, http.MethodPost, strings.TrimRight(s.endpointForCall(ctx), "/")+"/v1/write", payload)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("write endpoint returned HTTP %d: %s", status, truncateBody(body))
	}
	var response simulator.WriteResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("decode write response: %w", err)
	}
	if !response.OK {
		return nil, fmt.Errorf("write endpoint rejected request")
	}
	return &response, nil
}

func (s *Session) readObject(ctx context.Context, objectID string) (*simulator.ObjectState, error) {
	endpoint := strings.TrimRight(s.endpointForCall(ctx), "/") + "/v1/read?objectId=" + url.QueryEscape(objectID)
	body, status, err := s.doJSON(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("readback endpoint returned HTTP %d: %s", status, truncateBody(body))
	}
	var state simulator.ObjectState
	if err := json.Unmarshal(body, &state); err != nil {
		return nil, fmt.Errorf("decode readback response: %w", err)
	}
	if state.ID != objectID {
		return nil, fmt.Errorf("readback identity mismatch: requested %q received %q", objectID, state.ID)
	}
	return &state, nil
}

// endpointForCall is populated from the active execution report. Keeping the
// endpoint on the per-call request path avoids sharing one endpoint across
// concurrent rounds with different targets.
func (s *Session) endpointForCall(ctx context.Context) string {
	if endpoint, ok := ctx.Value(endpointContextKey{}).(string); ok {
		return endpoint
	}
	return ""
}

func (s *Session) doJSON(ctx context.Context, method, endpoint string, payload []byte) ([]byte, int, error) {
	timeout := time.Duration(s.Samples.Policy.WriteTimeoutMs) * time.Millisecond
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	transport := &http.Transport{
		Proxy:                 nil,
		DisableKeepAlives:     true,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          0,
		ResponseHeaderTimeout: timeout,
		DialContext:           (&net.Dialer{Timeout: timeout, KeepAlive: -1}).DialContext,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return fmt.Errorf("redirects are not permitted")
		},
	}
	defer transport.CloseIdleConnections()
	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(requestCtx, method, endpoint, reader)
	if err != nil {
		return nil, 0, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

func classifyHTTPError(err error) (apperr.Code, string) {
	if err == nil {
		return "", ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return apperr.WriteTimeout, "write/readback deadline exceeded and underlying connection was closed"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return apperr.WriteTimeout, "network timeout: " + err.Error()
	}
	text := strings.ToLower(err.Error())
	if strings.Contains(text, "connection refused") || strings.Contains(text, "actively refused") || strings.Contains(text, "no connection could be made") || (strings.Contains(text, "wsa") && strings.Contains(text, "refused")) {
		return apperr.EndpointRefused, "local test endpoint refused the connection: " + err.Error()
	}
	return apperr.WriteFailed, err.Error()
}

func truncateBody(body []byte) string {
	text := strings.TrimSpace(string(body))
	if len(text) > 512 {
		return text[:512]
	}
	return text
}

func validateLoopbackEndpoint(endpoint string) (string, error) {
	if strings.TrimSpace(endpoint) == "" {
		return "", apperr.New(apperr.InvalidArgument, "write endpoint is required")
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || u.Host == "" {
		return "", apperr.New(apperr.EndpointInvalid, "endpoint must be an absolute http loopback URL")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", apperr.New(apperr.EndpointInvalid, "endpoint must not include credentials, query, or fragment")
	}
	host := u.Hostname()
	if !strings.EqualFold(host, "localhost") {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return "", apperr.New(apperr.EndpointInvalid, "endpoint host %q is not loopback", host)
		}
	}
	u.Path = strings.TrimRight(u.Path, "/")
	return u.String(), nil
}

type endpointContextKey struct{}

var _ = validateLoopbackEndpoint
