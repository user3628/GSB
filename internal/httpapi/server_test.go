package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"local/objectstore/internal/engine"
	"local/objectstore/internal/model"
	"local/objectstore/internal/store"
)

type apiHarness struct {
	engine *engine.Engine
	store  *store.Store
	server *httptest.Server
	client *http.Client
}

func newAPIHarness(t *testing.T, options engine.Options) *apiHarness {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "api.db")
	database, err := store.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if options.VerifyQueueCapacity == 0 {
		options.VerifyQueueCapacity = 32
	}
	service := engine.New(database, options)
	api := New(service, Config{Logger: log.New(io.Discard, "", 0)})
	server := httptest.NewServer(api)
	t.Cleanup(func() {
		server.Close()
		service.Close()
		_ = database.Close()
	})
	return &apiHarness{engine: service, store: database, server: server, client: server.Client()}
}

func shaHex(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

type httpResult struct {
	Status int
	Body   []byte
	Header http.Header
}

func doJSON(t *testing.T, client *http.Client, method, url, requestID string, body any) httpResult {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encode body: %v", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if requestID != "" {
		request.Header.Set("Idempotency-Key", requestID)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("HTTP request: %v", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return httpResult{Status: response.StatusCode, Body: responseBody, Header: response.Header.Clone()}
}

func decodeResponse[T any](t *testing.T, result httpResult) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(result.Body, &value); err != nil {
		t.Fatalf("decode response %s: %v", result.Body, err)
	}
	return value
}

type testCreateResponse struct {
	RequestID string `json:"request_id"`
	engine.CreateSessionOutput
}

type testPartResponse struct {
	RequestID string `json:"request_id"`
	engine.SubmitPartOutput
}

type testMergeResponse struct {
	RequestID string `json:"request_id"`
	engine.MergeSessionOutput
}

func createViaHTTP(t *testing.T, harness *apiHarness, requestID, key string) testCreateResponse {
	t.Helper()
	result := doJSON(t, harness.client, http.MethodPost, harness.server.URL+"/v1/uploads", requestID, map[string]any{
		"object_key": key, "lower_bound": 1, "ttl_ms": 3_600_000,
	})
	if result.Status != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", result.Status, result.Body)
	}
	return decodeResponse[testCreateResponse](t, result)
}

func submitViaHTTP(t *testing.T, harness *apiHarness, sessionID, requestID string, partNumber int64, content string) httpResult {
	t.Helper()
	return doJSON(t, harness.client, http.MethodPut,
		harness.server.URL+"/v1/uploads/"+sessionID+"/parts/"+jsonNumber(partNumber), requestID, map[string]any{
			"size": len(content), "sha256": shaHex(content), "arrived_at_ms": partNumber * 10,
			"content_base64": base64.StdEncoding.EncodeToString([]byte(content)),
		})
}

func jsonNumber(value int64) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func TestHTTPOutOfOrderMergeAndRequestReplay(t *testing.T) {
	harness := newAPIHarness(t, engine.Options{VerifyBudget: 5 * time.Second, VerifyBudgetSet: true})
	created := createViaHTTP(t, harness, "http-create", "http/example.bin")
	sessionID := created.Session.SessionID

	partTwo := submitViaHTTP(t, harness, sessionID, "http-part-2", 2, "world")
	if partTwo.Status != http.StatusCreated {
		t.Fatalf("part 2 status=%d body=%s", partTwo.Status, partTwo.Body)
	}
	partOneBody := map[string]any{
		"size": 5, "sha256": shaHex("hello"), "arrived_at_ms": 100,
		"content_base64": base64.StdEncoding.EncodeToString([]byte("hello")),
	}
	partOne := doJSON(t, harness.client, http.MethodPut,
		harness.server.URL+"/v1/uploads/"+sessionID+"/parts/1", "http-part-1", partOneBody)
	if partOne.Status != http.StatusCreated {
		t.Fatalf("part 1 status=%d body=%s", partOne.Status, partOne.Body)
	}
	replayed := doJSON(t, harness.client, http.MethodPut,
		harness.server.URL+"/v1/uploads/"+sessionID+"/parts/1", "http-part-1", partOneBody)
	if replayed.Status != partOne.Status || !bytes.Equal(replayed.Body, partOne.Body) {
		t.Fatalf("replayed response changed: first=%s second=%s", partOne.Body, replayed.Body)
	}
	if replayed.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("missing replay header: %+v", replayed.Header)
	}

	conflicting := doJSON(t, harness.client, http.MethodPut,
		harness.server.URL+"/v1/uploads/"+sessionID+"/parts/1", "http-part-1", map[string]any{
			"size": 5, "sha256": shaHex("other"), "content_base64": base64.StdEncoding.EncodeToString([]byte("other")),
		})
	conflictError := decodeResponse[apiError](t, conflicting)
	if conflicting.Status != http.StatusConflict || conflictError.Code != model.CodeConflictingRequest {
		t.Fatalf("conflicting request: status=%d body=%s", conflicting.Status, conflicting.Body)
	}

	merged := doJSON(t, harness.client, http.MethodPost,
		harness.server.URL+"/v1/uploads/"+sessionID+"/merge", "http-merge", map[string]any{
			"expected_total_size": 10, "expected_part_count": 2,
		})
	if merged.Status != http.StatusOK {
		t.Fatalf("merge status=%d body=%s", merged.Status, merged.Body)
	}
	mergeOutput := decodeResponse[testMergeResponse](t, merged)
	expectedSHA := shaHex("helloworld")
	if mergeOutput.Object.SHA256 != expectedSHA || mergeOutput.Object.PartCount != 2 {
		t.Fatalf("merge output = %+v", mergeOutput)
	}
	if len(mergeOutput.Verification.VerifiedPartNumbers) != 2 {
		t.Fatalf("verification output = %+v", mergeOutput.Verification)
	}

	objectResult := doJSON(t, harness.client, http.MethodGet,
		harness.server.URL+"/v1/objects?key=http%2Fexample.bin", "", nil)
	if objectResult.Status != http.StatusOK {
		t.Fatalf("get object status=%d body=%s", objectResult.Status, objectResult.Body)
	}
	var object model.ObjectMetadata
	if err := json.Unmarshal(objectResult.Body, &object); err != nil {
		t.Fatalf("decode object: %v", err)
	}
	if object.SHA256 != expectedSHA || object.Generation != 1 {
		t.Fatalf("object = %+v", object)
	}

	recovered := doJSON(t, harness.client, http.MethodPost, harness.server.URL+"/v1/admin/recover", "", map[string]any{})
	if recovered.Status != http.StatusOK {
		t.Fatalf("recover status=%d body=%s", recovered.Status, recovered.Body)
	}
	var report store.RecoveryReport
	if err := json.Unmarshal(recovered.Body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if !report.Checks.Passed || len(report.Objects) != 1 || report.Objects[0].SHA256 != expectedSHA {
		t.Fatalf("recovery report = %+v", report)
	}
	latest := doJSON(t, harness.client, http.MethodGet, harness.server.URL+"/v1/admin/recovery-report", "", nil)
	if latest.Status != http.StatusOK {
		t.Fatalf("latest report status=%d body=%s", latest.Status, latest.Body)
	}
}

func TestHTTPConcurrentDuplicateRequestExecutesOnce(t *testing.T) {
	harness := newAPIHarness(t, engine.Options{})
	const callers = 20
	body := map[string]any{"object_key": "http/same-request.bin", "lower_bound": 1, "ttl_ms": 3_600_000}
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan httpResult, callers)
	for index := 0; index < callers; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- doJSON(t, harness.client, http.MethodPost, harness.server.URL+"/v1/uploads", "same-http-request", body)
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var expectedBody []byte
	var sessionID string
	replays := 0
	for result := range results {
		if result.Status != http.StatusCreated {
			t.Fatalf("duplicate request status=%d body=%s", result.Status, result.Body)
		}
		decoded := decodeResponse[testCreateResponse](t, result)
		if sessionID == "" {
			sessionID = decoded.Session.SessionID
			expectedBody = result.Body
		}
		if decoded.Session.SessionID != sessionID {
			t.Fatalf("different sessions for one request id: %s vs %s", decoded.Session.SessionID, sessionID)
		}
		if !bytes.Equal(result.Body, expectedBody) {
			t.Fatalf("different replayed bodies:\n%s\n%s", expectedBody, result.Body)
		}
		if result.Header.Get("Idempotent-Replayed") == "true" {
			replays++
		}
	}
	if replays < callers-1 {
		t.Fatalf("replays = %d, want at least %d", replays, callers-1)
	}
	var count int
	if err := harness.store.DB().QueryRow(`SELECT COUNT(*) FROM sessions WHERE object_key = 'http/same-request.bin'`).Scan(&count); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if count != 1 {
		t.Fatalf("sessions = %d, want 1", count)
	}
}

func TestHTTPConcurrentDifferentRequestsSessionConflict(t *testing.T) {
	harness := newAPIHarness(t, engine.Options{})
	const callers = 12
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan httpResult, callers)
	for index := 0; index < callers; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			results <- doJSON(t, harness.client, http.MethodPost, harness.server.URL+"/v1/uploads",
				"different-"+jsonNumber(int64(index)), map[string]any{
					"object_key": "http/conflict.bin", "lower_bound": 1, "ttl_ms": 3_600_000,
				})
		}(index)
	}
	close(start)
	wg.Wait()
	close(results)
	winners := 0
	conflicts := 0
	for result := range results {
		switch result.Status {
		case http.StatusCreated:
			winners++
		case http.StatusConflict:
			var business apiError
			if err := json.Unmarshal(result.Body, &business); err != nil {
				t.Fatalf("decode conflict: %v", err)
			}
			if business.Code != model.CodeSessionConflict || business.Context["existing_session_id"] == "" {
				t.Fatalf("conflict body = %s", result.Body)
			}
			conflicts++
		default:
			t.Fatalf("unexpected create status=%d body=%s", result.Status, result.Body)
		}
	}
	if winners != 1 || conflicts != callers-1 {
		t.Fatalf("winners=%d conflicts=%d", winners, conflicts)
	}
}

func TestHTTPUnifiedBusinessErrors(t *testing.T) {
	harness := newAPIHarness(t, engine.Options{})
	created := createViaHTTP(t, harness, "error-create", "http/errors.bin")
	sessionID := created.Session.SessionID

	mismatchResult := doJSON(t, harness.client, http.MethodPut,
		harness.server.URL+"/v1/uploads/"+sessionID+"/parts/1", "bad-digest", map[string]any{
			"size": 5, "sha256": shaHex("hello"), "content_base64": base64.StdEncoding.EncodeToString([]byte("wrong")),
		})
	var mismatchError apiError
	if err := json.Unmarshal(mismatchResult.Body, &mismatchError); err != nil {
		t.Fatalf("decode mismatch: %v", err)
	}
	if mismatchResult.Status != http.StatusUnprocessableEntity || mismatchError.Code != model.CodePartMismatch {
		t.Fatalf("mismatch response status=%d body=%s", mismatchResult.Status, mismatchResult.Body)
	}

	submitViaHTTP(t, harness, sessionID, "first-good-part", 1, "hello")
	missingResult := doJSON(t, harness.client, http.MethodPost,
		harness.server.URL+"/v1/uploads/"+sessionID+"/merge", "missing-merge", map[string]any{
			"expected_part_count": 2,
		})
	var missingError apiError
	if err := json.Unmarshal(missingResult.Body, &missingError); err != nil {
		t.Fatalf("decode missing: %v", err)
	}
	if missingResult.Status != http.StatusUnprocessableEntity || missingError.Code != model.CodePartMissing {
		t.Fatalf("missing response status=%d body=%s", missingResult.Status, missingResult.Body)
	}

	submitViaHTTP(t, harness, sessionID, "second-good-part", 2, "world")
	sizeResult := doJSON(t, harness.client, http.MethodPost,
		harness.server.URL+"/v1/uploads/"+sessionID+"/merge", "size-merge", map[string]any{
			"expected_total_size": 999,
		})
	var sizeError apiError
	if err := json.Unmarshal(sizeResult.Body, &sizeError); err != nil {
		t.Fatalf("decode size: %v", err)
	}
	if sizeResult.Status != http.StatusUnprocessableEntity || sizeError.Code != model.CodeSizeMismatch {
		t.Fatalf("size response status=%d body=%s", sizeResult.Status, sizeResult.Body)
	}

	conflictCreate := doJSON(t, harness.client, http.MethodPost, harness.server.URL+"/v1/uploads", "second-create", map[string]any{
		"object_key": "http/errors.bin", "lower_bound": 1, "ttl_ms": 3_600_000,
	})
	var conflictError apiError
	if err := json.Unmarshal(conflictCreate.Body, &conflictError); err != nil {
		t.Fatalf("decode session conflict: %v", err)
	}
	if conflictCreate.Status != http.StatusConflict || conflictError.Code != model.CodeSessionConflict {
		t.Fatalf("session conflict status=%d body=%s", conflictCreate.Status, conflictCreate.Body)
	}
}

func TestHTTPHealth(t *testing.T) {
	harness := newAPIHarness(t, engine.Options{})
	result := doJSON(t, harness.client, http.MethodGet, harness.server.URL+"/healthz", "", nil)
	if result.Status != http.StatusOK {
		t.Fatalf("health status=%d body=%s", result.Status, result.Body)
	}
}

func TestRenderVerifyQueueFullError(t *testing.T) {
	outcome := renderError("queue-request", model.NewError(model.CodeVerifyQueueFull, "queue full").
		WithContext("queue_capacity", 0))
	if outcome.Status != http.StatusTooManyRequests || outcome.Code != string(model.CodeVerifyQueueFull) {
		t.Fatalf("outcome = %+v", outcome)
	}
	var body apiError
	if err := json.Unmarshal(outcome.Body, &body); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if body.Code != model.CodeVerifyQueueFull || body.Context["queue_capacity"] != float64(0) {
		t.Fatalf("error body = %+v", body)
	}
}

func TestHTTPRequestReplaySurvivesRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "restart.db")
	firstStore, err := store.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	firstEngine := engine.New(firstStore, engine.Options{})
	firstServer := httptest.NewServer(New(firstEngine, Config{Logger: log.New(io.Discard, "", 0)}))
	body := map[string]any{"object_key": "http/restart.bin", "lower_bound": 1, "ttl_ms": 3_600_000}
	first := doJSON(t, firstServer.Client(), http.MethodPost, firstServer.URL+"/v1/uploads", "restart-request", body)
	if first.Status != http.StatusCreated {
		t.Fatalf("first status=%d body=%s", first.Status, first.Body)
	}
	firstServer.Close()
	firstEngine.Close()
	if err := firstStore.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	secondStore, err := store.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open second store: %v", err)
	}
	t.Cleanup(func() {
		_ = secondStore.Close()
	})
	secondEngine := engine.New(secondStore, engine.Options{})
	t.Cleanup(secondEngine.Close)
	secondServer := httptest.NewServer(New(secondEngine, Config{Logger: log.New(io.Discard, "", 0)}))
	t.Cleanup(secondServer.Close)
	second := doJSON(t, secondServer.Client(), http.MethodPost, secondServer.URL+"/v1/uploads", "restart-request", body)
	if second.Status != first.Status || !bytes.Equal(second.Body, first.Body) {
		t.Fatalf("replay after restart changed:\nfirst=%s\nsecond=%s", first.Body, second.Body)
	}
	if second.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("missing replay header after restart: %+v", second.Header)
	}
	var count int
	if err := secondStore.DB().QueryRow(`SELECT COUNT(*) FROM sessions WHERE object_key = 'http/restart.bin'`).Scan(&count); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if count != 1 {
		t.Fatalf("sessions after restart replay = %d, want 1", count)
	}
}
