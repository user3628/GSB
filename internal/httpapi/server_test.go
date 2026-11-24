package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"local/sqlitepool/internal/dbconn"
	"local/sqlitepool/internal/domain"
	"local/sqlitepool/internal/pool"
	"local/sqlitepool/internal/store"
)

type testAPI struct {
	server *httptest.Server
	pool   *pool.Pool
	store  *store.Store
}

func newTestAPI(t *testing.T, cfg domain.Config) *testAPI {
	t.Helper()
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "meta.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.SeedConfig(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	p, err := pool.New(context.Background(), cfg, st, dbconn.SQLiteOpener{Path: filepath.Join(t.TempDir(), "data.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(New(p, st, Options{AllowDebug: true}).Handler())
	t.Cleanup(func() {
		ts.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = p.Shutdown(ctx)
		_ = st.Close()
	})
	return &testAPI{server: ts, pool: p, store: st}
}

type rawResponse struct {
	status int
	body   []byte
}

type parsedEnvelope struct {
	OK        bool            `json:"ok"`
	RequestID string          `json:"request_id"`
	Data      json.RawMessage `json:"data"`
	Error     struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Context map[string]any `json:"context"`
	} `json:"error"`
}

func (a *testAPI) request(t *testing.T, method, path, requestID string, body any) rawResponse {
	t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, a.server.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if requestID != "" {
		req.Header.Set("X-Request-Id", requestID)
	}
	resp, err := a.server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return rawResponse{status: resp.StatusCode, body: data}
}

func parse(t *testing.T, raw rawResponse) parsedEnvelope {
	t.Helper()
	var env parsedEnvelope
	if err := json.Unmarshal(raw.body, &env); err != nil {
		t.Fatalf("decode response: %v body=%s", err, raw.body)
	}
	return env
}

func TestIdempotentAcquireAndContentConflict(t *testing.T) {
	api := newTestAPI(t, domain.Config{MaxOpen: 1, MaxIdle: 1, AcquireTimeoutMS: 2000, MaxWaiters: 2, Version: 1})
	var wg sync.WaitGroup
	responses := make([]rawResponse, 2)
	for i := 0; i < 2; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			responses[i] = api.request(t, http.MethodPost, "/v1/acquire", "same-request", map[string]any{"timeout_ms": 3000})
		}()
	}
	wg.Wait()
	if responses[0].status != http.StatusOK || responses[1].status != http.StatusOK {
		t.Fatalf("duplicate request statuses: %d %d", responses[0].status, responses[1].status)
	}
	if !bytes.Equal(responses[0].body, responses[1].body) {
		t.Fatalf("duplicate request did not reuse exact result\nfirst=%s\nsecond=%s", responses[0].body, responses[1].body)
	}
	env := parse(t, responses[0])
	var data struct {
		Lease domain.Lease `json:"lease"`
	}
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.Lease.ConnectionID == "" {
		t.Fatalf("missing lease: %s", responses[0].body)
	}

	conflict := api.request(t, http.MethodPost, "/v1/acquire", "same-request", map[string]any{"timeout_ms": 5000})
	conflictEnv := parse(t, conflict)
	if conflict.status != http.StatusConflict || conflictEnv.Error.Code != domain.CodeConflictingRequest {
		t.Fatalf("expected E_CONFLICTING_REQUEST, status=%d body=%s", conflict.status, conflict.body)
	}
	if conflictEnv.Error.Context == nil {
		t.Fatal("conflict response context is missing")
	}
	_, err := api.pool.Release(context.Background(), data.Lease.ConnectionID, "release-test")
	if err != nil {
		t.Fatal(err)
	}
}

func TestConfigVersionIsMonotonic(t *testing.T) {
	api := newTestAPI(t, domain.Config{MaxOpen: 2, MaxIdle: 2, AcquireTimeoutMS: 1000, MaxWaiters: 2, Version: 3})
	configBody := func(version int64) map[string]any {
		return map[string]any{
			"max_open": 2, "max_idle": 2, "max_lifetime_ms": 60000,
			"max_idle_time_ms": 30000, "acquire_timeout_ms": 1000,
			"max_waiters": 2, "version": version,
		}
	}
	stale := api.request(t, http.MethodPut, "/v1/config", "config-stale", configBody(3))
	staleEnv := parse(t, stale)
	if stale.status != http.StatusConflict || staleEnv.Error.Code != domain.CodeStaleConfig {
		t.Fatalf("expected E_STALE_CONFIG, status=%d body=%s", stale.status, stale.body)
	}
	applied := api.request(t, http.MethodPut, "/v1/config", "config-new", configBody(4))
	if applied.status != http.StatusOK {
		t.Fatalf("config update failed: %d %s", applied.status, applied.body)
	}
	if api.pool.Config().Version != 4 {
		t.Fatalf("config version not applied: %+v", api.pool.Config())
	}
	again := api.request(t, http.MethodPut, "/v1/config", "config-again", configBody(4))
	againEnv := parse(t, again)
	if again.status != http.StatusConflict || againEnv.Error.Code != domain.CodeStaleConfig {
		t.Fatalf("equal version should be stale: %d %s", again.status, again.body)
	}
}

func TestFailureResponseCarriesCodeAndContext(t *testing.T) {
	api := newTestAPI(t, domain.Config{MaxOpen: 1, MaxIdle: 1, AcquireTimeoutMS: 1000, MaxWaiters: 1, Version: 1})
	raw := api.request(t, http.MethodPost, "/v1/query", "query-missing", map[string]any{
		"conn_id": "does-not-exist", "sql": "SELECT 1",
	})
	env := parse(t, raw)
	if raw.status != http.StatusBadGateway || env.Error.Code != domain.CodeConnReset {
		t.Fatalf("expected E_CONN_RESET, status=%d body=%s", raw.status, raw.body)
	}
	if env.Error.Context["business_code"] != domain.CodeConnReset || env.Error.Context["request_id"] != "query-missing" {
		t.Fatalf("failure context missing: %+v", env.Error.Context)
	}
}
