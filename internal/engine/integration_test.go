package engine_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cdn-edge-consistency/internal/cache"
	"cdn-edge-consistency/internal/engine"
	"cdn-edge-consistency/internal/httpapi"
	"cdn-edge-consistency/internal/model"
	"cdn-edge-consistency/internal/policy"
	"cdn-edge-consistency/internal/store"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}

type harness struct {
	t         *testing.T
	eng       *engine.Engine
	cfg       model.Config
	clock     *fakeClock
	baseURL   string
	originDir string
	client    *http.Client
	server    *http.Server
	listener  net.Listener
	stopOnce  sync.Once
}

func defaultPolicy(nodes []string) *policy.Snapshot {
	return &policy.Snapshot{
		Version: 1, GeneratedAt: "2026-11-23T00:00:00Z", Nodes: nodes,
		TTLMs: 2000, SWRMs: 1000, RevalidateWindowMs: 5000, PurgeWindowMs: 3000,
		OriginTimeoutMs: 1000, RevalidateTimeoutMs: 1500,
		Vary:     []string{"Accept-Language", "X-Tenant"},
		KeyRules: cache.KeyRules{CollapseSlashes: true, MergeDotSegments: true, UppercasePercent: true, SortQuery: true},
		Scheduler: policy.SchedulerPolicy{
			GlobalOriginConcurrency: 2, PerOriginConcurrency: 1, QueueCapacity: 32,
			RoundBudgetMs: 50, DefaultTaskCostMs: 1,
		},
	}
}

func newHarness(t *testing.T, mutate func(*policy.Snapshot)) *harness {
	t.Helper()
	originDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(originDir, "static"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(originDir, "api"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeOrigin(t, originDir, "static/app.js", "console.log(\"app-v1\");\n")
	writeOrigin(t, originDir, "static/style.css", "body { color: #111; }\n")
	writeOrigin(t, originDir, "api/data.json", "{\"version\":1,\"items\":[1,2,3]}\n")
	tags := `{"/static/app.js":["static","app"],"/static/style.css":["static","css"],"/api/data.json":["api","dataset"]}`
	tagsPath := filepath.Join(originDir, "tags.json")
	if err := os.WriteFile(tagsPath, []byte(tags), 0o644); err != nil {
		t.Fatal(err)
	}
	nodes := []string{"edge-a", "edge-b", "edge-c", "edge-d"}
	p := defaultPolicy(nodes)
	if mutate != nil {
		mutate(p)
	}
	cfg := model.Config{
		ListenAddr: "127.0.0.1:0", DBPath: filepath.Join(t.TempDir(), "lab.db"),
		OriginRoot: originDir, TagsFile: tagsPath,
		OriginTimeoutMs: 1000, RevalidateTimeout: 1500,
		AllowDebugCrash: true,
		Nodes: []model.NodeConfig{
			{ID: "edge-a", OriginID: "origin-main", Priority: 40},
			{ID: "edge-b", OriginID: "origin-main", Priority: 30},
			{ID: "edge-c", OriginID: "origin-main", Priority: 20},
			{ID: "edge-d", OriginID: "origin-main", Priority: 10},
		},
	}
	clock := &fakeClock{now: time.UnixMilli(1_800_000_000_000)}
	eng, err := engine.New(context.Background(), cfg, p, clock)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	api := httpapi.New(eng, &engine.OriginSource{Root: originDir, TagsFile: tagsPath}, cfg, log.New(io.Discard, "", 0))
	server := &http.Server{Handler: api.Handler()}
	go func() {
		_ = server.Serve(listener)
	}()
	eng.SetOriginBaseURL("http://" + listener.Addr().String() + "/origin")
	h := &harness{
		t: t, eng: eng, cfg: cfg, clock: clock, originDir: originDir,
		baseURL: "http://" + listener.Addr().String(), server: server, listener: listener,
		client: &http.Client{Timeout: 8 * time.Second},
	}
	t.Cleanup(h.stop)
	return h
}

func (h *harness) stop() {
	h.stopOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if h.server != nil {
			_ = h.server.Shutdown(ctx)
		}
		if h.eng != nil {
			_ = h.eng.Close(ctx)
		}
	})
}

func writeOrigin(t *testing.T, root, rel, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(rel)), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

type response struct {
	status int
	header http.Header
	body   []byte
}

func (r response) code(t *testing.T) string {
	t.Helper()
	var v struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(r.body, &v); err != nil {
		t.Fatalf("decode error body %q: %v", r.body, err)
	}
	return v.Code
}

func (h *harness) request(method, path string, headers map[string]string) response {
	h.t.Helper()
	req, err := http.NewRequest(method, h.baseURL+path, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return response{status: resp.StatusCode, header: resp.Header.Clone(), body: body}
}

func (h *harness) edge(path string, headers map[string]string) response {
	return h.request(http.MethodGet, path, headers)
}

func (h *harness) postJSON(path, requestID string, value any) response {
	h.t.Helper()
	raw, _ := json.Marshal(value)
	req, _ := http.NewRequest(http.MethodPost, h.baseURL+path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if requestID != "" {
		req.Header.Set("X-Request-ID", requestID)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return response{status: resp.StatusCode, header: resp.Header.Clone(), body: body}
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}

func bodyETag(body []byte) string {
	sum := sha256.Sum256(body)
	return `"sha256-` + hex.EncodeToString(sum[:]) + `"`
}

func TestMultiNodeHitVaryAndNormalization(t *testing.T) {
	h := newHarness(t, nil)
	r1 := h.edge("/edge/edge-a/static//./app.js?b=2&a=1", nil)
	if r1.status != 200 || r1.header.Get("X-Cache-Status") != "MISS" {
		t.Fatalf("first response: %d %s %s", r1.status, r1.header.Get("X-Cache-Status"), r1.body)
	}
	key := r1.header.Get("X-Cache-Key")
	if key == "" {
		t.Fatal("missing cache key")
	}
	r2 := h.edge("/edge/edge-b/static/app.js?a=1&b=2", nil)
	if r2.status != 200 || r2.header.Get("X-Cache-Status") != "HIT" || r2.header.Get("X-Cache-Key") != key {
		t.Fatalf("equivalent URL did not hit: %d %s key=%s want=%s", r2.status, r2.header.Get("X-Cache-Status"), r2.header.Get("X-Cache-Key"), key)
	}
	if !bytes.Equal(r1.body, r2.body) {
		t.Fatal("body changed across equivalent edge nodes")
	}
	r3 := h.edge("/edge/edge-c/static/app.js?a=1&b=2", map[string]string{"Accept-Language": "en"})
	if r3.status != 200 || r3.header.Get("X-Cache-Status") != "MISS" || r3.header.Get("X-Cache-Key") == key {
		t.Fatalf("Vary variant was not distinct: %d %s", r3.status, r3.header.Get("X-Cache-Status"))
	}
	r4 := h.edge("/edge/edge-d/static/app.js?b=2&a=1", map[string]string{"Accept-Language": " en "})
	if r4.header.Get("X-Cache-Status") != "HIT" || r4.header.Get("X-Cache-Key") != r3.header.Get("X-Cache-Key") {
		t.Fatalf("Vary hit failed: %s", r4.header.Get("X-Cache-Status"))
	}
	metrics, err := h.eng.Metrics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if metrics["origin_fetches"] != 2 {
		t.Fatalf("origin fetches=%d want 2", metrics["origin_fetches"])
	}
	entries, _ := h.eng.Store().ListEntries(context.Background(), store.EntryFilter{NormPath: "/static/app.js"})
	if len(entries) != 8 {
		t.Fatalf("entries=%d want 8 (2 variants x 4 nodes)", len(entries))
	}
}

func TestCoalescedOriginAcrossNodes(t *testing.T) {
	h := newHarness(t, nil)
	var wg sync.WaitGroup
	results := make(chan response, 4)
	start := make(chan struct{})
	for _, node := range []string{"edge-a", "edge-b", "edge-c", "edge-d"} {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- h.edge("/edge/"+node+"/static/app.js?coalesce=1&__fault=origin_delay&__delay_ms=250", map[string]string{
				"X-Request-ID": "coalesce-" + node,
			})
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	keys := map[string]bool{}
	for r := range results {
		if r.status != 200 {
			t.Fatalf("coalesced request failed: %d %s", r.status, r.body)
		}
		keys[r.header.Get("X-Content-Version")] = true
	}
	if len(keys) != 1 {
		t.Fatalf("nodes received different versions: %+v", keys)
	}
	metrics, _ := h.eng.Metrics(context.Background())
	if metrics["origin_fetches"] != 1 {
		t.Fatalf("origin_fetches=%d want 1", metrics["origin_fetches"])
	}
	if metrics["runtime_coalesced_waiters"] < 1 {
		t.Fatalf("coalesced waiters=%d want >=1", metrics["runtime_coalesced_waiters"])
	}
	originOwners := 0
	for _, node := range []string{"edge-a", "edge-b", "edge-c", "edge-d"} {
		trace, err := h.eng.TraceFor(context.Background(), "coalesce-"+node)
		if err != nil {
			t.Fatal(err)
		}
		if trace.Decision == "MISS_FETCH" {
			originOwners++
			if trace.OriginFetches != 1 {
				t.Errorf("origin owner %s fetch count=%d", node, trace.OriginFetches)
			}
		} else if trace.OriginFetches != 0 {
			t.Errorf("non-owner %s unexpectedly fetched origin: %+v", node, trace)
		}
	}
	if originOwners != 1 {
		t.Fatalf("origin owners=%d want 1", originOwners)
	}
}

func TestStaleWhileRevalidateAndVersionPropagation(t *testing.T) {
	h := newHarness(t, nil)
	first := h.edge("/edge/edge-a/static/app.js", nil)
	if first.status != 200 {
		t.Fatalf("prime failed: %d", first.status)
	}
	v1 := first.header.Get("X-Content-Version")
	newBody := []byte("console.log(\"app-v2\");\n")
	writeOrigin(t, h.originDir, "static/app.js", string(newBody))
	h.clock.Advance(2000 * time.Millisecond)
	stale := h.edge("/edge/edge-b/static/app.js", map[string]string{"X-Request-ID": "swr-stale"})
	if stale.status != 200 || stale.header.Get("X-Cache-Status") != "STALE-SWR" {
		t.Fatalf("expected stale SWR: %d %s", stale.status, stale.header.Get("X-Cache-Status"))
	}
	if !strings.Contains(stale.header.Get("Warning"), "110") {
		t.Fatalf("missing stale warning: %v", stale.header)
	}
	if !bytes.Contains(stale.body, []byte("app-v1")) {
		t.Fatalf("SWR did not serve old body: %q", stale.body)
	}
	v2 := bodyETag(newBody)
	waitFor(t, 3*time.Second, func() bool {
		entries, err := h.eng.Store().ListEntries(context.Background(), store.EntryFilter{NormPath: "/static/app.js"})
		if err != nil || len(entries) != 4 {
			return false
		}
		for _, e := range entries {
			if e.ETag != v2 || e.State != "FRESH" {
				return false
			}
		}
		return true
	})
	after := h.edge("/edge/edge-c/static/app.js", nil)
	if after.header.Get("X-Content-Version") != strings.Trim(v2, `"`) || after.header.Get("X-Cache-Status") != "HIT" {
		t.Fatalf("version did not propagate: %s %s", after.header.Get("X-Content-Version"), after.header.Get("X-Cache-Status"))
	}
	if v1 == strings.Trim(v2, `"`) {
		t.Fatal("test origin version did not change")
	}
	if !bytes.Equal(after.body, newBody) {
		t.Fatalf("new body mismatch: %q", after.body)
	}
}

func TestPurgeIsAtomicAndIncompleteHasNoPartialState(t *testing.T) {
	h := newHarness(t, nil)
	h.edge("/edge/edge-a/static/app.js", nil)
	h.edge("/edge/edge-a/static/app.js", map[string]string{"Accept-Language": "en"})
	incomplete, err := h.eng.Purge(context.Background(), engine.PurgeRequest{
		Kind: "path", Path: "/static/app.js", InterruptNodes: []string{"edge-b"}, RequestID: "purge-fail",
	})
	if err == nil || model.AsBiz(err).Code != model.CodePurgeIncomplete {
		t.Fatalf("expected E_PURGE_INCOMPLETE: %v", err)
	}
	if incomplete == nil || len(incomplete.IncompleteNodes) != 1 || incomplete.IncompleteNodes[0] != "edge-b" {
		t.Fatalf("incomplete nodes=%+v", incomplete)
	}
	entries, _ := h.eng.Store().ListEntries(context.Background(), store.EntryFilter{NormPath: "/static/app.js"})
	if len(entries) != 8 {
		t.Fatalf("entries=%d", len(entries))
	}
	for _, e := range entries {
		if e.State == "PURGED" {
			t.Fatalf("partial purge visible on %s", e.NodeID)
		}
	}
	var activeRoutes int
	if err := h.eng.Store().Reader().QueryRow(`SELECT COUNT(*) FROM route_map WHERE active=1`).Scan(&activeRoutes); err != nil {
		t.Fatal(err)
	}
	if activeRoutes != 8 {
		t.Fatalf("route indexes invalidated by aborted purge: %d", activeRoutes)
	}

	purged, err := h.eng.Purge(context.Background(), engine.PurgeRequest{
		Kind: "tag", Tag: "static", RequestID: "purge-ok",
	})
	if err != nil {
		t.Fatalf("successful purge: %v", err)
	}
	if purged.AffectedEntries != 8 {
		t.Fatalf("affected=%d want 8", purged.AffectedEntries)
	}
	entries, _ = h.eng.Store().ListEntries(context.Background(), store.EntryFilter{NormPath: "/static/app.js"})
	for _, e := range entries {
		if e.State != "PURGED" {
			t.Fatalf("node %s state=%s", e.NodeID, e.State)
		}
	}
	var activeTags int
	if err := h.eng.Store().Reader().QueryRow(`SELECT COUNT(*) FROM entry_tags WHERE active=1`).Scan(&activeTags); err != nil {
		t.Fatal(err)
	}
	if activeTags != 0 {
		t.Fatalf("tag index still active: %d", activeTags)
	}
	repopulated := h.edge("/edge/edge-d/static/app.js", nil)
	if repopulated.header.Get("X-Cache-Status") != "MISS" || !bytes.Contains(repopulated.body, []byte("app-v1")) {
		t.Fatalf("repopulation after purge failed: %s %q", repopulated.header.Get("X-Cache-Status"), repopulated.body)
	}
}

func TestSnapshotVersioningAndBoundRead(t *testing.T) {
	h := newHarness(t, nil)
	base := h.eng.CurrentPolicy()
	v2 := base.Clone()
	v2.Version = 2
	v2.GeneratedAt = "2026-11-23T01:00:00Z"
	v2.TTLMs = 5000
	v2.Checksum = ""
	if _, err := h.eng.SubmitPolicy(context.Background(), v2, "policy-v2"); err != nil {
		t.Fatalf("submit v2: %v", err)
	}
	if _, err := h.eng.SubmitPolicy(context.Background(), base.Clone(), "policy-v1-again"); model.AsBiz(err).Code != model.CodeStaleSnapshot {
		t.Fatalf("expected stale snapshot, got %v", err)
	}
	if h.eng.CurrentPolicy().Version != 2 {
		t.Fatal("stale submission replaced current policy")
	}
	bad := base.Clone()
	bad.Version = 3
	bad.GeneratedAt = "2026-11-23T02:00:00Z"
	bad.Checksum = "deadbeef"
	if _, err := h.eng.SubmitPolicy(context.Background(), bad, "policy-bad-checksum"); model.AsBiz(err).Code != model.CodeSnapshotChecksum {
		t.Fatalf("expected checksum failure, got %v", err)
	}
	if h.eng.CurrentPolicy().Version != 2 {
		t.Fatal("failed policy replaced current policy")
	}
	oldRead := h.edge("/edge/edge-a/static/app.js", map[string]string{"X-Policy-Version": "1"})
	if oldRead.status != 200 || oldRead.header.Get("X-Snapshot-Version") != "1" {
		t.Fatalf("bound old version failed: %d %s", oldRead.status, oldRead.header.Get("X-Snapshot-Version"))
	}
	missing := h.edge("/edge/edge-a/static/app.js", map[string]string{"X-Policy-Version": "99"})
	if missing.status != 409 || missing.code(t) != model.CodeVersionRetired {
		t.Fatalf("expected E_VERSION_RETIRED, got %d %s", missing.status, missing.body)
	}
}

func TestQueryCompletesOnBoundPolicyDuringReplacement(t *testing.T) {
	h := newHarness(t, nil)
	done := make(chan response, 1)
	go func() {
		done <- h.edge("/edge/edge-a/static/app.js?bound=1&__fault=origin_delay&__delay_ms=300", map[string]string{
			"X-Request-ID": "bound-policy",
		})
	}()
	time.Sleep(40 * time.Millisecond)
	next := h.eng.CurrentPolicy()
	next.Version = 2
	next.GeneratedAt = "2026-11-23T01:00:00Z"
	next.Checksum = ""
	if _, err := h.eng.SubmitPolicy(context.Background(), next, "during-query"); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.status != 200 || r.header.Get("X-Snapshot-Version") != "1" {
		t.Fatalf("query did not complete on bound version: %d snapshot=%s body=%s", r.status, r.header.Get("X-Snapshot-Version"), r.body)
	}
}

func TestFaultCodesAndIdempotentReplay(t *testing.T) {
	h := newHarness(t, func(p *policy.Snapshot) { p.OriginTimeoutMs = 80 })
	cases := []struct {
		name string
		path string
		code string
	}{
		{"timeout", "/edge/edge-a/static/style.css?case=timeout&__fault=origin_timeout&__delay_ms=1000", model.CodeOriginTimeout},
		{"refused", "/edge/edge-a/static/style.css?case=refused&__fault=origin_refused", model.CodeOriginRefused},
		{"corrupt", "/edge/edge-a/static/style.css?case=corrupt&__fault=body_corrupt&__corrupt=truncate&__corrupt_bytes=4", model.CodeBodyCorrupt},
		{"status", "/edge/edge-a/static/style.css?case=status&__fault=origin_status&__status=503", model.CodeOriginStatus},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := h.edge(tc.path, map[string]string{"X-Request-ID": "fault-" + tc.name})
			if r.code(t) != tc.code {
				t.Fatalf("code=%s want %s body=%s", r.code(t), tc.code, r.body)
			}
			tr, err := h.eng.TraceFor(context.Background(), "fault-"+tc.name)
			if err != nil {
				t.Fatal(err)
			}
			if tr.BusinessCode != tc.code || tr.HTTPStatus < 400 {
				t.Fatalf("trace=%+v", tr)
			}
		})
	}
	first := h.edge("/edge/edge-a/api/data.json", map[string]string{"X-Request-ID": "idem-1"})
	second := h.edge("/edge/edge-a/api/data.json", map[string]string{"X-Request-ID": "idem-1"})
	if first.status != 200 || second.status != 200 {
		t.Fatalf("idempotent statuses: %d %d", first.status, second.status)
	}
	if second.header.Get("X-Idempotent-Replay") != "true" {
		t.Fatal("duplicate request was not replayed")
	}
	if first.header.Get("X-Trace-Hash") != second.header.Get("X-Trace-Hash") {
		t.Fatalf("trace hash changed: %s vs %s", first.header.Get("X-Trace-Hash"), second.header.Get("X-Trace-Hash"))
	}
	if !bytes.Equal(first.body, second.body) {
		t.Fatal("replayed body differs")
	}
	metrics, _ := h.eng.Metrics(context.Background())
	if metrics["origin_fetches"] != 1 {
		t.Fatalf("idempotency triggered another origin fetch: %d", metrics["origin_fetches"])
	}
	records, _ := h.eng.Store().ListTraces(context.Background(), 100)
	idemCount := 0
	for _, rec := range records {
		if rec.RequestID == "idem-1" {
			idemCount++
		}
	}
	if idemCount != 1 {
		t.Fatalf("idempotent request stored %d records", idemCount)
	}
}

func TestForcedCrashRecoveryReportAndIntentRollback(t *testing.T) {
	h := newHarness(t, nil)
	prime := h.edge("/edge/edge-a/static/app.js", nil)
	if prime.status != 200 {
		t.Fatalf("prime: %d", prime.status)
	}
	staleCheck := h.edge("/edge/edge-a/static/style.css?case=timeout&__fault=origin_timeout&__delay_ms=1000", map[string]string{"X-Request-ID": "recover-timeout"})
	if staleCheck.code(t) != model.CodeOriginTimeout {
		t.Fatalf("seed timeout code=%s", staleCheck.code(t))
	}
	if _, err := h.eng.SubmitPolicy(context.Background(), h.eng.CurrentPolicy().Clone(), "recover-stale-policy"); model.AsBiz(err).Code != model.CodeStaleSnapshot {
		t.Fatalf("seed stale policy: %v", err)
	}
	before, _ := h.eng.Store().ListEntries(context.Background(), store.EntryFilter{NormPath: "/static/app.js"})
	if len(before) != 4 {
		t.Fatalf("before entries=%d", len(before))
	}
	originalETag := before[0].ETag
	if err := h.eng.DebugCrash(context.Background(), "edge-a", "/static/app.js", "crash-fixture-1"); err != nil {
		t.Fatalf("debug crash fixture: %v", err)
	}
	partial, _ := h.eng.Store().ListEntries(context.Background(), store.EntryFilter{NormPath: "/static/app.js"})
	for _, e := range partial {
		if e.State != "PURGED" {
			t.Fatalf("crash fixture did not leave partial state: %s", e.State)
		}
	}
	h.stop()

	eng, err := engine.New(context.Background(), h.cfg, nil, h.clock)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer eng.Close(context.Background())
	report := eng.LatestRecoveryReport()
	if report == nil || !report.FactsOnly {
		t.Fatalf("missing recovery report: %+v", report)
	}
	if report.ScannedIntents == 0 || report.RolledBackRows == 0 || len(report.AbortedPurges) == 0 {
		t.Fatalf("recovery did not roll back pending work: %+v", report)
	}
	codes := map[string]bool{}
	for _, c := range report.BusinessCodes {
		codes[c.Code] = true
	}
	for _, want := range []string{model.CodePurgeIncomplete, model.CodeOriginTimeout, model.CodeStaleSnapshot} {
		if !codes[want] {
			t.Errorf("recovery report missing persisted code %s: %+v", want, report.BusinessCodes)
		}
	}
	restored, _ := eng.Store().ListEntries(context.Background(), store.EntryFilter{NormPath: "/static/app.js"})
	if len(restored) != 4 {
		t.Fatalf("restored entries=%d", len(restored))
	}
	for _, e := range restored {
		if e.State != "FRESH" || e.ETag != originalETag {
			t.Fatalf("entry not rolled back: node=%s state=%s etag=%s want=%s", e.NodeID, e.State, e.ETag, originalETag)
		}
	}
	purges, _ := eng.ListPurges(context.Background(), 10)
	foundAborted := false
	for _, p := range purges {
		if p.Status == "aborted" && p.ErrorCode == model.CodePurgeIncomplete {
			foundAborted = true
		}
	}
	if !foundAborted {
		t.Fatal("pending purge was not marked aborted")
	}
}

func TestRevalidateTimeoutLeavesSharedOriginRunning(t *testing.T) {
	h := newHarness(t, func(p *policy.Snapshot) {
		p.OriginTimeoutMs = 1000
		p.RevalidateTimeoutMs = 80
	})
	start := time.Now()
	r := h.edge("/edge/edge-a/static/app.js?case=revalidate-timeout&__fault=origin_delay&__delay_ms=300", map[string]string{
		"X-Request-ID": "revalidate-timeout",
	})
	if r.code(t) != model.CodeRevalidateTimeout {
		t.Fatalf("code=%s want %s body=%s", r.code(t), model.CodeRevalidateTimeout, r.body)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("revalidate timeout took too long: %v", elapsed)
	}
	waitFor(t, 2*time.Second, func() bool {
		entries, err := h.eng.Store().ListEntries(context.Background(), store.EntryFilter{NormPath: "/static/app.js"})
		if err != nil || len(entries) != 4 {
			return false
		}
		for _, e := range entries {
			if e.State != "FRESH" {
				return false
			}
		}
		return true
	})
	hit := h.edge("/edge/edge-b/static/app.js?case=revalidate-timeout", nil)
	if hit.header.Get("X-Cache-Status") != "HIT" {
		t.Fatalf("shared origin did not finish after waiter timeout: %s", hit.header.Get("X-Cache-Status"))
	}
}

func TestRevalidateWindowBoundsVersionPropagation(t *testing.T) {
	h := newHarness(t, func(p *policy.Snapshot) {
		p.RevalidateWindowMs = 100
		p.OriginTimeoutMs = 1000
		p.RevalidateTimeoutMs = 1000
	})
	start := time.Now()
	r := h.edge("/edge/edge-a/static/app.js?case=revalidate-window&__fault=origin_delay&__delay_ms=800", map[string]string{
		"X-Request-ID": "revalidate-window",
	})
	if r.code(t) != model.CodeRevalidateTimeout {
		t.Fatalf("code=%s want %s body=%s", r.code(t), model.CodeRevalidateTimeout, r.body)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("revalidate window did not bound the request: %v", elapsed)
	}
	entries, err := h.eng.Store().ListEntries(context.Background(), store.EntryFilter{NormPath: "/static/app.js"})
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.State == cache.StateFresh || entry.ETag != "" {
			t.Fatalf("late origin result was committed after window: node=%s state=%s etag=%s", entry.NodeID, entry.State, entry.ETag)
		}
	}
}

func TestOriginQueueFullAndBudgetSkip(t *testing.T) {
	t.Run("queue-full", func(t *testing.T) {
		h := newHarness(t, func(p *policy.Snapshot) { p.Scheduler.QueueCapacity = 0 })
		r := h.edge("/edge/edge-a/static/app.js", nil)
		if r.code(t) != model.CodeOriginQueueFull {
			t.Fatalf("code=%s want %s body=%s", r.code(t), model.CodeOriginQueueFull, r.body)
		}
	})
	t.Run("budget-skip", func(t *testing.T) {
		h := newHarness(t, func(p *policy.Snapshot) {
			p.Scheduler.GlobalOriginConcurrency = 1
			p.Scheduler.PerOriginConcurrency = 1
			p.Scheduler.RoundBudgetMs = 1
			p.Scheduler.DefaultTaskCostMs = 1
			p.Scheduler.QueueCapacity = 8
		})
		first := make(chan response, 1)
		go func() {
			first <- h.edge("/edge/edge-a/static/app.js?budget=1&__fault=origin_delay&__delay_ms=180", map[string]string{"X-Request-ID": "budget-1"})
		}()
		time.Sleep(40 * time.Millisecond)
		second := h.edge("/edge/edge-b/static/style.css?budget=2&__fault=origin_delay&__delay_ms=180", map[string]string{"X-Request-ID": "budget-2"})
		r1 := <-first
		if r1.status != 200 {
			t.Fatalf("first request failed: %d %s", r1.status, r1.body)
		}
		if second.code(t) != model.CodeBudgetExhausted {
			t.Fatalf("second code=%s want %s body=%s", second.code(t), model.CodeBudgetExhausted, second.body)
		}
		tr, err := h.eng.TraceFor(context.Background(), "budget-2")
		if err != nil {
			t.Fatal(err)
		}
		skipped, ok := tr.ErrorDetails["skipped"].([]any)
		if !ok || len(skipped) == 0 {
			t.Fatalf("budget trace missing deterministic skipped list: %+v", tr.ErrorDetails)
		}
		report := h.eng.SchedulerReport()
		if len(report.Skipped) == 0 {
			t.Fatalf("scheduler report missing skip facts: %+v", report)
		}
	})
}
