package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"time"

	"example.com/gsb-router/internal/errs"
	"example.com/gsb-router/internal/routing"
	"example.com/gsb-router/internal/sample"
	"example.com/gsb-router/internal/store"
)

type envelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data"`
	Error *errs.Error     `json:"error"`
}

type childRouter struct {
	cmd    *exec.Cmd
	done   chan error
	exited bool
}

type runner struct {
	root    string
	demoDir string
	baseURL string
	bin     string
	config  string
	client  *http.Client
	logPath string
}

func main() {
	port := flag.Int("port", 18190, "local HTTP port")
	demoDir := flag.String("dir", "", "demo directory (default: var/demo-go)")
	flag.Parse()

	root, err := os.Getwd()
	if err != nil {
		fail("get working directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		fail("run from repository root (go.mod not found): %v", err)
	}
	if *demoDir == "" {
		*demoDir = filepath.Join(root, "var", "demo-go")
	}
	absDemo, err := filepath.Abs(*demoDir)
	if err != nil {
		fail("resolve demo directory: %v", err)
	}
	binaryName := "routerd"
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	r := &runner{
		root:    root,
		demoDir: absDemo,
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", *port),
		bin:     filepath.Join(root, ".tmp", binaryName),
		config:  filepath.Join(absDemo, "config.json"),
		client:  &http.Client{Timeout: 10 * time.Second},
		logPath: filepath.Join(absDemo, "server.log"),
	}
	if err := r.execute(); err != nil {
		fail("%v", err)
	}
}

func (r *runner) execute() error {
	say("0. build binary and generate fixed-seed JSON samples")
	if err := os.RemoveAll(r.demoDir); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(r.demoDir, "samples"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(r.bin), 0o755); err != nil {
		return err
	}
	if output, err := exec.Command("go", "build", "-o", r.bin, "./cmd/routerd").CombinedOutput(); err != nil {
		return fmt.Errorf("go build failed: %w\n%s", err, output)
	}
	if output, err := exec.Command(r.bin, "samples", "-out", filepath.Join(r.demoDir, "samples")).CombinedOutput(); err != nil {
		return fmt.Errorf("generate samples failed: %w\n%s", err, output)
	}
	config := map[string]any{
		"listen":   strings.TrimPrefix(r.baseURL, "http://"),
		"database": filepath.Join(r.demoDir, "router.db"),
		"seed":     sample.Seed,
	}
	if err := writeJSON(filepath.Join(r.demoDir, "config.json"), config); err != nil {
		return err
	}
	ok("binary=%s", r.bin)
	ok("samples=%s", filepath.Join(r.demoDir, "samples"))
	ok("config=%s", r.config)
	if r.healthy() {
		return fmt.Errorf("port is already serving a healthy process: %s", r.baseURL)
	}

	say("1. load full snapshot v1")
	child, err := r.start()
	if err != nil {
		return err
	}
	defer r.stop(child)
	ok("server pid=%d health=ok", child.cmd.Process.Pid)
	body := store.SnapshotCommitRequest{CommitID: "godemo-snapshot-v1", Snapshot: sample.SnapshotV1()}
	status, env, err := r.api(http.MethodPost, "/api/snapshots", body)
	if err != nil {
		return err
	}
	if err := expectStatus(status, env, http.StatusOK, "snapshot v1"); err != nil {
		return err
	}
	var commit store.CommitResult
	if err := json.Unmarshal(env.Data, &commit); err != nil {
		return err
	}
	if commit.Version != 1 {
		return fmt.Errorf("expected snapshot v1, got %#v", commit)
	}
	ok("snapshot v1 committed")

	queryV1 := routing.Query{TableID: "orders", Observed: 1, Kind: routing.KindEqual, Value: ptr("1")}
	status, firstEnv, err := r.api(http.MethodPost, "/api/query", queryV1)
	if err != nil {
		return err
	}
	if err := expectStatus(status, firstEnv, http.StatusOK, "v1 query"); err != nil {
		return err
	}
	status, secondEnv, err := r.api(http.MethodPost, "/api/query", queryV1)
	if err != nil {
		return err
	}
	if err := expectStatus(status, secondEnv, http.StatusOK, "v1 repeat query"); err != nil {
		return err
	}
	var first, second store.RouteResponse
	if err := json.Unmarshal(firstEnv.Data, &first); err != nil {
		return err
	}
	if err := json.Unmarshal(secondEnv.Data, &second); err != nil {
		return err
	}
	if !reflect.DeepEqual(first.PhysicalTables, []string{"db0.orders_01"}) {
		return fmt.Errorf("unexpected v1 route: %#v", first.PhysicalTables)
	}
	if !reflect.DeepEqual(first.PhysicalTables, second.PhysicalTables) || !reflect.DeepEqual(first.Hits, second.Hits) {
		return fmt.Errorf("repeat route is not deterministic: %#v vs %#v", first, second)
	}
	ok("orders user=1 -> db0.orders_01; repeated compute identical")

	say("2. submit v2 increment affecting only orders")
	status, env, err = r.api(http.MethodPost, "/api/increments", sample.IncrementV2())
	if err != nil {
		return err
	}
	if err := expectStatus(status, env, http.StatusOK, "increment v2"); err != nil {
		return err
	}
	if err := json.Unmarshal(env.Data, &commit); err != nil {
		return err
	}
	if commit.Version != 2 || !reflect.DeepEqual(commit.AffectedTables, []string{"orders"}) {
		return fmt.Errorf("unexpected increment result: %#v", commit)
	}
	ok("increment v2 committed; affected tables=%v", commit.AffectedTables)
	if err := r.checkCaches(); err != nil {
		return err
	}
	ok("orders and order_overview cascaded atomically; unrelated caches retained")

	say("3. replaced-version read is rejected")
	status, env, err = r.api(http.MethodPost, "/api/query", queryV1)
	if err != nil {
		return err
	}
	if err := expectCode(status, env, http.StatusConflict, errs.CodeStaleRead, "stale read"); err != nil {
		return err
	}
	if env.Error.Context["cache_version"] != float64(2) || env.Error.Context["observed_version"] != float64(1) {
		return fmt.Errorf("stale-read context missing versions: %#v", env.Error.Context)
	}
	ok("observed v1 -> E_STALE_READ cache_version=2 observed_version=1")
	queryV2 := routing.Query{TableID: "orders", Observed: 2, Kind: routing.KindEqual, Value: ptr("1")}
	status, env, err = r.api(http.MethodPost, "/api/query", queryV2)
	if err != nil {
		return err
	}
	if err := expectStatus(status, env, http.StatusOK, "v2 query"); err != nil {
		return err
	}
	var routeV2 store.RouteResponse
	if err := json.Unmarshal(env.Data, &routeV2); err != nil {
		return err
	}
	if !reflect.DeepEqual(routeV2.PhysicalTables, []string{"db0.orders_00"}) || routeV2.UsedVersion != 2 {
		return fmt.Errorf("unexpected v2 route: %#v", routeV2)
	}
	ok("observed v2 -> db0.orders_00 used_version=2")

	say("4. persist intent, force termination, restart and recover")
	status, env, err = r.api(http.MethodPost, "/api/increments", sample.IncrementLedgerCrash())
	if err != nil {
		return err
	}
	if err := expectCode(status, env, http.StatusConflict, errs.CodeRebuildInterrupted, "rebuild interrupt"); err != nil {
		return err
	}
	ok("received E_REBUILD_INTERRUPTED after intent persistence")
	select {
	case waitErr := <-child.done:
		child.exited = true
		if waitErr == nil {
			return fmt.Errorf("crash injection exited cleanly; expected forced termination")
		}
		ok("server terminated abnormally: %v", waitErr)
	case <-time.After(10 * time.Second):
		return fmt.Errorf("server did not terminate after interrupt injection")
	}
	if r.healthy() {
		return fmt.Errorf("server is still healthy after forced termination")
	}
	child, err = r.start()
	if err != nil {
		return err
	}
	defer func() {
		if child != nil {
			r.stop(child)
		}
	}()
	ok("server restarted pid=%d", child.cmd.Process.Pid)
	status, env, err = r.api(http.MethodGet, "/api/recovery", nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK || !env.OK {
		return fmt.Errorf("recovery request failed: %d %#v", status, env.Error)
	}
	var recovery struct {
		Status  string                `json:"status"`
		Report  *store.RecoveryReport `json:"report"`
		Current int64                 `json:"current_version"`
	}
	if err := json.Unmarshal(env.Data, &recovery); err != nil {
		return err
	}
	if recovery.Status != "recovered" || recovery.Current != 2 || recovery.Report == nil ||
		recovery.Report.PendingFound != 1 || len(recovery.Report.Actions) != 1 ||
		recovery.Report.Actions[0].Decision != "aborted_incomplete" || !recovery.Report.FactsOnly {
		return fmt.Errorf("unexpected recovery report: %s", string(env.Data))
	}
	ok("recovery report: pending=1 aborted_incomplete current_version=2")
	status, env, err = r.api(http.MethodPost, "/api/query", queryV2)
	if err != nil {
		return err
	}
	if err := expectStatus(status, env, http.StatusOK, "post-recovery query"); err != nil {
		return err
	}
	ok("snapshot v2 remains active; interrupted v3 intent is aborted")

	say("5. five-code injection matrix")
	snapshot := sample.SnapshotV1()
	snapshot.Version = 3
	snapshot.Checksum = "auto"
	status, env, err = r.api(http.MethodPost, "/api/snapshots", store.SnapshotCommitRequest{Snapshot: snapshot, SealChecksum: true, Inject: store.InjectMappingMissing})
	if err != nil {
		return err
	}
	mappingCode, err := captureCode(status, env, http.StatusBadRequest, "mapping fault")
	if err != nil {
		return err
	}
	status, env, err = r.api(http.MethodPost, "/api/snapshots", store.SnapshotCommitRequest{Snapshot: snapshot, SealChecksum: true, Inject: store.InjectRangeOverlap})
	if err != nil {
		return err
	}
	overlapCode, err := captureCode(status, env, http.StatusBadRequest, "overlap fault")
	if err != nil {
		return err
	}
	baseInc := sample.IncrementV2()
	baseInc.CommitID = "godemo-base-regression"
	baseInc.Inject = store.InjectBaseRegression
	baseInc.Checksum = ""
	status, env, err = r.api(http.MethodPost, "/api/increments", baseInc)
	if err != nil {
		return err
	}
	baseCode, err := captureCode(status, env, http.StatusConflict, "base regression fault")
	if err != nil {
		return err
	}
	status, env, err = r.api(http.MethodPost, "/api/query", map[string]any{
		"table_id": "orders", "observed_version": 1, "kind": "eq", "value": "1", "inject": store.InjectObservedAhead,
	})
	if err != nil {
		return err
	}
	aheadCode, err := captureCode(status, env, http.StatusConflict, "observed ahead fault")
	if err != nil {
		return err
	}
	codes := []string{mappingCode, overlapCode, baseCode, aheadCode, errs.CodeRebuildInterrupted}
	seen := map[string]bool{}
	for _, code := range codes {
		seen[code] = true
	}
	if len(seen) != 5 {
		return fmt.Errorf("fault codes are not distinct: %v", codes)
	}
	ok("distinct codes: %s", strings.Join(codes, ", "))

	say("demo complete")
	ok("final current version=2")
	ok("artifacts: %s", r.demoDir)
	ok("server log: %s", r.logPath)
	return nil
}

func (r *runner) start() (*childRouter, error) {
	logFile, err := os.OpenFile(r.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(r.bin, "serve", "-config", r.config)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, err
	}
	child := &childRouter{cmd: cmd, done: make(chan error, 1)}
	go func() {
		err := cmd.Wait()
		_ = logFile.Close()
		child.done <- err
	}()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if r.healthy() {
			return child, nil
		}
		select {
		case err := <-child.done:
			return nil, fmt.Errorf("routerd exited before health check: %v", err)
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	return nil, fmt.Errorf("routerd did not become healthy at %s", r.baseURL)
}

func (r *runner) stop(child *childRouter) {
	if child == nil || child.cmd == nil || child.cmd.Process == nil {
		return
	}
	if child.exited {
		return
	}
	select {
	case <-child.done:
		child.exited = true
		return
	default:
	}
	_ = child.cmd.Process.Kill()
	select {
	case <-child.done:
		child.exited = true
	case <-time.After(3 * time.Second):
	}
}

func (r *runner) healthy() bool {
	ctxClient := &http.Client{Timeout: 700 * time.Millisecond}
	resp, err := ctxClient.Get(r.baseURL + "/api/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (r *runner) api(method, path string, body any) (int, envelope, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, envelope{}, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, r.baseURL+path, reader)
	if err != nil {
		return 0, envelope{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, envelope{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, envelope{}, err
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return resp.StatusCode, envelope{}, fmt.Errorf("decode %s %s: %w body=%s", method, path, err, raw)
	}
	return resp.StatusCode, env, nil
}

func (r *runner) checkCaches() error {
	status, env, err := r.api(http.MethodGet, "/api/caches", nil)
	if err != nil {
		return err
	}
	if err := expectStatus(status, env, http.StatusOK, "cache graph"); err != nil {
		return err
	}
	var data struct {
		RouteCaches     []store.RouteCache     `json:"route_caches"`
		AggregateCaches []store.AggregateCache `json:"aggregate_caches"`
	}
	if err := json.Unmarshal(env.Data, &data); err != nil {
		return err
	}
	route := map[string]store.RouteCache{}
	for _, cache := range data.RouteCaches {
		route[cache.TableID] = cache
	}
	views := map[string]store.AggregateCache{}
	for _, cache := range data.AggregateCaches {
		views[cache.ViewID] = cache
	}
	if route["orders"].CacheVersion != 2 || views["order_overview"].CacheVersion != 2 {
		return fmt.Errorf("changed table or dependent view was not rebuilt: %#v %#v", route["orders"], views["order_overview"])
	}
	if route["events"].CacheVersion != 1 || views["audit_rollup"].CacheVersion != 1 {
		return fmt.Errorf("unrelated caches were incorrectly rebuilt: %#v %#v", route["events"], views["audit_rollup"])
	}
	return nil
}

func expectStatus(status int, env envelope, expected int, label string) error {
	if status != expected || !env.OK {
		return fmt.Errorf("%s: expected HTTP %d ok, got %d %#v", label, expected, status, env.Error)
	}
	return nil
}

func expectCode(status int, env envelope, expectedStatus int, code, label string) error {
	if status != expectedStatus || env.OK || env.Error == nil || env.Error.Code != code {
		return fmt.Errorf("%s: expected HTTP %d %s, got %d data=%s error=%#v", label, expectedStatus, code, status, env.Data, env.Error)
	}
	return nil
}

func captureCode(status int, env envelope, expectedStatus int, label string) (string, error) {
	if status != expectedStatus || env.Error == nil {
		return "", fmt.Errorf("%s: expected HTTP %d error, got %d data=%s", label, expectedStatus, status, env.Data)
	}
	if env.Error.Context == nil {
		return "", fmt.Errorf("%s: error context is missing", label)
	}
	return env.Error.Code, nil
}

func writeJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

func ptr[T any](value T) *T { return &value }

func say(message string) {
	fmt.Printf("\n== %s ==\n", message)
}

func ok(format string, args ...any) {
	fmt.Printf("  [ok] "+format+"\n", args...)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[demo failed] "+format+"\n", args...)
	os.Exit(1)
}
