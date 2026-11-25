// Command verifyrun is the reproducible end-to-end verification tool. It
// generates the fixed-seed sample, drives the HTTP API through a full
// verification, an incremental verification, divergence location, error
// code checks and a hard-kill recovery rehearsal, printing PASS/FAIL for
// every check.
//
// When started with BLOCKINSPECT_CHILD=1 it instead runs the API server
// (used by the tool itself to rehearse crash recovery).
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"blockinspect/internal/api"
	"blockinspect/internal/manifest"
	"blockinspect/internal/sample"
	"blockinspect/internal/service"
	"blockinspect/internal/store"
)

var (
	checks  int
	failed  int
	baseURL string
)

func check(name string, ok bool, detail string) {
	checks++
	status := "PASS"
	if !ok {
		status = "FAIL"
		failed++
	}
	fmt.Printf("[%s] %s", status, name)
	if detail != "" {
		fmt.Printf(" -- %s", detail)
	}
	fmt.Println()
}

func post(path string, body any) (int, map[string]any) {
	b, _ := json.Marshal(body)
	resp, err := http.Post(baseURL+path, "application/json", bytes.NewReader(b))
	if err != nil {
		return 0, map[string]any{"transport_error": err.Error()}
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(data, &out)
	return resp.StatusCode, out
}

func get(path string) (int, map[string]any) {
	resp, err := http.Get(baseURL + path)
	if err != nil {
		return 0, map[string]any{"transport_error": err.Error()}
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(data, &out)
	return resp.StatusCode, out
}

func errCode(body map[string]any) string {
	if e, ok := body["error"].(map[string]any); ok {
		if c, ok := e["code"].(string); ok {
			return c
		}
	}
	return ""
}

func errCtx(body map[string]any) map[string]any {
	if e, ok := body["error"].(map[string]any); ok {
		if c, ok := e["context"].(map[string]any); ok {
			return c
		}
	}
	return nil
}

func waitReady(addr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/v1/recovery")
		if err == nil {
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			// confirm it is really our server, not a foreign listener
			if bytes.Contains(data, []byte("recovered_at")) {
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func startServer(addr, db, sampleDir string, extraEnv ...string) *exec.Cmd {
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		"BLOCKINSPECT_CHILD=1",
		"BLOCKINSPECT_ADDR="+addr,
		"BLOCKINSPECT_DB="+db,
		"BLOCKINSPECT_SAMPLE_DIR="+sampleDir,
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Println("cannot start server:", err)
		os.Exit(1)
	}
	return cmd
}

func main() {
	if os.Getenv("BLOCKINSPECT_CHILD") == "1" {
		runServer()
		return
	}
	runChecks()
}

func runServer() {
	st, err := store.Open(os.Getenv("BLOCKINSPECT_DB"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "open store:", err)
		os.Exit(1)
	}
	if _, err := st.Recover(); err != nil {
		fmt.Fprintln(os.Stderr, "recover:", err)
		os.Exit(1)
	}
	svc := service.New(st)
	srv := api.NewServer(svc, os.Getenv("BLOCKINSPECT_SAMPLE_DIR"))
	addr := os.Getenv("BLOCKINSPECT_ADDR")
	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		fmt.Fprintln(os.Stderr, "serve:", err)
		os.Exit(1)
	}
}

func runChecks() {
	workDir, err := os.MkdirTemp("", "verifyrun-*")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer os.RemoveAll(workDir)
	sampleDir := filepath.Join(workDir, "sample")
	dbPath := filepath.Join(workDir, "inspect.db")
	addr := "127.0.0.1:18473"
	baseURL = "http://" + addr
	spec := sample.DefaultSpec()

	fmt.Printf("verifyrun: workdir=%s seed=%d pages=%d\n\n", workDir, spec.Seed, spec.PageCount)

	srv := startServer(addr, dbPath, sampleDir)
	cleanup := func() {
		_ = srv.Process.Kill()
		_, _ = srv.Process.Wait()
	}
	defer cleanup()
	if !waitReady(addr, 10*time.Second) {
		fmt.Println("server did not become ready")
		os.Exit(1)
	}

	// --- sample generation & manifest load --------------------------------
	code, body := post("/v1/sample/generate", spec)
	check("generate fixed-seed sample", code == 200 && body["manifest"] != nil, "")

	code, m1 := get("/v1/manifest/" + spec.BlockID)
	check("manifest loaded", code == 200 && m1["block_id"] == spec.BlockID, "")

	// invalid manifest must fail with E_MANIFEST_INVALID and keep the old one
	badManifest := map[string]any{
		"block_id": "blk-bad", "rows": 1, "page_count": 2, "columns_per_page": 1,
		"page_lengths": []int{10, 10},
		"leaf_digests": []string{
			"0000000000000000000000000000000000000000000000000000000000000000",
			"1111111111111111111111111111111111111111111111111111111111111111",
		},
		"root_digest": "2222222222222222222222222222222222222222222222222222222222222222",
		"seed":        1,
	}
	badPath := filepath.Join(workDir, "bad-manifest.json")
	bb, _ := json.Marshal(badManifest)
	os.WriteFile(badPath, bb, 0o644)
	code, body = post("/v1/manifest/load", map[string]any{"path": badPath})
	check("E_MANIFEST_INVALID on root/leaf mismatch",
		errCode(body) == "E_MANIFEST_INVALID", errCode(body))
	code, m2 := get("/v1/manifest/" + spec.BlockID)
	check("old manifest still usable after invalid load",
		code == 200 && m2["block_id"] == spec.BlockID, "")

	// --- replica reports & full verification ------------------------------
	for _, rid := range spec.Replicas {
		rep, err := sample.LoadReport(filepath.Join(sampleDir, "reports", rid+".json"))
		if err != nil {
			fmt.Println("load report:", err)
			os.Exit(1)
		}
		code, _ = post("/v1/reports", rep)
		check("submit report "+rid, code == 200, "")
	}

	code, full := post("/v1/verify", map[string]any{
		"request_id": "req-full-1", "block_id": spec.BlockID, "mode": "full",
	})
	check("full verification succeeds", code == 200, fmt.Sprintf("http=%d err=%s", code, errCode(full)))
	cons, _ := full["consensus"].(map[string]any)
	check("quorum reached", cons != nil && cons["quorum"] == true,
		fmt.Sprintf("consensus_root=%v", cons["consensus_root"]))
	check("first divergent page located",
		cons != nil && int(cons["first_divergent_page"].(float64)) == spec.CorruptPages[0],
		fmt.Sprintf("first_divergent_page=%v", cons["first_divergent_page"]))
	div, _ := cons["divergence"].(map[string]any)
	reg, _ := div["registered"].(map[string]any)
	rec, _ := div["recomputed"].(map[string]any)
	check("divergence carries both sides' child digests and owners",
		div != nil && reg["left_digest"] != "" && rec["left_digest"] != "" &&
			len(reg["replicas"].([]any)) == 2 && len(rec["replicas"].([]any)) == 1,
		fmt.Sprintf("first_level=%v node=%v", div["first_level"], div["node_index"]))

	tr, _ := full["trust"].(map[string]any)
	scores, _ := tr["scores"].([]any)
	check("trust scores computed for all replicas", len(scores) == len(spec.Replicas), "")
	fmt.Println("\n  trust details:")
	for _, s := range scores {
		sm := s.(map[string]any)
		fmt.Printf("    %-10s score=%s\n", sm["replica_id"], sm["score_text"])
	}
	fmt.Printf("    ranking=%v cumulative_rounding_error_micro=%v steps=%d\n\n",
		tr["ranking"], tr["cumulative_error_micro"], len(tr["steps"].([]any)))
	dmg, _ := full["damage"].(map[string]any)
	check("damage range derived",
		dmg != nil && int(dmg["start_page"].(float64)) == spec.CorruptPages[0] &&
			int(dmg["end_page"].(float64)) == spec.CorruptPages[len(spec.CorruptPages)-1],
		fmt.Sprintf("start=%v end=%v cols=%d", dmg["start_page"], dmg["end_page"],
			len(dmg["affected_columns"].([]any))))

	// idempotency: same request_id + same payload reuses the result
	code, dup := post("/v1/verify", map[string]any{
		"request_id": "req-full-1", "block_id": spec.BlockID, "mode": "full",
	})
	check("duplicate request_id reuses result",
		code == 200 && dup["checkpoint_id"] == full["checkpoint_id"], "")
	// same request_id + different payload conflicts
	code, conflict := post("/v1/verify", map[string]any{
		"request_id": "req-full-1", "block_id": spec.BlockID, "mode": "incremental",
		"checkpoint_id": 0, "changed_pages": []int{1},
	})
	check("E_CONFLICTING_REQUEST on payload mismatch",
		errCode(conflict) == "E_CONFLICTING_REQUEST", errCode(conflict))
	// query by request id
	code, queried := get("/v1/verify/req-full-1")
	check("query verification result by request_id",
		code == 200 && queried["request_id"] == "req-full-1", "")

	// --- incremental verification -----------------------------------------
	cpID := int64(full["checkpoint_id"].(float64))
	changed := []int{3, 40, 41}
	incReq := map[string]any{
		"request_id": "req-inc-1", "block_id": spec.BlockID, "mode": "incremental",
		"checkpoint_id": cpID, "changed_pages": changed,
	}
	code, inc := post("/v1/verify", incReq)
	check("incremental verification succeeds", code == 200, fmt.Sprintf("err=%s", errCode(inc)))
	incr, _ := inc["incremental"].(map[string]any)
	recomputed := int(incr["recomputed_nodes"].(float64))
	reused := int(incr["reused_nodes"].(float64))
	total := int(incr["total_nodes"].(float64))
	check("incremental reuses unchanged subtrees",
		recomputed > 0 && reused > 0 && recomputed+reused == total && recomputed < total,
		fmt.Sprintf("recomputed=%d reused=%d savings=%v", recomputed, reused, incr["savings_text"]))
	// same block + same checkpoint must be bit-identical
	incReq2 := map[string]any{
		"request_id": "req-inc-2", "block_id": spec.BlockID, "mode": "incremental",
		"checkpoint_id": cpID, "changed_pages": changed,
	}
	code, inc2 := post("/v1/verify", incReq2)
	b1, _ := json.Marshal(inc["incremental"])
	b2, _ := json.Marshal(inc2["incremental"])
	check("incremental result bit-identical for same checkpoint",
		code == 200 && bytes.Equal(b1, b2), "")
	code, stats := get("/v1/blocks/" + spec.BlockID + "/incremental")
	check("incremental stats queryable", code == 200 && stats["total_nodes"] != nil, "")

	// --- E_PAGE_MISMATCH ---------------------------------------------------
	pagePath := sample.PagePath(sampleDir, 5)
	orig, _ := os.ReadFile(pagePath)
	os.WriteFile(pagePath, orig[:len(orig)-4], 0o644) // truncate page 5
	code, pm := post("/v1/verify", map[string]any{
		"request_id": "req-pm-1", "block_id": spec.BlockID, "mode": "full",
	})
	ctx := errCtx(pm)
	check("E_PAGE_MISMATCH with expected/actual lengths",
		errCode(pm) == "E_PAGE_MISMATCH" && ctx != nil &&
			ctx["expected_length"] != nil && ctx["actual_length"] != nil,
		fmt.Sprintf("page=%v expected=%v actual=%v", ctx["page"], ctx["expected_length"], ctx["actual_length"]))
	os.WriteFile(pagePath, orig, 0o644) // restore

	// --- E_NO_QUORUM -------------------------------------------------------
	// two extra replicas with distinct corruptions -> votes 2/2/1, no majority
	extra := map[string][]int{"replica-d": {0, 1}, "replica-e": {2}}
	for rid, pages := range extra {
		rep, _ := sample.LoadReport(filepath.Join(sampleDir, "reports", "replica-a.json"))
		rep.ReplicaID = rid
		var leaves []manifest.Digest
		for i, h := range rep.PageDigests {
			d, _ := manifest.ParseDigest(h)
			for _, p := range pages {
				if i == p {
					d[0] ^= 0x5A // deterministic extra corruption
				}
			}
			leaves = append(leaves, d)
		}
		for i, d := range leaves {
			rep.PageDigests[i] = d.Hex()
		}
		rep.RootDigest = manifest.BuildTree(leaves).Root().Hex()
		code, _ = post("/v1/reports", rep)
		check("submit extra report "+rid, code == 200, "")
	}
	code, nq := post("/v1/verify", map[string]any{
		"request_id": "req-nq-1", "block_id": spec.BlockID, "mode": "full",
	})
	nqCtx := errCtx(nq)
	check("E_NO_QUORUM with per-candidate votes",
		errCode(nq) == "E_NO_QUORUM" && nqCtx != nil && nqCtx["votes"] != nil,
		fmt.Sprintf("votes=%v", nqCtx["votes"]))

	// --- hard-kill recovery -------------------------------------------------
	crashReq := map[string]any{
		"request_id": "req-crash-1", "block_id": spec.BlockID, "mode": "full",
		"debug_crash_after_intent": true,
	}
	go post("/v1/verify", crashReq) // server will die mid-request
	srv.Wait()                      // process killed itself after the intent
	check("server hard-killed after intent", true, "")

	srv2 := startServer(addr, dbPath, sampleDir)
	defer func() {
		_ = srv2.Process.Kill()
		_, _ = srv2.Process.Wait()
	}()
	if !waitReady(addr, 10*time.Second) {
		fmt.Println("server did not restart")
		os.Exit(1)
	}
	recCode, rec := get("/v1/recovery")
	aborted, _ := rec["aborted_intents"].([]any)
	check("recovery reports aborted intent from crash",
		recCode == 200 && len(aborted) >= 1, fmt.Sprintf("aborted=%d", len(aborted)))
	cps, _ := rec["last_checkpoints"].([]any)
	check("last committed checkpoint intact after crash", len(cps) >= 1, "")
	code, crashed := get("/v1/verify/req-crash-1")
	check("E_RECOVERY_INCOMPLETE for crashed request",
		errCode(crashed) == "E_RECOVERY_INCOMPLETE", errCode(crashed))

	fmt.Printf("\nverifyrun: %d checks, %d failed\n", checks, failed)
	if failed > 0 {
		os.Exit(1)
	}
}
