package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"mvccengine/internal/engine"
	"mvccengine/internal/httpapi"
	"mvccengine/pkg/mvcc"
	"mvccengine/samples"
)

type Check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail any    `json:"detail,omitempty"`
}

type DemoReport struct {
	OK                    bool              `json:"ok"`
	GeneratedAtMS         int64             `json:"generated_at_ms"`
	GoVersion             string            `json:"go_version"`
	TempDir               string            `json:"temp_dir"`
	ConcurrencyServerAddr string            `json:"concurrency_server_addr"`
	Checks                []Check           `json:"checks"`
	ErrorCoverage         map[string]string `json:"error_coverage"`
	RecoveryReport        any               `json:"recovery_report,omitempty"`
	ReclaimResult         any               `json:"reclaim_result,omitempty"`
	Crash                 any               `json:"hard_kill_recovery,omitempty"`
}

type runner struct {
	checks []Check
	failed int
}

func (r *runner) check(name string, fn func() (any, error)) {
	detail, err := fn()
	if err != nil {
		r.failed++
		errorDetail := map[string]any{"error": err.Error()}
		var apiErr *apiError
		if errors.As(err, &apiErr) {
			errorDetail["code"] = apiErr.Code
			errorDetail["context"] = apiErr.Context
			errorDetail["status"] = apiErr.Status
		}
		if detail != nil {
			errorDetail["detail"] = detail
		}
		r.checks = append(r.checks, Check{Name: name, OK: false, Detail: errorDetail})
		return
	}
	r.checks = append(r.checks, Check{Name: name, OK: true, Detail: detail})
}

func runDemo() (*DemoReport, error) {
	now := time.Now().UnixMilli()
	tempDir, err := os.MkdirTemp("", "mvcc-demo-")
	if err != nil {
		return nil, err
	}
	eng, err := engine.Open(filepath.Join(tempDir, "demo.db"))
	if err != nil {
		return nil, err
	}
	defer eng.Close()

	addr, srv, err := startHTTP(eng)
	if err != nil {
		return nil, err
	}
	defer srv.Shutdown(context.Background())
	client := newHTTPClient(addr)

	r := &runner{}
	errorCoverage := map[string]string{}

	r.check("deterministic_sample_valid", func() (any, error) {
		sample, err := samples.Deterministic()
		if err != nil {
			return nil, err
		}
		return eng.ImportSample(sample)
	})

	r.check("sample_self_uncommitted_and_aborted_reasons", func() (any, error) {
		result, err := eng.Read(engine.ReadRequest{TxnSeq: 4, RowKey: "acct:alpha"})
		if err != nil {
			return nil, err
		}
		if !result.Visible || result.HitVersionSeq == nil || *result.HitVersionSeq != 2 {
			return result, fmt.Errorf("expected v2 visible")
		}
		reasons := map[int64]string{}
		for _, candidate := range result.Candidates {
			reasons[candidate.VersionSeq] = candidate.Reason
		}
		if reasons[5] != "SELF_UNCOMMITTED" || reasons[3] != "ROLLED_BACK" {
			return result, fmt.Errorf("missing distinguishable reasons: %v", reasons)
		}
		repeat, err := eng.Read(engine.ReadRequest{TxnSeq: 4, RowKey: "acct:alpha"})
		if err != nil {
			return nil, err
		}
		if string(result.CanonicalJSON()) != string(repeat.CanonicalJSON()) {
			return nil, fmt.Errorf("repeated snapshot read is not bitwise consistent")
		}
		return result, nil
	})

	r.check("gapless_txn_validation", func() (any, error) {
		bad, _ := samples.Deterministic()
		bad.Transactions = append([]mvcc.SampleTxn{}, bad.Transactions...)
		bad.Transactions[1].TxnSeq = 3
		err := bad.Validate()
		me := mvcc.AsError(err)
		if me.Code != mvcc.CodeTxnInvalid {
			return nil, fmt.Errorf("code=%s", me.Code)
		}
		errorCoverage[string(me.Code)] = "gapless_txn_validation"
		return me, nil
	})

	r.check("version_validation", func() (any, error) {
		bad, _ := samples.Deterministic()
		bad.Versions = append([]mvcc.SampleVersion{}, bad.Versions...)
		bad.Versions[0].RowKey = ""
		err := bad.Validate()
		me := mvcc.AsError(err)
		if me.Code != mvcc.CodeVersionInvalid {
			return nil, fmt.Errorf("code=%s", me.Code)
		}
		errorCoverage[string(me.Code)] = "version_validation"
		return me, nil
	})

	var isolationDetail any
	r.check("concurrent_workers_and_idempotent_waiters", func() (any, error) {
		reader, err := client.post("/txns/begin", map[string]any{})
		if err != nil {
			return nil, err
		}
		readerSeq := intField(reader, "txn_seq")
		if readerSeq != 5 {
			return nil, fmt.Errorf("reader seq=%d want5", readerSeq)
		}

		const workers = 10
		var wg sync.WaitGroup
		errCh := make(chan error, workers)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				row := fmt.Sprintf("conc:row:%02d", i)
				txn, err := client.post("/txns/begin", map[string]any{"request_id": fmt.Sprintf("conc-begin-%d", i)})
				if err != nil {
					errCh <- err
					return
				}
				seq := intField(txn, "txn_seq")
				if _, err := client.post("/versions", map[string]any{"request_id": fmt.Sprintf("conc-write-%d", i), "txn_seq": seq, "row_key": row, "content": fmt.Sprintf("v-%d", i)}); err != nil {
					errCh <- err
					return
				}
				if _, err := client.post("/txns/commit", map[string]any{"request_id": fmt.Sprintf("conc-commit-%d", i), "txn_seq": seq}); err != nil {
					errCh <- err
				}
			}(i)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				return nil, err
			}
		}

		outside, err := client.post("/rows/read", map[string]any{"txn_seq": readerSeq, "row_key": "conc:row:00"})
		if err != nil {
			return nil, err
		}
		if outside["visible"] == true {
			return outside, fmt.Errorf("old snapshot saw snapshot-outside commit")
		}

		sameTxn, err := client.post("/txns/begin", map[string]any{})
		if err != nil {
			return nil, err
		}
		sameSeq := intField(sameTxn, "txn_seq")
		if _, err := client.post("/versions", map[string]any{"txn_seq": sameSeq, "row_key": "idem:row", "content": "same-request"}); err != nil {
			return nil, err
		}
		const waiters = 8
		var commitWG sync.WaitGroup
		type commitOut struct {
			result map[string]any
			err    error
		}
		outCh := make(chan commitOut, waiters)
		start := make(chan struct{})
		for i := 0; i < waiters; i++ {
			commitWG.Add(1)
			go func() {
				defer commitWG.Done()
				<-start
				res, err := client.post("/txns/commit", map[string]any{"request_id": "demo-same-request", "txn_seq": sameSeq})
				outCh <- commitOut{res, err}
			}()
		}
		close(start)
		commitWG.Wait()
		close(outCh)
		intentIDs := map[string]bool{}
		okResponses := 0
		for out := range outCh {
			if out.err != nil {
				return nil, out.err
			}
			okResponses++
			intentIDs[stringField(out.result, "intent_id")] = true
		}
		if okResponses != waiters || len(intentIDs) != 1 {
			return nil, fmt.Errorf("idempotent waiters ok=%d intents=%d", okResponses, len(intentIDs))
		}

		finalReader, err := client.post("/txns/begin", map[string]any{})
		if err != nil {
			return nil, err
		}
		finalSeq := intField(finalReader, "txn_seq")
		for i := 0; i < workers; i++ {
			row := fmt.Sprintf("conc:row:%02d", i)
			read, err := client.post("/rows/read", map[string]any{"txn_seq": finalSeq, "row_key": row})
			if err != nil {
				return nil, err
			}
			if read["visible"] != true {
				return read, fmt.Errorf("new snapshot did not see worker row %s", row)
			}
		}
		state, err := client.get("/state")
		if err != nil {
			return nil, err
		}
		detail := map[string]any{
			"workers":                workers,
			"same_request_responses": okResponses,
			"write_serial":           intField(state, "next_txn_seq"),
			"reader_txn_seq":         readerSeq,
			"new_txn_seq":            finalSeq,
			"intent_ids":             intentIDs,
		}
		isolationDetail = map[string]any{
			"reader_txn_seq": readerSeq,
			"new_txn_seq":    finalSeq,
			"workers":        workers,
		}
		return detail, nil
	})
	r.check("snapshot_isolation_concurrent_commits", func() (any, error) {
		if isolationDetail == nil {
			return nil, fmt.Errorf("concurrency scenario did not complete")
		}
		return isolationDetail, nil
	})

	r.check("conflicting_idempotent_request", func() (any, error) {
		_, err := client.post("/txns/commit", map[string]any{"request_id": "demo-same-request", "txn_seq": 999})
		apiErr, assertErr := mustAPICode(err, "E_CONFLICTING_REQUEST")
		if assertErr != nil {
			return nil, assertErr
		}
		errorCoverage["E_CONFLICTING_REQUEST"] = "conflicting_idempotent_request"
		return map[string]any{"code": apiErr.Code, "context": apiErr.Context, "message": apiErr.Message}, nil
	})

	r.check("http_txn_invalid", func() (any, error) {
		_, err := client.post("/txns/commit", map[string]any{"txn_seq": 99999})
		apiErr, assertErr := mustAPICode(err, "E_TXN_INVALID")
		if assertErr != nil {
			return nil, assertErr
		}
		errorCoverage["E_TXN_INVALID"] = "http_txn_invalid"
		return apiErr.Body, nil
	})

	r.check("http_version_invalid", func() (any, error) {
		_, err := client.post("/versions", map[string]any{"txn_seq": 1, "row_key": "", "digest": "bad"})
		apiErr, assertErr := mustAPICode(err, "E_VERSION_INVALID")
		if assertErr != nil {
			return nil, assertErr
		}
		errorCoverage["E_VERSION_INVALID"] = "http_version_invalid"
		return apiErr.Body, nil
	})

	seedDemoFork(eng)
	if err := eng.Reload(); err != nil {
		return nil, err
	}
	r.check("chain_fork_detection", func() (any, error) {
		_, err := client.post("/chains/validate", map[string]any{"row_key": "chain:fork"})
		apiErr, assertErr := mustAPICode(err, "E_VERSION_CHAIN_BROKEN")
		if assertErr != nil {
			return nil, assertErr
		}
		errorCoverage["E_VERSION_CHAIN_BROKEN"] = "chain_fork_detection"
		return map[string]any{"code": apiErr.Code, "context": apiErr.Context, "message": apiErr.Message}, nil
	})
	r.check("chain_fork_repair", func() (any, error) {
		repaired, err := client.post("/chains/repair", map[string]any{"request_id": "demo-repair-fork", "row_key": "chain:fork"})
		if err != nil {
			return nil, err
		}
		if _, err := client.post("/chains/validate", map[string]any{"row_key": "chain:fork"}); err != nil {
			return nil, err
		}
		return repaired, nil
	})

	badSnapshotPath := filepath.Join(tempDir, "bad-snapshot.json")
	badSnapshot := mvcc.Snapshot{SnapshotSeq: 2, GeneratedAtMS: now, TxnUpperBound: 17, Checksum: "0000000000000000000000000000000000000000000000000000000000000000"}
	_ = os.WriteFile(badSnapshotPath, []byte(fmt.Sprintf(`{"snapshot_seq":2,"generated_at_ms":%d,"txn_upper_bound":17,"checksum":%q}`, now, badSnapshot.Checksum)), 0o644)
	r.check("invalid_snapshot_checksum", func() (any, error) {
		_, err := client.post("/snapshots/switch", map[string]any{"request_id": "demo-bad-snapshot", "path": badSnapshotPath})
		apiErr, assertErr := mustAPICode(err, "E_SNAPSHOT_INVALID")
		if assertErr != nil {
			return nil, assertErr
		}
		errorCoverage["E_SNAPSHOT_INVALID"] = "invalid_snapshot_checksum"
		return map[string]any{"code": apiErr.Code, "context": apiErr.Context}, nil
	})
	r.check("snapshot_atomic_switch", func() (any, error) {
		valid := mvcc.Snapshot{SnapshotSeq: 2, GeneratedAtMS: now + 1, TxnUpperBound: 17}.WithChecksum()
		path := filepath.Join(tempDir, "snapshot-2.json")
		if err := engine.WriteSnapshotFile(path, valid); err != nil {
			return nil, err
		}
		result, err := client.post("/snapshots/switch", map[string]any{"request_id": "demo-good-snapshot", "path": path})
		if err != nil {
			return nil, err
		}
		if intField(result, "affected_transactions") != 3 {
			return result, fmt.Errorf("affected=%v", result["affected_transactions"])
		}
		return result, nil
	})

	crashDetail, err := runCrashScenario(tempDir)
	if err != nil {
		r.check("hard_kill_recovery_scenario", func() (any, error) { return crashDetail, err })
	} else {
		r.check("hard_kill_after_commit_intent", func() (any, error) { return crashDetail["hard_kill"], nil })
		r.check("recovery_preserves_snapshot_reads_row_by_row", func() (any, error) { return crashDetail, nil })
		if code, ok := crashDetail["reclaim_unsafe_code"].(string); ok {
			errorCoverage[code] = "long_snapshot_blocks_reclaim"
		}
		r.check("long_snapshot_blocks_reclaim", func() (any, error) {
			if crashDetail["reclaim_unsafe_code"] != "E_RECLAIM_UNSAFE" {
				return crashDetail["reclaim_unsafe_error"], fmt.Errorf("missing E_RECLAIM_UNSAFE")
			}
			return crashDetail["reclaim_unsafe_error"], nil
		})
		r.check("safe_reclaim_after_long_snapshot_ends", func() (any, error) { return crashDetail["reclaim_result"], nil })
	}

	report := &DemoReport{
		OK:                    r.failed == 0 && err == nil,
		GeneratedAtMS:         now,
		GoVersion:             goVersion(),
		TempDir:               tempDir,
		ConcurrencyServerAddr: addr,
		Checks:                r.checks,
		ErrorCoverage:         errorCoverage,
	}
	if crashDetail != nil {
		report.Crash = crashDetail
		report.RecoveryReport = crashDetail["recovery"]
		report.ReclaimResult = crashDetail["reclaim_result"]
	}
	if err != nil {
		report.OK = false
		return report, err
	}
	if r.failed > 0 {
		report.OK = false
		return report, fmt.Errorf("%d demo checks failed", r.failed)
	}
	return report, nil
}

func startHTTP(eng *engine.Engine) (string, *http.Server, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	server := &http.Server{Handler: httpapi.NewServer(eng).Handler()}
	go func() { _ = server.Serve(listener) }()
	return "http://" + listener.Addr().String(), server, nil
}

func readProjection(c *httpClient, txnSeq int64, rowKey string) (map[string]any, error) {
	result, err := c.post("/rows/read", map[string]any{"txn_seq": txnSeq, "row_key": rowKey})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"visible":         result["visible"],
		"outcome":         result["outcome"],
		"reason":          result["reason"],
		"hit_version_seq": result["hit_version_seq"],
		"hit_digest":      result["hit_digest"],
	}, nil
}

func runCrashScenario(tempDir string) (map[string]any, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	dbPath := filepath.Join(tempDir, "crash.db")
	snapshotDir := filepath.Join(tempDir, "crash-snapshots")
	readyFile := filepath.Join(tempDir, "crash-ready")
	crashChild, err := startChildServer(exe, dbPath, snapshotDir, []string{
		"MVCC_CRASH_COMMIT_REQUEST_ID=crash-commit",
		"MVCC_CRASH_READY_FILE=" + readyFile,
	})
	if err != nil {
		return nil, err
	}
	defer crashChild.kill()
	c := newHTTPClient(crashChild.addr)

	txn1, err := c.post("/txns/begin", map[string]any{})
	if err != nil {
		return nil, err
	}
	txn1Seq := intField(txn1, "txn_seq")
	if _, err := c.post("/versions", map[string]any{"txn_seq": txn1Seq, "row_key": "recover:row1", "content": "row1-v1"}); err != nil {
		return nil, err
	}
	if _, err := c.post("/versions", map[string]any{"txn_seq": txn1Seq, "row_key": "recover:row2", "content": "row2-v1"}); err != nil {
		return nil, err
	}
	if _, err := c.post("/txns/commit", map[string]any{"request_id": "recover-seed-commit", "txn_seq": txn1Seq}); err != nil {
		return nil, err
	}
	longTxn, err := c.post("/txns/begin", map[string]any{})
	if err != nil {
		return nil, err
	}
	longSeq := intField(longTxn, "txn_seq")
	txn3, err := c.post("/txns/begin", map[string]any{})
	if err != nil {
		return nil, err
	}
	txn3Seq := intField(txn3, "txn_seq")
	if _, err := c.post("/versions", map[string]any{"request_id": "recover-write-crash", "txn_seq": txn3Seq, "row_key": "recover:row1", "content": "uncommitted-before-kill"}); err != nil {
		return nil, err
	}
	expected1, err := readProjection(c, longSeq, "recover:row1")
	if err != nil {
		return nil, err
	}
	expected2, err := readProjection(c, longSeq, "recover:row2")
	if err != nil {
		return nil, err
	}

	type commitResult struct {
		result map[string]any
		err    error
	}
	commitCh := make(chan commitResult, 1)
	go func() {
		res, err := c.post("/txns/commit", map[string]any{"request_id": "crash-commit", "txn_seq": txn3Seq})
		commitCh <- commitResult{res, err}
	}()
	if err := waitForFile(readyFile, 10*time.Second); err != nil {
		return nil, fmt.Errorf("crash hook did not persist intent: %w", err)
	}
	intentBytes, _ := os.ReadFile(readyFile)
	if err := crashChild.kill(); err != nil {
		return nil, err
	}
	commitOut := <-commitCh
	hardKill := map[string]any{
		"intent_id":       string(intentBytes),
		"transport_error": fmt.Sprint(commitOut.err),
		"response":        commitOut.result,
		"process_killed":  true,
	}
	if commitOut.err == nil {
		return map[string]any{"hard_kill": hardKill}, fmt.Errorf("commit unexpectedly returned after kill")
	}

	restarted, err := startChildServer(exe, dbPath, snapshotDir, nil)
	if err != nil {
		return nil, err
	}
	defer restarted.kill()
	rc := newHTTPClient(restarted.addr)
	recoveryEnvelope, err := rc.get("/recovery")
	if err != nil {
		return nil, err
	}
	recovery := recoveryEnvelope["recovery"]

	actual1, err := readProjection(rc, longSeq, "recover:row1")
	if err != nil {
		return nil, err
	}
	actual2, err := readProjection(rc, longSeq, "recover:row2")
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(expected1, actual1) || !reflect.DeepEqual(expected2, actual2) {
		return map[string]any{
			"recovery": recovery,
			"expected": map[string]any{"recover:row1": expected1, "recover:row2": expected2},
			"actual":   map[string]any{"recover:row1": actual1, "recover:row2": actual2},
		}, fmt.Errorf("snapshot read projections differ after recovery")
	}

	txn4, err := rc.post("/txns/begin", map[string]any{})
	if err != nil {
		return nil, err
	}
	txn4Seq := intField(txn4, "txn_seq")
	if _, err := rc.post("/versions", map[string]any{"txn_seq": txn4Seq, "row_key": "recover:row1", "content": "row1-v2-committed"}); err != nil {
		return nil, err
	}
	if _, err := rc.post("/txns/commit", map[string]any{"request_id": "recover-v2-commit", "txn_seq": txn4Seq}); err != nil {
		return nil, err
	}
	sweepReader, err := rc.post("/txns/begin", map[string]any{})
	if err != nil {
		return nil, err
	}
	sweepReaderSeq := intField(sweepReader, "txn_seq")
	preReclaim, err := readProjection(rc, sweepReaderSeq, "recover:row1")
	if err != nil {
		return nil, err
	}

	_, unsafeErr := rc.post("/reclaim", map[string]any{"request_id": "demo-reclaim-blocked", "row_key": "recover:row1", "version_seq": 1})
	apiErr, codeAssert := mustAPICode(unsafeErr, "E_RECLAIM_UNSAFE")
	unsafeDetail := map[string]any{}
	unsafeCode := ""
	if codeAssert == nil {
		unsafeCode = apiErr.Code
		unsafeDetail = map[string]any{"code": apiErr.Code, "context": apiErr.Context, "message": apiErr.Message}
	}

	if _, err := rc.post("/txns/rollback", map[string]any{"request_id": "demo-end-long-txn", "txn_seq": longSeq}); err != nil {
		return nil, err
	}
	reclaimResult, err := rc.post("/reclaim", map[string]any{"request_id": "demo-reclaim-safe", "all": true})
	if err != nil {
		return nil, err
	}
	postReclaim, err := readProjection(rc, sweepReaderSeq, "recover:row1")
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(preReclaim, postReclaim) {
		return nil, fmt.Errorf("visibility changed after reclaim: %v vs %v", preReclaim, postReclaim)
	}

	return map[string]any{
		"hard_kill":            hardKill,
		"recovery":             recovery,
		"expected":             map[string]any{"recover:row1": expected1, "recover:row2": expected2},
		"actual":               map[string]any{"recover:row1": actual1, "recover:row2": actual2},
		"reclaim_unsafe_code":  unsafeCode,
		"reclaim_unsafe_error": unsafeDetail,
		"reclaim_result":       reclaimResult,
		"pre_reclaim_read":     preReclaim,
		"post_reclaim_read":    postReclaim,
	}, codeAssert
}

func seedDemoFork(eng *engine.Engine) {
	nowMS := time.Now().UnixMilli()
	_, _ = eng.DB().Exec(`INSERT OR REPLACE INTO txns(seq,status,started_at_ms,snapshot_seq,upper_bound) VALUES(80,'COMMITTED',?,1,80)`, nowMS)
	digest := mvcc.DigestContent("fork")
	for _, seq := range []int64{17, 18, 19} {
		var prev any
		if seq != 17 {
			prev = int64(17)
		}
		_, _ = eng.DB().Exec(`INSERT OR REPLACE INTO versions(row_key,version_seq,create_txn_seq,commit_state,digest,content,prev_version_seq,is_delete,created_at_ms,updated_at_ms)
			VALUES('chain:fork',?,80,'COMMITTED',?,'fork',?,0,?,?)`, seq, digest, prev, nowMS, nowMS)
	}
	_, _ = eng.DB().Exec(`INSERT OR REPLACE INTO row_heads(row_key,version_seq) VALUES('chain:fork',19)`)
}

func goVersion() string {
	return "go1.22+ (built with " + runtimeVersion() + ")"
}
