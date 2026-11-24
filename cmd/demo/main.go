package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"blockpool"
	"blockpool/httpapi"
)

type demo struct {
	pool      *blockpool.Pool
	sample    blockpool.Sample
	demoBlock blockpool.SampleBlock
	content   []byte
	namespace string
	mu        sync.Mutex
	counter   int64
}

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "loopback HTTP listen address")
	dbPath := flag.String("db", filepath.Join(".demo", "blockpool.sqlite"), "SQLite database path")
	samplePath := flag.String("sample", filepath.Join("samples", "fixed.json"), "fixed local sample")
	flag.Parse()

	sample, err := blockpool.ReadSample(*samplePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read sample: %v\n", err)
		os.Exit(1)
	}
	pool, openErr := blockpool.Open(*dbPath, blockpool.Options{DefaultLeaseTTL: 60 * time.Second})
	if openErr != nil {
		fmt.Fprintf(os.Stderr, "open pool: %v\n", openErr)
		os.Exit(1)
	}
	defer pool.Close()
	if err := blockpool.SeedSample(context.Background(), pool, sample); err != nil {
		fmt.Fprintf(os.Stderr, "seed sample: %v\n", err)
		os.Exit(1)
	}

	d := &demo{
		pool:      pool,
		sample:    sample,
		demoBlock: sample.Blocks[5],
		namespace: sample.Namespaces[0],
	}
	d.content, _ = base64.StdEncoding.DecodeString(d.demoBlock.DataBase64)

	api := httpapi.New(pool)
	api.Mux().HandleFunc("/v1/demo/info", d.info)
	api.Mux().HandleFunc("/v1/demo/lease-expiry", d.leaseExpiry)
	api.Mux().HandleFunc("/v1/demo/reclaim-race", d.reclaimRace)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("block pool demo listening on http://%s\n", *addr)
	fmt.Printf("database: %s\nsample: %s (seed=%d, blocks=%d, actions=%d)\n",
		*dbPath, *samplePath, int64(sample.Seed), len(sample.Blocks), len(sample.Actions))
	fmt.Printf("demo block: %s size=%d retention_ms=%d\n", d.demoBlock.Digest, int64(d.demoBlock.SizeBytes), int64(d.demoBlock.RetentionMS))
	fmt.Println("POST /v1/demo/lease-expiry   -> mount, wait for persisted expiry, reclaim")
	fmt.Println("POST /v1/demo/reclaim-race   -> race one mount against one reclaim")
	if err := httpapi.ListenAndServe(ctx, *addr, api.Handler()); err != nil {
		fmt.Fprintf(os.Stderr, "serve: %v\n", err)
		os.Exit(1)
	}
}

func (d *demo) nextRequestID(prefix string) string {
	d.counter++
	return fmt.Sprintf("demo-%s-%d-%d", prefix, time.Now().UnixNano(), d.counter)
}

func (d *demo) uniqueNode(prefix string) (blockpool.NodeRef, *blockpool.Error) {
	ref := blockpool.NodeRef{Namespace: d.namespace, Path: fmt.Sprintf("demo/%s-%d", prefix, time.Now().UnixNano())}
	_, err := d.pool.CreateNode(context.Background(), ref)
	return ref, err
}

func (d *demo) resetDemoBlock(ctx context.Context) *blockpool.Error {
	_, err := d.pool.Ingest(ctx, blockpool.IngestRequest{
		RequestID: d.nextRequestID("ingest"), Digest: d.demoBlock.Digest,
		Data: d.content, RetentionMS: 0,
	})
	return err
}

func (d *demo) info(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		d.method(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sample_seed": int64(d.sample.Seed),
		"demo_digest": d.demoBlock.Digest,
		"demo_size":   int64(d.demoBlock.SizeBytes),
		"endpoints":   []string{"POST /v1/demo/lease-expiry", "POST /v1/demo/reclaim-race"},
		"note":        "All demo bindings are loopback-only. Reclaim removes content; the next demo request re-ingests the fixed sample bytes.",
	})
}

func (d *demo) leaseExpiry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		d.method(w, r)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	ctx := r.Context()
	if err := d.resetDemoBlock(ctx); err != nil {
		writeError(w, err)
		return
	}
	node, err := d.uniqueNode("lease-expiry")
	if err != nil {
		writeError(w, err)
		return
	}
	const leaseMS int64 = 120
	lease, bizErr := d.pool.Mount(ctx, blockpool.MountRequest{
		RequestID: d.nextRequestID("mount"), Namespace: node.Namespace, Path: node.Path,
		Digest: d.demoBlock.Digest, LeaseTTLMS: leaseMS,
	})
	if bizErr != nil {
		writeError(w, bizErr)
		return
	}
	select {
	case <-time.After(time.Duration(leaseMS+40) * time.Millisecond):
	case <-ctx.Done():
		writeError(w, blockpool.Errorf(blockpool.CodeContextCanceled, "demo request canceled"))
		return
	}
	after, afterErr := d.pool.GetBlock(ctx, d.demoBlock.Digest)
	if afterErr != nil {
		writeError(w, afterErr)
		return
	}
	reclaimed, reclaimErr := d.pool.Reclaim(ctx, blockpool.ReclaimRequest{
		RequestID: d.nextRequestID("reclaim"), Digest: d.demoBlock.Digest,
	})
	if reclaimErr != nil {
		writeError(w, reclaimErr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"lease":              lease,
		"after_expiry":       after,
		"expiry_was_honored": after.RefCount == 0 && after.ActiveLeaseCount == 0,
		"reclaim":            reclaimed,
	})
}

func (d *demo) reclaimRace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		d.method(w, r)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	ctx := r.Context()
	if err := d.resetDemoBlock(ctx); err != nil {
		writeError(w, err)
		return
	}
	node, err := d.uniqueNode("race")
	if err != nil {
		writeError(w, err)
		return
	}
	type mountOutcome struct {
		lease blockpool.Lease
		err   *blockpool.Error
	}
	type reclaimOutcome struct {
		result blockpool.ReclaimResult
		err    *blockpool.Error
	}
	mountDone := make(chan mountOutcome, 1)
	reclaimDone := make(chan reclaimOutcome, 1)
	mountRequestID := d.nextRequestID("race-mount")
	reclaimRequestID := d.nextRequestID("race-reclaim")
	start := make(chan struct{})
	go func() {
		<-start
		lease, mountErr := d.pool.Mount(ctx, blockpool.MountRequest{
			RequestID: mountRequestID, Namespace: node.Namespace, Path: node.Path,
			Digest: d.demoBlock.Digest, LeaseTTLMS: 60_000,
		})
		mountDone <- mountOutcome{lease: lease, err: mountErr}
	}()
	go func() {
		<-start
		reclaimed, reclaimErr := d.pool.Reclaim(ctx, blockpool.ReclaimRequest{
			RequestID: reclaimRequestID, Digest: d.demoBlock.Digest,
		})
		reclaimDone <- reclaimOutcome{result: reclaimed, err: reclaimErr}
	}()
	close(start)
	mountResult := <-mountDone
	reclaimResult := <-reclaimDone
	final, finalErr := d.pool.GetBlock(ctx, d.demoBlock.Digest)
	if finalErr != nil && finalErr.Code != blockpool.CodeBlockNotFound {
		writeError(w, finalErr)
		return
	}

	// Leave the next invocation with the same clean starting facts.
	if mountResult.err == nil {
		_, _ = d.pool.Unmount(ctx, blockpool.UnmountRequest{
			RequestID: d.nextRequestID("cleanup-unmount"), Namespace: node.Namespace,
			Path: node.Path, Digest: d.demoBlock.Digest,
		})
	}
	check, checkErr := d.pool.GetBlock(ctx, d.demoBlock.Digest)
	if checkErr == nil && check.State == blockpool.BlockLive && check.RefCount == 0 {
		_, _ = d.pool.Reclaim(ctx, blockpool.ReclaimRequest{
			RequestID: d.nextRequestID("cleanup-reclaim"), Digest: d.demoBlock.Digest,
		})
	}

	response := map[string]any{
		"mount":                mountResult.lease,
		"mount_error":          mountResult.err,
		"reclaim":              reclaimResult.result,
		"reclaim_error":        reclaimResult.err,
		"final_before_cleanup": final,
		"mutually_exclusive":   (mountResult.err == nil) != (reclaimResult.err == nil),
	}
	writeJSON(w, http.StatusOK, response)
}

func (d *demo) method(w http.ResponseWriter, r *http.Request) {
	writeError(w, blockpool.Errorf(blockpool.CodeSpecInvalid, "method is not allowed",
		"method", r.Method, "path", r.URL.Path))
}

func writeError(w http.ResponseWriter, err *blockpool.Error) {
	status := http.StatusConflict
	switch err.Code {
	case blockpool.CodeSpecInvalid:
		status = http.StatusBadRequest
	case blockpool.CodeBlockNotFound, blockpool.CodeNodeUnknown, blockpool.CodeNotFound:
		status = http.StatusNotFound
	case blockpool.CodeInternal:
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, err)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}
