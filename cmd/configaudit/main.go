package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"configaudit/internal/audit"
	"configaudit/internal/httpapi"
	"configaudit/internal/parser"
	"configaudit/internal/report"
	"configaudit/internal/sample"
	"configaudit/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	dir := fs.String("dir", "data", "working directory for samples and sqlite database")
	db := fs.String("db", "audit.db", "sqlite database file name (relative to -dir unless absolute)")
	seed := fs.Int64("seed", sample.DefaultSeed, "deterministic sample generator seed")
	addr := fs.String("addr", "127.0.0.1:8080", "listen address (serve only)")
	_ = fs.Parse(os.Args[2:])

	ctx := context.Background()
	switch cmd {
	case "generate":
		runGenerate(*dir, *seed)
	case "audit":
		runAudit(ctx, *dir, *db)
	case "serve":
		runServe(ctx, *dir, *db, *addr)
	case "recover":
		runRecover(ctx, *dir, *db)
	case "crash-demo":
		runCrashDemo(ctx, *dir, *db)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: configaudit <generate|audit|serve|recover|crash-demo> [flags]")
}

func sampleDir(dir string) string { return filepath.Join(dir, "samples") }

func dbPath(dir, name string) string {
	if filepath.IsAbs(name) {
		return name
	}
	return filepath.Join(dir, name)
}

func runGenerate(dir string, seed int64) {
	layout, err := sample.Generate(sampleDir(dir), seed)
	must(err)
	fmt.Printf("samples generated under %s\n", layout.Dir)
	for _, p := range append([]string{layout.SpecFile, layout.NodeFile}, layout.Sources...) {
		fmt.Println(" -", p)
	}
	fmt.Println(" -", layout.Reference)
}

func loadDataset(dir string) (*parser.Dataset, error) {
	layout := sample.Layout{
		SpecFile:  filepath.Join(sampleDir(dir), "specs.jsonl"),
		NodeFile:  filepath.Join(sampleDir(dir), "nodes.jsonl"),
		Reference: filepath.Join(sampleDir(dir), "references.jsonl"),
	}
	matches, err := filepath.Glob(filepath.Join(sampleDir(dir), "sources-*.jsonl"))
	if err != nil {
		return nil, err
	}
	layout.Sources = matches
	return parser.Load(layout.SpecFile, layout.NodeFile, layout.Reference, layout.Sources)
}

func openStore(ctx context.Context, dir, db string) (*store.Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return store.Open(ctx, dbPath(dir, db))
}

func runAudit(ctx context.Context, dir, db string) {
	ds, err := loadDataset(dir)
	must(err)
	st, err := openStore(ctx, dir, db)
	must(err)
	defer st.Close()
	svc := audit.NewService(st, ds)
	reqID := fmt.Sprintf("full-%d", time.Now().UTC().UnixNano())
	snap, err := svc.FullAudit(ctx, reqID, map[string]any{"seed": sample.DefaultSeed})
	must(err)
	printSummary(snap.Report)
	fmt.Printf("snapshot v%d checksum=%s request=%s\n", snap.Version, snap.Checksum, reqID)
}

func runServe(ctx context.Context, dir, db, addr string) {
	ds, err := loadDataset(dir)
	must(err)
	st, err := openStore(ctx, dir, db)
	must(err)
	defer st.Close()
	rep, err := st.Recover(ctx)
	must(err)
	if rep != nil {
		b, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Fprintf(os.Stderr, "startup recovery: %s\n", string(b))
	}
	svc := audit.NewService(st, ds)
	srv := httpapi.NewServer(svc)
	fmt.Printf("listening on %s\n", addr)
	httpServer := &http.Server{Addr: addr, Handler: srv.Mux}
	must(httpServer.ListenAndServe())
}

func runRecover(ctx context.Context, dir, db string) {
	st, err := openStore(ctx, dir, db)
	must(err)
	defer st.Close()
	rep, err := st.Recover(ctx)
	must(err)
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
}

// runCrashDemo simulates a forced kill: it writes the audit intent and running
// row, then exits with status 9 before committing. A subsequent "recover" shows
// the rollback to the last complete snapshot.
func runCrashDemo(ctx context.Context, dir, db string) {
	if os.Getenv("CONFIGAUDIT_CRASH_CHILD") == "1" {
		ds, err := loadDataset(dir)
		must(err)
		st, err := openStore(ctx, dir, db)
		must(err)
		defer st.Close()
		svc := audit.NewService(st, ds)
		// One intent + one running audit row, then a forced exit before commit.
		_ = svc.Store().CreateAuditIntent(ctx, "crashed-req", "hash", "full", 0, 0)
		fmt.Fprintln(os.Stderr, "child: intent written, simulating SIGKILL")
		os.Exit(9)
	}
	// Parent: ensure one complete snapshot exists first.
	runAudit(ctx, dir, db)
	cmd := newChildCommand(dir, db)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), "CONFIGAUDIT_CRASH_CHILD=1")
	_ = cmd.Run()
	fmt.Println("parent: child force-exited; running recovery now")
	runRecover(ctx, dir, db)
}

func printSummary(r *report.Report) {
	fmt.Printf("audit report: nodes=%d keys=%d combinations=%d\n", r.NodeCount, r.KeyCount, r.CombinationCnt)
	for _, kr := range r.Keys {
		fmt.Printf("  %s/%s winner=%s order=%v overlay_err_ulp=%d effective=%s bound=%s\n",
			kr.NodeID, kr.KeyPath, kr.WinnerChannel, kr.OverlayOrder, kr.OverlayErrorULP,
			kr.Effective.Decimal(), kr.EffectiveBound)
		for _, c := range kr.Combinations {
			fmt.Printf("    - channel=%s final=%s ref=%s abs=%s rel=%s bound=%s within=%v\n",
				c.Channel, c.ChainFinal.Decimal(), c.Reference, c.AbsDeviation, c.RelDeviation,
				c.ErrorBound, c.WithinBound)
		}
		for _, c := range kr.Contributions {
			if c.Shadowed {
				fmt.Printf("    - shadow: channel=%s reason=%s\n", c.Channel, c.ShadowReason)
			}
		}
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
